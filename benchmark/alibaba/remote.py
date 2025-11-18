from datetime import datetime
from os import error
import os
from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import Ed25519Key
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os.path import basename, splitext
from time import sleep
import subprocess
import concurrent.futures
import yaml
from tqdm import tqdm
from functools import partial

from benchmark.config import Committee, Key, TSSKey, NodeParameters, BenchParameters, ConfigError
from benchmark.utils import BenchError, Print, PathMaker, progress_bar
from benchmark.commands import CommandMaker
from benchmark.logs import LogParser, ParseError
from alibaba.instance import InstanceManager


def run_concurrent_tasks(task_fn, iterable, desc, max_workers=10):
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(max_workers, len(iterable))) as executor:
        futures = {executor.submit(task_fn, *args) if isinstance(args, tuple) 
                    else executor.submit(task_fn, args): args for args in iterable}
        with tqdm(total=len(futures), desc=desc) as pbar:
            for future in concurrent.futures.as_completed(futures):
                pbar.update(1)


class FabricError(Exception):
    ''' Wrapper for Fabric exception with a meaningfull error message. '''

    def __init__(self, error):
        assert isinstance(error, GroupException)
        message = list(error.result.values())[-1]
        super().__init__(message)


class ExecutionError(Exception):
    pass


class Bench:
    def __init__(self, ctx):
        self.manager = InstanceManager.make()
        self.settings = self.manager.settings
        try:
            # ssh 连接
            ctx.connect_kwargs.pkey = Ed25519Key.from_private_key_file(
                self.manager.settings.key_path
            )
            self.connect = ctx.connect_kwargs
        except (IOError, PasswordRequiredException, SSHException) as e:
            raise BenchError('Failed to load SSH key', e)

    def _check_stderr(self, output):
        if isinstance(output, dict):
            for x in output.values():
                if x.stderr:
                    raise ExecutionError(x.stderr)
        else:
            if output.stderr:
                raise ExecutionError(output.stderr)

    def kill(self, hosts=[], delete_logs=False):
        assert isinstance(hosts, list)
        assert isinstance(delete_logs, bool)
        hosts = hosts if hosts else self.manager.hosts(flat=True)

        cmd = ["true", f'({CommandMaker.kill()} || true)']

        # note: please set hostname (ubuntu) 
        try:
            g = Group(*hosts, user='root', connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)
        except GroupException as e:
            raise BenchError('Failed to kill nodes', FabricError(e))

    def _select_hosts(self, bench_parameters):
        nodes = max(bench_parameters.nodes)

        # Ensure there are enough hosts.
        hosts = self.manager.hosts()
        if sum(len(x) for x in hosts.values()) < nodes:
            return []

        # Select the hosts in different data centers.
        ordered = [x for y in hosts.values() for x in y]
        return ordered[:nodes]

    def _background_run(self, host, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        c = Connection(host, user='root', connect_kwargs=self.connect)
        output = c.run(cmd, hide=True)
        self._check_stderr(output)

    def _update(self, hosts,node_parameters,ts):

        # update parameters file
        Print.info(
            f'Updating {len(hosts)} nodes ...'
        )
        
        # Cleanup all local configuration files.
        cmd = CommandMaker.cleanup_parameters()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # 
        cmd = CommandMaker.make_logs_and_result_dir(ts)
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Cleanup all nodes.
        cmd = [CommandMaker.cleanup_parameters(),CommandMaker.cleanup_db(),CommandMaker.make_logs_dir(ts)]
        g = Group(*hosts, user='root', connect_kwargs=self.connect)
        g.run("&&".join(cmd), hide=True)

        node_parameters.print(PathMaker.parameters_file()) #generate new parameters
        # Update configuration files.
        run_concurrent_tasks(partial(self.upload_to_host, local_path=PathMaker.parameters_file(), remote_path='.'), hosts, "Uploading parameter files")

    def install(self):
        Print.info("Installing dependencies on remote servers...")
        cmd = [
            'sudo apt-get update',
            'sudo DEBIAN_FRONTEND=noninteractive apt-get -y upgrade',
            'sudo apt-get -y autoremove',

            # Install tmux and git only
            'sudo apt-get -y install tmux git wget',

            # Download and install Go from official source
            'wget https://go.dev/dl/go1.21.5.linux-amd64.tar.gz',
            'sudo rm -rf /usr/local/go',
            'sudo tar -C /usr/local -xzf go1.21.5.linux-amd64.tar.gz',
            'rm go1.21.5.linux-amd64.tar.gz',

            # Create symlink to make 'go' available system-wide
            'sudo ln -sf /usr/local/go/bin/go /usr/local/bin/go',
            'sudo ln -sf /usr/local/go/bin/gofmt /usr/local/bin/gofmt',
        ]

        def install_on_host(host):
            c = Connection(host, user='root', connect_kwargs=self.connect)
            result = c.run(' && '.join(cmd), hide=True, warn=True)
            if result.failed:
                raise ExecutionError(f'Installation failed on {host}: {result.stderr}')
            return host

        hosts = self.manager.hosts(flat=True)
        try:
            run_concurrent_tasks(install_on_host, hosts, "Installing dependencies")
            Print.heading(f'Initialized testbed of {len(hosts)} nodes')
        except Exception as e:
            raise BenchError('Failed to install repo on testbed', e)

    def upload_to_host(self, host, local_path, remote_path):
        c = Connection(host, user='root', connect_kwargs=self.connect)
        c.put(local_path, remote_path)
        return host

    def download_from_host(self, host, remote_path, local_path):
        c = Connection(host, user='root', connect_kwargs=self.connect)
        c.get(remote_path, local=local_path)
        return host

    def pull_exec(self):
        hosts = self.manager.hosts(flat=True)
        repo_url = self.settings.repo_url
        branch = self.settings.repo_branch
        name = self.settings.repo_name

        Print.info(f'Pulling code from {repo_url} (branch: {branch}) and compiling on remote servers...')

        def pull_and_compile(host):
            c = Connection(host, user='root', connect_kwargs=self.connect)

            # Check if Loom directory exists
            result = c.run('test -d Wahoo', warn=True, hide=True)

            if result.ok:
                # Directory exists, pull latest changes
                c.run(f'cd Wahoo && git fetch origin && git checkout {branch} && git pull origin {branch} && cd ..', hide=True)
            else:
                # Directory doesn't exist, clone the repository
                c.run(f'git clone -b {branch} {repo_url}', hide=True)

            # Tidy modules and compile the code on remote server
            result = c.run('cd Wahoo && go mod tidy && go build main.go', warn=True)
            if result.failed:
                raise ExecutionError(f'Compilation failed on {host}: {result.stderr}')

            # Copy the compiled binary to home directory for execution
            c.run('cp Wahoo/main .', hide=True)

            return host

        # Pull and compile concurrently on all hosts
        run_concurrent_tasks(pull_and_compile, hosts, "Pulling code and compiling")

    def _config(self, hosts,bench_parameters):
        Print.info('Generating configuration files...')

        # Cleanup all local configuration files.
        cmd = CommandMaker.cleanup_configs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        node_instance = bench_parameters.node_instance
        nodes = len(hosts) * node_instance
        # Generate configuration files.
        keys = []
        key_files = [PathMaker.key_file(i) for i in range(nodes)]
        cmd = CommandMaker.generate_key(path="./",nodes=nodes).split()
        subprocess.run(cmd, check=True)
        for filename in key_files:
            keys += [Key.from_file(filename)]

        # Generate threshold signature files.
        tss_keys = []
        threshold_key_files = [PathMaker.threshold_key_file(i) for i in range(nodes)]
        N , T = nodes , 2 * (( nodes - 1 ) // 3) + 1
        cmd = CommandMaker.generate_tss_key(path = "./", N = N, T = T).split()
        subprocess.run(cmd, check=True)
        for filename in threshold_key_files:
            tss_keys += [TSSKey.from_file(filename)]

        names = [x.pubkey for x in keys]
        ids = [i for i in range(nodes)]
        consensus_addr = []
        for ip in hosts:
            for i in range(node_instance):
                consensus_addr += [f'{ip}:{self.settings.consensus_port+i}']

        committee = Committee(names, ids, consensus_addr)
        committee.print(PathMaker.committee_file())

        # Cleanup all nodes.
        cmd = f'{CommandMaker.cleanup_configs()} || true'
        g = Group(*hosts, user='root', connect_kwargs=self.connect)
        g.run(cmd, hide=True)

        # Upload configuration files.
        def upload_config_to_host(i, host):
            self.upload_to_host(host, PathMaker.committee_file(), '.')
            for j in range(node_instance):
                self.upload_to_host(host, PathMaker.key_file(i*node_instance+j), '.')
                self.upload_to_host(host, PathMaker.threshold_key_file(i*node_instance+j), '.')
            return host
        
        run_concurrent_tasks(upload_config_to_host, list(enumerate(hosts)), "Uploading config files")

        return committee

    def _run_single(self, hosts, bench_parameters, ts, debug=False):
        Print.info('Booting testbed...')

        # Kill any potentially unfinished run and delete logs.
        self.kill(hosts=hosts, delete_logs=True)
        Print.info('Killed previous instances')
        sleep(10)

        node_instance = bench_parameters.node_instance
        nodes = len(hosts) * node_instance

        # Run the nodes.
        key_files = [PathMaker.key_file(i) for i in range(nodes)]
        threshold_key_files = [PathMaker.threshold_key_file(i) for i in range(nodes)]
        dbs = [PathMaker.db_path(i) for i in range(nodes)]
        node_logs = [PathMaker.node_log_error_file(i,ts) for i in range(nodes)]
        for i,host in enumerate(hosts):
            for j in range(node_instance):
                cmd = CommandMaker.run_node(
                    i*node_instance+j,
                    key_files[i*node_instance+j],
                    threshold_key_files[i*node_instance+j],
                    PathMaker.committee_file(),
                    dbs[i*node_instance+j],
                    PathMaker.parameters_file(),
                    ts,
                    bench_parameters.log_level
                )
                self._background_run(host, cmd, node_logs[i*node_instance+j])

        # Wait for the nodes to synchronize
        Print.info('Waiting for the nodes to synchronize...')
        sleep(20)

        # Wait for all transactions to be processed.
        duration = bench_parameters.duration
        for _ in progress_bar(range(100), prefix=f'Running benchmark ({duration} secs):'):
            sleep(duration / 100)
        self.kill(hosts=hosts, delete_logs=False)

    def download(self,node_instance,ts):
        hosts = self.manager.hosts(flat=True)

        # Download log files (debug logs in download method).
        def download_debug_logs_from_host(host_idx, host):
            for j in range(node_instance):
                node_idx = host_idx * node_instance + j
                self.download_from_host(host, 
                    PathMaker.node_log_debug_file(node_idx, ts), 
                    PathMaker.node_log_debug_file(node_idx, ts))
            return host_idx

        run_concurrent_tasks(download_debug_logs_from_host, list(enumerate(hosts)), "Downloading logs")

        # Parse logs and return the parser.
        Print.info('Parsing logs and computing performance...')
        return LogParser.process(PathMaker.logs_path(ts))
    
    def _logs(self, hosts, faults, protocol, ddos,bench_parameters,ts):
        node_instance = bench_parameters.node_instance
        
        # Download log files (info logs in _logs method).
        def download_info_logs_from_host(host_idx, host):
            for j in range(node_instance):
                node_idx = host_idx * node_instance + j
                self.download_from_host(host, 
                    PathMaker.node_log_info_file(node_idx, ts), 
                    PathMaker.node_log_info_file(node_idx, ts))
            return host_idx

        run_concurrent_tasks(download_info_logs_from_host, list(enumerate(hosts)), "Downloading logs")

        # Parse logs and return the parser.
        Print.info('Parsing logs and computing performance...')
        return LogParser.process(PathMaker.logs_path(ts), faults=faults, protocol=protocol, ddos=ddos)

    def run(self, bench_parameters_dict, node_parameters_dict, debug=False):
        assert isinstance(debug, bool)

        Print.heading('Starting remote benchmark')
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

        # Recompile the latest code.
        cmd = CommandMaker.pull_and_compile().split()
        subprocess.run(cmd, check=True)

        #Step 1: Select which hosts to use.
        selected_hosts = self._select_hosts(bench_parameters)
        if not selected_hosts:
            Print.warn('There are not enough instances available')
            return

        Print.info(f'Running {bench_parameters.protocol}')
        Print.info(f'{node_parameters.faults} byzantine nodes')
        Print.info(f'tx_size {node_parameters.tx_size} byte, input rate {bench_parameters.rate} tx/s')
        Print.info(f'DDOS attack {node_parameters.ddos}')
        
        #Step 2: Run benchmarks.
        for n in bench_parameters.nodes:
            
            hosts = selected_hosts[:n]
            #Step 3: Upload all configuration files.
            try:
                self._config(hosts, bench_parameters)
            except (subprocess.SubprocessError, GroupException) as e:
                e = FabricError(e) if isinstance(e, GroupException) else e
                Print.error(BenchError('Failed to configure nodes', e))

            for batch_size in bench_parameters.batch_szie:
                Print.heading(f'\nRunning {n}/{bench_parameters.node_instance} nodes (batch size: {batch_size:,})')
                hosts = selected_hosts[:n]

                node_parameters.json['pool']['rate'] = bench_parameters.rate
                node_parameters.json['pool']['batch_size'] = batch_size
                self.ts = datetime.now().strftime(r"%Y-%m-%d-%H-%M-%S")
                
                #Step a: only upload parameters files.
                try:
                    self._update(hosts,node_parameters,self.ts)
                except (subprocess.SubprocessError, GroupException) as e:
                    e = FabricError(e) if isinstance(e, GroupException) else e
                    Print.error(BenchError('Failed to update nodes', e))
                    continue

                protocol = bench_parameters.protocol
                ddos = node_parameters.ddos

                # Run the benchmark.
                for i in range(bench_parameters.runs):
                    Print.heading(f'Run {i+1}/{bench_parameters.runs}')
                    try:
                        self._run_single(
                            hosts,bench_parameters, self.ts , debug
                        )
                        self._logs(hosts, node_parameters.faults , protocol, ddos,bench_parameters,self.ts).print(
                            PathMaker.result_file(
                                n, bench_parameters.rate, node_parameters.tx_size,batch_size , node_parameters.faults,self.ts
                        ))
                    except (subprocess.SubprocessError, GroupException, ParseError) as e:
                        self.kill(hosts=hosts)
                        if isinstance(e, GroupException):
                            e = FabricError(e)
                        Print.error(BenchError('Benchmark failed', e))
                        continue

    # ========== Wahoo/Tusk/GradedDAG Benchmark Methods ==========

    def install_wahoo_deps(self):
        """
        Install Wahoo-specific dependencies on remote servers (Go 1.16+).
        Based on lines 15-28 of ~/gitrepo/Wahoo/README.md (excluding ansible).
        """
        Print.info("Installing Wahoo dependencies (Go 1.16+) on remote servers...")
        cmd = [
            'sudo apt-get update',
            'mkdir -p /tmp/go_install',
            'cd /tmp/go_install',
            'wget -q https://dl.google.com/go/go1.16.15.linux-amd64.tar.gz',
            'sudo tar -xvf go1.16.15.linux-amd64.tar.gz',
            'sudo rm -rf /usr/local/go',
            'sudo mv go /usr/local',
            'echo "export PATH=\\$PATH:~/.local/bin:/usr/local/go/bin" >> ~/.bashrc',
            'export PATH=$PATH:/usr/local/go/bin',
            '/usr/local/go/bin/go env -w GO111MODULE=on',
            'rm -rf /tmp/go_install'
        ]

        def install_on_host(host):
            c = Connection(host, user='root', connect_kwargs=self.connect)
            result = c.run(' && '.join(cmd), hide=True, warn=True)
            if result.failed:
                raise ExecutionError(f'Wahoo deps installation failed on {host}: {result.stderr}')
            return host

        hosts = self.manager.hosts(flat=True)
        try:
            run_concurrent_tasks(install_on_host, hosts, "Installing Wahoo dependencies")
            Print.heading(f'Initialized Wahoo dependencies on {len(hosts)} nodes')
        except Exception as e:
            raise BenchError('Failed to install Wahoo dependencies', e)

    def pull_exec_wahoo(self, wahoo_repo_url="https://github.com/guozhengxuan/Wahoo.git", wahoo_branch="main"):
        """
        Pull Wahoo code from GitHub and compile on remote servers.
        """
        hosts = self.manager.hosts(flat=True)
        Print.info(f'Pulling Wahoo from {wahoo_repo_url} (branch: {wahoo_branch}) and compiling on remote servers...')

        def pull_and_compile(host):
            c = Connection(host, user='root', connect_kwargs=self.connect)

            # Check if Wahoo directory exists
            result = c.run('test -d Wahoo', warn=True, hide=True)

            if result.ok:
                # Directory exists, pull latest changes
                c.run(f'cd Wahoo && git fetch origin && git checkout {wahoo_branch} && git pull origin {wahoo_branch}', hide=True)
            else:
                # Directory doesn't exist, clone the repository
                c.run(f'git clone -b {wahoo_branch} {wahoo_repo_url}', hide=True)

            # Compile Wahoo on remote server
            Print.info(f'  {host}: Building Wahoo')
            result = c.run('cd Wahoo && /usr/local/go/bin/go build -o BFT main.go', warn=True)
            if result.failed:
                Print.error(BenchError(f'Wahoo compilation failed on {host}', Exception(result.stderr)))
                raise ExecutionError(f'Wahoo compilation failed on {host}: {result.stderr}')

            Print.info(f'  {host}: ✓ Successfully compiled Wahoo')
            return host

        # Pull and compile concurrently on all hosts
        run_concurrent_tasks(pull_and_compile, hosts, "Pulling and compiling Wahoo")

    def _run_single_wahoo(self, hosts, bench_parameters, node_parameters, ts, protocol='wahoo', debug=False):
        """
        Run a single Wahoo/Tusk/GradedDAG benchmark.
        Similar to _run_single but adapted for Wahoo's config structure.
        """
        Print.info(f'Booting {protocol} testbed...')

        # Kill any potentially unfinished run and delete logs
        self.kill_wahoo(hosts=hosts, delete_logs=True)
        Print.info('Killed previous instances')
        sleep(10)

        node_instance = bench_parameters.node_instance
        nodes = len(hosts) * node_instance

        # Create log directories on remote hosts
        cmd = CommandMaker.make_logs_dir(ts)
        g = Group(*hosts, user='root', connect_kwargs=self.connect)
        g.run(cmd, hide=True)

        # Run the nodes
        for i, host in enumerate(hosts):
            for j in range(node_instance):
                node_idx = i * node_instance + j
                node_name = f'node{node_idx}'

                # Wahoo uses config.yaml in its working directory
                # Log file follows Loom's pattern
                log_file = PathMaker.node_log_file(node_idx, ts)

                # Command to run Wahoo node
                cmd = f'cd ~/Wahoo && ./BFT'
                self._background_run(host, cmd, log_file)

        # Wait for the nodes to synchronize
        Print.info('Waiting for the nodes to synchronize...')
        sleep(20)

        # Wait for all transactions to be processed
        duration = bench_parameters.duration
        for _ in progress_bar(range(100), prefix=f'Running {protocol} benchmark ({duration} secs):'):
            sleep(duration / 100)

        self.kill_wahoo(hosts=hosts, delete_logs=False)

    def kill_wahoo(self, hosts=[], delete_logs=False):
        """
        Kill Wahoo processes on remote servers.
        """
        assert isinstance(hosts, list)
        assert isinstance(delete_logs, bool)
        hosts = hosts if hosts else self.manager.hosts(flat=True)

        cmd = ["true", "(killall BFT || true)"]
        if delete_logs:
            cmd += [f'({CommandMaker.clean_logs()} || true)']

        try:
            g = Group(*hosts, user='root', connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)
        except GroupException as e:
            raise BenchError('Failed to kill Wahoo nodes', FabricError(e))

    def _logs_wahoo(self, hosts, faults, protocol, bench_parameters, ts):
        """
        Download and parse Wahoo logs, similar to _logs for Loom.
        """
        node_instance = bench_parameters.node_instance

        # Download log files
        def download_logs_from_host(host_idx, host):
            for j in range(node_instance):
                node_idx = host_idx * node_instance + j
                self.download_from_host(host,
                    PathMaker.node_log_file(node_idx, ts),
                    PathMaker.node_log_file(node_idx, ts))
            return host_idx

        run_concurrent_tasks(download_logs_from_host, list(enumerate(hosts)), "Downloading Wahoo logs")

        # For now, just return a simple result structure
        # You may need to implement a Wahoo-specific log parser later
        Print.info('Wahoo logs downloaded')
        return {'protocol': protocol, 'timestamp': ts, 'nodes': len(hosts)}

    def _config_wahoo(self, hosts, protocol, bench_parameters, node_parameters):
        """
        Generate Wahoo configuration files and upload to remote servers.
        Similar to _config but uses Wahoo's config_gen to generate configs.
        """
        Print.info('Generating Wahoo configuration files...')

        # Cleanup all local configuration files.
        cmd = 'rm -f config_gen/*.yaml'
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        num_nodes = len(hosts)

        # Generate config_template.yaml for config_gen
        cluster_ips = {}
        peers_p2p_port = {}
        for i, host in enumerate(hosts):
            node_name = f'node{i}'
            cluster_ips[node_name] = host
            peers_p2p_port[node_name] = self.settings.consensus_port

        config_data = {
            'IPs': cluster_ips,
            'peers_p2p_port': peers_p2p_port,
            'max_pool': node_parameters.json['pool']['max_pool'],
            'log_level': 3,
            'batch_size': bench_parameters.batch_szie[0],
            'round': bench_parameters.round,
            'faulty_number': node_parameters.faults,
            'protocol': protocol
        }

        # Write config template
        template_path = 'config_gen/config_template.yaml'
        with open(template_path, 'w') as f:
            yaml.dump(config_data, f, default_flow_style=False, sort_keys=False)

        # Run config_gen to generate individual node configs
        Print.info(f'Running config_gen to generate {num_nodes} node configurations...')
        result = subprocess.run(
            'cd config_gen && go run main.go',
            shell=True,
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            raise BenchError(f'Wahoo config generation failed: {result.stderr}')

        # Cleanup all nodes.
        cmd = 'rm -rf ~/Wahoo/config.yaml || true'
        g = Group(*hosts, user='root', connect_kwargs=self.connect)
        g.run(cmd, hide=True)

        # Upload configuration files.
        def upload_config_to_host(i, host):
            c = Connection(host, user='root', connect_kwargs=self.connect)
            # Create working directory for Wahoo
            c.run('mkdir -p ~/Wahoo', hide=True)
            # Upload config file (config_gen generates node{i}_0.yaml files)
            config_file = f'config_gen/node{i}_0.yaml'
            c.put(config_file, '~/Wahoo/config.yaml')
            return host

        run_concurrent_tasks(upload_config_to_host, list(enumerate(hosts)), "Uploading Wahoo config files")

    def run_wahoo(self, bench_parameters_dict, node_parameters_dict, protocol='wahoo', debug=False):
        """
        Run Wahoo/Tusk/GradedDAG benchmarks on Aliyun instances.

        Args:
            bench_parameters_dict: Benchmark parameters
            node_parameters_dict: Node parameters
            protocol: 'wahoo', 'tusk', or 'gradeddag'
            debug: Enable debug mode
        """
        assert isinstance(debug, bool)
        assert protocol in ['wahoo', 'tusk', 'gradeddag']

        Print.heading(f'Starting {protocol.upper()} remote benchmark')
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

        # Select hosts
        selected_hosts = self._select_hosts(bench_parameters)
        if not selected_hosts:
            Print.warn('There are not enough instances available')
            return

        # Display settings
        Print.info(f'Running {protocol.upper()}')
        Print.info(f'{node_parameters.faults} byzantine nodes')
        Print.info(f'Batch size: {bench_parameters.batch_szie[0]}')
        Print.info(f'Duration: {bench_parameters.duration}s')

        # Run benchmarks for each configuration
        for n in bench_parameters.nodes:
            hosts = selected_hosts[:n]

            # Generate and upload Wahoo configs for each node
            try:
                self._config_wahoo(hosts, protocol, bench_parameters, node_parameters)
            except (subprocess.SubprocessError, GroupException) as e:
                e = FabricError(e) if isinstance(e, GroupException) else e
                Print.error(BenchError('Failed to configure Wahoo nodes', e))
                continue

            for batch_size in bench_parameters.batch_szie:
                Print.heading(f'\nRunning {n} nodes (batch size: {batch_size:,})')

                self.ts = datetime.now().strftime(r"%Y-%m-%d-%H-%M-%S")

                # Run benchmark
                for i in range(bench_parameters.runs):
                    Print.heading(f'Run {i+1}/{bench_parameters.runs}')
                    try:
                        self._run_single_wahoo(hosts, bench_parameters, node_parameters, self.ts, protocol, debug)

                        result = self._logs_wahoo(hosts, node_parameters.faults, protocol, bench_parameters, self.ts)

                        # Save results
                        result_file = f'./wahoo_results/{protocol}_{n}_{batch_size}_{self.ts}.txt'
                        import os
                        os.makedirs('./wahoo_results', exist_ok=True)
                        with open(result_file, 'w') as f:
                            f.write(str(result))

                        Print.info(f'Results saved to {result_file}')

                    except (subprocess.SubprocessError, GroupException, ParseError) as e:
                        self.kill_wahoo(hosts=hosts)
                        if isinstance(e, GroupException):
                            e = FabricError(e)
                        Print.error(BenchError(f'{protocol} benchmark failed', e))
                        continue
