from datetime import datetime
from os import error
import os
from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import Ed25519Key
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os.path import basename, splitext, join
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
                # Propagate any exceptions that occurred during task execution
                future.result()


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

        # Retry logic for SSH connection issues
        max_retries = 3
        for attempt in range(max_retries):
            try:
                c = Connection(host, user='root', connect_kwargs=self.connect)
                output = c.run(cmd, hide=True)
                self._check_stderr(output)
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    sleep(2)  # Wait before retry
                    continue
                else:
                    raise  # Re-raise if all retries failed

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
        Print.info("Installing dependencies and pulling code on remote servers...")

        repo_url = self.settings.repo_url
        branch = self.settings.repo_branch

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

        def install_and_pull(host):
            c = Connection(host, user='root', connect_kwargs=self.connect)
            remote_root = PathMaker.remote_root_path()
            remote_wahoo = PathMaker.remote_wahoo_path()

            # Install dependencies
            result = c.run(' && '.join(cmd), hide=True, warn=True)
            if result.failed:
                raise ExecutionError(f'Installation failed on {host}: {result.stderr}')

            # Pull or clone the repository
            result = c.run(f'test -d {remote_wahoo}', warn=True, hide=True)
            if result.ok:
                # Directory exists, pull latest changes
                c.run(f'cd {remote_wahoo} && git fetch origin && git checkout {branch} && git pull origin {branch}', hide=True)
            else:
                # Directory doesn't exist, clone the repository
                c.run(f'cd {remote_root} && git clone -b {branch} {repo_url}', hide=True)

            # Verify Wahoo directory exists after clone/pull
            result = c.run(f'test -d {remote_wahoo}', warn=True, hide=True)
            if result.failed:
                raise ExecutionError(f'Wahoo directory not found on {host} after install')

            return host

        hosts = self.manager.hosts(flat=True)
        try:
            run_concurrent_tasks(install_and_pull, hosts, "Installing dependencies and pulling code")

            # Final verification: check all hosts have Wahoo and Go installed
            Print.info('Verifying installation on all hosts...')
            def verify_install(host):
                c = Connection(host, user='root', connect_kwargs=self.connect)
                remote_wahoo = PathMaker.remote_wahoo_path()

                # Check Wahoo directory
                result = c.run(f'test -d {remote_wahoo}', warn=True, hide=True)
                if result.failed:
                    return (host, 'Wahoo directory missing')

                # Check Go installation
                result = c.run('which go', warn=True, hide=True)
                if result.failed:
                    return (host, 'Go not found in PATH')

                # Check Wahoo/main.go exists
                result = c.run(f'test -f {remote_wahoo}/main.go', warn=True, hide=True)
                if result.failed:
                    return (host, 'main.go missing')

                return (host, 'OK')

            verification_results = []
            for host in hosts:
                verification_results.append(verify_install(host))

            # Report results
            failed_hosts = [(h, r) for h, r in verification_results if r != 'OK']
            if failed_hosts:
                error_msg = '\n'.join([f'  {host}: {reason}' for host, reason in failed_hosts])
                raise BenchError(f'Installation verification failed:\n{error_msg}', Exception('Verification failed'))

            Print.heading(f'✓ Successfully initialized testbed of {len(hosts)} nodes')
        except BenchError:
            raise
        except Exception as e:
            raise BenchError('Failed to install and pull code on testbed', e)

    def upload_to_host(self, host, local_path, remote_path):
        c = Connection(host, user='root', connect_kwargs=self.connect)
        c.put(local_path, remote_path)
        return host

    def download_from_host(self, host, remote_path, local_path):
        c = Connection(host, user='root', connect_kwargs=self.connect)
        c.get(remote_path, local=local_path)
        return host

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

    def _check_wahoo_completion(self, host, log_file):
        """
        Check if a Wahoo node has completed by monitoring its log file for completion signal.

        The main.go RunLoop() function ends after completing preset rounds and logs:
        - "the average" (latency and throughput)
        - "the total commit" (block number and time)

        Returns True if the node has completed, False otherwise.
        """
        completion_pattern = "the total commit"

        try:
            c = Connection(host, user='root', connect_kwargs=self.connect)
            # Check if completion log message exists in the log file
            result = c.run(f'grep -q "{completion_pattern}" {log_file}', warn=True, hide=True)
            return result.ok
        except Exception:
            return False

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

        # Run the nodes using concurrent execution
        def start_node(node_idx, host):
            cmd = f'cd {PathMaker.remote_wahoo_path()} && ./BFT'

            try:
                self._background_run(host, cmd, PathMaker.remote_log_file(node_idx, ts))
                Print.info(f'  Node {node_idx} started on {host}')
                return host
            except Exception as e:
                raise BenchError(f'Failed to start node {node_idx} on {host}', e)

        # Build list of (node_idx, host) tuples for all nodes
        node_tasks = []
        for i, host in enumerate(hosts):
            for j in range(node_instance):
                node_idx = i * node_instance + j
                node_tasks.append((node_idx, host))

        run_concurrent_tasks(start_node, node_tasks, "Starting Wahoo nodes")

        # Wait for the nodes to synchronize
        Print.info('Waiting for the nodes to synchronize...')
        sleep(10)

        # Monitor for completion based on preset rounds in config
        # Wahoo's RunLoop() ends after completing n.roundNumber rounds
        # Check the first non-faulty node's log file
        monitor_host = hosts[0]
        monitor_log = PathMaker.remote_log_file(0, ts)

        Print.info(f'Monitoring {protocol} benchmark completion (checking every 5 seconds)...')
        max_wait_time = 90  # 90s max timeout
        check_interval = 5   # Check every 5 seconds
        elapsed_time = 0

        while elapsed_time < max_wait_time:
            if self._check_wahoo_completion(monitor_host, monitor_log):
                Print.info(f'{protocol} benchmark completed after {elapsed_time} seconds')
                # Give a bit more time for all nodes to finish their final outputs
                sleep(5)
                break
            sleep(check_interval)
            elapsed_time += check_interval
        else:
            Print.warn(f'{protocol} benchmark did not complete within {max_wait_time} seconds')

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
                remote_log_path = PathMaker.remote_log_file(node_idx, ts)
                local_log_path = PathMaker.local_node_log_file(node_idx, ts)

                # Ensure local log directory exists
                import os
                os.makedirs(os.path.dirname(local_log_path), exist_ok=True)

                try:
                    self.download_from_host(host, remote_log_path, local_log_path)
                    Print.info(f'  Downloaded {remote_log_path} from {host}')
                except Exception as e:
                    Print.warn(f'  Failed to download {remote_log_path} from {host}: {e}')
                    raise
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
        cmd = f'rm -f {PathMaker.config_gen_dir()}/*.yaml'
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        num_nodes = len(hosts)

        # Generate config_template.yaml for config_gen
        cluster_ips = {}
        for i, host in enumerate(hosts):
            node_name = f'node{i}'
            cluster_ips[node_name] = host

        config_data = {
            'IPs': cluster_ips,
            'p2p_port': self.settings.consensus_port,
            'max_pool': node_parameters.json['pool']['max_pool'],
            'log_level': 3,
            'batch_size': bench_parameters.batch_szie[0],
            'round': bench_parameters.round,
            'faulty_number': node_parameters.faults,
            'protocol': protocol
        }

        # Write config template
        template_path = PathMaker.config_gen_template_file()
        with open(template_path, 'w') as f:
            yaml.dump(config_data, f, default_flow_style=False, sort_keys=False)

        # Run config_gen to generate individual node configs
        Print.info(f'Running config_gen to generate {num_nodes} node configurations...')
        result = subprocess.run(
            f'cd {PathMaker.config_gen_dir()} && go run main.go',
            shell=True,
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            raise BenchError(f'Wahoo config generation failed: {result.stderr}')

        # Cleanup all nodes.
        remote_wahoo = PathMaker.remote_wahoo_path()
        cmd = f'rm -rf {remote_wahoo}/config.yaml || true'
        g = Group(*hosts, user='root', connect_kwargs=self.connect)
        g.run(cmd, hide=True)

        # Upload configuration files and compile BFT executable
        def upload_config_and_compile(i, host):
            c = Connection(host, user='root', connect_kwargs=self.connect)

            # Get remote Wahoo directory and create it
            remote_wahoo = PathMaker.remote_wahoo_path()

            result = c.run(f'mkdir -p {remote_wahoo}', warn=True, hide=True)
            if result.failed:
                raise ExecutionError(f'Failed to create {remote_wahoo} directory on {host}: {result.stderr}')

            # Upload config file (config_gen generates node{i}_0.yaml files)
            config_file = PathMaker.config_gen_node_file(i)

            # Verify local file exists before upload
            import os
            if not os.path.exists(config_file):
                raise FileNotFoundError(f'Local config file not found: {config_file}')

            try:
                remote_config_path = f'{remote_wahoo}/config.yaml'
                c.put(config_file, remote_config_path)
            except Exception as e:
                raise BenchError(f'Failed to upload {config_file} to {host}:{remote_config_path}', e)

            # Compile BFT executable from main.go
            Print.info(f'  {host}: Building BFT executable...')
            result = c.run(f'cd {remote_wahoo} && go build -o BFT main.go', warn=True)
            if result.failed:
                raise ExecutionError(f'BFT compilation failed on {host}: {result.stderr}')

            return host

        run_concurrent_tasks(upload_config_and_compile, list(enumerate(hosts)), "Uploading configs and compiling BFT")

        # Final verification: check all hosts have config and BFT executable
        Print.info('Verifying configuration on all hosts...')
        def verify_config(host):
            c = Connection(host, user='root', connect_kwargs=self.connect)

            # Get remote Wahoo directory
            remote_wahoo = PathMaker.remote_wahoo_path()

            # Check config.yaml
            result = c.run(f'test -f {remote_wahoo}/config.yaml', warn=True, hide=True)
            if result.failed:
                raise ExecutionError(f'config.yaml missing on {host}')

            # Check BFT executable
            result = c.run(f'test -f {remote_wahoo}/BFT', warn=True, hide=True)
            if result.failed:
                raise ExecutionError(f'BFT executable missing on {host}')

            # Check BFT is executable
            result = c.run(f'test -x {remote_wahoo}/BFT', warn=True, hide=True)
            if result.failed:
                raise ExecutionError(f'BFT not executable on {host}')

            return host

        run_concurrent_tasks(verify_config, hosts, "Verifying configuration")

        Print.info(f'✓ All {len(hosts)} nodes configured successfully')

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
        Print.info(f'Round: {bench_parameters.round} rounds')

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
                        result_file = join(PathMaker.wahoo_results_dir(), f'{protocol}_{n}_{batch_size}_{self.ts}.txt')
                        import os
                        os.makedirs(PathMaker.wahoo_results_dir(), exist_ok=True)
                        with open(result_file, 'w') as f:
                            f.write(str(result))

                        Print.info(f'Results saved to {result_file}')

                    except (subprocess.SubprocessError, GroupException, ParseError) as e:
                        self.kill_wahoo(hosts=hosts)
                        if isinstance(e, GroupException):
                            e = FabricError(e)
                        Print.error(BenchError(f'{protocol} benchmark failed', e))
                        continue
