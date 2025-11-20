import subprocess
from fabric import task

from benchmark.commands import CommandMaker
from benchmark.local import LocalBench
from benchmark.logs import ParseError, LogParser
from benchmark.utils import BenchError,Print
from alibaba.instance import InstanceManager
from alibaba.remote import Bench

@task
def local(ctx):
    ''' Run benchmarks on localhost '''
    bench_params = {
        'nodes': 7,
        'duration': 20,
        'rate': 3_000,                  # tx send rate
        'batch_size': 800,              # the max number of tx that can be hold 
        'log_level': 0b1111,            # 0x1 infolevel 0x2 debuglevel 0x4 warnlevel 0x8 errorlevel
        'protocol_name': "Wahoo++"
    }
    node_params = {
        "pool": {
            # "rate": 1_000,              # ignore: tx send rate 
            "tx_size": 250,               # tx size
            # "batch_size": 200,          # ignore: the max number of tx that can be hold 
            "max_queue_size": 10_000 
	    },
        "consensus": {
            "sync_timeout": 500,        # node sync time
            "network_delay": 50,        # network delay
            "min_block_delay": 0,       # send block delay
            "ddos": False,              # DDOS attack
            "faults": 0,                # the number of byzantine node
            "retry_delay": 5_000        # request block period
        }
    }
    try:
        ret = LocalBench(bench_params, node_params).run(debug=True).result()
        print(ret)
    except BenchError as e:
        Print.error(e)

@task
def create(ctx, nodes=4):
    ''' Create a testbed'''
    try:
        InstanceManager.make().create_instances(nodes)
    except BenchError as e:
        Print.error(e)

@task
def destroy(ctx):
    ''' Destroy the testbed '''
    try:
        InstanceManager.make().terminate_instances()
    except BenchError as e:
        Print.error(e)

@task
def cleansecurity(ctx):
    ''' Destroy the testbed '''
    try:
        InstanceManager.make().delete_security()
    except BenchError as e:
        Print.error(e)

@task
def start(ctx, max=10):
    ''' Start at most `max` machines per data center '''
    try:
        InstanceManager.make().start_instances(max)
    except BenchError as e:
        Print.error(e)

@task
def stop(ctx):
    ''' Stop all machines '''
    try:
        InstanceManager.make().stop_instances()
    except BenchError as e:
        Print.error(e)

@task
def install(ctx):
    try:
        Bench(ctx).install()
    except BenchError as e:
        Print.error(e)

@task
def uploadexec(ctx):
    try:
        Bench(ctx).pull_exec()
    except BenchError as e:
        Print.error(e)

@task
def info(ctx):
    ''' Display connect information about all the available machines '''
    try:
        InstanceManager.make().print_info()
    except BenchError as e:
        Print.error(e)

@task
def remote(ctx):
    ''' Run benchmarks on AWS '''
    bench_params = {
        'nodes': [4],
        'node_instance': 1,                                             # the number of running instance for a node  (max = 4)
        'round': 30,
        'rate': 8_000,                                                  # tx send rate
        'batch_size': [1000],                              # the max number of tx that can be hold 
        'log_level': 0b1111,                                            # 0x1 infolevel 0x2 debuglevel 0x4 warnlevel 0x8 errorlevel
        'protocol_name': "Wahoo++",
        'runs': 1
    }
    node_params = {
        "pool": {
            # "rate": 1_000,              # ignore: tx send rate 
            "tx_size": 250,               # tx size
            # "batch_size": 200,          # ignore: the max number of tx that can be hold 
            "max_queue_size": 100_000 
	    },
        "consensus": {
            "sync_timeout": 1_000,      # node sync time
            "network_delay": 1_000,     # network delay
            "min_block_delay": 0,       # send block delay
            "ddos": False,              # DDOS attack
            "faults": 0,                # the number of byzantine node
            "retry_delay": 5_000        # request block period
        }
    }
    try:
        Bench(ctx).run(bench_params, node_params, debug=False)
    except BenchError as e:
        Print.error(e)

@task
def kill(ctx):
    ''' Stop any HotStuff execution on all machines '''
    try:
        Bench(ctx).kill()
    except BenchError as e:
        Print.error(e)

@task
def download(ctx,node_instance=2,ts="2024-06-19-09-02-46"):
    ''' download logs '''
    try:
        print(Bench(ctx).download(node_instance,ts).result())
    except BenchError as e:
        Print.error(e)

@task
def clean(ctx):
    cmd = f'{CommandMaker.cleanup_configs()};rm -f main'
    subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

@task
def logs(ctx):
    ''' Print a summary of the logs '''
    # try:
    print(LogParser.process('./logs/2024-06-03v11:18:47').result())
    # except ParseError as e:
    #     Print.error(BenchError('Failed to parse logs', e))

# ========== Wahoo/Tusk/GradedDAG Benchmark Tasks ==========

# @task
# def install_w(ctx):
#     ''' Install Wahoo dependencies (Go 1.16+) on remote servers '''
#     try:
#         Bench(ctx).install_wahoo_deps()
#     except BenchError as e:
#         Print.error(e)

# @task
# def upload_exec_w(ctx):
#     ''' Pull Wahoo code and compile on remote servers '''
#     try:
#         Bench(ctx).pull_exec_wahoo()
#     except BenchError as e:
#         Print.error(e)

@task
def remote_w(ctx, protocol='wahoo'):
    ''' Run Wahoo/Tusk/GradedDAG benchmarks on Aliyun

    Args:
        protocol: Protocol to test - 'wahoo', 'tusk', or 'gradeddag'

    Usage:
        fab remote_w                  # Run Wahoo (default)
        fab remote_w:protocol=tusk    # Run Tusk
        fab remote_w:protocol=gradeddag  # Run GradedDAG
    '''
    bench_params = {
        'nodes': [4],
        'node_instance': 1,
        'round': 30,
        'rate': 5_000,
        'batch_size': [1000],
        'log_level': 0b1111,
        'protocol_name': protocol,
        'runs': 1
    }
    node_params = {
        "pool": {
            "tx_size": 250,
            "max_pool": 31,
            "max_queue_size": 100_000
        },
        "consensus": {
            "sync_timeout": 1_000,
            "network_delay": 1_000,
            "min_block_delay": 0,
            "ddos": False,
            "faults": 0,
            "retry_delay": 5_000
        }
    }
    try:
        Bench(ctx).run_wahoo(bench_params, node_params, protocol=protocol, debug=False)
    except BenchError as e:
        Print.error(e)
