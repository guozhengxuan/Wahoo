import subprocess
from fabric import task

from benchmark.commands import CommandMaker
from benchmark.local import LocalBench
from benchmark.logs import ParseError, LogParser
from benchmark.utils import BenchError,Print
from alibaba.instance import InstanceManager
from alibaba.remote import Bench

@task
def create(ctx, nodes=2):
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
def config(ctx):
    ''' Generate Wahoo configs, upload to instances, and compile BFT executable '''
    try:
        Bench(ctx).config_wahoo()
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

@task
def remote(ctx, protocol='tusk'):
    ''' Run Wahoo/Tusk/GradedDAG benchmarks on Aliyun

    Args:
        protocol: Protocol to test - 'wahoo', 'tusk', or 'gradeddag'

    Usage:
        fab remote-w
    '''
    bench_params = {
        'nodes': [7],
        'node_instance': 1,
        'round': 15,
        'rate': 8_000,
        'batch_size': [200, 500, 750, 1500, 3000, 5000],
        'log_level': 0b1111,
        'protocol_name': protocol,
        'runs': 1
    }
    node_params = {
        "pool": {
            "tx_size": 250,
            "max_pool": 64,
            "max_queue_size": 100_000
        },
        "consensus": {
            "sync_timeout": 1_000,
            "network_delay": 1_000,
            "min_block_delay": 0,
            "ddos": False,
            "faults": 2,
            "retry_delay": 5_000
        }
    }
    try:
        Bench(ctx).run_wahoo(bench_params, node_params, protocol=protocol, debug=False)
    except BenchError as e:
        Print.error(e)
