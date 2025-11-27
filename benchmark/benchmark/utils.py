from os.path import join, dirname, abspath
import os


class BenchError(Exception):
    def __init__(self, message, error):
        assert isinstance(error, Exception)
        self.message = message
        self.cause = error
        super().__init__(message)


class PathMaker:
    @staticmethod
    def execute_file():
        return "main"
    
    @staticmethod
    def committee_file():
        return '.committee.json'

    @staticmethod
    def parameters_file():
        return '.parameters.json'

    @staticmethod
    def key_file(i):
        assert isinstance(i, int) and i >= 0
        return f'.node-key-{i}.json'

    @staticmethod
    def threshold_key_file(i):
        assert isinstance(i, int) and i >= 0
        return f'.node-ts-key-{i}.json'
        
    @staticmethod
    def db_path(i):
        assert isinstance(i, int) and i >= 0
        return f'db-{i}'

    @staticmethod
    def logs_path(ts):
        assert isinstance(ts, str)
        return f'logs/{ts}'
    
    @staticmethod
    def local_node_log_file(i, ts):
        """Generic log file for Wahoo nodes"""
        assert isinstance(i, int) and i >= 0
        return join(PathMaker.logs_path(ts), f'node-{i}.log')

    @staticmethod
    def node_log_info_file(i,ts):
        assert isinstance(i, int) and i >= 0
        return join(PathMaker.logs_path(ts), f'node-info-{i}.log')
    
    @staticmethod
    def node_log_debug_file(i,ts):
        assert isinstance(i, int) and i >= 0
        return join(PathMaker.logs_path(ts), f'node-debug-{i}.log')
    
    @staticmethod
    def node_log_warn_file(i,ts):
        assert isinstance(i, int) and i >= 0
        return join(PathMaker.logs_path(ts), f'node-warn-{i}.log')
    
    @staticmethod
    def node_log_error_file(i,ts):
        assert isinstance(i, int) and i >= 0
        return join(PathMaker.logs_path(ts), f'node-error-{i}.log')

    @staticmethod
    def results_path(ts):
        assert isinstance(ts, str)
        return f'results/{ts}'

    @staticmethod
    def result_file(nodes, rate, tx_size, batch_size ,faults,ts):
        return join(
            PathMaker.results_path(ts), f'bench-{nodes}-{rate}-{tx_size}-{batch_size}-{faults}.txt'
        )

    @staticmethod
    def project_root():
        """Returns absolute path to the Wahoo project root directory"""
        # benchmark/benchmark/utils.py -> benchmark/benchmark -> benchmark -> Wahoo (project root)
        return abspath(join(dirname(__file__), '..', '..'))

    @staticmethod
    def benchmark_dir():
        """Returns absolute path to the benchmark directory"""
        return join(PathMaker.project_root(), 'benchmark')

    @staticmethod
    def config_gen_dir():
        """Returns absolute path to the config_gen directory"""
        return join(PathMaker.project_root(), 'config_gen')

    @staticmethod
    def config_gen_node_file(i):
        """Returns absolute path to a config_gen node file"""
        assert isinstance(i, int) and i >= 0
        return join(PathMaker.config_gen_dir(), f'node{i}_0.yaml')

    @staticmethod
    def config_gen_template_file():
        """Returns absolute path to the config_gen template file"""
        return join(PathMaker.config_gen_dir(), 'config_template.yaml')

    @staticmethod
    def wahoo_results_dir():
        """Returns absolute path to the wahoo_results directory"""
        return join(PathMaker.benchmark_dir(), 'wahoo_results')

    @staticmethod
    def remote_root_path():
        """
        Get remote root directory path.

        Args:
            connection: fabric.Connection object

        Returns:
            str: remote root path ('/')
        """
        return '/'

    @staticmethod
    def remote_wahoo_path():
        """
        Get remote Wahoo directory path.

        Args:
            connection: fabric.Connection object

        Returns:
            str: remote Wahoo directory path ('/Wahoo')
        """
        return '/Wahoo'

    @staticmethod
    def remote_wahoo_node_path(i):
        """
        Get remote Wahoo node instance directory path.

        Args:
            i: node instance index (0-based)

        Returns:
            str: remote Wahoo node instance directory path ('/Wahoo/node_{i}')
        """
        assert isinstance(i, int) and i >= 0
        return f'/Wahoo/node_{i}'

    @staticmethod
    def remote_log_path(ts):
        """
        Get remote Wahoo logs directory path.

        Args:
            connection: fabric.Connection object

        Returns:
            str: remote log directory path ('/logs/{ts}')
        """
        assert isinstance(ts, str)
        return f'/logs/{ts}'
    
    @staticmethod
    def remote_log_file(i, ts):
        """Generic log file for Wahoo nodes"""
        assert isinstance(i, int) and i >= 0
        return join(PathMaker.remote_log_path(ts), f'node-{i}.log')

    @staticmethod
    def wahoo_config_path():
        """Path for Wahoo configuration files"""
        return './wahoo_configs'


class Color:
    HEADER = '\033[95m'
    OK_BLUE = '\033[94m'
    OK_GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class Print:
    @staticmethod
    def heading(message):
        assert isinstance(message, str)
        print(f'{Color.OK_GREEN}{message}{Color.END}')

    @staticmethod
    def info(message):
        assert isinstance(message, str)
        print(message)

    @staticmethod
    def warn(message):
        assert isinstance(message, str)
        print(f'{Color.BOLD}{Color.WARNING}WARN{Color.END}: {message}')

    @staticmethod
    def error(e):
        assert isinstance(e, BenchError)
        print(f'\n{Color.BOLD}{Color.FAIL}ERROR{Color.END}: {e}\n')
        causes, current_cause = [], e.cause
        while isinstance(current_cause, BenchError):
            causes += [f'  {len(causes)}: {e.cause}\n']
            current_cause = current_cause.cause
        causes += [f'  {len(causes)}: {type(current_cause)}\n']
        causes += [f'  {len(causes)}: {current_cause}\n']
        print(f'Caused by: \n{"".join(causes)}\n')


def progress_bar(iterable, prefix='', suffix='', decimals=1, length=30, fill='█', print_end='\r'):
    total = len(iterable)

    def printProgressBar(iteration):
        formatter = '{0:.'+str(decimals)+'f}'
        percent = formatter.format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=print_end)

    printProgressBar(0)
    for i, item in enumerate(iterable):
        yield item
        printProgressBar(i + 1)
    print()
