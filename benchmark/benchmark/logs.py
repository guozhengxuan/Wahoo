from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean

from benchmark.utils import Print


class ParseError(Exception):
    pass


class LogParser:
    def __init__(self,nodes, faults, protocol, ddos):

        assert all(isinstance(x, str) for x in nodes)

        self.protocol = protocol
        self.ddos = ddos
        self.faults = faults
        self.committee_size = len(nodes)

        # Parse the nodes logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_nodes, nodes)
        except (ValueError, IndexError) as e:
            raise ParseError(f'Failed to parse node logs: {e}')
        batchs,proposals, commits,configs, wait_times, round_advances, refs_collected = zip(*results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        self.commits = self._merge_results([x.items() for x in commits])
        self.batchs = self._merge_results([x.items() for x in batchs])
        self.configs = configs[0]
        
        # Flatten lists for new metrics
        self.wait_times = [item for sublist in wait_times for item in sublist]
        self.round_advances = [item for sublist in round_advances for item in sublist]
        self.refs_collected = [item for sublist in refs_collected for item in sublist]

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _parse_nodes(self, log):
        if search(r'panic', log) is not None:
            raise ParseError('Client(s) panicked')
        
        tmp = findall(r'\[INFO] (.*) pool.* Received Batch (\d+)', log)
        batchs = { id:self._to_posix(t) for t,id in tmp}
        
        tmp = findall(r'\[INFO] (.*) core.* create Block height \d+ node \d+ batch_id (\d+)', log)
        proposals = { id:self._to_posix(t) for t,id in tmp }


        tmp = findall(r'\[INFO] (.*) commitor.* commit Block height \d+ node \d+ batch_id (\d+)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        commits = self._merge_results([tmp])

        configs = {
            'consensus': {
                'faults': int(
                    search(r'Consensus DDos: .*, Faults: (\d+)', log).group(1)
                ),
            },
            'pool': {
                'tx_size': int(
                    search(r'Transaction pool tx size set to (\d+)', log).group(1)
                ),
                'batch_size': int(
                    search(r'Transaction pool batch size set to (\d+)', log).group(1)
                ),
                'rate':int(
                    search(r'Transaction pool tx rate set to (\d+)', log).group(1)
                ),
            }
        }
        
        # [EVAL] Parsing
        # Graph 3: Latency (Wait duration)
        # Log: [EVAL] WAIT_FOR_REFS_END node=... round=... timestamp_ns=... wait_duration_ns=...
        # OR simple format: [INFO] ... [EVAL] WAIT_FOR_REFS_END node 0 round 1 ... wait_duration_ns 500
        # Wahoo/Loom use hclog which might format as key=value or plain text depending on setup.
        # Based on wahoo/node.go: n.logger.Info("[EVAL] WAIT_FOR_REFS_END", "node", n.name, ...)
        # This usually outputs: "[INFO]  [EVAL] WAIT_FOR_REFS_END: node=node0 round=1 ..."
        
        # We will use regex that captures key-value pairs flexibly
        
        wait_times = []
        # Matches: ... [EVAL] WAIT_FOR_REFS_END ... wait_duration_ns=12345 ...
        # OR: ... [EVAL] WAIT_FOR_REFS_END ... wait_duration_ns 12345 ...
        # Let's try to be robust. 
        # From code: n.logger.Info("[EVAL] WAIT_FOR_REFS_END", "node", n.name, "round", currentRound, "timestamp_ns", waitEnd, "wait_duration_ns", waitDuration)
        
        tmp_waits = findall(r'\[EVAL\] WAIT_FOR_REFS_END.*round[=\s](\d+).*wait_duration_ns[=\s](\d+)', log)
        for r, d in tmp_waits:
            wait_times.append({'round': int(r), 'duration_ns': int(d)})

        round_advances = []
        # From code: [EVAL] ROUND_ADVANCED node %d old_round %d new_round %d
        # or wahoo: [EVAL] ROUND_ADVANCED ... old_round=... new_round=... blocks_collected=...
        # Let's look for "ROUND_ADVANCED" and extract available data.
        
        # Try finding Loom style (plain text)
        tmp_rounds_loom = findall(r'\[EVAL\] ROUND_ADVANCED node (\d+) old_round (\d+) new_round (\d+)', log)
        for n, old_r, new_r in tmp_rounds_loom:
            round_advances.append({'node': int(n), 'old_round': int(old_r), 'new_round': int(new_r), 'collected': -1}) # -1 if not available

        # Try finding Wahoo style (key-value)
        tmp_rounds_wahoo = findall(r'\[EVAL\] ROUND_ADVANCED.*node[=\s](\S+).*old_round[=\s](\d+).*new_round[=\s](\d+).*blocks_collected[=\s](\d+)', log)
        for n, old_r, new_r, col in tmp_rounds_wahoo:
             round_advances.append({'node': n, 'old_round': int(old_r), 'new_round': int(new_r), 'collected': int(col)})

        refs_collected = []
        # Loom: [EVAL] REFS_COLLECTED node %d height %d round %d ref_count %d
        tmp_refs = findall(r'\[EVAL\] REFS_COLLECTED node (\d+) height (\d+) round (\d+) ref_count (\d+)', log)
        for n, h, r, c in tmp_refs:
            refs_collected.append({'node': int(n), 'height': int(h), 'round': int(r), 'count': int(c)})
            
        # Loom: [EVAL] BLOCK_PROPOSED node %d height %d round %d ref_count %d
        # (This can also be used for cumulative input if needed, similar to REFS_COLLECTED)
        # We can store it in the same list or a new one if needed, but for now refs_collected covers the "refs" graph.

        return batchs,proposals, commits,configs, wait_times, round_advances, refs_collected

    def _to_posix(self, string):
        # 解析时间字符串为 datetime 对象
        dt = datetime.strptime(string, "%Y/%m/%d %H:%M:%S.%f")
        # 转换为 Unix 时间戳
        timestamp = dt.timestamp()
        return timestamp

    def _consensus_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.commits.values())
        duration = end - start
        tps = len(self.commits)*self.configs['pool']['batch_size'] / duration
        return tps, duration

    def _consensus_latency(self):
        latency = [c - self.proposals[d] for d, c in self.commits.items() if d in self.proposals]
        return mean(latency) if latency else 0

    def _end_to_end_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.batchs.values()), max(self.commits.values())
        duration = end - start
        tps = len(self.commits)*self.configs['pool']['batch_size'] / duration
        return tps, duration

    def _end_to_end_latency(self):
        latency = []
        for id,t in self.commits.items():
            if id in self.batchs:
                latency += [t-self.batchs[id]]
        return mean(latency) if latency else 0

    def result(self):
        consensus_latency = self._consensus_latency() * 1000
        consensus_tps, _ = self._consensus_throughput()
        end_to_end_tps, duration = self._end_to_end_throughput()
        end_to_end_latency = self._end_to_end_latency() * 1000
        tx_size = self.configs['pool']['tx_size']
        batch_size = self.configs['pool']['batch_size']
        rate = self.configs['pool']['rate']
        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Protocol: {self.protocol} \n'
            f' DDOS attack: {self.ddos} \n'
            f' Committee size: {self.committee_size} nodes\n'
            f' Input rate: {rate:,} tx/s\n'
            f' Transaction size: {tx_size:,} B\n'
            f' Batch size: {batch_size:,} tx/Batch\n'
            f' Faults: {self.faults} nodes\n'
            f' Execution time: {round(duration):,} s\n'
            '\n'
            ' + RESULTS:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
            '-----------------------------------------\n'
        )

    def write_json(self, filename):
        import json
        data = {
            'config': self.configs,
            'summary': {
                'consensus_tps': self._consensus_throughput()[0],
                'consensus_latency': self._consensus_latency() * 1000,
                'end_to_end_tps': self._end_to_end_throughput()[0],
                'end_to_end_latency': self._end_to_end_latency() * 1000,
            },
            'wait_times': self.wait_times,
            'round_advances': self.round_advances,
            'refs_collected': self.refs_collected
        }
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f:
            f.write(self.result())

    @classmethod
    def process(cls, directory, faults=0, protocol="", ddos=False):
        assert isinstance(directory, str)

        nodes = []
        for filename in sorted(glob(join(directory, 'node-info-*.log'))):
            with open(filename, 'r') as f:
                nodes += [f.read()]

        parser = cls(nodes, faults=faults, protocol=protocol, ddos=ddos)
        parser.write_json(join(directory, 'metrics.json'))
        return parser


class WahooLogParser:
    pass
