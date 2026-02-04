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
    def __init__(self, nodes, faults, protocol, ddos):

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

        batchs, proposals, commits, configs, eval_metrics = zip(*results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        self.commits = self._merge_results([x.items() for x in commits])
        self.batchs = self._merge_results([x.items() for x in batchs])
        self.configs = configs[0]

        # Merge eval metrics from all nodes
        self.eval_metrics = self._merge_eval_metrics(eval_metrics)

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _merge_eval_metrics(self, metrics_list):
        """Merge evaluation metrics from all nodes."""
        merged = {
            'comm_cost': [],
            'block_new': [],
            'broadcast_end': [],
            'round_advanced': [],
        }
        for m in metrics_list:
            merged['comm_cost'].extend(m.get('comm_cost', []))
            merged['block_new'].extend(m.get('block_new', []))
            merged['broadcast_end'].extend(m.get('broadcast_end', []))
            merged['round_advanced'].extend(m.get('round_advanced', []))
        return merged

    def _parse_nodes(self, log):
        if search(r'panic', log) is not None:
            raise ParseError('Client(s) panicked')

        tmp = findall(r'\[INFO] (.*) pool.* Received Batch (\d+)', log)
        batchs = {id: self._to_posix(t) for t, id in tmp}

        tmp = findall(r'\[INFO] (.*) core.* create Block height \d+ node \d+ batch_id (\d+)', log)
        proposals = {id: self._to_posix(t) for t, id in tmp}

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
                'rate': int(
                    search(r'Transaction pool tx rate set to (\d+)', log).group(1)
                ),
            }
        }

        # =====================================================
        # [EVAL] Parsing for Graph 1, 2, 3
        # Wahoo/GradedDAG/Tusk use hclog format: key=value
        # =====================================================
        eval_metrics = {
            'comm_cost': [],
            'block_new': [],
            'broadcast_end': [],
            'round_advanced': [],
        }

        # --- Graph 1: Communication Rounds ---
        # Try new format first: [EVAL] COMM_COST: node=X val=Y round=Z ts=W
        tmp_comm_new = findall(r'\[EVAL\] COMM_COST.*node[=\s](\S+).*val[=\s](\d+).*round[=\s](\d+).*ts[=\s](\d+)', log)
        if tmp_comm_new:
            for n, val, r, ts in tmp_comm_new:
                eval_metrics['comm_cost'].append({
                    'node': n,
                    'value': int(val),
                    'round': int(r),
                    'ts_ns': int(ts)
                })
        else:
            # Fall back to old format: [EVAL] COMM_COST: val=X round=Y ts=Z
            # Node will be inferred later from BLOCK_NEW
            tmp_comm = findall(r'\[EVAL\] COMM_COST.*val[=\s](\d+).*round[=\s](\d+).*ts[=\s](\d+)', log)
            for val, r, ts in tmp_comm:
                eval_metrics['comm_cost'].append({
                    'node': None,  # Will be set after parsing BLOCK_NEW
                    'value': int(val),
                    'round': int(r),
                    'ts_ns': int(ts)
                })

        # --- Graph 1: Round Advance ---
        # Wahoo/GradedDAG/Tusk format: [EVAL] ROUND_ADVANCED: node=X old_round=Y new_round=Z blocks_collected=W
        tmp_rounds = findall(r'\[EVAL\] ROUND_ADVANCED.*node[=\s](\S+).*old_round[=\s](\d+).*new_round[=\s](\d+).*blocks_collected[=\s](\d+)', log)
        for n, old_r, new_r, col in tmp_rounds:
            eval_metrics['round_advanced'].append({
                'node': n,
                'old_round': int(old_r),
                'new_round': int(new_r),
                'blocks_collected': int(col)
            })

        # --- Graph 2: Block Creation (Input Throughput) ---
        # Wahoo/GradedDAG/Tusk format: [EVAL] BLOCK_NEW: node=X round=Y ts=Z
        tmp_blocks = findall(r'\[EVAL\] BLOCK_NEW.*node[=\s](\S+).*round[=\s](\d+).*ts[=\s](\d+)', log)
        for n, r, ts in tmp_blocks:
            eval_metrics['block_new'].append({
                'node': n,
                'round': int(r),
                'ts_ns': int(ts)
            })

        # --- Graph 3: Broadcast End ---
        # Wahoo format: [EVAL] BROADCAST_END: type=EPBC/PBC round=X blockSender=Y ts=Z
        # GradedDAG format: [EVAL] BROADCAST_END: type=RBC/CBC round=X blockSender=Y ts=Z
        # Tusk format: [EVAL] BROADCAST_END: dataSN=X proposer=Y ts=Z

        # Wahoo/GradedDAG with type
        tmp_broadcast_typed = findall(r'\[EVAL\] BROADCAST_END.*type[=\s](\S+).*round[=\s](\d+).*blockSender[=\s](\S+).*ts[=\s](\d+)', log)
        for btype, r, sender, ts in tmp_broadcast_typed:
            eval_metrics['broadcast_end'].append({
                'type': btype,
                'round': int(r),
                'block_sender': sender,
                'ts_ns': int(ts)
            })

        # Tusk format (without type, uses dataSN)
        tmp_broadcast_tusk = findall(r'\[EVAL\] BROADCAST_END.*dataSN[=\s](\d+).*proposer[=\s](\S+).*ts[=\s](\d+)', log)
        for sn, proposer, ts in tmp_broadcast_tusk:
            eval_metrics['broadcast_end'].append({
                'type': 'RBC',
                'round': int(sn),  # dataSN corresponds to round
                'block_sender': proposer,
                'ts_ns': int(ts)
            })

        # Infer node for old COMM_COST logs (without node info)
        if eval_metrics['block_new']:
            inferred_node = eval_metrics['block_new'][0]['node']
            for item in eval_metrics['comm_cost']:
                if item['node'] is None:
                    item['node'] = inferred_node

        return batchs, proposals, commits, configs, eval_metrics

    def _to_posix(self, string):
        dt = datetime.strptime(string, "%Y/%m/%d %H:%M:%S.%f")
        timestamp = dt.timestamp()
        return timestamp

    def _consensus_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.commits.values())
        duration = end - start
        tps = len(self.commits) * self.configs['pool']['batch_size'] / duration
        return tps, duration

    def _consensus_latency(self):
        latency = [c - self.proposals[d] for d, c in self.commits.items() if d in self.proposals]
        return mean(latency) if latency else 0

    def _end_to_end_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.batchs.values()), max(self.commits.values())
        duration = end - start
        tps = len(self.commits) * self.configs['pool']['batch_size'] / duration
        return tps, duration

    def _end_to_end_latency(self):
        latency = []
        for id, t in self.commits.items():
            if id in self.batchs:
                latency += [t - self.batchs[id]]
        return mean(latency) if latency else 0

    def _calculate_graph1_metrics(self):
        """Calculate Graph 1: Wave Efficiency metrics (per-node, then averaged)."""
        metrics = self.eval_metrics

        # Group by node
        comm_by_node = {}
        blocks_by_node = {}
        rounds_by_node = {}

        for item in metrics['comm_cost']:
            # For Wahoo/GradedDAG/Tusk, node info comes from block_new events
            # comm_cost doesn't have node, so we count per round and divide by num_nodes later
            round_num = item['round']
            if 'all' not in comm_by_node:
                comm_by_node['all'] = 0
            comm_by_node['all'] += item['value']

        for item in metrics['block_new']:
            node = item['node']
            if node not in blocks_by_node:
                blocks_by_node[node] = 0
            blocks_by_node[node] += 1

        for item in metrics['round_advanced']:
            node = item['node']
            if node not in rounds_by_node:
                rounds_by_node[node] = 0
            rounds_by_node[node] += 1

        # Calculate per-node metrics
        all_nodes = set(blocks_by_node.keys())
        num_nodes = len(all_nodes) if all_nodes else 1

        # Comm cost is logged once per round (not per node), so divide by num_nodes
        total_comm = comm_by_node.get('all', 0)
        avg_comm_per_node = total_comm / num_nodes if num_nodes > 0 else 0

        per_node_data = {}
        for node in all_nodes:
            node_blocks = blocks_by_node.get(node, 0)
            node_rounds = rounds_by_node.get(node, 0)
            # Estimate per-node comm cost based on blocks ratio
            node_comm = avg_comm_per_node
            per_node_data[node] = {
                'comm_rounds': node_comm,
                'blocks': node_blocks,
                'round_advances': node_rounds,
                'comm_rounds_per_block': node_comm / node_blocks if node_blocks > 0 else 0,
            }

        # Calculate averages across nodes
        avg_blocks = mean([d['blocks'] for d in per_node_data.values()]) if per_node_data else 0
        avg_round_advances = mean([d['round_advances'] for d in per_node_data.values()]) if per_node_data else 0
        avg_comm_per_block = mean([d['comm_rounds_per_block'] for d in per_node_data.values()]) if per_node_data else 0

        return {
            'avg_comm_rounds_per_node': avg_comm_per_node,
            'avg_blocks_per_node': avg_blocks,
            'avg_round_advances_per_node': avg_round_advances,
            'avg_comm_rounds_per_block': avg_comm_per_block,
            'num_nodes': num_nodes,
            'per_node_data': per_node_data,
        }

    def _calculate_graph2_metrics(self):
        """Calculate Graph 2: Input Throughput metrics (per-node cumulative blocks over time)."""
        # Group by node
        blocks_by_node = {}
        for event in self.eval_metrics['block_new']:
            node = event['node']
            if node not in blocks_by_node:
                blocks_by_node[node] = []
            blocks_by_node[node].append(event)

        per_node_cumulative = {}
        for node, events in blocks_by_node.items():
            sorted_events = sorted(events, key=lambda x: x['ts_ns'])
            if not sorted_events:
                continue
            start_ts = sorted_events[0]['ts_ns']
            cumulative = []
            for i, event in enumerate(sorted_events):
                cumulative.append({
                    'time_ms': (event['ts_ns'] - start_ts) / 1e6,
                    'cumulative_count': i + 1,
                    'round': event['round'],
                })
            per_node_cumulative[node] = cumulative

        return {'per_node_cumulative': per_node_cumulative}

    def _calculate_graph3_metrics(self):
        """Calculate Graph 3: Latency Decomposition (per-node, then averaged).

        For Wahoo/GradedDAG/Tusk, we use round R:
        - Broadcast Time = BROADCAST_END(R) - BLOCK_NEW(R)
        - Wait-for-Ref Time = BLOCK_NEW(R+1) - BROADCAST_END(R)
        """
        # Group events by node
        block_new_by_node = {}
        broadcast_end_by_node = {}

        for event in self.eval_metrics['block_new']:
            node = event['node']
            round_num = event['round']
            if node not in block_new_by_node:
                block_new_by_node[node] = {}
            block_new_by_node[node][round_num] = event['ts_ns']

        for event in self.eval_metrics['broadcast_end']:
            sender = event['block_sender']
            round_num = event['round']
            if sender not in broadcast_end_by_node:
                broadcast_end_by_node[sender] = {}
            # Keep earliest broadcast end for each round
            if round_num not in broadcast_end_by_node[sender] or broadcast_end_by_node[sender][round_num] > event['ts_ns']:
                broadcast_end_by_node[sender][round_num] = event['ts_ns']

        per_node_latency = {}

        for node, rounds in block_new_by_node.items():
            broadcast_ends = broadcast_end_by_node.get(node, {})
            sorted_rounds = sorted(rounds.keys())

            node_broadcast_times = []
            node_wait_times = []

            for i, r in enumerate(sorted_rounds):
                block_new_ts = rounds[r]
                broadcast_end_ts = broadcast_ends.get(r)

                if broadcast_end_ts:
                    broadcast_time_ns = broadcast_end_ts - block_new_ts
                    node_broadcast_times.append(broadcast_time_ns)

                    # Wait-for-ref: time from BROADCAST_END(R) to BLOCK_NEW(R+1)
                    if (r + 1) in rounds:
                        next_block_new_ts = rounds[r + 1]
                        wait_for_ref_ns = next_block_new_ts - broadcast_end_ts
                        node_wait_times.append(wait_for_ref_ns)

            per_node_latency[node] = {
                'avg_broadcast_time_ms': mean(node_broadcast_times) / 1e6 if node_broadcast_times else 0,
                'avg_wait_for_ref_ms': mean(node_wait_times) / 1e6 if node_wait_times else 0,
                'num_samples': len(node_broadcast_times),
            }

        # Calculate overall averages across nodes
        all_broadcast = [d['avg_broadcast_time_ms'] for d in per_node_latency.values() if d['num_samples'] > 0]
        all_wait = [d['avg_wait_for_ref_ms'] for d in per_node_latency.values() if d['num_samples'] > 0]

        return {
            'avg_broadcast_time_ms': mean(all_broadcast) if all_broadcast else 0,
            'avg_wait_for_ref_ms': mean(all_wait) if all_wait else 0,
            'per_node_latency': per_node_latency,
        }

    def result(self):
        consensus_latency = self._consensus_latency() * 1000
        consensus_tps, _ = self._consensus_throughput()
        end_to_end_tps, duration = self._end_to_end_throughput()
        end_to_end_latency = self._end_to_end_latency() * 1000
        tx_size = self.configs['pool']['tx_size']
        batch_size = self.configs['pool']['batch_size']
        rate = self.configs['pool']['rate']

        graph1 = self._calculate_graph1_metrics()
        graph3 = self._calculate_graph3_metrics()

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
            '\n'
            ' + EVAL METRICS (Graph 1 - Wave Efficiency):\n'
            f' Total Comm. Rounds: {graph1["total_comm_rounds"]}\n'
            f' Total Blocks: {graph1["total_blocks"]}\n'
            f' Comm. Rounds per Block: {graph1["comm_rounds_per_block"]:.2f}\n'
            '\n'
            ' + EVAL METRICS (Graph 3 - Latency Decomposition):\n'
            f' Avg Broadcast Time: {graph3["avg_broadcast_time_ms"]:.2f} ms\n'
            f' Avg Wait-for-Ref Time: {graph3["avg_wait_for_ref_ms"]:.2f} ms\n'
            '-----------------------------------------\n'
        )

    def write_json(self, filename):
        import json

        graph1 = self._calculate_graph1_metrics()
        graph2 = self._calculate_graph2_metrics()
        graph3 = self._calculate_graph3_metrics()

        data = {
            'config': self.configs,
            'summary': {
                'consensus_tps': self._consensus_throughput()[0],
                'consensus_latency': self._consensus_latency() * 1000,
                'end_to_end_tps': self._end_to_end_throughput()[0],
                'end_to_end_latency': self._end_to_end_latency() * 1000,
            },
            'graph1_wave_efficiency': graph1,
            'graph2_input_throughput': graph2,
            'graph3_latency_decomposition': graph3,
            'raw_eval_metrics': self.eval_metrics,
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
