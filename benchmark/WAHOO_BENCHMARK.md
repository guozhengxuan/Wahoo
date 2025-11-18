# Wahoo/Tusk/GradedDAG Benchmark Integration

This document describes how to use the Loom benchmark framework to test Wahoo, Tusk, and GradedDAG protocols on Aliyun instances.

## Overview

The Wahoo benchmark integration follows the same workflow as Loom benchmarks, with specialized tasks for Wahoo-specific requirements:

1. **fab create** - Create Aliyun instances (same as Loom)
2. **fab install_w** - Install Wahoo dependencies (Go 1.16+)
3. **fab upload_exec_w** - Pull Wahoo code and compile on remote servers
4. **fab remote_w** - Run Wahoo/Tusk/GradedDAG benchmarks

## Setup Steps

### 1. Create Aliyun Instances
```bash
fab create
```
Creates the specified number of instances across configured regions.

### 2. Install Wahoo Dependencies
```bash
fab install_w
```
This task installs:
- Go 1.16.15 (required by Wahoo)
- Sets up Go environment variables
- Configures Go module proxy

Based on lines 15-28 of Wahoo README (excluding ansible as we don't need it).

### 3. Deploy Wahoo Code
```bash
fab upload_exec_w
```
This task:
- Pulls Wahoo from https://github.com/guozhengxuan/Wahoo.git
- Compiles the BFT binary on each remote server
- Works concurrently across all hosts

### 4. Run Benchmarks

#### Run Wahoo
```bash
fab remote_w
# or explicitly
fab remote_w:protocol=wahoo
```

#### Run Tusk
```bash
fab remote_w:protocol=tusk
```

#### Run GradedDAG
```bash
fab remote_w:protocol=gradeddag
```

## Configuration

The benchmark parameters can be modified in `fabfile.py` under the `remote_w` task:

```python
bench_params = {
    'nodes': [10],              # Number of nodes to test
    'node_instance': 1,          # Instances per node
    'rounds': 30,              # Benchmark duration in rounds
    'rate': 5_000,               # Transaction rate
    'batch_size': [500, 1000],   # Batch sizes to test
    'log_level': 0b1111,         # Log level
    'protocol_name': protocol,   # Protocol name
    'runs': 1                    # Number of runs
}
```

## Key Differences from Loom Benchmarks

1. **Dependencies**: Wahoo requires Go 1.16+ (installed via `install_w`)
2. **Code Deployment**: Wahoo code is pulled from CGCL-codes/Wahoo repo
3. **Configuration**: Automatic config generation with Wahoo's config_gen (see below)
4. **Binary**: Wahoo builds a `BFT` executable (vs Loom's `main`)
5. **Process Management**: Uses `killall BFT` instead of `tmux kill-server`

## Configuration Generation

The benchmark automatically handles Wahoo configuration:

1. **Config Template**: `remote_w` generates `config_template.yaml` with actual instance IPs
2. **Key Generation**: Runs `config_gen/main.go` to generate ED25519 and threshold signature keys
3. **Node Configs**: Creates individual `node{i}_0.yaml` files for each node
4. **Upload**: Distributes configs to remote servers as `~/Wahoo/config.yaml`

**Settings** (in `settings.json`):
```json
"wahoo": {
    "path": "/Users/zhengxuanguo/gitrepo/Wahoo",
    "p2p_port": 9000
}
```

## TODO

- [ ] Add Wahoo-specific log parser for performance metrics
- [ ] Implement result parsing for throughput/latency calculation

## Architecture

The implementation follows Loom's benchmark pattern:

```
remote.py:
- install_wahoo_deps()                    # Install Go 1.16+
- pull_exec_wahoo()                       # Pull and compile Wahoo
- _generate_wahoo_config_template()       # Generate config_template.yaml with IPs
- _generate_and_upload_wahoo_configs()    # Run config_gen and upload configs
- _run_single_wahoo()                     # Run benchmark (similar to _run_single)
- kill_wahoo()                            # Kill processes (similar to kill)
- _logs_wahoo()                           # Download logs (similar to _logs)
- run_wahoo()                             # Main benchmark runner (similar to run)

fabfile.py:
- install_w                   # Fab task for install_wahoo_deps
- upload_exec_w               # Fab task for pull_exec_wahoo
- remote_w                    # Fab task for run_wahoo with protocol selection
```

## Notes

- Logs are saved following Loom's pattern: `logs/{timestamp}/node-{i}.log`
- Results are saved to `wahoo_results/{protocol}_{nodes}_{batch_size}_{timestamp}.txt`
- The framework handles concurrent operations for faster deployment
- All tasks use the same SSH credentials as Loom benchmarks
