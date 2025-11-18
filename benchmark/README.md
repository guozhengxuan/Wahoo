# Loom Benchmark Suite

This repository contains the benchmarking tools and scripts for the Loom consensus protocol.

## Overview

This benchmark suite provides tools for testing Loom performance both locally and on cloud infrastructure (Alibaba Cloud).

## Quick Start

### Local Benchmarks

1. Install dependencies:
```shell
pip install -r requirements.txt
```

2. Install tmux (required for running nodes in background):
```shell
# macOS
brew install tmux

# Ubuntu/Debian
sudo apt-get install tmux
```

3. Run local benchmark:
```shell
fab local
```

### Cloud Benchmarks

See [WAHOO_BENCHMARK.md](WAHOO_BENCHMARK.md) for detailed instructions on running benchmarks on Alibaba Cloud.

## Configuration

- `settings.json` - Cloud infrastructure settings
- `fabfile.py` - Fabric tasks for orchestrating benchmarks
- `requirements.txt` - Python dependencies

## Repository Structure

```
benchmark/
├── alibaba/          # Alibaba Cloud integration
├── benchmark/        # Core benchmark scripts
├── fabfile.py        # Fabric automation tasks
├── settings.json     # Configuration settings
└── requirements.txt  # Python dependencies
```

## Related

This benchmark suite is designed for the [Loom consensus protocol](https://github.com/zhengxuanguo/Loom).
