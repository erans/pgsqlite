# pgsqlite Benchmarks

This directory contains performance benchmarks comparing direct SQLite access with PostgreSQL client access through pgsqlite.

## Overview

The benchmark suite measures the overhead introduced by the PostgreSQL wire protocol translation layer. It performs identical operations using both direct SQLite connections and PostgreSQL clients connecting through pgsqlite.

## Latest Performance Results (2025-08-08)

### Best Performance: psycopg3-binary
- **SELECT**: 0.139ms (139x overhead) - **5x better than target!**
- **Overall**: 168x overhead - **69% better than psycopg2**
- **Recommendation**: Use psycopg3-binary for production deployments

### Driver Comparison
| Driver | SELECT (ms) | Overall Overhead | vs psycopg2 |
|--------|------------|------------------|-------------|
| psycopg3-binary | 0.139 | 168x | 69% better |
| psycopg3-text | 0.680 | 331x | 38% better |
| psycopg2 | 2.631 | 539x | baseline |

## Running Benchmarks

### Basic Usage

By default, benchmarks run using:
- **Unix domain sockets** for connection (lowest latency)
- **In-memory databases** to measure pure protocol overhead

```bash
# Run with default settings (1000 operations, Unix socket, in-memory)
./run_benchmark.sh

# Run with custom iterations
./run_benchmark.sh -i 5000

# Run with custom iterations and batch size
./run_benchmark.sh -i 10000 -b 200
```

### TCP Mode

To benchmark using TCP/IP connections instead of Unix sockets:

```bash
# Run benchmark using TCP
./run_benchmark.sh --tcp

# Combine with other options
./run_benchmark.sh --tcp -i 5000
```

### File-Based Mode

To benchmark with disk I/O included:

```bash
# Run benchmark using file-based databases
./run_benchmark.sh --file-based

# With custom settings
./run_benchmark.sh --file-based -i 10000
```

### Driver Comparison Mode

To compare performance across different PostgreSQL drivers:

```bash
# Run comprehensive comparison of all drivers (psycopg2, psycopg3-text, psycopg3-binary)
./run_driver_comparison.sh

# This will:
# 1. Build pgsqlite in release mode
# 2. Start pgsqlite server
# 3. Run benchmarks with psycopg2
# 4. Run benchmarks with psycopg3-text
# 5. Run benchmarks with psycopg3-binary
# 6. Display comparison results
```

You can also run individual driver benchmarks:

```bash
# Run with specific driver
poetry run python benchmark_drivers.py --driver psycopg3-binary --port 5432

# Available drivers:
# - psycopg2 (traditional, legacy)
# - psycopg3-text (modern, text protocol)
# - psycopg3-binary (modern, binary protocol - FASTEST)
```

## What's Measured

The benchmark performs mixed operations including:
- **CREATE TABLE**: Table creation with various data types
- **INSERT**: Adding new records with random data
- **UPDATE**: Modifying existing records
- **DELETE**: Removing records
- **SELECT**: Querying data with WHERE conditions

For each operation type, the benchmark tracks:
- Average execution time (milliseconds)
- Total execution time (seconds)
- Min/max/median times
- Overhead percentage (pgsqlite vs direct SQLite)

## Setup Requirements

1. **Poetry**: Python dependency management
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   ```

2. **Python 3.8+**: Required for running the benchmark script

The `run_benchmark.sh` script handles:
- Building pgsqlite in release mode
- Setting up Python virtual environment
- Installing dependencies
- Starting/stopping the pgsqlite server
- Running the benchmark
- Cleanup

## Understanding Results

The benchmark output shows:
- **SQLite Avg**: Average time for direct SQLite operations
- **pgsqlite Avg**: Average time through PostgreSQL protocol
- **Overhead**: Percentage difference between the two
- **Count**: Number of operations performed
- **Total time**: Cumulative time for all operations

Lower overhead percentages indicate better protocol translation efficiency.

## Tips for Accurate Benchmarking

1. **Use Release Mode**: Always compile with `--release` for accurate measurements
2. **Default Settings**: Benchmarks use Unix sockets and in-memory databases by default for minimal overhead
3. **TCP Testing**: Use `--tcp` to measure TCP/IP networking overhead
4. **File-Based Testing**: Use `--file-based` when you need to include disk I/O in measurements
5. **Multiple Runs**: Run benchmarks multiple times to account for system variability
6. **Sufficient Iterations**: Use at least 1000 operations for meaningful averages
7. **System Load**: Run on a quiet system for consistent results

## Connection Modes Comparison

- **Unix Socket (default)**: Local-only connection via filesystem socket, lowest latency
- **TCP**: Standard network connection, includes TCP/IP overhead
- **In-Memory (default)**: SQLite database in RAM, eliminates disk I/O
- **File-Based**: SQLite database on disk, includes disk I/O overhead