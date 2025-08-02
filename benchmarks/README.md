# pgsqlite Benchmarks

This directory contains performance benchmarks comparing direct SQLite access with PostgreSQL client access through pgsqlite.

## Overview

The benchmark suite measures the overhead introduced by the PostgreSQL wire protocol translation layer. It performs identical operations using both direct SQLite connections and PostgreSQL clients connecting through pgsqlite.

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

# Run with PostgreSQL binary wire format (requires psycopg3)
./run_benchmark.sh --binary-format
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

### Binary Wire Format Mode

To benchmark using PostgreSQL's binary wire format instead of text format:

```bash
# Run benchmark with binary format (uses psycopg3)
./run_benchmark.sh --binary-format

# Combine with other options
./run_benchmark.sh --binary-format --tcp -i 5000
```

Binary format provides:
- **18% faster INSERT operations**
- **11-13% faster UPDATE/DELETE operations**
- **7-45% slower SELECT operations** (due to encoding overhead)
- Recommended for write-heavy workloads

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