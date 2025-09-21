# pgsqlite Benchmarks

This directory contains performance benchmarks comparing direct SQLite access with PostgreSQL client access through pgsqlite.

## Overview

The benchmark suite measures the overhead introduced by the PostgreSQL wire protocol translation layer. It performs identical operations using both direct SQLite connections and PostgreSQL clients connecting through pgsqlite.

## Latest Performance Results (2025-08-12)

### Best Performance by Operation Type

#### Read Operations: psycopg3-text
- **SELECT**: 0.136ms (125x overhead) - **21.8x faster than psycopg2!**
- **SELECT (cached)**: 0.299ms (90x overhead) - **5.5x faster than psycopg2**
- **Recommendation**: Use psycopg3-text for read-heavy workloads

#### Write Operations: psycopg2
- **INSERT**: 0.185ms (107x overhead) - **3.6x faster than psycopg3**
- **UPDATE**: 0.057ms (45x overhead) - **1.5x faster than psycopg3**
- **DELETE**: 0.036ms (38x overhead) - **2.0x faster than psycopg3**
- **Recommendation**: Use psycopg2 for write-heavy workloads

### Driver Comparison
| Driver | SELECT (ms) | INSERT (ms) | UPDATE (ms) | DELETE (ms) | Best For |
|--------|------------|-------------|-------------|-------------|----------|
| psycopg3-text | **0.136** 🏆 | 0.661 | 0.084 | 0.072 | Read-heavy workloads |
| psycopg2 | 2.963 | **0.185** 🏆 | **0.057** 🏆 | **0.036** 🏆 | Write-heavy workloads |
| psycopg3-binary | 0.497 | 0.691 | 0.086 | 0.071 | Complex data types |

### Overhead Analysis vs Pure SQLite

**Real-World Performance** (200 operations):
- **Pure SQLite**: 44.4ms (0.22ms per operation) - Maximum speed baseline
- **pgsqlite (all drivers)**: ~16 seconds (~80ms per operation) - **~360x overhead**

**Trade-off Analysis:**
- **Pure SQLite**: Maximum performance, no PostgreSQL compatibility
- **pgsqlite**: Full PostgreSQL compatibility + ORM support at 360x overhead cost
- **Context**: 80ms database operations feel instant to users in web applications

### Key Findings
- **psycopg3-text** dominates read performance with exceptional SELECT optimization
- **psycopg2** remains superior for write operations despite being legacy
- **psycopg3-binary** shows overhead that exceeds benefits for simple operations
- Binary protocol is fully functional but best suited for complex data types (BYTEA, arrays, etc.)
- **360x overhead** is acceptable trade-off for PostgreSQL compatibility in most web applications
- Database operations typically represent 10-20% of total request time in web apps

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

### Overhead Analysis Mode

To measure pgsqlite overhead compared to pure SQLite:

```bash
# Run overhead comparison (pgsqlite vs pure SQLite)
poetry run python overhead_comparison.py

# This will:
# 1. Run identical operations on pure SQLite
# 2. Run same operations on pgsqlite (requires server running on port 45000)
# 3. Calculate and display overhead percentages
# 4. Show real-world performance context
```

**Complete overhead testing workflow:**
```bash
# 1. Build and start pgsqlite server
cargo build --release
./target/release/pgsqlite --database test_overhead.db --port 45000 &

# 2. Run comprehensive overhead analysis
cd benchmarks && poetry run python overhead_comparison.py

# 3. Run individual driver tests for comparison
../test_sqlite.py          # Pure SQLite baseline
../test_pgsqlite_text.py   # pgsqlite text mode
../test_pgsqlite_binary.py # pgsqlite binary mode
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

## Specialized Benchmarks

### Array Binary Protocol Benchmark

Tests the performance of PostgreSQL array types with psycopg3 binary protocol:

```bash
# Run array benchmark with default settings (100 iterations)
./run_array_benchmark.sh

# Run with custom iterations
./run_array_benchmark.sh --iterations 500

# Run with custom port
./run_array_benchmark.sh --port 15500

# Run with debug build
./run_array_benchmark.sh --debug
```

**What's tested:**
- Integer arrays (`INTEGER[]`)
- Bigint arrays (`BIGINT[]`)
- Text arrays (`TEXT[]`)
- Float arrays (`DOUBLE PRECISION[]`)
- Boolean arrays (`BOOLEAN[]`)

**Array sizes tested:** 5, 10, 50, 100, 500 elements

**Operations:** CREATE, INSERT, SELECT, UPDATE with binary protocol encoding

**Requirements:**
- Poetry for dependency management: `curl -sSL https://install.python-poetry.org | python3 -`
- Dependencies are automatically installed by the script via `poetry install`
- Includes: psycopg3, tabulate, colorama (already configured in pyproject.toml)

This benchmark specifically measures the overhead of PostgreSQL array binary encoding/decoding compared to direct SQLite JSON array storage, which is essential for understanding the performance impact of using modern ORM array fields (Django ArrayField, SQLAlchemy ARRAY, Rails arrays) with psycopg3 binary mode.