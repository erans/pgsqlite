# Performance Analysis

This document provides detailed performance benchmarks and analysis of pgsqlite.

## Executive Summary

**⚠️ CRITICAL: As of 2025-07-29, pgsqlite is experiencing severe performance regression.**

The recent connection-per-session architecture changes have introduced massive overhead:
- SELECT operations are 568x worse than the documented target
- All DML operations (INSERT, UPDATE, DELETE) are 100-300x worse than targets
- Even cached queries are performing 3.5x worse than expected

**Root Cause Analysis (Preliminary):**
1. Connection-per-session architecture may have excessive mutex contention
2. Session state management overhead in hot paths
3. Possible connection lookup inefficiencies
4. LazyQueryProcessor allocations may be impacting performance

**Immediate Actions Required:**
1. Profile the connection management code
2. Identify and remove bottlenecks in session handling
3. Optimize hot paths to reduce overhead
4. Consider connection pooling within sessions

## Benchmark Results

### ⚠️ CRITICAL PERFORMANCE REGRESSION (2025-07-29)

After implementing connection-per-session architecture, performance has severely degraded:

```
================================================================================
                     SEVERE PERFORMANCE REGRESSION ALERT
================================================================================

Benchmark Configuration:
- Records: 1,000+ operations per type
- SQLite: In-memory database
- pgsqlite: In-memory with connection-per-session architecture
- Connection: Unix Socket

================================================================================
                          Current vs Target Performance
================================================================================

Operation        | SQLite (ms) | pgsqlite (ms) | Current    | Target   | Status
-----------------|-------------|---------------|------------|----------|--------
CREATE TABLE     |    0.145    |    15.769     | 10,792.1%  | ~100x    | ❌ 107x worse
INSERT (single)  |    0.002    |     0.174     | 10,753.0%  | 36.6x    | ❌ 294x worse
SELECT (first)   |    0.001    |     3.827     | 383,068.5% | 674.9x   | ❌ 568x worse
SELECT (cached)  |    0.005    |     0.159     |  3,185.9%  | 17.2x    | ❌ 3.5x worse
UPDATE           |    0.001    |     0.063     |  5,368.6%  | 50.9x    | ❌ 105x worse
DELETE           |    0.001    |     0.045     |  4,636.9%  | 35.8x    | ❌ 130x worse

Cache Effectiveness: 24.1x speedup (3.827ms → 0.159ms) - Good ratio but poor absolute performance
Connection-per-session: SEVERE OVERHEAD - needs immediate optimization
```

### Previous Best Results (2025-07-27 - Pre-regression)

```
Operation        | SQLite (ms) | pgsqlite (ms) | Overhead | Performance
-----------------|-------------|---------------|----------|-------------
SELECT (first)   |    0.001    |     0.669     |  674.9x  | Good
SELECT (cached)  |    0.003    |     0.046     |   17.2x  | Excellent ⭐
UPDATE           |    0.001    |     0.059     |   50.9x  | Excellent ⭐
DELETE           |    0.001    |     0.034     |   35.8x  | Excellent ⭐
INSERT (single)  |    0.002    |     0.060     |   36.6x  | Excellent ⭐
```

### Historical Performance Improvements

| Date | SELECT (cached) | UPDATE | DELETE | Key Optimization |
|------|----------------|---------|---------|------------------|
| 2025-01-01 | 118x | 68x | 65x | Initial implementation |
| 2025-01-15 | 67x | 55x | 52x | Zero-copy architecture |
| 2025-02-01 | 45x | 50x | 47x | Fast path optimization |
| 2025-07-08 | 39x | 48x | 44x | Ultra-fast path + caching |
| 2025-07-18 | 74x | 53x | 43x | **Query optimization system** |

**Latest Optimization (2025-07-18)**:
- **Read-Only Optimizer**: Direct execution path for SELECT queries
- **Enhanced Statement Caching**: 200+ cached query plans with priority eviction
- **Query Plan Caching**: Complexity classification and type conversion caching
- **Cache Effectiveness**: 2.4x speedup for cached queries (was 1.9x)

## Operation-Specific Analysis

### SELECT Operations

#### First Query (Non-cached)
- **Overhead**: 294x
- **Breakdown**:
  - Protocol parsing: ~40%
  - Query analysis: ~20%
  - Execution: ~10%
  - Result formatting: ~30%

#### Cached Query
- **Overhead**: 39x (excellent)
- **Optimizations**:
  - Result set cache hit
  - Prepared statement reuse
  - Pre-computed metadata

### INSERT Operations

#### Single-Row INSERT
- **Overhead**: 332x
- **Why**: Full protocol round-trip per row
- **Recommendation**: Use multi-row INSERT

#### Multi-Row INSERT Performance
```sql
-- Benchmark results for batch inserts (1000 rows total)
Batch Size | Time (ms) | Rows/sec | Speedup vs Single
-----------|-----------|----------|------------------
1          | 332       | 3,012    | 1.0x (baseline)
10         | 28.9      | 34,602   | 11.5x
100        | 6.48      | 154,321  | 51.3x
1000       | 4.35      | 229,885  | 76.4x

-- Example: Insert 1000 rows as single batch
INSERT INTO table (col1, col2) VALUES 
  (val1, val2),
  (val3, val4),
  ... -- 998 more rows
```

#### Batch INSERT Optimization (2025-07-11)
- **Fast Path Detection**: Batch INSERTs without datetime/decimal values bypass translation
- **Prepared Statement Caching**: Batch patterns are fingerprinted for metadata reuse
- **Performance**: Up to 112.9x speedup achieved with fast path optimization

### UPDATE/DELETE Operations

- **Overhead**: 44-48x (excellent)
- **Why efficient**:
  - Fast path optimization
  - Minimal translation needed
  - Efficient change detection

## Cache Performance

### Cache Hit Rates (Typical Workload)

| Cache Type | Hit Rate | Impact |
|------------|----------|---------|
| Query Plan | 92% | Avoids parsing |
| Result Set | 45% | Skips execution |
| Statement Pool | 88% | Reuses prepared statements |
| Schema | 99% | Fast type lookup |

### Cache Configuration Impact

```
# Baseline (default settings)
SELECT performance: 294ms first, 156ms cached

# Aggressive caching
--query-cache-size 10000 --result-cache-size 1000
SELECT performance: 285ms first, 142ms cached (9% improvement)

# Minimal caching
--query-cache-size 100 --result-cache-size 10
SELECT performance: 312ms first, 198ms cached (27% worse)
```

## Fast Path Optimization

The fast path optimizer bypasses full query parsing for simple operations:

### Eligible Queries
- Simple INSERT/UPDATE/DELETE
- No PostgreSQL-specific casts (::type)
- No datetime functions
- No JOINs or subqueries

### Performance Impact
```
Operation     | Regular Path | Fast Path | Improvement
--------------|--------------|-----------|------------
INSERT        | 332x         | 48x       | 6.9x faster
UPDATE        | 156x         | 48x       | 3.3x faster
DELETE        | 147x         | 44x       | 3.3x faster
```

## Memory Usage

### Typical Memory Profile

```
Component          | Memory (MB) | Percentage
-------------------|-------------|------------
Query Cache        | 12.5        | 25%
Result Cache       | 18.2        | 36%
Statement Pool     | 8.4         | 17%
Schema Cache       | 3.2         | 6%
Buffer Pool        | 5.1         | 10%
Other              | 3.1         | 6%
-------------------|-------------|------------
Total              | 50.5        | 100%
```

### Memory vs Performance Trade-off

| Cache Size | Memory | Avg Latency | 99th Percentile |
|------------|---------|-------------|-----------------|
| Small | 25 MB | 0.285 ms | 1.2 ms |
| Default | 50 MB | 0.156 ms | 0.8 ms |
| Large | 150 MB | 0.142 ms | 0.6 ms |

## Network Protocol Overhead

### Message Size Analysis

```
Query: SELECT id, name FROM users WHERE id = 1

PostgreSQL Protocol:
- Query message: 45 bytes
- RowDescription: 89 bytes
- DataRow: 28 bytes
- CommandComplete: 13 bytes
Total: 175 bytes

Raw SQLite Result: 12 bytes
Overhead: 14.6x
```

### Latency Breakdown

```
TCP Connection (localhost): 0.05ms
SSL Handshake (if enabled): 2.1ms
Query Parse: 0.08ms
Execute: 0.02ms
Format Response: 0.09ms
Network Send: 0.04ms
```

## Optimization Strategies

### 1. For Read-Heavy Workloads

```bash
pgsqlite \
  --query-cache-size 10000 \
  --result-cache-size 2000 \
  --result-cache-ttl 600 \
  --pragma-journal-mode WAL
```

Expected improvement: 40-60% better read performance

### 2. For Write-Heavy Workloads

```bash
pgsqlite \
  --query-cache-size 1000 \
  --result-cache-size 100 \
  --pragma-synchronous NORMAL \
  --pragma-journal-mode WAL
```

Expected improvement: 20-30% better write performance

### 3. For Mixed Workloads

```bash
pgsqlite \
  --query-cache-size 5000 \
  --result-cache-size 500 \
  --statement-pool-size 300 \
  --pragma-journal-mode WAL
```

Balanced performance for both reads and writes

## Benchmarking Tools

### Running the Benchmark Suite

```bash
# Comprehensive benchmark
./benchmarks/benchmark.py

# Specific operation benchmark
./benchmarks/benchmark.py --operation select

# Custom configuration
./benchmarks/benchmark.py --records 100000 --cache-size 10000
```

### Profiling pgsqlite

```bash
# CPU profiling
cargo build --release --features profiling
perf record --call-graph=dwarf ./target/release/pgsqlite
perf report

# Memory profiling
valgrind --tool=massif ./target/release/pgsqlite
ms_print massif.out.*
```

## Performance Guidelines

### DO:
- ✅ Use multi-row INSERT for bulk data (up to 76x faster)
- ✅ Batch INSERTs in groups of 100-1000 for optimal performance
- ✅ Enable WAL mode for concurrent reads
- ✅ Size caches based on working set
- ✅ Use prepared statements
- ✅ Batch similar operations

### DON'T:
- ❌ Use single-row INSERT for bulk loading
- ❌ Create batches larger than 1000 rows (diminishing returns)
- ❌ Over-provision caches (diminishing returns)
- ❌ Disable all caching
- ❌ Use complex queries without testing
- ❌ Ignore connection pooling

## Future Performance Work

1. **Query Plan Cache Serialization**: Persist across restarts
2. **Adaptive Caching**: Auto-tune based on workload
3. **Parallel Query Execution**: For read-only queries
4. **JIT Query Compilation**: For hot queries
5. **Connection Multiplexing**: Handle more concurrent clients