# Performance Tuning Guide

This guide helps you optimize pgsqlite for different workloads and use cases.

## Understanding Performance

pgsqlite adds a translation layer between PostgreSQL protocol and SQLite. The overhead varies by operation:

- **Cached SELECT**: ~74x overhead (excellent with read-only optimizer)
- **UPDATE/DELETE**: ~44-48x overhead (excellent)
- **Non-cached SELECT**: ~369x overhead (protocol translation)
- **Single-row INSERT**: ~332x overhead (use batch inserts instead)

### New Optimization Features (2025-07-18)

- **Read-Only Optimizer**: Automatically applied to SELECT queries for 2.4x speedup
- **Enhanced Statement Caching**: Intelligent caching with priority-based eviction
- **Query Plan Caching**: Up to 200 cached query plans with complexity classification
- **Type Conversion Caching**: Optimized boolean, datetime, and numeric type handling

## Quick Optimization Wins

### 1. Enable WAL Mode

```bash
# Significantly improves concurrent read performance
pgsqlite --pragma-journal-mode WAL
```

### 2. Optimize Cache Configuration

```bash
# For read-heavy workloads (recommended settings)
pgsqlite \
  --query-cache-size 5000 \
  --result-cache-size 500 \
  --statement-pool-size 200

# Note: Read-only optimizer and enhanced statement caching are automatically enabled
# These provide additional query plan caching (200+ plans) and 2.4x speedup for cached queries
```

### 3. Use Batch Operations

```sql
-- Instead of multiple INSERT statements
INSERT INTO users (name) VALUES ('Alice');
INSERT INTO users (name) VALUES ('Bob');

-- Use multi-row INSERT (up to 76x faster)
INSERT INTO users (name) VALUES 
  ('Alice'),
  ('Bob'),
  ('Charlie');
```

## Workload-Specific Tuning

### Read-Heavy Workloads

```bash
pgsqlite \
  --pragma-journal-mode WAL \
  --query-cache-size 10000 \
  --result-cache-size 1000 \
  --result-cache-ttl 300 \
  --schema-cache-ttl 3600 \
  --statement-pool-size 500
```

**Key optimizations**:
- Large query and result caches
- Extended cache TTLs
- WAL mode for concurrent reads

### Write-Heavy Workloads

```bash
pgsqlite \
  --pragma-journal-mode WAL \
  --pragma-synchronous NORMAL \
  --query-cache-size 1000 \
  --result-cache-size 100 \
  --statement-pool-size 200
```

**Key optimizations**:
- Smaller caches (writes invalidate cache)
- NORMAL synchronous mode (faster writes)
- Focus on statement pool for prepared statements

### Mixed Workloads

```bash
pgsqlite \
  --pragma-journal-mode WAL \
  --pragma-synchronous NORMAL \
  --query-cache-size 5000 \
  --result-cache-size 500 \
  --statement-pool-size 300 \
  --auto-cleanup \
  --memory-threshold 134217728
```

## Cache Configuration Deep Dive

### Query Plan Cache

Stores parsed and analyzed queries.

```bash
# Default: 1000 entries, 600s TTL
--query-cache-size 5000      # Increase for diverse queries
--query-cache-ttl 1800       # Longer TTL for stable workloads
```

### Result Set Cache

Caches complete query results for repeated identical queries.

```bash
# Default: 100 entries, 60s TTL
--result-cache-size 500      # More entries for common queries
--result-cache-ttl 300       # 5-minute TTL for slower-changing data
```

### Statement Pool

Reuses SQLite prepared statements.

```bash
# Default: 100 statements
--statement-pool-size 500    # Increase for many different queries
```

## SQLite PRAGMA Tuning

### Journal Modes

| Mode | Use Case | Performance | Durability |
|------|----------|-------------|------------|
| DELETE | Single writer | Baseline | High |
| WAL | Concurrent readers | Best | High |
| MEMORY | Temporary data | Fastest | None |
| OFF | Bulk imports | Fastest | None |

### Synchronous Modes

| Mode | Performance | Durability | Use Case |
|------|-------------|------------|----------|
| FULL | Slowest | Highest | Financial data |
| NORMAL | Balanced | Good | General use |
| OFF | Fastest | None | Temporary data |

### Example Configurations

```bash
# Development/Testing
pgsqlite \
  --pragma-journal-mode MEMORY \
  --pragma-synchronous OFF

# Production
pgsqlite \
  --pragma-journal-mode WAL \
  --pragma-synchronous NORMAL \
  --pragma-cache-size -128000  # 128MB page cache
```

## Memory Management

### Monitor Memory Usage

```bash
pgsqlite \
  --memory-monitoring \
  --memory-check-interval 30
```

### Automatic Cleanup

```bash
pgsqlite \
  --auto-cleanup \
  --memory-threshold 268435456     # 256MB threshold
  --high-memory-threshold 536870912 # 512MB high threshold
```

### Memory-Mapped I/O

For large databases:

```bash
pgsqlite \
  --enable-mmap \
  --pragma-mmap-size 1073741824  # 1GB mmap
```

## Connection Optimization

### Unix Sockets vs TCP

For local connections, Unix sockets provide 10-20% better performance:

```bash
# Unix socket configuration
pgsqlite --socket-dir /tmp

# Connect via socket
psql -h /tmp -p 5432 -d mydb
```

### Connection Pooling

Use connection pooling in your application:

```python
# Python example with psycopg2
from psycopg2 import pool

connection_pool = pool.SimpleConnectionPool(
    1, 20,  # min/max connections
    host="localhost",
    port=5432,
    database="mydb"
)
```

## Monitoring Performance

### Enable Metrics

```bash
pgsqlite \
  --cache-metrics-interval 60 \
  --buffer-monitoring \
  --memory-monitoring
```

### What to Monitor

1. **Cache hit rates**: Should be >80% for read-heavy workloads
2. **Memory usage**: Watch for cleanup events
3. **Statement pool efficiency**: High reuse indicates good performance
4. **Query patterns**: Identify slow or frequent queries

## Optimization Checklist

- [ ] Enable WAL mode for concurrent access
- [ ] Size caches based on workload
- [ ] Use batch operations for bulk data
- [ ] Configure appropriate synchronous mode
- [ ] Enable monitoring for production
- [ ] Use Unix sockets for local connections
- [ ] Implement connection pooling
- [ ] Regular VACUUM for long-running databases

## Benchmarking

### Quick Performance Test

```python
# benchmark.py
import psycopg2
import time

conn = psycopg2.connect("postgresql://localhost:5432/test")
cur = conn.cursor()

# Test query performance
start = time.time()
for i in range(1000):
    cur.execute("SELECT * FROM users WHERE id = %s", (i,))
    cur.fetchone()
print(f"Queries/second: {1000 / (time.time() - start)}")
```

### Load Testing

Use tools like pgbench for comprehensive testing:

```bash
# Initialize test database
pgbench -i -h localhost -p 5432 test

# Run benchmark
pgbench -c 10 -j 2 -t 1000 -h localhost -p 5432 test
```

## Common Pitfalls

1. **Over-caching**: Too large caches can increase memory usage
2. **Wrong journal mode**: DELETE mode poor for concurrent reads
3. **Single-row operations**: Always batch when possible
4. **Ignoring Unix sockets**: Significant performance for local connections
5. **No monitoring**: Can't optimize what you don't measure