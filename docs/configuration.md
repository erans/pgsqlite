# Configuration Reference

This document provides a comprehensive reference for all pgsqlite configuration options.

## Configuration Methods

pgsqlite can be configured through:

1. **Command line arguments** (highest priority)
2. **Environment variables** (with `PGSQLITE_` prefix)
3. **Default values** (lowest priority)

## Basic Configuration

### Server Options

| Option | CLI Flag | Environment Variable | Default | Description |
|--------|----------|---------------------|---------|-------------|
| Port | `--port`, `-p` | `PGSQLITE_PORT` | `5432` | PostgreSQL port to listen on |
| Database | `--database`, `-d` | `PGSQLITE_DATABASE` | `sqlite.db` | Path to SQLite database file |
| Log Level | `--log-level` | `PGSQLITE_LOG_LEVEL` | `info` | Logging level (error, warn, info, debug, trace) |
| In-Memory | `--in-memory` | `PGSQLITE_IN_MEMORY` | `false` | Use in-memory SQLite database |
| Socket Directory | `--socket-dir` | `PGSQLITE_SOCKET_DIR` | `/tmp` | Directory for Unix domain socket |
| No TCP | `--no-tcp` | `PGSQLITE_NO_TCP` | `false` | Disable TCP listener, use only Unix socket |

### SSL/TLS Configuration

| Option | CLI Flag | Environment Variable | Default | Description |
|--------|----------|---------------------|---------|-------------|
| SSL Enabled | `--ssl` | `PGSQLITE_SSL` | `false` | Enable SSL/TLS support (TCP only) |
| SSL Certificate | `--ssl-cert` | `PGSQLITE_SSL_CERT` | Auto-generated | Path to SSL certificate file |
| SSL Key | `--ssl-key` | `PGSQLITE_SSL_KEY` | Auto-generated | Path to SSL private key file |
| SSL CA | `--ssl-ca` | `PGSQLITE_SSL_CA` | None | Path to CA certificate file (optional) |
| SSL Ephemeral | `--ssl-ephemeral` | `PGSQLITE_SSL_EPHEMERAL` | `false` | Generate ephemeral certificates on startup |

## Performance Configuration

### Cache Settings

| Option | CLI Flag | Environment Variable | Default | Description |
|--------|----------|---------------------|---------|-------------|
| Row Description Cache Size | `--row-desc-cache-size` | `PGSQLITE_ROW_DESC_CACHE_SIZE` | `1000` | Number of RowDescription cache entries |
| Row Description Cache TTL | `--row-desc-cache-ttl` | `PGSQLITE_ROW_DESC_CACHE_TTL_MINUTES` | `10` | RowDescription cache TTL in minutes |
| Parameter Cache Size | `--param-cache-size` | `PGSQLITE_PARAM_CACHE_SIZE` | `500` | Number of parameter cache entries |
| Parameter Cache TTL | `--param-cache-ttl` | `PGSQLITE_PARAM_CACHE_TTL_MINUTES` | `30` | Parameter cache TTL in minutes |
| Query Cache Size | `--query-cache-size` | `PGSQLITE_QUERY_CACHE_SIZE` | `1000` | Number of query plan cache entries |
| Query Cache TTL | `--query-cache-ttl` | `PGSQLITE_QUERY_CACHE_TTL` | `600` | Query cache TTL in seconds |
| Execution Cache TTL | `--execution-cache-ttl` | `PGSQLITE_EXECUTION_CACHE_TTL` | `300` | Execution metadata TTL in seconds |
| Result Cache Size | `--result-cache-size` | `PGSQLITE_RESULT_CACHE_SIZE` | `100` | Number of result set cache entries |
| Result Cache TTL | `--result-cache-ttl` | `PGSQLITE_RESULT_CACHE_TTL` | `60` | Result cache TTL in seconds |
| Statement Pool Size | `--statement-pool-size` | `PGSQLITE_STATEMENT_POOL_SIZE` | `100` | Prepared statement pool size |
| Schema Cache TTL | `--schema-cache-ttl` | `PGSQLITE_SCHEMA_CACHE_TTL` | `300` | Schema cache TTL in seconds |
| Cache Metrics Interval | `--cache-metrics-interval` | `PGSQLITE_CACHE_METRICS_INTERVAL` | `300` | Cache metrics logging interval in seconds |

### Buffer Pool Configuration

| Option | CLI Flag | Environment Variable | Default | Description |
|--------|----------|---------------------|---------|-------------|
| Buffer Monitoring | `--buffer-monitoring` | `PGSQLITE_BUFFER_MONITORING` | `false` | Enable buffer pool monitoring |
| Buffer Pool Size | `--buffer-pool-size` | `PGSQLITE_BUFFER_POOL_SIZE` | `50` | Buffer pool size |
| Buffer Initial Capacity | `--buffer-initial-capacity` | `PGSQLITE_BUFFER_INITIAL_CAPACITY` | `4096` | Initial buffer capacity in bytes |
| Buffer Max Capacity | `--buffer-max-capacity` | `PGSQLITE_BUFFER_MAX_CAPACITY` | `65536` | Maximum buffer capacity in bytes |

### Memory Management

| Option | CLI Flag | Environment Variable | Default | Description |
|--------|----------|---------------------|---------|-------------|
| Auto Cleanup | `--auto-cleanup` | `PGSQLITE_AUTO_CLEANUP` | `false` | Enable automatic memory pressure response |
| Memory Monitoring | `--memory-monitoring` | `PGSQLITE_MEMORY_MONITORING` | `false` | Enable detailed memory monitoring |
| Memory Threshold | `--memory-threshold` | `PGSQLITE_MEMORY_THRESHOLD` | `67108864` | Memory threshold for cleanup (bytes) |
| High Memory Threshold | `--high-memory-threshold` | `PGSQLITE_HIGH_MEMORY_THRESHOLD` | `134217728` | High memory threshold (bytes) |
| Memory Check Interval | `--memory-check-interval` | `PGSQLITE_MEMORY_CHECK_INTERVAL` | `10` | Memory check interval in seconds |

### Memory Mapping

| Option | CLI Flag | Environment Variable | Default | Description |
|--------|----------|---------------------|---------|-------------|
| Enable Memory Mapping | `--enable-mmap` | `PGSQLITE_ENABLE_MMAP` | `false` | Enable memory mapping for large values |
| Min MMap Size | `--mmap-min-size` | `PGSQLITE_MMAP_MIN_SIZE` | `65536` | Minimum size for memory mapping (bytes) |
| Max MMap Memory | `--mmap-max-memory` | `PGSQLITE_MMAP_MAX_MEMORY` | `1048576` | Max in-memory size before temp files (bytes) |
| Temp Directory | `--temp-dir` | `PGSQLITE_TEMP_DIR` | System temp | Directory for temporary files |

## SQLite Configuration

### PRAGMA Settings

| Option | CLI Flag | Environment Variable | Default | Description |
|--------|----------|---------------------|---------|-------------|
| Journal Mode | `--pragma-journal-mode` | `PGSQLITE_JOURNAL_MODE` | `WAL` | SQLite journal mode (DELETE, TRUNCATE, PERSIST, MEMORY, WAL, OFF) |
| Synchronous Mode | `--pragma-synchronous` | `PGSQLITE_SYNCHRONOUS` | `NORMAL` | SQLite synchronous mode (OFF, NORMAL, FULL, EXTRA) |
| Cache Size | `--pragma-cache-size` | `PGSQLITE_CACHE_SIZE` | `-64000` | SQLite page cache size (negative = KB, positive = pages) |
| MMap Size | `--pragma-mmap-size` | `PGSQLITE_MMAP_SIZE` | `268435456` | SQLite memory-mapped I/O size in bytes |

## Schema Migration

| Option | CLI Flag | Environment Variable | Default | Description |
|--------|----------|---------------------|---------|-------------|
| Migrate | `--migrate` | N/A | `false` | Run pending migrations and exit |

## Usage Examples

### Command Line

```bash
# Basic usage with custom port
pgsqlite --port 5433 --database myapp.db

# In-memory database with SSL
pgsqlite --in-memory --ssl

# Aggressive caching for read-heavy workloads
pgsqlite \
  --query-cache-size 5000 \
  --result-cache-size 500 \
  --statement-pool-size 200

# Production-like settings with monitoring
pgsqlite \
  --database /data/prod.db \
  --pragma-journal-mode WAL \
  --pragma-synchronous FULL \
  --buffer-monitoring \
  --memory-monitoring \
  --auto-cleanup

# Unix socket only (no TCP)
pgsqlite --socket-dir /var/run/pgsqlite --no-tcp
```

### Environment Variables

```bash
# Basic configuration
export PGSQLITE_PORT=5433
export PGSQLITE_DATABASE=/data/myapp.db
export PGSQLITE_LOG_LEVEL=debug

# Enable SSL with custom certificates
export PGSQLITE_SSL=true
export PGSQLITE_SSL_CERT=/etc/pgsqlite/server.crt
export PGSQLITE_SSL_KEY=/etc/pgsqlite/server.key

# Performance tuning
export PGSQLITE_QUERY_CACHE_SIZE=5000
export PGSQLITE_RESULT_CACHE_SIZE=500
export PGSQLITE_STATEMENT_POOL_SIZE=200

# Run pgsqlite
pgsqlite
```

### Configuration File (.env)

Create a `.env` file in your working directory:

```bash
# Server settings
PGSQLITE_PORT=5432
PGSQLITE_DATABASE=./data/production.db
PGSQLITE_LOG_LEVEL=info

# Performance
PGSQLITE_JOURNAL_MODE=WAL
PGSQLITE_SYNCHRONOUS=NORMAL
PGSQLITE_QUERY_CACHE_SIZE=2000
PGSQLITE_RESULT_CACHE_SIZE=200

# SSL
PGSQLITE_SSL=true
PGSQLITE_SSL_CERT=./certs/server.crt
PGSQLITE_SSL_KEY=./certs/server.key

# Monitoring
PGSQLITE_BUFFER_MONITORING=1
PGSQLITE_MEMORY_MONITORING=1
```

## Performance Profiles

### Development Profile
```bash
pgsqlite --in-memory --log-level debug
```

### Testing Profile
```bash
pgsqlite \
  --in-memory \
  --pragma-synchronous OFF \
  --query-cache-size 100 \
  --result-cache-size 50
```

### Production Profile
```bash
pgsqlite \
  --database /data/prod.db \
  --pragma-journal-mode WAL \
  --pragma-synchronous NORMAL \
  --query-cache-size 5000 \
  --result-cache-size 500 \
  --statement-pool-size 200 \
  --auto-cleanup \
  --ssl
```

## Monitoring and Metrics

When monitoring is enabled, pgsqlite logs detailed metrics:

- **Cache hit rates**: Monitor effectiveness of query and result caching
- **Memory usage**: Track memory consumption and cleanup events
- **Buffer pool stats**: Understand buffer utilization
- **Connection metrics**: Track active connections and query patterns

Enable monitoring with:
```bash
pgsqlite \
  --memory-monitoring \
  --buffer-monitoring \
  --cache-metrics-interval 60
```