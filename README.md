# pgsqlite
## 🐘+🪶=<span style="color: red; font-size: 1.5em">♥</span>

<img width="150" src="./pgsqlite.png"/>

**PostgreSQL protocol for SQLite databases.** Turn any SQLite database into a PostgreSQL server that your existing tools and applications can connect to.

> **⚠️ WARNING: Experimental Project**
> This is an experimental project and is not yet ready for production use. It is under active development and may contain bugs, incomplete features, or breaking changes.

## Why pgsqlite?

**pgsqlite** lets you use PostgreSQL tools and libraries with SQLite databases. This is perfect for:

- **🚀 Rapid Testing & CI/CD**: Run integration tests without spinning up PostgreSQL. Just copy your SQLite file and go.
- **🌿 Feature Branch Deployments**: Each branch gets its own database. Just copy the SQLite file - no complex database provisioning.
- **🤖 AI Agent Development**: Multiple agents can work on isolated sandbox environments with zero setup. Clone the database instantly.
- **💻 Local Development**: Use your favorite PostgreSQL tools (psql, pgAdmin, DataGrip) with lightweight SQLite storage.
- **🔧 Migration Path**: Prototype with SQLite, seamlessly move to PostgreSQL later without changing application code.

## Quick Start

### Installation

**Option 1: Download Pre-built Binaries (Recommended)**

Visit the [GitHub Releases page](https://github.com/erans/pgsqlite/releases) to download the latest pre-built binary for your platform:

```bash
# Example for Linux x64:
wget https://github.com/erans/pgsqlite/releases/latest/download/pgsqlite-linux-x64.tar.gz
tar -xzf pgsqlite-linux-x64.tar.gz
chmod +x pgsqlite
./pgsqlite
```

**Option 2: Build from Source**

```bash
# Clone and build from source
git clone https://github.com/erans/pgsqlite
cd pgsqlite
cargo build --release
./target/release/pgsqlite
```

### Basic Usage

1. **Start pgsqlite with a SQLite database:**
```bash
# Use an existing SQLite database
pgsqlite --database ./my-database.db

# Or start with an in-memory database for testing
pgsqlite --in-memory
```

2. **Connect with any PostgreSQL client:**
```bash
# Using psql
psql -h localhost -p 5432 -d my-database

# Using connection string
psql "postgresql://localhost:5432/my-database"
```

3. **Use it just like PostgreSQL:**
```sql
-- Create tables with PostgreSQL syntax
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert data
INSERT INTO users (email) VALUES ('user@example.com');

-- Query with PostgreSQL functions
SELECT * FROM users WHERE created_at > NOW() - INTERVAL '7 days';
```

## Usage Examples

### For Testing Environments

```bash
# Copy your template database for each test run
cp template.db test-1.db
pgsqlite --database test-1.db --port 5433 &

# Run your tests against it
npm test -- --database-url postgresql://localhost:5433/test-1

# Cleanup is just removing the file
rm test-1.db
```

### For Feature Branch Deployments

```bash
# Each branch gets its own database copy
cp main.db feature-branch-123.db
pgsqlite --database feature-branch-123.db --port 5433
```

### Connect from Your Application

**Python (psycopg2):**
```python
import psycopg2
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="myapp"
)
```

**Node.js (pg):**
```javascript
const { Client } = require('pg')
const client = new Client({
  host: 'localhost',
  port: 5432,
  database: 'myapp'
})
```

**Any PostgreSQL-compatible ORM:** Works with SQLAlchemy, Django ORM, ActiveRecord, Prisma, etc.

## Configuration

### Essential Options

```bash
# Basic options
pgsqlite \
  --database <path>     # SQLite database file (default: sqlite.db)
  --port <port>         # PostgreSQL port (default: 5432)
  --in-memory           # Use in-memory database

# Security
pgsqlite \
  --ssl                 # Enable SSL/TLS encryption
  --ssl-cert <path>     # Custom SSL certificate
  --ssl-key <path>      # Custom SSL key

# Performance
pgsqlite \
  --journal-mode WAL    # Enable WAL mode for better concurrency

# Connection Pooling (for concurrent workloads)
PGSQLITE_USE_POOLING=true pgsqlite \
  --database <path>     # Enable read/write connection separation
```

For all configuration options, see the [Configuration Reference](docs/configuration.md).

## Features

### PostgreSQL Compatibility

- ✅ **Wire Protocol**: Full PostgreSQL v3 protocol implementation
- ✅ **Clients**: Works with psql, pgAdmin, DBeaver, and all PostgreSQL drivers
- ✅ **SQL Syntax**: Most PostgreSQL queries work without modification
- ✅ **Data Types**: 40+ PostgreSQL types including SERIAL, JSON, UUID, arrays (stored as JSON)
- ✅ **Transactions**: Full ACID compliance via SQLite

### Notable Features

- **Connection Pooling**: Optional read/write connection separation for improved concurrent performance (enabled via `PGSQLITE_USE_POOLING=true`)
- **Query Optimization System**: Advanced optimization infrastructure with context merging, lazy schema loading, pattern recognition, and integrated optimization management
- **PostgreSQL Functions**: Comprehensive function support including:
  - **String Functions**: `split_part()`, `string_agg()`, `translate()`, `ascii()`, `chr()`, `repeat()`, `reverse()`, `left()`, `right()`, `lpad()`, `rpad()`
  - **Math Functions**: `trunc()`, `round()`, `ceil()`, `floor()`, `sign()`, `abs()`, `mod()`, `power()`, `sqrt()`, `exp()`, `ln()`, `log()`, trigonometric functions, `random()`
- **Array Types**: Full support for PostgreSQL arrays (e.g., `INTEGER[]`, `TEXT[][]`) with ARRAY literal syntax, ALL operator, and unnest() WITH ORDINALITY
- **JSON Support**: Complete `JSON` and `JSONB` implementation with operators (`->`, `->>`, `@>`, `<@`, `#>`, `#>>`, `?`, `?|`, `?&`) and functions (json_agg, json_object_agg, row_to_json, json_populate_record, json_to_record, jsonb_insert, jsonb_delete, jsonb_pretty, etc.)
- **Full-Text Search**: Complete PostgreSQL FTS implementation with `tsvector`/`tsquery` types, `@@` operator, `to_tsvector()`, `to_tsquery()`, `plainto_tsquery()` functions using SQLite FTS5 backend
- **ENUM Types**: `CREATE TYPE status AS ENUM ('active', 'pending', 'archived')`
- **RETURNING Clauses**: `INSERT INTO users (email) VALUES ('test@example.com') RETURNING id`
- **CTEs**: `WITH` and `WITH RECURSIVE` queries
- **Generated Columns**: `SERIAL` and `BIGSERIAL` auto-increment columns
- **VARCHAR/CHAR Constraints**: Length validation for `VARCHAR(n)` and `CHAR(n)` with proper padding
- **NUMERIC/DECIMAL Constraints**: Precision and scale validation for `NUMERIC(p,s)` and `DECIMAL(p,s)`
- **CREATE INDEX with Operator Classes**: Support for PostgreSQL operator classes like `varchar_pattern_ops`, `text_pattern_ops` (mapped to SQLite `COLLATE BINARY` for pattern matching optimization)
- **psql Compatibility**: Enhanced psql support with `\d`, `\dt`, and `\d tablename` commands fully working

### Limitations

- ❌ Stored procedures and custom functions
- ❌ PostgreSQL-specific system functions (`pg_*`)
- ❌ Some advanced data types (ranges, geometric types)
- ⚠️  Some advanced array features (array assignment operations, advanced indexing)
- ❌ Multiple concurrent writers (SQLite allows only one writer at a time, mitigated by connection pooling for reads)

For detailed compatibility information, see [Type Mapping Documentation](docs/type-mapping-prd.md).

## Performance Considerations

pgsqlite acts as a translation layer between PostgreSQL protocol and SQLite, providing full PostgreSQL compatibility with measurable overhead:

### Real-World Performance (2025-09-20)

**Driver Performance Comparison** (100 operations each):
| Driver | SELECT | INSERT | UPDATE | DELETE | Best For |
|--------|--------|--------|--------|--------|----------|
| **psycopg3-binary** | 0.452ms | 0.976ms | 0.219ms | 0.176ms | **Read-heavy** workloads |
| **psycopg3-text** | 0.925ms | 1.067ms | 0.304ms | 0.271ms | **Balanced** usage |
| **psycopg2** | 2.939ms | 0.214ms | 0.089ms | 0.063ms | **Write-heavy** workloads |

**Overhead vs Pure SQLite** (200 operations):
- **Pure SQLite**: 44.4ms (0.22ms per operation) - Maximum speed
- **pgsqlite**: ~16 seconds (~80ms per operation) - **~360x overhead**
- **Trade-off**: Raw performance vs full PostgreSQL compatibility + ORM support

### When pgsqlite is the Right Choice
- **Web applications**: 80ms database operations feel instant to users
- **ORM integration**: Django, SQLAlchemy, Rails, Ecto work seamlessly
- **Development/testing**: Full PostgreSQL feature compatibility
- **API endpoints**: Database time typically 10-20% of total request time

### Performance Optimizations
- **Protocol choice**: Binary mode (psycopg3-binary) is 3.1% faster than text mode
- **Batch operations**: Multi-row INSERT provides dramatic improvements:
  - 10-row batches: ~11x faster than single-row INSERTs
  - 100-row batches: ~51x faster
  - 1000-row batches: ~76x faster
- **Connection architecture**: Connection-per-session provides excellent isolation
- **Ultra-fast path**: Optimized execution for simple SELECT queries

For applications requiring microsecond-level performance, use pure SQLite. For PostgreSQL compatibility with acceptable overhead, pgsqlite is ideal.

### Connection Pooling

For concurrent read-heavy workloads, enable connection pooling to improve performance:

```bash
# Enable connection pooling with default settings (5 connections)
PGSQLITE_USE_POOLING=true pgsqlite --database mydb.db

# Custom pool configuration
PGSQLITE_USE_POOLING=true \
PGSQLITE_POOL_SIZE=10 \
PGSQLITE_POOL_TIMEOUT=60 \
pgsqlite --database mydb.db
```

**When to use connection pooling:**
- ✅ Multiple concurrent clients with read-heavy workloads
- ✅ TCP connections with sustained connection patterns
- ✅ Applications with frequent SELECT queries
- ❌ Single-client applications or simple scripts
- ❌ Memory-constrained environments
- ❌ Unix socket connections with low concurrency

**Configuration options:**
- `PGSQLITE_POOL_SIZE` - Maximum connections in read pool (default: 5)
- `PGSQLITE_POOL_IDLE_TIMEOUT` - Idle connection timeout in seconds (default: 300)
- `PGSQLITE_POOL_HEALTH_INTERVAL` - Health check interval in seconds (default: 60)

Connection pooling automatically routes SELECT queries to the read pool while directing write operations (INSERT/UPDATE/DELETE) to the primary connection for consistency.

## Advanced Topics

- **[Schema Migrations](docs/migrations.md)**: Automatic migration system for pgsqlite metadata
- **[SSL/TLS Setup](docs/ssl-setup.md)**: Secure connections configuration
- **[Unix Sockets](docs/unix-sockets.md)**: Lower latency local connections
- **[Performance Tuning](docs/performance-tuning.md)**: Cache configuration and optimization
- **[Architecture Overview](docs/architecture.md)**: How pgsqlite works internally
- **[Array Support](docs/array-support.md)**: Comprehensive guide to PostgreSQL arrays  
- **[JSON/JSONB Support](docs/json-support.md)**: Comprehensive guide to JSON functionality
- **[Full-Text Search](docs/fts_implementation_plan.md)**: PostgreSQL FTS implementation details
- **[Future Features](docs/future-features.md)**: Roadmap for enhanced SQLite capabilities

## Development

### Building from Source

```bash
# Clone the repository
git clone https://github.com/erans/pgsqlite
cd pgsqlite

# Build
cargo build --release

# Run tests
cargo test

# Run with debug logging
RUST_LOG=debug ./target/release/pgsqlite
```

### Running Integration Tests

```bash
# Run all test suites (includes comprehensive JSON/array function testing)
./tests/runner/run_ssl_tests.sh

# Run specific test mode
./tests/runner/run_ssl_tests.sh --mode tcp-ssl --verbose

# Run unit tests
cargo test
```

The test suite includes comprehensive validation of all JSON and array functions across multiple connection modes (TCP with/without SSL, Unix sockets, file-based databases).

### Contributing

We welcome contributions! When reporting issues, please include:

1. The SQL query that caused the problem
2. Expected behavior
3. Actual behavior
4. Any error messages

See [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines.

## Documentation

- [Type Mapping Reference](docs/type-mapping-prd.md) - Detailed PostgreSQL to SQLite type mappings
- [Configuration Reference](docs/configuration.md) - All configuration options
- [Architecture Overview](docs/architecture.md) - Technical deep dive
- [Performance Analysis](docs/performance.md) - Detailed benchmarks and optimization strategies

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.