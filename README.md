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

- **ENUM Types**: `CREATE TYPE status AS ENUM ('active', 'pending', 'archived')`
- **RETURNING Clauses**: `INSERT INTO users (email) VALUES ('test@example.com') RETURNING id`
- **CTEs**: `WITH` and `WITH RECURSIVE` queries
- **JSON Support**: `JSON` and `JSONB` types with PostgreSQL operators
- **Generated Columns**: `SERIAL` and `BIGSERIAL` auto-increment columns
- **VARCHAR/CHAR Constraints**: Length validation for `VARCHAR(n)` and `CHAR(n)` with proper padding
- **NUMERIC/DECIMAL Constraints**: Precision and scale validation for `NUMERIC(p,s)` and `DECIMAL(p,s)`
- **psql Compatibility**: Use psql's `\d` and `\dt` commands to explore your database

### Limitations

- ❌ Stored procedures and custom functions
- ❌ PostgreSQL-specific system functions (`pg_*`)
- ❌ Some advanced data types (ranges, geometric types, full-text search)
- ❌ Multiple concurrent writers (SQLite allows only one writer at a time)

For detailed compatibility information, see [Type Mapping Documentation](docs/type-mapping-prd.md).

## Performance Considerations

pgsqlite acts as a translation layer between PostgreSQL protocol and SQLite, which does add overhead:

- **Best for**: Development, testing, prototyping, and single-user applications or low write throughput applications
- **Typical overhead**: 40-300x vs raw SQLite depending on operation
- **Optimizations**: Built-in query caching, connection pooling, and prepared statements

For production use cases with high performance requirements, consider using native PostgreSQL.

## Advanced Topics

- **[Schema Migrations](docs/migrations.md)**: Automatic migration system for pgsqlite metadata
- **[SSL/TLS Setup](docs/ssl-setup.md)**: Secure connections configuration
- **[Unix Sockets](docs/unix-sockets.md)**: Lower latency local connections
- **[Performance Tuning](docs/performance-tuning.md)**: Cache configuration and optimization
- **[Architecture Overview](docs/architecture.md)**: How pgsqlite works internally
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
# Run all test suites
./run_ssl_tests.sh

# Run specific test mode
./run_ssl_tests.sh --mode tcp-ssl --verbose
```

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