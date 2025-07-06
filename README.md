# pgsqlite
## 🐘+🪶=<span style="color: red; font-size: 1.5em">♥</span>

<img width="150" src="./pgsqlite.png"/>

A PostgreSQL protocol adapter for SQLite databases. This project allows PostgreSQL clients to connect to and query SQLite databases using the PostgreSQL wire protocol.

> **⚠️ WARNING: Experimental Project**
> This is an experimental project and is not yet ready for production use. It is under active development and may contain bugs, incomplete features, or breaking changes.

## How It Works

**pgsqlite** acts as a translation layer between PostgreSQL clients and SQLite databases, creating the illusion that clients are connecting to a real PostgreSQL server:

### Core Architecture

1. **Protocol Compatibility**: Implements the complete PostgreSQL Wire Protocol v3, allowing any PostgreSQL client (psql, pgAdmin, language drivers) to connect seamlessly without modifications.

2. **Type System Translation**:
   - Maps SQLite's simple type system (NULL, INTEGER, REAL, TEXT, BLOB) to PostgreSQL's rich type system
   - Implements custom decimal handling for PostgreSQL NUMERIC types using rust_decimal
   - Maintains type consistency through query rewriting and value conversion

3. **SQL Translation & Rewriting**:
   - Converts PostgreSQL-specific SQL syntax to SQLite-compatible equivalents
   - Handles complex features like RETURNING clauses through simulation
   - Rewrites arithmetic operations for decimal types to maintain precision

4. **Metadata Simulation**:
   - Presents PostgreSQL-compatible schema information and system catalogs
   - Maintains PostgreSQL OIDs for type compatibility
   - Handles session management and prepared statements

The result is that PostgreSQL applications can operate on SQLite databases without knowing they're not connected to actual PostgreSQL.

## Schema Mapping and Performance Architecture

### PostgreSQL to SQLite Schema Translation

pgsqlite implements a sophisticated schema mapping system that maintains PostgreSQL compatibility while leveraging SQLite's simplicity:

#### Type System Bridge
- **PostgreSQL → SQLite Mapping**: Rich PostgreSQL types (100+ types) are mapped to SQLite's 5 storage classes (NULL, INTEGER, REAL, TEXT, BLOB)
- **Metadata Registry**: The `__pgsqlite_schema` table stores original PostgreSQL type information for each column
- **Bidirectional Translation**: Values are converted back to PostgreSQL format during query results
- **Custom Type Handling**: Special cases like DECIMAL use rust_decimal for precision, BOOLEAN converts 0/1 ↔ f/t

#### Schema Information Flow
```
PostgreSQL Client → Wire Protocol → Type Mapper → SQLite Schema
                                       ↓
                  Schema Cache ← __pgsqlite_schema ← SQLite Storage
```

## Performance

**Latest Performance Results (2025-07-06):**

```
Operation        | Overhead | Time (ms) | Note
================|==========|===========|==================
UPDATE          |    36x   |   0.042   | Excellent ⭐⭐
DELETE          |    42x   |   0.039   | Excellent ⭐⭐
SELECT (cached) |    17x   |   0.068   | Outstanding ⭐⭐⭐
SELECT          |   126x   |   0.126   | Protocol overhead
INSERT          |   172x   |   0.299   | Expected for 1-row
----------------+----------+-----------+------------------
OVERALL         |   ~95x   |     -     | Improved performance
```

CREATE operations are significantly slower as we also update the `__pgsqlite_schema` table, however those are siginificantly less frequent than other operations.


### Optimizations used in the project

• **Fast Path Execution** - Direct SQL execution for simple INSERT/UPDATE/DELETE queries, bypassing full parsing pipeline (achieves 1.0-1.5x overhead)

• **Multi-Level Caching Strategy**:
  - Query Plan Cache: LRU cache of parsed queries with metadata (1000 entries)
  - Statement Pool: Reusable SQLite prepared statements (100 statements, LRU eviction)
  - Execution Cache: Pre-computed metadata for query execution
  - Result Set Cache: Complete query results for repeated queries (100 entries, 60s TTL)
  - Schema Cache: In-memory table metadata with bulk preloading
  - RowDescription Cache: Field descriptions for SELECT queries (1000 entries, 10min TTL)

• **Zero-Copy Protocol Architecture**:
  - Memory-mapped values for large BLOB/TEXT data
  - Thread-safe buffer pooling with automatic recycling
  - Direct socket communication bypassing tokio-util framing
  - Intelligent message batching to reduce syscall overhead

• **Query Optimization**:
  - Case-insensitive query type detection using byte comparison (400,000x speedup)
  - Static string command tags for common cases (0/1 rows)
  - Decimal query rewriting only for tables with NUMERIC types
  - Binary protocol support for efficient data transfer

• **Architecture Simplification**:
  - Single consolidated executor eliminating code duplication
  - Mutex-based database handler (2.2-3.5x faster than channel-based)
  - Dynamic batch sizing based on result set size

• **Smart Type Handling**:
  - Cached type conversions and mappings
  - Optimized boolean conversion (0/1 → f/t)
  - Direct pass-through for non-decimal arithmetic
  - Fast integer formatting with itoa library (21% speedup)

• **Protocol Serialization Optimizations** (2025-07-06):
  - Eliminated unnecessary clones in batch row sending
  - Added itoa for faster integer-to-string conversion
  - Profiled and identified protocol overhead distribution

• **Small Value Optimization** (2025-07-06):
  - Zero-allocation handling for common values (booleans, 0, 1, -1)
  - Stack-based formatting for small integers and floats
  - Static references for boolean and empty string values
  - 8% improvement in cached SELECT queries, 3% in UPDATE/DELETE

These optimizations combined achieve **17x overhead for cached SELECT queries** and **~95x overall overhead**, with some operations (UPDATE/DELETE) reaching as low as **36-42x overhead**.

### Performance Monitoring
Run benchmarks to measure overhead:
```bash
# Comprehensive benchmark comparing pgsqlite vs raw SQLite
./benchmarks/benchmark.py

# Fast path effectiveness test
cargo test benchmark_fast_path -- --ignored --nocapture

# Cache effectiveness benchmark
cargo test benchmark_cache_effectiveness -- --ignored --nocapture

# Statement pool performance test
cargo test test_statement_pool_basic
```

The architecture prioritizes correctness and compatibility while providing multiple optimization layers for different query patterns. Through comprehensive executor consolidation and optimization, we achieved **10x overhead for cached SELECT queries** (exceeding the original 10-20x target) and **77x overall performance** - representing excellent results for a protocol adapter that provides full PostgreSQL compatibility for SQLite databases.

## Project Structure

```
pgsqlite/
├── src/
│   ├── lib.rs              # Main library entry point
│   ├── protocol/           # PostgreSQL Wire Protocol v3 implementation
│   │   ├── messages.rs     # Protocol message definitions
│   │   └── codec.rs        # Message encoding/decoding
│   ├── session/            # Session state management
│   │   ├── state.rs        # Session state and parameters
│   │   ├── pool.rs         # SQLite connection pooling
│   │   └── db_handler.rs   # Database connection handling
│   ├── query/              # Query execution handlers
│   │   ├── executor.rs     # Simple query execution
│   │   └── extended.rs     # Extended query protocol (prepared statements)
│   ├── translator/         # SQL translation between PostgreSQL and SQLite
│   │   ├── create_table_translator.rs  # CREATE TABLE statement translation
│   │   ├── json_translator.rs          # JSON function translation
│   │   └── returning_translator.rs     # RETURNING clause simulation
│   ├── types/              # Type mappings and conversions
│   │   ├── type_mapper.rs          # Bidirectional type mapping
│   │   ├── schema_type_mapper.rs   # Schema-aware type mapping
│   │   ├── value_converter.rs      # Value format conversion
│   │   └── decimal_handler.rs      # Custom decimal type handling
│   ├── rewriter/           # Query rewriting for compatibility
│   │   ├── decimal_rewriter.rs        # Decimal arithmetic rewriting
│   │   └── expression_type_resolver.rs # Type resolution for expressions
│   ├── functions/          # Custom function implementations
│   │   ├── uuid_functions.rs    # UUID generation and validation
│   │   ├── json_functions.rs    # JSON/JSONB functions
│   │   └── decimal_functions.rs # Decimal arithmetic functions
│   ├── catalog/            # PostgreSQL system catalog simulation
│   └── metadata/           # Schema metadata management
├── tests/                  # Test files
├── docs/                   # Documentation
│   └── type-mapping-prd.md # Type mapping design document
└── Cargo.toml             # Rust project configuration
```

## Type Mapping

pgsqlite implements a comprehensive type mapping system between PostgreSQL and SQLite. For detailed information about how types are mapped between the two systems, see our [Type Mapping PRD](docs/type-mapping-prd.md).

## Supported Features

### ✅ Supported
- Basic CRUD operations (CREATE, INSERT, SELECT, UPDATE, DELETE)
- PostgreSQL wire protocol communication
- SSL/TLS encryption for secure connections (TCP only)
- Unix domain socket support for local connections
- Type mapping for common PostgreSQL types:
  - Basic types: BOOLEAN, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE PRECISION
  - Text types: CHAR, VARCHAR, TEXT
  - Date/Time types: DATE, TIMESTAMP, TIME
  - Binary types: BYTEA
  - JSON types: JSON, JSONB
  - Network types: CIDR, INET, MACADDR, MACADDR8
  - Other types: UUID, MONEY, NUMERIC/DECIMAL, BIT, BIT VARYING
  - Range types: INT4RANGE, INT8RANGE, NUMRANGE
  - SERIAL/BIGSERIAL (mapped to AUTOINCREMENT)
  - **ENUM types**: Full PostgreSQL ENUM support with CREATE TYPE AS ENUM
- Type preservation through metadata registry

### Supported SQL Features
- **SELECT queries**: All standard SELECT features including JOINs, subqueries, GROUP BY, ORDER BY, LIMIT
- **Common Table Expressions (CTEs)**: WITH and WITH RECURSIVE queries
- **Subqueries**: Correlated and non-correlated subqueries in SELECT, FROM, and WHERE clauses
- **UNION/INTERSECT/EXCEPT**: Set operations
- **Window functions**: SQLite's window function support
- **Aggregate functions**: All standard aggregates (COUNT, SUM, AVG, MIN, MAX, etc.)
- **RETURNING clause**: Simulated support for INSERT/UPDATE/DELETE RETURNING
- **ENUM support**: CREATE TYPE AS ENUM, ALTER TYPE ADD VALUE, DROP TYPE
- **Type casting**: Both PostgreSQL cast operators (::) and CAST() syntax

### Supported PostgreSQL Functions

#### Custom Implementations
- **UUID Functions**: `gen_random_uuid()`, `uuid_generate_v4()`, `is_valid_uuid()`, `uuid_normalize()`
- **JSON/JSONB Functions**: `json_valid()`, `json_typeof()`, `jsonb_typeof()`, `json_array_length()`, `jsonb_array_length()`, `jsonb_object_keys()`, `to_json()`, `to_jsonb()`, `json_build_object()`, `json_extract_scalar()`, `jsonb_contains()`, `jsonb_contained()`

#### SQLite Built-in Functions
All SQLite functions are available, including:
- **Core**: `abs()`, `coalesce()`, `length()`, `lower()`, `upper()`, `substr()`, `replace()`, `trim()`, etc.
- **Date/Time**: `date()`, `time()`, `datetime()`, `strftime()`, `julianday()`
- **Aggregate**: `count()`, `sum()`, `avg()`, `min()`, `max()`, `group_concat()`
- **JSON**: SQLite's native JSON functions

### ❌ Not Yet Supported
- PostgreSQL system functions (`pg_*`, `current_database()`, `current_schema()`)
- PostgreSQL date/time functions (`now()`, `age()`, `extract()`, `date_part()`)
- Array functions (`array_agg()`, `unnest()`)
- Geometric types (POINT, LINE, LSEG, BOX, PATH, POLYGON, CIRCLE)
- Text search types (TSVECTOR, TSQUERY)
- Advanced date/time types (INTERVAL, TSRANGE, TSTZRANGE, DATERANGE)
- XML type
- Composite and domain types
- Array operations (arrays are stored as JSON strings)
- Advanced PostgreSQL features (stored procedures, triggers, etc.)

## Building and Testing

```bash
# Build the project
cargo build

# Run tests
cargo test

# Run the project
cargo run

# Run with in-memory database (for testing/benchmarking)
cargo run -- --in-memory

# Run with Unix socket support (creates socket in /tmp)
cargo run -- --socket-dir /tmp

# Run with Unix socket only (no TCP listener)
cargo run -- --socket-dir /tmp --no-tcp
```

### Integration Testing

pgsqlite includes comprehensive integration tests that verify PostgreSQL client compatibility across multiple connection modes:

```bash
# Run all integration tests (requires psql client)
./run_ssl_tests.sh

# Run specific connection mode tests
./run_ssl_tests.sh --mode tcp-ssl      # TCP with SSL (in-memory)
./run_ssl_tests.sh --mode tcp-no-ssl   # TCP without SSL (in-memory)
./run_ssl_tests.sh --mode unix-socket  # Unix socket (in-memory)
./run_ssl_tests.sh --mode file-ssl     # File database with SSL
./run_ssl_tests.sh --mode file-no-ssl  # File database without SSL

# Run with verbose output
./run_ssl_tests.sh --mode tcp-ssl --verbose

# Run with custom SQL test file
./run_ssl_tests.sh --sql-file my_tests.sql
```

The integration test suite (`test_queries.sql`) includes:
- Schema operations (CREATE TABLE with 40+ PostgreSQL types)
- Data manipulation (INSERT, UPDATE, DELETE)
- Complex queries (JOINs, CTEs, subqueries, window functions)
- Transaction tests (BEGIN, COMMIT, ROLLBACK)
- Type conversion verification
- System catalog queries

All integration tests are automatically run in CI/CD across all connection modes.

### In-Memory Mode

pgsqlite supports an in-memory SQLite database mode for testing and benchmarking purposes. This mode eliminates disk I/O overhead, making it ideal for measuring the pure protocol translation overhead.

```bash
# Start pgsqlite with in-memory database
cargo run -- --in-memory

# Or in release mode for benchmarking
cargo build --release
./target/release/pgsqlite --in-memory
```

**Note**: In-memory databases are ephemeral - all data is lost when the server stops. This mode is intended for testing and benchmarking only, not for production use.

### Unix Socket Support

pgsqlite supports Unix domain sockets for local connections, providing lower latency than TCP/IP:

```bash
# Connect via Unix socket with psql
psql -h /tmp -p 5432 -d your_database

# Connect with psycopg2
conn = psycopg2.connect(host='/tmp', port=5432, dbname='your_database')
```

The socket file is created as `.s.PGSQL.{port}` in the specified directory. Both TCP and Unix socket listeners run simultaneously by default, or you can use `--no-tcp` to disable TCP.

### SSL/TLS Support

pgsqlite supports SSL/TLS encryption for secure connections over TCP. SSL is not available for Unix socket connections.

#### Quick Start

```bash
# Enable SSL with auto-generated certificates
pgsqlite --ssl

# Use existing certificates
pgsqlite --ssl --ssl-cert /path/to/server.crt --ssl-key /path/to/server.key

# Generate ephemeral certificates (not saved to disk)
pgsqlite --ssl --ssl-ephemeral
```

#### Certificate Management

pgsqlite handles SSL certificates in the following priority order:

1. **Provided Certificates**: Use paths specified via `--ssl-cert` and `--ssl-key`
2. **File System Discovery**: Look for certificates next to the database file
   - For `mydb.sqlite`, looks for `mydb.crt` and `mydb.key`
3. **Auto-Generation**: Generate self-signed certificates if not found

#### Certificate Behavior

- **Memory Databases** (`:memory:`): Always use ephemeral in-memory certificates
- **File Databases with `--ssl-ephemeral`**: Generate temporary certificates (not saved)
- **File Databases without ephemeral**: Generate and save certificates next to database file

#### Connecting with SSL

```bash
# Connect with psql requiring SSL
psql "postgresql://localhost:5432/mydb?sslmode=require"

# Connect with Python (psycopg2)
import psycopg2
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="mydb",
    sslmode="require"
)
```

#### SSL Configuration Examples

```bash
# Use SSL with in-memory database (auto-generates ephemeral certificates)
pgsqlite --in-memory --ssl

# Use SSL with file database (generates and saves certificates if missing)
pgsqlite --database /path/to/data.db --ssl

# Use existing certificates from custom location
pgsqlite --ssl --ssl-cert /etc/pgsqlite/server.crt --ssl-key /etc/pgsqlite/server.key

# Environment variable configuration
export PGSQLITE_SSL=true
export PGSQLITE_SSL_CERT=/etc/pgsqlite/server.crt
export PGSQLITE_SSL_KEY=/etc/pgsqlite/server.key
pgsqlite
```

## Reporting Issues

We welcome bug reports and feature requests! When reporting an issue, please include:

1. **PostgreSQL SQL statements** that reproduce the issue
2. **Expected outcome** - what should happen
3. **Actual outcome** - what actually happened
4. **Error messages** if any

### Example Issue Report

```
Title: CREATE TABLE with NUMERIC(10,2) fails

PostgreSQL SQL:
CREATE TABLE prices (
    id SERIAL PRIMARY KEY,
    amount NUMERIC(10,2)
);

Expected: Table should be created successfully
Actual: Error: "Invalid type specification"
```

This helps us quickly understand and reproduce the issue.

## Configuration

pgsqlite can be configured through command line arguments or environment variables. Command line arguments take precedence over environment variables.

### Configuration Precedence
1. Command line arguments (highest priority)
2. Environment variables
3. Default values (lowest priority)

### Command Line Arguments

```bash
# Basic configuration
--port, -p <PORT>                # PostgreSQL port to listen on (default: 5432)
--database, -d <DATABASE>        # Path to SQLite database file (default: sqlite.db)
--log-level <LOG_LEVEL>          # Logging level (default: info)
--in-memory                      # Use in-memory SQLite database
--socket-dir <SOCKET_DIR>        # Directory for Unix domain socket (default: /tmp)
--no-tcp                         # Disable TCP listener, use only Unix socket

# Cache configuration
--row-desc-cache-size <SIZE>     # RowDescription cache entries (default: 1000)
--row-desc-cache-ttl <MINUTES>   # RowDescription cache TTL (default: 10)
--param-cache-size <SIZE>        # Parameter cache entries (default: 500)
--param-cache-ttl <MINUTES>      # Parameter cache TTL (default: 30)
--query-cache-size <SIZE>        # Query plan cache entries (default: 1000)
--query-cache-ttl <SECONDS>      # Query cache TTL (default: 600)
--execution-cache-ttl <SECONDS>  # Execution metadata TTL (default: 300)
--result-cache-size <SIZE>       # Result set cache entries (default: 100)
--result-cache-ttl <SECONDS>     # Result cache TTL (default: 60)
--statement-pool-size <SIZE>     # Prepared statement pool size (default: 100)
--cache-metrics-interval <SEC>   # Cache metrics logging interval (default: 300)
--schema-cache-ttl <SECONDS>     # Schema cache TTL (default: 300)

# Buffer pool configuration
--buffer-monitoring              # Enable buffer pool monitoring
--buffer-pool-size <SIZE>        # Buffer pool size (default: 50)
--buffer-initial-capacity <SIZE> # Initial buffer capacity (default: 4096)
--buffer-max-capacity <SIZE>     # Max buffer capacity (default: 65536)

# Memory configuration
--auto-cleanup                   # Enable automatic memory pressure response
--memory-monitoring              # Enable detailed memory monitoring
--memory-threshold <BYTES>       # Memory threshold for cleanup (default: 64MB)
--high-memory-threshold <BYTES>  # High memory threshold (default: 128MB)
--memory-check-interval <SEC>    # Memory check interval (default: 10)

# Memory mapping configuration
--enable-mmap                    # Enable memory mapping for large values
--mmap-min-size <BYTES>          # Min size for memory mapping (default: 64KB)
--mmap-max-memory <BYTES>        # Max in-memory size before temp files (default: 1MB)
--temp-dir <DIR>                 # Directory for temporary files

# SQLite PRAGMA settings
--pragma-journal-mode <MODE>     # SQLite journal mode (default: WAL)
--pragma-synchronous <MODE>      # SQLite synchronous mode (default: NORMAL)
--pragma-cache-size <SIZE>       # SQLite page cache size (default: -64000)
--pragma-mmap-size <BYTES>       # SQLite memory-mapped I/O size (default: 256MB)

# SSL/TLS configuration
--ssl                            # Enable SSL/TLS support (TCP only)
--ssl-cert <PATH>                # Path to SSL certificate file
--ssl-key <PATH>                 # Path to SSL private key file
--ssl-ca <PATH>                  # Path to CA certificate file (optional)
--ssl-ephemeral                  # Generate ephemeral certificates on startup
```

### Environment Variables

All command line arguments can also be set via environment variables by prefixing with `PGSQLITE_`:

```bash
# Basic configuration
PGSQLITE_PORT=5432
PGSQLITE_DATABASE=sqlite.db
PGSQLITE_LOG_LEVEL=info
PGSQLITE_IN_MEMORY=false
PGSQLITE_SOCKET_DIR=/tmp
PGSQLITE_NO_TCP=false

# Cache configuration
PGSQLITE_ROW_DESC_CACHE_SIZE=1000
PGSQLITE_ROW_DESC_CACHE_TTL_MINUTES=10
PGSQLITE_PARAM_CACHE_SIZE=500
PGSQLITE_PARAM_CACHE_TTL_MINUTES=30
PGSQLITE_QUERY_CACHE_SIZE=1000
PGSQLITE_QUERY_CACHE_TTL=600
PGSQLITE_EXECUTION_CACHE_TTL=300
PGSQLITE_RESULT_CACHE_SIZE=100
PGSQLITE_RESULT_CACHE_TTL=60
PGSQLITE_STATEMENT_POOL_SIZE=100
PGSQLITE_CACHE_METRICS_INTERVAL=300
PGSQLITE_SCHEMA_CACHE_TTL=300

# Buffer pool configuration
PGSQLITE_BUFFER_MONITORING=0        # Set to 1 to enable
PGSQLITE_BUFFER_POOL_SIZE=50
PGSQLITE_BUFFER_INITIAL_CAPACITY=4096
PGSQLITE_BUFFER_MAX_CAPACITY=65536

# Memory configuration
PGSQLITE_AUTO_CLEANUP=0             # Set to 1 to enable
PGSQLITE_MEMORY_MONITORING=0        # Set to 1 to enable
PGSQLITE_MEMORY_THRESHOLD=67108864
PGSQLITE_HIGH_MEMORY_THRESHOLD=134217728
PGSQLITE_MEMORY_CHECK_INTERVAL=10

# Memory mapping configuration
PGSQLITE_ENABLE_MMAP=0              # Set to 1 to enable
PGSQLITE_MMAP_MIN_SIZE=65536
PGSQLITE_MMAP_MAX_MEMORY=1048576
PGSQLITE_TEMP_DIR=/tmp

# SQLite PRAGMA settings
PGSQLITE_JOURNAL_MODE=WAL
PGSQLITE_SYNCHRONOUS=NORMAL
PGSQLITE_CACHE_SIZE=-64000
PGSQLITE_MMAP_SIZE=268435456

# SSL/TLS configuration
PGSQLITE_SSL=true                   # Set to true to enable SSL
PGSQLITE_SSL_CERT=/path/to/cert.pem # Path to SSL certificate
PGSQLITE_SSL_KEY=/path/to/key.pem   # Path to SSL private key
PGSQLITE_SSL_CA=/path/to/ca.pem     # Path to CA certificate (optional)
PGSQLITE_SSL_EPHEMERAL=true         # Set to true for ephemeral certificates
```

### Configuration File

Copy `.env.example` to `.env` and adjust values as needed:

```bash
cp .env.example .env
# Edit .env with your preferred settings
```

### Examples

```bash
# Run with custom port and in-memory database
pgsqlite --port 5433 --in-memory

# Run with environment variables
export PGSQLITE_PORT=5433
export PGSQLITE_LOG_LEVEL=debug
pgsqlite

# Run with aggressive caching
pgsqlite --query-cache-size 5000 --result-cache-size 500 --statement-pool-size 200

# Run with memory optimizations enabled
pgsqlite --enable-mmap --buffer-monitoring --auto-cleanup

# Run with custom SQLite settings
pgsqlite --pragma-journal-mode DELETE --pragma-synchronous FULL
```

## Documentation

For detailed information about specific features and implementation details, see:

- [Type Mapping](docs/type-mapping-prd.md) - PostgreSQL to SQLite type mappings
- [ENUM Type Implementation](docs/enum-type-implementation.md) - How ENUM types are implemented using triggers
- [Zero-Copy Architecture](docs/zero-copy-architecture.md) - Performance optimizations
- [System Catalog Research](docs/pg_catalog_research.md) - PostgreSQL catalog emulation

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.