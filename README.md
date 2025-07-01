# pgsqlite

<img width="150" src="./pgsqlite.png"/>

> **⚠️ WARNING: Experimental Project**  
> This is an experimental project and is not yet ready for production use. It is under active development and may contain bugs, incomplete features, or breaking changes.

A PostgreSQL protocol adapter for SQLite databases. This project allows PostgreSQL clients to connect to and query SQLite databases using the PostgreSQL wire protocol.

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

### Performance Optimization Strategy

pgsqlite achieves reasonable performance through a multi-layered optimization approach:

#### Fast Path Execution (~35-40x overhead vs raw SQLite)
**Conditions for fast path:**
- Simple queries: `SELECT/INSERT/UPDATE/DELETE` with basic WHERE clauses
- Simple WHERE predicates: `=`, `>`, `<`, `>=`, `<=`, `!=`, `<>`
- Single-table operations with column comparisons
- Tables without DECIMAL columns (avoids query rewriting)
- Parameterized queries ($1, $2, etc.) in extended protocol

**Fast path benefits:**
- Bypasses SQL parsing and AST manipulation
- Skips decimal arithmetic rewriting
- Direct SQLite execution with minimal overhead
- Cached schema lookups for decimal detection
- Optimized parameter binding for extended protocol

#### Query Plan Caching ✅ IMPLEMENTED
**Comprehensive query optimization with significant performance improvements:**

**Query Plan Cache Architecture:**
- **LRU Cache**: Stores parsed and analyzed query plans by normalized query text (1000 entries, 10min TTL)
- **Cached Metadata**: Table schemas, column types, and decimal detection results stored with plans
- **Rewrite Cache**: Pre-computed decimal query rewrites for tables with NUMERIC columns
- **Statement Pool**: Reuses SQLite prepared statements and metadata (100 statements, LRU eviction)

**Cache Benefits:**
- **Eliminates Re-parsing**: Skip expensive SQL parsing for repeated query patterns
- **Avoids Schema Lookups**: Table metadata cached with query plans, eliminating `__pgsqlite_schema` queries
- **Rewrite Optimization**: Decimal arithmetic rewriting computed once per unique query structure
- **Prepared Statement Optimization**: Statement metadata caching and parameter optimization

**Performance Results (2025-07-01):**
```
Uncached SELECT: ~190x overhead (0.193ms vs 0.001ms SQLite)
Cached SELECT: ~39x overhead (0.088ms vs 0.002ms SQLite)
Cache Speedup: 2.2x improvement for repeated queries
UPDATE: ~36x overhead (best DML operation)
INSERT: ~186x overhead (worst performer due to protocol overhead)
```

#### Full Query Pipeline (~190x overhead for uncached, ~39x for cached)
For complex queries that can't use fast path:
- Complete PostgreSQL SQL parsing with query plan caching
- Query rewriting for decimal arithmetic (cached when possible)
- Type-aware result processing with statement pool optimization
- Boolean value conversion
- Cached schema metadata lookups

#### Caching Strategy ✅ FULLY IMPLEMENTED
- **Schema Cache**: In-memory table metadata with bulk preloading and bloom filters
- **Decimal Table Cache**: Cached detection of tables requiring decimal rewriting  
- **Type Information**: Cached PostgreSQL type mappings for result formatting
- **Query Plan Cache**: LRU cache of parsed queries, metadata, and decimal rewrites (1000 entries)
- **Statement Pool**: Cached SQLite prepared statements with metadata (100 statements, LRU eviction)
- **Execution Cache**: Pre-computed metadata for query execution with type converters
- **Result Set Cache**: LRU cache for complete query results (100 entries, 60s TTL)
- **Cache Metrics**: Hit/miss tracking and periodic logging for monitoring

#### Where Overhead Comes From
1. **Protocol Translation** (~20-30%): PostgreSQL wire protocol encoding/decoding
2. **SQL Parsing & Rewriting** (~40-50%): Converting PostgreSQL SQL to SQLite-compatible queries
3. **Type Conversion** (~15-20%): Converting values between PostgreSQL and SQLite formats
4. **Schema Lookups** (~10-15%): Retrieving type metadata for proper result formatting

#### Performance Monitoring
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

The architecture prioritizes correctness and compatibility while providing multiple optimization layers for different query patterns. While the initial target of 10-20x overhead was not fully achieved, the current performance (35-40x for most operations) is reasonable for a protocol adapter that provides full PostgreSQL compatibility for SQLite databases.

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

## Advanced Features

### Binary Protocol Support ✅ IMPLEMENTED
pgsqlite now supports PostgreSQL's binary protocol for efficient data transfer:
- Binary encoding for common types: BOOLEAN, INT2/4/8, FLOAT4/8, BYTEA
- Automatic format detection from client preferences
- Correct FieldDescription format codes based on Portal settings
- Seamless fallback to text protocol when needed

### Zero-Copy Message Construction ✅ IMPLEMENTED
Optimized protocol message construction for reduced allocations:
- `ZeroCopyMessageBuilder` for efficient DataRow construction
- Direct buffer writing without intermediate allocations
- Support for batch message construction

### Result Set Caching ✅ IMPLEMENTED
Intelligent caching of complete query results:
- Automatic caching for queries taking >1ms or returning >10 rows
- Cache key includes query text and parameter values
- 100 entry LRU cache with 60-second TTL
- Automatic invalidation on DDL statements
- 2.2x speedup for cached queries

## Supported Features

### ✅ Supported
- Basic CRUD operations (CREATE, INSERT, SELECT, UPDATE, DELETE)
- PostgreSQL wire protocol communication
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
- Type preservation through metadata registry

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

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.