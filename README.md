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