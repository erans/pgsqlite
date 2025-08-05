# pgsqlite Project Context

## Overview
pgsqlite is a PostgreSQL protocol adapter for SQLite databases, allowing PostgreSQL clients to connect to and query SQLite databases using the PostgreSQL wire protocol.

## Quick Reference

### Build & Test Commands
```bash
cargo build              # Build project
cargo test              # Run unit tests
cargo check             # Check for errors/warnings
./tests/runner/run_ssl_tests.sh  # Run integration tests (from project root)
```

**Pre-commit checklist**: Run ALL of these before committing:
1. `cargo check` - No errors or warnings
2. `cargo build` - Successful build
3. `cargo test` - All tests pass

### Development Workflow
1. Check TODO.md for prioritized tasks
2. Run full test suite after implementing features
3. Update TODO.md: mark completed tasks `[x]`, add new discoveries
4. Follow pre-commit checklist above

## Project Structure
```
src/
├── lib.rs              # Main library entry
├── protocol/           # PostgreSQL wire protocol
├── session/           # Session state management
├── query/             # Query execution handlers
└── migration/         # Schema migration system

tests/
├── runner/            # Test runner scripts
├── sql/               # SQL test files by category
└── output/            # Test outputs, temp databases
```

## Core Design Principles

### Type Inference
NEVER use column names to infer types. Use only:
- Explicit PostgreSQL type declarations in CREATE TABLE
- SQLite schema info via PRAGMA table_info
- Explicit type casts in queries (e.g., $1::int4)
- Value-based inference as last resort

### DateTime Storage
All datetime types use INTEGER storage (microseconds/days since epoch):
- DATE: INTEGER days since 1970-01-01
- TIME/TIMETZ: INTEGER microseconds since midnight
- TIMESTAMP/TIMESTAMPTZ: INTEGER microseconds since epoch
- INTERVAL: INTEGER microseconds

### Query Translation
- Full INSERT SELECT support with datetime/array translation
- Decimal aggregates: Only NUMERIC types need decimal_from_text wrapping
- FLOAT types (REAL, DOUBLE PRECISION) don't need wrapping
- Zero performance impact design for all translations

## Performance Targets

### Target (2025-07-27)
- SELECT: ~674.9x overhead (0.669ms)
- SELECT (cached): ~17.2x overhead (0.046ms) ✓
- UPDATE: ~50.9x overhead (0.059ms) ✓
- DELETE: ~35.8x overhead (0.034ms) ✓
- INSERT: ~36.6x overhead (0.060ms) ✓

### Current (2025-08-01) - SEVERE REGRESSION
- SELECT: ~389,541.9% overhead (4.016ms) - **599x worse than target**
- SELECT (cached): ~2,892.9% overhead (0.079ms) - **1.7x worse than target**
- UPDATE: ~4,591.1% overhead (0.053ms) - **90x worse than target**
- DELETE: ~3,560.5% overhead (0.033ms) - **100x worse than target**  
- INSERT: ~9,847.9% overhead (0.163ms) - **269x worse than target**

**Critical Issue**: Massive performance regression detected. Investigation and immediate optimization required.

### Performance Characteristics
- **Connection-per-session architecture** may be causing overhead
- **Debug logging in hot paths** needs to be reduced
- **Type detection improvements** may have introduced latency
- **Batch operations still provide best performance** (10x-50x speedup)

### Batch INSERT Best Practices
```sql
INSERT INTO table (col1, col2) VALUES 
  (val1, val2),
  (val3, val4);  -- 10-row: 11.5x speedup, 100-row: 51.3x speedup
```

## Schema Migrations

### Creating New Migrations
When modifying `__pgsqlite_*` tables:

1. Add to `src/migration/registry.rs`:
```rust
register_vX_your_feature(&mut registry);
```

2. Define migration:
```rust
fn register_vX_your_feature(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(X, Migration {
        version: X,
        name: "feature_name",
        description: "What this does",
        up: MigrationAction::Sql(r#"
            ALTER TABLE __pgsqlite_schema ADD COLUMN new_column TEXT;
        "#),
        down: Some(MigrationAction::Sql(r#"
            -- Rollback SQL
        "#)),
        dependencies: vec![X-1],
    });
}
```

3. Update Current Migrations list in this file

### Current Migrations
- v1: Initial schema
- v2: ENUM support
- v3: DateTime support
- v4: DateTime INTEGER storage
- v5: PostgreSQL catalog tables
- v6: VARCHAR/CHAR constraints
- v7: NUMERIC/DECIMAL constraints
- v8: Array support
- v9: Full-Text Search support
- v10: typcategory column in pg_type view

## Key Features & Fixes

### Recently Fixed (2025-08-05)
- **Schema-Based Type Inference for Empty Result Sets**: Fixed columns defaulting to TEXT when no data rows
  - All columns were incorrectly returning TEXT (OID 25) instead of proper PostgreSQL types
  - Implemented async schema lookup when queries return no data rows
  - Two-level fallback: alias resolution → table extraction from FROM clause
  - Uses `db.get_schema_type()` to fetch actual types from __pgsqlite_schema
  - Fixes SQLAlchemy lazy loading and relationship queries with proper type OIDs
- **Column Alias Type Inference**: Fixed incorrect PostgreSQL type OIDs for aliased columns
  - `SELECT users.id AS users_id` now returns INT4 (23) instead of TEXT (25)
  - Implemented `extract_source_table_column_for_alias()` to parse `table.column AS alias` patterns
  - Fixed SQLAlchemy compatibility where psycopg3 tried to parse strings as integers
  - Resolves "invalid literal for int() with base 10: 'Test User'" errors
- **Multi-Row INSERT RETURNING**: Fixed row count mismatch for bulk insert operations
  - SQLAlchemy multi-row INSERT with RETURNING now returns correct number of rows
  - Uses rowid range queries instead of just `last_insert_rowid()` for multiple rows
  - Fixes "Multi-row INSERT statement did not produce the correct number of INSERTed rows" error
- **Date Function Translation**: Fixed malformed SQL in datetime function translation
  - `func.date('now', '-30 days')` no longer creates invalid julianday syntax
  - Skip translation for parameterized date functions to prevent nested cast issues
  - Fixes "SQLite error: near 'AS': syntax error" in datetime queries
- **Aggregate Type Detection**: Fixed unknown PostgreSQL type OID 25 for numeric aggregates
  - AVG/SUM/COUNT on DECIMAL columns now return NUMERIC (1700) instead of TEXT (25)
  - Added query context to `get_aggregate_return_type_with_query()` for better type detection
  - Improved SQLAlchemy compatibility with aggregate functions on numeric columns
- **DateTime Conversion for psycopg3 Text Mode**: Fixed timestamp parsing errors with comprehensive query pattern support
  - psycopg3 text mode was receiving raw INTEGER microseconds like '1754404262713579' instead of formatted timestamps
  - Fixed table-prefixed aliases (`SELECT table.created_at AS alias`) by updating SIMPLE_SELECT_REGEX pattern
  - Fixed wildcard patterns (`SELECT table.*`) with session-based schema lookup for connection-per-session architecture
  - All query patterns now work: `SELECT *`, `SELECT col`, `SELECT table.*`, `SELECT table.col AS alias`
  - Prevents "timestamp too large (after year 10K)" errors in SQLAlchemy datetime queries

### Previously Fixed (2025-08-04)
- **Binary Protocol Support for psycopg3**: Implemented core binary format encoders
  - Added binary encoders for Numeric/Decimal, UUID, JSON/JSONB, Money types
  - PostgreSQL binary NUMERIC format with proper weight/scale/digit encoding
  - Test infrastructure updated to support psycopg2, psycopg3-text, psycopg3-binary drivers
  - SQLAlchemy tests can now run with `--driver psycopg3-binary` option

### Previously Fixed (2025-08-01)
- **SQLAlchemy MAX/MIN Aggregate Types**: Fixed "Unknown PG numeric type: 25" error
  - Added aggregate_type_fixer.rs to detect aliased aggregate columns
  - SQLite returns TEXT for MAX/MIN on TEXT columns storing decimals
  - Now properly returns NUMERIC (1700) for MAX/MIN on DECIMAL columns
  - Handles SQLAlchemy's alias patterns like "max_1"
- **Build Warnings**: Fixed all compilation warnings
  - Fixed unused variables/functions in simple_query_detector.rs
  - Fixed unused variant/fields in unified_processor.rs
  - All 372 unit tests now pass without warnings

### Previously Fixed (2025-07-29)
- **Connection-per-Session Architecture**: Implemented true connection isolation matching PostgreSQL behavior
  - Each client session gets its own SQLite connection
  - Fixes SQLAlchemy transaction persistence issues with WAL mode
  - Eliminates transaction visibility problems between sessions
  - Tests now use temporary files instead of :memory: for proper isolation
- **AT TIME ZONE Support**: Fixed simple_query protocol issues
  - Fixed UTF-8 encoding errors when using simple_query with AT TIME ZONE
  - AT TIME ZONE operator now properly returns float values
  - Tests updated to use prepared statements for reliable behavior
  - Added datetime translation support to LazyQueryProcessor
- **Test Infrastructure Stability**: Fixed migration lock contention and build system reliability
  - Resolved "Migration lock held by process" errors in concurrent tests
  - Updated test files to use unique temporary databases instead of shared `:memory:`
  - Fixed common test module compatibility with connection-per-session architecture
- **Logging Optimization**: Converted info to debug logging in hot paths
  - Changed query logging from info!() to debug!() level
  - Performance regression still present despite optimization

### Previously Fixed (2025-07-27)
- **UUID/NOW() Functions**: Fixed duplicate UUIDs and epoch timestamps in cached queries
- **SQLAlchemy ORM**: Full compatibility with VALUES clause conversion and datetime handling
- **DateTime Column Aliases**: Fixed "unable to parse date" errors in SELECT queries with aliases

### Major Features
- **Binary Protocol Support**: psycopg3 compatibility with binary format encoders
- **Connection Pooling**: Enable with `PGSQLITE_USE_POOLING=true`
- **SSL/TLS**: Use `--ssl` flag or `PGSQLITE_SSL=true`
- **40+ PostgreSQL Types**: Including arrays, JSON/JSONB, ENUMs
- **Full-Text Search**: PostgreSQL tsvector/tsquery with SQLite FTS5
- **psql Compatibility**: \d commands work via catalog tables

### JSON/JSONB Support
- All operators: `->`, `->>`, `@>`, `<@`, `#>`, `#>>`, `?`, `?|`, `?&`
- Functions: json_agg, json_object_agg, json_each, jsonb_set, jsonb_delete
- Row conversions: row_to_json, json_populate_record

### Array Support
- 30+ array types with JSON storage
- Operators: `@>`, `<@`, `&&`, `||`
- Functions: unnest(), array_agg() with DISTINCT/ORDER BY

## SQLAlchemy Compatibility

**Full SQLAlchemy ORM support** with transaction persistence and datetime handling:

```python
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine('postgresql://postgres@localhost:5432/main')
Session = sessionmaker(bind=engine)

# All SQLAlchemy operations work correctly:
# - Table creation, INSERT, UPDATE, DELETE with RETURNING
# - Complex JOINs with proper type inference  
# - Transaction management and persistence
# - Datetime operations with proper formatting
```

**Production Configuration**:
```bash
# Default configuration with connection-per-session architecture
# Each PostgreSQL client session gets its own SQLite connection
# Full SQLAlchemy compatibility with proper transaction isolation
pgsqlite --database mydb.db

# Journal mode options (both work with connection-per-session):
PGSQLITE_JOURNAL_MODE=WAL pgsqlite --database mydb.db    # Better performance
PGSQLITE_JOURNAL_MODE=DELETE pgsqlite --database mydb.db  # More conservative
```

**psycopg3 Binary Protocol Support**:
```bash
# Test with different PostgreSQL drivers
./tests/python/run_sqlalchemy_tests.sh                    # Default psycopg2
./tests/python/run_sqlalchemy_tests.sh --driver psycopg3-text
./tests/python/run_sqlalchemy_tests.sh --driver psycopg3-binary

# psycopg3 automatically uses binary format when beneficial
# Binary format provides better performance for:
# - Large binary data (BYTEA)
# - Numeric/Decimal values
# - UUID, JSON/JSONB data
# - Date/Time types
```

## Connection Pooling

Enable for concurrent workloads:
```bash
PGSQLITE_USE_POOLING=true \
PGSQLITE_POOL_SIZE=10 \
pgsqlite --database mydb.db
```

Environment variables:
- `PGSQLITE_USE_POOLING`: Enable pooling (default: false)
- `PGSQLITE_POOL_SIZE`: Max read connections (default: 5)
- `PGSQLITE_POOL_TIMEOUT`: Acquisition timeout seconds (default: 30)

## Quality Standards
- Test edge cases, not just happy paths
- Verify end-to-end functionality
- Only mark tasks complete when fully working
- No assumptions - test everything

## Known Limitations
- Array ORDER BY in array_agg relies on outer query ORDER BY
- Multi-array unnest (edge case)
- Some catalog queries and CAST operations still use get_mut_connection (needs update for per-session)

## Code Style
- Follow Rust conventions
- Use existing patterns
- Avoid comments unless necessary
- Keep code concise and idiomatic