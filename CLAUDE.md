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

### Realistic Target
- **Goal**: Keep overhead within one order of magnitude (~10x) of pure SQLite
- **Note**: TCP adds expected network overhead; Unix sockets provide better performance

### Current Performance (2025-08-01) - After Optimizations

#### Unix Socket Performance (File-Based DB)
- SELECT: ~45,275% overhead (0.657ms) - Still needs work
- SELECT (cached): ~700% overhead (0.081ms) - **Within target ✓**
- UPDATE: ~2,480% overhead (0.062ms)
- DELETE: ~2,138% overhead (0.040ms)
- INSERT: ~1,934% overhead (0.072ms)

#### Optimization Progress
- **93.5% improvement** in SELECT (4.016ms → 0.657ms)
- **55.8% improvement** in INSERT with WAL optimization
- Cached queries now meet the ~10x target
- Connection thread affinity, removed debug logging, schema batching, and WAL optimization implemented

**Status**: Significant progress made. Cached queries meet target. Uncached operations still have high overhead due to PostgreSQL protocol translation costs.

### Performance Optimization Tips
1. **Use Unix Sockets** instead of TCP for ~35% better performance
2. **Enable Connection Pooling** with `PGSQLITE_USE_POOLING=true`
3. **Batch operations** provide 10x-50x speedup for bulk operations
4. **Cached queries** show dramatic improvement (up to 40x faster)
5. **Binary Format** - Performance optimized (fixed 2025-08-03)
   - SELECT: 10.5% faster with binary format ✅
   - INSERT/UPDATE/DELETE: Performance restored with RETURNING fix
   - Requires psycopg3 with `cursor(binary=True)`
   - Recommended for all workloads

### Optimized Configuration
```bash
# Best performance configuration
PGSQLITE_USE_POOLING=true \
PGSQLITE_POOL_SIZE=10 \
pgsqlite --database mydb.db

# Connect via Unix socket (psycopg2 example)
conn = psycopg2.connect(
    host='/tmp',  # Unix socket directory
    port=5432,
    dbname='main'
)
```

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

### Recently Fixed (2025-08-03)
- **Binary Format DML Performance Regression**: Fixed severe performance issue
  - DML operations (INSERT/UPDATE/DELETE) were 8.7x slower with binary format
  - Root cause: RETURNING clause was executing queries twice (DML + SELECT)
  - Solution: Implemented native SQLite RETURNING support for single execution
  - Performance improvement: 10.7x faster (1.39ms → 0.13ms)
  - Binary format now recommended for all workloads
- **Server Hanging After Binary Format Operations**: Fixed critical issue
  - Server would become unresponsive after handling binary format requests
  - Root cause: session.cleanup_connection().await was hanging
  - Temporary fix: Commented out cleanup call (needs proper implementation)
  - Binary format now works without hanging the server

### Recently Fixed (2025-08-02)
- **PostgreSQL Binary Wire Protocol**: Full implementation with psycopg3 compatibility
  - Fixed duplicate RowDescription issue causing protocol errors
  - Preserved client parameter types (INT2, FLOAT8) for proper binary decoding
  - Fixed double execution bug with INSERT...RETURNING statements
  - Added binary encoding in fast path for DataRow messages
  - Fixed field type detection in fast path (INT4 instead of INT8)
  - Performance: Severe regressions discovered - binary format 2.9x slower overall

### Recently Fixed (2025-08-01)
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
- **Binary Wire Protocol**: Full support for PostgreSQL binary format (psycopg3 compatible)
  - ✅ Performance regression FIXED: Binary format now comparable to text format
  - SELECT operations: 10.5% faster with binary format
  - DML operations: Performance restored with native RETURNING support
  - Use with psycopg3's `cursor(binary=True)` for optimal performance
  - Benchmark with `python benchmarks/benchmark.py --binary-format`
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