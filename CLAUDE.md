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

### Current Performance (2025-08-01) with Unified Processor
- SELECT 1 (minimal): ~85x overhead (0.060ms) - **Protocol minimum**
- SELECT (cached): ~186x overhead (0.67ms) ✅
- UPDATE: ~150x overhead (0.15ms) ✅
- DELETE: ~120x overhead (0.12ms) ✅
- INSERT: ~180x overhead (0.18ms) ✅

**Note**: The 85x overhead for "SELECT 1" represents the absolute minimum due to PostgreSQL wire protocol over TCP/IP. This is not reducible without changing the fundamental architecture.

### Performance Characteristics
- **Protocol overhead dominates** simple queries (85x minimum)
- **Unified processor is 10-20% faster** than previous implementation
- **Complex queries benefit most** from optimizations
- **Batch operations provide best performance** (10x-50x speedup)

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

### Recently Fixed (2025-07-29)
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
  - Should help reduce performance overhead (pending benchmark validation)

### Previously Fixed (2025-07-27)
- **UUID/NOW() Functions**: Fixed duplicate UUIDs and epoch timestamps in cached queries
- **SQLAlchemy ORM**: Full compatibility with VALUES clause conversion and datetime handling
- **DateTime Column Aliases**: Fixed "unable to parse date" errors in SELECT queries with aliases

### Major Features
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