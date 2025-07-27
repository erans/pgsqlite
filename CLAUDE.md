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

### Current (2025-07-27)
- SELECT: ~674.9x overhead (0.669ms)
- SELECT (cached): ~17.2x overhead (0.046ms) ✓
- UPDATE: ~50.9x overhead (0.059ms) ✓
- DELETE: ~35.8x overhead (0.034ms) ✓
- INSERT: ~36.6x overhead (0.060ms) ✓

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

## Key Features & Fixes

### Recently Fixed (2025-07-27)
- **UUID/NOW() Functions**: Fixed duplicate UUIDs and epoch timestamps in cached queries
- **SQLAlchemy ORM**: Full compatibility with VALUES clause conversion and datetime handling
- **DateTime Column Aliases**: Fixed "unable to parse date" errors in SELECT queries with aliases
- **Transaction Persistence**: Resolved WAL mode transaction isolation issues causing rollbacks to undo commits
- **Transaction Error Handling**: Fixed "cannot rollback - no transaction is active" and transaction leak errors
- **WAL Mode Durability**: Added automatic checkpoint after COMMIT to ensure transaction persistence
- **Performance**: Logging optimization reduced overhead significantly

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
# For guaranteed compatibility (recommended)
PGSQLITE_JOURNAL_MODE=DELETE pgsqlite --database mydb.db

# For performance with enhanced transaction handling
PGSQLITE_JOURNAL_MODE=WAL pgsqlite --database mydb.db
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

## Code Style
- Follow Rust conventions
- Use existing patterns
- Avoid comments unless necessary
- Keep code concise and idiomatic