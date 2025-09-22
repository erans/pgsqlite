# pgsqlite Project Context

## Overview
pgsqlite is a PostgreSQL protocol adapter for SQLite databases, allowing PostgreSQL clients to connect to and query SQLite databases using the PostgreSQL wire protocol.

## Quick Reference

### Build & Test Commands
```bash
cargo build              # Build project
cargo test              # Run unit tests
cargo check             # Check for errors/warnings
cargo clippy            # Check for code quality issues
./tests/runner/run_ssl_tests.sh  # Run integration tests (from project root)
```

**Pre-commit checklist**: Run ALL of these before committing:
1. `cargo check` - No errors or warnings
2. `cargo clippy` - Review and fix warnings where reasonable
3. `cargo build` - Successful build
4. `cargo test` - All tests pass

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

## Performance Summary

### Driver Recommendations
- **psycopg3-binary**: Best overall performance (6.5x faster SELECT operations)
- **psycopg2**: Best for write-heavy workloads
- **psycopg3-text**: Balanced performance

### Performance Context
- ~80ms per operation vs 0.22ms pure SQLite (~360x overhead)
- Trade-off: Raw performance vs full PostgreSQL compatibility
- Suitable for web applications, APIs, and ORM workloads
- Use pure SQLite for high-frequency (>1000 ops/sec) or embedded systems

## Schema Migrations

### Creating New Migrations
When modifying `__pgsqlite_*` tables:

1. Add to `src/migration/registry.rs`: `register_vX_your_feature(&mut registry);`
2. Define migration with version, name, description, up/down SQL, and dependencies
3. Update Current Migrations list below

### Current Migrations (v1-v25)
- v1-v10: Initial schema, ENUM, DateTime, Arrays, Full-Text Search, catalog tables
- v15-v19: pg_depend, pg_proc, pg_description, pg_roles/pg_user, pg_stats
- v20-v25: information_schema support (routines, views, referential_constraints, check_constraints, triggers), pg_tablespace

## Major Features

### PostgreSQL Compatibility
- **Full PostgreSQL Catalog Support**: pg_constraint, pg_index, pg_depend, pg_proc, pg_description, pg_roles/pg_user, pg_stats, pg_tablespace
- **information_schema Support**: routines, views, referential_constraints, check_constraints, triggers
- **Session Functions**: current_user(), current_database(), current_schema(), current_schemas()
- **Permission Functions**: pg_has_role(), has_table_privilege() with security-aware handling
- **Binary Protocol Support**: psycopg3 compatibility with binary format encoders
- **40+ PostgreSQL Types**: Including arrays, JSON/JSONB, ENUMs, datetime types
- **CREATE INDEX with Operator Classes**: Support for varchar_pattern_ops, text_pattern_ops, etc.

### JSON/JSONB Support
- All operators: `->`, `->>`, `@>`, `<@`, `#>`, `#>>`, `?`, `?|`, `?&`
- Functions: json_agg, json_object_agg, json_each, jsonb_set, jsonb_delete
- Row conversions: row_to_json, json_populate_record

### Array Support
- 30+ array types with JSON storage and PostgreSQL binary protocol support
- Operators: `@>`, `<@`, `&&`, `||`
- Functions: unnest(), array_agg() with DISTINCT/ORDER BY
- Binary encoding: Full psycopg3 compatibility with proper array wire format

### Infrastructure
- **Connection-per-Session Architecture**: True connection isolation matching PostgreSQL
- **Full-Text Search**: PostgreSQL tsvector/tsquery with SQLite FTS5
- **Connection Pooling**: Enable with `PGSQLITE_USE_POOLING=true`
- **SSL/TLS**: Use `--ssl` flag or `PGSQLITE_SSL=true`
- **psql Compatibility**: \d commands work via catalog tables

## ORM Framework Compatibility

**Full ORM compatibility** achieved with comprehensive PostgreSQL catalog support:

### Key Capabilities
- **Constraint Introspection**: Django `inspectdb`, Rails ActiveRecord, SQLAlchemy automap, Ecto schema discovery
- **Index Management**: Complete index reflection for all major ORMs
- **Column Metadata**: Defaults, constraints, identity detection, generated columns
- **Sequence Detection**: Rails `pk_and_sequence_for`, Django SERIAL introspection
- **User/Role Management**: Enterprise authentication workflows via pg_roles/pg_user
- **Comment Support**: Table/column documentation via pg_description
- **Trigger Introspection**: Business logic discovery via information_schema.triggers
- **Function Metadata**: Available functions via information_schema.routines
- **View Metadata**: View discovery and analysis via information_schema.views
- **Statistics**: Query optimization via pg_stats

### Example ORM Patterns
```sql
-- Foreign key discovery (Django/Rails)
SELECT conname, contype, conrelid, confrelid FROM pg_constraint WHERE contype = 'f';

-- Index information (SQLAlchemy Inspector)
SELECT ic.relname, i.indnatts, i.indkey, i.indisunique FROM pg_index i JOIN pg_class ic ON i.indexrelid = ic.oid;

-- Column metadata with defaults
SELECT attname, attnotnull, atthasdef, attidentity FROM pg_attribute WHERE attnum > 0;

-- User management (Django/Rails auth)
SELECT rolname, rolsuper, rolcreatedb FROM pg_roles WHERE rolcanlogin = 't';
```

### SQLAlchemy Production Setup
```bash
# Full compatibility with connection-per-session architecture
pgsqlite --database mydb.db

# Driver options (all 8/8 tests pass):
./tests/python/run_sqlalchemy_tests.sh                     # psycopg2
./tests/python/run_sqlalchemy_tests.sh --driver psycopg3-text    # psycopg3-text
./tests/python/run_sqlalchemy_tests.sh --driver psycopg3-binary  # psycopg3-binary
```

## Connection Pooling
```bash
PGSQLITE_USE_POOLING=true PGSQLITE_POOL_SIZE=10 pgsqlite --database mydb.db
```
Environment variables: `PGSQLITE_USE_POOLING` (default: false), `PGSQLITE_POOL_SIZE` (default: 5), `PGSQLITE_POOL_TIMEOUT` (default: 30s)

## Quality Standards
- Test edge cases, not just happy paths
- Verify end-to-end functionality
- Only mark tasks complete when fully working
- Run pre-commit checklist before committing

## Known Limitations
- Array ORDER BY in array_agg relies on outer query ORDER BY
- Multi-array unnest (edge case)
- Some catalog queries still use get_mut_connection (needs per-session update)

## Code Style
- Follow Rust conventions and existing patterns
- Avoid comments unless necessary
- Keep code concise and idiomatic