# pgsqlite Project Context

## Overview
pgsqlite is a PostgreSQL protocol adapter for SQLite databases. It allows PostgreSQL clients to connect to and query SQLite databases using the PostgreSQL wire protocol.

## Project Structure
- `src/` - Main source code directory
  - `lib.rs` - Main library entry point
  - `protocol/` - PostgreSQL wire protocol implementation
  - `session/` - Session state management
  - `query/` - Query execution handlers
- `tests/` - Test files
- `Cargo.toml` - Rust project configuration
- `TODO.md` - Comprehensive task list for future development

## Build Commands
- `cargo build` - Build the project
- `cargo test` - Run tests
- `cargo run` - Run the project

## Development Workflow
- After implementing any feature, always run the full test suite with `cargo test` to ensure nothing is broken
- **ALWAYS update TODO.md when completing work or discovering new tasks**:
  - Mark completed tasks with `[x]`
  - Add new discovered tasks or subtasks
  - Document partial progress with detailed notes
  - Update task descriptions if implementation reveals complexity
- Check TODO.md for prioritized tasks when planning development work
- Use TODO.md as the authoritative source for tracking all future work
- **NEVER commit code before ensuring ALL of the following pass**:
  - `cargo check` - No compilation errors or warnings
  - `cargo build` - Successfully builds the project
  - `cargo test` - All tests pass
  - If any of these fail, fix the issues before committing

## Code Style
- Follow Rust conventions
- Use existing imports and patterns
- Avoid adding comments unless necessary
- Keep code concise and idiomatic

## Schema Migration System
- **In-memory databases**: Migrations are run automatically on startup (since they always start fresh)
- **New file-based databases**: Migrations are run automatically when creating a new database file
- **Existing file-based databases**: Schema version is checked on startup
- **Error on outdated schema**: If an existing database schema is outdated, pgsqlite will exit with an error message
- **Explicit migration**: Use `--migrate` command line flag to run pending migrations and exit

### Usage
```bash
# In-memory databases (auto-migrate on startup)
pgsqlite --in-memory

# New database file (auto-migrate on first run)
pgsqlite --database newdb.db

# Run migrations on an existing file-based database
pgsqlite --database existingdb.db --migrate

# Normal operation with existing database (will fail if schema is outdated)
pgsqlite --database existingdb.db
```

### Current Migrations
- **v1**: Initial schema (creates __pgsqlite_schema, metadata tables)
- **v2**: ENUM support (creates enum types, values, and usage tracking tables)
- **v3**: DateTime support (adds datetime_format and timezone_offset columns to __pgsqlite_schema, creates datetime cache and session settings tables)
- **v4**: DateTime INTEGER storage (converts all datetime types to INTEGER microseconds/days for perfect precision)
- **v5**: PostgreSQL catalog tables (creates pg_class, pg_namespace, pg_am, pg_type, pg_attribute views; pg_constraint, pg_attrdef, pg_index tables)
- **v6**: VARCHAR/CHAR constraints (adds type_modifier to __pgsqlite_schema, creates __pgsqlite_string_constraints table)
- **v7**: NUMERIC/DECIMAL constraints (creates __pgsqlite_numeric_constraints table for precision/scale validation)
- **v8**: Array support (creates __pgsqlite_array_types table, updates pg_type view with typarray field)

### Creating New Migrations
**IMPORTANT**: When modifying internal pgsqlite tables (any table starting with `__pgsqlite_`), you MUST create a new migration:

1. **Add migration to registry** in `src/migration/registry.rs`:
   ```rust
   register_vX_your_feature(&mut registry);
   ```

2. **Define the migration function**:
   ```rust
   fn register_vX_your_feature(registry: &mut BTreeMap<u32, Migration>) {
       registry.insert(X, Migration {
           version: X,
           name: "your_feature_name",
           description: "Description of what this migration does",
           up: MigrationAction::Sql(r#"
               ALTER TABLE __pgsqlite_schema ADD COLUMN new_column TEXT;
               -- Other schema changes
           "#),
           down: Some(MigrationAction::Sql(r#"
               -- Rollback SQL if possible
           "#)),
           dependencies: vec![X-1], // Previous migration version
       });
   }
   ```

3. **For complex migrations** that need data transformation, use `MigrationAction::Combined` or `MigrationAction::Function`

4. **Update this file** to list the new migration in the "Current Migrations" section above

## Important Design Decisions
- **Type Inference**: NEVER use column names to infer types. Types should be determined from:
  - Explicit PostgreSQL type declarations in CREATE TABLE statements
  - SQLite schema information via PRAGMA table_info
  - Explicit type casts in queries (e.g., $1::int4)
  - Value-based inference only when schema information is unavailable

- **Decimal Query Rewriting**: 
  - Only NUMERIC types (stored as DECIMAL in SQLite) require decimal_from_text wrapping for aggregates
  - FLOAT types (REAL, DOUBLE PRECISION, FLOAT4, FLOAT8) should NOT be wrapped as they're already decimal-compatible
  - Correlated subqueries must inherit outer context to recognize outer table columns
  - Context merging is essential for proper type resolution in nested queries

- **DateTime Storage (INTEGER Microseconds)**:
  - All datetime types use INTEGER storage for perfect precision (no floating point errors)
  - Storage formats:
    - DATE: INTEGER days since epoch (1970-01-01)
    - TIME/TIMETZ: INTEGER microseconds since midnight
    - TIMESTAMP/TIMESTAMPTZ: INTEGER microseconds since epoch
    - INTERVAL: INTEGER microseconds
  - Microsecond precision matches PostgreSQL's maximum precision
  - Conversion implementation:
    - InsertTranslator converts datetime literals to INTEGER during INSERT/UPDATE
    - Fast path value converters transform INTEGER back to datetime strings during SELECT
    - Supports both single-row and multi-row INSERT statements
    - No triggers needed - all conversion happens in the query pipeline
  - Clients see proper PostgreSQL datetime formats via wire protocol

## Quality Standards
- Write tests that actually verify functionality, not tests that are designed to pass easily
- Only mark tasks as complete when they are actually finished and working
- Test edge cases and error conditions, not just happy paths
- Verify implementations work end-to-end, not just in isolation
- Don't claim something works without actually testing it

## Performance Characteristics
### Current Performance (as of 2025-07-08)
- **Overall System**: ~134x overhead vs raw SQLite (comprehensive benchmark results)
- **SELECT**: ~294x overhead (protocol translation overhead)
- **SELECT (cached)**: ~39x overhead (excellent caching performance)
- **INSERT (single-row)**: ~332x overhead (use batch INSERTs for better performance)
- **UPDATE**: ~48x overhead (excellent)
- **DELETE**: ~44x overhead (excellent)

### Performance Optimization Results (2025-07-08)
- **Cached SELECT performance**: 39x overhead (0.156ms) - excellent caching effectiveness
- **UPDATE/DELETE performance**: 44-48x overhead (0.044-0.048ms) - excellent
- **Cache effectiveness**: 1.9x speedup for repeated queries (0.294ms → 0.156ms)
- **Multi-row INSERT**: Dramatic performance improvements with batch operations
- **DateTime conversion**: Complete bidirectional conversion with minimal performance impact
- **Detection**: Regex-based patterns identify simple SELECT/INSERT/UPDATE/DELETE queries
- **Coverage**: Queries without PostgreSQL casts (::), datetime functions, JOINs, or complex expressions

### Batch INSERT Performance
Multi-row INSERT syntax provides dramatic improvements:
```sql
INSERT INTO table (col1, col2) VALUES 
  (val1, val2),
  (val3, val4),
  (val5, val6);
```
- 10-row batches: 11.5x speedup over single-row
- 100-row batches: 51.3x speedup
- 1000-row batch: 76.4x speedup

#### Best Practices for Batch INSERTs
1. **Optimal Batch Size**: 100-1000 rows per INSERT statement provides best performance
2. **Fast Path Optimization**: Simple batch INSERTs without datetime/decimal values use the ultra-fast path
3. **Prepared Statement Caching**: Batch INSERTs with same column structure share cached metadata
4. **Error Handling**: Batch operations are atomic - all rows succeed or all fail
5. **DateTime Values**: Use standard formats (YYYY-MM-DD, HH:MM:SS) to avoid conversion errors
6. **Memory Usage**: Very large batches (>10,000 rows) may require more memory
7. **Network Efficiency**: Reduces round trips between client and server

## Recent Major Features
- **PostgreSQL Type Support**: 40+ types including ranges, network types, binary types
- **ENUM Types**: Full PostgreSQL ENUM implementation with CREATE/ALTER/DROP TYPE
- **Zero-Copy Architecture**: Achieved 67% improvement in cached SELECT queries
- **System Catalog Support**: Full pg_class, pg_namespace, pg_am views and catalog tables for psql compatibility
- **SSL/TLS Support**: Available for TCP connections with automatic certificate management
- **Ultra-Fast Path Optimization (2025-07-08)**: 19% SELECT performance improvement via translation bypass
- **DateTime/Timezone Support (2025-07-07)**: INTEGER microsecond storage with full PostgreSQL compatibility
- **DateTime Value Conversion (2025-07-08)**: Complete bidirectional conversion between text and INTEGER storage
- **Multi-row INSERT Support (2025-07-08)**: Enhanced InsertTranslator to handle multi-row VALUES with datetime conversion
- **Comprehensive Performance Profiling (2025-07-08)**: Detailed pipeline metrics and optimization monitoring
- **Arithmetic Type Inference (2025-07-08)**: Smart type propagation for aliased arithmetic expressions
- **psql \d Command Support (2025-07-08)**: Full support for psql meta-commands \d and \dt through enhanced catalog system
- **Array Type Support (2025-07-12)**: Complete PostgreSQL array implementation with JSON storage
  - Support for 30+ array types (INTEGER[], TEXT[][], BOOLEAN[], etc.)
  - JSON-based storage with automatic validation constraints
  - Array literal conversion (ARRAY[1,2,3] and '{1,2,3}' formats)
  - Wire protocol array support with proper type OIDs
  - Multi-row INSERT with array values fully supported
  - Comprehensive test coverage in CI/CD pipeline
- **JSON/JSONB Support (2025-07-12)**: Full operator and function support with comprehensive documentation
  - All major operators: ->, ->>, @>, <@, #>, #>>
  - Core functions: json_valid, json_typeof, json_array_length, jsonb_object_keys
  - Manipulation functions: jsonb_set, json_extract_path, json_strip_nulls
  - Automatic operator translation in query pipeline
  - Full test coverage for operators and functions

## Known Issues
- **BIT type casts**: Prepared statements with multiple columns containing BIT type casts may return empty strings
- **Array operators**: Array-specific operators (ANY, ALL, @>, etc.) not yet implemented
- **Array functions**: Functions like unnest(), array_agg() not yet implemented
- **Array subscripts**: Array element access (arr[1]) not yet supported

## Database Handler Architecture
Uses a Mutex-based implementation for thread safety:
- Single `rusqlite::Connection` with `SQLITE_OPEN_FULL_MUTEX`
- `parking_lot::Mutex` for efficient synchronization
- Schema cache for performance
- Fast path optimization for simple queries

## SSL/TLS Configuration
Enable via command line or environment variables:
- `--ssl` / `PGSQLITE_SSL=true` - Enable SSL support
- `--ssl-cert` / `PGSQLITE_SSL_CERT` - Path to SSL certificate
- `--ssl-key` / `PGSQLITE_SSL_KEY` - Path to SSL private key
- `--ssl-ca` / `PGSQLITE_SSL_CA` - Path to CA certificate (optional)
- `--ssl-ephemeral` / `PGSQLITE_SSL_EPHEMERAL` - Generate ephemeral certificates

# important-instruction-reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.