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
- **No automatic migrations**: Migrations are NOT run automatically on startup
- **Version checking**: Database schema version is checked on startup
- **Error on outdated schema**: If the database schema is outdated, pgsqlite will exit with an error message
- **Explicit migration**: Use `--migrate` command line flag to run pending migrations and exit

### Usage
```bash
# Run migrations on a database
pgsqlite --database mydb.db --migrate

# Normal operation (will fail if schema is outdated)
pgsqlite --database mydb.db
```

### Current Migrations
- **v1**: Initial schema (creates __pgsqlite_schema, metadata tables)
- **v2**: ENUM support (creates enum types, values, and usage tracking tables)

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

## Quality Standards
- Write tests that actually verify functionality, not tests that are designed to pass easily
- Only mark tasks as complete when they are actually finished and working
- Test edge cases and error conditions, not just happy paths
- Verify implementations work end-to-end, not just in isolation
- Don't claim something works without actually testing it

## Performance Characteristics
### Current Performance (as of 2025-07-06)
- **Overall System**: ~77x overhead vs raw SQLite
- **SELECT**: ~89x overhead
- **SELECT (cached)**: ~10x overhead (exceeds 10-20x target)
- **INSERT (single-row)**: ~165x overhead (use batch INSERTs for better performance)
- **UPDATE**: ~33x overhead (excellent)
- **DELETE**: ~37x overhead (excellent)

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

## Recent Major Features
- **PostgreSQL Type Support**: 40+ types including ranges, network types, binary types
- **ENUM Types**: Full PostgreSQL ENUM implementation with CREATE/ALTER/DROP TYPE
- **Zero-Copy Architecture**: Achieved 67% improvement in cached SELECT queries
- **System Catalog Support**: Basic pg_class and pg_attribute for psql compatibility
- **SSL/TLS Support**: Available for TCP connections with automatic certificate management

## Known Issues
- **BIT type casts**: Prepared statements with multiple columns containing BIT type casts may return empty strings
- **Array types**: Not yet implemented
- **System catalogs**: Limited to pg_class and pg_attribute, no JOIN support

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