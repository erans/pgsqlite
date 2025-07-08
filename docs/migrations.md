# Schema Migrations

pgsqlite uses an internal migration system to manage its metadata tables (`__pgsqlite_*`). This ensures smooth upgrades as the project evolves.

## How It Works

### Migration Behavior

1. **In-memory databases**: Migrations run automatically on startup (always start fresh)
2. **New file databases**: Migrations run automatically when creating a new database
3. **Existing file databases**: Schema version is checked on startup
   - If outdated, pgsqlite exits with an error
   - You must explicitly run migrations with `--migrate`

### Running Migrations

```bash
# Check if migrations are needed (will error if outdated)
pgsqlite --database mydb.db

# Example error message:
# Error: Failed to create database handler: Database schema is outdated. 
# Current version: 2, Required version: 4. Please run with --migrate to update the schema.

# Run pending migrations and exit
pgsqlite --database mydb.db --migrate

# After migration, run normally
pgsqlite --database mydb.db
```

## Current Migration Versions

| Version | Name | Description |
|---------|------|-------------|
| v1 | Initial schema | Creates core metadata tables (`__pgsqlite_schema`) |
| v2 | ENUM support | Adds ENUM type tracking tables |
| v3 | DateTime support | Adds datetime format and timezone columns |
| v4 | DateTime INTEGER storage | Converts datetime storage to INTEGER microseconds |

## Migration Safety

- All migrations run in transactions
- Automatic rollback on failure
- SHA256 checksums verify migration integrity
- Concurrent migrations prevented via locking
- Migration history tracked in `__pgsqlite_migrations`

## For Developers

When modifying pgsqlite's internal tables, you must create a migration:

1. Add to `src/migration/registry.rs`
2. Define migration with up/down SQL
3. Update CLAUDE.md with new migration info
4. Test with both new and existing databases

See the [development guide](../CLAUDE.md) for detailed instructions.