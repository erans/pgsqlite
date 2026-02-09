# Schema Migrations

pgsqlite uses an internal migration system to manage its metadata tables (`__pgsqlite_*`). This ensures smooth upgrades as the project evolves.

## How It Works

### Migration Behavior

Migrations are applied automatically when a database is opened:

1. **In-memory databases**: Migrations run automatically on startup (always start fresh).
2. **New file databases**: Migrations run automatically when creating a new database.
3. **Existing file databases**: Schema version is checked and any pending migrations are applied automatically on startup.

### Running Migrations

```bash
# Run pending migrations and exit
pgsqlite --database mydb.db --migrate

# After migration, run normally
pgsqlite --database mydb.db
```

Notes:

- In directory layout (`--database ./data`), `--migrate` applies to the *default database* file (`<data-dir>/<default-database>.db`).
- The current target schema version is the highest migration number registered in `src/migration/registry.rs`.

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
