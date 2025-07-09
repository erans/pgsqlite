# Schema Drift Detection

## Overview

pgsqlite maintains metadata about PostgreSQL-to-SQLite type mappings in the `__pgsqlite_schema` table. Schema drift occurs when the actual SQLite schema diverges from this metadata, which can happen when:

- SQLite tables are modified directly (bypassing pgsqlite)
- Failed migrations leave the database in an inconsistent state
- Manual ALTER TABLE operations are performed
- Columns are dropped without updating metadata

## How It Works

### Detection Process

When pgsqlite connects to an existing database, it automatically:

1. Checks the schema version (via migration system)
2. **Detects schema drift** by comparing:
   - Columns in `__pgsqlite_schema` vs actual SQLite schema (`PRAGMA table_info`)
   - PostgreSQL type mappings vs actual SQLite column types
   - Presence/absence of columns in both schemas

### Drift Types

The system detects three types of drift:

1. **Missing in SQLite**: Columns exist in metadata but not in the actual table
2. **Missing in Metadata**: Columns exist in SQLite but not tracked in metadata
3. **Type Mismatches**: Column types differ between metadata and SQLite schema

### Type Normalization

The detector normalizes SQLite types for comparison:
- `INT`, `INT4` → `INTEGER`
- `INT8`, `BIGINT` → `INTEGER`
- `FLOAT`, `DOUBLE` → `REAL`
- `VARCHAR`, `CHAR` → `TEXT`
- `BOOL`, `BOOLEAN` → `INTEGER`
- `BYTEA` → `BLOB`
- `NUMERIC`, `DECIMAL` → `DECIMAL`

## Error Handling

When drift is detected, pgsqlite will:

1. **Exit with an error** describing the drift
2. Provide a detailed report showing:
   - Which tables have drift
   - What columns are affected
   - Specific type mismatches

Example error message:
```
Schema drift detected:

Table 'users' has schema drift:
  Columns in metadata but missing from SQLite:
    - email (text)
  Type mismatches:
    - age expected SQLite type 'TEXT' but found 'INTEGER'

To fix this, ensure your SQLite schema matches the pgsqlite metadata.
```

## Resolution

To resolve schema drift:

### Option 1: Fix the SQLite Schema
```sql
-- Add missing columns
ALTER TABLE users ADD COLUMN email TEXT;

-- For type mismatches, you may need to recreate the column
-- SQLite doesn't support changing column types directly
```

### Option 2: Update the Metadata
```sql
-- Remove outdated metadata
DELETE FROM __pgsqlite_schema WHERE table_name = 'users' AND column_name = 'old_column';

-- Add new metadata
INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type)
VALUES ('users', 'new_column', 'text', 'TEXT');
```

### Option 3: Recreate the Database
If drift is extensive, it may be easier to:
1. Export your data
2. Create a fresh database with migrations
3. Import your data

## Prevention

To prevent schema drift:

1. **Always use pgsqlite** for schema modifications
2. **Use migrations** for schema changes (see [migrations.md](migrations.md))
3. **Avoid direct SQLite modifications** on pgsqlite-managed databases
4. **Test schema changes** thoroughly before deploying

## Technical Details

### Implementation

The drift detection is implemented in `src/schema_drift.rs` with:
- `SchemaDriftDetector`: Main detection logic
- `SchemaDrift`: Container for all detected drifts
- `TableDrift`: Drift information for a single table
- Type normalization for accurate comparison

### Performance

Schema drift detection:
- Runs only on database connection (not per-query)
- Uses efficient SQL queries to compare schemas
- Minimal overhead (typically < 10ms)

### Limitations

- Detection runs at startup only (not during runtime)
- Cannot detect semantic changes (e.g., constraint modifications)
- Type normalization may miss some edge cases
- No automatic fix option (manual intervention required)

## Future Enhancements

Potential improvements:
- [ ] Optional auto-fix for simple drift cases
- [ ] Warning mode (log but don't error)
- [ ] Runtime drift detection for long-running processes
- [ ] Schema versioning for better migration tracking
- [ ] Constraint and index drift detection