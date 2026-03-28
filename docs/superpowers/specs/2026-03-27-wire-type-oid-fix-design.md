# Wire Protocol Type OID Fix

**Issue**: [#68](https://github.com/erans/pgsqlite/issues/68) - Wire protocol: type oid is incorrect for integer and float types
**Date**: 2026-03-27
**Scope**: Fix fallback type OID inference for pre-existing SQLite tables that lack `__pgsqlite_schema` metadata

## Problem

When returning query results, pgsqlite sends OID 25 (TEXT) for integer, bigint, smallint, boolean, and float columns. This happens because the type inference chain defaults to TEXT when `__pgsqlite_schema` has no entry for a column.

Tables created through pgsqlite's `CREATE TABLE` already work correctly — they populate `__pgsqlite_schema` with PG type names. But pre-existing SQLite tables (or tables created by other tools) have no metadata, so every column gets OID 25.

PostgreSQL client libraries use the wire OID to deserialize values into the correct host language type (e.g., Python `int`, Go `int64`). With OID 25, all values arrive as strings.

## Root Cause

Multiple code paths default to OID 25 when `__pgsqlite_schema` has no entry:

- **executor.rs line 1249**: final fallback in FieldDescription builder
- **extended.rs lines 851, 855, 899, 903**: Parse-time `inferred_types` construction — when `get_schema_type_with_session` returns `None` for source table.column lookups
- **extended.rs lines 4487-4511**: Execute-time `schema_types` population — same `get_schema_type_with_session` fallback

The codebase already has `sqlite_type_info.rs` which contains a complete `PRAGMA table_info` → PG OID mapping, but it's never called from the main query paths.

## Design

### New DbHandler method

**File**: `src/session/db_handler.rs`

Add `get_column_types_from_pragma` — uses `with_session_connection` to run `PRAGMA table_info(<table>)` and returns `HashMap<String, String>` mapping column names to PG type name strings. Internally calls `sqlite_type_info::sqlite_type_to_pg_type_name` to convert SQLite declared types to PG type names.

```rust
pub async fn get_column_types_from_pragma(
    &self,
    session_id: &Uuid,
    table_name: &str,
) -> Result<HashMap<String, String>, PgSqliteError>
```

Returns PG type name strings (e.g., `"integer"`, `"double precision"`, `"boolean"`) so callers can insert them directly into `schema_types` HashMaps.

### New helper function

**File**: `src/types/sqlite_type_info.rs`

Add `sqlite_type_to_pg_type_name` that maps SQLite declared types to PG type name strings. Uses the same logic as the existing `sqlite_type_to_pg_oid` but returns `&'static str`:

- `"INTEGER"` → `"integer"`
- `"INT8"` / `"BIGINT"` → `"bigint"`
- `"INT2"` / `"SMALLINT"` → `"smallint"`
- `"REAL"` / `"FLOAT"` / `"DOUBLE"` → `"double precision"`
- `"BOOLEAN"` / `"BOOL"` → `"boolean"`
- `"TEXT"` / `"VARCHAR"` / `"CHAR"` → `"text"`
- `"BLOB"` → `"bytea"`
- `"DATE"` → `"date"`
- `"TIMESTAMP"` → `"timestamp"`
- `"NUMERIC"` / `"DECIMAL"` → `"numeric"`
- Default → `"text"`

### Simple query protocol fix

**File**: `src/query/executor.rs`

In the `schema_types` population loop (lines 1088-1155), after attempting `get_schema_type_with_session` for each column and finding no match, fall back to PRAGMA-based lookup. Fetch PRAGMA results once per table (before the column loop) and cache in a local `HashMap`. For each column without schema metadata, look up its type in the PRAGMA cache and insert the PG type name into `schema_types`.

This ensures the final fallback at line 1249 is rarely reached — only for computed expressions or columns not in any table.

### Extended query protocol fix

**File**: `src/query/extended.rs`

Three locations need the PRAGMA fallback:

**1. Parse-time `schema_types` population (lines 563-640):** After `get_schema_type_with_session` returns `None`, fall back to PRAGMA lookup. Same pattern as executor.rs.

**2. Parse-time `inferred_types` construction (lines 654-912):** This is the critical path for clients using the extended protocol (e.g., psycopg3). The fallback chain at lines 848-903 calls `get_schema_type_with_session` multiple times via alias resolution. At each `Ok(None)` branch (lines 851, 899) and `Err` branch (lines 855, 903), instead of pushing `PgType::Text.to_oid()`, call `get_column_types_from_pragma` and use `pg_type_string_to_oid` to get the correct OID.

To avoid calling PRAGMA per-column, fetch the PRAGMA results once per table when the table name is first resolved (line 837 or 886) and cache in a local variable.

**3. Execute-time `schema_types` population (lines 4487-4511):** Same pattern as #1 — after `get_schema_type_with_session` returns `None`, fall back to PRAGMA lookup.

The downstream consumers (lines 2799, 2832, 3783 in `send_select_response` and `encode_row`) receive `field_types` from the Parse or Execute paths above. Once those paths produce correct OIDs, no changes are needed at these downstream locations.

## Files Changed

| File | Change |
|------|--------|
| `src/session/db_handler.rs` | Add `get_column_types_from_pragma` method |
| `src/types/sqlite_type_info.rs` | Add `sqlite_type_to_pg_type_name` function + tests |
| `src/query/executor.rs` | Add PRAGMA fallback in schema_types population loop |
| `src/query/extended.rs` | Add PRAGMA fallback in 3 locations: schema_types (2) + inferred_types (1) |

## Testing

- Pre-existing SQLite table with INTEGER column → OID 23 (int4)
- Pre-existing SQLite table with REAL column → OID 701 (float8)
- Pre-existing SQLite table with BOOLEAN column → OID 16 (bool)
- Pre-existing SQLite table with TEXT column → OID 25 (text)
- Pre-existing SQLite table with BLOB column → OID 17 (bytea)
- Pre-existing SQLite table with BIGINT column → OID 20 (int8)
- Pre-existing SQLite table with SMALLINT column → OID 21 (int2)
- Tables created via pgsqlite `CREATE TABLE` still work correctly (no regression)
- `sqlite_type_to_pg_type_name` unit tests for all type mappings
- `get_column_types_from_pragma` unit test with a test table

## Known Limitations

- Computed expressions without a table (e.g., `SELECT 1+2`) still default to TEXT — would require value-based inference, which is out of scope
- SQLite's type affinity system means `INTEGER` column could contain any type at runtime; we trust the declared type
- `PRAGMA table_info` doesn't distinguish between `INT4` and `INT8` when the declared type is just `INTEGER` — defaults to `int4`
