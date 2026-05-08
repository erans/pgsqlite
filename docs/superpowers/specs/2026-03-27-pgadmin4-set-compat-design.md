# pgAdmin4 SET Command Compatibility

**Issue**: [#71](https://github.com/erans/pgsqlite/issues/71) - SET without spaces around equal sign
**Date**: 2026-03-27
**Scope**: Fix SET parsing + add `set_config()` and `pg_show_all_settings()` for pgAdmin4 compatibility

## Problem

pgAdmin4 sends this compound query on connection:

```sql
SET DateStyle=ISO; SET client_min_messages=notice; SELECT set_config('bytea_output','hex',false) FROM pg_show_all_settings() WHERE name = 'bytea_output'; SET client_encoding='utf-8';
```

Three things fail:
1. `SET DateStyle=ISO` — regex requires spaces around `=`
2. `pg_show_all_settings()` — function not recognized
3. `set_config(...)` — function not implemented

## Design

### 1. SET Regex Fix

**File**: `src/query/set_handler.rs`

Current regex (`SET_PARAMETER_PATTERN`):
```
(?i)^\s*SET\s+(\w+)\s+(?:TO|=)\s+(.+)$
```

Fixed regex:
```
(?i)^\s*SET\s+(\w+)(?:\s*=\s*|\s+TO\s+)(.+)$
```

- `=` allows optional whitespace on both sides (`\s*=\s*`)
- `TO` still requires whitespace on both sides (`\s+TO\s+`) to prevent ambiguity with parameter names

Covers all PostgreSQL-valid forms: `SET x=y`, `SET x = y`, `SET x TO y`.

### 2. pg_show_all_settings() Rewrite

**Location**: Query preprocessing in `src/query/executor.rs`, before SQL parsing

`pg_show_all_settings()` returns the same data as `pg_settings`. Rewrite the function call to the table name via case-insensitive text replacement:

```
pg_show_all_settings() → pg_settings
```

After rewriting, the query routes through the existing `PgSettingsHandler`.

**Limitation**: This is a plain text substitution. It would incorrectly rewrite the function name inside string literals (e.g., `'pg_show_all_settings()'`). In practice this never occurs in real client queries.

### 3. set_config() Function

**Location**: Handled as a special case in `src/query/executor.rs`, at the `execute_single_statement` level (before normal query routing).

Detection regex:
```
(?i)set_config\(\s*'([^']+)'\s*,\s*'([^']*)'\s*,\s*(true|false)\s*\)
```

Note: The value capture uses `[^']*` (star, not plus) to allow empty string values like `set_config('application_name', '', false)`.

Processing:
1. Detect that the query is a SELECT containing `set_config(...)`
2. Extract setting name, new value, and is_local flag from regex captures
3. Set the parameter in the session state (same as SET handler)
4. Send a synthetic response directly on the wire:
   - `RowDescription`: one column named `set_config`, type Text
   - `DataRow`: one row containing the new value
   - `CommandComplete`: tag `"SELECT 1"`
5. Return early — do **not** pass through to `PgSettingsHandler` or normal query execution

This avoids the problem of `PgSettingsHandler` not knowing how to handle string literal projections. The `set_config()` handler owns the full response.

**`is_local` flag**: Treated identically to `false` (session-level SET). pgsqlite does not support transaction-scoped settings. This is a known limitation — the parameter is always set at session scope regardless of the flag value.

**Error handling**: If `set_config()` arguments can't be parsed, the query falls through to normal execution.

### 4. server_version Consistency (Drive-by Fix)

Four locations report different PostgreSQL version numbers:

| Location | Current Value | Action |
|----------|--------------|--------|
| `src/catalog/pg_settings.rs:102` | `16.0` | Keep (canonical) |
| `src/query/set_handler.rs:110` | `15.0` | Update to `16.0` |
| `src/session/state.rs:58` | `14.0 (SQLite wrapper)` | Update to `16.0` |
| `src/functions/system_functions.rs:16` | `15.0` | Update to `16.0` |

Align all to `16.0` so pgAdmin4 sees a consistent version across all code paths.

## Files Changed

| File | Change |
|------|--------|
| `src/query/set_handler.rs` | Fix `SET_PARAMETER_PATTERN` regex; update `server_version` to `16.0` |
| `src/query/executor.rs` | Add `pg_show_all_settings()` rewrite and `set_config()` handler in preprocessing |
| `src/session/state.rs` | Update `server_version` to `16.0` |
| `src/functions/system_functions.rs` | Update version string to `16.0` |

## Testing

- Unit tests for SET regex: `SET x=y`, `SET x = y`, `SET x TO y` variants
- Unit test for `pg_show_all_settings()` rewrite
- Unit test for `set_config()` detection, parameter extraction, and empty-value handling
- Integration test with the exact pgAdmin4 compound query

## Known Limitations

- `set_config()` with `is_local=true` behaves as session-level (no transaction-scoped settings)
- `set_config()` detection is regex-based, not AST-based — handles the standard `SELECT set_config(...)` pattern but not deeply nested usage
- `set_config()` regex does not handle escaped single quotes in values (e.g., `'my''app'`); unlikely in practice for settings values
- `pg_show_all_settings()` rewrite is plain text substitution, not AST-aware; would incorrectly rewrite occurrences inside string literals
