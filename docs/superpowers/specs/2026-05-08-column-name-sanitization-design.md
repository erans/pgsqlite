# Column Name Sanitization: Strip Function Parentheses from Result Column Names

## Problem

When PostgreSQL clients execute `SELECT version()`, PostgreSQL returns a result column named `version`. SQLite returns `version()` (or `function_name(arg1, arg2)` for functions with arguments). This breaks clients like pgAdmin4 that expect column names to match PostgreSQL behavior — they look for a `version` column and find `version()` instead.

The existing `FunctionParenthesesTranslator` only strips `()` from `current_user()` and `session_user()` in query text before execution. It does not address column names in result sets, and it does not handle general function call column names like `version()`, `count(*)`, `max(id)`, etc.

## Solution

Add a `sanitize_column_name()` function that strips parenthesized suffixes from column names. Apply it at the boundary where SQLite column names enter the pgsqlite protocol layer — specifically when building `FieldDescription` objects or `DbResponse.columns`.

## Design

### New Function

A small utility function in `src/query/column_sanitizer.rs` (new file):

```rust
/// Strip parenthesized arguments from column names to match PostgreSQL behavior.
/// PostgreSQL: SELECT version() -> column name "version"
/// SQLite:     SELECT version() -> column name "version()"
/// This function normalizes SQLite's behavior to match PostgreSQL.
pub fn sanitize_column_name(name: &str) -> &str {
    if let Some(pos) = name.find('(') {
        &name[..pos]
    } else {
        name
    }
}
```

### Where to Apply

Sanitize column names **after** internal type lookups (datetime, boolean, enum, schema lookups) but **before** they reach the protocol layer. This means:

1. **`DbResponse.columns`** — When building the response from `stmt.column_name(i)`, sanitize the names. Since type lookups in the executor also use `response.columns`, we need to ensure the sanitization is applied consistently.

   **Strategy:** Sanitize at the point where `DbResponse.columns` is constructed (the `stmt.column_name(i)` collection sites), BUT keep raw names accessible for type lookups. The cleanest approach: sanitize in `DbResponse.columns` and update type-lookup logic to use sanitized names too (since the schema cache keys use real column names, not function-call expressions).

2. **Direct `stmt.column_name(i)` calls** in fast_path, extended, and other executor paths — sanitize uniformly.

### Application Sites

**`src/session/db_handler.rs`:**
- Line ~529: `columns.push(stmt.column_name(i)?.to_string())` — SELECT path
- Line ~664: same pattern — another SELECT path
- Line ~777: same — catalog query path
- Line ~895: same
- Line ~1242: same
- Line ~1453: same
- Line ~1722: same
- Line ~2324-2326: column collection for type inference

**`src/query/fast_path.rs`:**
- Line 377: `columns.push(stmt.column_name(i)?.to_string())`
- Line 619: same
- Line 737: same

**`src/query/extended.rs`:**
- Lines creating `column_names` from `stmt.column_name(i)` — sanitize similarly

**`src/query/executor.rs`:**
- Type-lookup sites that use column names from `response.columns` — these need to work with sanitized names since that's what flows through the system.

### Type Lookup Compatibility

Internal type lookups (datetime, boolean, enum, schema) use column names to query `__pgsqlite_schema`. Since real table columns never contain `(`, sanitized names won't break these lookups. Function-call column names like `version()` or `max(id)` aren't in `__pgsqlite_schema`, so stripping parens is safe — the lookup simply returns `None` as it does today.

For the executor's `column_mappings` (alias → real column), these are built from `AS` aliases in the query. If the alias is a function call like `max(id)`, stripping parens gives `max`, which also won't match schema lookups — same behavior as today (fallback to text type).

### Edge Cases

| SQLite Column Name | After Sanitization | PostgreSQL Behavior | Match? |
|---|---|---|---|
| `version()` | `version` | `version` | Yes |
| `count(*)` | `count` | `count` | Yes |
| `max(id)` | `max` | `max` | Yes |
| `current_timestamp` | `current_timestamp` | `current_timestamp` | Yes |
| `pg_catalog.version()` | `pg_catalog.version` | N/A (qualified) | Acceptable |
| `my_alias` | `my_alias` | `my_alias` | Yes |
| `1` | `1` | `?column?` | Acceptable |

### What About the Existing FunctionParenthesesTranslator?

The `FunctionParenthesesTranslator` strips `current_user()` and `session_user()` from query text. This is still needed because SQLite doesn't recognize `current_user` as a function call — it needs the parentheses removed from the query itself. But it's incomplete:

1. It doesn't handle `version()` in the query (unnecessary since SQLite supports it)
2. It doesn't affect result column names (the actual bug)

The column name sanitizer is orthogonal: it fixes result column names regardless of what the query text looks like. No changes to `FunctionParenthesesTranslator` are needed for this fix.

## Testing

Add unit tests in `column_sanitizer.rs` covering:
- Basic function call: `version()` → `version`
- Function with args: `count(*)` → `count`, `max(id)` → `max`
- No parens: `current_timestamp` → `current_timestamp`
- Nested parens: `COALESCE(max(id), 0)` → `COALESCE`
- Already clean: `my_column` → `my_column`
- Schema-qualified: `pg_catalog.version()` → `pg_catalog.version`

Integration test: connect via psql/psycopg, execute `SELECT version()`, verify column name is `version`.