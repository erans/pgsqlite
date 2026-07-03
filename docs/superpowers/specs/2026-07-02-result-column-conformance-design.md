# Result Column Conformance & Sanitization Redesign

**Status:** Design (pending implementation plan)
**Date:** 2026-07-02
**Scope:** Full PostgreSQL result-column-name & type conformance for the SELECT paths, fixing the issues found in the adversarial GLM-5.2 review (and the GPT-5.5 adjudication) of range `204380d..c5814eb`.

## Background

Commit `3b9c49f` ("strip function parentheses from result column names") added a `sanitize_column_name()` that truncates every result-column name at the first `(`, applied at ~10 sites in `src/session/db_handler.rs`. The feature's design doc argued this was safe because "real table columns never contain `(`." That premise is false: PostgreSQL quoted identifiers (e.g. `"price(usd)"`) may contain `(`, and SQLite returns them verbatim. The blind truncation introduced a cluster of regressions, and the associated bare-name type-inference block introduced another.

An adversarial review (two GLM-5.2 reviewers, adjudicated by GPT-5.5 xhigh) confirmed 10 findings with 0 false positives, plus 1 additional issue GPT-5.5 found. This spec fixes all of them.

## Goals

1. **Full PostgreSQL conformance** for result column names and types on all SELECT paths (simple, extended/portal, read-only).
2. **Fix every confirmed finding** (F1–F10 + F-new).
3. **Backward-compat escape hatch** via a session GUC, defaulting to conformant behavior.
4. **No regression to the zero-alloc fast path** for trivial `SELECT *` / plain-column queries.

## Non-goals

- Lowercasing column *data* or identifiers in DDL (already handled at `CREATE TABLE` time via `__pgsqlite_schema`).
- A general "execute arbitrary aggregate against catalog" engine (deferred; see F-new scope).
- Changing non-SELECT result metadata (DML returning columns is out of scope).

## Decisions (locked during brainstorming)

| Decision | Choice | Meaning |
|---|---|---|
| Scope | **C — Full conformance** | Global lowercase-folding of unquoted result identifiers; `?column?` for unnamed expressions; quoted-case preservation; function names bare + lowercased. |
| Backward compat | **C2 — Cutover with GUC off-switch** | Default conformant; `pgsqlite.legacy_result_columns = on` opts back to legacy casing. The paren-corruption, empty-name, and datetime fixes are **not** behind the GUC. |
| Architecture | **Approach 1 — Schema-canonical + lazy AST for aliases** | Real columns & wildcards use schema-stored effective names (no parse). Function-call names use the raw SQLite name's shape. Only queries containing `AS` parse the projection (cached). |
| Type inference | **T3 — Argument-preserving + datetime side effect** | `min/max/sum` over a datetime inner column → datetime OID **and** `datetime_flag` so the existing converter formats microseconds. |

## Architecture

### The central resolver

A new `ProjectionResolver` (in `src/query/projection_resolver.rs`) is the single place that turns SQLite's `column_name(i)` into PostgreSQL-correct metadata. Every SELECT path calls it instead of `sanitize_column_name` + ad-hoc type inference.

```
ProjectionResolver::resolve(raw_name, position, ctx) -> ColumnMeta
ColumnMeta { wire_name: String, type_oid: i32, datetime_flag: bool }
```

**Context (`ctx`) per query:**
1. `raw_name = stmt.column_name(i)` — SQLite's string (shape only: has `(`? empty?).
2. `schema_types: HashMap<col, pg_type_string>` — already built from `__pgsqlite_schema`. Holds the **effective PG name** (correct case) for real columns. Truth for real columns & wildcards.
3. Parsed projection alias view (only when the query contains `AS`) → per-position `(alias, is_quoted, source_expr)`.
4. `translation_metadata` hints (already produced by the translator) — carry `source_column` + `expression_type` for function calls.
5. Session GUC `pgsqlite.legacy_result_columns` (off = conform).

### Resolution precedence (first match wins)

1. **Schema match** on the *raw* name → real column. `wire_name` = schema-stored effective name (correct case), `type_oid` = schema type. *(F1)*
2. **Function-call shape** (raw name contains `(`): `wire_name` = bare lowercased function name; `type_oid` from the function→type table (argument-preserving for `min/max/sum/first/last` by resolving the inner column via schema or `translation_metadata`); if the resolved type is datetime, set `datetime_flag`. *(F2, F5)*
3. **Alias** (query has `AS`, position maps to an alias item): `wire_name` = quoted ? `alias` : `alias.to_lowercase()`; `type_oid` = source-expr type via schema or generic. *(F6)*
4. **Unnamed non-function expression**: `wire_name` = `?column?` (subsequent duplicates: `?column?2`, `?column?3`, …); `type_oid` from `translation_metadata` hint or TEXT. *(F4)*
5. **Fallback**: raw name (lowercased if GUC off) + TEXT.

**GUC effect:** when `pgsqlite.legacy_result_columns = on`, steps 3 & 5 skip lowercasing. Steps 1, 2, 4 stay conformant regardless.

### Data flow

```
client SQL
  → process_query (translator) → translation_metadata (already produced)
  → SQLite prepare + query
  → for each column i:
       raw_name = stmt.column_name(i)
       ctx = { schema_types, translation_metadata, alias_view(query) [if AS], legacy_guc }
       ColumnMeta = resolver.resolve(raw_name, i, ctx)
  → DbResponse.columns           = ColumnMeta.wire_name
  → field_descriptions[i].type_oid = ColumnMeta.type_oid
  → if ColumnMeta.datetime_flag: datetime_columns.insert(wire_name, pg_type)  // feeds existing converter
```

### Path convergence (fixes F3)

Three SELECT entry points today produce result metadata independently:
- `DbHandler` (multiple sites in `db_handler.rs`) — sanitized.
- `ReadOnlyDbHandler` (`read_only_handler.rs`) — unsanitized.
- Extended/portal (`extended.rs`) — its own AST walk.

All three call one helper: `resolve_columns(stmt, query, schema_types, hints, session) -> Vec<ColumnMeta>`. The extended path's existing AST walk becomes the *source* of the alias view rather than a parallel implementation. Output is identical across pooling on/off.

### Alias view caching (the only parse)

`alias_view(query)` parses the projection with `sqlparser` only when the query contains `AS`. The result is cached on `PreparedStatement` (in `src/session/state.rs`) as a new optional field:

```rust
pub struct PreparedStatement {
    // ... existing fields ...
    pub projection_metadata: Option<Vec<AliasItem>>,
}
```

`AliasItem { alias: String, is_quoted: bool, source_expr: Expr }` — `is_quoted` derived from `Ident::quote_style.is_some()`.

Prepared-statement re-execution hits the cache; one-off simple queries without `AS` never parse, preserving the zero-alloc fast path.

### `fn_shape` correctness (the F2 root)

The current `direct_pattern` regex runs against the *sanitized* name (broken — parens stripped). The resolver's `fn_shape` runs against the **raw** SQLite name (`max(created_at)`), so the inner column is extractable and its schema type resolvable. This is the mechanical fix that makes T3 work.

### Type table (T3)

| Function | Generic OID | Argument-preserving? |
|---|---|---|
| `count` | int8 | no |
| `sum` | numeric | yes (int→int4/8, numeric, float→float8) |
| `avg` | numeric | no |
| `min` / `max` | (arg type) | **yes — datetime→datetime** |
| `json_extract`, `json_agg`, `row_to_json`, … | text | no |
| `array_length`, `array_upper`, … | int4 | no |
| `now` / `current_timestamp` | timestamptz | no |

For `min/max/sum` over a datetime inner column, `datetime_flag = true` → the existing converter formats the microseconds.

### Removals

- `src/query/column_sanitizer.rs` (`sanitize_column_name`) — deleted; subsumed by the resolver.
- The bare-name OID block in `src/types/schema_type_mapper.rs:344+` — deleted; the resolver's function→type table replaces it. The existing query-regex path below it (`:379`) remains as a fallback for aliased aggregates resolved from the query text.

## Catalog fixes (independent of the resolver)

Localized to `src/catalog/query_interceptor.rs` and `src/catalog/pg_roles.rs`.

### F7 — `break` on Wildcard drops trailing items
In `extract_selected_columns` (`query_interceptor.rs:1756-1761`) and `PgRolesHandler::get_selected_columns` (`pg_roles.rs:77-83`, `102-106`), the `Wildcard`/`QualifiedWildcard` arm does `cols.extend(...); break;`.
**Fix:** remove `break`; continue the loop. For `QualifiedWildcard`, extend with all columns of the *named* relation only. No dedupe — PostgreSQL emits duplicates for `SELECT *, x`.

### F-new — catalog aggregate projections dropped
`extract_projection_source_column` (`query_interceptor.rs:1767`) handles `Identifier`/`CompoundIdentifier`/`Cast`/`Nested` but returns `None` for `Expr::Function`. So `SELECT count(*) FROM pg_catalog.pg_namespace` → `UnnamedExpr(Function)` → zero selected columns → empty result.
**Fix (minimal scope, locked):** add an `Expr::Function` arm resolving to the bare function name; when no catalog column matches, the handler detects a `count`-shaped aggregate on a catalog table and computes the row count of its static dataset (e.g. `pg_namespace` → 2). The broader "execute any aggregate verbatim against SQLite" path is deferred as future work.

### F6 — alias case not folded
`query_interceptor.rs:1749-1754` and `pg_roles.rs:95-100` push `alias.value.clone()` verbatim.
**Fix:** push `alias.value.to_lowercase()` for **unquoted** aliases, preserve for quoted. In sqlparser 0.57 the `alias` in `ExprWithAlias` is an `Ident` carrying `quote_style: Option<char>` (the codebase already reads `alias.value`); quoted iff `alias.quote_style.is_some()`. Under `pgsqlite.legacy_result_columns = on`, skip lowercasing (same GUC as the hot path).

### F10 — `catalog_alias_test.rs` coverage gaps
Add tests for: uppercase aliases (F6), mid-list `SELECT *, oid` (F7), `SELECT count(*) FROM pg_catalog.pg_namespace` (F-new), `CAST`/`Nested` projections, `CompoundIdentifier` source, `WHERE` + alias interaction, unknown source column.

## Hygiene

### F8 — mislabeled debug log
`query_interceptor.rs:1082` (inside `handle_pg_namespace_query`) says `"pg_type query"`. → `"pg_namespace query"`.

### F9 — stray test file
Delete `tests/test_batch_7b.py`. Confirmed unreferenced by any runner.

## GUC mechanism

`pgsqlite.legacy_result_columns` is a normal session parameter:
- **Stored** in `SessionState.parameters` (`HashMap<String,String>`), the existing GUC store.
- **Set** via the existing `SetHandler::handle_set_command` (`src/query/set_handler.rs`), which already stores arbitrary `SET key = value`.
- **Read** by `ProjectionResolver` (hot path) and the catalog handlers (F6) via a session lookup; default `off` (conformant).
- No new infrastructure.

## Testing

### Unit tests — `src/query/projection_resolver.rs` (new, pure)
- Schema match wins over function shape: raw `"price(usd)"` in schema → keeps full name + schema type. *(F1)*
- `fn_shape`: `max(created_at)`→`("max","created_at")`, `count(*)`→`("count","*")`, `COALESCE(max(id),0)`→`("COALESCE",...)`. *(F2 root)*
- T3: `min` over timestamp → timestamp OID + `datetime_flag=true`; over int4 → int4; `count(*)` → int8. *(F2/F5)*
- Alias: `AS Foo`→`foo`, `AS "Foo"`→`Foo`; GUC on→`Foo`. *(F6)*
- Unnamed non-function expr → `?column?`, duplicate → `?column?2`. *(F4)*
- GUC off lowercases unquoted; on preserves. *(C2)*

### Unit tests — catalog
- `extract_selected_columns` with `SELECT *, oid` → trailing `oid` retained. *(F7)*
- `extract_projection_source_column` on `Expr::Function(count)` → resolves. *(F-new)*
- `SELECT oid AS Did` → `did` (GUC off), `Did` (GUC on). *(F6)*

### Integration tests (shell runner / Python, new SQL files)
- `CREATE TABLE t ("price(usd)" INT); SELECT "price(usd)" FROM t;` → column `price(usd)`, int4. *(F1)*
- `SELECT max(created_at) FROM t;` → formatted timestamp, not microseconds. *(F2)*
- `SELECT upper(name) AS count FROM t;` → column `count` typed **text**, decodes. *(F5)*
- `SELECT 1+1;` → column `?column?`. *(F4)*
- `SELECT count(*) FROM pg_catalog.pg_namespace;` → `2`. *(F-new)*
- `SELECT *, oid FROM pg_catalog.pg_namespace;` → 3 columns. *(F7)*
- `SET pgsqlite.legacy_result_columns = on; SELECT MyCol FROM t;` → `MyCol` (legacy); default → `mycol`. *(C2)*

### Existing-test update
The casing default changes, so existing tests asserting as-written unquoted column case get updated to the lowercased expectation. The GUC-on path is used only in a dedicated legacy-compat test — not to keep the old suite passing.

### Regression guard
The F2 repro (`max(timestamp)` returns raw microseconds) becomes a test that fails on current HEAD and passes after the fix.

## Findings addressed

| Finding | Severity (adjudicated) | Fixed by |
|---|---|---|
| F1 — sanitize corrupts paren-columns + type lookup | Important | Resolver precedence step 1 (schema match keeps raw name) |
| F2 — `MAX/MIN(datetime)` → raw microseconds | Important | `fn_shape` on raw name + T3 datetime_flag |
| F3 — ReadOnly vs DbHandler name divergence | Minor (config-path) | Path convergence via single `resolve_columns` helper |
| F4 — leading `(` → empty wire name | Minor | Resolver step 4 (`?column?`) |
| F5 — bare-name OID block mis-types aliases | Important | Delete bare block; resolver function→type table from expression, not alias name |
| F6 — alias case not folded | Minor | Resolver step 3 + catalog `quoted` flag |
| F7 — `break` on wildcard drops trailing items | Minor | Remove `break` in both catalog extractors |
| F8 — mislabeled debug log | Trivial | One-line log fix |
| F9 — stray `tests/test_batch_7b.py` | Minor | Delete |
| F10 — `catalog_alias_test.rs` gaps | Minor (test-only) | New tests |
| F-new — catalog aggregate projection dropped | Important | `Expr::Function` arm + `count(*)` dataset row count |

## Risks & mitigations

- **Casing default change breaks a client.** Mitigated by `pgsqlite.legacy_result_columns = on` (C2).
- **Parse cost on aliased queries.** Mitigated by caching `projection_metadata` on `PreparedStatement`; non-`AS` queries never parse.
- **T3 inner-column resolution miss.** Falls back to the function's generic OID (e.g. `max`→TEXT); documented behavior.
- **Catalog `count(*)` minimal path misses non-count aggregates.** Accepted; broader path deferred (future work).

## Out of scope / future work

- General catalog aggregate execution engine (F-new broader path).
- Lowercasing consistency audit for DDL identifiers.
- Migrating the remaining catalog handlers off `get_mut_connection` (pre-existing known limitation).
