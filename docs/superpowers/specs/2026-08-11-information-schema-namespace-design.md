# Namespace-aware `information_schema` served from SQLite

Issue: [#88](https://github.com/erans/pgsqlite/issues/88)
Date: 2026-08-11
Status: Approved, ready for implementation planning

## Problem

`information_schema.tables` reports pgsqlite's own materialized catalog
relations as user tables in the `public` schema:

```
$ psql -tA -c "SELECT table_schema, table_name FROM information_schema.tables ORDER BY 1,2"
public|pg_am
public|pg_attrdef
public|pg_class
...
public|information_schema_tables
public|information_schema_columns
...
public|customers
```

28 internal relations alongside the one user table. Django `inspectdb` and
SQLAlchemy `automap` would generate models for `pg_constraint` and friends.

In real PostgreSQL these rows *do* exist — `information_schema.tables` on a
stock cluster returns ~180 rows — but under `table_schema = 'pg_catalog'` and
`table_schema = 'information_schema'`. Clients filter on `table_schema`; that
is the mechanism that makes them invisible, not omission.

### Root cause

Not the handler cited in the issue. `src/catalog/query_interceptor.rs:2005` sits
inside the *columns* handler. The live path is
`handle_information_schema_tables_query` at
`src/catalog/query_interceptor.rs:1844`, dispatched from the arm at
`src/catalog/query_interceptor.rs:729`. It reads:

```rust
db.query("SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') \
          AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__pgsqlite_%'")
```

— no catalog filtering at all — and emits `Some("public".to_string().into_bytes())`
for every row's `table_schema`.

Migration v28 (from #87) already assigns `pg_%` relations to namespace 11 and
`information_schema_%` to 13000 in the `pg_class` view, which is why `\dt` is
clean. The `information_schema_tables` *view* at
`src/migration/registry.rs:1815` selects from `pg_class` but hardcodes
`'public' as table_schema`, discarding that work — and the Rust handler bypasses
both.

### The handlers are stale, not just unfiltered

The same class of defect #87 found in `PgClassHandler`. Measured against the
Rust handler on a fresh database:

| Query | Result |
| --- | --- |
| `SELECT table_name FROM information_schema.tables ORDER BY 1` | 29 rows, **unsorted** — `ORDER BY` ignored |
| `SELECT count(*) FROM information_schema.tables WHERE table_schema='public'` | **0 rows** — aggregates unsupported |
| `SELECT count(*) FROM information_schema.tables WHERE table_type='BASE TABLE'` | **0 rows** |
| `SELECT DISTINCT table_name FROM information_schema.columns` | 60 rows, **`DISTINCT` ignored** |

`WHERE` support extends only to equality on `table_name`
(`extract_table_name_filters`); every other predicate is silently dropped rather
than erroring. Any fix that keeps the handler inherits all of this.

### Why the `columns` half is harder

`information_schema_columns` reads `pg_attribute`, whose `atttypid` is a `CASE`
over the *SQLite* declared type (`src/migration/registry.rs:2604`), not the
PostgreSQL type recorded in `__pgsqlite_schema`. The two paths are differently
wrong, measured on
`CREATE TABLE fidelity (id SERIAL PRIMARY KEY, amount NUMERIC(10,2), uid UUID,
doc JSONB, tags TEXT[], ts TIMESTAMPTZ, flag BOOLEAN, nick VARCHAR(50))`:

| column | declared | Rust handler | SQLite view | correct |
| --- | --- | --- | --- | --- |
| `id` | `SERIAL` | `text` ✗ | `integer` ✓ | `integer` |
| `amount` | `NUMERIC(10,2)` | `numeric` ✓ | `text` ✗ | `numeric` |
| `uid` | `UUID` | `uuid` ✓ | `text` ✗ | `uuid` |
| `doc` | `JSONB` | `jsonb` ✓ | `text` ✗ | `jsonb` |
| `tags` | `TEXT[]` | `text` ✗ | `text` ✗ | `ARRAY` |
| `ts` | `TIMESTAMPTZ` | `text` ✗ | `integer` ✗✗ | `timestamp with time zone` |
| `flag` | `BOOLEAN` | `boolean` ✓ | `integer` ✗ | `boolean` |
| `nick` | `VARCHAR(50)` | `character varying` ✓ | `text` ✗ | `character varying` |

Routing `columns` to the view as-is would regress six of eight columns, and
`ts → integer` would leak the INTEGER datetime storage into a client-visible
surface. `pg_attribute` must become type-accurate first. The authoritative
mapping already exists as `SchemaTypeMapper::pg_type_string_to_oid`
(`src/types/schema_type_mapper.rs:112`); it simply is not reachable from SQL.

### Prefix matching is not sufficient

v28 identifies internal relations with `name LIKE 'pg\_%' ESCAPE '\'`. A user
table named `pg_myreport` is therefore filed under `pg_catalog` — today that
only hides it from `\dt`, but once `information_schema.tables` derives
`table_schema` from the same expression, the misfiling hides it from ORM
introspection too.

The prefix test also *under*-matches. Migrations create eight indexes named
`idx_*`, which no `__pgsqlite_%` or `pg_%` filter catches:

```
$ psql -c "\di"
 Schema |             Name              | Type  |  Owner   | Table
--------+-------------------------------+-------+----------+-------
 public | idx_array_types_table         | index | postgres |
 public | idx_comments_lookup           | index | postgres |
 ... 8 rows
```

All eight are `CREATE INDEX IF NOT EXISTS` statements in
`src/migration/registry.rs`, and all eight leak into `\di` and ORM index
reflection today.

## Design

Make the SQLite views the single source of truth, delete the Rust handlers, and
identify internal relations by exact name rather than by prefix.

### Component 1: internal-relation registry

New module `src/catalog/internal_relations.rs` holding a `const` list of the
relation names pgsqlite's migrations create — 4 tables, 24 views, 8 indexes —
each tagged with its namespace OID (11 `pg_catalog`, 13000 `information_schema`).

This is the authoritative answer to "is this ours?", replacing the `LIKE 'pg\_%'`
heuristic. It is a static list because the migration registry is static; adding a
catalog relation to a future migration means adding a line here, and the
migration round-trip test below fails loudly if the two drift.

Exposed to SQL as one UDF in `src/functions/catalog_functions.rs`:

- `__pgsqlite_relnamespace(name TEXT) -> INTEGER` — returns `11`, `13000`, or
  `2200` for anything not in the list.

### Component 2: type-resolution UDFs

Two more UDFs in the same module, so views can reach the existing Rust type
logic:

- `__pgsqlite_pg_type_oid(pg_type TEXT) -> INTEGER` — wraps
  `SchemaTypeMapper::pg_type_string_to_oid`, handling modifiers (`NUMERIC(10,2)`)
  and array suffixes (`TEXT[]`). Returns `NULL` on unrecognized input so the view
  can fall back.
- `__pgsqlite_format_type_is(oid INTEGER) -> TEXT` — OID to the SQL-standard
  `data_type` spelling `information_schema` requires: `integer`,
  `character varying`, `timestamp with time zone`, `ARRAY` for any array OID.
  Distinct from the existing `SchemaTypeMapper::pg_oid_to_type_name`, which
  returns PostgreSQL internal names (`int4`, `varchar`).

All three register through `functions::register_all_functions`
(`src/functions/mod.rs:20`), so every connection gets them, including the one
`db.query()` uses for the fall-through path.

### Component 3: migration v29

Four view redefinitions, following the v28 pattern of `DROP VIEW` + `CREATE VIEW`
with a `down` that restores the prior definitions verbatim.

- **`pg_class`** — `relnamespace` becomes `__pgsqlite_relnamespace(name)`. Every
  downstream consumer inherits exact-name correctness; fixes the `pg_myreport`
  misfiling and the eight leaking `idx_*` indexes in one place.
- **`pg_attribute`** — `atttypid` becomes
  `COALESCE(__pgsqlite_pg_type_oid(s.pg_type), <existing CASE>)` via a
  `LEFT JOIN __pgsqlite_schema s`. The fallback preserves behavior for tables
  with no `__pgsqlite_schema` row (databases created outside pgsqlite).
- **`information_schema_tables`** — `table_schema` from
  `pg_namespace.nspname` joined on `pg_class.relnamespace`, replacing
  `'public' as table_schema`.
- **`information_schema_columns`** — same `table_schema` join;
  `data_type` and `udt_name` from `__pgsqlite_format_type_is(a.atttypid)`,
  replacing the inline `CASE`.

### Component 4: routing

`SchemaPrefixTranslator::translate_query`
(`src/translator/schema_prefix_translator.rs:11`) already strips `pg_catalog.`
and the interceptor executes the rewritten query via `db.query()` when no
handler claims it (`src/catalog/query_interceptor.rs:198`) — the mechanism that
carries `pg_class` to SQLite post-#87. Line 91's comment, *"Don't remove
information_schema prefix - it's handled by query interceptor"*, is the thing
being changed.

Add `information_schema.tables → information_schema_tables` and
`information_schema.columns → information_schema_columns`, then delete:

- the dispatch arms at `src/catalog/query_interceptor.rs:729` and `:734`
- `handle_information_schema_tables_query` and
  `handle_information_schema_columns_query_with_session`
- the unreachable duplicates at `src/session/db_handler.rs:1174`, `:1650`,
  `:2663` and the branch at `src/query/extended.rs:2262`

The JOIN-rewrite path at `src/catalog/query_interceptor.rs:275` already performs
the same substitution and stays as-is; after this change both paths converge on
the same views instead of disagreeing.

### Data flow

```
psql
  └─ executor
      └─ CatalogInterceptor::intercept
          ├─ SchemaPrefixTranslator            information_schema.tables
          │                                  → information_schema_tables
          ├─ handle_catalog_query → None      (no arm matches)
          └─ db.query(translated)
              └─ SQLite view
                  └─ pg_class → pg_namespace
                      └─ __pgsqlite_relnamespace(name)
```

### Error handling

- `__pgsqlite_pg_type_oid` returns `NULL` for unknown or `NULL` input; the
  `COALESCE` in `pg_attribute` falls back to the SQLite-type `CASE`.
- `__pgsqlite_relnamespace` returns `2200` for any unlisted name, so an
  unrecognized relation is treated as a user table — the safe direction, since
  the failure mode is showing an internal relation rather than hiding a user's.
- `__pgsqlite_format_type_is` returns `text` for unmapped OIDs, matching the
  current view's `ELSE 'text'`.
- Migration `down` restores the v28 `pg_class`, v26 `pg_attribute`, and v14
  `information_schema_*` definitions verbatim.

## Testing

TDD throughout, starting from a test that fails on the current tree.

1. **Regression test for #88** — `information_schema.tables` reports
   `pg_catalog` / `information_schema` for internal relations and `public` only
   for user tables; `WHERE table_schema='public'` returns exactly the user
   tables.
2. **Handler defects** — `ORDER BY` sorts, `count(*)` returns a count,
   `WHERE table_type='BASE TABLE'` filters, `DISTINCT` deduplicates. All four
   fail today.
3. **Type fidelity** — the eight-column table above, asserting the "correct"
   column of that table.
4. **Exact-name matching** — a user table named `pg_myreport` reports
   `table_schema='public'` and appears in `\dt`; the eight `idx_*` indexes do not
   appear in `\di`.
5. **No regressions** — `tests/information_schema_test.rs`,
   `tests/information_schema_comprehensive_test.rs`,
   `tests/orm_constraint_discovery_test.rs`, `tests/permission_functions_test.rs`
   and the SQLAlchemy suite stay green.
6. **Migration round-trip** — v29 up then down leaves the v28 schema, and the
   internal-relation list matches what a migrated-to-head database actually
   contains (guards list drift).

## Non-goals

- **Columns of views.** `pg_attribute` remains tables-only, so
  `information_schema.columns` reports nothing for a view — true today on both
  paths. Follow-up issue.
- **Stripping the `information_schema_` prefix** from `table_name`, so
  `information_schema.tables` reports `information_schema_tables` rather than
  PostgreSQL's `tables`. Cosmetic; no client filters on it.
- **The remaining `information_schema` Rust handlers** —
  `key_column_usage`, `table_constraints`, `referential_constraints`, `routines`,
  `views`, `schemata` keep their handlers and very likely share these defects.
  Out of scope; worth an umbrella issue once this lands and the pattern is
  proven twice.
