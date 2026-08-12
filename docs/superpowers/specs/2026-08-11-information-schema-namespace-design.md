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
only hides it from `\dt`. If `information_schema.tables` derived `table_schema`
by joining `pg_namespace` on `pg_class.relnamespace`, it would inherit the
misfiling and hide that table from ORM introspection too — a regression on
current behavior, where it is at least visible. The `information_schema` views
therefore call the exact-name UDF directly rather than reading
`pg_class.relnamespace`; correcting `pg_class` itself is a follow-up.

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
reflection today. Indexes are not visible through `information_schema.tables`
(which selects `relkind IN ('r','v')`), so this leak is fixed by the `pg_class`
follow-up, not here — but the registry this design introduces is what makes that
fix a one-line change.

## Design

Make the SQLite views the single source of truth, delete the Rust handlers, and
identify internal relations by exact name rather than by prefix.

### Component 1: internal-relation registry

New module `src/catalog/internal_relations.rs` holding a `const` list of the
relation names pgsqlite's migrations create — 4 tables, 24 views, 8 indexes —
each tagged with its namespace OID (11 `pg_catalog`, 13000 `information_schema`).

This is the authoritative answer to "is this ours?". It supersedes the
`LIKE 'pg\_%'` heuristic for the `information_schema` views in this change, and
for `pg_class` in the follow-up. It is a static list because the migration
registry is static; adding a catalog relation to a future migration means adding
a line here, and the drift test below fails loudly if the two disagree.

Exposed to SQL as one UDF in `src/functions/catalog_functions.rs`:

- `__pgsqlite_relnamespace(name TEXT) -> INTEGER` — returns `11`, `13000`, or
  `2200` for anything not in the list.

### Component 2: type-resolution UDFs

The handler being deleted resolves a column's PostgreSQL type through
`map_sqlite_type_to_pg_column_info` (`src/catalog/query_interceptor.rs:2190`),
a private function returning
`(data_type, character_maximum_length, numeric_precision, numeric_scale)` from
the declared type string. Three of those four are `NULL` in the current view, so
deleting the handler without replacing them would regress `VARCHAR(50)` to no
length and `NUMERIC(10,2)` to no precision — both read by Django and SQLAlchemy.

Move that function to `src/catalog/column_type_info.rs` as
`pub fn pg_column_info(pg_type: &str) -> PgColumnInfo`, fixing the gaps the
measurement above exposed (`SERIAL`, `TIMESTAMPTZ`, `TIME`/`TIMETZ`, array
suffixes), and expose it as four UDFs in `src/functions/catalog_functions.rs`:

- `__pgsqlite_pg_data_type(pg_type TEXT) -> TEXT` — the SQL-standard `data_type`
  spelling: `integer`, `character varying`, `timestamp with time zone`, `ARRAY`
- `__pgsqlite_char_max_length(pg_type TEXT) -> INTEGER | NULL`
- `__pgsqlite_numeric_precision(pg_type TEXT) -> INTEGER | NULL`
- `__pgsqlite_numeric_scale(pg_type TEXT) -> INTEGER | NULL`

Deriving from the type string rather than an OID is deliberate: an OID cannot
carry the `(50)` or `(10,2)` modifier those last three columns need.

All five UDFs register through `functions::register_all_functions`
(`src/functions/mod.rs:20`), so every connection gets them, including the one
`db.query()` uses for the fall-through path.

### Component 3: migration v29

Two view redefinitions, following the v28 pattern of `DROP VIEW` + `CREATE VIEW`
with a `down` that restores the v14 definitions verbatim. `pg_class` and
`pg_attribute` are both deliberately left alone — see Non-goals.

- **`information_schema_tables`** — `table_schema` becomes
  `__pgsqlite_relnamespace(relname)` resolved through `pg_namespace.nspname`,
  replacing `'public' as table_schema`. It reads the UDF directly rather than
  `pg_class.relnamespace`, so it does not inherit v28's prefix heuristic.
- **`information_schema_columns`** — rebuilt on `sqlite_master`,
  `pragma_table_info`, and `__pgsqlite_schema` directly, the same sources
  `pg_attribute` reads, rather than layering on `pg_attribute`. This keeps v29
  off a view every ORM reads for column reflection, and is what makes the four
  recovered columns reachable:

  | column | source |
  | --- | --- |
  | `table_schema` | `__pgsqlite_relnamespace(m.name)` → `pg_namespace.nspname` |
  | `column_default` | `pragma_table_info.dflt_value` |
  | `is_nullable` | `pragma_table_info.notnull` **or** `pk` — an `INTEGER PRIMARY KEY` is `NOT NULL`, which the current view gets wrong and `tests/information_schema_test.rs:183` asserts |
  | `data_type`, `udt_name` | `__pgsqlite_pg_data_type(COALESCE(s.pg_type, p.type))` |
  | `character_maximum_length`, `character_octet_length` | `__pgsqlite_char_max_length(...)` |
  | `numeric_precision`, `numeric_scale` | `__pgsqlite_numeric_precision/scale(...)` |

  `COALESCE(s.pg_type, p.type)` preserves behavior for tables with no
  `__pgsqlite_schema` row (databases created outside pgsqlite).

`information_schema_tables` continues to select `FROM pg_class` for the relation
list and `relkind`; only the namespace derivation bypasses it.

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
                  ├─ pg_class          (relation list, relkind)
                  └─ __pgsqlite_relnamespace(relname) → pg_namespace.nspname
```

### Error handling

- `__pgsqlite_relnamespace` returns `2200` for any unlisted name, so an
  unrecognized relation is treated as a user table — the safe direction, since
  the failure mode is showing an internal relation rather than hiding a user's.
- `__pgsqlite_pg_data_type` returns `text` for unmapped type strings, matching
  both the current view's `ELSE 'text'` and the handler's final fallback.
- `__pgsqlite_char_max_length`, `__pgsqlite_numeric_precision` and
  `__pgsqlite_numeric_scale` return `NULL` when the type carries no modifier —
  `NULL` is what `information_schema` specifies for a type without one.
- All five UDFs return `NULL` on `NULL` input rather than erroring, so a view
  row with a missing `__pgsqlite_schema` entry degrades instead of failing the
  query.
- Migration `down` restores the v14 `information_schema_*` definitions verbatim.
  `pg_class` and `pg_attribute` are untouched by v29, so their v28 and v26
  definitions survive both directions.

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
4. **Recovered columns** — `VARCHAR(50)` reports
   `character_maximum_length = 50`, `NUMERIC(10,2)` reports precision 10 /
   scale 2, `DEFAULT true` reports a non-`NULL` `column_default`, and an
   `INTEGER PRIMARY KEY` reports `is_nullable = 'NO'`. These guard the four
   columns the handler populates and the current view does not.
5. **Exact-name matching** — a user table named `pg_myreport` reports
   `table_schema='public'` in `information_schema.tables`. The same test asserts
   it is still misfiled in `pg_class` under v28's heuristic, documenting the
   known divergence until the follow-up lands; that assertion flips there.
6. **No regressions** — `tests/information_schema_test.rs`,
   `tests/information_schema_comprehensive_test.rs`,
   `tests/orm_constraint_discovery_test.rs`, `tests/permission_functions_test.rs`
   and the SQLAlchemy suite stay green.
7. **Migration `down`** — v29 supplies one restoring the v14 views verbatim, per
   repo convention. It cannot be tested: no rollback path exists anywhere in
   `src/migration/`, and `Migration::down` is never executed. The convention is
   worth following so a future rollback runner finds it populated, but the plan
   should not claim a round-trip test it cannot run.
8. **Registry drift** — the internal-relation list matches exactly what a
   migrated-to-head database contains, so a future migration that adds a catalog
   relation without updating the list fails here rather than silently leaking it.

## Non-goals

- **`pg_class` namespace assignment.** v28's `LIKE 'pg\_%'` heuristic stays, so a
  user table named `pg_myreport` remains misfiled in `pg_class` and the eight
  `idx_*` indexes keep leaking into `\di`. Split out to keep this change off the
  path of `\dt`, `\di`, and every ORM introspection query one week after #87
  landed there. The registry and UDF this design introduces make the follow-up a
  one-expression swap. Tracked as
  [#102](https://github.com/erans/pgsqlite/issues/102).
- **`pg_attribute.atttypid`.** Still derived from the SQLite declared type, so
  `\d` and ORM column reflection keep reporting `text` for `NUMERIC`, `UUID` and
  `JSONB`. `information_schema_columns` no longer reads `pg_attribute`, so #88 is
  fully fixed without touching it — and the same blast-radius argument that split
  `pg_class` out applies here. Folded into
  [#102](https://github.com/erans/pgsqlite/issues/102).
- **Columns of views.** `information_schema.columns` reports nothing for a view,
  since the rebuilt view keeps the handler's `type = 'table'` restriction — true
  today on both paths. Follow-up issue.
- **ENUM columns.** `data_type` reports `text` rather than `USER-DEFINED` with
  `udt_name` set to the enum type. Unchanged from today; a scalar UDF cannot
  reach `EnumMetadata`, which needs the connection.
- **Stripping the `information_schema_` prefix** from `table_name`, so
  `information_schema.tables` reports `information_schema_tables` rather than
  PostgreSQL's `tables`. Cosmetic; no client filters on it.
- **The remaining `information_schema` Rust handlers** —
  `key_column_usage`, `table_constraints`, `referential_constraints`, `routines`,
  `views`, `schemata` keep their handlers and very likely share these defects.
  Out of scope; worth an umbrella issue once this lands and the pattern is
  proven twice.

## Follow-ups

1. **`pg_class` exact-name namespacing + `pg_attribute.atttypid`** —
   [#102](https://github.com/erans/pgsqlite/issues/102). Swap v28's prefix `CASE`
   for `__pgsqlite_relnamespace(name)`, and resolve `atttypid` from
   `__pgsqlite_schema`. Fixes the `pg_myreport` misfiling, the eight leaking
   `idx_*` indexes, and `\d` type reporting; removes the divergence test 5
   documents.
2. **View columns in `information_schema.columns` and `pg_attribute`** — extend
   beyond `type = 'table'`. To be filed.
3. **Umbrella: remaining `information_schema` handlers** — audit the other six
   for the same `ORDER BY` / aggregate / predicate defects. To be filed once this
   lands.

