# Serve `pg_class` from SQLite instead of a Rust handler

Issue: [#87](https://github.com/erans/pgsqlite/issues/87)
Date: 2026-08-10
Status: Approved, ready for implementation planning

## Problem

`\dt` reports "Did not find any tables" on a database that has tables. The table
exists and is visible through other routes — `SELECT name FROM sqlite_master`
returns it, and so does `information_schema.tables`.

### Root cause

psql expands `\dt` to a query containing `n.nspname !~ '^pg_toast'`. Three
mechanisms combine to turn that predicate into a universal row filter:

1. `RegexTranslator` runs *before* catalog interception
   (`src/catalog/query_interceptor.rs:101`) and rewrites the regex operator into
   a function call. Confirmed in the debug log:

   ```
   INTERCEPT: RegexTranslator changed query to:
     'SELECT c.relname FROM pg_class AS c WHERE NOT regexp('^pg_toast', c.relname)'
   ```

2. `WhereEvaluator` has no case for `regexp()`. Unknown functions fall through to
   `true` — "default to including the row"
   (`src/catalog/where_evaluator.rs:53-56`).

3. `NOT` inverts that default into an exclusion
   (`src/catalog/where_evaluator.rs:151`): `!true` is `false`. Every row fails.

The dedicated `PGRegexMatch` / `PGRegexNotMatch` arms at
`where_evaluator.rs:131-136` are dead code on this path — the operator is gone
before the AST reaches them.

The mechanism was confirmed by prediction rather than by reading alone. An
arbitrary unknown function behaves identically:

| Query | Predicted | Actual |
| --- | --- | --- |
| `WHERE foobar(c.relname)` | 13 rows | 13 rows |
| `WHERE NOT foobar(c.relname)` | 0 rows | 0 rows |
| `WHERE c.relname ~ '^cust'` | all rows (ignored) | 13 rows |
| `WHERE c.relname !~ '^pg_toast'` | 0 rows | 0 rows |

The third row is a second, unreported bug: **`~` is silently ignored on catalog
tables**, returning unfiltered results with no error.

### Two further defects behind it

A narrow fix to the regex handling alone would not fix `\dt`.

**Joined columns do not resolve.** `PgClassHandler` synthesizes only `pg_class`
columns, so the `LEFT JOIN pg_namespace` is never materialized and `n.nspname`
evaluates to `None`:

| Query | Correct | Actual |
| --- | --- | --- |
| `n.nspname = 'public'` | 5 rows | 0 rows |
| `n.nspname <> 'public'` | 0 rows | 5 rows |

Both are wrong. `\dt`'s `n.nspname <> 'pg_catalog'` passes only by accident
(`None != Some(..)` is true). Once `regexp` were understood,
`evaluate_regex_match` returns `false` for an unresolvable value
(`where_evaluator.rs:267-269`) and `\dt` would still return zero rows.

**JOINs are not executed at all.** `pg_class JOIN pg_constraint ON con.conrelid
= c.oid` returns all 13 `pg_class` rows with no `conname` column. The join
predicate is never evaluated.

### The underlying shape

`src/catalog/` is ~9,300 lines of hand-rolled query engine — WHERE evaluation,
projection, join handling — sitting on top of catalog relations that already
exist as real SQLite views. Every gap in that reimplementation is a silent
wrong-answer bug. #87 is one symptom.

## Approach

Delete `PgClassHandler` and let SQLite execute `pg_class` queries. Joins, `WHERE`,
`regexp()`, `ORDER BY`, and projection all become the engine's job.

Everything required is already present, verified end-to-end:

- `pg_class` and `pg_namespace` exist as SQLite views (`migration/registry.rs`).
- `regexp`, `pg_table_is_visible`, and `pg_get_userbyid` are registered UDFs
  (`functions/regex_functions.rs:11`, `functions/catalog_functions.rs:11`,
  `functions/system_functions.rs:373`).
- `SchemaPrefixTranslator` and `RegexTranslator` already run on the
  non-intercepted path (`query/unified_processor.rs:390,430`).

Run against the raw views, the `\dt` query returns the correct answer today:

```
Schema      Name       kind
------  -------------  ----
public  customers      r
```

Rejected alternatives:

- **Bail out of interception on hard queries** — the bail-list is itself a
  guess-list, and any gap means interception happens when it shouldn't, i.e. the
  wrong answers persist.
- **Try SQLite, fall back to Rust on error** — a query that *succeeds* but
  returns wrong data never triggers the fallback, so silent bugs survive.

Deleting the handler leaves one code path and no decision logic to drift. A
missing column becomes a loud SQLite error rather than silent wrong rows.

## Scope

`pg_class` only. The other catalog handlers keep working untouched. This
establishes the pattern for later per-catalog specs.

## Design

### Migration v28: recreate the `pg_class` view

The migration registry currently runs through v27 (`register_v27_fix_pg_proc_types`),
so this work lands as **v28**. The current `pg_class` view is owned by v26
(`register_v26_enhanced_pg_attribute_support`, `registry.rs:2544`), so v28 must
`DROP VIEW IF EXISTS pg_class` before recreating it. Note that CLAUDE.md's
"Current Migrations (v1-v25)" list is stale and should be corrected to v28 as
part of this work.

The current view has 25 columns; the Rust handler serves 33. Add the nine
missing: `reltype`, `reloftype`, `relnatts`, `relchecks`, `relhasrules`,
`relhastriggers`, `relhassubclass`, `relrowsecurity`, `relforcerowsecurity`.

Computed rather than hardcoded:

| Column | Source |
| --- | --- |
| `relnatts` | `(SELECT COUNT(*) FROM pragma_table_info(m.name))` |
| `relhastriggers` | `EXISTS(SELECT 1 FROM sqlite_master t WHERE t.type='trigger' AND t.tbl_name = m.name)` |
| `reltype` | `oid + 1`, matching current Rust behavior |
| `relkind` | `table` → `r`, `view` → `v`, `index` → `i` |

`relchecks` stays `0`, matching today's Rust behavior. Deriving it from
`pg_constraint` is out of scope.

Three existing view bugs are fixed in passing:

- `relkind_full` is not a real PostgreSQL column — drop it.
- `relreplident` should be `'d'`, not `'v'`.
- `relispartition` should be `'f'`, not `'t'`.

### Namespace assignment

`relnamespace` becomes conditional so psql's own `n.nspname <> 'pg_catalog'`
predicate hides internal relations, with no special-casing anywhere in pgsqlite:

- `pg_%` → `11` (`pg_catalog`)
- `information_schema_%` → `13000` (`information_schema`)
- everything else → `2200` (`public`)

`information_schema` is a new row in the `pg_namespace` view — one extra
`UNION ALL`, and more correct than lumping those relations into `pg_catalog`.

`sqlite_%` and `__pgsqlite_%` remain excluded from the view entirely, as today.

Accepted limitation: a user table legitimately named `pg_foo` is misfiled into
`pg_catalog`. PostgreSQL reserves the prefix, so this is acceptable.

### Remove the interception branch

Delete the `pg_class` branch from `query_interceptor.rs`, and delete
`src/catalog/pg_class.rs` (372 lines) including its `generate_oid_from_name`.

### OID consistency

No formula change, and no data migration.

Three OID formulas exist in the codebase today:

| Formula | Used by | Stable? |
| --- | --- | --- |
| unicode of first 3 chars + length | views, `constraint_populator.rs:97` | yes |
| `hash31` | `pg_class.rs`, `pg_attribute.rs`, `pg_sequence.rs`, `pg_trigger.rs` | yes |
| `oid_hash` UDF (Rust `DefaultHasher`) | registered, little used | **no** |

OIDs are persisted: `pg_constraint.conrelid`, `pg_index.indrelid`,
`pg_attrdef.adrelid`, and `pg_depend.objid`/`refobjid` all store table OIDs. The
persisted values use the unicode formula, while `PgClassHandler` serves `hash31`
— so `pg_class` and the stored catalogs already disagree today. Measured on a
fresh database, `pg_class` reports `customers` as `578453`, while the unicode
formula used for persisted OIDs yields `197947`.

Serving `pg_class` from the view therefore *removes* an existing inconsistency.
The unicode formula becomes canonical because it is already the truth on disk.

Because `pg_class` OIDs change value (from `hash31` to unicode), any existing
test asserting a specific `pg_class` OID must be updated. This is expected fallout,
not a regression — the new values are the ones the rest of the catalog already uses.

Two known problems are deliberately left alone and filed separately:

- The unicode formula collides whenever two names share their first three
  characters and length. Demonstrated: `orders`/`orderz`, `user_roles`/`user_rolez`,
  and `customers`/`customerz` each produce one OID. Pre-existing, not introduced
  here.
- `oid_hash` uses `DefaultHasher`, which is not stable across Rust releases, so
  OIDs could change on rebuild.

### Deliberately not fixed

`WhereEvaluator`'s `NOT`-inverts-unknown bug (`where_evaluator.rs:53-56,151`)
still affects the ~20 remaining handlers. Fixing it in this change would be
unverifiable — with `pg_class` gone, the `\dt` test cannot exercise it. It
belongs to whichever catalog migrates next, or to its own issue.

## Failure modes

**A column the view lacks.** Surfaces as a loud SQLite `no such column` error,
not silent wrong rows — the property that makes this approach preferable to the
rejected alternatives. Guarded by a test asserting all 33 columns are selectable.

**`trusted_schema`.** `pragma_table_info` inside a view requires
`trusted_schema=ON`. That is the C API default and pgsqlite does not override
it, but the dependency is implicit: the `sqlite3` CLI disables it and rejects the
view with `unsafe use of virtual table "pragma_table_info"`. Set the pragma
explicitly where the per-session connection registers its UDFs, and cover it with
a test.

**Performance.** Two correlated subqueries per row. `pg_class` is small;
acceptable.

## Testing

Written test-first, beginning with a failing test for #87.

1. Regression test for #87: psql's exact `\dt` query returns `customers` and not
   `pg_constraint`, `pg_attrdef`, `pg_index`, or `pg_depend`.
2. `~` filters correctly, and `!~` does not zero out results — the two bugs found
   during investigation.
3. Cross-catalog join `pg_class JOIN pg_constraint ON conrelid = oid` returns
   rows.
4. All 33 columns are selectable; `relnatts` matches real column counts.
5. The 34 existing catalog test files pass, with the sole expected exception of
   assertions on literal `pg_class` OID values, which move to the unicode formula.

## Follow-up issues to file

- Collision-resistant, stable OID function shared by every view and Rust site;
  retire `oid_hash`'s `DefaultHasher`.
- `WhereEvaluator` unknown-predicate handling under `NOT`, for the remaining
  handlers.
- Migrate the next catalog handler to SQLite using this pattern.
