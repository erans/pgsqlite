# Hide internal `__pgsqlite_*` tables behind a flag

Issue: [#80](https://github.com/erans/pgsqlite/issues/80)
Date: 2026-08-05
Status: Approved, ready for implementation planning

## Problem

pgsqlite stores its bookkeeping in tables named `__pgsqlite_*`. Every catalog
query pgsqlite synthesizes itself already filters them out, but a
`sqlite_master` (or `sqlite_schema`) query written directly by a client is
passed through verbatim. Schema browsers, ORMs, and any tool that lists the
native SQLite catalog therefore see pgsqlite's internals mixed in with the
user's own tables. The reporter runs a hosted SQLite product on top of pgsqlite
and has had users conclude their data was corrupted.

### Observed leak (v0.0.22, fresh database with one user table)

A client `SELECT type, name FROM sqlite_master` returns 37 internal rows
alongside `customers`:

| Kind | Count | Examples |
| --- | --- | --- |
| `__pgsqlite_*` tables | 15 | `__pgsqlite_schema`, `__pgsqlite_migrations`, `__pgsqlite_enum_values` |
| Indexes without the prefix | 8 | `idx_enum_values_label`, `idx_comments_lookup` |
| Implicit unique indexes | 14 | `sqlite_autoindex___pgsqlite_schema_1` |

The last two groups matter: a `name LIKE '__pgsqlite_%'` test matches only 15 of
the 37 rows. All 37 are caught by additionally testing `tbl_name`, which every
index row in `sqlite_master` carries.

## Scope

Hidden: `__pgsqlite_*` tables and the index rows owned by them. Nothing else.

Not hidden, and not addressed by this work:

- The materialized `pg_*` and `information_schema_*` relations (4 real tables,
  24 views) that pgsqlite creates. Filtering those is a broader policy question
  because a user may legitimately name a table `pg_something`.
- `PRAGMA table_list`, which returns the internal tables to a client that asks
  for it. It is a fixed-shape result needing a different mechanism (row
  filtering), and the tools causing the reported confusion read `sqlite_master`.

"Hidden" means hidden from listings only. `SELECT * FROM __pgsqlite_schema`
continues to work when the table is named explicitly, so a live deployment
remains debuggable over the wire and pgsqlite's own access is unaffected.

## Two problems found while scoping, filed separately

Neither is caused by this change and neither is fixed by it.

1. `\dt` reports "Did not find any tables" on a database that has a user table.
2. `information_schema.tables` lists all 27 materialized `pg_*` /
   `information_schema_*` relations next to the user's tables.

## Flag

```rust
#[arg(long, env = "PGSQLITE_HIDE_INTERNAL_TABLES",
      help = "Hide pgsqlite's internal __pgsqlite_* tables from client sqlite_master queries")]
pub hide_internal_tables: bool,
```

Added to `src/config.rs` alongside the existing options and read through the
existing `CONFIG` lazy_static. Default false: internal tables stay visible
unless the operator opts in. Server-wide and fixed at startup; there is no
per-session override, because pgsqlite has no GUC/`SET` plumbing today
(`__pgsqlite_session_settings` exists as a migration artifact but nothing reads
it, and no `Statement::SetVariable` handler exists), and building it is a
feature in its own right.

## Mechanism: relation substitution

A new `src/translator/sqlite_master_filter.rs` exposes

```rust
pub fn translate(query: &str) -> Cow<'_, str>
```

following the shape of the existing `SchemaPrefixTranslator`.

**Gate, cheapest test first.** Return the input borrowed if the flag is off, or
if the lowercased query contains neither `sqlite_master` nor `sqlite_schema`.
Only past that gate is the query parsed.

**Rewrite.** Parse with `PostgreSqlDialect`. Walk the statement for every
`TableFactor::Table` naming `sqlite_master` or `sqlite_schema`, bare or
qualified with `main` or `temp`, wherever it appears: joins, subqueries, CTEs,
set operations, `EXISTS`. Replace each such relation with a derived table:

```sql
(SELECT * FROM sqlite_master
 WHERE substr(name, 1, 11) <> '__pgsqlite_'
   AND (tbl_name IS NULL OR substr(tbl_name, 1, 11) <> '__pgsqlite_')) AS sqlite_master
```

If the client gave the relation its own alias, that alias is preserved instead
so qualified references such as `m.name` keep resolving.

Two details behind the predicate:

- `substr(...) <> '__pgsqlite_'` rather than `LIKE '__pgsqlite_%'`. In `LIKE`,
  `_` is a single-character wildcard, so the `LIKE` form also matches unrelated
  names. Existing code in the repository uses the `LIKE` form; new code should
  not.
- The `tbl_name` half is what hides the 22 index rows whose own names carry no
  `__pgsqlite_` prefix.

**Why substitution rather than the alternatives.** The client's WHERE clause,
projections, and joins are never modified, so there is no predicate to splice
and nothing to corrupt — the failure mode that sank the earlier query-rewriting
attempt (PR #82) on large `NOT IN` lists and `ESCAPE` clauses. Because the
filtering happens inside the relation, `count(*)`, joins, subqueries, `EXISTS`,
and `SELECT sql FROM sqlite_master` are all correct with no extra handling.

The alternative considered and rejected was filtering result rows after running
the query verbatim (PR #83). It cannot corrupt SQL and is already written, but
it sees only what the client projected, needs a probe query per call to map
`name` to `tbl_name`, and silently returns unfiltered results for `count(*)`,
joins, subqueries, and `SELECT sql FROM sqlite_master`. A schema browser
displaying DDL is an ordinary thing to do and would dump every internal
`CREATE TABLE` unfiltered. For a flag whose contract is "hidden", silent partial
coverage is the wrong trade.

Also rejected: moving the internal tables into an ATTACHed sidecar database.
It is the only approach that also holds when someone opens the file directly
with `sqlite3`, but it makes a pgsqlite database two files instead of one and
requires a large migration.

## Hook points

The rewrite applies to client queries only.

| Protocol | Location |
| --- | --- |
| Simple | `src/query/executor.rs:281`, in or immediately after `preprocess_query` within `execute_single_statement` — ahead of the SELECT branch at :326 and ahead of the wire-protocol cache |
| Extended | `src/query/extended.rs`, top of `handle_parse` (:82), before the `#[cfg(not(feature = "unified_processor"))]` translation block at :420-450 so the filter is not feature-gated |

The filter must not be placed at `DbHandler::process_query`
(`src/session/db_handler.rs:485`), despite that being the single chokepoint all
queries pass through. pgsqlite's own code probes `sqlite_master` for its
internal tables in at least `migration/runner.rs:61,302,310,339`,
`metadata/enum_metadata.rs:203,341,389`, `rewriter/enum_rewriter.rs:26`,
`cache/lazy_schema_loader.rs:124`, and `cache/schema.rs:117`. Filtering there
would make `SELECT 1 FROM sqlite_master WHERE name='__pgsqlite_metadata'` return
nothing and pgsqlite would re-run its migrations on every start.

## Failure handling

Fail open. A parse failure, or any AST shape the walker does not recognize,
returns the query unchanged rather than raising an error: failing to hide a row
is cosmetic, rejecting a client's query is not.

The AST is re-rendered through sqlparser's `Display` only when a `sqlite_master`
reference was actually found and replaced; every other query is returned
borrowed and untouched. Round-tripping drops comments and may normalize exotic
syntax, which is acceptable for the narrow class of queries that reference
`sqlite_master`.

## Tests

Unit tests on the translator:

- Rewrite shapes: bare relation, aliased relation, `main.`/`temp.` qualified,
  join, subquery, CTE, `count(*)`, `SELECT sql`, `sqlite_schema` spelling.
- No-op cases: flag off, no `sqlite_master` reference, unparseable input.
- Predicate correctness: a user table named `abpgsqliteX`, which
  `LIKE '__pgsqlite_%'` matches but `substr(name, 1, 11) = '__pgsqlite_'` does
  not, is still returned.

Wire-level integration tests, mirroring the issue's reproduction, run in both
flag states:

- Flag on: none of the 37 internal rows appear; the user's table, its indexes,
  and its views do; `SELECT count(*) FROM sqlite_master` counts only visible
  rows; `SELECT sql FROM sqlite_master` returns no internal DDL.
- Flag off: all 37 internal rows still appear, proving the default is unchanged.
