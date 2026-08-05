# Extend `--hide-internal-tables` to `CREATE VIEW` and `CREATE TABLE ... AS SELECT`

Issue: [#86](https://github.com/erans/pgsqlite/issues/86)
Follow-up to: [#80](https://github.com/erans/pgsqlite/issues/80) (the flag), [#85](https://github.com/erans/pgsqlite/issues/85) (narrowing to read contexts)
Date: 2026-08-05
Status: Approved, ready for implementation planning

## Problem

`SqliteMasterFilter::translate` rewrites client `sqlite_master` references in two
statement positions only: `Statement::Query`, and the `source` of
`Statement::Insert`. A view body is neither, so with the flag on:

```sql
CREATE VIEW v AS SELECT name FROM sqlite_master;
SELECT * FROM v;  -- unfiltered catalog, including __pgsqlite_* rows
```

The second statement never mentions `sqlite_master`, so nothing filters it. The
view's stored definition references the raw catalog, and SQLite expands that
definition on every read.

`CREATE TABLE t AS SELECT ... FROM sqlite_master` has the same gap.

The restriction to those two positions was deliberate. Before #85,
`DELETE FROM sqlite_master WHERE name='zzz'` was rewritten into syntactically
invalid SQL, and the resulting SQLite error pasted the internal prefix straight
back to the client:

```
SQLite error: near "(": syntax error in DELETE FROM (SELECT * FROM sqlite_master WHERE SUBSTR(name, 1, 11) <> '__pgsqlite_' ...
```

Narrowing to read contexts fixed that but dropped view and CTAS bodies along
with the writes.

### Severity

Not a security hole. Per the #80 design, hiding is listing-only and
`SELECT * FROM __pgsqlite_schema` keeps working by name, so nothing becomes
reachable that was not already. The flag exists so end users do not stumble onto
internal tables and conclude their data is corrupt, and a schema browser that
creates views defeats that.

## Change

Two arms added to the `match` in `SqliteMasterFilter::translate`
(`src/translator/sqlite_master_filter.rs:88`):

```rust
Statement::CreateView { query, .. } => {
    let _ = query.visit(&mut visitor);
}
Statement::CreateTable(create_table) => {
    if let Some(query) = create_table.query.as_mut() {
        let _ = query.visit(&mut visitor);
    }
}
```

Nothing else moves. The visitor, the `substr(...)` predicate, the alias
handling, the cheap substring gate, the fail-open behavior, and both wire hook
points are unchanged. This only widens which AST positions the existing visitor
is pointed at.

`CREATE MATERIALIZED VIEW` and `CREATE TEMP VIEW` are covered for free: same
`Statement::CreateView` variant, same `query` field.

The `_ => {}` arm keeps its existing comment, extended with the rule that
governs this and any future statement kind:

> Rewrite where the filtered rows are read back; do not rewrite where filtering
> would change which rows a write touches.

## Decisions

### The filter predicate is persisted into view definitions

SQLite stores the literal `CREATE VIEW` statement text in `sqlite_master.sql`.
Rewriting the body means a client reading the definition back sees:

```sql
CREATE VIEW v AS SELECT name FROM (SELECT * FROM sqlite_master
  WHERE SUBSTR(name, 1, 11) <> '__pgsqlite_'
    AND (tbl_name IS NULL OR SUBSTR(tbl_name, 1, 11) <> '__pgsqlite_')) AS sqlite_master
```

That is the flag's own purpose working against itself: the user gets back a
definition full of `__pgsqlite_`. Accepted anyway.

The alternative considered was adding a migration for an internal
`__pgsqlite_visible_master` view and rewriting `sqlite_master` to that name,
which keeps the stored DDL short. Rejected: it makes a pgsqlite-managed object
load-bearing for *user* schema. A future migration that renames or rebuilds it
silently breaks the user's view, and anyone who opens the database file with
plain `sqlite3` after moving off pgsqlite inherits a view referencing an
internal name. The inline predicate is ordinary SQL that works anywhere with no
pgsqlite present. It also needs no migration, no new schema object that exists
even when the flag is off, and no second shape for
`is_generated_filter_subquery` to recognize.

There is precedent for not returning view text verbatim: PostgreSQL's
`pg_get_viewdef` returns a normalized rewrite, not what the user typed.

Not doing the rewrite at all and fixing CTAS only was also rejected — it leaves
the issue's own reproduction open.

### The rewrite stays an allowlist

Two wider scopes were considered and rejected.

**Also visiting the read-only fields of `UPDATE`/`DELETE`** (`selection`,
`assignments`, `using`, `returning`, never the target relation) is cheap and
carries no invalid-SQL risk, because it is still an allowlist. Rejected because
it changes what a write *does* rather than what a user *sees*: filtering
`DELETE FROM my_log WHERE table_name IN (SELECT name FROM sqlite_master)`
silently deletes fewer rows than the user's SQL says. That is a worse outcome
than an unfiltered listing and is not what the flag promises.

**Inverting to a denylist** — visit every statement wholesale, skip only known
write targets — is future-proof against new statement kinds, and was rejected on
failure-mode asymmetry. An allowlist that misses a statement kind fails into
*not filtered*, which is cosmetic and can be filed as a follow-up. A denylist
that misses a write target fails into *invalid SQL whose error text leaks the
internal prefix*, which is #85 verbatim. sqlparser 0.57 already carries a live
landmine for this: `Statement::Merge`'s target is itself a `TableFactor`, so the
visitor would substitute a derived table for a MERGE target. The module's
existing doc comment states the same principle — "failing to hide a row is
cosmetic, rejecting a client's query is not."

### Views bake in the filter permanently

Because the rewrite happens at creation time and SQLite persists the text, a
view created while the flag is on keeps filtering after the operator turns the
flag *off*; symmetrically, a view created while the flag was off keeps leaking
after it is turned on. This is inherent to rewriting at creation time. The
alternative — resolving and rewriting view bodies at read time — is a much
larger mechanism, and the flag is a startup-fixed server-wide setting, so
flipping it on a live database is already a rare event.

### CTAS leaves no trace

SQLite stores the expanded column list for `CREATE TABLE t AS SELECT ...`
(`CREATE TABLE t(name TEXT)`), not the select. So a CTAS is a clean one-shot
filtered copy with no visible DDL change, and the concern above applies to views
only.

## What does not change

**The SQL injection detector needs no changes.** `analyze_statement`
(`src/security/sql_injection_detector.rs:126`) handles `Statement::CreateTable`
in the DDL arm without recursing into it, and `Statement::CreateView` falls
through to `_ => {}`. Neither descends into the body, so the derived table
spliced into a view or CTAS source cannot trip the `depth > 1` system-table rule
that required the `is_generated_filter_subquery` escape hatch for the plain
`SELECT` path in #85. That hatch stays exactly as it is, serving the query path.

**Hook points.** Unchanged: `preprocess_query` in `src/query/executor.rs:40` for
the simple protocol, `handle_parse` in `src/query/extended.rs:94` for the
extended protocol. DDL reaches both.

**`EXPLAIN` and `DECLARE ... CURSOR`** remain excluded. pgsqlite has no handler
for either.

## Error handling

Unchanged — fail open. The new arms introduce no new failure path:

- Unparseable `CREATE VIEW` returns the input borrowed; the view leaks. Cosmetic,
  and consistent with every other path in this module.
- `visitor.replaced == 0` returns the input borrowed. This matters more than
  before: `CREATE TABLE sqlite_master_backup (id INT)` passes the cheap
  substring gate, parses to a `CreateTable` with `query: None`, replaces
  nothing, and must come back untouched. The new arm must not perturb ordinary
  DDL.

## Tests

### Unit — `src/translator/sqlite_master_filter.rs`

Alongside the existing tests, reusing the `FILTERED` constant:

| Test | Asserts |
| --- | --- |
| `rewrites_create_view_body` | `CREATE VIEW v AS SELECT name FROM sqlite_master` — body carries the derived table |
| `rewrites_create_table_as_select_source` | CTAS source rewritten |
| `rewrites_materialized_view` | `CREATE MATERIALIZED VIEW` covered by the same arm |
| `leaves_create_table_without_query_borrowed` | `CREATE TABLE sqlite_master_backup (id INT)` returns `Cow::Borrowed` |
| `leaves_create_view_over_attached_db_borrowed` | `otherdb.sqlite_master` in a view body untouched |

### Wire-level

The issue's reproduction, in both flag states.

`tests/sqlite_master_filter_enabled_test.rs`:

- Create `v` over `sqlite_master`, then `SELECT * FROM v` — no `__pgsqlite_*`
  rows, `customers` present. This is the test that would have caught the bug.
- Same for a CTAS snapshot table.

`tests/sqlite_master_filter_disabled_test.rs`:

- Same view with the flag off — internal rows still present, proving the default
  is unchanged.

No test asserts on the view's persisted `sql` text. That is the one thing
knowingly accepted as ugly, and pinning sqlparser's exact rendering into an
assertion makes the test fail on every sqlparser upgrade for no signal.
