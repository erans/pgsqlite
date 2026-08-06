# Extend `--hide-internal-tables` to `CREATE VIEW` and CTAS — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `--hide-internal-tables` also filter `sqlite_master` references inside `CREATE VIEW ... AS SELECT` and `CREATE TABLE ... AS SELECT`, closing [#86](https://github.com/erans/pgsqlite/issues/86).

**Architecture:** `SqliteMasterFilter::translate` already owns the whole mechanism — a cheap substring gate, a `VisitorMut` that swaps each `sqlite_master` `TableFactor::Table` for a filtered derived table, and a `match` that decides which statement positions the visitor is pointed at. This change adds two arms to that `match` and nothing else. The visitor, predicate, alias handling, fail-open behavior, and both wire hook points are untouched.

**Tech Stack:** Rust, `sqlparser` 0.57 (`ast::VisitMut` / `VisitorMut`, `PostgreSqlDialect`), `tokio-postgres` for wire-level tests.

**Spec:** `docs/superpowers/specs/2026-08-05-view-ctas-sqlite-master-filter-design.md`

## Global Constraints

- **Allowlist only.** Never convert the `match` in `translate` to a denylist that visits statements wholesale. `Statement::Merge`'s target is itself a `TableFactor`, and substituting a derived table there produces invalid SQL whose error text leaks `__pgsqlite_` to the client — the #85 regression.
- **Do not touch `Statement::Update` or `Statement::Delete`,** including their subquery positions. Filtering there would change which rows a write touches.
- **Fail open.** Never return an error from `translate`. Unhandled shape, parse failure, or nothing replaced ⇒ return the input `Cow::Borrowed`.
- **No changes to `src/security/sql_injection_detector.rs`.** `analyze_statement` does not descend into `CreateTable` or `CreateView` bodies, so the spliced derived table cannot trip the `depth > 1` rule.
- **Pre-commit checklist (from `CLAUDE.md`), all four, before every commit:** `cargo check` (no errors/warnings), `cargo clippy`, `cargo build`, `cargo test`.
- Prefix filter is `substr(name, 1, 11) <> '__pgsqlite_'`, never `LIKE '__pgsqlite_%'`.

---

### Task 1: Point the visitor at view and CTAS bodies

**Files:**
- Modify: `src/translator/sqlite_master_filter.rs:88-99` (the `match statement` block)
- Test: `src/translator/sqlite_master_filter.rs` (the `mod tests` block at the bottom of the same file)

**Interfaces:**
- Consumes: existing private items in this module — `RelationReplacer` (the `VisitorMut`), the `rewritten(&str) -> String` test helper, and the `FILTERED` test constant holding the rendered derived table.
- Produces: no signature changes. `pub fn translate(query: &str) -> Cow<'_, str>` keeps its exact shape; only which inputs it rewrites changes.

- [ ] **Step 1: Write the five failing tests**

Append to the `mod tests` block in `src/translator/sqlite_master_filter.rs`, after `rewrites_insert_select_source`:

```rust
    #[test]
    fn rewrites_create_view_body() {
        // The gap in #86: a view body is read back on every SELECT against the
        // view, and the client's later `SELECT * FROM v` never mentions
        // sqlite_master, so this is the only chance to filter it.
        let out = rewritten("CREATE VIEW v AS SELECT name FROM sqlite_master");
        assert_eq!(
            out,
            format!("CREATE VIEW v AS SELECT name FROM {FILTERED} AS sqlite_master")
        );
    }

    #[test]
    fn rewrites_create_table_as_select_source() {
        let out = rewritten("CREATE TABLE snapshot AS SELECT name FROM sqlite_master");
        assert_eq!(
            out,
            format!("CREATE TABLE snapshot AS SELECT name FROM {FILTERED} AS sqlite_master")
        );
    }

    #[test]
    fn rewrites_materialized_view() {
        // Same Statement::CreateView variant, same `query` field: free.
        let out = rewritten("CREATE MATERIALIZED VIEW mv AS SELECT name FROM sqlite_master");
        assert_eq!(
            out,
            format!("CREATE MATERIALIZED VIEW mv AS SELECT name FROM {FILTERED} AS sqlite_master")
        );
    }

    #[test]
    fn leaves_create_table_without_query_borrowed() {
        // Passes the cheap substring gate on the *table name*, parses to a
        // CreateTable with `query: None`, replaces nothing, and must come back
        // untouched. Guards the new arm against perturbing ordinary DDL.
        assert!(matches!(
            SqliteMasterFilter::translate("CREATE TABLE sqlite_master_backup (id INT)"),
            Cow::Borrowed(_)
        ));
    }

    #[test]
    fn leaves_create_view_over_attached_db_borrowed() {
        // Only `main.` and `temp.` name the SQLite catalog.
        assert!(matches!(
            SqliteMasterFilter::translate(
                "CREATE VIEW v AS SELECT name FROM otherdb.sqlite_master"
            ),
            Cow::Borrowed(_)
        ));
    }
```

- [ ] **Step 2: Run the tests to verify the right ones fail**

Run: `cargo test --lib sqlite_master_filter -- --nocapture`

Expected: exactly three failures — `rewrites_create_view_body`, `rewrites_create_table_as_select_source`, `rewrites_materialized_view`. Each fails on `assert_eq!` because the output still contains the bare `FROM sqlite_master` instead of the derived table.

The other two (`leaves_create_table_without_query_borrowed`, `leaves_create_view_over_attached_db_borrowed`) **pass already** — they are regression guards for the new arm, not drivers of it. If either one fails at this point, something else is wrong; stop and investigate rather than proceeding.

- [ ] **Step 3: Add the two match arms**

In `src/translator/sqlite_master_filter.rs`, replace the `match statement { ... }` block (currently lines 88-99) with:

```rust
            // Rewrite where the filtered rows are read back; never where
            // filtering would change which rows a write touches.
            //
            // `UPDATE`/`DELETE` name their target relation with the same
            // `TableFactor::Table`, and substituting a derived table there
            // produces syntactically invalid SQL whose error text would leak
            // the `__pgsqlite_` prefix straight back to the client. Writes to
            // `sqlite_master` are SQLite's to reject, unmodified. Their
            // subqueries are left alone too: filtering a `DELETE ... WHERE
            // name IN (SELECT ... FROM sqlite_master)` would silently delete
            // fewer rows than the client's SQL says.
            //
            // This stays an allowlist. A statement kind we miss is merely
            // unfiltered; a write target we fail to recognize is invalid SQL.
            match statement {
                Statement::Query(query) => {
                    let _ = query.visit(&mut visitor);
                }
                Statement::Insert(insert) => {
                    if let Some(source) = insert.source.as_mut() {
                        let _ = source.visit(&mut visitor);
                    }
                }
                // SQLite persists the literal CREATE VIEW text and expands it
                // on every read, so creation time is the only chance to filter.
                // Covers MATERIALIZED and TEMP views: same variant, same field.
                Statement::CreateView { query, .. } => {
                    let _ = query.visit(&mut visitor);
                }
                // `CREATE TABLE ... AS SELECT`. `query` is `None` for ordinary
                // CREATE TABLE, which is then left untouched.
                Statement::CreateTable(create_table) => {
                    if let Some(query) = create_table.query.as_mut() {
                        let _ = query.visit(&mut visitor);
                    }
                }
                _ => {}
            }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --lib sqlite_master_filter`

Expected: PASS, all tests in the module including the pre-existing ones. `leaves_delete_untouched` and `leaves_update_untouched` passing is the check that the allowlist stayed narrow.

- [ ] **Step 5: Run the pre-commit checklist**

```bash
cargo check && cargo clippy && cargo build && cargo test
```

Expected: no errors, no new warnings, all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/translator/sqlite_master_filter.rs
git commit -m "fix(hide-internal-tables): filter sqlite_master in CREATE VIEW and CTAS bodies

A view or CTAS built over sqlite_master read the unfiltered catalog: the
rewrite covered only Statement::Query and Insert sources, and the client's
later SELECT against the view never mentions sqlite_master, so nothing
filtered it.

Points the existing visitor at CreateView's query and CreateTable's AS
SELECT source. Stays an allowlist -- UPDATE/DELETE remain wholly untouched,
including their subqueries.

Refs #86"
```

---

### Task 2: Wire-level regression tests in both flag states

**Files:**
- Modify: `tests/sqlite_master_filter_enabled_test.rs` (append two tests)
- Modify: `tests/sqlite_master_filter_disabled_test.rs` (append one test)

**Interfaces:**
- Consumes: `SqliteMasterFilter::translate` behavior from Task 1, reached over the wire through `preprocess_query` (`src/query/executor.rs:287`). Both test files already define a local `table_names(&tokio_postgres::Client, &str) -> Vec<String>` helper that runs `simple_query` and collects column 0; reuse it, do not redefine it.
- Produces: nothing consumed by later tasks.

These are separate test *binaries* on purpose: `pgsqlite::config::set_hide_internal_tables(true)` sets process-global state, so the flag-off assertions have to live in a binary that never calls it.

- [ ] **Step 1: Write the failing flag-on tests**

Append to `tests/sqlite_master_filter_enabled_test.rs`:

```rust
#[tokio::test]
async fn hides_internal_objects_through_a_view() {
    pgsqlite::config::set_hide_internal_tables(true);

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)").await?;
            Ok(())
        })
    })
    .await;

    // The issue's reproduction. The SELECT below never mentions sqlite_master,
    // so the view body is the only place the filter can be applied.
    server
        .client
        .simple_query("CREATE VIEW schema_names AS SELECT name FROM sqlite_master")
        .await
        .expect("CREATE VIEW over sqlite_master should succeed");

    let names = table_names(&server.client, "SELECT name FROM schema_names ORDER BY name").await;
    assert!(
        !names.iter().any(|n| n.starts_with("__pgsqlite_")),
        "internal tables leaked through a view: {names:?}"
    );
    assert!(names.iter().any(|n| n == "customers"), "user table missing: {names:?}");
}

#[tokio::test]
async fn hides_internal_objects_through_create_table_as_select() {
    pgsqlite::config::set_hide_internal_tables(true);

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total TEXT)").await?;
            Ok(())
        })
    })
    .await;

    server
        .client
        .simple_query("CREATE TABLE catalog_snapshot AS SELECT name FROM sqlite_master")
        .await
        .expect("CTAS over sqlite_master should succeed");

    let names = table_names(
        &server.client,
        "SELECT name FROM catalog_snapshot ORDER BY name",
    )
    .await;
    assert!(
        !names.iter().any(|n| n.starts_with("__pgsqlite_")),
        "internal tables leaked into a CTAS snapshot: {names:?}"
    );
    assert!(names.iter().any(|n| n == "orders"), "user table missing: {names:?}");
}
```

- [ ] **Step 2: Run them to verify they fail against pre-Task-1 code**

Because Task 1 is already committed at this point, these would pass immediately. To confirm they actually exercise the fix rather than passing vacuously, run them once against the pre-fix translator. This assumes Task 1's commit is `HEAD` and nothing else has been committed since; verify with `git log --oneline -1` first.

```bash
git checkout HEAD~1 -- src/translator/sqlite_master_filter.rs
cargo test --test sqlite_master_filter_enabled_test hides_internal_objects_through
```

Expected: both FAIL with `internal tables leaked ...` listing `__pgsqlite_*` names.

Then restore the fix:

```bash
git checkout HEAD -- src/translator/sqlite_master_filter.rs
git status --short src/translator/sqlite_master_filter.rs   # must print nothing
```

If `CREATE VIEW` or the CTAS instead fails with a pgsqlite error unrelated to filtering, stop — that is a separate defect and must be reported, not worked around.

- [ ] **Step 3: Run them against the fixed code**

Run: `cargo test --test sqlite_master_filter_enabled_test`

Expected: PASS, including the three pre-existing tests in that file.

- [ ] **Step 4: Write the flag-off test**

Append to `tests/sqlite_master_filter_disabled_test.rs`:

```rust
#[tokio::test]
async fn default_still_shows_internal_objects_through_a_view() {
    // No call to set_hide_internal_tables — this asserts the shipped default.
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)").await?;
            Ok(())
        })
    })
    .await;

    server
        .client
        .simple_query("CREATE VIEW schema_names AS SELECT name FROM sqlite_master")
        .await
        .expect("CREATE VIEW over sqlite_master should succeed");

    let names = table_names(&server.client, "SELECT name FROM schema_names ORDER BY name").await;
    assert!(
        names.iter().any(|n| n == "__pgsqlite_schema"),
        "default behaviour changed — a view over sqlite_master should still show internals: {names:?}"
    );
    assert!(names.iter().any(|n| n == "customers"));
}
```

- [ ] **Step 5: Run it**

Run: `cargo test --test sqlite_master_filter_disabled_test`

Expected: PASS. This proves the fix is gated on the flag and the default is unchanged.

- [ ] **Step 6: Run the pre-commit checklist**

```bash
cargo check && cargo clippy && cargo build && cargo test
```

Expected: no errors, no new warnings, full suite passes.

- [ ] **Step 7: Commit**

```bash
git add tests/sqlite_master_filter_enabled_test.rs tests/sqlite_master_filter_disabled_test.rs
git commit -m "test(hide-internal-tables): cover views and CTAS over sqlite_master

Wire-level reproduction from #86 in both flag states: with the flag on, a
view and a CTAS built over sqlite_master return no __pgsqlite_* rows; with
it off, they still do.

Closes #86"
```

---

## Out of scope

Deliberately not done, per the spec — do not add these:

- Filtering `sqlite_master` subqueries inside `UPDATE`/`DELETE`.
- An internal `__pgsqlite_visible_master` view to keep the persisted view DDL short. Rejected because it makes a pgsqlite-managed object load-bearing for user schema.
- Any test asserting on a view's persisted `sql` text. It pins sqlparser's exact rendering and would break on every upgrade for no signal.
- Un-filtering views created while the flag was on, after the operator turns it off. Baking is inherent to rewriting at creation time and is accepted.
