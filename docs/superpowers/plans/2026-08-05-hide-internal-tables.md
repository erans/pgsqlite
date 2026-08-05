# Hide Internal `__pgsqlite_*` Tables Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in `--hide-internal-tables` flag that removes pgsqlite's `__pgsqlite_*` bookkeeping tables and their indexes from client `sqlite_master` / `sqlite_schema` queries.

**Architecture:** A pure translator parses the client's SQL with sqlparser and replaces every `sqlite_master`/`sqlite_schema` relation with an equivalent filtered derived table, leaving the client's own projections, predicates, and joins byte-for-byte untouched. The translator is invoked from the two wire-protocol entry points only — never from `DbHandler` — so pgsqlite's own bookkeeping probes still see the real catalog. A process-global `AtomicBool`, set once from `Config` at startup, gates the call.

**Tech Stack:** Rust, clap (config), sqlparser 0.57 with the `visitor` feature, tokio-postgres (integration tests).

**Spec:** `docs/superpowers/specs/2026-08-05-hide-internal-tables-design.md`

## Global Constraints

- **Default off.** With no flag and no env var, every `sqlite_master` query must behave exactly as it does today. Every task that touches behaviour needs a test proving the default is unchanged.
- **Never filter pgsqlite's own queries.** The translator must not be reachable from `DbHandler::process_query` (`src/session/db_handler.rs:485`). `migration/runner.rs:61,302,310,339`, `metadata/enum_metadata.rs:203,341,389`, `rewriter/enum_rewriter.rs:26`, `cache/lazy_schema_loader.rs:124`, and `cache/schema.rs:117` all probe `sqlite_master` for `__pgsqlite_*` names; filtering them would make pgsqlite re-run its migrations on every start.
- **Fail open.** A parse failure or an unrecognized AST shape returns the query unchanged. Never turn a hide-failure into a client-visible error.
- **Use `substr(name, 1, 11) <> '__pgsqlite_'`, never `LIKE '__pgsqlite_%'`.** In `LIKE`, `_` is a single-character wildcard, so the `LIKE` form also matches unrelated names such as `abpgsqliteX`. Existing repository code uses the `LIKE` form; do not copy it.
- **Scope is `__pgsqlite_*` only.** Do not filter the materialized `pg_*` / `information_schema_*` relations, and do not touch `PRAGMA table_list`. Both are deliberate exclusions recorded in the spec.
- **Never dereference `CONFIG` from test code.** `config::CONFIG` calls `Config::parse()` on the test binary's argv; running `cargo test -- --nocapture` then aborts the process with `error: unexpected argument '--nocapture'`. This is verified behaviour, and it is the reason Task 1 introduces an atomic rather than reading `CONFIG` at the call sites.
- **Pre-commit checklist** (from `CLAUDE.md`) before every commit: `cargo check`, `cargo clippy`, `cargo build`, `cargo test`.

## File Structure

| File | Responsibility |
| --- | --- |
| `src/config.rs` (modify) | The `hide_internal_tables` CLI/env flag, plus the process-global toggle and its accessors |
| `src/main.rs` (modify) | Copies the parsed flag into the global toggle at startup |
| `Cargo.toml` (modify) | Enables sqlparser's `visitor` feature |
| `src/translator/sqlite_master_filter.rs` (create) | The pure rewrite: text gate, AST walk, relation substitution. No config, no I/O |
| `src/translator/mod.rs` (modify) | Exports `SqliteMasterFilter` |
| `src/query/executor.rs` (modify) | Simple-protocol hook, inside `preprocess_query` |
| `src/query/extended.rs` (modify) | Extended-protocol hook, at the top of `handle_parse` |
| `tests/sqlite_master_filter_enabled_test.rs` (create) | Wire-level behaviour with the flag on. Own binary, so the global toggle cannot race another test |
| `tests/sqlite_master_filter_disabled_test.rs` (create) | Wire-level proof that the default is unchanged. Own binary |
| `docs/configuration.md`, `README.md` (modify) | User-facing documentation of the flag |

---

### Task 1: Flag and process-global toggle

**Files:**
- Modify: `src/config.rs` (add arg after `no_tcp` at :26; add statics and accessors after the `CONFIG` block at :221)
- Modify: `src/main.rs:30`
- Test: `src/config.rs` (new `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: nothing.
- Produces: `pgsqlite::config::hide_internal_tables() -> bool` and `pgsqlite::config::set_hide_internal_tables(bool)`. Task 3 and Task 4 call the getter; the integration tests call the setter.

- [ ] **Step 1: Write the failing test**

Append to `src/config.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hide_internal_tables_defaults_off_and_is_settable() {
        // Note: this test must never touch CONFIG — Config::parse() would run
        // against the test binary's argv and abort the process.
        assert!(!hide_internal_tables());
        set_hide_internal_tables(true);
        assert!(hide_internal_tables());
        set_hide_internal_tables(false);
        assert!(!hide_internal_tables());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib config::tests::hide_internal_tables_defaults_off_and_is_settable`
Expected: FAIL to compile — `cannot find function 'hide_internal_tables' in this scope`.

- [ ] **Step 3: Write minimal implementation**

Add the CLI argument to `struct Config`, immediately after the `no_tcp` field (`src/config.rs:25-26`):

```rust
    #[arg(long, env = "PGSQLITE_HIDE_INTERNAL_TABLES", help = "Hide pgsqlite's internal __pgsqlite_* tables from client sqlite_master queries")]
    pub hide_internal_tables: bool,
```

Add the toggle at the end of `src/config.rs`, after the `lazy_static!` block:

```rust
use std::sync::atomic::{AtomicBool, Ordering};

/// Process-global mirror of `Config::hide_internal_tables`, set once at startup.
///
/// The wire-protocol hooks read this instead of `CONFIG` because dereferencing
/// `CONFIG` calls `Config::parse()` on the current process argv, which aborts
/// any test binary invoked with harness arguments such as `--nocapture`.
static HIDE_INTERNAL_TABLES: AtomicBool = AtomicBool::new(false);

/// Set the global "hide internal tables" toggle. Called once from `main`.
pub fn set_hide_internal_tables(enabled: bool) {
    HIDE_INTERNAL_TABLES.store(enabled, Ordering::Relaxed);
}

/// Whether client `sqlite_master` queries should have `__pgsqlite_*` objects filtered out.
pub fn hide_internal_tables() -> bool {
    HIDE_INTERNAL_TABLES.load(Ordering::Relaxed)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib config::tests::hide_internal_tables_defaults_off_and_is_settable`
Expected: PASS.

- [ ] **Step 5: Wire the flag into startup**

In `src/main.rs`, immediately after `let config = Config::load();` (:30):

```rust
    pgsqlite::config::set_hide_internal_tables(config.hide_internal_tables);
```

- [ ] **Step 6: Verify the flag is exposed**

Run: `cargo run --quiet -- --help 2>&1 | grep -A 1 hide-internal-tables`
Expected: the flag and its help text appear in the usage output.

- [ ] **Step 7: Commit**

```bash
cargo check && cargo clippy && cargo build && cargo test --lib
git add src/config.rs src/main.rs
git commit -m "feat(config): add --hide-internal-tables flag and global toggle (#80)"
```

---

### Task 2: The `SqliteMasterFilter` translator

**Files:**
- Modify: `Cargo.toml:24`
- Create: `src/translator/sqlite_master_filter.rs`
- Modify: `src/translator/mod.rs` (add `mod` near :15, `pub use` near :44)
- Test: `src/translator/sqlite_master_filter.rs` (inline `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: nothing from Task 1. This module is pure — it never reads config, so its tests are safe.
- Produces: `SqliteMasterFilter::translate(query: &str) -> Cow<'_, str>`. Returns `Cow::Borrowed` unchanged when there is nothing to do (no textual mention, parse failure, no relation matched, or the idempotence guard trips) and `Cow::Owned` with the rewritten SQL otherwise. Tasks 3 and 4 call this.

- [ ] **Step 1: Enable the sqlparser visitor feature**

In `Cargo.toml`, change line 24 from:

```toml
sqlparser = { version = "0.57.0", features = ["serde"] }
```

to:

```toml
sqlparser = { version = "0.57.0", features = ["serde", "visitor"] }
```

This pulls in the `sqlparser_derive` proc-macro crate, which generates the `VisitMut` impls used below.

- [ ] **Step 2: Write the failing tests**

Create `src/translator/sqlite_master_filter.rs` containing only this test module for now:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// The filtered relation as sqlparser renders it back out (note: uppercase SUBSTR).
    const FILTERED: &str = "(SELECT * FROM sqlite_master WHERE SUBSTR(name, 1, 11) <> '__pgsqlite_' AND (tbl_name IS NULL OR SUBSTR(tbl_name, 1, 11) <> '__pgsqlite_'))";

    fn rewritten(query: &str) -> String {
        SqliteMasterFilter::translate(query).into_owned()
    }

    #[test]
    fn rewrites_bare_relation_and_keeps_client_predicate() {
        let out = rewritten("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name");
        assert_eq!(
            out,
            format!("SELECT name FROM {FILTERED} AS sqlite_master WHERE type = 'table' ORDER BY name")
        );
    }

    #[test]
    fn rewrites_aggregate() {
        let out = rewritten("SELECT count(*) FROM sqlite_master");
        assert_eq!(out, format!("SELECT count(*) FROM {FILTERED} AS sqlite_master"));
    }

    #[test]
    fn preserves_client_alias_in_join() {
        let out = rewritten("SELECT m.name FROM sqlite_schema m JOIN foo f ON f.n = m.name");
        assert_eq!(
            out,
            format!("SELECT m.name FROM {FILTERED} AS m JOIN foo AS f ON f.n = m.name")
        );
    }

    #[test]
    fn rewrites_schema_qualified_relation() {
        let out = rewritten("SELECT name FROM main.sqlite_master");
        assert_eq!(out, format!("SELECT name FROM {FILTERED} AS sqlite_master"));
    }

    #[test]
    fn rewrites_inside_cte() {
        let out = rewritten("WITH t AS (SELECT name FROM sqlite_master) SELECT * FROM t");
        assert_eq!(
            out,
            format!("WITH t AS (SELECT name FROM {FILTERED} AS sqlite_master) SELECT * FROM t")
        );
    }

    #[test]
    fn rewrites_inside_exists_subquery() {
        let out = rewritten("SELECT * FROM foo WHERE EXISTS (SELECT 1 FROM sqlite_master WHERE name = foo.n)");
        assert_eq!(
            out,
            format!("SELECT * FROM foo WHERE EXISTS (SELECT 1 FROM {FILTERED} AS sqlite_master WHERE name = foo.n)")
        );
    }

    #[test]
    fn rewrites_select_sql_projection() {
        let out = rewritten("SELECT sql FROM sqlite_master WHERE type = 'table'");
        assert_eq!(
            out,
            format!("SELECT sql FROM {FILTERED} AS sqlite_master WHERE type = 'table'")
        );
    }

    #[test]
    fn does_not_corrupt_complex_predicates() {
        // This is the shape that broke the earlier query-rewriting attempt (PR #82).
        let out = rewritten(r"SELECT name FROM sqlite_master WHERE name NOT IN ('a', 'b') AND name LIKE 'x%' ESCAPE '\'");
        assert_eq!(
            out,
            format!(r"SELECT name FROM {FILTERED} AS sqlite_master WHERE name NOT IN ('a', 'b') AND name LIKE 'x%' ESCAPE '\'")
        );
    }

    #[test]
    fn leaves_unrelated_queries_borrowed() {
        assert!(matches!(
            SqliteMasterFilter::translate("SELECT * FROM customers"),
            Cow::Borrowed(_)
        ));
    }

    #[test]
    fn leaves_unparseable_input_borrowed() {
        assert!(matches!(
            SqliteMasterFilter::translate("SELECT FROM WHERE sqlite_master ((("),
            Cow::Borrowed(_)
        ));
    }

    #[test]
    fn leaves_explicit_internal_lookups_alone() {
        // "Hidden from listings, still queryable by name" — a client that names an
        // internal table explicitly is asking for it, and rewriting would also
        // double-apply the filter when both protocol hooks run.
        let q = "SELECT name FROM sqlite_master WHERE name = '__pgsqlite_schema'";
        assert!(matches!(SqliteMasterFilter::translate(q), Cow::Borrowed(_)));
    }

    #[test]
    fn does_not_match_unrelated_table_named_like_a_wildcard_match() {
        // 'abpgsqliteX' matches LIKE '__pgsqlite_%' but not substr(name,1,11).
        // Guard that we generate the substr form, not the LIKE form.
        let out = rewritten("SELECT name FROM sqlite_master");
        assert!(out.contains("SUBSTR(name, 1, 11) <> '__pgsqlite_'"));
        assert!(!out.contains("LIKE '__pgsqlite_"));
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test --lib sqlite_master_filter`
Expected: FAIL to compile — `cannot find type 'SqliteMasterFilter'`.

- [ ] **Step 4: Write the implementation**

Prepend to `src/translator/sqlite_master_filter.rs`, above the test module:

```rust
use std::borrow::Cow;
use std::ops::ControlFlow;

use sqlparser::ast::{
    Ident, ObjectNamePart, Statement, TableAlias, TableFactor, VisitMut, VisitorMut,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tracing::debug;

/// The relation a client `sqlite_master` reference is replaced with.
///
/// `substr(...)` rather than `LIKE '__pgsqlite_%'` because `_` is a
/// single-character wildcard in LIKE, which would also match unrelated names.
/// The `tbl_name` half is what hides the index rows whose own names carry no
/// `__pgsqlite_` prefix (`idx_enum_values_label`, `sqlite_autoindex___pgsqlite_schema_1`).
const FILTERED_RELATION_SQL: &str = "SELECT * FROM sqlite_master \
     WHERE substr(name, 1, 11) <> '__pgsqlite_' \
     AND (tbl_name IS NULL OR substr(tbl_name, 1, 11) <> '__pgsqlite_')";

/// Rewrites client references to `sqlite_master` / `sqlite_schema` so that
/// pgsqlite's own `__pgsqlite_*` objects are not listed.
///
/// This type is deliberately pure: it does not read configuration. Callers gate
/// it on `crate::config::hide_internal_tables()`, and it must only ever be
/// invoked on queries that arrived from a client over the wire.
pub struct SqliteMasterFilter;

impl SqliteMasterFilter {
    /// Cheap allocation-free gate: is there any point parsing this query?
    pub fn needs_translation(query: &str) -> bool {
        contains_ignore_ascii_case(query, "sqlite_master")
            || contains_ignore_ascii_case(query, "sqlite_schema")
    }

    /// Returns the rewritten query, or the input unchanged if there is nothing
    /// to do or the query cannot be handled. Never returns an error: failing to
    /// hide a row is cosmetic, rejecting a client's query is not.
    pub fn translate(query: &str) -> Cow<'_, str> {
        if !Self::needs_translation(query) {
            return Cow::Borrowed(query);
        }

        // Idempotence guard, and the "still queryable by name" half of the design:
        // a query that already mentions the internal prefix is either one we
        // rewrote in an earlier hook or a client deliberately naming an internal
        // table. Either way, leave it alone.
        if query.contains("__pgsqlite_") {
            return Cow::Borrowed(query);
        }

        let mut statements = match Parser::parse_sql(&PostgreSqlDialect {}, query) {
            Ok(statements) => statements,
            Err(e) => {
                debug!("sqlite_master filter: parse failed, passing through: {e}");
                return Cow::Borrowed(query);
            }
        };

        let mut visitor = RelationReplacer { replaced: 0 };
        for statement in &mut statements {
            let _ = statement.visit(&mut visitor);
        }

        if visitor.replaced == 0 {
            return Cow::Borrowed(query);
        }

        let rewritten = statements
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join("; ");
        debug!("sqlite_master filter: {query} -> {rewritten}");
        Cow::Owned(rewritten)
    }
}

struct RelationReplacer {
    replaced: usize,
}

impl VisitorMut for RelationReplacer {
    type Break = ();

    /// Replacement happens in `post_visit` rather than `pre_visit`: the visitor
    /// descends into a node's children between the two, so replacing in
    /// `pre_visit` would recurse into the `sqlite_master` reference inside the
    /// relation we just substituted, forever.
    fn post_visit_table_factor(&mut self, table_factor: &mut TableFactor) -> ControlFlow<()> {
        let TableFactor::Table { name, alias, .. } = table_factor else {
            return ControlFlow::Continue(());
        };

        let relation = match name.0.last() {
            Some(ObjectNamePart::Identifier(ident)) => ident.value.to_ascii_lowercase(),
            _ => return ControlFlow::Continue(()),
        };
        if relation != "sqlite_master" && relation != "sqlite_schema" {
            return ControlFlow::Continue(());
        }

        // Only `main.` and `temp.` qualify the SQLite catalog. Anything else
        // (e.g. an attached database) is left alone.
        if name.0.len() > 1 {
            match name.0.first() {
                Some(ObjectNamePart::Identifier(qualifier)) => {
                    let qualifier = qualifier.value.to_ascii_lowercase();
                    if qualifier != "main" && qualifier != "temp" {
                        return ControlFlow::Continue(());
                    }
                }
                _ => return ControlFlow::Continue(()),
            }
        }

        // Keep the client's own alias if it had one, so `m.name` still resolves;
        // otherwise alias to the spelling the client used, so `sqlite_schema.name` does.
        let effective_alias = alias.clone().unwrap_or(TableAlias {
            name: Ident::new(relation),
            columns: vec![],
        });

        let Ok(statements) = Parser::parse_sql(&PostgreSqlDialect {}, FILTERED_RELATION_SQL) else {
            return ControlFlow::Continue(());
        };
        let Some(Statement::Query(subquery)) = statements.into_iter().next() else {
            return ControlFlow::Continue(());
        };

        *table_factor = TableFactor::Derived {
            lateral: false,
            subquery,
            alias: Some(effective_alias),
        };
        self.replaced += 1;
        ControlFlow::Continue(())
    }
}

/// Allocation-free case-insensitive substring test. This runs on every client
/// query while the flag is on, so it must not allocate.
fn contains_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    let haystack = haystack.as_bytes();
    let needle = needle.as_bytes();
    if needle.len() > haystack.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}
```

- [ ] **Step 5: Export the translator**

In `src/translator/mod.rs`, add alongside the other module declarations (near :15):

```rust
mod sqlite_master_filter;
```

and alongside the other re-exports (near :44):

```rust
pub use sqlite_master_filter::SqliteMasterFilter;
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test --lib sqlite_master_filter`
Expected: PASS, 12 tests.

- [ ] **Step 7: Commit**

```bash
cargo check && cargo clippy && cargo build && cargo test --lib
git add Cargo.toml Cargo.lock src/translator/sqlite_master_filter.rs src/translator/mod.rs
git commit -m "feat(translator): add SqliteMasterFilter relation substitution (#80)"
```

---

### Task 3: Simple-protocol hook and wire-level tests

**Files:**
- Modify: `src/query/executor.rs:33-40` (`preprocess_query`)
- Create: `tests/sqlite_master_filter_enabled_test.rs`
- Create: `tests/sqlite_master_filter_disabled_test.rs`

**Interfaces:**
- Consumes: `crate::config::hide_internal_tables()` (Task 1), `crate::translator::SqliteMasterFilter::translate` (Task 2).
- Produces: filtered behaviour on the simple query protocol, exercised via `tokio_postgres::Client::simple_query`. Task 4 extends both test files with extended-protocol cases.

Each test file is its own binary, so `set_hide_internal_tables(true)` in one cannot race the other. Do not merge them into a single file.

- [ ] **Step 1: Write the failing tests**

Create `tests/sqlite_master_filter_enabled_test.rs`:

```rust
mod common;
use common::*;
use tokio_postgres::SimpleQueryMessage;

async fn table_names(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    client
        .simple_query(sql)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn hides_internal_objects_from_simple_protocol() {
    pgsqlite::config::set_hide_internal_tables(true);

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)").await?;
            db.execute("CREATE INDEX idx_customers_name ON customers(name)").await?;
            // Matches LIKE '__pgsqlite_%' but not substr(name, 1, 11) — must stay visible.
            db.execute("CREATE TABLE abpgsqliteX (id INTEGER PRIMARY KEY)").await?;
            Ok(())
        })
    })
    .await;

    let names = table_names(
        &server.client,
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
    )
    .await;
    assert!(
        !names.iter().any(|n| n.starts_with("__pgsqlite_")),
        "internal tables leaked: {names:?}"
    );
    assert!(names.iter().any(|n| n == "customers"), "user table missing: {names:?}");
    assert!(names.iter().any(|n| n == "abpgsqliteX"), "LIKE-wildcard false positive: {names:?}");

    // Indexes owned by internal tables carry no __pgsqlite_ prefix of their own
    // and must be caught via tbl_name.
    let indexes = table_names(
        &server.client,
        "SELECT name FROM sqlite_master WHERE type = 'index' ORDER BY name",
    )
    .await;
    assert!(
        !indexes.iter().any(|n| n.starts_with("idx_enum_") || n.contains("__pgsqlite_")),
        "internal indexes leaked: {indexes:?}"
    );
    assert!(
        indexes.iter().any(|n| n == "idx_customers_name"),
        "user index missing: {indexes:?}"
    );

    // The DDL projection must not dump internal CREATE TABLE statements.
    let ddl = table_names(&server.client, "SELECT sql FROM sqlite_master WHERE type = 'table'").await;
    assert!(
        !ddl.iter().any(|s| s.contains("__pgsqlite_")),
        "internal DDL leaked"
    );

    // sqlite_schema is an alias for the same relation.
    let via_alias = table_names(
        &server.client,
        "SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name",
    )
    .await;
    assert!(!via_alias.iter().any(|n| n.starts_with("__pgsqlite_")));

    // Aggregates must count only visible rows.
    let counts = table_names(&server.client, "SELECT count(*) FROM sqlite_master WHERE type = 'table'").await;
    assert_eq!(counts.len(), 1);
    let visible: usize = counts[0].parse().unwrap();
    assert_eq!(visible, names.len(), "count disagrees with the listing");

    pgsqlite::config::set_hide_internal_tables(false);
}
```

Create `tests/sqlite_master_filter_disabled_test.rs`:

```rust
mod common;
use common::*;
use tokio_postgres::SimpleQueryMessage;

async fn table_names(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    client
        .simple_query(sql)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn default_still_shows_internal_objects() {
    // No call to set_hide_internal_tables — this asserts the shipped default.
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)").await?;
            Ok(())
        })
    })
    .await;

    let names = table_names(
        &server.client,
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
    )
    .await;
    assert!(
        names.iter().any(|n| n == "__pgsqlite_schema"),
        "default behaviour changed — internal tables should still be visible: {names:?}"
    );
    assert!(names.iter().any(|n| n == "customers"));
}
```

- [ ] **Step 2: Run tests to verify the enabled one fails**

Run: `cargo test --test sqlite_master_filter_enabled_test`
Expected: FAIL — `internal tables leaked: [...]`, because nothing calls the translator yet.

Run: `cargo test --test sqlite_master_filter_disabled_test`
Expected: PASS already — it documents current behaviour.

- [ ] **Step 3: Write the implementation**

Replace `preprocess_query` in `src/query/executor.rs:33-40` with:

```rust
fn preprocess_query(query: &str) -> String {
    let query: Cow<'_, str> = if PG_SHOW_ALL_SETTINGS_PATTERN.is_match(query) {
        Cow::Owned(PG_SHOW_ALL_SETTINGS_PATTERN.replace_all(query, "pg_settings").to_string())
    } else {
        Cow::Borrowed(query)
    };

    if crate::config::hide_internal_tables() {
        crate::translator::SqliteMasterFilter::translate(&query).into_owned()
    } else {
        query.into_owned()
    }
}
```

`Cow` is already imported in this file (it is the return type of `create_command_tag` at :151). `preprocess_query` is called from `execute_single_statement` at :281, ahead of the SELECT branch at :326 and ahead of the wire-protocol cache, so the cache is keyed on the rewritten text.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --test sqlite_master_filter_enabled_test --test sqlite_master_filter_disabled_test`
Expected: PASS, both.

- [ ] **Step 5: Verify pgsqlite's own bookkeeping still works**

The risk this guards is migrations re-running because an internal probe stopped seeing `__pgsqlite_metadata`.

Run: `cargo test migration`
Expected: PASS.

Run: `cargo test --test sqlite_master_filter_enabled_test`
Expected: PASS. Do **not** add `-- --nocapture` to any integration test in this repository: creating a session initializes lazy statics that dereference `config::CONFIG` (`src/session/state.rs:15`, `src/cache/statement_pool.rs:35`, `src/cache/execution.rs:130`, `src/cache/result_cache.rs:246`), and `CONFIG` runs `Config::parse()` against the test binary's argv, which rejects `--nocapture` and aborts the process.

The end-to-end proof that internal probes are unaffected is the two-restart manual run in Task 4, Step 6: the second server start opens a database whose migrations are already applied, with the flag on.

- [ ] **Step 6: Commit**

```bash
cargo check && cargo clippy && cargo build && cargo test
git add src/query/executor.rs tests/sqlite_master_filter_enabled_test.rs tests/sqlite_master_filter_disabled_test.rs
git commit -m "feat(query): filter sqlite_master on the simple protocol (#80)"
```

---

### Task 4: Extended-protocol hook and documentation

**Files:**
- Modify: `src/query/extended.rs:82` (`handle_parse`)
- Modify: `tests/sqlite_master_filter_enabled_test.rs`
- Modify: `tests/sqlite_master_filter_disabled_test.rs`
- Modify: `docs/configuration.md:15-25` (Server Options table)
- Modify: `README.md:135-140` (Essential Options block)

**Interfaces:**
- Consumes: `crate::config::hide_internal_tables()` (Task 1), `crate::translator::SqliteMasterFilter::translate` (Task 2).
- Produces: the completed feature. Nothing depends on this task.

`tokio_postgres::Client::query` uses Parse/Bind/Execute, so it exercises this hook, while `simple_query` from Task 3 exercises the other one. Both are needed for full coverage.

- [ ] **Step 1: Write the failing tests**

Append to `tests/sqlite_master_filter_enabled_test.rs`:

```rust
#[tokio::test]
async fn hides_internal_objects_from_extended_protocol() {
    pgsqlite::config::set_hide_internal_tables(true);

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total TEXT)").await?;
            Ok(())
        })
    })
    .await;

    // client.query() goes through Parse/Bind/Execute, not simple query.
    let rows = server
        .client
        .query("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name", &[])
        .await
        .unwrap();
    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();

    assert!(
        !names.iter().any(|n| n.starts_with("__pgsqlite_")),
        "internal tables leaked over extended protocol: {names:?}"
    );
    assert!(names.iter().any(|n| n == "orders"), "user table missing: {names:?}");

    pgsqlite::config::set_hide_internal_tables(false);
}
```

Append to `tests/sqlite_master_filter_disabled_test.rs`:

```rust
#[tokio::test]
async fn default_still_shows_internal_objects_on_extended_protocol() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY)").await?;
            Ok(())
        })
    })
    .await;

    let rows = server
        .client
        .query("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name", &[])
        .await
        .unwrap();
    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();

    assert!(
        names.iter().any(|n| n == "__pgsqlite_schema"),
        "default behaviour changed on extended protocol: {names:?}"
    );
}
```

- [ ] **Step 2: Run tests to verify the enabled one fails**

Run: `cargo test --test sqlite_master_filter_enabled_test hides_internal_objects_from_extended_protocol`
Expected: FAIL — `internal tables leaked over extended protocol: [...]`.

- [ ] **Step 3: Write the implementation**

In `src/query/extended.rs`, at the very top of `handle_parse`'s body — before the `info!("PARSE: Starting parse...")` line at :93, so that the prepared-statement cache at :96 stores and compares the rewritten text:

```rust
        let query = if crate::config::hide_internal_tables() {
            crate::translator::SqliteMasterFilter::translate(&query).into_owned()
        } else {
            query
        };
```

This shadows the owned `query: String` parameter. It must sit outside the `#[cfg(not(feature = "unified_processor"))]` translation block at :420-450, so the filter is never feature-gated.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --test sqlite_master_filter_enabled_test --test sqlite_master_filter_disabled_test`
Expected: PASS, 4 tests total.

- [ ] **Step 5: Document the flag**

In `docs/configuration.md`, add a row to the Server Options table after the `No TCP` row (:25):

```markdown
| Hide Internal Tables | `--hide-internal-tables` | `PGSQLITE_HIDE_INTERNAL_TABLES` | `false` | Hide pgsqlite's internal `__pgsqlite_*` tables and their indexes from client `sqlite_master` / `sqlite_schema` queries. The tables remain queryable when named explicitly. Does not affect the materialized `pg_*` / `information_schema_*` relations or `PRAGMA table_list`. |
```

In `README.md`, add to the "Basic options" block (after `--in-memory` at :140):

```bash
  --hide-internal-tables # Hide pgsqlite's __pgsqlite_* tables from sqlite_master listings
```

- [ ] **Step 6: Full verification**

Run: `cargo check && cargo clippy && cargo build && cargo test`
Expected: no errors, no new warnings, all tests pass.

Then reproduce the issue's own scenario manually, both ways:

```bash
cargo build
rm -f /tmp/t.sqlite
./target/debug/pgsqlite --database /tmp/t.sqlite --port 5599 &
psql "host=127.0.0.1 port=5599 user=postgres dbname=main gssencmode=disable" -c "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT);"
psql "host=127.0.0.1 port=5599 user=postgres dbname=main gssencmode=disable" -tA -c "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
# Expected: internal tables present (default off)
kill %1

./target/debug/pgsqlite --database /tmp/t.sqlite --port 5599 --hide-internal-tables &
psql "host=127.0.0.1 port=5599 user=postgres dbname=main gssencmode=disable" -tA -c "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
# Expected: only "customers"
psql "host=127.0.0.1 port=5599 user=postgres dbname=main gssencmode=disable" -tA -c "SELECT COUNT(*) FROM __pgsqlite_schema;"
# Expected: a number — hidden from listings, still queryable by name
kill %1
```

- [ ] **Step 7: Commit**

```bash
git add src/query/extended.rs tests/sqlite_master_filter_enabled_test.rs tests/sqlite_master_filter_disabled_test.rs docs/configuration.md README.md
git commit -m "feat(query): filter sqlite_master on the extended protocol, document flag (#80)"
```

---

## Notes for the reviewer

- **`\dt` and `information_schema.tables`** still list the materialized `pg_*` / `information_schema_*` relations, and `\dt` reports "Did not find any tables" on a database that has one. Both are pre-existing and out of scope; file separately.
- **PR #83** implements the same issue by filtering result rows. This plan supersedes it. When closing, the reasoning is in the spec's "Mechanism" section: row filtering cannot see `count(*)`, joins, subqueries, or `SELECT sql`, and fails open silently on all of them.
- **The idempotence guard was removed during execution.** Task 2's plan text specified an early return when the query already contained `__pgsqlite_`. Review found it disabled filtering for any query containing that literal anywhere, including a comment (`SELECT name FROM sqlite_master -- __pgsqlite_`). Ruling: removed. It was not load-bearing — `SELECT * FROM __pgsqlite_schema` never reaches it, since that query has no `sqlite_master` reference for `needs_translation` to match, so "hidden from listings, still queryable by name" holds without it. Double application, if both hooks ever fire on one query, nests one derived table inside another: identical rows, bounded at two passes.
