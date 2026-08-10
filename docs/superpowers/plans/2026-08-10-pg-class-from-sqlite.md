# pg_class from SQLite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix issue #87 (`\dt` reports "Did not find any tables") by deleting `PgClassHandler` and letting SQLite execute `pg_class` queries against an enriched view.

**Architecture:** pgsqlite currently intercepts `pg_class` queries and evaluates them with a hand-rolled Rust engine that cannot handle `regexp()`, joined columns, or JOINs. The `pg_class` and `pg_namespace` SQLite views already exist, and `regexp`/`pg_table_is_visible`/`pg_get_userbyid` are already registered UDFs. We enrich the `pg_class` view to full column parity, assign internal relations to the `pg_catalog` namespace, then remove the interception branch so queries flow to SQLite.

**Tech Stack:** Rust, rusqlite/SQLite, tokio, tokio-postgres (tests), sqlparser.

**Spec:** `docs/superpowers/specs/2026-08-10-pg-class-sqlite-engine-design.md`

## Global Constraints

- Migration registry currently runs through **v27**. The new migration is **v28**. Do not reuse v26 — it exists and owns the current `pg_class` view.
- The canonical OID formula is the **unicode formula**: `((unicode(c1)*1000000) + (unicode(c2)*10000) + (unicode(c3)*100) + (len*7)) % 1000000 + 16384`, cast to TEXT. Do not introduce a new formula and do not migrate persisted OIDs.
- `pg_class.oid` is **TEXT**, matching `pg_constraint.conrelid` / `pg_index.indrelid` / `pg_attrdef.adrelid`.
- Namespace OIDs: `pg_catalog` = 11, `information_schema` = 13000, `public` = 2200.
- `relchecks` stays `0`. Do not derive it from `pg_constraint`.
- Do **not** modify `src/catalog/where_evaluator.rs`. Its `NOT`-inverts-unknown bug is filed separately (Task 8).
- Pre-commit checklist for every commit: `cargo check` (no warnings), `cargo clippy`, `cargo build`, `cargo test`.

---

### Task 1: Failing regression test for #87

**Files:**
- Create: `tests/pg_class_dt_test.rs`

**Interfaces:**
- Consumes: `common::setup_test_server_with_init` (existing harness; takes a closure returning a boxed future, yields a struct with a `.client` field that is a `tokio_postgres::Client`).
- Produces: nothing consumed by later tasks. This test is the acceptance gate for Task 4.

- [ ] **Step 1: Write the failing test**

Create `tests/pg_class_dt_test.rs`:

```rust
mod common;
use common::setup_test_server_with_init;

/// The exact query psql 18 expands `\dt` into.
const DT_QUERY: &str = r#"
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
WHERE c.relkind IN ('r','p','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2
"#;

#[tokio::test]
async fn test_dt_lists_user_tables() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)").await?;
            Ok(())
        })
    }).await;

    let rows = server.client.query(DT_QUERY, &[]).await
        .expect("\\dt query should succeed");

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>("Name")).collect();

    assert!(names.iter().any(|n| n == "customers"),
        "issue #87: \\dt must list the user table, got {names:?}");
}

#[tokio::test]
async fn test_dt_hides_internal_relations() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)").await?;
            Ok(())
        })
    }).await;

    let rows = server.client.query(DT_QUERY, &[]).await
        .expect("\\dt query should succeed");

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>("Name")).collect();

    for internal in ["pg_constraint", "pg_attrdef", "pg_index", "pg_depend"] {
        assert!(!names.iter().any(|n| n == internal),
            "pgsqlite's own {internal} must not appear in \\dt, got {names:?}");
    }
}

#[tokio::test]
async fn test_dt_reports_public_schema_and_owner() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)").await?;
            Ok(())
        })
    }).await;

    let rows = server.client.query(DT_QUERY, &[]).await
        .expect("\\dt query should succeed");

    let row = rows.iter()
        .find(|r| r.get::<_, String>("Name") == "customers")
        .expect("customers row must be present");

    assert_eq!(row.get::<_, String>("Schema"), "public");
    assert_eq!(row.get::<_, String>("Type"), "table");
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --test pg_class_dt_test 2>&1 | tail -30`

Expected: all three FAIL. `test_dt_lists_user_tables` fails on the `customers` assertion because the query returns 0 rows. `test_dt_reports_public_schema_and_owner` fails on `expect("customers row must be present")`.

If instead you see a panic about a missing `Schema` column, that is the same root cause (the interceptor drops projected columns) and still counts as RED.

- [ ] **Step 3: Commit the failing test**

```bash
git add tests/pg_class_dt_test.rs
git commit -m "test: failing regression test for \dt returning no tables (#87)"
```

---

### Task 2: Guarantee `trusted_schema=ON` on session connections

**Files:**
- Modify: `src/session/db_handler.rs` (wherever `register_*_functions` are called on a new connection — locate with the grep in Step 1)
- Test: `tests/pg_class_dt_test.rs` (append)

**Interfaces:**
- Consumes: nothing.
- Produces: the guarantee that `pragma_table_info()` may be used inside a view. Task 3's `relnatts` column depends on this.

**Why:** `pragma_table_info` is a virtual table. SQLite refuses virtual tables inside views unless `trusted_schema` is ON. It is ON by default in the C API (so this works today), but the `sqlite3` CLI disables it and rejects the view with `unsafe use of virtual table "pragma_table_info"`. Making it explicit prevents a future default change from silently breaking `relnatts`.

- [ ] **Step 1: Locate connection initialization**

Run: `grep -rn "register_hash_functions\|register_regex_functions" src/session/db_handler.rs | head`

Every site that opens a connection and registers UDFs needs the pragma. There may be more than one (in-memory vs file, pooled vs per-session).

- [ ] **Step 2: Write the failing test**

Append to `tests/pg_class_dt_test.rs`:

```rust
#[tokio::test]
async fn test_trusted_schema_allows_pragma_in_views() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)").await?;
            Ok(())
        })
    }).await;

    let rows = server.client.query("PRAGMA trusted_schema", &[]).await
        .expect("PRAGMA trusted_schema should be readable");

    assert_eq!(rows.len(), 1, "expected one row from PRAGMA trusted_schema");
    let enabled: i32 = rows[0].get(0);
    assert_eq!(enabled, 1, "trusted_schema must be ON so views may call pragma_table_info()");
}
```

- [ ] **Step 3: Run it**

Run: `cargo test --test pg_class_dt_test test_trusted_schema_allows_pragma_in_views 2>&1 | tail -20`

Expected: PASS if the C API default already applies. That is an acceptable outcome — the test is pinning existing behavior so it cannot regress.

If it FAILS, or if `PRAGMA trusted_schema` does not round-trip through the protocol, replace the test body with a direct assertion instead:

```rust
    let rows = server.client
        .query("SELECT COUNT(*) FROM pragma_table_info('customers')", &[])
        .await
        .expect("pragma_table_info must be callable");
    let n: i64 = rows[0].get(0);
    assert_eq!(n, 2, "customers has 2 columns");
```

- [ ] **Step 4: Add the explicit pragma**

At each connection-initialization site found in Step 1, immediately before the `register_*_functions` calls, add:

```rust
conn.execute_batch("PRAGMA trusted_schema=ON;")?;
```

- [ ] **Step 5: Re-run the test**

Run: `cargo test --test pg_class_dt_test test_trusted_schema_allows_pragma_in_views 2>&1 | tail -20`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/session/db_handler.rs tests/pg_class_dt_test.rs
git commit -m "fix: set trusted_schema=ON so views may call pragma_table_info()"
```

---

### Task 3: Migration v28 — enriched `pg_class` view and `information_schema` namespace

**Files:**
- Modify: `src/migration/registry.rs` (add the call after line 36 `register_v27_fix_pg_proc_types(&mut registry);`, and add the function at end of file)
- Test: `tests/pg_class_view_test.rs` (create)

**Interfaces:**
- Consumes: `trusted_schema=ON` from Task 2.
- Produces: a `pg_class` view with these 33 columns, in this order — `oid, relname, relnamespace, reltype, reloftype, relowner, relam, relfilenode, reltablespace, relpages, reltuples, relallvisible, reltoastrelid, relhasindex, relisshared, relpersistence, relkind, relnatts, relchecks, relhasrules, relhastriggers, relhassubclass, relrowsecurity, relforcerowsecurity, relispopulated, relreplident, relispartition, relrewrite, relfrozenxid, relminmxid, relacl, reloptions, relpartbound`. Also a `pg_namespace` view with rows `(11, pg_catalog)`, `(2200, public)`, `(13000, information_schema)`. Task 4 relies on both.

- [ ] **Step 1: Write the failing test**

Create `tests/pg_class_view_test.rs`:

```rust
mod common;
use common::setup_test_server_with_init;

const ALL_PG_CLASS_COLUMNS: &str = "oid, relname, relnamespace, reltype, reloftype, \
relowner, relam, relfilenode, reltablespace, relpages, reltuples, relallvisible, \
reltoastrelid, relhasindex, relisshared, relpersistence, relkind, relnatts, relchecks, \
relhasrules, relhastriggers, relhassubclass, relrowsecurity, relforcerowsecurity, \
relispopulated, relreplident, relispartition, relrewrite, relfrozenxid, relminmxid, \
relacl, reloptions, relpartbound";

#[tokio::test]
async fn test_pg_class_has_full_column_parity() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)").await?;
            Ok(())
        })
    }).await;

    let sql = format!("SELECT {ALL_PG_CLASS_COLUMNS} FROM pg_catalog.pg_class WHERE relname = 'customers'");
    let rows = server.client.query(&sql, &[]).await
        .expect("all 33 pg_class columns must be selectable");

    assert_eq!(rows.len(), 1, "expected exactly one row for customers");
}

#[tokio::test]
async fn test_pg_class_namespace_assignment() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)").await?;
            Ok(())
        })
    }).await;

    // Compare in SQL rather than binding integers in Rust: relnamespace is an
    // INTEGER in the view, and its inferred wire type is not worth guessing.
    let public_rows = server.client.query(
        "SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = 2200 AND relname = 'customers'",
        &[]
    ).await.expect("query should succeed");
    assert_eq!(public_rows.len(), 1, "user tables belong to the public namespace (2200)");

    let catalog_rows = server.client.query(
        "SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = 11 AND relname = 'pg_constraint'",
        &[]
    ).await.expect("query should succeed");
    assert_eq!(catalog_rows.len(), 1, "internal pg_* relations belong to pg_catalog (11)");

    let misfiled = server.client.query(
        "SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = 2200 AND relname LIKE 'pg\\_%'",
        &[]
    ).await.expect("query should succeed");
    assert!(misfiled.is_empty(), "no pg_* relation may remain in the public namespace");
}

#[tokio::test]
async fn test_pg_class_relnatts_is_real_column_count() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE three_cols (a INTEGER, b TEXT, c REAL)").await?;
            Ok(())
        })
    }).await;

    let rows = server.client.query(
        "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'three_cols' AND relnatts = 3",
        &[]
    ).await.expect("query should succeed");

    assert_eq!(rows.len(), 1, "relnatts must reflect the real column count (3)");
}

#[tokio::test]
async fn test_pg_namespace_has_information_schema() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY)").await?;
            Ok(())
        })
    }).await;

    let rows = server.client.query(
        "SELECT nspname FROM pg_catalog.pg_namespace ORDER BY nspname", &[]
    ).await.expect("query should succeed");

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();

    for expected in ["pg_catalog", "public", "information_schema"] {
        assert!(names.iter().any(|n| n == expected),
            "v28 must provide the {expected} namespace, got {names:?}");
    }

    // Confirm the oid values via SQL, avoiding an integer wire-type binding.
    let is_row = server.client.query(
        "SELECT nspname FROM pg_catalog.pg_namespace WHERE oid = 13000", &[]
    ).await.expect("query should succeed");
    assert_eq!(is_row.len(), 1, "information_schema must have oid 13000");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --test pg_class_view_test 2>&1 | tail -30`

Expected: FAIL. `test_pg_class_has_full_column_parity` fails because `reltype`/`relnatts`/etc. do not exist; `test_pg_namespace_has_information_schema` fails because there is no 13000 row.

- [ ] **Step 3: Register the migration**

In `src/migration/registry.rs`, directly after line 36 (`register_v27_fix_pg_proc_types(&mut registry);`), add:

```rust
        register_v28_pg_class_full_columns(&mut registry);
```

- [ ] **Step 4: Add the migration function**

Append to the end of `src/migration/registry.rs`. The view SQL below was verified against SQLite before being written into this plan.

```rust
/// Version 28: Serve pg_class from SQLite with full column parity.
/// Adds the nine columns PgClassHandler used to synthesize, assigns internal
/// pg_*/information_schema_* relations to their proper namespaces, and fixes
/// three pre-existing view bugs (relkind_full is not a real PostgreSQL column;
/// relreplident should be 'd'; relispartition should be 'f').
fn register_v28_pg_class_full_columns(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(28, Migration {
        version: 28,
        name: "pg_class_full_columns",
        description: "Enrich pg_class view to full 33-column parity and namespace internal relations so SQLite can serve pg_class directly",
        up: MigrationAction::SqlBatch(&[
            r#"DROP VIEW IF EXISTS pg_class"#,
            r#"DROP VIEW IF EXISTS pg_namespace"#,

            r#"
            CREATE VIEW pg_namespace AS
                SELECT 11 as oid, 'pg_catalog' as nspname, 10 as nspowner, NULL as nspacl
                UNION ALL
                SELECT 2200 as oid, 'public' as nspname, 10 as nspowner, NULL as nspacl
                UNION ALL
                SELECT 13000 as oid, 'information_schema' as nspname, 10 as nspowner, NULL as nspacl
            "#,

            r#"
            CREATE VIEW pg_class AS
            WITH base AS (
                SELECT name, type,
                    ((unicode(substr(name, 1, 1)) * 1000000) +
                     (unicode(substr(name || ' ', 2, 1)) * 10000) +
                     (unicode(substr(name || '  ', 3, 1)) * 100) +
                     (length(name) * 7)) % 1000000 + 16384 AS oid_num
                FROM sqlite_master
                WHERE type IN ('table', 'view', 'index')
                  AND name NOT LIKE 'sqlite_%'
                  AND name NOT LIKE '__pgsqlite_%'
            )
            SELECT
                CAST(oid_num AS TEXT) as oid,
                name as relname,
                CASE
                    WHEN name LIKE 'pg\_%' ESCAPE '\' THEN 11
                    WHEN name LIKE 'information\_schema\_%' ESCAPE '\' THEN 13000
                    ELSE 2200
                END as relnamespace,
                CAST(oid_num + 1 AS TEXT) as reltype,
                0 as reloftype,
                10 as relowner,
                CASE WHEN type = 'index' THEN 403 ELSE 0 END as relam,
                0 as relfilenode,
                0 as reltablespace,
                0 as relpages,
                -1 as reltuples,
                0 as relallvisible,
                0 as reltoastrelid,
                CASE WHEN type = 'table' THEN 't' ELSE 'f' END as relhasindex,
                'f' as relisshared,
                'p' as relpersistence,
                CASE type
                    WHEN 'table' THEN 'r'
                    WHEN 'view' THEN 'v'
                    WHEN 'index' THEN 'i'
                END as relkind,
                (SELECT COUNT(*) FROM pragma_table_info(base.name)) as relnatts,
                0 as relchecks,
                'f' as relhasrules,
                CASE WHEN EXISTS(
                    SELECT 1 FROM sqlite_master t
                    WHERE t.type = 'trigger' AND t.tbl_name = base.name
                ) THEN 't' ELSE 'f' END as relhastriggers,
                'f' as relhassubclass,
                'f' as relrowsecurity,
                'f' as relforcerowsecurity,
                't' as relispopulated,
                'd' as relreplident,
                'f' as relispartition,
                0 as relrewrite,
                0 as relfrozenxid,
                0 as relminmxid,
                NULL as relacl,
                NULL as reloptions,
                NULL as relpartbound
            FROM base
            "#,

            r#"
            UPDATE __pgsqlite_metadata
            SET value = '28', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::SqlBatch(&[
            r#"DROP VIEW IF EXISTS pg_class"#,
            r#"DROP VIEW IF EXISTS pg_namespace"#,

            // Restore the v26 pg_class view: copy the CREATE VIEW block verbatim
            // from register_v26_enhanced_pg_attribute_support's `up`
            // (registry.rs lines 2544-2586, the block ending just before line 2587's `"#,`).
            r#"
            <paste v26 pg_class CREATE VIEW block here>
            "#,

            // Restore the two-row pg_namespace view: copy from registry.rs lines 270-281.
            r#"
            <paste original pg_namespace CREATE VIEW block here>
            "#,

            r#"
            UPDATE __pgsqlite_metadata
            SET value = '27', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ])),
        dependencies: vec![27],
    });
}
```

The two paste markers above are the only place in this plan where you supply
content: copy the exact SQL from the line ranges given. `register_v26_enhanced_pg_attribute_support`
restores its previous view in `down` (`registry.rs:138-146` of that function), so
v28 follows the same convention rather than leaving the database view-less on rollback.

- [ ] **Step 5: Run the tests**

Run: `cargo test --test pg_class_view_test 2>&1 | tail -30`

Expected: `test_pg_namespace_has_information_schema` PASSES. The other three may still FAIL, because `PgClassHandler` still intercepts and shadows the view — that is removed in Task 4. If they still fail with "column does not exist", that confirms interception is still active and is the expected state at this point.

- [ ] **Step 6: Verify the view directly, bypassing interception**

Run:

```bash
cargo test --test pg_class_view_test test_pg_namespace_has_information_schema 2>&1 | tail -5
```

Expected: PASS. This is the one assertion in this task that does not depend on Task 4.

- [ ] **Step 7: Commit**

```bash
git add src/migration/registry.rs tests/pg_class_view_test.rs
git commit -m "feat: migration v28 enriches pg_class view to full column parity (#87)"
```

---

### Task 4: Remove `pg_class` interception and delete `PgClassHandler`

**Files:**
- Modify: `src/catalog/query_interceptor.rs:550-553` (remove the branch), `:12` (remove the import)
- Modify: `src/catalog/mod.rs` (remove the `pg_class` module declaration)
- Delete: `src/catalog/pg_class.rs`

**Interfaces:**
- Consumes: the v28 view from Task 3.
- Produces: `pg_class` queries reaching SQLite. This is what makes Task 1's tests pass.

- [ ] **Step 1: Remove the interception branch**

In `src/catalog/query_interceptor.rs`, delete these four lines at 550-553:

```rust
            // Handle pg_class queries
            if table_name.contains("pg_class") || table_name.contains("pg_catalog.pg_class") {
                return Some(PgClassHandler::handle_query(select, &db).await);
            }
```

- [ ] **Step 2: Remove the import**

In `src/catalog/query_interceptor.rs:12`, remove `pg_class::PgClassHandler, ` from the `use super::{...}` list. Leave the other handlers untouched.

- [ ] **Step 3: Delete the handler and its module declaration**

```bash
git rm src/catalog/pg_class.rs
grep -n "pub mod pg_class;\|mod pg_class;" src/catalog/mod.rs
```

Remove the matching line from `src/catalog/mod.rs`.

- [ ] **Step 4: Build**

Run: `cargo check 2>&1 | grep -E "^error|^warning: unused" | head -20`

Expected: clean. If you see `unused import` for something that was only used by the deleted branch, remove it too.

- [ ] **Step 5: Run the acceptance tests from Task 1**

Run: `cargo test --test pg_class_dt_test 2>&1 | tail -30`

Expected: all PASS. This closes #87.

If `test_dt_lists_user_tables` still returns 0 rows, the query is being caught by an earlier interception branch rather than the one just removed. Diagnose with:

```bash
RUST_LOG=pgsqlite=debug cargo test --test pg_class_dt_test test_dt_lists_user_tables 2>&1 | grep -i "INTERCEPT\|CHECK_TABLE_FACTOR" | head -20
```

The candidates are the system-function branch at `query_interceptor.rs:334` (entered because `\dt` contains `pg_table_is_visible`) and the catalog-JOIN branch at `:402`. Neither should claim this query — `:341` requires the literal `pg_class.relname` and `:402` requires a join to `pg_attribute`/`pg_type` — but if the log shows one of them returning `Some`, that branch needs the same treatment.

- [ ] **Step 6: Run the view tests from Task 3**

Run: `cargo test --test pg_class_view_test 2>&1 | tail -30`

Expected: all four PASS now that the view is no longer shadowed.

- [ ] **Step 7: Commit**

```bash
git add -A src/catalog/
git commit -m "fix: serve pg_class from SQLite instead of PgClassHandler (#87)"
```

---

### Task 5: Regression tests for the regex operators

**Files:**
- Create: `tests/catalog_regex_operators_test.rs`

**Interfaces:**
- Consumes: working `pg_class` from Task 4.
- Produces: nothing.

**Why:** Investigation found two distinct regex bugs. `!~` excluded every row (the #87 cause) and `~` was silently *ignored*, returning unfiltered results with no error. The second is a wrong-answer bug that no existing test covers.

- [ ] **Step 1: Write the tests**

Create `tests/catalog_regex_operators_test.rs`:

```rust
mod common;
use common::setup_test_server_with_init;

async fn server_with_two_tables() -> common::TestServer {
    setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY)").await?;
            db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY)").await?;
            Ok(())
        })
    }).await
}

#[tokio::test]
async fn test_regex_match_actually_filters() {
    let _ = env_logger::builder().is_test(true).try_init();
    let server = server_with_two_tables().await;

    let rows = server.client.query(
        "SELECT relname FROM pg_catalog.pg_class WHERE relname ~ '^cust'", &[]
    ).await.expect("~ query should succeed");

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();

    assert!(names.iter().any(|n| n == "customers"), "~ must match customers, got {names:?}");
    assert!(!names.iter().any(|n| n == "orders"),
        "~ must actually filter -- 'orders' does not match '^cust', got {names:?}");
}

#[tokio::test]
async fn test_regex_not_match_does_not_exclude_everything() {
    let _ = env_logger::builder().is_test(true).try_init();
    let server = server_with_two_tables().await;

    let rows = server.client.query(
        "SELECT relname FROM pg_catalog.pg_class WHERE relname !~ '^pg_toast'", &[]
    ).await.expect("!~ query should succeed");

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();

    assert!(names.iter().any(|n| n == "customers"),
        "issue #87: !~ must not exclude non-matching rows, got {names:?}");
    assert!(names.iter().any(|n| n == "orders"),
        "!~ must not exclude non-matching rows, got {names:?}");
}

#[tokio::test]
async fn test_regex_not_match_still_excludes_matches() {
    let _ = env_logger::builder().is_test(true).try_init();
    let server = server_with_two_tables().await;

    let rows = server.client.query(
        "SELECT relname FROM pg_catalog.pg_class WHERE relname !~ '^cust'", &[]
    ).await.expect("!~ query should succeed");

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();

    assert!(!names.iter().any(|n| n == "customers"),
        "!~ '^cust' must exclude customers, got {names:?}");
    assert!(names.iter().any(|n| n == "orders"),
        "!~ '^cust' must keep orders, got {names:?}");
}
```

- [ ] **Step 2: Run**

Run: `cargo test --test catalog_regex_operators_test 2>&1 | tail -30`

Expected: all three PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/catalog_regex_operators_test.rs
git commit -m "test: cover ~ and !~ operators on catalog tables (#87)"
```

---

### Task 6: Cross-catalog JOIN test

**Files:**
- Create: `tests/catalog_join_test.rs`

**Interfaces:**
- Consumes: working `pg_class` from Task 4.
- Produces: nothing.

**Why:** Before this work, `pg_class JOIN pg_constraint ON con.conrelid = c.oid` returned all `pg_class` rows with no `conname` column — the join predicate was never evaluated. It also could not have matched, because `pg_class` served `hash31` OIDs while `pg_constraint` persists unicode-formula OIDs. Both are fixed by serving `pg_class` from the view.

- [ ] **Step 1: Write the test**

Create `tests/catalog_join_test.rs`:

```rust
mod common;
use common::setup_test_server_with_init;

#[tokio::test]
async fn test_pg_class_joins_pg_constraint_on_oid() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)").await?;
            Ok(())
        })
    }).await;

    let rows = server.client.query(
        "SELECT c.relname, con.conname \
         FROM pg_catalog.pg_class c \
         JOIN pg_catalog.pg_constraint con ON con.conrelid = c.oid \
         WHERE c.relname = 'customers'",
        &[]
    ).await.expect("cross-catalog join should succeed");

    assert!(!rows.is_empty(),
        "pg_class.oid must match the persisted pg_constraint.conrelid");

    for row in &rows {
        let relname: String = row.get(0);
        assert_eq!(relname, "customers");
        let _conname: String = row.get(1);
    }
}

#[tokio::test]
async fn test_pg_class_oid_matches_persisted_formula() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)").await?;
            Ok(())
        })
    }).await;

    let rows = server.client.query(
        "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'customers' AND oid = '197947'",
        &[]
    ).await.expect("query should succeed");

    // unicode formula for 'customers':
    // c=99, u=117, s=115, len=9
    // (99*1000000 + 117*10000 + 115*100 + 63) % 1000000 + 16384 = 197947
    assert_eq!(rows.len(), 1,
        "pg_class must use the canonical unicode OID formula that constraint_populator persists");
}
```

- [ ] **Step 2: Run**

Run: `cargo test --test catalog_join_test 2>&1 | tail -30`

Expected: both PASS.

If `test_pg_class_joins_pg_constraint_on_oid` returns zero rows, check whether `constraint_populator` actually ran for this table: `SELECT conrelid, conname FROM pg_constraint` via a direct SQLite read of the test database. A table with only a PRIMARY KEY should still produce a `customers_pkey` row.

- [ ] **Step 3: Commit**

```bash
git add tests/catalog_join_test.rs
git commit -m "test: cover cross-catalog joins through pg_class.oid (#87)"
```

---

### Task 7: Full suite, existing-test fallout, and docs

**Files:**
- Modify: whichever existing tests assert literal `pg_class` OID values (identified in Step 2)
- Modify: `CLAUDE.md` (migration list)

**Interfaces:**
- Consumes: everything above.
- Produces: a green suite.

- [ ] **Step 1: Run the full suite**

Run: `cargo test 2>&1 | tail -40`

- [ ] **Step 2: Triage failures**

Expected fallout, per the spec: tests asserting a literal `pg_class` OID now see unicode-formula values instead of `hash31`. Find them:

```bash
cargo test 2>&1 | grep -E "^test .* FAILED|panicked at" | head -30
grep -rn "generate_oid_from_name\|16384" tests/ --include=*.rs | head -20
```

For each, update the expected OID to the unicode-formula value. Do **not** change the view to match an old test — the new values are the ones `pg_constraint`/`pg_index`/`pg_attrdef` already use on disk.

Any other failure is *not* expected fallout. Stop and investigate before changing the test.

- [ ] **Step 3: Update the stale migration list in CLAUDE.md**

`CLAUDE.md` says "Current Migrations (v1-v25)", which was already wrong (v26 and v27 exist). Update that section to v28 and add a line describing v28:

```markdown
### Current Migrations (v1-v28)
- v1-v10: Initial schema, ENUM, DateTime, Arrays, Full-Text Search, catalog tables
- v15-v19: pg_depend, pg_proc, pg_description, pg_roles/pg_user, pg_stats
- v20-v25: information_schema support (routines, views, referential_constraints, check_constraints, triggers), pg_tablespace
- v26-v27: Enhanced pg_attribute, pg_proc type fixes
- v28: pg_class full column parity; internal relations moved to pg_catalog/information_schema namespaces
```

- [ ] **Step 4: Pre-commit checklist**

Run each, and fix what it reports:

```bash
cargo check
cargo clippy
cargo build
cargo test
```

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore: update tests and docs for pg_class OID and migration v28 (#87)"
```

---

### Task 8: File the follow-up issues

**Files:** none — this task creates GitHub issues.

**Interfaces:**
- Consumes: nothing.
- Produces: nothing.

The spec deliberately deferred three problems. File them so they are not lost.

- [ ] **Step 1: File the OID collision issue**

```bash
gh issue create --title "pg_class OIDs collide for names sharing first 3 chars and length" --body "$(cat <<'EOF'
The canonical OID formula reads only the first three characters and the length:

```
((unicode(c1)*1000000) + (unicode(c2)*10000) + (unicode(c3)*100) + (len*7)) % 1000000 + 16384
```

Any two relations sharing those produce one OID. Demonstrated:

| Names | Shared OID |
| --- | --- |
| `orders` / `orderz` | 166426 |
| `user_roles` / `user_rolez` | 176554 |
| `customers` / `customerz` | 197947 |

OID is the join key from `pg_class` to `pg_attribute`, `pg_index`, `pg_constraint`, and `pg_depend`, so an ORM introspecting `orders` can receive `orderz`'s columns.

Pre-existing, not introduced by #87. Fixing it means one canonical low-collision UDF used by every view and Rust site, plus a data migration rewriting persisted OIDs in `pg_constraint.conrelid`, `pg_index.indrelid`, `pg_attrdef.adrelid`, and `pg_depend.objid`/`refobjid`.

Related: `oid_hash` (`src/functions/hash_functions.rs:25`) uses Rust's `DefaultHasher`, which is not guaranteed stable across Rust releases, so OIDs derived from it could change on rebuild. It should be retired in the same work.
EOF
)"
```

- [ ] **Step 2: File the WhereEvaluator issue**

```bash
gh issue create --title "WhereEvaluator: unknown predicates flip to exclusion under NOT" --body "$(cat <<'EOF'
`WhereEvaluator::evaluate` returns `true` for anything it cannot evaluate — "default to including the row" (`src/catalog/where_evaluator.rs:53-56`). Under `NOT`, `!true` becomes `false` (`:151`), so an unevaluable predicate silently *excludes* every row.

This was the root cause of #87 for `pg_class`, which no longer uses this code path. The ~20 remaining catalog handlers still do.

Demonstrated with an arbitrary unknown function:

| Query | Result |
| --- | --- |
| `WHERE foobar(c.relname)` | all rows |
| `WHERE NOT foobar(c.relname)` | 0 rows |

Suggested fix: make evaluation tri-state (`Option<bool>`, `None` = cannot evaluate). `NOT None` stays `None`; `None AND x` is `x`; the top level treats `None` as include. Keep the current `evaluate() -> bool` as a thin `evaluate_opt().unwrap_or(true)` wrapper so the 22 call sites are untouched.

A related gap: joined columns do not resolve at all. `n.nspname = 'public'` returns 0 rows and `n.nspname <> 'public'` returns everything — both wrong.
EOF
)"
```

- [ ] **Step 3: File the next-catalog migration issue**

```bash
gh issue create --title "Migrate remaining catalog handlers to SQLite views" --body "$(cat <<'EOF'
#87 established the pattern: enrich the SQLite view to full column parity, then delete the Rust handler and its interception branch, so SQLite executes joins, WHERE, regex, and projection.

`src/catalog/` is ~9,300 lines of hand-rolled query engine over relations that mostly already exist as views. Each remaining handler is a source of silent wrong-answer bugs of the kind #87 documented.

Suggested order, most-used first: `pg_attribute`, `pg_proc`, `pg_description`, `pg_roles`/`pg_user`, `pg_stats`.

Prerequisite for handlers whose views do not yet exist: create the view first.

See `docs/superpowers/specs/2026-08-10-pg-class-sqlite-engine-design.md`.
EOF
)"
```

- [ ] **Step 4: Comment on #88**

Moving internal relations into the `pg_catalog` namespace (v28) is the same mechanism #88 needs.

```bash
gh issue comment 88 --body "Migration v28 (from #87) assigns \`pg_%\` relations to the \`pg_catalog\` namespace (oid 11) and \`information_schema_%\` to a new \`information_schema\` namespace (oid 13000) in the \`pg_class\` view. Clients filtering on \`nspname\` — which \`\\dt\` and most ORMs do — no longer see them in \`public\`.

This does not close #88 on its own: \`information_schema.tables\` builds its own row set and needs the same namespace awareness."
```

- [ ] **Step 5: Close #87**

```bash
gh issue close 87 --comment "Fixed by serving pg_class from SQLite instead of PgClassHandler. Root cause: RegexTranslator rewrote \`n.nspname !~ '^pg_toast'\` into \`NOT regexp(...)\` before catalog interception; WhereEvaluator defaulted the unknown function to true; NOT inverted that into a universal row filter.

Also fixed: \`~\` was silently ignored on catalog tables (returning unfiltered results), cross-catalog joins through \`pg_class.oid\` never matched because pg_class served hash31 OIDs while pg_constraint/pg_index/pg_attrdef persist unicode-formula OIDs, and \`\\dt\` no longer lists pgsqlite's own pg_* relations.

Design: \`docs/superpowers/specs/2026-08-10-pg-class-sqlite-engine-design.md\`"
```
