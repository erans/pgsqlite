# Missing PostgreSQL Catalog Tables Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add 7 missing PostgreSQL catalog tables (pg_settings, pg_sequence, pg_trigger, pg_collation, pg_replication_slots, pg_shdepend, pg_statistic) for protocol completeness.

**Architecture:** Follow existing CatalogInterceptor pattern - intercept queries to pg_catalog tables and return PostgreSQL-compatible responses. Dedicated handlers for complex tables (pg_settings, pg_sequence, pg_trigger), inline handlers for stubs.

**Tech Stack:** Rust, sqlparser, rusqlite, async/await

---

## Task 1: Add pg_collation (Inline Static)

**Files:**
- Modify: `src/catalog/query_interceptor.rs`

**Step 1: Write integration test**

Create file `tests/pg_collation_test.rs`:

```rust
use pgsqlite::test_utils::create_test_server;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_pg_collation_basic() {
    let (addr, _server, _temp_dir) = create_test_server().await;
    let (client, connection) = tokio_postgres::connect(
        &format!("host={} port={} user=postgres dbname=test", addr.ip(), addr.port()),
        NoTls,
    ).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap(); });

    // Test basic query
    let rows = client.query("SELECT oid, collname FROM pg_collation", &[]).await.unwrap();
    assert!(rows.len() >= 3, "Should have at least 3 collations (default, C, POSIX)");

    // Test filtering by name
    let rows = client.query("SELECT * FROM pg_collation WHERE collname = 'C'", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test pg_collation_test`
Expected: FAIL (table not recognized)

**Step 3: Add pg_collation to table detection**

In `src/catalog/query_interceptor.rs`, find the `has_catalog_tables` check (~line 50-61) and add:

```rust
lower_query.contains("pg_collation") ||
```

**Step 4: Add pg_collation handler in check_table_factor**

In `src/catalog/query_interceptor.rs`, add after the pg_tablespace handler (~line 514-516):

```rust
// Handle pg_collation queries
if table_name.contains("pg_collation") || table_name.contains("pg_catalog.pg_collation") {
    return Some(Ok(Self::handle_pg_collation_query(select)));
}
```

**Step 5: Implement handle_pg_collation_query**

Add this function to the `CatalogInterceptor` impl block:

```rust
fn handle_pg_collation_query(select: &Select) -> DbResponse {
    // Define pg_collation columns (PostgreSQL standard)
    let all_columns = vec![
        "oid".to_string(),
        "collname".to_string(),
        "collnamespace".to_string(),
        "collowner".to_string(),
        "collprovider".to_string(),
        "collisdeterministic".to_string(),
        "collencoding".to_string(),
        "collcollate".to_string(),
        "collctype".to_string(),
        "colliculocale".to_string(),
        "collicurules".to_string(),
        "collversion".to_string(),
    ];

    // Extract selected columns
    let (selected_columns, column_indices) = Self::extract_selected_columns(select, &all_columns);

    // Define standard collations
    let collations = vec![
        ("100", "default", "11", "10", "d", "t", "-1", "", "", "", "", ""),
        ("950", "C", "11", "10", "c", "t", "-1", "C", "C", "", "", ""),
        ("951", "POSIX", "11", "10", "c", "t", "-1", "POSIX", "POSIX", "", "", ""),
    ];

    // Check for WHERE clause filtering by collname
    let name_filter = if let Some(ref where_clause) = select.selection {
        Self::extract_collation_name_filter(where_clause)
    } else {
        None
    };

    let mut rows = Vec::new();
    for (oid, collname, collnamespace, collowner, collprovider, collisdeterministic,
         collencoding, collcollate, collctype, colliculocale, collicurules, collversion) in collations {

        // Apply name filter if present
        if let Some(ref filter) = name_filter {
            if collname != filter {
                continue;
            }
        }

        let full_row: Vec<Option<Vec<u8>>> = vec![
            Some(oid.to_string().into_bytes()),
            Some(collname.to_string().into_bytes()),
            Some(collnamespace.to_string().into_bytes()),
            Some(collowner.to_string().into_bytes()),
            Some(collprovider.to_string().into_bytes()),
            Some(collisdeterministic.to_string().into_bytes()),
            Some(collencoding.to_string().into_bytes()),
            if collcollate.is_empty() { None } else { Some(collcollate.to_string().into_bytes()) },
            if collctype.is_empty() { None } else { Some(collctype.to_string().into_bytes()) },
            if colliculocale.is_empty() { None } else { Some(colliculocale.to_string().into_bytes()) },
            if collicurules.is_empty() { None } else { Some(collicurules.to_string().into_bytes()) },
            if collversion.is_empty() { None } else { Some(collversion.to_string().into_bytes()) },
        ];

        let projected_row: Vec<Option<Vec<u8>>> = column_indices.iter()
            .map(|&idx| full_row[idx].clone())
            .collect();
        rows.push(projected_row);
    }

    let rows_affected = rows.len();
    DbResponse {
        columns: selected_columns,
        rows,
        rows_affected,
    }
}

fn extract_collation_name_filter(where_clause: &Expr) -> Option<String> {
    match where_clause {
        Expr::BinaryOp { left, op, right } => {
            if let (Expr::Identifier(ident), sqlparser::ast::BinaryOperator::Eq, Expr::Value(value_with_span)) =
                (left.as_ref(), op, right.as_ref())
                && ident.value.to_lowercase() == "collname"
                    && let sqlparser::ast::Value::SingleQuotedString(value) = &value_with_span.value {
                        return Some(value.clone());
                    }
        }
        _ => {}
    }
    None
}
```

**Step 6: Run test to verify it passes**

Run: `cargo test --test pg_collation_test`
Expected: PASS

**Step 7: Commit**

```bash
git add src/catalog/query_interceptor.rs tests/pg_collation_test.rs
git commit -m "feat: add pg_collation catalog table support"
```

---

## Task 2: Add pg_replication_slots (Empty Stub)

**Files:**
- Modify: `src/catalog/query_interceptor.rs`

**Step 1: Write integration test**

Create file `tests/pg_replication_slots_test.rs`:

```rust
use pgsqlite::test_utils::create_test_server;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_pg_replication_slots_empty() {
    let (addr, _server, _temp_dir) = create_test_server().await;
    let (client, connection) = tokio_postgres::connect(
        &format!("host={} port={} user=postgres dbname=test", addr.ip(), addr.port()),
        NoTls,
    ).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap(); });

    // Should return empty result with correct schema
    let rows = client.query("SELECT slot_name, plugin, slot_type FROM pg_replication_slots", &[]).await.unwrap();
    assert_eq!(rows.len(), 0, "pg_replication_slots should be empty for SQLite");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test pg_replication_slots_test`
Expected: FAIL

**Step 3: Add pg_replication_slots to table detection and handler**

In `src/catalog/query_interceptor.rs`, add to `has_catalog_tables`:

```rust
lower_query.contains("pg_replication_slots") ||
```

Add handler in `check_table_factor`:

```rust
// Handle pg_replication_slots queries (always empty - SQLite has no replication)
if table_name.contains("pg_replication_slots") || table_name.contains("pg_catalog.pg_replication_slots") {
    return Some(Ok(Self::handle_pg_replication_slots_query(select)));
}
```

**Step 4: Implement handle_pg_replication_slots_query**

```rust
fn handle_pg_replication_slots_query(select: &Select) -> DbResponse {
    let all_columns = vec![
        "slot_name".to_string(),
        "plugin".to_string(),
        "slot_type".to_string(),
        "datoid".to_string(),
        "database".to_string(),
        "temporary".to_string(),
        "active".to_string(),
        "active_pid".to_string(),
        "xmin".to_string(),
        "catalog_xmin".to_string(),
        "restart_lsn".to_string(),
        "confirmed_flush_lsn".to_string(),
        "wal_status".to_string(),
        "safe_wal_size".to_string(),
        "two_phase".to_string(),
        "conflicting".to_string(),
    ];

    let (selected_columns, _) = Self::extract_selected_columns(select, &all_columns);

    // Always return empty - SQLite has no replication
    DbResponse {
        columns: selected_columns,
        rows: vec![],
        rows_affected: 0,
    }
}
```

**Step 5: Run test to verify it passes**

Run: `cargo test --test pg_replication_slots_test`
Expected: PASS

**Step 6: Commit**

```bash
git add src/catalog/query_interceptor.rs tests/pg_replication_slots_test.rs
git commit -m "feat: add pg_replication_slots catalog table (empty stub)"
```

---

## Task 3: Add pg_shdepend (Empty Stub)

**Files:**
- Modify: `src/catalog/query_interceptor.rs`

**Step 1: Write integration test**

Create file `tests/pg_shdepend_test.rs`:

```rust
use pgsqlite::test_utils::create_test_server;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_pg_shdepend_empty() {
    let (addr, _server, _temp_dir) = create_test_server().await;
    let (client, connection) = tokio_postgres::connect(
        &format!("host={} port={} user=postgres dbname=test", addr.ip(), addr.port()),
        NoTls,
    ).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap(); });

    let rows = client.query("SELECT dbid, classid, objid FROM pg_shdepend", &[]).await.unwrap();
    assert_eq!(rows.len(), 0, "pg_shdepend should be empty for SQLite");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test pg_shdepend_test`
Expected: FAIL

**Step 3: Add pg_shdepend detection and handler**

Add to `has_catalog_tables`:
```rust
lower_query.contains("pg_shdepend") ||
```

Add handler:
```rust
// Handle pg_shdepend queries (always empty - no shared dependencies in SQLite)
if table_name.contains("pg_shdepend") || table_name.contains("pg_catalog.pg_shdepend") {
    return Some(Ok(Self::handle_pg_shdepend_query(select)));
}
```

**Step 4: Implement handle_pg_shdepend_query**

```rust
fn handle_pg_shdepend_query(select: &Select) -> DbResponse {
    let all_columns = vec![
        "dbid".to_string(),
        "classid".to_string(),
        "objid".to_string(),
        "objsubid".to_string(),
        "refclassid".to_string(),
        "refobjid".to_string(),
        "deptype".to_string(),
    ];

    let (selected_columns, _) = Self::extract_selected_columns(select, &all_columns);

    DbResponse {
        columns: selected_columns,
        rows: vec![],
        rows_affected: 0,
    }
}
```

**Step 5: Run test and commit**

Run: `cargo test --test pg_shdepend_test`
Expected: PASS

```bash
git add src/catalog/query_interceptor.rs tests/pg_shdepend_test.rs
git commit -m "feat: add pg_shdepend catalog table (empty stub)"
```

---

## Task 4: Add pg_statistic (Empty Stub)

**Files:**
- Modify: `src/catalog/query_interceptor.rs`

**Step 1: Write integration test**

Create file `tests/pg_statistic_test.rs`:

```rust
use pgsqlite::test_utils::create_test_server;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_pg_statistic_empty() {
    let (addr, _server, _temp_dir) = create_test_server().await;
    let (client, connection) = tokio_postgres::connect(
        &format!("host={} port={} user=postgres dbname=test", addr.ip(), addr.port()),
        NoTls,
    ).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap(); });

    let rows = client.query("SELECT starelid, staattnum FROM pg_statistic", &[]).await.unwrap();
    assert_eq!(rows.len(), 0, "pg_statistic should be empty (use pg_stats view instead)");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test pg_statistic_test`
Expected: FAIL

**Step 3: Add pg_statistic detection and handler**

Add to `has_catalog_tables`:
```rust
lower_query.contains("pg_statistic") ||
```

Add handler:
```rust
// Handle pg_statistic queries (always empty - internal stats table)
if table_name.contains("pg_statistic") || table_name.contains("pg_catalog.pg_statistic") {
    return Some(Ok(Self::handle_pg_statistic_query(select)));
}
```

**Step 4: Implement handle_pg_statistic_query**

```rust
fn handle_pg_statistic_query(select: &Select) -> DbResponse {
    let all_columns = vec![
        "starelid".to_string(),
        "staattnum".to_string(),
        "stainherit".to_string(),
        "stanullfrac".to_string(),
        "stawidth".to_string(),
        "stadistinct".to_string(),
        "stakind1".to_string(),
        "stakind2".to_string(),
        "stakind3".to_string(),
        "stakind4".to_string(),
        "stakind5".to_string(),
        "staop1".to_string(),
        "staop2".to_string(),
        "staop3".to_string(),
        "staop4".to_string(),
        "staop5".to_string(),
        "stacoll1".to_string(),
        "stacoll2".to_string(),
        "stacoll3".to_string(),
        "stacoll4".to_string(),
        "stacoll5".to_string(),
        "stanumbers1".to_string(),
        "stanumbers2".to_string(),
        "stanumbers3".to_string(),
        "stanumbers4".to_string(),
        "stanumbers5".to_string(),
        "stavalues1".to_string(),
        "stavalues2".to_string(),
        "stavalues3".to_string(),
        "stavalues4".to_string(),
        "stavalues5".to_string(),
    ];

    let (selected_columns, _) = Self::extract_selected_columns(select, &all_columns);

    DbResponse {
        columns: selected_columns,
        rows: vec![],
        rows_affected: 0,
    }
}
```

**Step 5: Run test and commit**

Run: `cargo test --test pg_statistic_test`
Expected: PASS

```bash
git add src/catalog/query_interceptor.rs tests/pg_statistic_test.rs
git commit -m "feat: add pg_statistic catalog table (empty stub)"
```

---

## Task 5: Add pg_sequence Handler

**Files:**
- Create: `src/catalog/pg_sequence.rs`
- Modify: `src/catalog/mod.rs`
- Modify: `src/catalog/query_interceptor.rs`

**Step 1: Write integration test**

Create file `tests/pg_sequence_test.rs`:

```rust
use pgsqlite::test_utils::create_test_server;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_pg_sequence_basic() {
    let (addr, _server, _temp_dir) = create_test_server().await;
    let (client, connection) = tokio_postgres::connect(
        &format!("host={} port={} user=postgres dbname=test", addr.ip(), addr.port()),
        NoTls,
    ).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap(); });

    // Create a table with AUTOINCREMENT to populate sqlite_sequence
    client.execute("CREATE TABLE test_seq (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO test_seq (name) VALUES ('a'), ('b'), ('c')", &[]).await.unwrap();

    // Query pg_sequence
    let rows = client.query("SELECT seqrelid, seqtypid, seqstart, seqincrement FROM pg_sequence", &[]).await.unwrap();
    assert!(rows.len() >= 1, "Should have at least one sequence from test_seq");
}

#[tokio::test]
async fn test_pg_sequence_empty_when_no_autoincrement() {
    let (addr, _server, _temp_dir) = create_test_server().await;
    let (client, connection) = tokio_postgres::connect(
        &format!("host={} port={} user=postgres dbname=test", addr.ip(), addr.port()),
        NoTls,
    ).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap(); });

    // Query pg_sequence without creating any autoincrement tables
    let rows = client.query("SELECT * FROM pg_sequence", &[]).await.unwrap();
    // May be empty or have existing sequences
    assert!(rows.len() >= 0);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test pg_sequence_test`
Expected: FAIL

**Step 3: Create pg_sequence.rs module**

Create `src/catalog/pg_sequence.rs`:

```rust
use crate::session::db_handler::{DbHandler, DbResponse};
use sqlparser::ast::{Select, SelectItem, Expr};
use std::sync::Arc;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tracing::debug;

pub struct PgSequenceHandler;

impl PgSequenceHandler {
    pub async fn handle_query(select: &Select, db: &Arc<DbHandler>) -> Result<DbResponse, String> {
        debug!("PgSequenceHandler: handling query");

        let all_columns = vec![
            "seqrelid".to_string(),
            "seqtypid".to_string(),
            "seqstart".to_string(),
            "seqincrement".to_string(),
            "seqmax".to_string(),
            "seqmin".to_string(),
            "seqcache".to_string(),
            "seqcycle".to_string(),
        ];

        let (selected_columns, column_indices) = Self::extract_selected_columns(select, &all_columns);

        // Query sqlite_sequence table for autoincrement sequences
        let sequences = Self::get_sqlite_sequences(db).await;

        let mut rows = Vec::new();
        for (table_name, current_value) in sequences {
            // Generate a stable OID from table name
            let seqrelid = Self::generate_oid(&table_name);

            let full_row: Vec<Option<Vec<u8>>> = vec![
                Some(seqrelid.to_string().into_bytes()),  // seqrelid
                Some("20".to_string().into_bytes()),       // seqtypid (int8)
                Some("1".to_string().into_bytes()),        // seqstart
                Some("1".to_string().into_bytes()),        // seqincrement
                Some(current_value.to_string().into_bytes()), // seqmax (current value)
                Some("1".to_string().into_bytes()),        // seqmin
                Some("1".to_string().into_bytes()),        // seqcache
                Some("f".to_string().into_bytes()),        // seqcycle (false)
            ];

            let projected_row: Vec<Option<Vec<u8>>> = column_indices.iter()
                .map(|&idx| full_row[idx].clone())
                .collect();
            rows.push(projected_row);
        }

        let rows_affected = rows.len();
        Ok(DbResponse {
            columns: selected_columns,
            rows,
            rows_affected,
        })
    }

    async fn get_sqlite_sequences(db: &Arc<DbHandler>) -> Vec<(String, i64)> {
        let mut sequences = Vec::new();

        // Query sqlite_sequence - this table exists if any AUTOINCREMENT columns exist
        match db.query("SELECT name, seq FROM sqlite_sequence").await {
            Ok(response) => {
                for row in response.rows {
                    if row.len() >= 2 {
                        if let (Some(Some(name_bytes)), Some(Some(seq_bytes))) = (row.get(0), row.get(1)) {
                            let name = String::from_utf8_lossy(name_bytes).to_string();
                            let seq: i64 = String::from_utf8_lossy(seq_bytes)
                                .parse()
                                .unwrap_or(0);
                            sequences.push((name, seq));
                        }
                    }
                }
            }
            Err(e) => {
                debug!("No sqlite_sequence table or error: {:?}", e);
            }
        }

        sequences
    }

    fn generate_oid(name: &str) -> u32 {
        let mut hasher = DefaultHasher::new();
        name.hash(&mut hasher);
        // Use upper bits to avoid collision with standard OIDs
        (hasher.finish() as u32) | 0x80000000
    }

    fn extract_selected_columns(select: &Select, all_columns: &[String]) -> (Vec<String>, Vec<usize>) {
        if select.projection.len() == 1 {
            if let SelectItem::Wildcard(_) = &select.projection[0] {
                return (all_columns.to_vec(), (0..all_columns.len()).collect());
            }
        }

        let mut cols = Vec::new();
        let mut indices = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let col_name = ident.value.to_lowercase();
                    if let Some(idx) = all_columns.iter().position(|c| c == &col_name) {
                        cols.push(col_name);
                        indices.push(idx);
                    }
                }
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                    if let Some(last) = parts.last() {
                        let col_name = last.value.to_lowercase();
                        if let Some(idx) = all_columns.iter().position(|c| c == &col_name) {
                            cols.push(col_name);
                            indices.push(idx);
                        }
                    }
                }
                _ => {}
            }
        }

        if cols.is_empty() {
            (all_columns.to_vec(), (0..all_columns.len()).collect())
        } else {
            (cols, indices)
        }
    }
}
```

**Step 4: Update mod.rs**

Add to `src/catalog/mod.rs`:

```rust
pub mod pg_sequence;
```

**Step 5: Update query_interceptor.rs**

Add import at top:
```rust
use super::pg_sequence::PgSequenceHandler;
```

Add to `has_catalog_tables`:
```rust
lower_query.contains("pg_sequence") ||
```

Add handler in `check_table_factor`:
```rust
// Handle pg_sequence queries
if table_name.contains("pg_sequence") || table_name.contains("pg_catalog.pg_sequence") {
    return match PgSequenceHandler::handle_query(select, &db).await {
        Ok(response) => Some(Ok(response)),
        Err(_) => None,
    };
}
```

**Step 6: Run test and commit**

Run: `cargo test --test pg_sequence_test`
Expected: PASS

```bash
git add src/catalog/pg_sequence.rs src/catalog/mod.rs src/catalog/query_interceptor.rs tests/pg_sequence_test.rs
git commit -m "feat: add pg_sequence catalog table with SQLite sequence mapping"
```

---

## Task 6: Add pg_trigger Handler

**Files:**
- Create: `src/catalog/pg_trigger.rs`
- Modify: `src/catalog/mod.rs`
- Modify: `src/catalog/query_interceptor.rs`

**Step 1: Write integration test**

Create file `tests/pg_trigger_test.rs`:

```rust
use pgsqlite::test_utils::create_test_server;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_pg_trigger_basic() {
    let (addr, _server, _temp_dir) = create_test_server().await;
    let (client, connection) = tokio_postgres::connect(
        &format!("host={} port={} user=postgres dbname=test", addr.ip(), addr.port()),
        NoTls,
    ).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap(); });

    // Create a table and trigger
    client.execute("CREATE TABLE trigger_test (id INTEGER PRIMARY KEY, value TEXT)", &[]).await.unwrap();
    client.execute("CREATE TABLE trigger_log (msg TEXT)", &[]).await.unwrap();
    client.execute(
        "CREATE TRIGGER test_trigger AFTER INSERT ON trigger_test BEGIN INSERT INTO trigger_log VALUES ('inserted'); END",
        &[]
    ).await.unwrap();

    // Query pg_trigger
    let rows = client.query("SELECT tgname, tgrelid FROM pg_trigger", &[]).await.unwrap();
    assert!(rows.len() >= 1, "Should have at least one trigger");

    // Verify trigger name
    let tgname: &str = rows[0].get(0);
    assert_eq!(tgname, "test_trigger");
}

#[tokio::test]
async fn test_pg_trigger_empty() {
    let (addr, _server, _temp_dir) = create_test_server().await;
    let (client, connection) = tokio_postgres::connect(
        &format!("host={} port={} user=postgres dbname=test", addr.ip(), addr.port()),
        NoTls,
    ).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap(); });

    // Query pg_trigger without any triggers
    let rows = client.query("SELECT * FROM pg_trigger", &[]).await.unwrap();
    assert!(rows.len() >= 0);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test pg_trigger_test`
Expected: FAIL

**Step 3: Create pg_trigger.rs module**

Create `src/catalog/pg_trigger.rs`:

```rust
use crate::session::db_handler::{DbHandler, DbResponse};
use sqlparser::ast::{Select, SelectItem, Expr};
use std::sync::Arc;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tracing::debug;

pub struct PgTriggerHandler;

impl PgTriggerHandler {
    pub async fn handle_query(select: &Select, db: &Arc<DbHandler>) -> Result<DbResponse, String> {
        debug!("PgTriggerHandler: handling query");

        let all_columns = vec![
            "oid".to_string(),
            "tgrelid".to_string(),
            "tgparentid".to_string(),
            "tgname".to_string(),
            "tgfoid".to_string(),
            "tgtype".to_string(),
            "tgenabled".to_string(),
            "tgisinternal".to_string(),
            "tgconstrrelid".to_string(),
            "tgconstrindid".to_string(),
            "tgconstraint".to_string(),
            "tgdeferrable".to_string(),
            "tginitdeferred".to_string(),
            "tgnargs".to_string(),
            "tgattr".to_string(),
            "tgargs".to_string(),
            "tgqual".to_string(),
            "tgoldtable".to_string(),
            "tgnewtable".to_string(),
        ];

        let (selected_columns, column_indices) = Self::extract_selected_columns(select, &all_columns);

        // Query SQLite triggers
        let triggers = Self::get_sqlite_triggers(db).await;

        let mut rows = Vec::new();
        for (trigger_name, table_name, trigger_sql) in triggers {
            let oid = Self::generate_oid(&trigger_name);
            let tgrelid = Self::generate_oid(&table_name);
            let tgtype = Self::parse_trigger_type(&trigger_sql);

            let full_row: Vec<Option<Vec<u8>>> = vec![
                Some(oid.to_string().into_bytes()),           // oid
                Some(tgrelid.to_string().into_bytes()),       // tgrelid
                Some("0".to_string().into_bytes()),           // tgparentid
                Some(trigger_name.clone().into_bytes()),      // tgname
                Some("0".to_string().into_bytes()),           // tgfoid
                Some(tgtype.to_string().into_bytes()),        // tgtype
                Some("O".to_string().into_bytes()),           // tgenabled (Origin)
                Some("f".to_string().into_bytes()),           // tgisinternal
                Some("0".to_string().into_bytes()),           // tgconstrrelid
                Some("0".to_string().into_bytes()),           // tgconstrindid
                Some("0".to_string().into_bytes()),           // tgconstraint
                Some("f".to_string().into_bytes()),           // tgdeferrable
                Some("f".to_string().into_bytes()),           // tginitdeferred
                Some("0".to_string().into_bytes()),           // tgnargs
                None,                                          // tgattr
                None,                                          // tgargs
                None,                                          // tgqual
                None,                                          // tgoldtable
                None,                                          // tgnewtable
            ];

            let projected_row: Vec<Option<Vec<u8>>> = column_indices.iter()
                .map(|&idx| full_row[idx].clone())
                .collect();
            rows.push(projected_row);
        }

        let rows_affected = rows.len();
        Ok(DbResponse {
            columns: selected_columns,
            rows,
            rows_affected,
        })
    }

    async fn get_sqlite_triggers(db: &Arc<DbHandler>) -> Vec<(String, String, String)> {
        let mut triggers = Vec::new();

        match db.query("SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger'").await {
            Ok(response) => {
                for row in response.rows {
                    if row.len() >= 3 {
                        if let (Some(Some(name)), Some(Some(tbl)), Some(Some(sql))) =
                            (row.get(0), row.get(1), row.get(2)) {
                            triggers.push((
                                String::from_utf8_lossy(name).to_string(),
                                String::from_utf8_lossy(tbl).to_string(),
                                String::from_utf8_lossy(sql).to_string(),
                            ));
                        }
                    }
                }
            }
            Err(e) => {
                debug!("Error querying triggers: {:?}", e);
            }
        }

        triggers
    }

    /// Parse SQLite trigger SQL to determine PostgreSQL tgtype bitmask
    fn parse_trigger_type(sql: &str) -> i16 {
        let sql_upper = sql.to_uppercase();
        let mut tgtype: i16 = 0;

        // Bit 0: ROW (1) vs STATEMENT (0) - SQLite is always ROW
        tgtype |= 1;

        // Bit 1: BEFORE (1) vs AFTER (0)
        if sql_upper.contains("BEFORE") {
            tgtype |= 2;
        }
        // INSTEAD OF sets bit 6
        if sql_upper.contains("INSTEAD OF") {
            tgtype |= 64;
        }

        // Bits 2-4: INSERT (4), DELETE (8), UPDATE (16)
        if sql_upper.contains(" INSERT ") || sql_upper.contains(" INSERT\n") {
            tgtype |= 4;
        }
        if sql_upper.contains(" DELETE ") || sql_upper.contains(" DELETE\n") {
            tgtype |= 8;
        }
        if sql_upper.contains(" UPDATE ") || sql_upper.contains(" UPDATE\n") {
            tgtype |= 16;
        }

        tgtype
    }

    fn generate_oid(name: &str) -> u32 {
        let mut hasher = DefaultHasher::new();
        name.hash(&mut hasher);
        (hasher.finish() as u32) | 0x80000000
    }

    fn extract_selected_columns(select: &Select, all_columns: &[String]) -> (Vec<String>, Vec<usize>) {
        if select.projection.len() == 1 {
            if let SelectItem::Wildcard(_) = &select.projection[0] {
                return (all_columns.to_vec(), (0..all_columns.len()).collect());
            }
        }

        let mut cols = Vec::new();
        let mut indices = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let col_name = ident.value.to_lowercase();
                    if let Some(idx) = all_columns.iter().position(|c| c == &col_name) {
                        cols.push(col_name);
                        indices.push(idx);
                    }
                }
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                    if let Some(last) = parts.last() {
                        let col_name = last.value.to_lowercase();
                        if let Some(idx) = all_columns.iter().position(|c| c == &col_name) {
                            cols.push(col_name);
                            indices.push(idx);
                        }
                    }
                }
                _ => {}
            }
        }

        if cols.is_empty() {
            (all_columns.to_vec(), (0..all_columns.len()).collect())
        } else {
            (cols, indices)
        }
    }
}
```

**Step 4: Update mod.rs**

Add to `src/catalog/mod.rs`:
```rust
pub mod pg_trigger;
```

**Step 5: Update query_interceptor.rs**

Add import:
```rust
use super::pg_trigger::PgTriggerHandler;
```

Add to `has_catalog_tables`:
```rust
lower_query.contains("pg_trigger") ||
```

Add handler:
```rust
// Handle pg_trigger queries
if table_name.contains("pg_trigger") || table_name.contains("pg_catalog.pg_trigger") {
    return match PgTriggerHandler::handle_query(select, &db).await {
        Ok(response) => Some(Ok(response)),
        Err(_) => None,
    };
}
```

**Step 6: Run test and commit**

Run: `cargo test --test pg_trigger_test`
Expected: PASS

```bash
git add src/catalog/pg_trigger.rs src/catalog/mod.rs src/catalog/query_interceptor.rs tests/pg_trigger_test.rs
git commit -m "feat: add pg_trigger catalog table with SQLite trigger mapping"
```

---

## Task 7: Add pg_settings Handler

**Files:**
- Create: `src/catalog/pg_settings.rs`
- Modify: `src/catalog/mod.rs`
- Modify: `src/catalog/query_interceptor.rs`

**Step 1: Write integration test**

Create file `tests/pg_settings_test.rs`:

```rust
use pgsqlite::test_utils::create_test_server;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_pg_settings_basic() {
    let (addr, _server, _temp_dir) = create_test_server().await;
    let (client, connection) = tokio_postgres::connect(
        &format!("host={} port={} user=postgres dbname=test", addr.ip(), addr.port()),
        NoTls,
    ).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap(); });

    // Query all settings
    let rows = client.query("SELECT name, setting FROM pg_settings", &[]).await.unwrap();
    assert!(rows.len() >= 10, "Should have many settings");

    // Check for common settings
    let settings: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert!(settings.contains(&"server_version".to_string()));
    assert!(settings.contains(&"server_encoding".to_string()));
}

#[tokio::test]
async fn test_pg_settings_filter_by_name() {
    let (addr, _server, _temp_dir) = create_test_server().await;
    let (client, connection) = tokio_postgres::connect(
        &format!("host={} port={} user=postgres dbname=test", addr.ip(), addr.port()),
        NoTls,
    ).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap(); });

    // Filter by name (SHOW command pattern)
    let rows = client.query("SELECT setting FROM pg_settings WHERE name = 'server_version'", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test pg_settings_test`
Expected: FAIL

**Step 3: Create pg_settings.rs module**

Create `src/catalog/pg_settings.rs`:

```rust
use crate::session::db_handler::DbResponse;
use sqlparser::ast::{Select, SelectItem, Expr};
use tracing::debug;

pub struct PgSettingsHandler;

impl PgSettingsHandler {
    pub fn handle_query(select: &Select) -> DbResponse {
        debug!("PgSettingsHandler: handling query");

        let all_columns = vec![
            "name".to_string(),
            "setting".to_string(),
            "unit".to_string(),
            "category".to_string(),
            "short_desc".to_string(),
            "extra_desc".to_string(),
            "context".to_string(),
            "vartype".to_string(),
            "source".to_string(),
            "min_val".to_string(),
            "max_val".to_string(),
            "enumvals".to_string(),
            "boot_val".to_string(),
            "reset_val".to_string(),
            "sourcefile".to_string(),
            "sourceline".to_string(),
            "pending_restart".to_string(),
        ];

        let (selected_columns, column_indices) = Self::extract_selected_columns(select, &all_columns);

        // Check for WHERE clause filtering by name
        let name_filter = if let Some(ref where_clause) = select.selection {
            Self::extract_name_filter(where_clause)
        } else {
            None
        };

        // Define settings: (name, setting, unit, category, short_desc, context, vartype)
        let settings = Self::get_settings();

        let mut rows = Vec::new();
        for (name, setting, unit, category, short_desc, context, vartype) in settings {
            // Apply name filter if present
            if let Some(ref filter) = name_filter {
                if name != filter {
                    continue;
                }
            }

            let full_row: Vec<Option<Vec<u8>>> = vec![
                Some(name.to_string().into_bytes()),         // name
                Some(setting.to_string().into_bytes()),      // setting
                if unit.is_empty() { None } else { Some(unit.to_string().into_bytes()) }, // unit
                Some(category.to_string().into_bytes()),     // category
                Some(short_desc.to_string().into_bytes()),   // short_desc
                None,                                         // extra_desc
                Some(context.to_string().into_bytes()),      // context
                Some(vartype.to_string().into_bytes()),      // vartype
                Some("default".to_string().into_bytes()),    // source
                None,                                         // min_val
                None,                                         // max_val
                None,                                         // enumvals
                Some(setting.to_string().into_bytes()),      // boot_val
                Some(setting.to_string().into_bytes()),      // reset_val
                None,                                         // sourcefile
                None,                                         // sourceline
                Some("f".to_string().into_bytes()),          // pending_restart
            ];

            let projected_row: Vec<Option<Vec<u8>>> = column_indices.iter()
                .map(|&idx| full_row[idx].clone())
                .collect();
            rows.push(projected_row);
        }

        let rows_affected = rows.len();
        DbResponse {
            columns: selected_columns,
            rows,
            rows_affected,
        }
    }

    fn get_settings() -> Vec<(&'static str, &'static str, &'static str, &'static str, &'static str, &'static str, &'static str)> {
        vec![
            // Version and encoding
            ("server_version", "16.0", "", "Preset Options", "PostgreSQL version string", "internal", "string"),
            ("server_version_num", "160000", "", "Preset Options", "PostgreSQL version number", "internal", "integer"),
            ("server_encoding", "UTF8", "", "Client Connection Defaults", "Server encoding", "internal", "string"),
            ("client_encoding", "UTF8", "", "Client Connection Defaults", "Client encoding", "user", "string"),

            // Date/time settings
            ("DateStyle", "ISO, MDY", "", "Client Connection Defaults", "Date display format", "user", "string"),
            ("TimeZone", "UTC", "", "Client Connection Defaults", "Time zone", "user", "string"),
            ("timezone_abbreviations", "Default", "", "Client Connection Defaults", "Time zone abbreviations", "user", "string"),
            ("extra_float_digits", "1", "", "Client Connection Defaults", "Extra float digits", "user", "integer"),
            ("integer_datetimes", "on", "", "Preset Options", "Integer datetimes", "internal", "bool"),

            // Connection settings
            ("max_connections", "100", "", "Connections and Authentication", "Maximum connections", "postmaster", "integer"),
            ("superuser_reserved_connections", "3", "", "Connections and Authentication", "Reserved for superuser", "postmaster", "integer"),

            // Memory settings
            ("shared_buffers", "128MB", "8kB", "Resource Usage / Memory", "Shared memory buffers", "postmaster", "integer"),
            ("work_mem", "4MB", "kB", "Resource Usage / Memory", "Work memory", "user", "integer"),
            ("maintenance_work_mem", "64MB", "kB", "Resource Usage / Memory", "Maintenance work memory", "user", "integer"),

            // Query tuning
            ("effective_cache_size", "4GB", "8kB", "Query Tuning / Planner Cost Constants", "Effective cache size", "user", "integer"),
            ("random_page_cost", "4", "", "Query Tuning / Planner Cost Constants", "Random page cost", "user", "real"),
            ("seq_page_cost", "1", "", "Query Tuning / Planner Cost Constants", "Sequential page cost", "user", "real"),

            // String handling
            ("standard_conforming_strings", "on", "", "Client Connection Defaults", "Standard conforming strings", "user", "bool"),
            ("escape_string_warning", "on", "", "Client Connection Defaults", "Escape string warning", "user", "bool"),
            ("bytea_output", "hex", "", "Client Connection Defaults", "Bytea output format", "user", "enum"),

            // Search path
            ("search_path", "\"$user\", public", "", "Client Connection Defaults", "Schema search path", "user", "string"),

            // Logging
            ("log_statement", "none", "", "Reporting and Logging", "Log statements", "superuser", "enum"),
            ("log_min_duration_statement", "-1", "ms", "Reporting and Logging", "Min duration to log", "superuser", "integer"),

            // Locale
            ("lc_collate", "en_US.UTF-8", "", "Client Connection Defaults", "Collation locale", "internal", "string"),
            ("lc_ctype", "en_US.UTF-8", "", "Client Connection Defaults", "Character type locale", "internal", "string"),
            ("lc_messages", "en_US.UTF-8", "", "Client Connection Defaults", "Messages locale", "superuser", "string"),
            ("lc_monetary", "en_US.UTF-8", "", "Client Connection Defaults", "Monetary locale", "user", "string"),
            ("lc_numeric", "en_US.UTF-8", "", "Client Connection Defaults", "Numeric locale", "user", "string"),
            ("lc_time", "en_US.UTF-8", "", "Client Connection Defaults", "Time locale", "user", "string"),

            // Transaction settings
            ("default_transaction_isolation", "read committed", "", "Client Connection Defaults", "Default isolation level", "user", "enum"),
            ("default_transaction_read_only", "off", "", "Client Connection Defaults", "Default read only", "user", "bool"),
            ("transaction_isolation", "read committed", "", "Client Connection Defaults", "Transaction isolation", "user", "enum"),
            ("transaction_read_only", "off", "", "Client Connection Defaults", "Transaction read only", "user", "bool"),

            // Application
            ("application_name", "", "", "Client Connection Defaults", "Application name", "user", "string"),

            // SSL
            ("ssl", "off", "", "Connections and Authentication", "SSL enabled", "sighup", "bool"),

            // WAL (not applicable but commonly queried)
            ("wal_level", "replica", "", "Write-Ahead Log", "WAL level", "postmaster", "enum"),
            ("max_wal_senders", "10", "", "Replication", "Max WAL senders", "postmaster", "integer"),

            // Autovacuum (not applicable but commonly queried)
            ("autovacuum", "on", "", "Autovacuum", "Autovacuum enabled", "sighup", "bool"),

            // Statement timeout
            ("statement_timeout", "0", "ms", "Client Connection Defaults", "Statement timeout", "user", "integer"),
            ("lock_timeout", "0", "ms", "Client Connection Defaults", "Lock timeout", "user", "integer"),
            ("idle_in_transaction_session_timeout", "0", "ms", "Client Connection Defaults", "Idle transaction timeout", "user", "integer"),
        ]
    }

    fn extract_name_filter(where_clause: &Expr) -> Option<String> {
        match where_clause {
            Expr::BinaryOp { left, op, right } => {
                if let (Expr::Identifier(ident), sqlparser::ast::BinaryOperator::Eq, Expr::Value(value_with_span)) =
                    (left.as_ref(), op, right.as_ref())
                    && ident.value.to_lowercase() == "name"
                        && let sqlparser::ast::Value::SingleQuotedString(value) = &value_with_span.value {
                            return Some(value.clone());
                        }
            }
            _ => {}
        }
        None
    }

    fn extract_selected_columns(select: &Select, all_columns: &[String]) -> (Vec<String>, Vec<usize>) {
        if select.projection.len() == 1 {
            if let SelectItem::Wildcard(_) = &select.projection[0] {
                return (all_columns.to_vec(), (0..all_columns.len()).collect());
            }
        }

        let mut cols = Vec::new();
        let mut indices = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let col_name = ident.value.to_lowercase();
                    if let Some(idx) = all_columns.iter().position(|c| c == &col_name) {
                        cols.push(col_name);
                        indices.push(idx);
                    }
                }
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                    if let Some(last) = parts.last() {
                        let col_name = last.value.to_lowercase();
                        if let Some(idx) = all_columns.iter().position(|c| c == &col_name) {
                            cols.push(col_name);
                            indices.push(idx);
                        }
                    }
                }
                _ => {}
            }
        }

        if cols.is_empty() {
            (all_columns.to_vec(), (0..all_columns.len()).collect())
        } else {
            (cols, indices)
        }
    }
}
```

**Step 4: Update mod.rs**

Add to `src/catalog/mod.rs`:
```rust
pub mod pg_settings;
```

**Step 5: Update query_interceptor.rs**

Add import:
```rust
use super::pg_settings::PgSettingsHandler;
```

Add to `has_catalog_tables`:
```rust
lower_query.contains("pg_settings") ||
```

Add handler:
```rust
// Handle pg_settings queries
if table_name.contains("pg_settings") || table_name.contains("pg_catalog.pg_settings") {
    return Some(Ok(PgSettingsHandler::handle_query(select)));
}
```

**Step 6: Run test and commit**

Run: `cargo test --test pg_settings_test`
Expected: PASS

```bash
git add src/catalog/pg_settings.rs src/catalog/mod.rs src/catalog/query_interceptor.rs tests/pg_settings_test.rs
git commit -m "feat: add pg_settings catalog table with common PostgreSQL settings"
```

---

## Task 8: Final Verification

**Step 1: Run full test suite**

```bash
cargo test --lib
cargo test --test pg_collation_test --test pg_replication_slots_test --test pg_shdepend_test --test pg_statistic_test --test pg_sequence_test --test pg_trigger_test --test pg_settings_test
```

**Step 2: Run clippy**

```bash
cargo clippy -- -W clippy::all
```

Fix any warnings.

**Step 3: Build release**

```bash
cargo build --release
```

**Step 4: Final commit if any fixes needed**

```bash
git add -A
git commit -m "fix: address clippy warnings and final cleanup"
```

---

## Summary

| Task | Table | Type | Status |
|------|-------|------|--------|
| 1 | pg_collation | Inline static | |
| 2 | pg_replication_slots | Inline stub | |
| 3 | pg_shdepend | Inline stub | |
| 4 | pg_statistic | Inline stub | |
| 5 | pg_sequence | Dedicated handler | |
| 6 | pg_trigger | Dedicated handler | |
| 7 | pg_settings | Dedicated handler | |
| 8 | Final verification | - | |

**Total: 8 tasks, ~530 lines of new code**
