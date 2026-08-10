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
