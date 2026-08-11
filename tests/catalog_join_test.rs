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
