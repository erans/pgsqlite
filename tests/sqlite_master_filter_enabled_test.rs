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
}

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
}
