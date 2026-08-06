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
