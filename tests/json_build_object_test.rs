use tokio::net::TcpListener;
use tokio_postgres::{Client, NoTls};

async fn start_test_server() -> (Client, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(pgsqlite::session::DbHandler::new(":memory:").unwrap());
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler)
            .await
            .unwrap();
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=localhost port={port} dbname=test user=testuser"),
        NoTls,
    )
    .await
    .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });

    (client, server_handle)
}

/// Run a query over the extended protocol and decode the JSON-typed result.
async fn json_scalar(client: &Client, sql: &str) -> serde_json::Value {
    let rows = client.query(sql, &[]).await.unwrap();
    rows[0].get(0)
}

/// Run a query over the simple-query protocol and return the raw text value.
async fn raw_scalar(client: &Client, sql: &str) -> String {
    let messages = client.simple_query(sql).await.unwrap();
    messages
        .iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
            _ => None,
        })
        .expect("expected a row")
}

#[tokio::test]
async fn test_json_build_object_text_values() {
    let (client, server_handle) = start_test_server().await;

    // Issue #110: text must serialise as a JSON string, not an array of byte values
    assert_eq!(
        json_scalar(&client, "SELECT json_build_object('d', 'abc')").await,
        serde_json::json!({"d": "abc"})
    );
    assert_eq!(
        json_scalar(&client, "SELECT json_build_object('d', '')").await,
        serde_json::json!({"d": ""})
    );
    assert_eq!(
        json_scalar(&client, "SELECT json_build_object('d', 'a\"b')").await,
        serde_json::json!({"d": "a\"b"})
    );

    // Non-text scalars still round-trip
    assert_eq!(
        json_scalar(&client, "SELECT json_build_object('id', 1)").await,
        serde_json::json!({"id": 1})
    );

    server_handle.abort();
}

#[tokio::test]
async fn test_json_build_object_key_escaping() {
    let (client, server_handle) = start_test_server().await;

    // Issue #110 secondary: keys containing quotes or backslashes must be escaped
    assert_eq!(
        json_scalar(&client, "SELECT json_build_object('a\"b', 1)").await,
        serde_json::json!({"a\"b": 1})
    );
    assert_eq!(
        json_scalar(&client, "SELECT json_build_object('a\\b', 1)").await,
        serde_json::json!({"a\\b": 1})
    );

    server_handle.abort();
}

#[tokio::test]
async fn test_json_build_object_variadic() {
    let (client, server_handle) = start_test_server().await;

    assert_eq!(
        json_scalar(&client, "SELECT json_build_object('id', 1, 'name', 'x')").await,
        serde_json::json!({"id": 1, "name": "x"})
    );

    // Duplicate keys are preserved
    assert_eq!(
        raw_scalar(&client, "SELECT json_build_object('k', 1, 'k', 2)").await,
        "{\"k\": 1, \"k\": 2}"
    );

    // Zero arguments yields an empty object
    assert_eq!(
        json_scalar(&client, "SELECT json_build_object()").await,
        serde_json::json!({})
    );

    // jsonb_build_object is an alias
    assert_eq!(
        json_scalar(&client, "SELECT jsonb_build_object('d', 'abc')").await,
        serde_json::json!({"d": "abc"})
    );

    // Odd argument count is rejected
    assert!(
        client
            .query("SELECT json_build_object('k')", &[])
            .await
            .is_err()
    );

    server_handle.abort();
}

#[tokio::test]
async fn test_json_build_object_simple_query_protocol() {
    let (client, server_handle) = start_test_server().await;

    let raw = raw_scalar(&client, "SELECT json_build_object('id', 1, 'name', 'x')").await;
    let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(parsed, serde_json::json!({"id": 1, "name": "x"}));

    server_handle.abort();
}
