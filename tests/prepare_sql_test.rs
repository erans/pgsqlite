mod common;

use crate::common::setup_test_server;

#[tokio::test]
async fn test_sql_prepare_execute_deallocate_via_simple_query() {
    let server = setup_test_server().await;
    let client = &server.client;

    client
        .simple_query("PREPARE p1 (int4) AS SELECT $1::int4 + 1")
        .await
        .unwrap();

    let msgs = client.simple_query("EXECUTE p1(41)").await.unwrap();
    let mut saw_row = false;
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            assert_eq!(row.get(0), Some("42"));
            saw_row = true;
        }
    }
    assert!(saw_row);

    client.simple_query("DEALLOCATE p1").await.unwrap();

    let err = client.simple_query("EXECUTE p1(1)").await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("does not exist") || msg.contains("not exist"));
}

#[tokio::test]
async fn test_sql_prepare_execute_without_typed_list() {
    let server = setup_test_server().await;
    let client = &server.client;

    client
        .simple_query("PREPARE p2 AS SELECT $1")
        .await
        .unwrap();

    let msgs = client.simple_query("EXECUTE p2('hello')").await.unwrap();
    let mut saw_row = false;
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            assert_eq!(row.get(0), Some("hello"));
            saw_row = true;
        }
    }
    assert!(saw_row);
}

#[tokio::test]
async fn test_sql_deallocate_all() {
    let server = setup_test_server().await;
    let client = &server.client;

    client.simple_query("PREPARE p3 AS SELECT 1").await.unwrap();
    client.simple_query("PREPARE p4 AS SELECT 2").await.unwrap();
    client.simple_query("DEALLOCATE ALL").await.unwrap();

    assert!(client.simple_query("EXECUTE p3").await.is_err());
    assert!(client.simple_query("EXECUTE p4").await.is_err());
}
