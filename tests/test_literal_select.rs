mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_literal_string_select() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test literal string select
    let row = client.query_one("SELECT 'hello'", &[]).await.unwrap();
    let value: String = row.get(0);
    println!("Literal SELECT 'hello' returned: '{}'", value);
    assert_eq!(value, "hello");
    
    server.abort();
}

#[tokio::test]
async fn test_literal_int_select() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test literal int select
    let row = client.query_one("SELECT 42", &[]).await.unwrap();
    let value: i32 = row.get(0);
    println!("Literal SELECT 42 returned: {}", value);
    assert_eq!(value, 42);
    
    server.abort();
}