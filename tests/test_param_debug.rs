mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_param_with_type_cast() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // This should work - explicit type cast
    let row = client
        .query_one("SELECT $1::text", &[&"hello"])
        .await
        .unwrap();
    
    let value: String = row.get(0);
    println!("With type cast: '{}'", value);
    assert_eq!(value, "hello");
    
    server.abort();
}

#[tokio::test]
async fn test_param_without_type_cast() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // This is failing - no explicit type cast
    let row = client
        .query_one("SELECT $1", &[&"world"])
        .await
        .unwrap();
    
    let value: String = row.get(0);
    println!("Without type cast: '{}'", value);
    assert_eq!(value, "world");
    
    server.abort();
}

#[tokio::test]
async fn test_int_param_with_cast() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test with integer and cast
    let row = client
        .query_one("SELECT $1::int4", &[&42i32])
        .await
        .unwrap();
    
    let value: i32 = row.get(0);
    println!("Int with cast: {}", value);
    assert_eq!(value, 42);
    
    server.abort();
}