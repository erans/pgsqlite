mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_simple_string_param() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create simple table
    client.execute("CREATE TABLE test_params (val TEXT)", &[]).await.unwrap();
    
    // Insert with parameter
    client.execute("INSERT INTO test_params (val) VALUES ($1)", &[&"hello"]).await.unwrap();
    
    // Select the value back without parameter
    let row = client.query_one("SELECT val FROM test_params", &[]).await.unwrap();
    let value: String = row.get(0);
    assert_eq!(value, "hello");
    
    // Now test SELECT with simple parameter (no type cast)
    println!("Testing SELECT $1 with parameter 'world'");
    let row = client.query_one("SELECT $1", &[&"world"]).await.unwrap();
    let value: String = row.get(0);
    println!("Got value: '{}'", value);
    assert_eq!(value, "world", "Expected 'world' but got '{}'", value);
    
    server.abort();
}

#[tokio::test]
async fn test_simple_int_param() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test SELECT with simple integer parameter with explicit cast
    // Without cast, parameters default to TEXT type (see Known Issues in CLAUDE.md)
    let row = client.query_one("SELECT $1::int4", &[&42i32]).await.unwrap();
    let value: i32 = row.get(0);
    assert_eq!(value, 42, "Expected 42 but got {}", value);
    
    server.abort();
}

#[tokio::test]
#[should_panic(expected = "WrongType")]
async fn test_parameter_type_limitation() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // This test demonstrates the known limitation:
    // SELECT $1 without cast defaults to TEXT type
    // and fails when trying to retrieve as integer
    let row = client.query_one("SELECT $1", &[&42i32]).await.unwrap();
    let _value: i32 = row.get(0); // This will panic with WrongType
    
    server.abort();
}