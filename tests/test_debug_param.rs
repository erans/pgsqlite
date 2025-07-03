mod common;
use common::setup_test_server;
use std::io::Write;

#[tokio::test]
async fn debug_simple_param() {
    // Set up logging
    unsafe {
        std::env::set_var("RUST_LOG", "pgsqlite=debug");
    }
    let _ = env_logger::try_init();
    
    let server = setup_test_server().await;
    let client = &server.client;
    
    println!("\n=== Starting debug_simple_param test ===");
    
    // Test with type cast first (this should work)
    println!("\n1. Testing SELECT $1::text with 'hello'");
    let row = client.query_one("SELECT $1::text", &[&"hello"]).await.unwrap();
    let value: String = row.get(0);
    println!("   Result: '{}'", value);
    assert_eq!(value, "hello");
    
    // Test without type cast (this is failing)
    println!("\n2. Testing SELECT $1 with 'world'");
    std::io::stdout().flush().unwrap();
    
    match client.query_one("SELECT $1", &[&"world"]).await {
        Ok(row) => {
            let value: String = row.get(0);
            println!("   Result: '{}'", value);
            assert_eq!(value, "world");
        }
        Err(e) => {
            println!("   Error: {:?}", e);
            panic!("Query failed: {:?}", e);
        }
    }
    
    println!("\n=== Test completed ===");
    server.abort();
}