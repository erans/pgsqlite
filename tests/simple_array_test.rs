mod common;
use common::*;

#[tokio::test]
async fn test_simple_array() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE test (id INTEGER, tags TEXT)").await?;
            db.execute(r#"INSERT INTO test VALUES (1, '["a", "b"]')"#).await?;
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    // First test without array operations
    let row = client.query_one("SELECT tags FROM test WHERE id = 1", &[]).await.unwrap();
    let tags: String = row.get(0);
    println!("Tags without operation: {}", tags);
    
    // Now test with array concatenation
    let row = client.query_one(
        "SELECT tags || '[\"c\"]' AS combined FROM test WHERE id = 1",
        &[]
    ).await.unwrap();
    
    let combined: String = row.get(0);
    println!("Combined: {}", combined);
    
    server.abort();
}