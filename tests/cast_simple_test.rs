mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_simple_cast() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test without enum first
    let results = client.simple_query("SELECT CAST('hello' AS text) as val")
        .await
        .expect("Failed to cast to text");
    
    for result in &results {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = result {
            println!("CAST to text: {:?}", row.get("val"));
            assert_eq!(row.get("val"), Some("hello"));
        }
    }
    
    server.abort();
}