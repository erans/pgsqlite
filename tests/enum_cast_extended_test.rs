mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_enum_cast_extended_protocol() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create an ENUM type
    client.simple_query("CREATE TYPE status AS ENUM ('active', 'inactive', 'pending')")
        .await
        .expect("Failed to create ENUM type");
    
    // Create a table with ENUM column
    client.simple_query("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, status status)")
        .await
        .expect("Failed to create table");
    
    // Test parameterized cast in prepared statement
    let stmt = client.prepare("INSERT INTO items (id, name, status) VALUES ($1, $2, $3::status)")
        .await
        .expect("Failed to prepare statement");
    
    client.execute(&stmt, &[&1i32, &"Item 1", &"active"])
        .await
        .expect("Failed to insert with parameterized cast");
    
    // Test parameterized SELECT with cast
    let stmt = client.prepare("SELECT $1::status as casted")
        .await
        .expect("Failed to prepare SELECT");
    
    let row = client.query_one(&stmt, &[&"pending"])
        .await
        .expect("Failed to execute parameterized SELECT");
    
    assert_eq!(row.get::<_, &str>(0), "pending");
    
    // Test invalid cast with parameters
    let stmt = client.prepare("INSERT INTO items (id, name, status) VALUES ($1, $2, $3::status)")
        .await
        .expect("Failed to prepare statement");
    
    let result = client.execute(&stmt, &[&2i32, &"Item 2", &"invalid"])
        .await;
    
    assert!(result.is_err(), "Should fail to insert invalid enum value");
    
    server.abort();
}

#[tokio::test]
async fn test_cast_in_cte() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create an ENUM type
    client.simple_query("CREATE TYPE priority AS ENUM ('low', 'medium', 'high')")
        .await
        .expect("Failed to create ENUM type");
    
    // Test cast in CTE
    let results = client.simple_query("
        WITH priorities AS (
            SELECT 'high'::priority as p
            UNION ALL
            SELECT 'low'::priority
        )
        SELECT * FROM priorities
    ")
        .await
        .expect("Failed to execute CTE with casts");
    
    let mut count = 0;
    for result in &results {
        if let tokio_postgres::SimpleQueryMessage::Row(_) = result {
            count += 1;
        }
    }
    
    assert_eq!(count, 2, "Should return 2 rows from CTE");
    
    server.abort();
}