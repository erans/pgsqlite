use tokio_postgres::{Client, NoTls};
use tokio::net::TcpListener;
use std::time::Duration;
use tokio::time::timeout;

async fn setup_test_db() -> Result<(u16, Client), Box<dyn std::error::Error + Send + Sync>> {
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    
    let server_handle = tokio::spawn(async move {
        // Create database handler
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        // Accept connection
        let (stream, addr) = listener.accept().await.unwrap();
        
        // Handle connection
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let (client, connection) = timeout(
        Duration::from_secs(5),
        tokio_postgres::connect(
            &format!("host=127.0.0.1 port={port} user=test dbname=test"),
            NoTls,
        ),
    ).await??;
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });
    
    // Spawn the server handle to prevent it from being dropped
    tokio::spawn(server_handle);
    
    Ok((port, client))
}

#[tokio::test]
async fn test_row_to_json_basic_subquery() {
    let (_port, client) = timeout(Duration::from_secs(10), setup_test_db()).await
        .expect("Timeout setting up test DB")
        .expect("Failed to set up test DB");
    
    // Create a test table
    client.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
        &[]
    ).await.expect("Failed to create table");
    
    // Insert test data
    client.execute(
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25)",
        &[]
    ).await.expect("Failed to insert data");
    
    // Test row_to_json with subquery
    let rows = client.query(
        "SELECT row_to_json(t) FROM (SELECT name, age FROM users WHERE id = 1) t",
        &[]
    ).await.expect("Failed to execute query");
    
    assert_eq!(rows.len(), 1);
    let json_result: String = rows[0].get(0);
    
    // The result should be a JSON object with name and age
    assert!(json_result.contains("\"name\":\"Alice\"") || json_result.contains("\"name\": \"Alice\""));
    assert!(json_result.contains("\"age\":30") || json_result.contains("\"age\": 30"));
    
    println!("Row to JSON result: {json_result}");
}

#[tokio::test]
async fn test_row_to_json_multiple_columns() {
    let (_port, client) = timeout(Duration::from_secs(10), setup_test_db()).await
        .expect("Timeout setting up test DB")
        .expect("Failed to set up test DB");
    
    // Create a test table with more columns
    client.execute(
        "CREATE TABLE products (id INTEGER, name TEXT, price DECIMAL(10,2), in_stock BOOLEAN)",
        &[]
    ).await.expect("Failed to create table");
    
    // Insert test data
    client.execute(
        "INSERT INTO products (id, name, price, in_stock) VALUES (1, 'Widget', 19.99, true)",
        &[]
    ).await.expect("Failed to insert data");
    
    // Test row_to_json with multiple column types
    let rows = client.query(
        "SELECT row_to_json(p) FROM (SELECT id, name, price, in_stock FROM products WHERE id = 1) p",
        &[]
    ).await.expect("Failed to execute query");
    
    assert_eq!(rows.len(), 1);
    let json_result: String = rows[0].get(0);
    
    println!("Multi-column row to JSON result: {json_result}");
    
    // Verify all columns are present
    assert!(json_result.contains("\"id\":1") || json_result.contains("\"id\": 1"), "Missing id field in: {json_result}");
    assert!(json_result.contains("\"name\":\"Widget\"") || json_result.contains("\"name\": \"Widget\""), "Missing name field in: {json_result}");
    assert!(json_result.contains("\"price\":19.99") || json_result.contains("\"price\": 19.99"), "Missing price field in: {json_result}");
    assert!(json_result.contains("\"in_stock\":1") || json_result.contains("\"in_stock\": 1") ||
            json_result.contains("\"in_stock\":true") || json_result.contains("\"in_stock\": true"), 
            "Missing in_stock field in: {json_result}");
}

#[tokio::test]
async fn test_row_to_json_with_aliases() {
    let (_port, client) = timeout(Duration::from_secs(10), setup_test_db()).await
        .expect("Timeout setting up test DB")
        .expect("Failed to set up test DB");
    
    // Create a test table
    client.execute(
        "CREATE TABLE employees (id INTEGER, first_name TEXT, last_name TEXT, salary INTEGER)",
        &[]
    ).await.expect("Failed to create table");
    
    // Insert test data
    client.execute(
        "INSERT INTO employees (id, first_name, last_name, salary) VALUES (1, 'John', 'Doe', 50000)",
        &[]
    ).await.expect("Failed to insert data");
    
    // Test row_to_json with column aliases
    let rows = client.query(
        "SELECT row_to_json(e) FROM (SELECT first_name AS fname, last_name AS lname, salary FROM employees WHERE id = 1) e",
        &[]
    ).await.expect("Failed to execute query");
    
    assert_eq!(rows.len(), 1);
    let json_result: String = rows[0].get(0);
    
    // Verify aliases are used in the JSON
    assert!(json_result.contains("\"fname\":\"John\"") || json_result.contains("\"fname\": \"John\""));
    assert!(json_result.contains("\"lname\":\"Doe\"") || json_result.contains("\"lname\": \"Doe\""));
    assert!(json_result.contains("\"salary\":50000") || json_result.contains("\"salary\": 50000"));
    
    println!("Aliased row to JSON result: {json_result}");
}

#[tokio::test]
async fn test_row_to_json_simple_function_call() {
    let (_port, client) = timeout(Duration::from_secs(10), setup_test_db()).await
        .expect("Timeout setting up test DB")
        .expect("Failed to set up test DB");
    
    // Test simple row_to_json function call (without subquery pattern)
    // This should use the SQLite function implementation
    let rows = client.query(
        "SELECT row_to_json('test value')",
        &[]
    ).await.expect("Failed to execute query");
    
    assert_eq!(rows.len(), 1);
    let json_result: String = rows[0].get(0);
    
    // For simple values, the SQLite function should handle it
    println!("Simple row to JSON result: {json_result}");
    
    // The result should be a JSON representation of the input
    assert!(json_result.contains("test value") || json_result == "\"test value\"");
}

#[tokio::test]
async fn test_row_to_json_multiple_rows() {
    let (_port, client) = timeout(Duration::from_secs(10), setup_test_db()).await
        .expect("Timeout setting up test DB")
        .expect("Failed to set up test DB");
    
    // Create a test table
    client.execute(
        "CREATE TABLE items (id INTEGER, name TEXT, category TEXT)",
        &[]
    ).await.expect("Failed to create table");
    
    // Insert test data
    client.execute(
        "INSERT INTO items (id, name, category) VALUES (1, 'Apple', 'Fruit'), (2, 'Carrot', 'Vegetable')",
        &[]
    ).await.expect("Failed to insert data");
    
    // Test row_to_json returning multiple rows
    let rows = client.query(
        "SELECT row_to_json(i) FROM (SELECT name, category FROM items ORDER BY id) i",
        &[]
    ).await.expect("Failed to execute query");
    
    assert_eq!(rows.len(), 2);
    
    let json_result1: String = rows[0].get(0);
    let json_result2: String = rows[1].get(0);
    
    // Verify both rows are correctly converted
    assert!(json_result1.contains("\"name\":\"Apple\"") || json_result1.contains("\"name\": \"Apple\""));
    assert!(json_result1.contains("\"category\":\"Fruit\"") || json_result1.contains("\"category\": \"Fruit\""));
    
    assert!(json_result2.contains("\"name\":\"Carrot\"") || json_result2.contains("\"name\": \"Carrot\""));
    assert!(json_result2.contains("\"category\":\"Vegetable\"") || json_result2.contains("\"category\": \"Vegetable\""));
    
    println!("Multiple rows - Row 1: {json_result1}");
    println!("Multiple rows - Row 2: {json_result2}");
}