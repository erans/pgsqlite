mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_enum_simple_query() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create an ENUM type using simple query
    let results = client.simple_query("CREATE TYPE status AS ENUM ('active', 'inactive', 'pending')")
        .await
        .expect("Failed to create ENUM type");
    
    assert_eq!(results.len(), 1);
    match &results[0] {
        tokio_postgres::SimpleQueryMessage::CommandComplete(n) => {
            assert_eq!(*n, 0); // DDL commands typically report 0 rows
        }
        _ => panic!("Expected CommandComplete"),
    }
    
    // Create a table with an ENUM column
    client.simple_query("CREATE TABLE tasks (id INTEGER PRIMARY KEY, name TEXT, status status)")
        .await
        .expect("Failed to create table");
    
    // Insert some data using simple query
    client.simple_query("INSERT INTO tasks (id, name, status) VALUES (1, 'Task 1', 'active')")
        .await
        .expect("Failed to insert row 1");
    
    // Query the data using simple query
    let results = client.simple_query("SELECT id, name, status FROM tasks ORDER BY id")
        .await
        .expect("Failed to query tasks");
    
    // Check we got the row back
    eprintln!("Got {} results", results.len());
    for (i, result) in results.iter().enumerate() {
        eprintln!("Result {}: {:?}", i, result);
    }
    
    // Skip RowDescription at index 0, get Row at index 1
    match &results[1] {
        tokio_postgres::SimpleQueryMessage::Row(row) => {
            assert_eq!(row.get("id"), Some("1"));
            assert_eq!(row.get("name"), Some("Task 1"));
            assert_eq!(row.get("status"), Some("active"));
        }
        _ => panic!("Expected Row at index 1"),
    }
    
    server.abort();
}

#[tokio::test]
async fn test_enum_check_constraint() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create an ENUM type
    client.simple_query("CREATE TYPE priority AS ENUM ('low', 'medium', 'high')")
        .await
        .expect("Failed to create ENUM type");
    
    // Create a table with an ENUM column
    client.simple_query("CREATE TABLE items (id INTEGER PRIMARY KEY, priority priority)")
        .await
        .expect("Failed to create table");
    
    // Test that invalid ENUM values are rejected
    let result = client.simple_query("INSERT INTO items (id, priority) VALUES (1, 'invalid')")
        .await;
    
    assert!(result.is_err(), "Should fail to insert invalid ENUM value");
    let err = result.unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("invalid input value for enum") || err_str.contains("CHECK constraint"),
        "Error should mention invalid enum value or CHECK constraint. Got: {}",
        err_str
    );
    
    // Test that valid values work
    client.simple_query("INSERT INTO items (id, priority) VALUES (1, 'high')")
        .await
        .expect("Should succeed with valid ENUM value");
    
    server.abort();
}