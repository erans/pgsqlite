mod common;
use common::*;

#[tokio::test]
async fn test_batch_insert_column_count_error() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table
    client.execute(
        "CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name TEXT,
            value INTEGER
        )",
        &[]
    ).await.unwrap();
    
    // Test column count mismatch in batch INSERT
    let result = client.simple_query(
        "INSERT INTO test_table (id, name, value) VALUES 
            (1, 'test1', 100),
            (2, 'test2'),  -- Missing value column
            (3, 'test3', 300)"
    ).await;
    
    assert!(result.is_err(), "Should fail with column count mismatch");
    let err = result.unwrap_err();
    // SQLite catches this error before our translator, so we get SQLite's error message
    assert!(err.to_string().contains("all VALUES must have the same number of terms"), 
        "Should get SQLite's column count error: {err}");
    
    server.abort();
}

#[tokio::test]
async fn test_batch_insert_date_format_error() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with date column
    client.execute(
        "CREATE TABLE date_test (
            id INTEGER PRIMARY KEY,
            event_date DATE
        )",
        &[]
    ).await.unwrap();
    
    // Test invalid date format in batch INSERT
    let result = client.simple_query(
        "INSERT INTO date_test (id, event_date) VALUES 
            (1, '2025-01-01'),
            (2, '01/15/2025'),  -- Wrong format
            (3, '2025-01-03')"
    ).await;
    
    assert!(result.is_err(), "Should fail with invalid date format");
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Invalid date value '01/15/2025'"), 
        "Error should show invalid date: {err}");
    assert!(err.to_string().contains("Expected format: YYYY-MM-DD"), 
        "Error should show expected format: {err}");
    
    server.abort();
}

#[tokio::test]
async fn test_batch_insert_time_format_error() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with time column
    client.execute(
        "CREATE TABLE time_test (
            id INTEGER PRIMARY KEY,
            event_time TIME
        )",
        &[]
    ).await.unwrap();
    
    // Test invalid time format in batch INSERT
    let result = client.simple_query(
        "INSERT INTO time_test (id, event_time) VALUES 
            (1, '14:30:00'),
            (2, '2:30 PM'),  -- Wrong format
            (3, '16:45:00')"
    ).await;
    
    assert!(result.is_err(), "Should fail with invalid time format");
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Invalid time value '2:30 PM'"), 
        "Error should show invalid time: {err}");
    assert!(err.to_string().contains("Expected format: HH:MM:SS"), 
        "Error should show expected format: {err}");
    
    server.abort();
}

#[tokio::test]
async fn test_batch_insert_timestamp_format_error() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with timestamp column
    client.execute(
        "CREATE TABLE timestamp_test (
            id INTEGER PRIMARY KEY,
            event_timestamp TIMESTAMP
        )",
        &[]
    ).await.unwrap();
    
    // Test invalid timestamp format in batch INSERT
    let result = client.simple_query(
        "INSERT INTO timestamp_test (id, event_timestamp) VALUES 
            (1, '2025-01-01 14:30:00'),
            (2, '2025-01-02T15:45:00Z'),  -- ISO format not supported
            (3, '2025-01-03 16:00:00')"
    ).await;
    
    assert!(result.is_err(), "Should fail with invalid timestamp format");
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Invalid timestamp value '2025-01-02T15:45:00Z'"), 
        "Error should show invalid timestamp: {err}");
    assert!(err.to_string().contains("Expected format: YYYY-MM-DD HH:MM:SS"), 
        "Error should show expected format: {err}");
    
    server.abort();
}

#[tokio::test]
async fn test_batch_insert_partial_failure_rollback() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with unique constraint
    client.execute(
        "CREATE TABLE unique_test (
            id INTEGER PRIMARY KEY,
            email TEXT UNIQUE
        )",
        &[]
    ).await.unwrap();
    
    // First insert
    client.execute(
        "INSERT INTO unique_test (id, email) VALUES (1, 'existing@example.com')",
        &[]
    ).await.unwrap();
    
    // Try batch INSERT with duplicate
    let result = client.simple_query(
        "INSERT INTO unique_test (id, email) VALUES 
            (2, 'new1@example.com'),
            (3, 'existing@example.com'),  -- Duplicate
            (4, 'new2@example.com')"
    ).await;
    
    assert!(result.is_err(), "Should fail with unique constraint");
    
    // Verify no rows were inserted from the failed batch
    let count = client.query_one("SELECT COUNT(*) FROM unique_test", &[]).await.unwrap();
    let count_val: i64 = count.get(0);
    assert_eq!(count_val, 1, "Failed batch should not insert any rows");
    
    server.abort();
}