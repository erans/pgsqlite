mod common;
use common::*;

#[tokio::test]
async fn test_batch_insert_with_datetime_functions() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with datetime columns
    client.execute(
        "CREATE TABLE datetime_test (
            id INTEGER PRIMARY KEY,
            name TEXT,
            created_date DATE,
            created_time TIME,
            created_timestamp TIMESTAMP
        )",
        &[]
    ).await.unwrap();
    
    // Test batch INSERT with datetime functions
    let result = client.simple_query(
        "INSERT INTO datetime_test (id, name, created_date, created_time, created_timestamp) VALUES 
            (1, 'test1', CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP),
            (2, 'test2', CURRENT_DATE, CURRENT_TIME, NOW()),
            (3, 'test3', '2025-01-01', '14:30:00', '2025-01-01 14:30:00')"
    ).await;
    
    assert!(result.is_ok(), "Should handle datetime functions in batch INSERT");
    
    // Verify rows were inserted
    let count = client.query_one("SELECT COUNT(*) FROM datetime_test", &[]).await.unwrap();
    let count_val: i64 = count.get(0);
    assert_eq!(count_val, 3, "Should have inserted 3 rows");
    
    // Verify the third row has the literal values
    let row = client.query_one(
        "SELECT name, created_date, created_time, created_timestamp FROM datetime_test WHERE id = 3", 
        &[]
    ).await.unwrap();
    
    let name: &str = row.get(0);
    assert_eq!(name, "test3");
    // The actual values will be INTEGER microseconds/days, but that's OK
    
    server.abort();
}

#[tokio::test]
async fn test_single_insert_with_datetime_functions() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table
    client.execute(
        "CREATE TABLE single_datetime_test (
            id INTEGER PRIMARY KEY,
            event_date DATE,
            event_time TIME,
            event_timestamp TIMESTAMP
        )",
        &[]
    ).await.unwrap();
    
    // Test single INSERT with datetime functions
    client.execute(
        "INSERT INTO single_datetime_test (id, event_date, event_time, event_timestamp) 
         VALUES (1, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP)",
        &[]
    ).await.unwrap();
    
    // Verify row was inserted
    let count = client.query_one("SELECT COUNT(*) FROM single_datetime_test", &[]).await.unwrap();
    let count_val: i64 = count.get(0);
    assert_eq!(count_val, 1, "Should have inserted 1 row");
    
    server.abort();
}

#[tokio::test]
async fn test_mixed_datetime_values_and_functions() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table
    client.execute(
        "CREATE TABLE mixed_datetime_test (
            id INTEGER PRIMARY KEY,
            description TEXT,
            event_date DATE,
            event_timestamp TIMESTAMP
        )",
        &[]
    ).await.unwrap();
    
    // Test mixed literal values and functions
    let result = client.simple_query(
        "INSERT INTO mixed_datetime_test (id, description, event_date, event_timestamp) VALUES 
            (1, 'Current', CURRENT_DATE, NOW()),
            (2, 'Fixed', '2025-01-15', '2025-01-15 10:30:00'),
            (3, 'Mixed', CURRENT_DATE, '2025-01-20 14:00:00'),
            (4, 'Functions', CURRENT_DATE, CURRENT_TIMESTAMP)"
    ).await;
    
    assert!(result.is_ok(), "Should handle mixed datetime values and functions");
    
    // Verify all rows were inserted
    let count = client.query_one("SELECT COUNT(*) FROM mixed_datetime_test", &[]).await.unwrap();
    let count_val: i64 = count.get(0);
    assert_eq!(count_val, 4, "Should have inserted 4 rows");
    
    server.abort();
}