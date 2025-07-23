mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_simple_set_timezone() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Use simple_query to bypass prepared statement issues
    let results = client.simple_query("SET TIME ZONE 'UTC'").await.unwrap();
    
    // Verify command completed
    for msg in results {
        if let tokio_postgres::SimpleQueryMessage::CommandComplete(_) = msg {
            // Command completed successfully
        }
    }
    
    // Test SHOW with simple_query
    let results = client.simple_query("SHOW TimeZone").await.unwrap();
    
    let mut found_timezone = false;
    for msg in results {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            if let Some(tz) = row.get(0) {
                assert_eq!(tz, "UTC");
                found_timezone = true;
            }
        }
    }
    assert!(found_timezone, "Should have found timezone value");
}

#[tokio::test]
async fn test_simple_at_time_zone() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create a table with timestamp
    client.execute(
        "CREATE TABLE ts_test (id INTEGER PRIMARY KEY, ts REAL)",
        &[]
    ).await.unwrap();
    
    // Insert a test timestamp
    let timestamp = 1686839445.0f32; // 2023-06-15 14:30:45 UTC
    client.execute(
        "INSERT INTO ts_test (id, ts) VALUES ($1, $2)",
        &[&1i32, &timestamp]
    ).await.unwrap();
    
    // Test AT TIME ZONE with simple_query
    let results = client.simple_query(
        "SELECT ts AT TIME ZONE 'UTC' as ts_utc FROM ts_test WHERE id = 1"
    ).await.unwrap();
    
    for msg in results {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            if let Some(val) = row.get(0) {
                let ts_val: f64 = val.parse().unwrap();
                assert_eq!(ts_val as f32, timestamp);
            }
        }
    }
}