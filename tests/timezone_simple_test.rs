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
    
    // Create a table with timestamp stored as DOUBLE PRECISION (common pattern in pgsqlite)
    client.execute(
        "CREATE TABLE ts_test (id INTEGER PRIMARY KEY, ts DOUBLE PRECISION)",
        &[]
    ).await.unwrap();
    
    // Insert a test timestamp (seconds since epoch)
    let timestamp = 1686839445.0f64; // 2023-06-15 14:30:45 UTC
    client.execute(
        "INSERT INTO ts_test (id, ts) VALUES ($1, $2)",
        &[&1i32, &timestamp]
    ).await.unwrap();
    
    // Test AT TIME ZONE with prepared statement
    // Note: simple_query returns text format which has issues with float values,
    // so we use prepared statements instead
    let rows = client.query(
        "SELECT ts AT TIME ZONE 'UTC' as ts_utc FROM ts_test WHERE id = $1",
        &[&1i32]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 1, "Expected exactly one row");
    let ts_utc: f64 = rows[0].get(0);
    
    // When timezone is UTC, the value should be unchanged
    assert!((ts_utc - timestamp).abs() < 1.0, 
            "UTC time should match original timestamp, got {ts_utc} expected {timestamp}");
}