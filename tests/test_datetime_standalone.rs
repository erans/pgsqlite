mod common;
use common::setup_test_server;
use chrono::{DateTime, Utc, Timelike};

#[tokio::test]
async fn test_standalone_now_returns_formatted_timestamp() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test SELECT NOW() - should return a proper timestamp
    let row = client.query_one("SELECT NOW()", &[]).await.unwrap();
    let now_value: DateTime<Utc> = row.get(0);
    let now_value = now_value.naive_utc();
    
    // Should be a recent timestamp (within the last hour)
    let now = chrono::Utc::now().naive_utc();
    let diff = (now - now_value).num_seconds().abs();
    assert!(diff < 3600, "NOW() should return a recent timestamp, but got {now_value} (diff: {diff} seconds)");
    
    // Verify it has microsecond precision
    assert!(now_value.nanosecond() > 0, "NOW() should have sub-second precision");
    
    // Also verify that when cast to text, it's properly formatted
    let row_text = client.query_one("SELECT CAST(NOW() AS TEXT)", &[]).await.unwrap();
    let now_text: String = row_text.get(0);
    assert!(now_text.contains('-') && now_text.contains(':'), 
           "NOW() cast to text should be formatted as timestamp, got: {now_text}");
}

#[tokio::test]
async fn test_standalone_current_timestamp_returns_formatted() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test SELECT CURRENT_TIMESTAMP() - should return a proper timestamp
    let row = client.query_one("SELECT CURRENT_TIMESTAMP()", &[]).await.unwrap();
    let ts_value: DateTime<Utc> = row.get(0);
    let ts_value = ts_value.naive_utc();
    
    // Should be a recent timestamp (within the last hour)
    let now = chrono::Utc::now().naive_utc();
    let diff = (now - ts_value).num_seconds().abs();
    assert!(diff < 3600, "CURRENT_TIMESTAMP() should return a recent timestamp, but got {ts_value} (diff: {diff} seconds)");
    
    // Verify it has microsecond precision
    assert!(ts_value.nanosecond() > 0, "CURRENT_TIMESTAMP() should have sub-second precision");
}

#[tokio::test]
async fn test_datetime_functions_with_table_context() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create a table with timestamp column
    client.execute("CREATE TABLE test_ts (id INTEGER, ts TIMESTAMPTZ)", &[]).await.unwrap();
    
    // Insert using NOW()
    client.execute("INSERT INTO test_ts VALUES (1, NOW())", &[]).await.unwrap();
    
    // Select should return a proper timestamp (TIMESTAMPTZ type)
    let row = client.query_one("SELECT ts FROM test_ts WHERE id = 1", &[]).await.unwrap();
    let ts_value: DateTime<Utc> = row.get(0);
    
    // Should be a recent timestamp (within the last hour)
    let now = Utc::now();
    let diff = (now - ts_value).num_seconds().abs();
    assert!(diff < 3600, "Timestamp from table should be recent, but got {ts_value} (diff: {diff} seconds)");
}