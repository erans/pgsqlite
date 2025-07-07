mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_set_timezone() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test SET TIME ZONE
    client.execute("SET TIME ZONE 'UTC'", &[]).await.unwrap();
    
    // Verify with SHOW
    let row = client.query_one("SHOW TimeZone", &[]).await.unwrap();
    let timezone: &str = row.get(0);
    assert_eq!(timezone, "UTC");
    
    // Test setting to different timezone
    client.execute("SET TIME ZONE 'America/New_York'", &[]).await.unwrap();
    let row = client.query_one("SHOW TimeZone", &[]).await.unwrap();
    let timezone: &str = row.get(0);
    assert_eq!(timezone, "America/New_York");
    
    // Test with offset format
    client.execute("SET TIME ZONE '+05:30'", &[]).await.unwrap();
    let row = client.query_one("SHOW TimeZone", &[]).await.unwrap();
    let timezone: &str = row.get(0);
    assert_eq!(timezone, "+05:30");
}

#[tokio::test]
async fn test_set_parameter() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test SET parameter
    client.execute("SET search_path TO public,test", &[]).await.unwrap();
    
    // Verify with SHOW
    let row = client.query_one("SHOW search_path", &[]).await.unwrap();
    let value: &str = row.get(0);
    assert_eq!(value, "public,test");
    
    // Test SET with = syntax
    client.execute("SET statement_timeout = '5min'", &[]).await.unwrap();
    let row = client.query_one("SHOW statement_timeout", &[]).await.unwrap();
    let value: &str = row.get(0);
    assert_eq!(value, "5min");
}

#[tokio::test]
async fn test_at_time_zone() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test AT TIME ZONE directly with microsecond timestamp values
    // 2023-06-15 14:30:45 = 1686839445 seconds = 1686839445000000 microseconds
    let base_timestamp_micros = 1686839445000000i64;
    
    // Test AT TIME ZONE with UTC (should be no change)
    let row = client.query_one(
        &format!("SELECT {} AT TIME ZONE 'UTC' as ts_utc", base_timestamp_micros),
        &[]
    ).await.unwrap();
    let ts_utc: f64 = row.get(0);
    // The result is returned in microseconds (datetime system uses INTEGER microseconds)
    let base_timestamp_micros = 1686839445000000.0;
    assert_eq!(ts_utc, base_timestamp_micros);
    
    // Test AT TIME ZONE with EST (should subtract 5 hours = 5 * 3600 * 1000000 microseconds)
    let row = client.query_one(
        &format!("SELECT {} AT TIME ZONE 'EST' as ts_est", base_timestamp_micros),
        &[]
    ).await.unwrap();
    let ts_est: f64 = row.get(0);
    // EST is 5 hours behind UTC, so subtract 5 * 3600 * 1000000 microseconds
    let expected_est_micros = base_timestamp_micros - 5.0 * 3600.0 * 1_000_000.0;
    assert_eq!(ts_est, expected_est_micros);
    
    // Test AT TIME ZONE with offset (+05:30 = +5.5 hours = 5.5 * 3600 * 1000000 microseconds)
    let row = client.query_one(
        &format!("SELECT {} AT TIME ZONE '+05:30' as ts_ist", base_timestamp_micros),
        &[]
    ).await.unwrap();
    let ts_ist: f64 = row.get(0);
    // +05:30 is 5.5 hours ahead of UTC, so add 5.5 * 3600 * 1000000 microseconds
    let expected_ist_micros = base_timestamp_micros + 5.5 * 3600.0 * 1_000_000.0;
    assert_eq!(ts_ist, expected_ist_micros);
}

#[tokio::test] 
async fn test_timestamptz_display() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Set timezone to EST
    client.execute("SET TIME ZONE 'EST'", &[]).await.unwrap();
    
    // Create table with timestamptz
    client.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY, event_time TIMESTAMPTZ)",
        &[]
    ).await.unwrap();
    
    // Insert a timestamp using chrono
    use chrono::{DateTime, Utc, TimeZone};
    let timestamp = Utc.timestamp_opt(1686839445, 0).unwrap(); // 2023-06-15 14:30:45 UTC
    client.execute(
        "INSERT INTO events (id, event_time) VALUES ($1, $2)",
        &[&1i32, &timestamp]
    ).await.unwrap();
    
    // Query should display in session timezone
    // Note: This test assumes the value converter handles timezone display
    // which would be implemented in Phase 3.2
    let row = client.query_one(
        "SELECT event_time FROM events WHERE id = 1",
        &[]
    ).await.unwrap();
    
    // Get the timestamp back
    let event_time: DateTime<Utc> = row.get(0);
    assert_eq!(event_time, timestamp);
}