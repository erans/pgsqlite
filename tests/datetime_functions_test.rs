mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_now_function() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test NOW() function - now returns INTEGER microseconds since epoch
    let row = client.query_one("SELECT NOW() as now", &[]).await.unwrap();
    let now_microseconds: i64 = row.get("now");
    
    // Convert to seconds for validation
    let now_seconds = now_microseconds as f64 / 1_000_000.0;
    
    // Verify it's a reasonable Unix timestamp (after 2020-01-01)
    assert!(now_seconds > 1577836800.0, "NOW() should return a Unix timestamp after 2020");
    assert!(now_seconds < 2000000000.0, "NOW() should return a reasonable Unix timestamp");
}

#[tokio::test]
async fn test_current_date_function() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test CURRENT_DATE function (PostgreSQL doesn't use parentheses)
    let row = client.query_one("SELECT CURRENT_DATE as today", &[]).await.unwrap();
    let today_str: String = row.get("today");
    
    // Verify it's a valid date string (YYYY-MM-DD format)
    assert!(today_str.len() == 10, "CURRENT_DATE should return date in YYYY-MM-DD format");
    assert!(today_str.chars().nth(4).unwrap() == '-');
    assert!(today_str.chars().nth(7).unwrap() == '-');
}

#[tokio::test]
async fn test_extract_function() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test EXTRACT directly on a timestamp value in microseconds (INTEGER storage)
    // 2023-06-15 14:30:45 = 1686839445 seconds = 1686839445000000 microseconds
    let timestamp_micros = 1686839445000000i64;
    let rows = client.query(
        &format!("SELECT extract('year', {}) as year, 
                         extract('month', {}) as month,
                         extract('day', {}) as day,
                         extract('hour', {}) as hour,
                         extract('minute', {}) as minute",
                timestamp_micros, timestamp_micros, timestamp_micros, timestamp_micros, timestamp_micros),
        &[]
    ).await.unwrap();
    
    assert!(!rows.is_empty(), "Query should return a row");
    let row = &rows[0];
    
    // EXTRACT returns int4 values
    let year: i32 = row.get("year");
    let month: i32 = row.get("month");
    let day: i32 = row.get("day");
    let hour: i32 = row.get("hour");
    let minute: i32 = row.get("minute");
    
    assert_eq!(year, 2023);
    assert_eq!(month, 6);
    assert_eq!(day, 15);
    assert_eq!(hour, 14);
    assert_eq!(minute, 30);
}

#[tokio::test]
async fn test_date_trunc_function() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test DATE_TRUNC directly on a timestamp value in microseconds (INTEGER storage)
    // 2023-06-15 14:30:45.123456 = 1686840645.123456 seconds = 1686840645123456 microseconds
    let timestamp_micros = 1686840645123456i64;
    let rows = client.query(
        &format!("SELECT date_trunc('hour', {}) as hour_trunc,
                         date_trunc('day', {}) as day_trunc,
                         date_trunc('month', {}) as month_trunc",
                timestamp_micros, timestamp_micros, timestamp_micros),
        &[]
    ).await.unwrap();
    
    assert!(!rows.is_empty(), "Query should return a row");
    let row = &rows[0];
    
    // date_trunc returns INTEGER microseconds
    let hour_trunc: i64 = row.get("hour_trunc");
    let day_trunc: i64 = row.get("day_trunc");
    let month_trunc: i64 = row.get("month_trunc");
    
    // Expected values in microseconds:
    // 2023-06-15 14:00:00 = 1686837600 seconds = 1686837600000000 microseconds
    assert_eq!(hour_trunc, 1686837600000000i64);
    // 2023-06-15 00:00:00 = 1686787200 seconds = 1686787200000000 microseconds
    assert_eq!(day_trunc, 1686787200000000i64);
    // 2023-06-01 00:00:00 = 1685577600 seconds = 1685577600000000 microseconds
    assert_eq!(month_trunc, 1685577600000000i64);
}

#[tokio::test]
async fn test_interval_arithmetic() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test interval arithmetic directly on a timestamp value in microseconds (INTEGER storage)
    // 2023-06-15 14:30:45 = 1686840645 seconds = 1686840645000000 microseconds
    let timestamp_micros = 1686840645000000i64;
    let rows = client.query(
        &format!("SELECT {} + INTERVAL '1 day' as tomorrow,
                         {} - INTERVAL '1 hour' as hour_ago",
                timestamp_micros, timestamp_micros),
        &[]
    ).await.unwrap();
    
    assert!(!rows.is_empty(), "Query should return a row");
    let row = &rows[0];
    
    // Interval arithmetic returns INTEGER microseconds
    let tomorrow: i64 = row.get("tomorrow");
    let hour_ago: i64 = row.get("hour_ago");
    
    // Verify the calculations (values in microseconds)
    // 2023-06-15 14:30:45 = 1686840645 seconds = 1686840645000000 microseconds
    let base_timestamp_micros = 1686840645000000i64;
    let expected_tomorrow = base_timestamp_micros + 86400000000i64; // +1 day = 86400 seconds = 86400000000 microseconds
    let expected_hour_ago = base_timestamp_micros - 3600000000i64;  // -1 hour = 3600 seconds = 3600000000 microseconds
    
    assert_eq!(tomorrow, expected_tomorrow, 
               "Tomorrow calculation incorrect: got {}, expected {}", tomorrow, expected_tomorrow);
    assert_eq!(hour_ago, expected_hour_ago,
               "Hour ago calculation incorrect: got {}, expected {}", hour_ago, expected_hour_ago);
}