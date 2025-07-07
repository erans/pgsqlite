mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_now_function() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test NOW() function
    let row = client.query_one("SELECT NOW() as now", &[]).await.unwrap();
    let now_timestamp: f64 = row.get("now");
    
    // Verify it's a reasonable Unix timestamp (after 2020-01-01)
    assert!(now_timestamp > 1577836800.0, "NOW() should return a Unix timestamp after 2020");
    assert!(now_timestamp < 2000000000.0, "NOW() should return a reasonable Unix timestamp");
}

#[tokio::test]
async fn test_current_date_function() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test CURRENT_DATE by casting it to text explicitly
    let row = client.query_one("SELECT CAST(CURRENT_DATE AS TEXT) as today", &[]).await.unwrap();
    let today_str: String = row.get("today");
    
    // Verify it's a valid date string (YYYY-MM-DD format)
    assert_eq!(today_str.len(), 10, "CURRENT_DATE should return date in YYYY-MM-DD format");
    assert_eq!(today_str.chars().nth(4).unwrap(), '-');
    assert_eq!(today_str.chars().nth(7).unwrap(), '-');
}

#[tokio::test]
async fn test_extract_function_direct() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test EXTRACT directly on a Unix timestamp value
    let test_timestamp = 1686840645.0; // 2023-06-15 14:30:45 UTC
    
    let row = client.query_one(
        &format!("SELECT EXTRACT(YEAR FROM {}) as year, 
                         EXTRACT(MONTH FROM {}) as month,
                         EXTRACT(DAY FROM {}) as day,
                         EXTRACT(HOUR FROM {}) as hour,
                         EXTRACT(MINUTE FROM {}) as minute",
                test_timestamp, test_timestamp, test_timestamp, test_timestamp, test_timestamp),
        &[]
    ).await.unwrap();
    
    let year: f64 = row.get("year");
    let month: f64 = row.get("month");
    let day: f64 = row.get("day");
    let hour: f64 = row.get("hour");
    let minute: f64 = row.get("minute");
    
    assert_eq!(year, 2023.0);
    assert_eq!(month, 6.0);
    assert_eq!(day, 15.0);
    assert_eq!(hour, 14.0);
    assert_eq!(minute, 30.0);
}

#[tokio::test]
async fn test_date_trunc_function_direct() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test DATE_TRUNC directly on a Unix timestamp value
    let test_timestamp = 1686840645.123456; // 2023-06-15 14:30:45.123456 UTC
    
    let row = client.query_one(
        &format!("SELECT DATE_TRUNC('hour', {}) as hour_trunc,
                         DATE_TRUNC('day', {}) as day_trunc,
                         DATE_TRUNC('month', {}) as month_trunc",
                test_timestamp, test_timestamp, test_timestamp),
        &[]
    ).await.unwrap();
    
    let hour_trunc: f64 = row.get("hour_trunc");
    let day_trunc: f64 = row.get("day_trunc");
    let month_trunc: f64 = row.get("month_trunc");
    
    // 2023-06-15 14:00:00
    assert_eq!(hour_trunc, 1686837600.0);
    // 2023-06-15 00:00:00
    assert_eq!(day_trunc, 1686787200.0);
    // 2023-06-01 00:00:00
    assert_eq!(month_trunc, 1685577600.0);
}

#[tokio::test]
async fn test_interval_arithmetic_direct() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test interval arithmetic directly on a Unix timestamp value
    let test_timestamp = 1686840645.0; // 2023-06-15 14:30:45 UTC
    
    let row = client.query_one(
        &format!("SELECT {} + INTERVAL '1 day' as tomorrow,
                         {} - INTERVAL '1 hour' as hour_ago",
                test_timestamp, test_timestamp),
        &[]
    ).await.unwrap();
    
    let tomorrow: f64 = row.get("tomorrow");
    let hour_ago: f64 = row.get("hour_ago");
    
    // Verify the calculations
    assert!((tomorrow - (test_timestamp + 86400.0)).abs() < 1.0); // +1 day
    assert!((hour_ago - (test_timestamp - 3600.0)).abs() < 1.0);  // -1 hour
}