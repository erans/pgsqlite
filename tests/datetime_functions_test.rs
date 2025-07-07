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
    
    // Create a table with a timestamp column
    // Use FLOAT8 type which maps to REAL in SQLite but preserves type metadata
    client.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY, created_at FLOAT8)",
        &[]
    ).await.unwrap();
    
    // Insert a known timestamp (2023-06-15 14:30:45)
    // Use Unix timestamp format as TIMESTAMP is stored as REAL
    let timestamp = 1686840645.0f64; // 2023-06-15 14:30:45 UTC
    client.execute(
        "INSERT INTO events (id, created_at) VALUES ($1, $2)",
        &[&1i32, &timestamp]
    ).await.unwrap();
    
    // Test EXTRACT
    let row = client.query_one(
        "SELECT EXTRACT(YEAR FROM created_at) as year, 
                EXTRACT(MONTH FROM created_at) as month,
                EXTRACT(DAY FROM created_at) as day,
                EXTRACT(HOUR FROM created_at) as hour,
                EXTRACT(MINUTE FROM created_at) as minute
         FROM events WHERE id = 1",
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
async fn test_date_trunc_function() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create a table with a timestamp column
    // Use FLOAT8 type which maps to REAL in SQLite but preserves type metadata
    client.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY, created_at FLOAT8)",
        &[]
    ).await.unwrap();
    
    // Insert a known timestamp (2023-06-15 14:30:45.123456)
    // Use Unix timestamp format as TIMESTAMP is stored as REAL
    let timestamp = 1686840645.123456f64; // 2023-06-15 14:30:45.123456 UTC
    client.execute(
        "INSERT INTO events (id, created_at) VALUES ($1, $2)",
        &[&1i32, &timestamp]
    ).await.unwrap();
    
    // Test DATE_TRUNC
    let row = client.query_one(
        "SELECT DATE_TRUNC('hour', created_at) as hour_trunc,
                DATE_TRUNC('day', created_at) as day_trunc,
                DATE_TRUNC('month', created_at) as month_trunc
         FROM events WHERE id = 1",
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
async fn test_interval_arithmetic() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create a table with a timestamp column
    // Use FLOAT8 type which maps to REAL in SQLite but preserves type metadata
    client.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY, created_at FLOAT8)",
        &[]
    ).await.unwrap();
    
    // Insert a known timestamp
    // Use Unix timestamp format as TIMESTAMP is stored as REAL
    let timestamp = 1686840645.0f64; // 2023-06-15 14:30:45 UTC
    client.execute(
        "INSERT INTO events (id, created_at) VALUES ($1, $2)",
        &[&1i32, &timestamp]
    ).await.unwrap();
    
    // Test interval arithmetic
    let row = client.query_one(
        "SELECT created_at + INTERVAL '1 day' as tomorrow,
                created_at - INTERVAL '1 hour' as hour_ago
         FROM events WHERE id = 1",
        &[]
    ).await.unwrap();
    
    let tomorrow: f64 = row.get("tomorrow");
    let hour_ago: f64 = row.get("hour_ago");
    
    // Verify the calculations (using approximate values since we're dealing with floats)
    let timestamp = 1686840645.0; // Expected timestamp
    assert!((tomorrow - (timestamp + 86400.0)).abs() < 1.0); // +1 day
    assert!((hour_ago - (timestamp - 3600.0)).abs() < 1.0);  // -1 hour
}