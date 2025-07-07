mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_now_function() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test NOW() function - NOW() now returns microseconds since epoch as INT8
    let row = client.query_one("SELECT NOW() as now", &[]).await.unwrap();
    let now_microseconds: i64 = row.get("now");
    
    // Convert microseconds to seconds for validation
    let now_timestamp = now_microseconds as f64 / 1_000_000.0;
    
    // Verify it's a reasonable Unix timestamp (after 2020-01-01)
    assert!(now_timestamp > 1577836800.0, "NOW() should return a Unix timestamp after 2020");
    assert!(now_timestamp < 2000000000.0, "NOW() should return a reasonable Unix timestamp");
}

#[tokio::test]
async fn test_current_date_text() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Use simple query protocol which preserves SQLite's text type for CURRENT_DATE
    let results = client.simple_query("SELECT CURRENT_DATE").await.unwrap();
    
    // Verify we got a result
    let mut found_date = false;
    for msg in results {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            if let Some(date_str) = row.get(0) {
                // Verify it's a valid date string (YYYY-MM-DD format)
                assert_eq!(date_str.len(), 10, "CURRENT_DATE should return date in YYYY-MM-DD format");
                assert_eq!(date_str.chars().nth(4).unwrap(), '-');
                assert_eq!(date_str.chars().nth(7).unwrap(), '-');
                found_date = true;
            }
        }
    }
    assert!(found_date, "Should have found a date value");
}

#[tokio::test]
async fn test_datetime_functions_with_table() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create a table with a REAL column to store timestamps
    client.execute(
        "CREATE TABLE timestamps (id INTEGER PRIMARY KEY, ts REAL)",
        &[]
    ).await.unwrap();
    
    // Insert a test timestamp
    let test_timestamp = 1686839445.0f32; // 2023-06-15 14:30:45 UTC
    client.execute(
        "INSERT INTO timestamps (id, ts) VALUES ($1, $2)",
        &[&1i32, &test_timestamp]
    ).await.unwrap();
    
    // Test EXTRACT function on the column - convert seconds to microseconds first
    let results = client.simple_query(
        "SELECT EXTRACT(YEAR FROM to_timestamp(ts)) as year, 
                EXTRACT(MONTH FROM to_timestamp(ts)) as month,
                EXTRACT(DAY FROM to_timestamp(ts)) as day,
                EXTRACT(HOUR FROM to_timestamp(ts)) as hour,
                EXTRACT(MINUTE FROM to_timestamp(ts)) as minute
         FROM timestamps WHERE id = 1"
    ).await.unwrap();
    
    // Verify results using simple query protocol
    for msg in results {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            assert_eq!(row.get(0), Some("2023"));
            assert_eq!(row.get(1), Some("6"));
            assert_eq!(row.get(2), Some("15"));
            assert_eq!(row.get(3), Some("14"));
            assert_eq!(row.get(4), Some("30"), "minute should be 30, got {:?}", row.get(4));
        }
    }
}

#[tokio::test]
async fn test_date_trunc_with_table() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create a table with a REAL column to store timestamps
    client.execute(
        "CREATE TABLE timestamps (id INTEGER PRIMARY KEY, ts REAL)",
        &[]
    ).await.unwrap();
    
    // Insert a test timestamp
    let test_timestamp = 1686839445.123456f32; // 2023-06-15 14:30:45.123456 UTC
    client.execute(
        "INSERT INTO timestamps (id, ts) VALUES ($1, $2)",
        &[&1i32, &test_timestamp]
    ).await.unwrap();
    
    // First check what type the column is being detected as
    let debug_results = client.simple_query(
        "SELECT typeof(ts), ts FROM timestamps WHERE id = 1"
    ).await.unwrap();
    
    for msg in debug_results {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            eprintln!("DEBUG: ts typeof: {:?}, value: {:?}", row.get(0), row.get(1));
        }
    }
    
    // Test DATE_TRUNC function - convert seconds to microseconds first
    let results = client.simple_query(
        "SELECT DATE_TRUNC('hour', to_timestamp(ts)) as hour_trunc,
                DATE_TRUNC('day', to_timestamp(ts)) as day_trunc,
                DATE_TRUNC('month', to_timestamp(ts)) as month_trunc
         FROM timestamps WHERE id = 1"
    ).await.unwrap();
    
    // Verify results
    for msg in results {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            // Values as strings
            let hour_str = row.get(0).unwrap();
            let day_str = row.get(1).unwrap();
            let month_str = row.get(2).unwrap();
            
            // Parse and verify - values are now in microseconds since epoch
            let hour_val: i64 = hour_str.parse().unwrap();
            let day_val: i64 = day_str.parse().unwrap();
            let month_val: i64 = month_str.parse().unwrap();
            
            // Convert expected values from seconds to microseconds
            // 2023-06-15 14:00:00
            let expected_hour = 1686837600i64 * 1_000_000;
            assert!((hour_val - expected_hour).abs() < 1_000_000, "hour_trunc: expected {}, got {}", expected_hour, hour_val);
            // 2023-06-15 00:00:00  
            let expected_day = 1686787200i64 * 1_000_000;
            assert!((day_val - expected_day).abs() < 1_000_000, "day_trunc: expected {}, got {}", expected_day, day_val);
            // 2023-06-01 00:00:00
            let expected_month = 1685577600i64 * 1_000_000;
            assert!((month_val - expected_month).abs() < 1_000_000, "month_trunc: expected {}, got {}", expected_month, month_val);
        }
    }
}

#[tokio::test]
async fn test_interval_arithmetic_with_table() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create a table with a REAL column to store timestamps
    client.execute(
        "CREATE TABLE timestamps (id INTEGER PRIMARY KEY, ts REAL)",
        &[]
    ).await.unwrap();
    
    // Insert a test timestamp
    let test_timestamp = 1686839445.0f32; // 2023-06-15 14:30:45 UTC
    client.execute(
        "INSERT INTO timestamps (id, ts) VALUES ($1, $2)",
        &[&1i32, &test_timestamp]
    ).await.unwrap();
    
    // Test interval arithmetic - cast results to text to avoid binary data
    let results = client.simple_query(
        "SELECT CAST(ts + 86400 AS TEXT) as tomorrow,
                CAST(ts - 3600 AS TEXT) as hour_ago
         FROM timestamps WHERE id = 1"
    ).await.unwrap();
    
    // Verify results
    for msg in results {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let tomorrow_str = row.get(0).unwrap();
            let hour_ago_str = row.get(1).unwrap();
            
            let tomorrow: f64 = tomorrow_str.parse().unwrap();
            let hour_ago: f64 = hour_ago_str.parse().unwrap();
            
            // Verify the calculations
            assert!((tomorrow - (test_timestamp as f64 + 86400.0)).abs() < 1.0, 
                    "tomorrow: expected {}, got {}", test_timestamp as f64 + 86400.0, tomorrow);
            assert!((hour_ago - (test_timestamp as f64 - 3600.0)).abs() < 1.0, 
                    "hour_ago: expected {}, got {}", test_timestamp as f64 - 3600.0, hour_ago);
        }
    }
}