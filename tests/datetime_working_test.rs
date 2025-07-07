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
    
    // Test EXTRACT function on the column
    let results = client.simple_query(
        "SELECT EXTRACT(YEAR FROM ts) as year, 
                EXTRACT(MONTH FROM ts) as month,
                EXTRACT(DAY FROM ts) as day,
                EXTRACT(HOUR FROM ts) as hour,
                EXTRACT(MINUTE FROM ts) as minute
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
    
    // Test DATE_TRUNC function - handle potential BLOB type issue
    let results = match client.simple_query(
        "SELECT DATE_TRUNC('hour', ts) as hour_trunc,
                DATE_TRUNC('day', ts) as day_trunc,
                DATE_TRUNC('month', ts) as month_trunc
         FROM timestamps WHERE id = 1"
    ).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("DATE_TRUNC failed: {:?}", e);
            // Try with CAST to REAL
            client.simple_query(
                "SELECT DATE_TRUNC('hour', CAST(ts AS REAL)) as hour_trunc,
                        DATE_TRUNC('day', CAST(ts AS REAL)) as day_trunc,
                        DATE_TRUNC('month', CAST(ts AS REAL)) as month_trunc
                 FROM timestamps WHERE id = 1"
            ).await.unwrap()
        }
    };
    
    // Verify results
    for msg in results {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            // Values as strings
            let hour_str = row.get(0).unwrap();
            let day_str = row.get(1).unwrap();
            let month_str = row.get(2).unwrap();
            
            // Parse and verify
            let hour_val: f64 = hour_str.parse().unwrap();
            let day_val: f64 = day_str.parse().unwrap();
            let month_val: f64 = month_str.parse().unwrap();
            
            // 2023-06-15 14:00:00
            assert!((hour_val - 1686837600.0).abs() < 1.0, "hour_trunc: expected 1686837600, got {}", hour_val);
            // 2023-06-15 00:00:00
            assert!((day_val - 1686787200.0).abs() < 1.0, "day_trunc: expected 1686787200, got {}", day_val);
            // 2023-06-01 00:00:00
            assert!((month_val - 1685577600.0).abs() < 1.0, "month_trunc: expected 1685577600, got {}", month_val);
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