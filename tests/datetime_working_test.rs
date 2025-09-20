#![allow(clippy::collapsible_if)]

mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_now_function() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test NOW() function - NOW() now returns formatted timestamp string
    let row = client.query_one("SELECT NOW() as now", &[]).await.unwrap();
    let now_str: String = row.get("now");
    
    // Verify it's a properly formatted timestamp (YYYY-MM-DD HH:MM:SS.ffffff)
    assert!(now_str.contains('-'), "NOW() should return formatted timestamp with dashes");
    assert!(now_str.contains(':'), "NOW() should return formatted timestamp with colons");
    assert!(now_str.contains('.'), "NOW() should return formatted timestamp with microseconds");
    assert!(now_str.len() > 20, "NOW() should return full timestamp string");
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
    // Use DOUBLE PRECISION to be more explicit about the type
    client.execute(
        "CREATE TABLE timestamps (id INTEGER PRIMARY KEY, ts DOUBLE PRECISION)",
        &[]
    ).await.unwrap();
    
    // Insert a test timestamp
    let test_timestamp = 1686839445.0f64; // 2023-06-15 14:30:45 UTC
    client.execute(
        "INSERT INTO timestamps (id, ts) VALUES ($1, $2)",
        &[&1i32, &test_timestamp]
    ).await.unwrap();
    
    // First convert the timestamp column to microseconds using CAST
    // Also debug the raw ts value
    // First check if to_timestamp works
    let to_timestamp_test = client.simple_query(
        "SELECT to_timestamp(ts) FROM timestamps WHERE id = 1"
    ).await;
    
    match to_timestamp_test {
        Ok(results) => {
            for msg in results {
                if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                    eprintln!("to_timestamp(ts) returned: {:?}", row.get(0));
                }
            }
        }
        Err(e) => {
            eprintln!("ERROR: to_timestamp failed: {e}");
        }
    }
    
    // Test with to_timestamp directly - this should work
    eprintln!("Testing EXTRACT with to_timestamp(ts)...");
    let results = client.simple_query(
        "SELECT EXTRACT(YEAR FROM to_timestamp(ts)) as year,
                EXTRACT(MONTH FROM to_timestamp(ts)) as month,
                EXTRACT(DAY FROM to_timestamp(ts)) as day,
                EXTRACT(HOUR FROM to_timestamp(ts)) as hour,
                EXTRACT(MINUTE FROM to_timestamp(ts)) as minute
         FROM timestamps WHERE id = 1"
    ).await;
    
    match results {
        Ok(msgs) => {
            for msg in msgs {
                if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                    eprintln!("SUCCESS: EXTRACT with to_timestamp returned year={:?}, month={:?}, day={:?}, hour={:?}, minute={:?}", 
                             row.get(0), row.get(1), row.get(2), row.get(3), row.get(4));
                    
                    // Verify results
                    assert_eq!(row.get(0), Some("2023"));
                    assert_eq!(row.get(1), Some("6"));
                    assert_eq!(row.get(2), Some("15"));
                    assert_eq!(row.get(3), Some("14"));
                    assert_eq!(row.get(4), Some("30"));
                    
                    // Test passed!
                    return;
                }
            }
        }
        Err(e) => {
            eprintln!("ERROR: EXTRACT with to_timestamp failed: {e}");
        }
    }
    
    // If we get here, the direct to_timestamp approach failed
    panic!("EXTRACT with to_timestamp(ts) failed");
}

#[tokio::test]
async fn test_date_trunc_with_table() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create a table with a REAL column to store timestamps
    // Use DOUBLE PRECISION to be more explicit about the type
    client.execute(
        "CREATE TABLE timestamps (id INTEGER PRIMARY KEY, ts DOUBLE PRECISION)",
        &[]
    ).await.unwrap();
    
    // Insert a test timestamp
    let test_timestamp = 1686839445.123456f64; // 2023-06-15 14:30:45.123456 UTC
    client.execute(
        "INSERT INTO timestamps (id, ts) VALUES ($1, $2)",
        &[&1i32, &test_timestamp]
    ).await.unwrap();
    
    // Test DATE_TRUNC function - use regular query to handle binary results
    let rows = client.query(
        "SELECT DATE_TRUNC('hour', to_timestamp(ts)) as hour_trunc,
                DATE_TRUNC('day', to_timestamp(ts)) as day_trunc,
                DATE_TRUNC('month', to_timestamp(ts)) as month_trunc
         FROM timestamps WHERE id = 1",
        &[]
    ).await.unwrap();
    
    // Verify results
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    
    // Values are i64 microseconds since epoch
    let hour_val: i64 = row.get(0);
    let day_val: i64 = row.get(1);
    let month_val: i64 = row.get(2);
    
    // Convert expected values from seconds to microseconds
    // 2023-06-15 14:00:00
    let expected_hour = 1686837600i64 * 1_000_000;
    assert!((hour_val - expected_hour).abs() < 1_000_000, "hour_trunc: expected {expected_hour}, got {hour_val}");
    // 2023-06-15 00:00:00  
    let expected_day = 1686787200i64 * 1_000_000;
    assert!((day_val - expected_day).abs() < 1_000_000, "day_trunc: expected {expected_day}, got {day_val}");
    // 2023-06-01 00:00:00
    let expected_month = 1685577600i64 * 1_000_000;
    assert!((month_val - expected_month).abs() < 1_000_000, "month_trunc: expected {expected_month}, got {month_val}");
}

#[tokio::test]
async fn test_interval_arithmetic_with_table() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create a table with a REAL column to store timestamps
    // Use DOUBLE PRECISION to be more explicit about the type
    client.execute(
        "CREATE TABLE timestamps (id INTEGER PRIMARY KEY, ts DOUBLE PRECISION)",
        &[]
    ).await.unwrap();
    
    // Insert a test timestamp
    let test_timestamp = 1686839445.0f64; // 2023-06-15 14:30:45 UTC
    client.execute(
        "INSERT INTO timestamps (id, ts) VALUES ($1, $2)",
        &[&1i32, &test_timestamp]
    ).await.unwrap();
    
    // Test interval arithmetic - convert to microseconds first
    let rows = client.query(
        "SELECT to_timestamp(ts) + 86400000000 as tomorrow,
                to_timestamp(ts) - 3600000000 as hour_ago
         FROM timestamps WHERE id = 1",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    
    // Values are i64 microseconds
    let tomorrow: i64 = row.get(0);
    let hour_ago: i64 = row.get(1);
    
    // Convert test_timestamp to microseconds
    let test_timestamp_micros = (test_timestamp * 1_000_000.0) as i64;
    
    // Verify the calculations
    let expected_tomorrow = test_timestamp_micros + 86400 * 1_000_000;
    let expected_hour_ago = test_timestamp_micros - 3600 * 1_000_000;
    
    assert_eq!(tomorrow, expected_tomorrow, 
               "tomorrow: expected {expected_tomorrow}, got {tomorrow}");
    assert_eq!(hour_ago, expected_hour_ago,
               "hour_ago: expected {expected_hour_ago}, got {hour_ago}");
}