mod common;
use common::*;

#[tokio::test]
async fn test_datetime_roundtrip() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with datetime columns
    client.execute(
        "CREATE TABLE roundtrip_test (
            id INTEGER PRIMARY KEY,
            date_col DATE,
            time_col TIME,
            timestamp_col TIMESTAMP
        )",
        &[]
    ).await.unwrap();
    
    // Check that metadata is stored correctly
    let metadata_check = client.query(
        "SELECT column_name, pg_type FROM __pgsqlite_schema WHERE table_name = 'roundtrip_test' ORDER BY column_name",
        &[]
    ).await.unwrap();
    
    println!("Metadata in __pgsqlite_schema:");
    for row in &metadata_check {
        let col: String = row.get(0);
        let typ: String = row.get(1);
        println!("  {col} -> {typ}");
    }
    
    // Insert using simple_query for translation
    client.simple_query(
        "INSERT INTO roundtrip_test VALUES (1, '2024-01-15', '14:30:00', '2024-01-15 14:30:00')"
    ).await.unwrap();
    
    // Verify storage is INTEGER
    let storage = client.simple_query(
        "SELECT typeof(date_col), typeof(time_col), typeof(timestamp_col) FROM roundtrip_test WHERE id = 1"
    ).await.unwrap();
    
    if let Some(msg) = storage.into_iter().find(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))) {
        if let tokio_postgres::SimpleQueryMessage::Row(data) = msg {
            println!("\nStorage types:");
            println!("  date: {}", data.get("typeof(date_col)").unwrap());
            println!("  time: {}", data.get("typeof(time_col)").unwrap());
            println!("  timestamp: {}", data.get("typeof(timestamp_col)").unwrap());
        }
    }
    
    // Now test retrieval with proper types
    let result = client.query(
        "SELECT date_col, time_col, timestamp_col FROM roundtrip_test WHERE id = 1",
        &[]
    ).await.unwrap();
    
    if !result.is_empty() {
        let row = &result[0];
        
        // These should work if conversion is correct
        let date_val: chrono::NaiveDate = row.get(0);
        let time_val: chrono::NaiveTime = row.get(1);
        let timestamp_val: chrono::NaiveDateTime = row.get(2);
        
        println!("\nRetrieved values:");
        println!("  Date: {date_val}");
        println!("  Time: {time_val}");
        println!("  Timestamp: {timestamp_val}");
        
        assert_eq!(date_val.to_string(), "2024-01-15");
        assert_eq!(time_val.to_string(), "14:30:00");
        assert_eq!(timestamp_val.to_string(), "2024-01-15 14:30:00");
    }
    
    server.abort();
}

#[tokio::test]
async fn test_simple_query_datetime_conversion() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table
    client.execute(
        "CREATE TABLE simple_test (id INT, date_col DATE)",
        &[]
    ).await.unwrap();
    
    // Insert with simple_query
    client.simple_query(
        "INSERT INTO simple_test VALUES (1, '2024-01-15')"
    ).await.unwrap();
    
    // Retrieve with simple_query - should get converted value
    let result = client.simple_query(
        "SELECT date_col FROM simple_test WHERE id = 1"
    ).await.unwrap();
    
    if let Some(msg) = result.into_iter().find(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))) {
        if let tokio_postgres::SimpleQueryMessage::Row(data) = msg {
            let date_str = data.get("date_col").unwrap();
            println!("Simple query result: {date_str}");
            
            // If conversion works, this should be '2024-01-15', not '19737'
            assert_eq!(date_str, "2024-01-15", "Date should be converted from INTEGER to string");
        }
    }
    
    server.abort();
}