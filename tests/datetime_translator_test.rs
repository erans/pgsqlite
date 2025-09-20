mod common;
use common::*;

// Note: Datetime conversion is now handled by InsertTranslator and value converters,
// not by triggers. These tests verify the old trigger behavior is no longer present.

#[tokio::test]
async fn test_datetime_trigger_creation() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Debug logging is already initialized by the test server
    
    // Create table with datetime columns
    client.execute(
        "CREATE TABLE trigger_test (
            id INTEGER PRIMARY KEY,
            date_col DATE,
            time_col TIME,
            timestamp_col TIMESTAMP
        )",
        &[]
    ).await.unwrap();
    
    // Check that datetime triggers are NOT created (we use InsertTranslator instead)
    let trigger_check = client.query(
        "SELECT name, sql FROM sqlite_master WHERE type = 'trigger' AND name LIKE '__pgsqlite_datetime%'",
        &[]
    ).await.unwrap();
    
    assert!(trigger_check.is_empty(), "Datetime triggers should not be created anymore");
    
    // Check __pgsqlite_schema for datetime columns
    let schema_check = client.query(
        "SELECT column_name, pg_type, sqlite_type FROM __pgsqlite_schema 
         WHERE table_name = 'trigger_test' AND pg_type IN ('date', 'time', 'timestamp')",
        &[]
    ).await.unwrap();
    
    println!("\nDatetime columns in schema:");
    for row in &schema_check {
        let col: String = row.get(0);
        let pg_type: String = row.get(1);
        let sqlite_type: String = row.get(2);
        println!("  {col} -> pg: {pg_type}, sqlite: {sqlite_type}");
    }
    
    // Test INSERT with datetime literals
    client.execute(
        "INSERT INTO trigger_test VALUES (1, '2024-01-15', '14:30:00', '2024-01-15 14:30:00')",
        &[]
    ).await.unwrap();
    
    // Small delay to let triggers execute
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Check storage types
    let type_check = client.query(
        "SELECT typeof(date_col), typeof(time_col), typeof(timestamp_col) 
         FROM trigger_test WHERE id = 1",
        &[]
    ).await.unwrap();
    
    if !type_check.is_empty() {
        let row = &type_check[0];
        let date_type: &str = row.get(0);
        let time_type: &str = row.get(1);
        let timestamp_type: &str = row.get(2);
        
        println!("\nStorage types after trigger execution:");
        println!("  date: {date_type}");
        println!("  time: {time_type}");
        println!("  timestamp: {timestamp_type}");
        
        // Also check the actual values using cast to text
        let value_check = client.query(
            "SELECT CAST(date_col AS TEXT), CAST(time_col AS TEXT), CAST(timestamp_col AS TEXT) FROM trigger_test WHERE id = 1",
            &[]
        ).await.unwrap();
        
        if !value_check.is_empty() {
            let row = &value_check[0];
            // These might be integers now if triggers worked
            println!("\nRaw values (cast to text):");
            println!("  date: {}", row.get::<_, String>(0));
            println!("  time: {}", row.get::<_, String>(1));
            println!("  timestamp: {}", row.get::<_, String>(2));
        }
    }
    
    server.abort();
}

#[tokio::test]
async fn test_insert_translator_conversion() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create simple table
    client.execute(
        "CREATE TABLE translator_test (
            id INTEGER PRIMARY KEY,
            date_col DATE
        )",
        &[]
    ).await.unwrap();
    
    // Test INSERT using simple_query (which uses InsertTranslator)
    client.simple_query(
        "INSERT INTO translator_test VALUES (1, '2024-01-15')"
    ).await.unwrap();
    
    // Check storage type
    let type_check = client.simple_query(
        "SELECT typeof(date_col) FROM translator_test WHERE id = 1"
    ).await.unwrap();
    
    if let Some(msg) = type_check.into_iter().find(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        && let tokio_postgres::SimpleQueryMessage::Row(data) = msg {
            let col_type = data.get("typeof(date_col)").unwrap();
            println!("Storage type after InsertTranslator: {col_type}");
            assert_eq!(col_type, "integer", "InsertTranslator should convert to INTEGER");
        }
    
    // Test retrieval conversion
    let value_check = client.simple_query(
        "SELECT date_col FROM translator_test WHERE id = 1"
    ).await.unwrap();
    
    if let Some(msg) = value_check.into_iter().find(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        && let tokio_postgres::SimpleQueryMessage::Row(data) = msg {
            let date_str = data.get("date_col").unwrap();
            println!("Retrieved value after conversion: {date_str}");
            assert_eq!(date_str, "2024-01-15", "Value converter should convert back to date string");
        }
    
    server.abort();
}