mod common;
use common::*;

#[tokio::test]
async fn test_insert_execution_path() {
    let server = setup_test_server().await;
    
    // Create table first
    server.client.execute(
        "CREATE TABLE test_table (id INT, date_col DATE, time_col TIME)",
        &[]
    ).await.unwrap();
    
    eprintln!("\n=== Testing INSERT execution path ===");
    
    // This query should:
    // 1. NOT use ultra-fast path (has datetime pattern)
    // 2. Go through InsertTranslator
    // 3. Convert '2024-01-15' to integer days
    let query = "INSERT INTO test_table (id, date_col, time_col) VALUES (1, '2024-01-15', '14:30:00')";
    eprintln!("Executing: {}", query);
    
    // Use simple_query to ensure we go through simple protocol
    server.client.simple_query(query).await.unwrap();
    
    // Check storage type
    let type_check = server.client.simple_query(
        "SELECT typeof(date_col), typeof(time_col) FROM test_table WHERE id = 1"
    ).await.unwrap();
    
    if let Some(row) = type_check.into_iter().find(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))) {
        if let tokio_postgres::SimpleQueryMessage::Row(data) = row {
            let date_type = data.get("typeof(date_col)").unwrap();
            let time_type = data.get("typeof(time_col)").unwrap();
            
            eprintln!("Storage types:");
            eprintln!("  Date: {}", date_type);
            eprintln!("  Time: {}", time_type);
            
            assert_eq!(date_type, "integer", "Date should be stored as INTEGER");
            assert_eq!(time_type, "integer", "Time should be stored as INTEGER");
            eprintln!("SUCCESS: DateTime values were converted to INTEGER!");
        }
    }
    
    // Check that values are properly converted back
    let value_check = server.client.simple_query(
        "SELECT date_col, time_col FROM test_table WHERE id = 1"
    ).await.unwrap();
    
    if let Some(row) = value_check.into_iter().find(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))) {
        if let tokio_postgres::SimpleQueryMessage::Row(data) = row {
            let date_value = data.get("date_col").unwrap();
            let time_value = data.get("time_col").unwrap();
            
            eprintln!("Retrieved values:");
            eprintln!("  Date: {}", date_value);
            eprintln!("  Time: {}", time_value);
            
            assert_eq!(date_value, "2024-01-15", "Date should be converted back to string");
            assert_eq!(time_value, "14:30:00", "Time should be converted back to string");
            eprintln!("SUCCESS: Values properly converted back to datetime strings!");
        }
    }
    
    server.abort();
}