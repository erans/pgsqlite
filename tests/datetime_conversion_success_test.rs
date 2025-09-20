mod common;
use common::*;

#[tokio::test]
async fn test_datetime_conversion_success() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table
    client.execute(
        "CREATE TABLE dt_test (id INT, date_col DATE, time_col TIME)",
        &[]
    ).await.unwrap();
    
    // Insert using simple_query for translation
    client.simple_query(
        "INSERT INTO dt_test VALUES (1, '2024-01-15', '14:30:00')"
    ).await.unwrap();
    
    // Verify storage is INTEGER
    let storage_check = client.simple_query(
        "SELECT typeof(date_col), typeof(time_col) FROM dt_test WHERE id = 1"
    ).await.unwrap();
    
    if let Some(msg) = storage_check.into_iter().find(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        && let tokio_postgres::SimpleQueryMessage::Row(data) = msg {
            let date_type = data.get("typeof(date_col)").unwrap();
            let time_type = data.get("typeof(time_col)").unwrap();

            println!("Storage check:");
            println!("  Date type: {date_type}");
            println!("  Time type: {time_type}");

            assert_eq!(date_type, "integer", "Date stored as INTEGER");
            assert_eq!(time_type, "integer", "Time stored as INTEGER");
        }
    
    // Now test retrieval - the value converter should convert back
    let retrieve = client.simple_query(
        "SELECT date_col, time_col FROM dt_test WHERE id = 1"
    ).await.unwrap();
    
    if let Some(msg) = retrieve.into_iter().find(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        && let tokio_postgres::SimpleQueryMessage::Row(data) = msg {
            let date_str = data.get("date_col").unwrap();
            let time_str = data.get("time_col").unwrap();

            println!("\nRetrieved values:");
            println!("  Date: {date_str}");
            println!("  Time: {time_str}");

            // These should be the converted values
            assert_eq!(date_str, "2024-01-15", "Date should be converted back");
            assert_eq!(time_str, "14:30:00", "Time should be converted back");
        }
    
    server.abort();
}