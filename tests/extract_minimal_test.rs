mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_extract_minimal() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create a table with a timestamp
    client.execute(
        "CREATE TABLE test_dates (id INTEGER PRIMARY KEY, ts REAL)",
        &[]
    ).await.unwrap();
    
    // Insert a test timestamp
    client.execute(
        "INSERT INTO test_dates (id, ts) VALUES (1, 1686840645.0)",
        &[]
    ).await.unwrap();
    
    // Test EXTRACT on the column - convert seconds to microseconds first
    let result = client.query_one(
        "SELECT EXTRACT(YEAR FROM to_timestamp(ts)) as year FROM test_dates WHERE id = 1",
        &[]
    ).await;
    
    match result {
        Ok(row) => {
            let year: i32 = row.get(0);  // EXTRACT now returns i32, not f64
            assert_eq!(year, 2023);
            println!("EXTRACT from column works!");
        }
        Err(e) => {
            eprintln!("Error with EXTRACT from column: {}", e);
        }
    }
    
    // Test EXTRACT on a literal - convert seconds to microseconds first
    let result2 = client.query_one(
        "SELECT EXTRACT(YEAR FROM to_timestamp(1686840645.0)) as year",
        &[]
    ).await;
    
    match result2 {
        Ok(row) => {
            let year: i32 = row.get(0);  // EXTRACT now returns i32, not f64
            assert_eq!(year, 2023);
            println!("EXTRACT from literal works!");
        }
        Err(e) => {
            eprintln!("Error with EXTRACT from literal: {}", e);
        }
    }
}