mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_datetime_on_columns() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create a simple table
    client.execute(
        "CREATE TABLE test_table (id INTEGER PRIMARY KEY, ts REAL)",
        &[]
    ).await.unwrap();
    
    // Insert a timestamp
    client.execute(
        "INSERT INTO test_table (id, ts) VALUES (1, 1686840645.0)",
        &[]
    ).await.unwrap();
    
    // Test simple SELECT first
    let rows = client.query(
        "SELECT ts FROM test_table WHERE id = 1",
        &[]
    ).await.unwrap();
    println!("Simple SELECT returned {} rows", rows.len());
    
    // Test with lowercase extract
    let rows = client.query(
        "SELECT extract('year', ts) as year FROM test_table WHERE id = 1",
        &[]
    ).await.unwrap();
    println!("Lowercase extract returned {} rows", rows.len());
    if !rows.is_empty() {
        // Check what type we're getting
        let col = rows[0].columns().get(0).unwrap();
        println!("Column type: {:?} (OID: {})", col.type_(), col.type_().oid());
        
        if col.type_().oid() == 23 { // int4
            let year: i32 = rows[0].get(0);
            println!("Year (as i32): {}", year);
        } else {
            let year: f64 = rows[0].get(0);
            println!("Year (as f64): {}", year);
        }
    }
    
    // Test with uppercase EXTRACT
    println!("\nTesting uppercase EXTRACT...");
    match client.query(
        "SELECT EXTRACT(YEAR FROM ts) as year FROM test_table WHERE id = 1",
        &[]
    ).await {
        Ok(rows) => {
            println!("Uppercase EXTRACT succeeded with {} rows", rows.len());
            if !rows.is_empty() {
                let col = rows[0].columns().get(0).unwrap();
                println!("Column type: {:?} (OID: {})", col.type_(), col.type_().oid());
                
                if col.type_().oid() == 23 { // int4
                    let year: i32 = rows[0].get(0);
                    println!("Year (as i32): {}", year);
                } else {
                    let year: f64 = rows[0].get(0);
                    println!("Year (as f64): {}", year);
                }
            }
        }
        Err(e) => {
            println!("Uppercase EXTRACT failed: {:?}", e);
            println!("Error kind: {:?}", e.as_db_error());
        }
    }
}