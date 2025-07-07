mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_datetime_query_debug() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    println!("Creating table...");
    match client.execute(
        "CREATE TABLE test_events (id INTEGER PRIMARY KEY, ts REAL)",
        &[]
    ).await {
        Ok(_) => println!("Table created successfully"),
        Err(e) => {
            println!("Failed to create table: {:?}", e);
            return;
        }
    }
    
    println!("\nInserting data...");
    match client.execute(
        "INSERT INTO test_events (id, ts) VALUES (1, 1686840645.0)",
        &[]
    ).await {
        Ok(_) => println!("Data inserted successfully"),
        Err(e) => {
            println!("Failed to insert data: {:?}", e);
            return;
        }
    }
    
    println!("\nTesting simple SELECT...");
    match client.query("SELECT * FROM test_events", &[]).await {
        Ok(rows) => println!("Simple SELECT succeeded: {} rows", rows.len()),
        Err(e) => println!("Simple SELECT failed: {:?}", e),
    }
    
    println!("\nTesting EXTRACT with direct value...");
    match client.query("SELECT EXTRACT(YEAR FROM 1686840645.0)", &[]).await {
        Ok(rows) => println!("EXTRACT with literal succeeded: {} rows", rows.len()),
        Err(e) => println!("EXTRACT with literal failed: {:?}", e),
    }
    
    println!("\nTesting lowercase extract with column...");
    match client.query("SELECT extract('year', ts) FROM test_events WHERE id = 1", &[]).await {
        Ok(rows) => println!("Lowercase extract succeeded: {} rows", rows.len()),
        Err(e) => println!("Lowercase extract failed: {:?}", e),
    }
    
    println!("\nTesting EXTRACT with column...");
    match client.query("SELECT EXTRACT(YEAR FROM ts) FROM test_events WHERE id = 1", &[]).await {
        Ok(rows) => println!("EXTRACT with column succeeded: {} rows", rows.len()),
        Err(e) => {
            println!("EXTRACT with column failed: {:?}", e);
            println!("Error details: {:?}", e.as_db_error());
            
            // Try a simpler version
            println!("\nTrying simpler query...");
            match client.query("SELECT ts FROM test_events WHERE id = 1", &[]).await {
                Ok(rows) => {
                    println!("Got ts value: {} rows", rows.len());
                    if !rows.is_empty() {
                        let ts: f32 = rows[0].get(0);
                        println!("ts = {}", ts);
                    }
                }
                Err(e) => println!("Even simple query failed: {:?}", e),
            }
        }
    }
}