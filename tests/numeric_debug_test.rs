mod common;
use common::*;

#[tokio::test]
async fn test_numeric_constraint_debug() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with NUMERIC constraint
    client.execute(
        "CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            amount NUMERIC(5,2)
        )",
        &[]
    ).await.unwrap();
    
    // Check if the constraints were stored
    let rows = client.query(
        "SELECT * FROM __pgsqlite_numeric_constraints WHERE table_name = 'test_table'",
        &[]
    ).await.unwrap();
    
    println!("Numeric constraints found: {}", rows.len());
    for row in &rows {
        let table_name: String = row.get(0);
        let column_name: String = row.get(1);
        let precision: i32 = row.get(2);
        let scale: i32 = row.get(3);
        println!("Constraint: {}.{} NUMERIC({},{})", table_name, column_name, precision, scale);
    }
    
    // Check if triggers were created
    let triggers = client.query(
        "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE '__pgsqlite_numeric_%'",
        &[]
    ).await.unwrap();
    
    println!("Triggers found: {}", triggers.len());
    for row in &triggers {
        let name: String = row.get(0);
        println!("Trigger: {}", name);
    }
    
    // Try to insert a valid value
    client.execute(
        "INSERT INTO test_table (id, amount) VALUES (1, 123.45)",
        &[]
    ).await.unwrap();
    println!("Valid insert succeeded");
    
    // Try to insert an invalid value (too many decimal places)
    let result = client.execute(
        "INSERT INTO test_table (id, amount) VALUES (2, 123.456)",
        &[]
    ).await;
    
    match result {
        Ok(_) => println!("ERROR: Invalid insert succeeded when it should have failed!"),
        Err(e) => println!("Invalid insert failed as expected: {}", e),
    }
    
    // Try to insert a value that exceeds precision
    let result = client.execute(
        "INSERT INTO test_table (id, amount) VALUES (3, 1234.56)",
        &[]
    ).await;
    
    match result {
        Ok(_) => println!("ERROR: Precision overflow insert succeeded when it should have failed!"),
        Err(e) => println!("Precision overflow insert failed as expected: {}", e),
    }
    
    server.abort();
}