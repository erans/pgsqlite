mod common;
use common::*;

#[tokio::test]
async fn test_single_numeric_constraint() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with NUMERIC(10,2)
    client.execute(
        "CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            price NUMERIC(10,2)
        )",
        &[]
    ).await.unwrap();
    
    // This should work: 8 digits before decimal, 2 after = 10 total
    client.execute(
        "INSERT INTO products VALUES (1, 99999999.99)",
        &[]
    ).await.unwrap();
    println!("✓ Valid value 99999999.99 inserted successfully");
    
    // This should fail: 8 digits before decimal, 3 after (too many decimal places)
    let result = client.execute(
        "INSERT INTO products VALUES (2, 99999999.999)",
        &[]
    ).await;
    
    match &result {
        Ok(_) => panic!("✗ ERROR: Value 99999999.999 was accepted but should have been rejected (too many decimal places)"),
        Err(e) => {
            println!("✓ Value 99999999.999 correctly rejected: {e}");
            println!("  Error code: {:?}", e.code());
        }
    }
    
    // This should fail: 9 digits before decimal exceeds precision-scale limit
    let result2 = client.execute(
        "INSERT INTO products VALUES (3, 999999999.99)",
        &[]
    ).await;
    
    match &result2 {
        Ok(_) => println!("✗ ERROR: Value 999999999.99 was accepted but should have been rejected (too many total digits)"),
        Err(e) => {
            println!("✓ Value 999999999.99 correctly rejected: {e}");
            println!("  Error code: {:?}", e.code());
        }
    }
    
    // Check what values were actually inserted
    let rows = client.query("SELECT id, price::text FROM products ORDER BY id", &[]).await.unwrap();
    println!("\nValues in table:");
    for row in rows {
        let id: i32 = row.get(0);
        let price: String = row.get(1);
        println!("  id={id}, price={price}");
    }
    
    // Check trigger
    let triggers = client.query(
        "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND tbl_name = 'products' AND name LIKE '%numeric%'",
        &[]
    ).await.unwrap();
    
    if !triggers.is_empty() {
        println!("\nTrigger SQL:");
        for row in triggers {
            let sql: String = row.get(0);
            println!("{sql}");
        }
    } else {
        println!("\nNo numeric triggers found!");
    }
    
    server.abort();
}