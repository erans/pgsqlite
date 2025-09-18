mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_schemata_only() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server().await;
    let client = &server.client;

    // Test just a simple query first
    println!("Testing simple SELECT 'hello'...");
    match client.query("SELECT 'hello' as greeting", &[]).await {
        Ok(rows) => {
            println!("✓ Simple query worked: {} rows", rows.len());
            if !rows.is_empty() {
                let greeting: &str = rows[0].get(0);
                println!("  Greeting: {}", greeting);
            }
        }
        Err(e) => {
            println!("✗ Simple query failed: {:?}", e);
        }
    }

    // Test schemata with specific column
    println!("Testing schemata with specific column...");
    match client.query("SELECT schema_name FROM information_schema.schemata", &[]).await {
        Ok(rows) => {
            println!("✓ Specific column query worked: {} rows", rows.len());
            for row in &rows {
                let schema: &str = row.get(0);
                println!("  Schema: {}", schema);
            }
        }
        Err(e) => {
            println!("✗ Specific column query failed: {:?}", e);
        }
    }

    // Test schemata with wildcard
    println!("Testing schemata with wildcard...");
    match client.query("SELECT * FROM information_schema.schemata", &[]).await {
        Ok(rows) => {
            println!("✓ Wildcard query worked: {} rows", rows.len());
        }
        Err(e) => {
            println!("✗ Wildcard query failed: {:?}", e);
        }
    }
}