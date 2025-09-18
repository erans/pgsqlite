mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_information_schema_simple() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server().await;
    let client = &server.client;

    // First, let's test if we can query a known working catalog table
    println!("Testing pg_class query...");
    let pg_class_rows = client.query("SELECT relname FROM pg_class LIMIT 1", &[]).await;
    println!("pg_class result: {:?}", pg_class_rows);

    // Now test information_schema.schemata
    println!("Testing information_schema.schemata...");
    match client.query("SELECT * FROM information_schema.schemata", &[]).await {
        Ok(rows) => {
            println!("✓ information_schema.schemata succeeded: {} rows", rows.len());
            for (i, row) in rows.iter().enumerate() {
                println!("  Row {}: {} columns", i, row.len());
                for j in 0..row.len() {
                    if let Ok(val) = row.try_get::<_, Option<String>>(j) {
                        println!("    Column {}: {:?}", j, val);
                    }
                }
            }
        }
        Err(e) => {
            println!("✗ information_schema.schemata failed: {:?}", e);
        }
    }

    // Test information_schema.tables
    println!("Testing information_schema.tables...");
    match client.query("SELECT table_name FROM information_schema.tables LIMIT 5", &[]).await {
        Ok(rows) => {
            println!("✓ information_schema.tables succeeded: {} rows", rows.len());
            for (i, row) in rows.iter().enumerate() {
                println!("  Row {}: {} columns", i, row.len());
                if let Ok(name) = row.try_get::<_, String>(0) {
                    println!("    Table: {}", name);
                }
            }
        }
        Err(e) => {
            println!("✗ information_schema.tables failed: {:?}", e);
        }
    }
}