mod common;
use common::setup_test_server_with_init;

#[tokio::test]
async fn test_catalog_text_format() {
    let _ = env_logger::builder().is_test(true).try_init();
    
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY)").await?;
            Ok(())
        })
    }).await;

    let client = &server.client;

    // Test 1: Force text format by creating a prepared statement
    eprintln!("\n=== Test 1: Prepared statement with text format ===");
    let stmt = match client.prepare("SELECT relname FROM pg_catalog.pg_class WHERE relkind = 'r'").await {
        Ok(s) => {
            eprintln!("✓ Prepare succeeded, columns: {:?}", s.columns());
            s
        }
        Err(e) => {
            eprintln!("✗ Prepare failed: {:?}", e);
            panic!("Prepare should work!");
        }
    };

    // Execute the prepared statement
    match client.query(&stmt, &[]).await {
        Ok(rows) => {
            eprintln!("✓ Query succeeded: {} rows", rows.len());
            for row in &rows {
                let name: &str = row.get(0);
                eprintln!("  Table: {}", name);
            }
        }
        Err(e) => {
            eprintln!("✗ Query failed: {:?}", e);
            panic!("Query should work!");
        }
    }

    server.abort();
}