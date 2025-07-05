mod common;
use common::setup_test_server_with_init;

#[tokio::test]
async fn test_catalog_query_projection() {
    let _ = env_logger::builder().is_test(true).try_init();
    
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY)").await?;
            Ok(())
        })
    }).await;

    let client = &server.client;

    // Test 1: Simple catalog query with just 2 columns
    eprintln!("\n=== Test 1: Two columns ===");
    match client.query("SELECT relname, relkind FROM pg_catalog.pg_class WHERE relkind = 'r'", &[]).await {
        Ok(rows) => {
            eprintln!("✓ Success! Got {} rows", rows.len());
            if !rows.is_empty() {
                eprintln!("  Row has {} columns", rows[0].len());
                let name: &str = rows[0].get(0);
                let kind: &str = rows[0].get(1);
                eprintln!("  relname: {}, relkind: {}", name, kind);
            }
        }
        Err(e) => {
            eprintln!("✗ Failed: {:?}", e);
            panic!("Two column query should work!");
        }
    }

    // Test 2: Try with simple protocol to compare
    eprintln!("\n=== Test 2: Same query with simple protocol ===");
    match client.simple_query("SELECT relname, relkind FROM pg_catalog.pg_class WHERE relkind = 'r'").await {
        Ok(messages) => {
            eprintln!("✓ Simple protocol works");
            let mut row_count = 0;
            for msg in &messages {
                if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                    row_count += 1;
                    eprintln!("  Row {} columns: relname={}, relkind={}", 
                        row.len(),
                        row.get("relname").unwrap_or("?"),
                        row.get("relkind").unwrap_or("?"));
                }
            }
            eprintln!("  Total rows: {}", row_count);
        }
        Err(e) => {
            eprintln!("✗ Simple protocol failed: {:?}", e);
        }
    }

    server.abort();
}