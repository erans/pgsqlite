mod common;
use common::setup_test_server_with_init;

#[tokio::test]
async fn test_catalog_basic_functionality() {
    let _ = env_logger::builder().is_test(true).try_init();
    
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)").await?;
            Ok(())
        })
    }).await;

    let client = &server.client;

    // Test 1: Simple 2-column query
    println!("\n=== Test 1: Simple 2-column query ===");
    match client.query(
        "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relkind = 'r'",
        &[]
    ).await {
        Ok(rows) => {
            println!("✓ Query succeeded: {} rows", rows.len());
            assert!(!rows.is_empty(), "Should find at least 1 table");
            for row in &rows {
                let relname: &str = row.get(0);
                let relkind: &str = row.get(1);
                println!("  Table: {relname}, Kind: {relkind}");
                assert_eq!(relkind, "r", "Should only return tables");
            }
        }
        Err(e) => {
            eprintln!("✗ Query failed: {e:?}");
            panic!("Basic catalog query should work!");
        }
    }

    // Test 2: SELECT * query
    println!("\n=== Test 2: SELECT * query ===");
    match client.query(
        "SELECT * FROM pg_catalog.pg_class WHERE relkind = 'r' LIMIT 1",
        &[]
    ).await {
        Ok(rows) => {
            println!("✓ SELECT * succeeded: {} rows", rows.len());
            if !rows.is_empty() {
                println!("  Row has {} columns", rows[0].len());
                assert_eq!(rows[0].len(), 33, "pg_class should have 33 columns");
            }
        }
        Err(e) => {
            eprintln!("✗ SELECT * failed: {e:?}");
            panic!("SELECT * should work!");
        }
    }
    
    server.abort();
}