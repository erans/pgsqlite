mod common;
use common::setup_test_server_with_init;

#[tokio::test]
async fn test_pg_class_all_columns_isolated() {
    // Initialize logging
    let _ = env_logger::builder().is_test(true).try_init();
    
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create a test table
            db.execute("CREATE TABLE test_all_cols (id INTEGER PRIMARY KEY, name TEXT)").await?;
            Ok(())
        })
    }).await;

    let client = &server.client;

    // First test listing all columns explicitly (33 columns in current PostgreSQL)
    let all_cols = "oid, relname, relnamespace, reltype, reloftype, relowner, relam, relfilenode, \
                    reltablespace, relpages, reltuples, relallvisible, reltoastrelid, relhasindex, \
                    relisshared, relpersistence, relkind, relnatts, relchecks, \
                    relhasrules, relhastriggers, relhassubclass, relrowsecurity, \
                    relforcerowsecurity, relispopulated, relreplident, relispartition, \
                    relrewrite, relfrozenxid, relminmxid, relacl, reloptions, relpartbound";
    
    println!("Testing with all columns listed explicitly...");
    println!("Query: SELECT {} FROM pg_catalog.pg_class WHERE relkind = 'r'", all_cols);
    match client.query(
        &format!("SELECT {} FROM pg_catalog.pg_class WHERE relkind = 'r'", all_cols),
        &[]
    ).await {
        Ok(rows) => {
            println!("Explicit columns works! Got {} rows with {} columns", 
                    rows.len(), if !rows.is_empty() { rows[0].len() } else { 0 });
        }
        Err(e) => {
            println!("Explicit columns failed: {:?}", e);
        }
    }
    
    // Test with SELECT * - this should work
    println!("\nTesting with SELECT *...");
    println!("Query: SELECT * FROM pg_catalog.pg_class WHERE relkind = 'r'");
    let rows = client.query(
        "SELECT * FROM pg_catalog.pg_class WHERE relkind = 'r'",
        &[]
    ).await.unwrap();
    
    println!("Found {} tables", rows.len());
    assert!(rows.len() >= 1, "Should find at least 1 table");
    
    // Check that we get all columns
    if !rows.is_empty() {
        println!("Row has {} columns", rows[0].len());
        assert_eq!(rows[0].len(), 33, "pg_class should have 33 columns");
    }
    
    server.abort();
}