mod common;
use common::setup_test_server_with_init;

#[tokio::test]
async fn string_format_test() {
    let _ = env_logger::builder().is_test(true).try_init();
    
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY)").await?;
            Ok(())
        })
    }).await;

    let client = &server.client;

    // Test 1: Multi-line string with backslashes (like in failing test)
    let all_cols_multiline = "oid, relname, relnamespace, reltype, relowner, relam, relfilenode, \
                    reltablespace, relpages, reltuples, reltoastrelid, relhasindex, \
                    relisshared, relpersistence, relkind, relnatts, relchecks, \
                    relhasrules, relhastriggers, relhassubclass, relrowsecurity, \
                    relforcerowsecurity, relispopulated, relreplident, relispartition, \
                    relrewrite, relfrozenxid, relminmxid";
    
    // Test 2: Single line string  
    let all_cols_single = "oid, relname, relnamespace, reltype, relowner, relam, relfilenode, reltablespace, relpages, reltuples, reltoastrelid, relhasindex, relisshared, relpersistence, relkind, relnatts, relchecks, relhasrules, relhastriggers, relhassubclass, relrowsecurity, relforcerowsecurity, relispopulated, relreplident, relispartition, relrewrite, relfrozenxid, relminmxid";
    
    eprintln!("\n=== String comparison ===");
    eprintln!("Multiline string length: {}", all_cols_multiline.len());
    eprintln!("Single line string length: {}", all_cols_single.len());
    eprintln!("Strings equal: {}", all_cols_multiline == all_cols_single);
    
    if all_cols_multiline != all_cols_single {
        eprintln!("\nDifference found!");
        // Print character by character
        for (i, (c1, c2)) in all_cols_multiline.chars().zip(all_cols_single.chars()).enumerate() {
            if c1 != c2 {
                eprintln!("First difference at position {}: '{}' vs '{}'", i, c1 as u32, c2 as u32);
                break;
            }
        }
    }
    
    eprintln!("\n=== Test 1: Multiline string query ===");
    match client.query(
        &format!("SELECT {} FROM pg_catalog.pg_class WHERE relkind = 'r'", all_cols_multiline),
        &[]
    ).await {
        Ok(rows) => eprintln!("✓ Multiline query succeeded: {} rows", rows.len()),
        Err(e) => eprintln!("✗ Multiline query failed: {:?}", e),
    }
    
    eprintln!("\n=== Test 2: Single line string query ===");
    match client.query(
        &format!("SELECT {} FROM pg_catalog.pg_class WHERE relkind = 'r'", all_cols_single),
        &[]
    ).await {
        Ok(rows) => eprintln!("✓ Single line query succeeded: {} rows", rows.len()),
        Err(e) => eprintln!("✗ Single line query failed: {:?}", e),
    }
    
    server.abort();
}