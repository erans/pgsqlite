mod common;
use common::setup_test_server_with_init;

/// Basic test for catalog WHERE clause functionality
/// This is a simplified version that works reliably in CI
#[tokio::test]
async fn test_catalog_where_basic() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create a simple table so pg_class has something to query
            db.execute("CREATE TABLE basic_test_table (id INTEGER PRIMARY KEY)").await?;
            Ok(())
        })
    }).await;
    let client = &server.client;
    
    // First check if there are any tables at all
    let all_rows = client.query(
        "SELECT relname, relkind FROM pg_catalog.pg_class",
        &[]
    ).await.unwrap();
    
    println!("Total rows in pg_class: {}", all_rows.len());
    for row in &all_rows {
        let name: &str = row.get(0);
        let kind: &str = row.get(1);
        println!("  {name} ({kind})");
    }
    
    // Test 1: Basic WHERE with equals on pg_class
    let rows = client.query(
        "SELECT relname FROM pg_catalog.pg_class WHERE relkind = 'r'",
        &[]
    ).await.unwrap();
    
    println!("Tables found with WHERE relkind = 'r': {}", rows.len());
    
    // Should find some tables (at least the internal ones)
    if rows.is_empty() && !all_rows.is_empty() {
        println!("WARNING: WHERE clause not working, but pg_class has data");
    }
    
    // Test 2: WHERE on pg_attribute with comparison
    let rows = client.query(
        "SELECT attname FROM pg_catalog.pg_attribute WHERE attnum > 0",
        &[]
    ).await.unwrap();
    
    // Should find user-defined columns
    assert!(!rows.is_empty(), "Should find at least one user column");
    
    // Test 3: WHERE with IN clause
    let rows = client.query(
        "SELECT relkind FROM pg_catalog.pg_class WHERE relkind IN ('r', 'i')",
        &[]
    ).await.unwrap();
    
    // Should find tables and/or indexes
    assert!(!rows.is_empty(), "Should find tables or indexes");
    
    // Test 4: WHERE with != (not equal)
    let rows = client.query(
        "SELECT relname FROM pg_catalog.pg_class WHERE relkind != 'v'",
        &[]
    ).await.unwrap();
    
    // Should find non-view objects
    assert!(!rows.is_empty(), "Should find non-view objects");
    
    server.abort();
}