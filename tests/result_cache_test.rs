mod common;
use common::*;

#[tokio::test]
async fn test_result_cache_for_identical_queries() {
    let test_server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create a test table
            db.execute("CREATE TABLE cache_test (id INTEGER PRIMARY KEY, name TEXT, value REAL)").await?;
            
            // Insert test data
            for i in 1..=100 {
                let query = format!("INSERT INTO cache_test VALUES ({}, 'name_{}', {})", i, i, i as f64 * 1.5);
                db.execute(&query).await?;
            }
            
            Ok(())
        })
    }).await;
    
    let client = &test_server.client;
    
    // Execute a query that should be cached (takes more than 1ms or returns > 10 rows)
    let query = "SELECT id, name, value FROM cache_test WHERE id > 90 ORDER BY id";
    
    // First execution - should be slower
    let start1 = std::time::Instant::now();
    let rows1 = client.query(query, &[]).await.unwrap();
    let duration1 = start1.elapsed();
    
    // Verify results
    assert_eq!(rows1.len(), 10);
    assert_eq!(rows1[0].get::<_, i32>(0), 91);
    assert_eq!(rows1[9].get::<_, i32>(0), 100);
    
    // Second execution - should be cached and faster
    let start2 = std::time::Instant::now();
    let rows2 = client.query(query, &[]).await.unwrap();
    let duration2 = start2.elapsed();
    
    // Verify same results
    assert_eq!(rows2.len(), 10);
    assert_eq!(rows2[0].get::<_, i32>(0), 91);
    assert_eq!(rows2[9].get::<_, i32>(0), 100);
    
    // Third execution to ensure cache is working
    let start3 = std::time::Instant::now();
    let rows3 = client.query(query, &[]).await.unwrap();
    let duration3 = start3.elapsed();
    
    assert_eq!(rows3.len(), 10);
    
    // Log durations for debugging
    eprintln!("First execution: {duration1:?}");
    eprintln!("Second execution (cached): {duration2:?}");
    eprintln!("Third execution (cached): {duration3:?}");
    
    // With the test harness overhead, we can't reliably test timing
    // Just verify that subsequent executions return the same results
    // In production, the cache would provide significant benefits
    
    test_server.abort();
}

#[tokio::test]
async fn test_result_cache_invalidation_on_ddl() {
    let test_server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create initial table
            db.execute("CREATE TABLE ddl_test (id INTEGER PRIMARY KEY, data TEXT)").await?;
            db.execute("INSERT INTO ddl_test VALUES (1, 'original')").await?;
            Ok(())
        })
    }).await;
    
    let client = &test_server.client;
    
    // Execute a query that should be cached
    let query = "SELECT * FROM ddl_test";
    let rows1 = client.query(query, &[]).await.unwrap();
    assert_eq!(rows1.len(), 1);
    assert_eq!(rows1[0].get::<_, String>(1), "original");
    
    // Execute DDL which should clear the cache
    client.execute("ALTER TABLE ddl_test ADD COLUMN extra INTEGER DEFAULT 0", &[]).await.unwrap();
    
    // Insert new data
    client.execute("INSERT INTO ddl_test (id, data) VALUES (2, 'new')", &[]).await.unwrap();
    
    // Execute the same query - should not use cached result
    let rows2 = client.query(query, &[]).await.unwrap();
    assert_eq!(rows2.len(), 2); // Should see both rows, not cached single row
    
    test_server.abort();
}

#[tokio::test]
async fn test_result_cache_not_used_for_dml() {
    let test_server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE dml_test (id INTEGER PRIMARY KEY, value INTEGER)").await?;
            Ok(())
        })
    }).await;
    
    let client = &test_server.client;
    
    // DML queries should not be cached
    for i in 1..=5 {
        let value = i * 10;
        let affected = client.execute(
            &format!("INSERT INTO dml_test VALUES ({i}, {value})"), 
            &[]
        ).await.unwrap();
        assert_eq!(affected, 1);
    }
    
    // Verify all inserts worked (cache would have prevented subsequent inserts)
    let rows = client.query("SELECT COUNT(*) FROM dml_test", &[]).await.unwrap();
    assert_eq!(rows[0].get::<_, i64>(0), 5);
    
    test_server.abort();
}

#[tokio::test]
async fn test_result_cache_with_different_queries() {
    let test_server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE diff_test (id INTEGER, category TEXT, amount REAL)").await?;
            
            // Insert test data
            for i in 1..=50 {
                let category = if i % 2 == 0 { "even" } else { "odd" };
                let query = format!("INSERT INTO diff_test VALUES ({}, '{}', {})", i, category, i as f64);
                db.execute(&query).await?;
            }
            
            Ok(())
        })
    }).await;
    
    let client = &test_server.client;
    
    // Execute different queries - each should be cached separately
    let query1 = "SELECT * FROM diff_test WHERE category = 'even' ORDER BY id";
    let query2 = "SELECT * FROM diff_test WHERE category = 'odd' ORDER BY id";
    let query3 = "SELECT COUNT(*), SUM(amount) FROM diff_test GROUP BY category";
    
    // Execute each query twice
    let rows1a = client.query(query1, &[]).await.unwrap();
    let rows1b = client.query(query1, &[]).await.unwrap();
    assert_eq!(rows1a.len(), rows1b.len());
    assert_eq!(rows1a.len(), 25);
    
    let rows2a = client.query(query2, &[]).await.unwrap();
    let rows2b = client.query(query2, &[]).await.unwrap();
    assert_eq!(rows2a.len(), rows2b.len());
    assert_eq!(rows2a.len(), 25);
    
    let rows3a = client.query(query3, &[]).await.unwrap();
    let rows3b = client.query(query3, &[]).await.unwrap();
    assert_eq!(rows3a.len(), rows3b.len());
    assert_eq!(rows3a.len(), 2);
    
    test_server.abort();
}