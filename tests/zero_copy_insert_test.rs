#[cfg(test)]
mod tests {
    use pgsqlite::query::QueryExecutor;
    use pgsqlite::session::DbHandler;
    use pgsqlite::protocol::PostgresCodec;
    use tokio_util::codec::Framed;
    use tokio::net::{TcpListener, TcpStream};
    use std::sync::Arc;
    use std::time::Instant;
    
    #[tokio::test]
    #[ignore] // Run with: cargo test zero_copy_insert_test -- --ignored --nocapture --features zero-copy-protocol
    async fn test_insert_with_zero_copy_optimization() {
        println!("\n=== Testing INSERT with Zero-Copy Optimization ===\n");
        
        // Create test database
        let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
        
        // Create test table
        db_handler.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)").await.unwrap();
        
        // Set up test socket pair
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream
        });
        
        let client = TcpStream::connect(addr).await.unwrap();
        let server = server_task.await.unwrap();
        
        // Create framed connection
        let codec = PostgresCodec::new();
        let mut framed = Framed::new(server, codec);
        
        // Test with zero-copy disabled
        unsafe { std::env::set_var("PGSQLITE_ZERO_COPY", "0"); }
        
        println!("Testing INSERT performance WITHOUT zero-copy optimization:");
        let start = Instant::now();
        
        for i in 0..1000 {
            let sql = format!("INSERT INTO test (id, data) VALUES ({}, 'test data')", i);
            QueryExecutor::execute_query(&mut framed, &db_handler, &sql).await.unwrap();
        }
        
        let elapsed_without = start.elapsed();
        println!("  Time for 1000 INSERTs: {:?}", elapsed_without);
        println!("  Per INSERT: {:?}", elapsed_without / 1000);
        
        // Test with zero-copy enabled
        unsafe { std::env::set_var("PGSQLITE_ZERO_COPY", "1"); }
        
        println!("\nTesting INSERT performance WITH zero-copy optimization:");
        let start = Instant::now();
        
        for i in 1000..2000 {
            let sql = format!("INSERT INTO test (id, data) VALUES ({}, 'test data')", i);
            QueryExecutor::execute_query(&mut framed, &db_handler, &sql).await.unwrap();
        }
        
        let elapsed_with = start.elapsed();
        println!("  Time for 1000 INSERTs: {:?}", elapsed_with);
        println!("  Per INSERT: {:?}", elapsed_with / 1000);
        
        // Calculate improvement
        let improvement = elapsed_without.as_secs_f64() / elapsed_with.as_secs_f64();
        println!("\nPerformance improvement: {:.2}x", improvement);
        
        // Verify data
        let result = db_handler.query("SELECT COUNT(*) FROM test").await.unwrap();
        assert_eq!(result.rows[0][0].as_ref().unwrap(), b"2000");
        
        println!("\nExpected improvements:");
        println!("  - Reduced allocations for command tags");
        println!("  - No BackendMessage enum allocation");
        println!("  - Direct buffer writing");
        
        drop(client);
    }
    
    #[tokio::test]
    #[cfg(feature = "zero-copy-protocol")]
    async fn test_zero_copy_dml_operations() {
        use pgsqlite::query::{QueryExecutorZeroCopy, should_use_zero_copy};
        
        println!("\n=== Testing Zero-Copy DML Operations ===\n");
        
        // Enable zero-copy
        unsafe { std::env::set_var("PGSQLITE_ZERO_COPY", "1"); }
        assert!(should_use_zero_copy());
        
        // Create test database
        let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
        
        // Create test table
        db_handler.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)").await.unwrap();
        
        // Set up test connection
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream
        });
        
        let client = TcpStream::connect(addr).await.unwrap();
        let server = server_task.await.unwrap();
        
        let codec = PostgresCodec::new();
        let mut framed = Framed::new(server, codec);
        
        // Test INSERT with 0 rows (edge case)
        println!("Testing INSERT with 0 rows affected...");
        // This would happen with INSERT ... WHERE false
        let result = QueryExecutor::execute_dml_optimized(&mut framed, &db_handler, 
            "INSERT INTO test SELECT 1, 1 WHERE 1=0").await;
        assert!(result.is_ok());
        
        // Test INSERT with 1 row (most common case)
        println!("Testing INSERT with 1 row affected...");
        let result = QueryExecutor::execute_dml_optimized(&mut framed, &db_handler, 
            "INSERT INTO test (id, value) VALUES (1, 100)").await;
        assert!(result.is_ok());
        
        // Test UPDATE with 0 rows
        println!("Testing UPDATE with 0 rows affected...");
        let result = QueryExecutor::execute_dml_optimized(&mut framed, &db_handler, 
            "UPDATE test SET value = 200 WHERE id = 999").await;
        assert!(result.is_ok());
        
        // Test UPDATE with 1 row
        println!("Testing UPDATE with 1 row affected...");
        let result = QueryExecutor::execute_dml_optimized(&mut framed, &db_handler, 
            "UPDATE test SET value = 200 WHERE id = 1").await;
        assert!(result.is_ok());
        
        // Test DELETE with 0 rows
        println!("Testing DELETE with 0 rows affected...");
        let result = QueryExecutor::execute_dml_optimized(&mut framed, &db_handler, 
            "DELETE FROM test WHERE id = 999").await;
        assert!(result.is_ok());
        
        // Test DELETE with 1 row
        println!("Testing DELETE with 1 row affected...");
        let result = QueryExecutor::execute_dml_optimized(&mut framed, &db_handler, 
            "DELETE FROM test WHERE id = 1").await;
        assert!(result.is_ok());
        
        println!("\nAll zero-copy DML operations completed successfully!");
        println!("These operations use pre-allocated strings for common cases (0 or 1 affected rows)");
        
        drop(client);
    }
}