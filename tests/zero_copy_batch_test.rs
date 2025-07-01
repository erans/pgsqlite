#[cfg(test)]
#[cfg(feature = "zero-copy-protocol")]
mod tests {
    use pgsqlite::query::{QueryExecutor, QueryExecutorBatch, BatchConfig};
    use pgsqlite::session::DbHandler;
    use pgsqlite::protocol::PostgresCodec;
    use tokio_util::codec::Framed;
    use tokio::net::{TcpListener, TcpStream};
    use std::sync::Arc;
    use std::time::Instant;
    
    #[tokio::test]
    #[ignore] // Run with: cargo test zero_copy_batch_test -- --ignored --nocapture --features zero-copy-protocol
    async fn test_select_with_batching() {
        println!("\n=== Testing SELECT with Message Batching ===\n");
        
        // Create test database
        let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
        
        // Create test table with many rows
        db_handler.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)").await.unwrap();
        
        // Insert test data
        println!("Inserting 10,000 test rows...");
        for i in 0..10000 {
            let sql = format!("INSERT INTO test (id, data) VALUES ({}, 'Row data {}')", i, i);
            db_handler.execute(&sql).await.unwrap();
        }
        
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
        
        // Test different batch sizes
        let batch_sizes = [1, 10, 100, 1000];
        
        for &batch_size in &batch_sizes {
            println!("\nTesting SELECT with batch size {}:", batch_size);
            let start = Instant::now();
            
            QueryExecutor::execute_select_batched(
                &mut framed,
                &db_handler,
                "SELECT * FROM test LIMIT 1000",
                batch_size
            ).await.unwrap();
            
            let elapsed = start.elapsed();
            println!("  Time: {:?}", elapsed);
            println!("  Flushes: {}", (1000 + batch_size - 1) / batch_size);
        }
        
        println!("\nExpected benefits of batching:");
        println!("  - Reduced syscall overhead");
        println!("  - Better network utilization");
        println!("  - Improved throughput for large result sets");
        
        drop(client);
    }
    
    #[tokio::test]
    async fn test_batch_config() {
        println!("\n=== Testing Batch Configuration ===\n");
        
        // Test default config
        let default_config = BatchConfig::default();
        println!("Default config: {:?}", default_config);
        assert_eq!(default_config.row_batch_size, 100);
        assert!(default_config.enabled);
        
        // Test environment variable configuration
        std::env::set_var("PGSQLITE_BATCH_SIZE", "500");
        std::env::set_var("PGSQLITE_BATCH_ENABLED", "1");
        
        let env_config = BatchConfig::from_env();
        println!("Environment config: {:?}", env_config);
        assert_eq!(env_config.row_batch_size, 500);
        assert!(env_config.enabled);
        
        // Test disabling batching
        std::env::set_var("PGSQLITE_BATCH_ENABLED", "0");
        let disabled_config = BatchConfig::from_env();
        println!("Disabled config: {:?}", disabled_config);
        assert!(!disabled_config.enabled);
        
        // Clean up
        std::env::remove_var("PGSQLITE_BATCH_SIZE");
        std::env::remove_var("PGSQLITE_BATCH_ENABLED");
    }
    
    #[tokio::test]
    #[ignore]
    async fn benchmark_batching_impact() {
        println!("\n=== Benchmarking Batching Impact ===\n");
        
        let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
        
        // Create large dataset
        db_handler.execute("CREATE TABLE bench (id INTEGER, value REAL, text TEXT)").await.unwrap();
        
        println!("Creating 50,000 row dataset...");
        for i in 0..50000 {
            if i % 10000 == 0 {
                println!("  {} rows inserted", i);
            }
            let sql = format!(
                "INSERT INTO bench VALUES ({}, {}.{}, 'Text value for row {}')", 
                i, i, i % 100, i
            );
            db_handler.execute(&sql).await.unwrap();
        }
        
        // Set up connection
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
        
        // Benchmark with batch size 1 (no batching)
        println!("\nBenchmarking without batching (batch_size=1):");
        let start = Instant::now();
        QueryExecutor::execute_select_batched(
            &mut framed,
            &db_handler,
            "SELECT * FROM bench",
            1
        ).await.unwrap();
        let no_batch_time = start.elapsed();
        println!("  Time: {:?}", no_batch_time);
        println!("  Rows/sec: {:.0}", 50000.0 / no_batch_time.as_secs_f64());
        
        // Benchmark with optimal batching
        println!("\nBenchmarking with batching (batch_size=1000):");
        let start = Instant::now();
        QueryExecutor::execute_select_batched(
            &mut framed,
            &db_handler,
            "SELECT * FROM bench",
            1000
        ).await.unwrap();
        let batch_time = start.elapsed();
        println!("  Time: {:?}", batch_time);
        println!("  Rows/sec: {:.0}", 50000.0 / batch_time.as_secs_f64());
        
        let speedup = no_batch_time.as_secs_f64() / batch_time.as_secs_f64();
        println!("\nBatching speedup: {:.2}x", speedup);
        
        drop(client);
    }
}