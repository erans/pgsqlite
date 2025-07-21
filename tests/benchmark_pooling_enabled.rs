use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

/// Test connection pooling performance by connecting through the PostgreSQL protocol
/// This will test the actual connection pooling implementation via lib.rs

#[tokio::test]
async fn test_pooled_concurrent_reads() {
    // Enable connection pooling
    unsafe { env::set_var("PGSQLITE_USE_POOLING", "true"); }
    
    println!("🧪 Testing concurrent reads WITH connection pooling enabled");
    
    // Start test server with pooling enabled
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        // Set up test data
        db_handler.execute("
            CREATE TABLE IF NOT EXISTS pooling_test (
                id INTEGER PRIMARY KEY,
                value INTEGER NOT NULL,
                description TEXT
            )
        ").await.unwrap();
        
        for i in 1..=100 {
            db_handler.execute(&format!(
                "INSERT INTO pooling_test (id, value, description) VALUES ({}, {}, 'test_{}')",
                i, i * 10, i
            )).await.unwrap();
        }
        
        // Accept multiple connections
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    let db_clone = db_handler.clone();
                    tokio::spawn(async move {
                        if let Err(e) = pgsqlite::handle_test_connection_with_pool(stream, addr, db_clone).await {
                            eprintln!("Connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("Accept error: {}", e);
                    break;
                }
            }
        }
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Connect multiple clients concurrently
    let start = Instant::now();
    let mut tasks = Vec::new();
    
    for _i in 0..4 {
        let task = tokio::spawn(async move {
            let (client, connection) = tokio_postgres::connect(
                &format!("host=127.0.0.1 port={} dbname=test user=test", port),
                NoTls,
            ).await.unwrap();
            
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });
            
            let mut query_count = 0;
            let end_time = Instant::now() + Duration::from_secs(2);
            
            while Instant::now() < end_time {
                match client.query("SELECT COUNT(*) FROM pooling_test", &[]).await {
                    Ok(_) => query_count += 1,
                    Err(e) => eprintln!("Query error: {}", e),
                }
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
            
            query_count
        });
        tasks.push(task);
    }
    
    let mut total_queries = 0;
    for task in tasks {
        total_queries += task.await.unwrap();
    }
    
    let duration = start.elapsed();
    let qps = total_queries as f64 / duration.as_secs_f64();
    
    println!("📊 Pooled Results:");
    println!("  Total queries: {}", total_queries);
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!("  QPS: {:.0}", qps);
    
    server_handle.abort();
    
    // Clean up environment variable
    unsafe { env::remove_var("PGSQLITE_USE_POOLING"); }
    
    assert!(total_queries > 10, "Should execute at least 10 queries with pooling");
    
    // Note: We don't have baseline comparison here, but we can manually compare
    // the QPS with the baseline test results
}

#[tokio::test]
async fn test_pooled_mixed_workload() {
    // Enable connection pooling
    unsafe { env::set_var("PGSQLITE_USE_POOLING", "true"); }
    
    println!("🧪 Testing mixed workload WITH connection pooling enabled");
    
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        // Set up test data
        db_handler.execute("
            CREATE TABLE IF NOT EXISTS mixed_test (
                id INTEGER PRIMARY KEY,
                counter INTEGER NOT NULL DEFAULT 0
            )
        ").await.unwrap();
        
        for i in 1..=20 {
            db_handler.execute(&format!(
                "INSERT INTO mixed_test (id, counter) VALUES ({}, {})",
                i, 0
            )).await.unwrap();
        }
        
        // Accept multiple connections
        for _ in 0..3 {
            let (stream, addr) = listener.accept().await.unwrap();
            let db = db_handler.clone();
            tokio::spawn(async move {
                pgsqlite::handle_test_connection_with_pool(stream, addr, db).await.unwrap();
            });
        }
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    
    let start = Instant::now();
    let mut tasks = Vec::new();
    
    // 2 read tasks + 1 write task
    for i in 0..3 {
        let is_writer = i == 2;
        
        let task = tokio::spawn(async move {
            let (client, connection) = tokio_postgres::connect(
                &format!("host=127.0.0.1 port={} dbname=test user=test", port),
                NoTls,
            ).await.unwrap();
            
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });
            
            let mut count = 0;
            let end_time = Instant::now() + Duration::from_secs(2);
            
            while Instant::now() < end_time {
                let result = if is_writer {
                    let new_value = count % 1000;
                    client.execute(&format!("UPDATE mixed_test SET counter = {} WHERE id = 1", new_value), &[]).await.map(|_| ())
                } else {
                    client.query("SELECT id, counter FROM mixed_test WHERE id <= 10", &[]).await.map(|_| ())
                };
                
                match result {
                    Ok(_) => count += 1,
                    Err(e) => eprintln!("Operation error: {}", e),
                }
                
                tokio::time::sleep(Duration::from_micros(if is_writer { 1000 } else { 100 })).await;
            }
            
            (count, is_writer)
        });
        tasks.push(task);
    }
    
    let mut total_reads = 0;
    let mut total_writes = 0;
    
    for task in tasks {
        let (count, is_writer) = task.await.unwrap();
        if is_writer {
            total_writes += count;
        } else {
            total_reads += count;
        }
    }
    
    let duration = start.elapsed();
    let total_ops = total_reads + total_writes;
    let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
    
    println!("📊 Pooled Mixed Workload Results:");
    println!("  Read operations: {}", total_reads);
    println!("  Write operations: {}", total_writes);
    println!("  Total operations: {}", total_ops);
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!("  Operations/sec: {:.0}", ops_per_sec);
    
    server_handle.abort();
    
    // Clean up environment variable
    unsafe { env::remove_var("PGSQLITE_USE_POOLING"); }
    
    assert!(total_reads > 10, "Should have substantial read operations with pooling");
    assert!(total_writes > 10, "Should have some write operations");
}

#[tokio::test]
async fn test_pooling_effectiveness() {
    println!("🧪 Comparing pooled vs non-pooled performance");
    
    // Test without pooling
    unsafe { env::remove_var("PGSQLITE_USE_POOLING"); }
    let baseline_qps = run_read_benchmark().await;
    
    // Test with pooling
    unsafe { env::set_var("PGSQLITE_USE_POOLING", "true"); }
    let pooled_qps = run_read_benchmark().await;
    
    // Clean up
    unsafe { env::remove_var("PGSQLITE_USE_POOLING"); }
    
    println!("📊 Performance Comparison:");
    println!("  Baseline (no pooling): {:.0} QPS", baseline_qps);
    println!("  With pooling: {:.0} QPS", pooled_qps);
    
    let improvement = ((pooled_qps - baseline_qps) / baseline_qps) * 100.0;
    println!("  Performance change: {:.1}%", improvement);
    
    // We expect some performance difference, but both should work
    assert!(baseline_qps > 1.0, "Baseline should have reasonable performance");
    assert!(pooled_qps > 1.0, "Pooled should have reasonable performance");
    
    if improvement > 5.0 {
        println!("✅ Connection pooling shows improvement!");
    } else if improvement < -10.0 {
        println!("⚠️  Connection pooling shows significant overhead");
    } else {
        println!("ℹ️  Connection pooling performance is comparable to baseline");
    }
}

async fn run_read_benchmark() -> f64 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        db_handler.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, val INTEGER)").await.unwrap();
        for i in 1..=50 {
            db_handler.execute(&format!("INSERT INTO bench VALUES ({}, {})", i, i * 2)).await.unwrap();
        }
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} dbname=test user=test", port),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    
    let start = Instant::now();
    let mut query_count = 0;
    let end_time = Instant::now() + Duration::from_secs(2);
    
    while Instant::now() < end_time {
        if client.query("SELECT COUNT(*) FROM bench", &[]).await.is_ok() {
            query_count += 1;
        }
        tokio::time::sleep(Duration::from_micros(200)).await;
    }
    
    let duration = start.elapsed();
    let qps = query_count as f64 / duration.as_secs_f64();
    
    server_handle.abort();
    qps
}