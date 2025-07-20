use std::sync::Arc;
use std::time::{Duration, Instant};
use pgsqlite::session::DbHandler;

/// Simple concurrent benchmark to test connection pooling vs single connection

#[tokio::test]
async fn test_concurrent_reads_baseline() {
    println!("🧪 Testing concurrent reads (baseline - single connection)");
    
    let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
    setup_test_data(&db_handler).await;
    
    let start = Instant::now();
    let mut tasks = Vec::new();
    
    // Run 4 concurrent read tasks for 2 seconds
    for _i in 0..4 {
        let db = db_handler.clone();
        let task = tokio::spawn(async move {
            let mut count = 0;
            let end_time = Instant::now() + Duration::from_secs(2);
            
            while Instant::now() < end_time {
                match db.query("SELECT COUNT(*) FROM test_data").await {
                    Ok(_) => count += 1,
                    Err(e) => eprintln!("Query error: {}", e),
                }
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
            count
        });
        tasks.push(task);
    }
    
    let mut total_queries = 0;
    for task in tasks {
        total_queries += task.await.unwrap();
    }
    
    let duration = start.elapsed();
    let qps = total_queries as f64 / duration.as_secs_f64();
    
    println!("📊 Baseline Results:");
    println!("  Total queries: {}", total_queries);
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!("  QPS: {:.0}", qps);
    
    assert!(total_queries > 1000, "Should execute at least 1000 queries");
}

#[tokio::test]  
async fn test_mixed_workload_simple() {
    println!("🧪 Testing simple mixed read/write workload");
    
    let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
    setup_test_data(&db_handler).await;
    
    let start = Instant::now();
    let mut tasks = Vec::new();
    
    // 2 read tasks + 1 write task
    for i in 0..3 {
        let db = db_handler.clone();
        let is_writer = i == 2; // Last task is writer
        
        let task = tokio::spawn(async move {
            let mut count = 0;
            let end_time = Instant::now() + Duration::from_secs(2);
            
            while Instant::now() < end_time {
                let result = if is_writer {
                    let new_value = count % 1000;
                    db.execute(&format!("UPDATE test_data SET value = {} WHERE id = 1", new_value)).await.map(|_| ())
                } else {
                    db.query("SELECT id, value FROM test_data WHERE id <= 10").await.map(|_| ())
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
    
    println!("📊 Mixed Workload Results:");
    println!("  Read operations: {}", total_reads);
    println!("  Write operations: {}", total_writes);
    println!("  Total operations: {}", total_ops);
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!("  Operations/sec: {:.0}", ops_per_sec);
    
    assert!(total_reads > 1000, "Should have substantial read operations");
    assert!(total_writes > 10, "Should have some write operations");
    assert!(total_ops > 1500, "Should have good overall throughput");
}

#[tokio::test]
async fn test_transaction_handling() {
    println!("🧪 Testing transaction handling");
    
    let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
    setup_test_data(&db_handler).await;
    
    // Test multiple concurrent transactions
    let mut tasks = Vec::new();
    
    for i in 0..3 {
        let db = db_handler.clone();
        let task = tokio::spawn(async move {
            let mut successful_tx = 0;
            let mut failed_tx = 0;
            
            for tx_id in 0..10 {
                let result = execute_simple_transaction(&db, i, tx_id).await;
                match result {
                    Ok(_) => successful_tx += 1,
                    Err(_) => failed_tx += 1,
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            
            (successful_tx, failed_tx)
        });
        tasks.push(task);
    }
    
    let mut total_success = 0;
    let mut total_failed = 0;
    
    for task in tasks {
        let (success, failed) = task.await.unwrap();
        total_success += success;
        total_failed += failed;
    }
    
    println!("📊 Transaction Results:");
    println!("  Successful transactions: {}", total_success);
    println!("  Failed transactions: {}", total_failed);
    println!("  Success rate: {:.1}%", 100.0 * total_success as f64 / (total_success + total_failed) as f64);
    
    assert!(total_success > 15, "Most transactions should succeed");
    assert!(total_failed < 15, "Failures should be minimal");
}

async fn setup_test_data(db_handler: &DbHandler) {
    // Create test table
    db_handler.execute("
        CREATE TABLE IF NOT EXISTS test_data (
            id INTEGER PRIMARY KEY,
            value INTEGER NOT NULL,
            description TEXT
        )
    ").await.unwrap();
    
    // Insert test data
    for i in 1..=50 {
        db_handler.execute(&format!(
            "INSERT OR REPLACE INTO test_data (id, value, description) VALUES ({}, {}, 'test_{}')",
            i, i * 100, i
        )).await.unwrap();
    }
}

async fn execute_simple_transaction(
    db: &DbHandler,
    worker_id: usize,
    tx_id: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Simple transaction: read a value, increment it, write it back
    db.execute("BEGIN").await?;
    
    let result = db.query(&format!("SELECT value FROM test_data WHERE id = {}", 1 + (worker_id % 10))).await?;
    
    if result.rows.is_empty() {
        db.execute("ROLLBACK").await?;
        return Err("No data found".into());
    }
    
    // Parse the current value
    let current_value = if let Some(ref value_bytes) = result.rows[0][0] {
        String::from_utf8(value_bytes.clone())?.parse::<i64>()?
    } else {
        0
    };
    
    let new_value = current_value + (tx_id as i64);
    
    db.execute(&format!(
        "UPDATE test_data SET value = {} WHERE id = {}",
        new_value, 1 + (worker_id % 10)
    )).await?;
    
    db.execute("COMMIT").await?;
    
    Ok(())
}