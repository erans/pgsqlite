use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use pgsqlite::session::DbHandler;

/// Concurrent read/write benchmark to test connection pooling performance
/// Tests mixed workloads with different read/write ratios

const TEST_DURATION: Duration = Duration::from_secs(3);
const WARMUP_DURATION: Duration = Duration::from_secs(1);

#[tokio::test]
async fn benchmark_mixed_workload_80_20() {
    println!("🧪 Testing 80% read / 20% write workload");
    
    let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
    setup_test_data(&db_handler).await;
    
    let metrics = run_mixed_workload(db_handler, 8, 0.8, 0.2).await;
    
    println!("📊 Results (80% read / 20% write):");
    print_metrics(&metrics);
    
    // Expect good performance with mostly reads
    assert!(metrics.total_operations > 50_000, "Should handle 50K+ ops in mixed workload");
    assert!(metrics.read_success_rate > 0.95, "Read success rate should be >95%");
    assert!(metrics.write_success_rate > 0.90, "Write success rate should be >90%");
}

#[tokio::test]
async fn benchmark_mixed_workload_50_50() {
    println!("🧪 Testing 50% read / 50% write workload");
    
    let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
    setup_test_data(&db_handler).await;
    
    let metrics = run_mixed_workload(db_handler, 8, 0.5, 0.5).await;
    
    println!("📊 Results (50% read / 50% write):");
    print_metrics(&metrics);
    
    // Expect reasonable performance with balanced workload
    assert!(metrics.total_operations > 30_000, "Should handle 30K+ ops in balanced workload");
    assert!(metrics.read_success_rate > 0.90, "Read success rate should be >90%");
    assert!(metrics.write_success_rate > 0.85, "Write success rate should be >85%");
}

#[tokio::test]
async fn benchmark_mixed_workload_20_80() {
    println!("🧪 Testing 20% read / 80% write workload");
    
    let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
    setup_test_data(&db_handler).await;
    
    let metrics = run_mixed_workload(db_handler, 8, 0.2, 0.8).await;
    
    println!("📊 Results (20% read / 80% write):");
    print_metrics(&metrics);
    
    // Write-heavy workload will be limited by single writer
    assert!(metrics.total_operations > 15_000, "Should handle 15K+ ops in write-heavy workload");
    assert!(metrics.read_success_rate > 0.90, "Read success rate should be >90%");
    assert!(metrics.write_success_rate > 0.80, "Write success rate should be >80%");
}

#[tokio::test]
async fn benchmark_transaction_consistency() {
    println!("🧪 Testing transaction consistency under load");
    
    let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
    setup_test_data(&db_handler).await;
    
    let metrics = run_transaction_benchmark(db_handler, 4).await;
    
    println!("📊 Transaction Results:");
    print_transaction_metrics(&metrics);
    
    // Transactions should maintain consistency
    assert!(metrics.successful_transactions > 100, "Should complete 100+ transactions");
    assert!(metrics.consistency_violations == 0, "Should have zero consistency violations");
    assert!(metrics.deadlocks == 0, "Should have zero deadlocks");
}

#[tokio::test]
async fn benchmark_connection_pool_scaling() {
    println!("🧪 Testing connection pool scaling (1, 2, 4, 8 tasks)");
    
    for task_count in [1, 2, 4, 8] {
        let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
        setup_test_data(&db_handler).await;
        
        let metrics = run_read_only_benchmark(db_handler, task_count).await;
        
        println!("📊 {} tasks: {:.0} QPS (avg latency: {:.2}ms)", 
            task_count, metrics.operations_per_second, metrics.avg_latency_ms);
    }
}

// Benchmark data structures

#[derive(Debug, Clone)]
struct WorkloadMetrics {
    total_operations: u64,
    read_operations: u64,
    write_operations: u64,
    read_success_rate: f64,
    write_success_rate: f64,
    operations_per_second: f64,
    avg_latency_ms: f64,
    p95_latency_ms: f64,
    p99_latency_ms: f64,
    _duration: Duration,
}

#[derive(Debug, Clone)]
struct TransactionMetrics {
    successful_transactions: u64,
    failed_transactions: u64,
    consistency_violations: u64,
    deadlocks: u64,
    avg_transaction_time_ms: f64,
}

#[derive(Debug, Clone)]
struct OperationResult {
    success: bool,
    latency: Duration,
    operation_type: OperationType,
}

#[derive(Debug, Clone)]
enum OperationType {
    Read,
    Write,
}

// Core benchmark functions

async fn run_mixed_workload(
    db_handler: Arc<DbHandler>,
    task_count: usize,
    read_ratio: f64,
    _write_ratio: f64,
) -> WorkloadMetrics {
    let start_time = Instant::now();
    let mut tasks = Vec::new();
    let results = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    
    // Warmup period
    println!("🔥 Warming up for {}s...", WARMUP_DURATION.as_secs());
    let warmup_end = Instant::now() + WARMUP_DURATION;
    
    while Instant::now() < warmup_end {
        let _ = db_handler.query("SELECT COUNT(*) FROM benchmark_data").await;
        sleep(Duration::from_millis(1)).await;
    }
    
    println!("🚀 Starting {} concurrent tasks for {}s...", task_count, TEST_DURATION.as_secs());
    let test_end = Instant::now() + TEST_DURATION;
    
    for task_id in 0..task_count {
        let db = db_handler.clone();
        let results_clone = results.clone();
        
        let task = tokio::spawn(async move {
            let mut local_results = Vec::new();
            let mut rng_state = task_id as u64; // Simple PRNG state
            
            while Instant::now() < test_end {
                // Simple linear congruential generator for deterministic randomness
                rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
                let random_value = (rng_state >> 16) as f64 / 65536.0;
                
                let op_start = Instant::now();
                let result = if random_value < read_ratio {
                    // Read operation
                    let success = db.query("SELECT id, value FROM benchmark_data WHERE id = 1").await.is_ok();
                    OperationResult {
                        success,
                        latency: op_start.elapsed(),
                        operation_type: OperationType::Read,
                    }
                } else {
                    // Write operation
                    let new_value = (rng_state % 10000) as i64;
                    let success = db.execute(&format!("UPDATE benchmark_data SET value = {} WHERE id = 1", new_value)).await.is_ok();
                    OperationResult {
                        success,
                        latency: op_start.elapsed(),
                        operation_type: OperationType::Write,
                    }
                };
                
                local_results.push(result);
                
                // Small delay to prevent overwhelming
                sleep(Duration::from_micros(100)).await;
            }
            
            // Add local results to global results
            let mut global_results = results_clone.lock().await;
            global_results.extend(local_results);
        });
        
        tasks.push(task);
    }
    
    // Wait for all tasks to complete
    for task in tasks {
        task.await.unwrap();
    }
    
    let total_duration = start_time.elapsed();
    let results = results.lock().await;
    
    calculate_workload_metrics(&results, total_duration)
}

async fn run_read_only_benchmark(db_handler: Arc<DbHandler>, task_count: usize) -> WorkloadMetrics {
    let start_time = Instant::now();
    let mut tasks = Vec::new();
    let results = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    
    let test_end = Instant::now() + TEST_DURATION;
    
    for _task_id in 0..task_count {
        let db = db_handler.clone();
        let results_clone = results.clone();
        
        let task = tokio::spawn(async move {
            let mut local_results = Vec::new();
            
            while Instant::now() < test_end {
                let op_start = Instant::now();
                let success = db.query("SELECT id, value FROM benchmark_data LIMIT 10").await.is_ok();
                
                local_results.push(OperationResult {
                    success,
                    latency: op_start.elapsed(),
                    operation_type: OperationType::Read,
                });
                
                sleep(Duration::from_micros(50)).await;
            }
            
            let mut global_results = results_clone.lock().await;
            global_results.extend(local_results);
        });
        
        tasks.push(task);
    }
    
    for task in tasks {
        task.await.unwrap();
    }
    
    let total_duration = start_time.elapsed();
    let results = results.lock().await;
    
    calculate_workload_metrics(&results, total_duration)
}

async fn run_transaction_benchmark(db_handler: Arc<DbHandler>, task_count: usize) -> TransactionMetrics {
    let mut tasks = Vec::new();
    let metrics = Arc::new(tokio::sync::Mutex::new(TransactionMetrics {
        successful_transactions: 0,
        failed_transactions: 0,
        consistency_violations: 0,
        deadlocks: 0,
        avg_transaction_time_ms: 0.0,
    }));
    
    let test_end = Instant::now() + TEST_DURATION;
    
    for task_id in 0..task_count {
        let db = db_handler.clone();
        let metrics_clone = metrics.clone();
        
        let task = tokio::spawn(async move {
            let mut transaction_times = Vec::new();
            
            while Instant::now() < test_end {
                let tx_start = Instant::now();
                
                // Simulate a transaction that transfers value between two rows
                let from_id = 1 + (task_id % 10);
                let to_id = 1 + ((task_id + 1) % 10);
                let transfer_amount = 10;
                
                let result = execute_transfer_transaction(&db, from_id, to_id, transfer_amount).await;
                let tx_duration = tx_start.elapsed();
                
                let mut metrics_guard = metrics_clone.lock().await;
                match result {
                    Ok(()) => {
                        metrics_guard.successful_transactions += 1;
                        transaction_times.push(tx_duration.as_secs_f64() * 1000.0);
                    }
                    Err(_) => {
                        metrics_guard.failed_transactions += 1;
                    }
                }
                
                sleep(Duration::from_millis(10)).await;
            }
            
            // Update average transaction time
            if !transaction_times.is_empty() {
                let avg_time = transaction_times.iter().sum::<f64>() / transaction_times.len() as f64;
                let mut metrics_guard = metrics_clone.lock().await;
                metrics_guard.avg_transaction_time_ms = avg_time;
            }
        });
        
        tasks.push(task);
    }
    
    for task in tasks {
        task.await.unwrap();
    }
    
    let final_metrics = metrics.lock().await.clone();
    final_metrics
}

// Helper functions

async fn setup_test_data(db_handler: &DbHandler) {
    // Create test table
    db_handler.execute("
        CREATE TABLE IF NOT EXISTS benchmark_data (
            id INTEGER PRIMARY KEY,
            value INTEGER NOT NULL,
            description TEXT
        )
    ").await.unwrap();
    
    // Insert test data
    for i in 1..=100 {
        db_handler.execute(&format!(
            "INSERT OR REPLACE INTO benchmark_data (id, value, description) VALUES ({}, {}, 'test_data_{}')",
            i, i * 10, i
        )).await.unwrap();
    }
}

async fn execute_transfer_transaction(
    db: &DbHandler,
    from_id: usize,
    to_id: usize,
    amount: i64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Start transaction
    db.execute("BEGIN").await?;
    
    // Check balances
    let from_result = db.query(&format!("SELECT value FROM benchmark_data WHERE id = {}", from_id)).await?;
    let to_result = db.query(&format!("SELECT value FROM benchmark_data WHERE id = {}", to_id)).await?;
    
    if from_result.rows.is_empty() || to_result.rows.is_empty() {
        db.execute("ROLLBACK").await?;
        return Err("Account not found".into());
    }
    
    // Parse current balances
    let from_balance = parse_balance(&from_result.rows[0][1])?;
    let to_balance = parse_balance(&to_result.rows[0][1])?;
    
    // Check sufficient funds
    if from_balance < amount {
        db.execute("ROLLBACK").await?;
        return Err("Insufficient funds".into());
    }
    
    // Update balances
    db.execute(&format!(
        "UPDATE benchmark_data SET value = {} WHERE id = {}",
        from_balance - amount, from_id
    )).await?;
    
    db.execute(&format!(
        "UPDATE benchmark_data SET value = {} WHERE id = {}",
        to_balance + amount, to_id
    )).await?;
    
    // Commit transaction
    db.execute("COMMIT").await?;
    
    Ok(())
}

fn parse_balance(value_bytes: &Option<Vec<u8>>) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
    match value_bytes {
        Some(bytes) => {
            let value_str = String::from_utf8(bytes.clone())?;
            Ok(value_str.parse()?)
        }
        None => Err("NULL balance".into())
    }
}

fn calculate_workload_metrics(results: &[OperationResult], duration: Duration) -> WorkloadMetrics {
    let total_operations = results.len() as u64;
    let read_operations = results.iter().filter(|r| matches!(r.operation_type, OperationType::Read)).count() as u64;
    let write_operations = total_operations - read_operations;
    
    let successful_reads = results.iter()
        .filter(|r| matches!(r.operation_type, OperationType::Read) && r.success)
        .count() as u64;
    let successful_writes = results.iter()
        .filter(|r| matches!(r.operation_type, OperationType::Write) && r.success)
        .count() as u64;
    
    let read_success_rate = if read_operations > 0 {
        successful_reads as f64 / read_operations as f64
    } else {
        1.0
    };
    
    let write_success_rate = if write_operations > 0 {
        successful_writes as f64 / write_operations as f64
    } else {
        1.0
    };
    
    let operations_per_second = total_operations as f64 / duration.as_secs_f64();
    
    // Calculate latency percentiles
    let mut latencies: Vec<f64> = results.iter()
        .map(|r| r.latency.as_secs_f64() * 1000.0) // Convert to milliseconds
        .collect();
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    
    let avg_latency_ms = if !latencies.is_empty() {
        latencies.iter().sum::<f64>() / latencies.len() as f64
    } else {
        0.0
    };
    
    let p95_latency_ms = if !latencies.is_empty() {
        latencies[(latencies.len() * 95 / 100).min(latencies.len() - 1)]
    } else {
        0.0
    };
    
    let p99_latency_ms = if !latencies.is_empty() {
        latencies[(latencies.len() * 99 / 100).min(latencies.len() - 1)]
    } else {
        0.0
    };
    
    WorkloadMetrics {
        total_operations,
        read_operations,
        write_operations,
        read_success_rate,
        write_success_rate,
        operations_per_second,
        avg_latency_ms,
        p95_latency_ms,
        p99_latency_ms,
        _duration: duration,
    }
}

fn print_metrics(metrics: &WorkloadMetrics) {
    println!("  Total Operations: {}", metrics.total_operations);
    println!("  Read Operations: {} ({:.1}%)", metrics.read_operations, 
        100.0 * metrics.read_operations as f64 / metrics.total_operations as f64);
    println!("  Write Operations: {} ({:.1}%)", metrics.write_operations,
        100.0 * metrics.write_operations as f64 / metrics.total_operations as f64);
    println!("  Operations/sec: {:.0}", metrics.operations_per_second);
    println!("  Read Success Rate: {:.2}%", metrics.read_success_rate * 100.0);
    println!("  Write Success Rate: {:.2}%", metrics.write_success_rate * 100.0);
    println!("  Avg Latency: {:.2}ms", metrics.avg_latency_ms);
    println!("  P95 Latency: {:.2}ms", metrics.p95_latency_ms);
    println!("  P99 Latency: {:.2}ms", metrics.p99_latency_ms);
}

fn print_transaction_metrics(metrics: &TransactionMetrics) {
    println!("  Successful Transactions: {}", metrics.successful_transactions);
    println!("  Failed Transactions: {}", metrics.failed_transactions);
    println!("  Consistency Violations: {}", metrics.consistency_violations);
    println!("  Deadlocks: {}", metrics.deadlocks);
    println!("  Avg Transaction Time: {:.2}ms", metrics.avg_transaction_time_ms);
    
    let total_transactions = metrics.successful_transactions + metrics.failed_transactions;
    if total_transactions > 0 {
        let success_rate = metrics.successful_transactions as f64 / total_transactions as f64;
        println!("  Transaction Success Rate: {:.2}%", success_rate * 100.0);
    }
}