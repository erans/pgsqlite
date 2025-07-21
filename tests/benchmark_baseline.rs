#[cfg(all(test, not(debug_assertions)))]
mod baseline_benchmark {
    use std::time::{Duration, Instant};
    use std::sync::Arc;
    use pgsqlite::session::DbHandler;

    /// Simple baseline benchmark using direct DbHandler calls
    #[tokio::test]
    async fn benchmark_single_thread_baseline() {
        eprintln!("\n=== Single Thread Baseline Benchmark ===");
        
        let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
        
        // Setup test data
        db_handler.execute("
            CREATE TABLE baseline_test (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER
            )
        ").await.expect("Failed to create table");

        for i in 0..1000 {
            db_handler.execute(&format!(
                "INSERT INTO baseline_test (id, name, value) VALUES ({}, 'name_{}', {})",
                i, i, i * 10
            )).await.expect("Failed to insert");
        }
        
        eprintln!("Data setup complete: 1,000 rows");
        
        // Benchmark queries
        let iterations = 100;
        let mut query_times = Vec::new();
        
        let start_time = Instant::now();
        
        for i in 0..iterations {
            let query_start = Instant::now();
            let id = i % 1000;
            
            let result = db_handler.query(
                &format!("SELECT * FROM baseline_test WHERE id = {}", id)
            ).await.expect("Query failed");
            
            assert!(!result.rows.is_empty());
            query_times.push(query_start.elapsed());
        }
        
        let total_duration = start_time.elapsed();
        let qps = iterations as f64 / total_duration.as_secs_f64();
        
        // Calculate statistics
        query_times.sort();
        let avg = query_times.iter().sum::<Duration>() / query_times.len() as u32;
        let min = query_times[0];
        let max = query_times[query_times.len() - 1];
        let p50 = query_times[query_times.len() / 2];
        let p95 = query_times[query_times.len() * 95 / 100];
        
        eprintln!("\nDirect DbHandler Baseline Results:");
        eprintln!("  Iterations: {}", iterations);
        eprintln!("  Duration: {:.3}s", total_duration.as_secs_f64());
        eprintln!("  QPS: {:.1}", qps);
        eprintln!("  Latency (avg): {:.3}ms", avg.as_secs_f64() * 1000.0);
        eprintln!("  Latency (min): {:.3}ms", min.as_secs_f64() * 1000.0);
        eprintln!("  Latency (p50): {:.3}ms", p50.as_secs_f64() * 1000.0);
        eprintln!("  Latency (p95): {:.3}ms", p95.as_secs_f64() * 1000.0);
        eprintln!("  Latency (max): {:.3}ms", max.as_secs_f64() * 1000.0);
    }

    /// Test lock contention with multiple async tasks
    #[tokio::test]
    async fn benchmark_async_contention() {
        eprintln!("\n=== Async Contention Benchmark ===");
        
        let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
        
        // Setup test data
        db_handler.execute("
            CREATE TABLE contention_test (
                id INTEGER PRIMARY KEY,
                value INTEGER
            )
        ").await.expect("Failed to create table");

        for i in 0..100 {
            db_handler.execute(&format!(
                "INSERT INTO contention_test (id, value) VALUES ({}, {})",
                i, i * 10
            )).await.expect("Failed to insert");
        }
        
        let task_counts = vec![1, 2, 4, 8];
        let iterations_per_task = 25;
        
        for task_count in task_counts {
            let start_time = Instant::now();
            let mut handles = Vec::new();
            
            for task_id in 0..task_count {
                let db_handler = db_handler.clone();
                let handle = tokio::spawn(async move {
                    let mut task_times = Vec::new();
                    
                    for i in 0..iterations_per_task {
                        let query_start = Instant::now();
                        let id = (task_id * iterations_per_task + i) % 100;
                        
                        let result = db_handler.query(
                            &format!("SELECT * FROM contention_test WHERE id = {}", id)
                        ).await.expect("Query failed");
                        
                        assert!(!result.rows.is_empty());
                        task_times.push(query_start.elapsed());
                    }
                    
                    task_times
                });
                
                handles.push(handle);
            }
            
            // Wait for all tasks
            let mut all_times = Vec::new();
            for handle in handles {
                let times = handle.await.expect("Task failed");
                all_times.extend(times);
            }
            
            let total_duration = start_time.elapsed();
            let total_queries = all_times.len();
            let qps = total_queries as f64 / total_duration.as_secs_f64();
            
            // Calculate latency stats
            all_times.sort();
            let avg = all_times.iter().sum::<Duration>() / all_times.len() as u32;
            let p50 = all_times[all_times.len() / 2];
            let p95 = all_times[all_times.len() * 95 / 100];
            
            eprintln!("\n{} async tasks:", task_count);
            eprintln!("  Total queries: {}", total_queries);
            eprintln!("  Duration: {:.3}s", total_duration.as_secs_f64());
            eprintln!("  QPS: {:.1}", qps);
            eprintln!("  Latency (avg): {:.3}ms", avg.as_secs_f64() * 1000.0);
            eprintln!("  Latency (p50): {:.3}ms", p50.as_secs_f64() * 1000.0);
            eprintln!("  Latency (p95): {:.3}ms", p95.as_secs_f64() * 1000.0);
        }
    }
}

#[cfg(debug_assertions)]
fn main() {
    println!("Benchmarks must be run in release mode");
}

#[cfg(not(debug_assertions))]
fn main() {}