#[cfg(all(test, not(debug_assertions)))]
mod simple_concurrent_test {
    use std::sync::{Arc, Barrier};
    use std::time::{Duration, Instant};
    use tokio::net::TcpListener;
    use tokio::task;
    use tokio_postgres::NoTls;

    async fn setup_test_server_and_data() -> (tokio::task::JoinHandle<()>, u16, tokio_postgres::Client) {
        // Start test server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        
        let server_handle = tokio::spawn(async move {
            let db_handler = std::sync::Arc::new(
                pgsqlite::session::DbHandler::new(":memory:").unwrap()
            );
            
            loop {
                let (stream, addr) = listener.accept().await.unwrap();
                let db_handler = db_handler.clone();
                tokio::spawn(async move {
                    if let Err(e) = pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await {
                        eprintln!("Connection error: {}", e);
                    }
                });
            }
        });
        
        // Wait for server to start
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Connect client
        let (client, connection) = tokio_postgres::connect(
            &format!("host=127.0.0.1 port={} dbname=test", port),
            NoTls,
        ).await.expect("Failed to connect");
        
        // Spawn connection task
        task::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });
        
        // Create test table and data
        client.execute("
            CREATE TABLE benchmark_test (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER
            )", &[]).await.expect("Failed to create table");

        // Insert test data
        for i in 0..1000 {
            let id = i as i32;
            let value = (i * 10) as i32;
            client.execute("
                INSERT INTO benchmark_test (id, name, value) VALUES ($1, $2, $3)",
                &[&id, &format!("name_{}", i), &value]
            ).await.expect("Failed to insert data");
        }

        println!("Benchmark data setup complete: 1,000 rows");
        
        (server_handle, port, client)
    }

    #[tokio::test]
    async fn benchmark_simple_concurrent() {
        println!("\n=== Simple Concurrent Benchmark ===");
        
        let (server_handle, port, _setup_client) = setup_test_server_and_data().await;
        
        let thread_count = 4;
        let iterations_per_thread = 25;
        
        let barrier = Arc::new(Barrier::new(thread_count));
        let start_time = Instant::now();
        
        let mut handles = vec![];
        
        for thread_id in 0..thread_count {
            let barrier = barrier.clone();
            
            let handle = task::spawn(async move {
                // Create new connection for this thread
                let (client, connection) = tokio_postgres::connect(
                    &format!("host=127.0.0.1 port={} dbname=test", port),
                    NoTls,
                ).await.expect("Failed to connect");
                
                // Spawn connection task
                task::spawn(async move {
                    if let Err(e) = connection.await {
                        eprintln!("Connection error: {}", e);
                    }
                });
                
                // Wait for all threads to be ready
                barrier.wait();
                
                let mut query_times = Vec::new();
                
                for i in 0..iterations_per_thread {
                    let query_start = Instant::now();
                    let id = ((thread_id * iterations_per_thread + i) % 1000) as i32;
                    let rows = client.query(
                        "SELECT * FROM benchmark_test WHERE id = $1",
                        &[&id]
                    ).await.expect("Query failed");
                    
                    assert_eq!(rows.len(), 1);
                    query_times.push(query_start.elapsed());
                }
                
                println!("Thread {} completed {} queries", thread_id, iterations_per_thread);
                query_times
            });
            
            handles.push(handle);
        }
        
        // Wait for all threads and collect results
        let mut all_times = Vec::new();
        for handle in handles {
            let times = handle.await.expect("Task panicked");
            all_times.extend(times);
        }
        
        let total_duration = start_time.elapsed();
        let total_queries = all_times.len();
        let qps = total_queries as f64 / total_duration.as_secs_f64();
        
        // Calculate latency statistics
        all_times.sort();
        let avg = all_times.iter().sum::<Duration>() / all_times.len() as u32;
        let min = all_times[0];
        let max = all_times[all_times.len() - 1];
        let p50 = all_times[all_times.len() / 2];
        let p95 = all_times[all_times.len() * 95 / 100];
        let p99 = all_times[all_times.len() * 99 / 100];
        
        println!("\nResults:");
        println!("  Threads: {}", thread_count);
        println!("  Iterations per thread: {}", iterations_per_thread);
        println!("  Total queries: {}", total_queries);
        println!("  Duration: {:.3}s", total_duration.as_secs_f64());
        println!("  QPS: {:.1}", qps);
        println!("  Latency (avg): {:.3}ms", avg.as_secs_f64() * 1000.0);
        println!("  Latency (min): {:.3}ms", min.as_secs_f64() * 1000.0);
        println!("  Latency (p50): {:.3}ms", p50.as_secs_f64() * 1000.0);
        println!("  Latency (p95): {:.3}ms", p95.as_secs_f64() * 1000.0);
        println!("  Latency (p99): {:.3}ms", p99.as_secs_f64() * 1000.0);
        println!("  Latency (max): {:.3}ms", max.as_secs_f64() * 1000.0);
        
        server_handle.abort();
    }

    #[tokio::test]
    #[ignore]
    async fn benchmark_scaling_analysis() {
        println!("\n=== Concurrent Scaling Analysis ===");
        
        let thread_counts = vec![1, 2, 4, 8];
        let iterations_per_thread = 20;
        
        let mut baseline_qps = 0.0;
        
        for thread_count in thread_counts {
            let (server_handle, port, _setup_client) = setup_test_server_and_data().await;
            
            let barrier = Arc::new(Barrier::new(thread_count));
            let start_time = Instant::now();
            
            let mut handles = vec![];
            
            for thread_id in 0..thread_count {
                let barrier = barrier.clone();
                
                let handle = task::spawn(async move {
                    let (client, connection) = tokio_postgres::connect(
                        &format!("host=127.0.0.1 port={} dbname=test", port),
                        NoTls,
                    ).await.expect("Failed to connect");
                    
                    task::spawn(async move {
                        if let Err(e) = connection.await {
                            eprintln!("Connection error: {}", e);
                        }
                    });
                    
                    barrier.wait();
                    
                    for i in 0..iterations_per_thread {
                        let id = ((thread_id * iterations_per_thread + i) % 1000) as i32;
                        let rows = client.query(
                            "SELECT * FROM benchmark_test WHERE id = $1",
                            &[&id]
                        ).await.expect("Query failed");
                        assert_eq!(rows.len(), 1);
                    }
                });
                
                handles.push(handle);
            }
            
            for handle in handles {
                handle.await.expect("Task panicked");
            }
            
            let total_duration = start_time.elapsed();
            let total_queries = thread_count * iterations_per_thread;
            let qps = total_queries as f64 / total_duration.as_secs_f64();
            
            if thread_count == 1 {
                baseline_qps = qps;
            }
            
            let scaling_factor = qps / baseline_qps;
            let efficiency = scaling_factor / thread_count as f64 * 100.0;
            
            println!("{} threads: {:.1} qps, {:.2}x scaling, {:.1}% efficiency",
                thread_count, qps, scaling_factor, efficiency);
            
            server_handle.abort();
        }
    }
}

#[cfg(debug_assertions)]
fn main() {
    println!("Benchmarks must be run in release mode");
}

#[cfg(not(debug_assertions))]
fn main() {}