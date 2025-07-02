use std::time::Instant;
use tokio_postgres::{NoTls, types::ToSql};
use std::process::{Command, Child};
use std::thread;

#[tokio::test]
#[ignore] // Run with: cargo test benchmark_extended_protocol -- --ignored --nocapture
async fn benchmark_extended_protocol_parameters() {
    println!("\n=== Extended Protocol Parameter Handling Benchmark ===");
    
    // Start pgsqlite server
    let mut server = start_server();
    
    // Wait for server to start
    thread::sleep(std::time::Duration::from_secs(2));
    
    // Connect to the server
    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=5432 user=postgres dbname=test",
        NoTls,
    )
    .await
    .expect("Failed to connect");
    
    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // Create test table
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS bench_params (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER,
                price NUMERIC(10,2)
            )",
            &[],
        )
        .await
        .expect("Failed to create table");
    
    // Warmup
    for i in 0..100 {
        let name = format!("warmup_{}", i);
        client
            .execute(
                "INSERT INTO bench_params (id, name, value, price) VALUES ($1, $2, $3, $4)",
                &[&i, &name, &(i * 10), &format!("{}.99", i)],
            )
            .await
            .expect("Failed to insert warmup data");
    }
    
    println!("\nBenchmarking parameter type inference (first execution):");
    
    // Test 1: Parameter type inference for new queries
    let mut total_inference_time = std::time::Duration::ZERO;
    for i in 100..200 {
        // Use different query each time to test inference
        let query = format!(
            "INSERT INTO bench_params (id, name, value, price) VALUES ($1, $2, $3, $4) -- {}",
            i
        );
        let name = format!("test_{}", i);
        let price = format!("{}.99", i);
        
        let start = Instant::now();
        client
            .execute(
                &query,
                &[&i, &name, &(i * 10), &price],
            )
            .await
            .expect("Failed to execute");
        total_inference_time += start.elapsed();
    }
    
    println!(
        "Average time with type inference: {:?}",
        total_inference_time / 100
    );
    
    println!("\nBenchmarking cached parameter types (repeated execution):");
    
    // Test 2: Cached parameter types
    let query = "INSERT INTO bench_params (id, name, value, price) VALUES ($1, $2, $3, $4)";
    let stmt = client.prepare(query).await.expect("Failed to prepare");
    
    let mut total_cached_time = std::time::Duration::ZERO;
    for i in 200..300 {
        let name = format!("cached_{}", i);
        let price = format!("{}.99", i);
        
        let start = Instant::now();
        client
            .execute(
                &stmt,
                &[&i, &name, &(i * 10), &price],
            )
            .await
            .expect("Failed to execute");
        total_cached_time += start.elapsed();
    }
    
    println!(
        "Average time with cached types: {:?}",
        total_cached_time / 100
    );
    
    println!("\nBenchmarking SELECT with parameters:");
    
    // Test 3: SELECT queries with parameters
    let select_stmt = client
        .prepare("SELECT * FROM bench_params WHERE id = $1 AND value > $2")
        .await
        .expect("Failed to prepare SELECT");
    
    let mut total_select_time = std::time::Duration::ZERO;
    for i in 0..100 {
        let start = Instant::now();
        let rows = client
            .query(&select_stmt, &[&i, &(i * 5)])
            .await
            .expect("Failed to execute SELECT");
        total_select_time += start.elapsed();
        
        // Verify we got results
        if i < 20 {
            assert!(!rows.is_empty(), "Expected rows for id {}", i);
        }
    }
    
    println!(
        "Average SELECT with parameters: {:?}",
        total_select_time / 100
    );
    
    println!("\nBenchmarking binary vs text parameter formats:");
    
    // Test 4: Binary parameter format
    let binary_stmt = client
        .prepare_typed(
            "INSERT INTO bench_params (id, name, value, price) VALUES ($1, $2, $3, $4)",
            &[
                tokio_postgres::types::Type::INT4,
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::INT4,
                tokio_postgres::types::Type::TEXT,
            ],
        )
        .await
        .expect("Failed to prepare with types");
    
    let mut total_binary_time = std::time::Duration::ZERO;
    for i in 300..400 {
        let name = format!("binary_{}", i);
        let price = format!("{}.99", i);
        
        let start = Instant::now();
        client
            .execute(&binary_stmt, &[&i, &name, &(i * 10), &price])
            .await
            .expect("Failed to execute");
        total_binary_time += start.elapsed();
    }
    
    println!(
        "Average time with binary format: {:?}",
        total_binary_time / 100
    );
    
    // Calculate improvements
    let inference_improvement = if total_cached_time < total_inference_time {
        let improvement = (total_inference_time.as_micros() as f64 - total_cached_time.as_micros() as f64)
            / total_inference_time.as_micros() as f64 * 100.0;
        format!("{:.1}% faster", improvement)
    } else {
        format!("{:.1}% slower", 
            (total_cached_time.as_micros() as f64 - total_inference_time.as_micros() as f64)
            / total_inference_time.as_micros() as f64 * 100.0)
    };
    
    println!("\n=== Summary ===");
    println!("Type inference (first run): {:?}", total_inference_time / 100);
    println!("Cached types (repeated):    {:?}", total_cached_time / 100);
    println!("Improvement:                {}", inference_improvement);
    println!("SELECT with parameters:     {:?}", total_select_time / 100);
    println!("Binary parameter format:    {:?}", total_binary_time / 100);
    
    // Cleanup
    client
        .execute("DROP TABLE bench_params", &[])
        .await
        .expect("Failed to drop table");
    
    // Stop server
    server.kill().expect("Failed to kill server");
}

fn start_server() -> Child {
    Command::new("cargo")
        .args(&["run", "--", "--port", "5432"])
        .spawn()
        .expect("Failed to start server")
}