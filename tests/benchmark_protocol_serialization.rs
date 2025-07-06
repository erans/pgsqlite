use std::time::{Duration, Instant};
use tokio_postgres::{NoTls, types::Type};
use std::process::{Command, Child};
use std::thread;
use tracing::instrument;

/// Profile protocol serialization overhead by measuring different components
#[tokio::test]
#[ignore] // Run with: cargo test benchmark_protocol_serialization -- --ignored --nocapture
async fn benchmark_protocol_serialization() {
    // Initialize tracing for better profiling visibility
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pgsqlite=trace,benchmark_protocol_serialization=debug")
        .try_init();

    println!("\n=== Protocol Serialization Profiling ===");
    
    // Start pgsqlite server
    let mut server = start_server();
    thread::sleep(Duration::from_secs(2));
    
    // Connect to server
    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=5433 user=postgres dbname=test",
        NoTls,
    )
    .await
    .expect("Failed to connect");
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // Create test table
    client
        .execute(
            "CREATE TABLE bench_protocol (
                id INTEGER PRIMARY KEY,
                text_col TEXT,
                int_col INTEGER,
                float_col REAL,
                bool_col BOOLEAN,
                bytes_col BYTEA,
                numeric_col NUMERIC(10,2)
            )",
            &[],
        )
        .await
        .expect("Failed to create table");
    
    // Warm up the system
    warmup(&client).await;
    
    // Run profiling tests
    profile_simple_protocol(&client).await;
    profile_extended_protocol(&client).await;
    profile_data_serialization(&client).await;
    profile_type_conversions(&client).await;
    profile_message_batching(&client).await;
    
    // Cleanup
    client.execute("DROP TABLE bench_protocol", &[]).await.ok();
    server.kill().expect("Failed to kill server");
}

async fn warmup(client: &tokio_postgres::Client) {
    println!("\nWarming up...");
    for i in 0..100 {
        client
            .execute(
                "INSERT INTO bench_protocol (id, text_col, int_col) VALUES ($1, $2, $3)",
                &[&i, &format!("warmup_{}", i), &(i * 10)],
            )
            .await
            .ok();
    }
}

#[instrument(skip(client))]
async fn profile_simple_protocol(client: &tokio_postgres::Client) {
    println!("\n--- Simple Query Protocol Profiling ---");
    
    let iterations = 1000;
    let mut timings = Vec::new();
    
    for i in 0..iterations {
        let query = format!(
            "SELECT * FROM bench_protocol WHERE id = {} AND int_col > {}",
            i % 100,
            i % 50
        );
        
        let start = Instant::now();
        let _rows = client.simple_query(&query).await.unwrap();
        let elapsed = start.elapsed();
        timings.push(elapsed);
    }
    
    analyze_timings("Simple protocol total", &timings);
    
    // Measure components
    println!("\nComponent breakdown:");
    
    // Query parsing overhead
    let mut parse_times = Vec::new();
    for i in 0..100 {
        let query = format!("SELECT * FROM bench_protocol WHERE id = {}", i);
        let start = Instant::now();
        // This will trigger parsing in pgsqlite
        let _ = client.simple_query(&query).await.unwrap();
        parse_times.push(start.elapsed());
    }
    analyze_timings("Query parsing", &parse_times);
}

#[instrument(skip(client))]
async fn profile_extended_protocol(client: &tokio_postgres::Client) {
    println!("\n--- Extended Query Protocol Profiling ---");
    
    let iterations = 1000;
    
    // Profile Parse phase
    println!("\nParse phase profiling:");
    let mut parse_times = Vec::new();
    for i in 0..100 {
        let query = format!(
            "SELECT * FROM bench_protocol WHERE id = $1 AND int_col > $2 -- {}",
            i // Unique query to force re-parsing
        );
        let start = Instant::now();
        let _stmt = client.prepare(&query).await.unwrap();
        parse_times.push(start.elapsed());
    }
    analyze_timings("Parse message handling", &parse_times);
    
    // Profile Bind/Execute with prepared statement
    println!("\nBind/Execute phase profiling:");
    let stmt = client
        .prepare("SELECT * FROM bench_protocol WHERE id = $1 AND int_col > $2")
        .await
        .unwrap();
    
    let mut bind_execute_times = Vec::new();
    for i in 0..iterations {
        let start = Instant::now();
        let _rows = client.query(&stmt, &[&(i % 100), &(i % 50)]).await.unwrap();
        bind_execute_times.push(start.elapsed());
    }
    analyze_timings("Bind+Execute", &bind_execute_times);
    
    // Profile parameter serialization
    println!("\nParameter serialization profiling:");
    let types = &[Type::INT4, Type::TEXT, Type::INT4, Type::FLOAT4, Type::BOOL];
    let typed_stmt = client
        .prepare_typed(
            "INSERT INTO bench_protocol (id, text_col, int_col, float_col, bool_col) VALUES ($1, $2, $3, $4, $5)",
            types,
        )
        .await
        .unwrap();
    
    let mut param_times = Vec::new();
    for i in 1000..1000 + iterations {
        let start = Instant::now();
        client
            .execute(
                &typed_stmt,
                &[&i, &format!("text_{}", i), &(i * 2), &(i as f32 * 1.5), &(i % 2 == 0)],
            )
            .await
            .unwrap();
        param_times.push(start.elapsed());
    }
    analyze_timings("Parameter serialization", &param_times);
}

#[instrument(skip(client))]
async fn profile_data_serialization(client: &tokio_postgres::Client) {
    println!("\n--- Data Row Serialization Profiling ---");
    
    // Test different result set sizes
    for size in &[1, 10, 100, 1000] {
        println!("\nResult set size: {} rows", size);
        
        // Insert test data if needed
        let max_id = client
            .query_one("SELECT MAX(id) FROM bench_protocol", &[])
            .await
            .unwrap()
            .get::<_, Option<i32>>(0)
            .unwrap_or(0);
        
        for i in max_id + 1..=max_id + *size {
            client
                .execute(
                    "INSERT INTO bench_protocol (id, text_col, int_col) VALUES ($1, $2, $3)",
                    &[&i, &format!("row_{}", i), &(i * 10)],
                )
                .await
                .ok();
        }
        
        let stmt = client
            .prepare(&format!("SELECT * FROM bench_protocol LIMIT {}", size))
            .await
            .unwrap();
        
        let mut times = Vec::new();
        for _ in 0..100 {
            let start = Instant::now();
            let rows = client.query(&stmt, &[]).await.unwrap();
            assert_eq!(rows.len(), *size as usize);
            times.push(start.elapsed());
        }
        
        analyze_timings(&format!("{} row serialization", size), &times);
    }
}

#[instrument(skip(client))]
async fn profile_type_conversions(client: &tokio_postgres::Client) {
    println!("\n--- Type Conversion Profiling ---");
    
    // Profile different data types
    let test_cases = vec![
        ("INTEGER", "SELECT int_col FROM bench_protocol WHERE id < 50"),
        ("TEXT", "SELECT text_col FROM bench_protocol WHERE id < 50"),
        ("BOOLEAN", "SELECT bool_col FROM bench_protocol WHERE id < 50"),
        ("FLOAT", "SELECT float_col FROM bench_protocol WHERE id < 50"),
        ("NUMERIC", "SELECT numeric_col FROM bench_protocol WHERE id < 50"),
    ];
    
    for (type_name, query) in test_cases {
        let stmt = client.prepare(query).await.unwrap();
        let mut times = Vec::new();
        
        for _ in 0..100 {
            let start = Instant::now();
            let _rows = client.query(&stmt, &[]).await.unwrap();
            times.push(start.elapsed());
        }
        
        analyze_timings(&format!("{} type conversion", type_name), &times);
    }
    
    // Profile binary vs text encoding
    println!("\nBinary vs Text encoding:");
    
    // Text format (default)
    let text_stmt = client
        .prepare("SELECT * FROM bench_protocol WHERE id = $1")
        .await
        .unwrap();
    
    let mut text_times = Vec::new();
    for i in 0..100 {
        let start = Instant::now();
        let _rows = client.query(&text_stmt, &[&i]).await.unwrap();
        text_times.push(start.elapsed());
    }
    analyze_timings("Text encoding", &text_times);
}

#[instrument(skip(_client))]
async fn profile_message_batching(_client: &tokio_postgres::Client) {
    println!("\n--- Message Batching Profiling ---");
    println!("Skipped - transaction borrowing not supported in test");
    
    // // Test transaction with multiple statements
    // let mut batch_times = Vec::new();
    // for i in 0..100 {
    //     let start = Instant::now();
    //     
    //     let tx = client.transaction().await.unwrap();
    //     for j in 0..10 {
    //         tx.execute(
    //             "INSERT INTO bench_protocol (id, text_col, int_col) VALUES ($1, $2, $3)",
    //             &[&(10000 + i * 10 + j), &format!("batch_{}_{}", i, j), &j],
    //         )
    //         .await
    //         .unwrap();
    //     }
    //     tx.commit().await.unwrap();
    //     
    //     batch_times.push(start.elapsed());
    // }
    // analyze_timings("10-statement transaction", &batch_times);
}

fn analyze_timings(label: &str, timings: &[Duration]) {
    let avg = average(timings);
    let min = timings.iter().min().unwrap();
    let max = timings.iter().max().unwrap();
    let p50 = percentile(timings, 50.0);
    let p95 = percentile(timings, 95.0);
    let p99 = percentile(timings, 99.0);
    
    println!("\n{}:", label);
    println!("  Average: {:?}", avg);
    println!("  Min:     {:?}", min);
    println!("  Max:     {:?}", max);
    println!("  P50:     {:?}", p50);
    println!("  P95:     {:?}", p95);
    println!("  P99:     {:?}", p99);
}

fn average(timings: &[Duration]) -> Duration {
    let sum: Duration = timings.iter().sum();
    sum / timings.len() as u32
}

fn percentile(timings: &[Duration], p: f64) -> Duration {
    let mut sorted = timings.to_vec();
    sorted.sort();
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx]
}

fn start_server() -> Child {
    Command::new("cargo")
        .args(&["run", "--", "--port", "5433"])
        .env("RUST_LOG", "pgsqlite=debug")
        .spawn()
        .expect("Failed to start server")
}