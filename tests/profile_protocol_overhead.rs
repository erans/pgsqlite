use std::time::{Duration, Instant};
use tokio_postgres::NoTls;
use std::process::{Command, Child};
use std::thread;

/// Detailed protocol overhead profiling with timing breakdowns
#[tokio::test]
#[ignore] // Run with: cargo test profile_protocol_overhead -- --ignored --nocapture
async fn profile_protocol_overhead() {
    println!("\n=== Detailed Protocol Overhead Analysis ===");
    
    // Start server with detailed logging
    #[allow(clippy::zombie_processes)]
    let mut server = start_server();
    thread::sleep(Duration::from_secs(2));
    
    // Connect
    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=5433 user=postgres dbname=test",
        NoTls,
    )
    .await
    .expect("Failed to connect");
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });
    
    // Create test tables
    setup_test_data(&client).await;
    
    // Profile different aspects
    profile_message_encoding_overhead(&client).await;
    profile_value_conversion_overhead(&client).await;
    profile_protocol_vs_direct(&client).await;
    profile_cache_effectiveness(&client).await;
    
    // Cleanup
    cleanup(&client).await;
    server.kill().expect("Failed to kill server");
}

async fn setup_test_data(client: &tokio_postgres::Client) {
    // Create tables with different data types
    client.execute(
        "CREATE TABLE profile_simple (id INTEGER PRIMARY KEY, value TEXT)",
        &[],
    ).await.unwrap();
    
    client.execute(
        "CREATE TABLE profile_complex (
            id INTEGER PRIMARY KEY,
            text_short TEXT,
            text_long TEXT,
            int_small INTEGER,
            int_large INTEGER,
            float_val REAL,
            double_val DOUBLE PRECISION,
            bool_val BOOLEAN,
            bytes_small BYTEA,
            bytes_large BYTEA,
            numeric_val NUMERIC(19,4),
            timestamp_val TIMESTAMP
        )",
        &[],
    ).await.unwrap();
    
    // Insert test data
    for i in 0..1000 {
        client.execute(
            "INSERT INTO profile_simple (id, value) VALUES ($1, $2)",
            &[&i, &format!("value_{i}")],
        ).await.unwrap();
        
        let long_text = "x".repeat(1000);
        let large_bytes = vec![0u8; 1000];
        
        client.execute(
            "INSERT INTO profile_complex (
                id, text_short, text_long, int_small, int_large,
                float_val, double_val, bool_val, bytes_small, bytes_large,
                numeric_val, timestamp_val
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
            )",
            &[
                &i,
                &format!("short_{i}"),
                &long_text,
                &(i % 100),
                &(i * 1000000),
                &(i as f32 * 1.1),
                &(i as f64 * 2.2),
                &(i % 2 == 0),
                &vec![0u8; 10],
                &large_bytes,
                &rust_decimal::Decimal::new(i as i64 * 10000, 4),
                &chrono::Utc::now().naive_utc(),
            ],
        ).await.unwrap();
    }
}

async fn profile_message_encoding_overhead(client: &tokio_postgres::Client) {
    println!("\n--- Message Encoding Overhead ---");
    
    // Measure different message sizes
    let queries = vec![
        ("Minimal", "SELECT 1", 0),
        ("Small result", "SELECT id FROM profile_simple WHERE id = 1", 1),
        ("Medium result", "SELECT * FROM profile_simple WHERE id < 10", 10),
        ("Large result", "SELECT * FROM profile_simple WHERE id < 100", 100),
        ("Complex types", "SELECT * FROM profile_complex WHERE id < 10", 10),
    ];
    
    for (label, query, _expected_rows) in queries {
        let mut times = Vec::new();
        let mut row_counts = Vec::new();
        
        for _ in 0..100 {
            let start = Instant::now();
            let rows = client.simple_query(query).await.unwrap();
            let elapsed = start.elapsed();
            
            let row_count = rows.iter()
                .filter(|r| matches!(r, tokio_postgres::SimpleQueryMessage::Row(_)))
                .count();
            
            times.push(elapsed);
            row_counts.push(row_count);
        }
        
        let avg_time = average(&times);
        let avg_rows = row_counts.iter().sum::<usize>() as f64 / row_counts.len() as f64;
        
        println!("\n{label}:");
        println!("  Average time: {avg_time:?}");
        println!("  Average rows: {avg_rows:.1}");
        if avg_rows > 0.0 {
            println!("  Time per row: {:?}", Duration::from_secs_f64(avg_time.as_secs_f64() / avg_rows));
        }
    }
}

async fn profile_value_conversion_overhead(client: &tokio_postgres::Client) {
    println!("\n--- Value Conversion Overhead ---");
    
    // Test specific type conversions
    let type_queries = vec![
        ("INTEGER", "SELECT int_small, int_large FROM profile_complex WHERE id < 100"),
        ("TEXT", "SELECT text_short, text_long FROM profile_complex WHERE id < 100"),
        ("FLOAT/DOUBLE", "SELECT float_val, double_val FROM profile_complex WHERE id < 100"),
        ("BOOLEAN", "SELECT bool_val FROM profile_complex WHERE id < 100"),
        ("BYTEA", "SELECT bytes_small, bytes_large FROM profile_complex WHERE id < 100"),
        ("NUMERIC", "SELECT numeric_val FROM profile_complex WHERE id < 100"),
        ("TIMESTAMP", "SELECT timestamp_val FROM profile_complex WHERE id < 100"),
    ];
    
    for (type_name, query) in type_queries {
        let stmt = client.prepare(query).await.unwrap();
        let mut times = Vec::new();
        
        for _ in 0..50 {
            let start = Instant::now();
            let rows = client.query(&stmt, &[]).await.unwrap();
            let elapsed = start.elapsed();
            
            // Force value parsing
            for row in &rows {
                for i in 0..row.len() {
                    let _: Option<String> = row.get(i);
                }
            }
            
            times.push(elapsed);
        }
        
        println!("\n{type_name} conversion:");
        println!("  Average: {:?}", average(&times));
        println!("  Per 100 rows: {:?}", average(&times));
    }
}

async fn profile_protocol_vs_direct(client: &tokio_postgres::Client) {
    println!("\n--- Protocol Translation Overhead ---");
    
    // Compare simple vs extended protocol
    let query = "SELECT * FROM profile_simple WHERE id = $1";
    
    // Extended protocol
    let stmt = client.prepare(query).await.unwrap();
    let mut extended_times = Vec::new();
    
    for i in 0..500 {
        let start = Instant::now();
        let _rows = client.query(&stmt, &[&i]).await.unwrap();
        extended_times.push(start.elapsed());
    }
    
    // Simple protocol
    let mut simple_times = Vec::new();
    for i in 0..500 {
        let query = format!("SELECT * FROM profile_simple WHERE id = {i}");
        let start = Instant::now();
        let _rows = client.simple_query(&query).await.unwrap();
        simple_times.push(start.elapsed());
    }
    
    let extended_avg = average(&extended_times);
    let simple_avg = average(&simple_times);
    
    println!("\nProtocol comparison:");
    println!("  Extended protocol: {extended_avg:?}");
    println!("  Simple protocol:   {simple_avg:?}");
    println!("  Extended overhead: {:.1}%", 
        (extended_avg.as_secs_f64() - simple_avg.as_secs_f64()) / simple_avg.as_secs_f64() * 100.0);
    
    // Measure prepared statement benefit
    println!("\nPrepared statement benefit:");
    
    // First execution (parse + execute)
    let mut first_times = Vec::new();
    for i in 1000..1100 {
        let query = format!("SELECT * FROM profile_simple WHERE id = {} -- {}", i % 100, i);
        let start = Instant::now();
        let _stmt = client.prepare(&query).await.unwrap();
        first_times.push(start.elapsed());
    }
    
    // Reused execution
    let reused_stmt = client.prepare("SELECT * FROM profile_simple WHERE id = $1").await.unwrap();
    let mut reused_times = Vec::new();
    for i in 0..100 {
        let start = Instant::now();
        let _rows = client.query(&reused_stmt, &[&i]).await.unwrap();
        reused_times.push(start.elapsed());
    }
    
    println!("  First execution:  {:?}", average(&first_times));
    println!("  Reused prepared:  {:?}", average(&reused_times));
    println!("  Speedup: {:.1}x", average(&first_times).as_secs_f64() / average(&reused_times).as_secs_f64());
}

async fn profile_cache_effectiveness(client: &tokio_postgres::Client) {
    println!("\n--- Cache Effectiveness ---");
    
    // Test query plan cache
    let queries = vec![
        "SELECT * FROM profile_simple WHERE id = 1",
        "SELECT * FROM profile_simple WHERE id = 2",
        "SELECT * FROM profile_simple WHERE id = 3",
    ];
    
    // Cold cache
    let mut cold_times = Vec::new();
    for query in &queries {
        let start = Instant::now();
        let _rows = client.simple_query(query).await.unwrap();
        cold_times.push(start.elapsed());
    }
    
    // Warm cache (repeat same queries)
    let mut warm_times = Vec::new();
    for _ in 0..10 {
        for query in &queries {
            let start = Instant::now();
            let _rows = client.simple_query(query).await.unwrap();
            warm_times.push(start.elapsed());
        }
    }
    
    println!("\nQuery plan cache:");
    println!("  Cold cache: {:?}", average(&cold_times));
    println!("  Warm cache: {:?}", average(&warm_times));
    println!("  Speedup: {:.1}x", average(&cold_times).as_secs_f64() / average(&warm_times).as_secs_f64());
    
    // Test row description cache
    let stmt1 = client.prepare("SELECT * FROM profile_complex WHERE id = $1").await.unwrap();
    let stmt2 = client.prepare("SELECT * FROM profile_complex WHERE id = $1 -- cached").await.unwrap();
    
    let mut first_desc_times = Vec::new();
    let mut cached_desc_times = Vec::new();
    
    for i in 0..100 {
        // First time seeing this result structure
        let start = Instant::now();
        let _rows = client.query(&stmt1, &[&i]).await.unwrap();
        first_desc_times.push(start.elapsed());
        
        // Cached row description
        let start = Instant::now();
        let _rows = client.query(&stmt2, &[&i]).await.unwrap();
        cached_desc_times.push(start.elapsed());
    }
    
    println!("\nRow description cache:");
    println!("  First query:  {:?}", average(&first_desc_times));
    println!("  Cached query: {:?}", average(&cached_desc_times));
}

async fn cleanup(client: &tokio_postgres::Client) {
    client.execute("DROP TABLE IF EXISTS profile_simple", &[]).await.ok();
    client.execute("DROP TABLE IF EXISTS profile_complex", &[]).await.ok();
}

fn average(times: &[Duration]) -> Duration {
    let sum: Duration = times.iter().sum();
    sum / times.len() as u32
}

fn start_server() -> Child {
    Command::new("cargo")
        .args(["run", "--", "--port", "5433"])
        .env("RUST_LOG", "pgsqlite=info")
        .spawn()
        .expect("Failed to start server")
}