use std::time::{Duration, Instant};
use tokio_postgres::NoTls;
use std::process::{Command, Child};
use std::thread;

/// Simple protocol profiling test
#[tokio::test]
#[ignore] // Run with: cargo test simple_protocol_profile -- --ignored --nocapture
async fn simple_protocol_profile() {
    println!("\n=== Simple Protocol Profiling ===");
    
    // Start server
    let mut server = start_server();
    thread::sleep(Duration::from_secs(3));
    
    // Connect
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
    client.execute(
        "CREATE TABLE protocol_test (id INTEGER PRIMARY KEY, value TEXT)",
        &[],
    ).await.unwrap();
    
    // Insert test data
    for i in 0..100 {
        client.execute(
            "INSERT INTO protocol_test (id, value) VALUES ($1, $2)",
            &[&i, &format!("value_{}", i)],
        ).await.unwrap();
    }
    
    println!("\n1. Simple Query Protocol:");
    let mut simple_times = Vec::new();
    for i in 0..100 {
        let query = format!("SELECT * FROM protocol_test WHERE id = {}", i);
        let start = Instant::now();
        let _rows = client.simple_query(&query).await.unwrap();
        simple_times.push(start.elapsed());
    }
    let simple_avg = average(&simple_times);
    println!("   Average: {:?}", simple_avg);
    
    println!("\n2. Extended Protocol (Prepared):");
    let stmt = client.prepare("SELECT * FROM protocol_test WHERE id = $1").await.unwrap();
    let mut extended_times = Vec::new();
    for i in 0..100 {
        let start = Instant::now();
        let _rows = client.query(&stmt, &[&i]).await.unwrap();
        extended_times.push(start.elapsed());
    }
    let extended_avg = average(&extended_times);
    println!("   Average: {:?}", extended_avg);
    println!("   Overhead vs simple: {:.1}%", 
        (extended_avg.as_secs_f64() - simple_avg.as_secs_f64()) / simple_avg.as_secs_f64() * 100.0);
    
    println!("\n3. Parse Message Overhead:");
    let mut parse_times = Vec::new();
    for i in 0..50 {
        let query = format!("SELECT * FROM protocol_test WHERE id = $1 -- {}", i);
        let start = Instant::now();
        let _stmt = client.prepare(&query).await.unwrap();
        parse_times.push(start.elapsed());
    }
    println!("   Average parse time: {:?}", average(&parse_times));
    
    println!("\n4. Different Result Sizes:");
    for size in &[1, 10, 50, 100] {
        let query = format!("SELECT * FROM protocol_test LIMIT {}", size);
        let mut times = Vec::new();
        for _ in 0..20 {
            let start = Instant::now();
            let rows = client.simple_query(&query).await.unwrap();
            times.push(start.elapsed());
            assert!(rows.len() > 0);
        }
        let avg = average(&times);
        println!("   {} rows: {:?} ({:.2} µs/row)", 
            size, avg, avg.as_micros() as f64 / *size as f64);
    }
    
    println!("\n5. Type Conversion Overhead:");
    
    // Create table with different types
    client.execute(
        "CREATE TABLE type_test (
            id INTEGER,
            text_val TEXT,
            int_val INTEGER,
            float_val REAL,
            bool_val BOOLEAN
        )",
        &[],
    ).await.unwrap();
    
    // Insert data
    for i in 0..100 {
        client.execute(
            "INSERT INTO type_test VALUES ($1, $2, $3, $4, $5)",
            &[&i, &format!("text_{}", i), &(i * 10), &(i as f32 * 1.5), &(i % 2 == 0)],
        ).await.unwrap();
    }
    
    // Test different types
    let type_queries = vec![
        ("INTEGER", "SELECT int_val FROM type_test WHERE id < 50"),
        ("TEXT", "SELECT text_val FROM type_test WHERE id < 50"),
        ("FLOAT", "SELECT float_val FROM type_test WHERE id < 50"),
        ("BOOLEAN", "SELECT bool_val FROM type_test WHERE id < 50"),
        ("ALL", "SELECT * FROM type_test WHERE id < 50"),
    ];
    
    for (type_name, query) in type_queries {
        let stmt = client.prepare(query).await.unwrap();
        let mut times = Vec::new();
        for _ in 0..20 {
            let start = Instant::now();
            let _rows = client.query(&stmt, &[]).await.unwrap();
            times.push(start.elapsed());
        }
        println!("   {} conversion: {:?}", type_name, average(&times));
    }
    
    // Cleanup
    client.execute("DROP TABLE protocol_test", &[]).await.ok();
    client.execute("DROP TABLE type_test", &[]).await.ok();
    server.kill().expect("Failed to kill server");
    
    println!("\n=== Summary ===");
    println!("Simple protocol avg: {:?}", simple_avg);
    println!("Extended protocol avg: {:?}", extended_avg);
    println!("Protocol translation adds ~{:.0} µs overhead per query", 
        (extended_avg.as_micros() - simple_avg.as_micros()) as f64);
}

fn average(times: &[Duration]) -> Duration {
    times.iter().sum::<Duration>() / times.len() as u32
}

fn start_server() -> Child {
    Command::new("cargo")
        .args(&["run", "--", "--port", "5433"])
        .spawn()
        .expect("Failed to start server")
}