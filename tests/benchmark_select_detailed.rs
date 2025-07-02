use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use rusqlite::{params, Connection};

/// Detailed benchmark to profile SELECT query performance
#[tokio::test]
#[ignore]
async fn benchmark_select_detailed() {
    // Setup test database
    let db_path = "/tmp/pgsqlite_select_bench.db";
    let _ = std::fs::remove_file(db_path);
    
    let conn = Connection::open(db_path).unwrap();
    
    // Create test table with various data types
    conn.execute(
        "CREATE TABLE select_test (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER NOT NULL,
            score REAL NOT NULL,
            active BOOLEAN NOT NULL,
            description TEXT
        )",
        [],
    ).unwrap();
    
    // Create index for WHERE clause tests
    conn.execute("CREATE INDEX idx_value ON select_test(value)", []).unwrap();
    conn.execute("CREATE INDEX idx_name ON select_test(name)", []).unwrap();
    
    // Create pgsqlite schema table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS __pgsqlite_schema (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            column_type TEXT NOT NULL,
            PRIMARY KEY (table_name, column_name)
        )",
        [],
    ).unwrap();
    
    // Insert schema info
    for (col, typ) in &[
        ("id", "int4"),
        ("name", "text"),
        ("value", "int4"),
        ("score", "float8"),
        ("active", "bool"),
        ("description", "text"),
    ] {
        conn.execute(
            "INSERT INTO __pgsqlite_schema VALUES ('select_test', ?1, ?2)",
            params![col, typ],
        ).unwrap();
    }
    
    // Insert test data
    const NUM_ROWS: usize = 10000;
    println!("Inserting {} test rows...", NUM_ROWS);
    
    let mut stmt = conn.prepare(
        "INSERT INTO select_test (id, name, value, score, active, description) 
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
    ).unwrap();
    
    for i in 0..NUM_ROWS {
        stmt.execute(params![
            i as i32,
            format!("name_{}", i % 100),  // 100 unique names
            (i % 1000) as i32,            // values 0-999
            (i as f64) * 0.1,
            i % 2 == 0,
            format!("Description for row {}", i)
        ]).unwrap();
    }
    drop(stmt);
    drop(conn);
    
    // Start pgsqlite server
    let port = 25438;
    let _ = tokio::process::Command::new("pkill")
        .args(&["-f", &format!("pgsqlite.*{}", port)])
        .output()
        .await;
    
    let mut server = tokio::process::Command::new("cargo")
        .args(&["run", "--release", "--", "-d", db_path, "-p", &port.to_string(), "--log-level", "error"])
        .spawn()
        .expect("Failed to start server");
    
    // Wait for server to start
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Connect to server
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("Failed to connect to server");
    
    stream.set_nodelay(true).unwrap();
    perform_startup(&mut stream).await;
    
    println!("\n=== SELECT Query Performance Analysis ===\n");
    
    // Test different types of SELECT queries
    let test_queries = vec![
        ("Simple SELECT LIMIT 1", "SELECT * FROM select_test LIMIT 1"),
        ("SELECT by primary key", "SELECT * FROM select_test WHERE id = 5000"),
        ("SELECT with indexed column", "SELECT * FROM select_test WHERE value = 500"),
        ("SELECT with text match", "SELECT * FROM select_test WHERE name = 'name_50'"),
        ("SELECT with range", "SELECT * FROM select_test WHERE value BETWEEN 100 AND 200"),
        ("SELECT count", "SELECT COUNT(*) FROM select_test"),
        ("SELECT with aggregation", "SELECT value, COUNT(*) FROM select_test GROUP BY value LIMIT 10"),
        ("SELECT with ORDER BY", "SELECT * FROM select_test ORDER BY score DESC LIMIT 10"),
        ("SELECT specific columns", "SELECT id, name, value FROM select_test LIMIT 100"),
        ("SELECT with boolean filter", "SELECT * FROM select_test WHERE active = true LIMIT 100"),
    ];
    
    // Warm up
    for _ in 0..5 {
        send_query(&mut stream, "SELECT 1").await;
        read_until_ready(&mut stream).await;
    }
    
    // Benchmark each query type
    for (name, query) in &test_queries {
        println!("\nTesting: {}", name);
        println!("Query: {}", query);
        
        let mut times = Vec::new();
        let mut first_time = None;
        
        // Run each query multiple times
        for i in 0..20 {
            let start = Instant::now();
            send_query(&mut stream, query).await;
            let row_count = read_until_ready(&mut stream).await;
            let elapsed = start.elapsed();
            
            if i == 0 {
                first_time = Some(elapsed);
            }
            times.push(elapsed);
        }
        
        // Calculate statistics
        let avg_time = times.iter().sum::<Duration>() / times.len() as u32;
        let min_time = times.iter().min().unwrap();
        let max_time = times.iter().max().unwrap();
        
        println!("  First run:  {:?}", first_time.unwrap());
        println!("  Average:    {:?}", avg_time);
        println!("  Min:        {:?}", min_time);
        println!("  Max:        {:?}", max_time);
        
        // Check cache effectiveness
        if let Some(first) = first_time {
            let cache_speedup = first.as_secs_f64() / min_time.as_secs_f64();
            if cache_speedup > 1.2 {
                println!("  Cache speedup: {:.1}x", cache_speedup);
            }
        }
    }
    
    // Now benchmark the same queries with direct SQLite
    println!("\n\n=== Direct SQLite Performance (Baseline) ===\n");
    
    let conn = Connection::open(db_path).unwrap();
    
    for (name, query) in &test_queries {
        println!("\nTesting: {}", name);
        
        let mut times = Vec::new();
        
        for _ in 0..20 {
            let start = Instant::now();
            let mut stmt = conn.prepare(query).unwrap();
            let mut rows = stmt.query([]).unwrap();
            let mut count = 0;
            while let Some(_) = rows.next().unwrap() {
                count += 1;
            }
            let elapsed = start.elapsed();
            times.push(elapsed);
        }
        
        let avg_time = times.iter().sum::<Duration>() / times.len() as u32;
        println!("  SQLite avg: {:?}", avg_time);
    }
    
    // Profile specific bottlenecks
    println!("\n\n=== Protocol Overhead Analysis ===\n");
    
    // Measure just the protocol round-trip time
    let mut protocol_times = Vec::new();
    for _ in 0..100 {
        let start = Instant::now();
        send_query(&mut stream, "SELECT 1").await;
        read_until_ready(&mut stream).await;
        protocol_times.push(start.elapsed());
    }
    let protocol_avg = protocol_times.iter().sum::<Duration>() / protocol_times.len() as u32;
    println!("Protocol round-trip (SELECT 1): {:?}", protocol_avg);
    
    // Test with different result set sizes
    println!("\n=== Result Set Size Impact ===\n");
    for limit in &[1, 10, 100, 1000] {
        let query = format!("SELECT * FROM select_test LIMIT {}", limit);
        let mut times = Vec::new();
        
        for _ in 0..10 {
            let start = Instant::now();
            send_query(&mut stream, &query).await;
            let row_count = read_until_ready(&mut stream).await;
            times.push(start.elapsed());
        }
        
        let avg_time = times.iter().sum::<Duration>() / times.len() as u32;
        println!("  {} rows: {:?} ({:.2} µs/row)", 
            limit, avg_time, avg_time.as_micros() as f64 / *limit as f64);
    }
    
    // Kill server
    server.kill().await.unwrap();
    let _ = tokio::process::Command::new("pkill")
        .args(&["-f", &format!("pgsqlite.*{}", port)])
        .output()
        .await;
}

async fn perform_startup(stream: &mut TcpStream) {
    // Send startup message
    let mut startup = vec![];
    startup.extend_from_slice(&196608u32.to_be_bytes()); // Protocol version 3.0
    startup.extend_from_slice(b"user\0test\0database\0test\0\0");
    let len = ((startup.len() + 4) as u32).to_be_bytes();
    stream.write_all(&len).await.unwrap();
    stream.write_all(&startup).await.unwrap();
    
    // Read until ReadyForQuery
    read_until_ready(stream).await;
}

async fn send_query(stream: &mut TcpStream, query: &str) {
    let mut msg = vec![b'Q'];
    msg.extend_from_slice(&((query.len() + 5) as u32).to_be_bytes());
    msg.extend_from_slice(query.as_bytes());
    msg.push(0);
    stream.write_all(&msg).await.unwrap();
}

async fn read_until_ready(stream: &mut TcpStream) -> usize {
    let mut row_count = 0;
    
    loop {
        let mut msg_type = [0u8; 1];
        stream.read_exact(&mut msg_type).await.unwrap();
        
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_be_bytes(len_buf) as usize - 4;
        
        let mut data = vec![0u8; len];
        stream.read_exact(&mut data).await.unwrap();
        
        match msg_type[0] {
            b'D' => row_count += 1, // DataRow
            b'Z' => break,          // ReadyForQuery
            _ => {}
        }
    }
    
    row_count
}