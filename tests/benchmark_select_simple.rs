use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use rusqlite::{params, Connection};

/// Simple benchmark to measure SELECT query overhead
#[tokio::test]
#[ignore]
async fn benchmark_select_simple() {
    // Start pgsqlite server with in-memory database
    let port = 25439;
    let _ = tokio::process::Command::new("pkill")
        .args(&["-f", &format!("pgsqlite.*{}", port)])
        .output()
        .await;
    
    let mut server = tokio::process::Command::new("cargo")
        .args(&["run", "--release", "--", "-p", &port.to_string(), "--in-memory", "--log-level", "error"])
        .spawn()
        .expect("Failed to start server");
    
    // Wait for server to start
    tokio::time::sleep(Duration::from_secs(3)).await;
    
    // Connect to server
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("Failed to connect to server");
    
    stream.set_nodelay(true).unwrap();
    perform_startup(&mut stream).await;
    
    println!("\n=== SELECT Query Overhead Analysis ===\n");
    
    // Create table through pgsqlite to ensure metadata is correct
    println!("Creating test table...");
    send_query(&mut stream, 
        "CREATE TABLE test_select (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER NOT NULL,
            score REAL NOT NULL,
            active BOOLEAN NOT NULL
        )"
    ).await;
    read_until_ready(&mut stream).await;
    
    // Insert test data using batch INSERT for efficiency
    println!("Inserting test data...");
    const BATCH_SIZE: usize = 100;
    const NUM_BATCHES: usize = 100;
    const TOTAL_ROWS: usize = BATCH_SIZE * NUM_BATCHES;
    
    for batch in 0..NUM_BATCHES {
        let mut values = Vec::new();
        for i in 0..BATCH_SIZE {
            let id = batch * BATCH_SIZE + i;
            values.push(format!(
                "({}, 'name_{}', {}, {}, {})",
                id,
                id % 100,
                id % 1000,
                (id as f64) * 0.1,
                if id % 2 == 0 { "true" } else { "false" }
            ));
        }
        
        let insert_query = format!(
            "INSERT INTO test_select (id, name, value, score, active) VALUES {}",
            values.join(", ")
        );
        
        send_query(&mut stream, &insert_query).await;
        read_until_ready(&mut stream).await;
    }
    
    println!("Inserted {} rows\n", TOTAL_ROWS);
    
    // Test queries
    let test_queries = vec![
        ("SELECT 1", "Protocol round-trip baseline"),
        ("SELECT * FROM test_select LIMIT 1", "Single row fetch"),
        ("SELECT * FROM test_select WHERE id = 5000", "Primary key lookup"),
        ("SELECT * FROM test_select WHERE value = 500 LIMIT 10", "Indexed column search"),
        ("SELECT COUNT(*) FROM test_select", "Aggregate query"),
        ("SELECT id, name FROM test_select LIMIT 100", "Specific columns"),
        ("SELECT * FROM test_select ORDER BY score DESC LIMIT 10", "ORDER BY query"),
    ];
    
    // Warm up
    println!("Warming up...");
    for _ in 0..10 {
        send_query(&mut stream, "SELECT 1").await;
        read_until_ready(&mut stream).await;
    }
    
    println!("\n--- pgsqlite Performance ---\n");
    
    // Benchmark each query
    for (query, description) in &test_queries {
        let mut times = Vec::new();
        let mut first_time = None;
        
        // Run 20 times
        for i in 0..20 {
            let start = Instant::now();
            send_query(&mut stream, query).await;
            let _row_count = read_until_ready(&mut stream).await;
            let elapsed = start.elapsed();
            
            if i == 0 {
                first_time = Some(elapsed);
            }
            times.push(elapsed);
        }
        
        let avg_time = times.iter().sum::<Duration>() / times.len() as u32;
        let min_time = times.iter().min().unwrap();
        
        println!("{}", description);
        println!("  Query: {}", query);
        println!("  First: {:?}, Avg: {:?}, Min: {:?}", first_time.unwrap(), avg_time, min_time);
        
        if let Some(first) = first_time {
            let cache_speedup = first.as_secs_f64() / min_time.as_secs_f64();
            if cache_speedup > 1.2 {
                println!("  Cache speedup: {:.1}x", cache_speedup);
            }
        }
        println!();
    }
    
    // Now test direct SQLite for comparison
    println!("\n--- Direct SQLite Performance (Baseline) ---\n");
    
    // Connect directly to the in-memory database - this won't work with pgsqlite's in-memory mode
    // So we'll create our own
    let conn = Connection::open_in_memory().unwrap();
    
    // Create same table
    conn.execute(
        "CREATE TABLE test_select (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER NOT NULL,
            score REAL NOT NULL,
            active BOOLEAN NOT NULL
        )",
        [],
    ).unwrap();
    
    // Insert same data
    let mut stmt = conn.prepare(
        "INSERT INTO test_select (id, name, value, score, active) VALUES (?1, ?2, ?3, ?4, ?5)"
    ).unwrap();
    
    for i in 0..TOTAL_ROWS {
        stmt.execute(params![
            i as i32,
            format!("name_{}", i % 100),
            (i % 1000) as i32,
            (i as f64) * 0.1,
            i % 2 == 0,
        ]).unwrap();
    }
    drop(stmt);
    
    // Benchmark same queries
    for (query, description) in &test_queries {
        if *query == "SELECT 1" {
            continue; // Skip protocol test
        }
        
        let mut times = Vec::new();
        
        for _ in 0..20 {
            let start = Instant::now();
            let mut stmt = conn.prepare(query).unwrap();
            let mut rows = stmt.query([]).unwrap();
            let mut _count = 0;
            while let Some(_) = rows.next().unwrap() {
                _count += 1;
            }
            let elapsed = start.elapsed();
            times.push(elapsed);
        }
        
        let avg_time = times.iter().sum::<Duration>() / times.len() as u32;
        println!("{}: {:?}", description, avg_time);
    }
    
    println!("\n--- Overhead Analysis ---\n");
    
    // Test with different result sizes
    for limit in &[1, 10, 100, 1000] {
        let query = format!("SELECT * FROM test_select LIMIT {}", limit);
        
        // pgsqlite timing
        let mut pg_times = Vec::new();
        for _ in 0..10 {
            let start = Instant::now();
            send_query(&mut stream, &query).await;
            let _ = read_until_ready(&mut stream).await;
            pg_times.push(start.elapsed());
        }
        let pg_avg = pg_times.iter().sum::<Duration>() / pg_times.len() as u32;
        
        // SQLite timing
        let mut sq_times = Vec::new();
        for _ in 0..10 {
            let start = Instant::now();
            let mut stmt = conn.prepare(&query).unwrap();
            let mut rows = stmt.query([]).unwrap();
            while let Some(_) = rows.next().unwrap() {}
            sq_times.push(start.elapsed());
        }
        let sq_avg = sq_times.iter().sum::<Duration>() / sq_times.len() as u32;
        
        let overhead = pg_avg.as_secs_f64() / sq_avg.as_secs_f64();
        println!("{} rows: pgsqlite {:?}, SQLite {:?}, overhead {:.1}x", 
            limit, pg_avg, sq_avg, overhead);
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