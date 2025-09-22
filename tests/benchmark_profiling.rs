#![allow(clippy::needless_range_loop)]

use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use rusqlite::{params, Connection};

/// Benchmark with detailed profiling to identify bottlenecks
#[tokio::test]
#[ignore]
async fn benchmark_with_profiling() {
    // Enable profiling
    pgsqlite::profiling::enable_profiling();
    pgsqlite::profiling::METRICS.reset();
    
    // Setup in-memory database
    let db_path = ":memory:";
    
    let conn = Connection::open(db_path).unwrap();
    
    // Create simple test table
    conn.execute(
        "CREATE TABLE profile_test (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER NOT NULL
        )",
        [],
    ).unwrap();
    
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
    conn.execute("INSERT INTO __pgsqlite_schema VALUES ('profile_test', 'id', 'int4')", []).unwrap();
    conn.execute("INSERT INTO __pgsqlite_schema VALUES ('profile_test', 'name', 'text')", []).unwrap();
    conn.execute("INSERT INTO __pgsqlite_schema VALUES ('profile_test', 'value', 'int4')", []).unwrap();
    
    // Insert test data
    const NUM_ROWS: usize = 1000;
    let mut stmt = conn.prepare("INSERT INTO profile_test (id, name, value) VALUES (?1, ?2, ?3)").unwrap();
    for i in 0..NUM_ROWS {
        stmt.execute(params![i as i32, format!("name_{}", i), i as i32]).unwrap();
    }
    drop(stmt);
    drop(conn);
    
    // Start pgsqlite server
    let port = 25440;
    let _ = tokio::process::Command::new("pkill")
        .args(["-f", &format!("pgsqlite.*{port}")])
        .output()
        .await;
    
    let mut server = tokio::process::Command::new("cargo")
        .args(["run", "--release", "--", "--in-memory", "-p", &port.to_string(), "--log-level", "error"])
        .spawn()
        .expect("Failed to start server");
    
    // Wait for server to start
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Connect to server
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .expect("Failed to connect to server");
    
    stream.set_nodelay(true).unwrap();
    perform_startup(&mut stream).await;
    
    println!("\n=== Profiling Query Performance ===\n");
    
    // Test queries
    let test_queries = vec![
        ("Simple SELECT", "SELECT * FROM profile_test WHERE id = 1"),
        ("SELECT with LIMIT", "SELECT * FROM profile_test LIMIT 10"),
        ("SELECT COUNT", "SELECT COUNT(*) FROM profile_test"),
        ("Simple INSERT", "INSERT INTO profile_test (id, name, value) VALUES (10001, 'test', 100)"),
        ("Simple UPDATE", "UPDATE profile_test SET value = 200 WHERE id = 1"),
        ("Simple DELETE", "DELETE FROM profile_test WHERE id = 10001"),
    ];
    
    // Warm up
    for _ in 0..10 {
        send_query(&mut stream, "SELECT 1").await;
        read_until_ready(&mut stream).await;
    }
    
    // Run each query type multiple times
    for (name, query) in &test_queries {
        println!("\nProfiling: {name}");
        
        for _ in 0..50 {
            send_query(&mut stream, query).await;
            read_until_ready(&mut stream).await;
        }
    }
    
    // Print profiling report
    println!("{}", pgsqlite::profiling::METRICS.report());
    
    // Test with raw SQLite for comparison
    println!("\n=== SQLite Direct Performance ===");
    let conn = Connection::open(":memory:").unwrap();
    
    // Re-create the test table for SQLite direct test
    conn.execute(
        "CREATE TABLE profile_test (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER NOT NULL
        )",
        [],
    ).unwrap();
    
    // Insert test data
    let mut stmt = conn.prepare("INSERT INTO profile_test (id, name, value) VALUES (?1, ?2, ?3)").unwrap();
    for i in 0..1000 {
        stmt.execute(params![{ i }, format!("name_{}", i), { i }]).unwrap();
    }
    drop(stmt);
    
    for (name, query) in &test_queries {
        let start = Instant::now();
        for _ in 0..50 {
            match conn.execute(query, []) {
                Ok(_) => {},
                Err(_) => {
                    // Query returns results, use prepare/query instead
                    let mut stmt = conn.prepare(query).unwrap();
                    let _rows: Vec<(i32, String, i32)> = stmt.query_map([], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    }).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
                }
            }
        }
        let elapsed = start.elapsed();
        println!("{}: {:.2}ms total, {:.2}µs avg", name, 
            elapsed.as_millis(), 
            elapsed.as_micros() as f64 / 50.0);
    }
    
    // Cleanup
    server.kill().await.unwrap();
}

async fn perform_startup(stream: &mut TcpStream) {
    // Send startup message
    let mut startup_msg = [0u8; 8];
    startup_msg[0..4].copy_from_slice(&196608i32.to_be_bytes()); // Protocol version
    startup_msg[4..8].copy_from_slice(&8i32.to_be_bytes()); // Message length
    
    // Add parameters
    let params = b"user\0postgres\0database\0test\0\0";
    let total_len = (8 + params.len()) as i32;
    
    let mut final_msg = vec![];
    final_msg.extend_from_slice(&total_len.to_be_bytes());
    final_msg.extend_from_slice(&startup_msg[0..4]);
    final_msg.extend_from_slice(params);
    
    stream.write_all(&final_msg).await.unwrap();
    
    // Read responses until ready
    let mut buf = vec![0u8; 4096];
    loop {
        let n = stream.read(&mut buf).await.unwrap();
        if n == 0 {
            panic!("Connection closed during startup");
        }
        
        // Look for ReadyForQuery message (type 'Z')
        for i in 0..n {
            if buf[i] == b'Z' {
                return;
            }
        }
    }
}

async fn send_query(stream: &mut TcpStream, query: &str) {
    let query_bytes = query.as_bytes();
    let msg_len = (4 + query_bytes.len() + 1) as i32;
    
    let mut msg = vec![b'Q'];
    msg.extend_from_slice(&msg_len.to_be_bytes());
    msg.extend_from_slice(query_bytes);
    msg.push(0); // Null terminator
    
    stream.write_all(&msg).await.unwrap();
}

async fn read_until_ready(stream: &mut TcpStream) -> usize {
    let mut buf = vec![0u8; 65536];
    let mut row_count = 0;
    
    loop {
        let n = stream.read(&mut buf).await.unwrap();
        if n == 0 {
            panic!("Connection closed while reading response");
        }
        
        // Parse messages
        let mut pos = 0;
        while pos < n {
            let msg_type = buf[pos];
            pos += 1;
            
            if pos + 4 > n {
                break;
            }
            
            let msg_len = i32::from_be_bytes([buf[pos], buf[pos+1], buf[pos+2], buf[pos+3]]) as usize;
            pos += 4;
            
            match msg_type {
                b'D' => row_count += 1, // DataRow
                b'Z' => return row_count, // ReadyForQuery
                _ => {}
            }
            
            pos += msg_len - 4;
        }
    }
}