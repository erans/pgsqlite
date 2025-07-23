use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use rusqlite::{params, Connection};

/// Benchmark to compare single vs batch INSERT performance
#[tokio::test]
#[ignore]
async fn benchmark_batch_insert_performance() {
    // Test parameters
    const TOTAL_ROWS: usize = 1000;
    const BATCH_SIZES: &[usize] = &[1, 10, 50, 100, 500, 1000];
    
    // Setup test database
    let db_path = "/tmp/pgsqlite_batch_insert_bench.db";
    let _ = std::fs::remove_file(db_path);
    
    let conn = Connection::open(db_path).unwrap();
    
    // Create test table
    conn.execute(
        "CREATE TABLE batch_test (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER NOT NULL,
            description TEXT
        )",
        [],
    ).unwrap();
    
    drop(conn); // Close connection before starting server
    
    // First run migration
    let output = tokio::process::Command::new("cargo")
        .args(["run", "--release", "--", "-d", db_path, "--migrate"])
        .output()
        .await
        .expect("Failed to run migration");
    
    if !output.status.success() {
        panic!("Migration failed: {}", String::from_utf8_lossy(&output.stderr));
    }
    
    // Now insert schema info for our test table
    let conn = Connection::open(db_path).unwrap();
    conn.execute("INSERT OR IGNORE INTO __pgsqlite_schema (table_name, column_name, pg_type) VALUES ('batch_test', 'id', 'int4')", []).unwrap();
    conn.execute("INSERT OR IGNORE INTO __pgsqlite_schema (table_name, column_name, pg_type) VALUES ('batch_test', 'name', 'text')", []).unwrap();
    conn.execute("INSERT OR IGNORE INTO __pgsqlite_schema (table_name, column_name, pg_type) VALUES ('batch_test', 'value', 'int4')", []).unwrap();
    conn.execute("INSERT OR IGNORE INTO __pgsqlite_schema (table_name, column_name, pg_type) VALUES ('batch_test', 'description', 'text')", []).unwrap();
    drop(conn);
    
    // Start pgsqlite server
    let port = 25437;
    let _ = tokio::process::Command::new("pkill")
        .args(["-f", &format!("pgsqlite.*{port}")])
        .output()
        .await;
    
    let mut server = tokio::process::Command::new("cargo")
        .args(["run", "--release", "--", "-d", db_path, "-p", &port.to_string(), "--log-level", "error"])
        .spawn()
        .expect("Failed to start server");
    
    // Wait for server to start
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Connect to server
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .expect("Failed to connect to server");
    
    // Set TCP_NODELAY for low latency
    stream.set_nodelay(true).unwrap();
    
    // Perform startup handshake
    perform_startup(&mut stream).await;
    
    println!("\n=== Batch INSERT Performance Benchmark ===\n");
    println!("Total rows to insert: {TOTAL_ROWS}");
    println!("Testing batch sizes: {BATCH_SIZES:?}\n");
    
    // Benchmark each batch size
    let mut results = Vec::new();
    
    for &batch_size in BATCH_SIZES {
        // Clear table before each test
        send_query(&mut stream, "DELETE FROM batch_test").await;
        read_until_ready(&mut stream).await;
        
        let num_batches = TOTAL_ROWS / batch_size;
        let remainder = TOTAL_ROWS % batch_size;
        
        println!("Testing batch size {} ({} batches{})", 
            batch_size, 
            num_batches,
            if remainder > 0 { format!(" + {remainder} rows") } else { String::new() }
        );
        
        let start = Instant::now();
        
        // Insert full batches
        for batch_num in 0..num_batches {
            let query = build_batch_insert_query(batch_num * batch_size, batch_size);
            send_query(&mut stream, &query).await;
            read_until_ready(&mut stream).await;
        }
        
        // Insert remainder if any
        if remainder > 0 {
            let query = build_batch_insert_query(num_batches * batch_size, remainder);
            send_query(&mut stream, &query).await;
            read_until_ready(&mut stream).await;
        }
        
        let elapsed = start.elapsed();
        let per_row = elapsed / TOTAL_ROWS as u32;
        let rows_per_sec = TOTAL_ROWS as f64 / elapsed.as_secs_f64();
        
        results.push((batch_size, elapsed, per_row, rows_per_sec));
        
        println!("  Total time: {elapsed:?}");
        println!("  Per row: {per_row:?}");
        println!("  Rows/sec: {rows_per_sec:.0}");
        println!();
    }
    
    // Also benchmark direct SQLite for comparison
    println!("Benchmarking direct SQLite...");
    let conn = Connection::open(db_path).unwrap();
    
    // Single-row inserts
    conn.execute("DELETE FROM batch_test", []).unwrap();
    let start = Instant::now();
    for i in 0..TOTAL_ROWS {
        conn.execute(
            "INSERT INTO batch_test (id, name, value, description) VALUES (?1, ?2, ?3, ?4)",
            params![i as i32, format!("test_{}", i), i as i32, format!("Description for row {}", i)],
        ).unwrap();
    }
    let sqlite_single_elapsed = start.elapsed();
    
    // Batch insert (100 rows at a time)
    conn.execute("DELETE FROM batch_test", []).unwrap();
    let start = Instant::now();
    for batch in 0..(TOTAL_ROWS / 100) {
        let mut query = String::from("INSERT INTO batch_test (id, name, value, description) VALUES ");
        for i in 0..100 {
            let row_id = batch * 100 + i;
            if i > 0 { query.push_str(", "); }
            query.push_str(&format!("({row_id}, 'test_{row_id}', {row_id}, 'Description for row {row_id}')"));
        }
        conn.execute(&query, []).unwrap();
    }
    let sqlite_batch_elapsed = start.elapsed();
    
    // Print summary
    println!("\n=== Summary ===\n");
    println!("Direct SQLite:");
    println!("  Single-row INSERTs: {:?} ({:.0} rows/sec)", 
        sqlite_single_elapsed, 
        TOTAL_ROWS as f64 / sqlite_single_elapsed.as_secs_f64());
    println!("  Batch INSERTs (100): {:?} ({:.0} rows/sec)", 
        sqlite_batch_elapsed,
        TOTAL_ROWS as f64 / sqlite_batch_elapsed.as_secs_f64());
    
    println!("\npgsqlite via PostgreSQL protocol:");
    for (batch_size, elapsed, _per_row, rows_per_sec) in &results {
        let speedup = results[0].1.as_secs_f64() / elapsed.as_secs_f64();
        println!("  Batch size {batch_size:4}: {elapsed:?} ({rows_per_sec:6.0} rows/sec, {speedup:.1}x speedup vs single)");
    }
    
    // Calculate overhead vs SQLite
    println!("\nOverhead vs direct SQLite:");
    let sqlite_best = sqlite_batch_elapsed.min(sqlite_single_elapsed);
    for (batch_size, elapsed, _, _) in &results {
        let overhead = elapsed.as_secs_f64() / sqlite_best.as_secs_f64();
        println!("  Batch size {batch_size:4}: {overhead:.1}x overhead");
    }
    
    // Find optimal batch size
    let (optimal_size, optimal_time, _, optimal_rate) = results.iter()
        .min_by_key(|(_, elapsed, _, _)| elapsed.as_nanos())
        .unwrap();
    println!("\nOptimal batch size: {optimal_size} ({optimal_time:?}, {optimal_rate:.0} rows/sec)");
    
    // Kill server
    server.kill().await.unwrap();
    let _ = tokio::process::Command::new("pkill")
        .args(["-f", &format!("pgsqlite.*{port}")])
        .output()
        .await;
}

fn build_batch_insert_query(start_id: usize, batch_size: usize) -> String {
    let mut query = String::from("INSERT INTO batch_test (id, name, value, description) VALUES ");
    
    for i in 0..batch_size {
        if i > 0 {
            query.push_str(", ");
        }
        let id = start_id + i;
        query.push_str(&format!(
            "({id}, 'test_{id}', {id}, 'Description for row {id}')"
        ));
    }
    
    query
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

async fn read_until_ready(stream: &mut TcpStream) {
    loop {
        let mut msg_type = [0u8; 1];
        stream.read_exact(&mut msg_type).await.unwrap();
        
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_be_bytes(len_buf) as usize - 4;
        
        let mut data = vec![0u8; len];
        stream.read_exact(&mut data).await.unwrap();
        
        if msg_type[0] == b'Z' { // ReadyForQuery
            break;
        }
    }
}