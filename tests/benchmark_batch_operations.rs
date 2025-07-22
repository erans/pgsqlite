use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use rusqlite::Connection;

/// Benchmark to establish baseline UPDATE/DELETE performance and test batch operations
#[tokio::test]
#[ignore]
async fn benchmark_batch_operations() {
    // Test parameters
    const TOTAL_ROWS: usize = 1000;
    const OPERATIONS: usize = 500;
    
    // Setup test database
    let db_path = "/tmp/pgsqlite_batch_ops_bench.db";
    let _ = std::fs::remove_file(db_path);
    
    let conn = Connection::open(db_path).unwrap();
    
    // Create test table
    conn.execute(
        "CREATE TABLE batch_ops_test (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER NOT NULL,
            status TEXT DEFAULT 'active'
        )",
        [],
    ).unwrap();
    
    // Insert test data
    for i in 0..TOTAL_ROWS {
        conn.execute(
            "INSERT INTO batch_ops_test (id, name, value) VALUES (?1, ?2, ?3)",
            rusqlite::params![i as i32, format!("item_{}", i), i as i32 * 10],
        ).unwrap();
    }
    
    drop(conn); // Close connection before starting server
    
    // Run migration
    let output = tokio::process::Command::new("cargo")
        .args(&["run", "--release", "--bin", "pgsqlite", "--", "-d", db_path, "--migrate"])
        .output()
        .await
        .expect("Failed to run migration");
    
    if !output.status.success() {
        panic!("Migration failed: {}", String::from_utf8_lossy(&output.stderr));
    }
    
    // Insert schema info
    let conn = Connection::open(db_path).unwrap();
    conn.execute("INSERT OR IGNORE INTO __pgsqlite_schema (table_name, column_name, pg_type) VALUES ('batch_ops_test', 'id', 'int4')", []).unwrap();
    conn.execute("INSERT OR IGNORE INTO __pgsqlite_schema (table_name, column_name, pg_type) VALUES ('batch_ops_test', 'name', 'text')", []).unwrap();
    conn.execute("INSERT OR IGNORE INTO __pgsqlite_schema (table_name, column_name, pg_type) VALUES ('batch_ops_test', 'value', 'int4')", []).unwrap();
    conn.execute("INSERT OR IGNORE INTO __pgsqlite_schema (table_name, column_name, pg_type) VALUES ('batch_ops_test', 'status', 'text')", []).unwrap();
    drop(conn);
    
    // Start pgsqlite server
    let port = 25438;
    let _ = tokio::process::Command::new("pkill")
        .args(&["-f", &format!("pgsqlite.*{}", port)])
        .output()
        .await;
    
    let mut server = tokio::process::Command::new("cargo")
        .args(&["run", "--release", "--bin", "pgsqlite", "--", "-d", db_path, "-p", &port.to_string(), "--log-level", "error"])
        .spawn()
        .expect("Failed to start server");
    
    // Wait for server to start
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Connect to server
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("Failed to connect to server");
    
    stream.set_nodelay(true).unwrap();
    
    // Perform startup handshake
    perform_startup(&mut stream).await;
    
    println!("\n=== UPDATE/DELETE Performance Baseline ===\n");
    println!("Total rows: {}", TOTAL_ROWS);
    println!("Operations per test: {}\n", OPERATIONS);
    
    // Benchmark single UPDATE operations
    println!("--- Single UPDATE Operations ---");
    let start = Instant::now();
    for i in 0..OPERATIONS {
        let query = format!("UPDATE batch_ops_test SET value = {} WHERE id = {}", i * 20, i);
        send_query(&mut stream, &query).await;
        read_until_ready(&mut stream).await;
    }
    let single_update_time = start.elapsed();
    println!("{} single UPDATEs: {:?}", OPERATIONS, single_update_time);
    println!("  Per operation: {:.3}ms", single_update_time.as_secs_f64() * 1000.0 / OPERATIONS as f64);
    println!("  Operations/sec: {:.0}", OPERATIONS as f64 / single_update_time.as_secs_f64());
    
    // Benchmark single DELETE operations
    println!("\n--- Single DELETE Operations ---");
    // First, re-insert some data to delete
    for i in TOTAL_ROWS..(TOTAL_ROWS + OPERATIONS) {
        let query = format!("INSERT INTO batch_ops_test (id, name, value) VALUES ({}, 'temp_{}', {})", 
            i, i, i * 10);
        send_query(&mut stream, &query).await;
        read_until_ready(&mut stream).await;
    }
    
    let start = Instant::now();
    for i in TOTAL_ROWS..(TOTAL_ROWS + OPERATIONS) {
        let query = format!("DELETE FROM batch_ops_test WHERE id = {}", i);
        send_query(&mut stream, &query).await;
        read_until_ready(&mut stream).await;
    }
    let single_delete_time = start.elapsed();
    println!("{} single DELETEs: {:?}", OPERATIONS, single_delete_time);
    println!("  Per operation: {:.3}ms", single_delete_time.as_secs_f64() * 1000.0 / OPERATIONS as f64);
    println!("  Operations/sec: {:.0}", OPERATIONS as f64 / single_delete_time.as_secs_f64());
    
    // Benchmark UPDATE with WHERE IN clause (batch-like)
    println!("\n--- Batch-like UPDATE Operations (WHERE IN) ---");
    let batch_sizes = vec![10, 50, 100];
    
    for batch_size in batch_sizes {
        let num_batches = OPERATIONS / batch_size;
        println!("\nBatch size {} ({} batches):", batch_size, num_batches);
        
        let start = Instant::now();
        for batch in 0..num_batches {
            let start_id = batch * batch_size;
            let ids: Vec<String> = (start_id..start_id + batch_size)
                .map(|id| id.to_string())
                .collect();
            let query = format!(
                "UPDATE batch_ops_test SET value = value + 100 WHERE id IN ({})",
                ids.join(",")
            );
            send_query(&mut stream, &query).await;
            read_until_ready(&mut stream).await;
        }
        let batch_update_time = start.elapsed();
        
        let speedup = single_update_time.as_secs_f64() / batch_update_time.as_secs_f64();
        println!("  Total time: {:?}", batch_update_time);
        println!("  Per batch: {:.3}ms", batch_update_time.as_secs_f64() * 1000.0 / num_batches as f64);
        println!("  Per row: {:.3}ms", batch_update_time.as_secs_f64() * 1000.0 / OPERATIONS as f64);
        println!("  Speedup vs single: {:.1}x", speedup);
    }
    
    // Benchmark DELETE with WHERE IN clause (batch-like)
    println!("\n--- Batch-like DELETE Operations (WHERE IN) ---");
    
    // Re-insert data for deletion
    for i in TOTAL_ROWS..(TOTAL_ROWS + OPERATIONS) {
        let query = format!("INSERT INTO batch_ops_test (id, name, value) VALUES ({}, 'temp_{}', {})", 
            i, i, i * 10);
        send_query(&mut stream, &query).await;
        read_until_ready(&mut stream).await;
    }
    
    for batch_size in vec![10, 50, 100] {
        let num_batches = OPERATIONS / batch_size;
        println!("\nBatch size {} ({} batches):", batch_size, num_batches);
        
        // Re-insert data for this test
        for i in 0..OPERATIONS {
            let id = TOTAL_ROWS * 2 + i;
            let query = format!("INSERT INTO batch_ops_test (id, name, value) VALUES ({}, 'batch_{}', {})", 
                id, id, id * 10);
            send_query(&mut stream, &query).await;
            read_until_ready(&mut stream).await;
        }
        
        let start = Instant::now();
        for batch in 0..num_batches {
            let start_id = TOTAL_ROWS * 2 + batch * batch_size;
            let ids: Vec<String> = (start_id..start_id + batch_size)
                .map(|id| id.to_string())
                .collect();
            let query = format!(
                "DELETE FROM batch_ops_test WHERE id IN ({})",
                ids.join(",")
            );
            send_query(&mut stream, &query).await;
            read_until_ready(&mut stream).await;
        }
        let batch_delete_time = start.elapsed();
        
        let speedup = single_delete_time.as_secs_f64() / batch_delete_time.as_secs_f64();
        println!("  Total time: {:?}", batch_delete_time);
        println!("  Per batch: {:.3}ms", batch_delete_time.as_secs_f64() * 1000.0 / num_batches as f64);
        println!("  Per row: {:.3}ms", batch_delete_time.as_secs_f64() * 1000.0 / OPERATIONS as f64);
        println!("  Speedup vs single: {:.1}x", speedup);
    }
    
    // Direct SQLite comparison
    println!("\n--- Direct SQLite Comparison ---");
    let conn = Connection::open(db_path).unwrap();
    
    // Single UPDATE
    let start = Instant::now();
    for i in 0..OPERATIONS {
        conn.execute(
            "UPDATE batch_ops_test SET value = ?1 WHERE id = ?2",
            rusqlite::params![i * 30, i],
        ).unwrap();
    }
    let sqlite_update_time = start.elapsed();
    println!("SQLite {} single UPDATEs: {:?}", OPERATIONS, sqlite_update_time);
    
    // Batch UPDATE with WHERE IN
    let start = Instant::now();
    for batch in 0..(OPERATIONS / 100) {
        let start_id = batch * 100;
        let ids: Vec<String> = (start_id..start_id + 100)
            .map(|id| id.to_string())
            .collect();
        conn.execute(
            &format!("UPDATE batch_ops_test SET value = value + 200 WHERE id IN ({})", ids.join(",")),
            [],
        ).unwrap();
    }
    let sqlite_batch_update_time = start.elapsed();
    println!("SQLite {} batch UPDATEs (100 per batch): {:?}", OPERATIONS / 100, sqlite_batch_update_time);
    
    // Calculate overheads
    println!("\n--- Performance Summary ---");
    println!("Single UPDATE overhead vs SQLite: {:.1}x", 
        single_update_time.as_secs_f64() / sqlite_update_time.as_secs_f64());
    
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