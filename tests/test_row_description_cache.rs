use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Test RowDescription caching performance
#[tokio::test]
#[ignore] // Skip in normal test runs due to server startup requirement
async fn test_row_description_cache() {
    // Start pgsqlite server
    let port = 25446;
    let _ = tokio::process::Command::new("pkill")
        .args(&["-f", &format!("pgsqlite.*{}", port)])
        .output()
        .await;
    
    let mut server = tokio::process::Command::new("cargo")
        .args(&["run", "--release", "--", "-p", &port.to_string(), "--in-memory", "--log-level", "info"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("Failed to start server");
    
    // Wait for server to start with retries
    let mut connected = false;
    let max_retries = if std::env::var("CI").is_ok() { 60 } else { 20 }; // 30s in CI, 10s locally
    for i in 0..max_retries {
        tokio::time::sleep(Duration::from_millis(500)).await;
        if let Ok(_) = TcpStream::connect(format!("127.0.0.1:{}", port)).await {
            connected = true;
            println!("Server started after {} attempts", i + 1);
            break;
        }
    }
    
    if !connected {
        // Try to get output from server for debugging
        let output = server.wait_with_output().await.unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        
        panic!(
            "Failed to connect to server after {} seconds\nExit status: {:?}\nStdout:\n{}\nStderr:\n{}", 
            max_retries / 2,
            output.status,
            stdout,
            stderr
        );
    }
    
    // Connect to server
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("Failed to connect to server");
    
    stream.set_nodelay(true).unwrap();
    perform_startup(&mut stream).await;
    
    // Create test table
    send_query(&mut stream, 
        "CREATE TABLE cache_test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)"
    ).await;
    read_until_ready(&mut stream).await;
    
    // Insert test data
    send_query(&mut stream, 
        "INSERT INTO cache_test (id, name, value) VALUES (1, 'test1', 100), (2, 'test2', 200)"
    ).await;
    read_until_ready(&mut stream).await;
    
    println!("\n=== RowDescription Cache Test ===\n");
    
    // Test queries that should benefit from caching
    let test_queries = vec![
        "SELECT * FROM cache_test",
        "SELECT id, name FROM cache_test",
        "SELECT COUNT(*) FROM cache_test",
        "SELECT * FROM cache_test WHERE id = 1",
    ];
    
    for query in &test_queries {
        println!("Testing query: {}", query);
        
        // First execution - cache miss
        let start = Instant::now();
        send_query(&mut stream, query).await;
        let row_count = read_until_ready(&mut stream).await;
        let first_time = start.elapsed();
        
        // Second execution - should hit cache
        let start = Instant::now();
        send_query(&mut stream, query).await;
        let row_count2 = read_until_ready(&mut stream).await;
        let cached_time = start.elapsed();
        
        println!("  First execution: {:?} ({} rows)", first_time, row_count);
        println!("  Cached execution: {:?} ({} rows)", cached_time, row_count2);
        
        if cached_time < first_time {
            let speedup = first_time.as_secs_f64() / cached_time.as_secs_f64();
            println!("  Cache speedup: {:.1}x", speedup);
        }
        println!();
    }
    
    // Test cache with different column orders (should be different cache entries)
    println!("Testing different column orders:");
    let query1 = "SELECT id, name, value FROM cache_test";
    let query2 = "SELECT value, name, id FROM cache_test";
    
    let start = Instant::now();
    send_query(&mut stream, query1).await;
    read_until_ready(&mut stream).await;
    let time1 = start.elapsed();
    
    let start = Instant::now();
    send_query(&mut stream, query2).await;
    read_until_ready(&mut stream).await;
    let time2 = start.elapsed();
    
    println!("  Query 1: {:?}", time1);
    println!("  Query 2: {:?} (different columns, new cache entry)", time2);
    
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