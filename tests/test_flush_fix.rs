use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::Command;

#[tokio::test]
async fn test_flush_performance() {
    // Use a unique port to avoid conflicts
    let port = 15435;
    
    // Kill any existing server on this port
    let _ = tokio::process::Command::new("pkill")
        .args(&["-f", &format!("pgsqlite.*{}", port)])
        .output()
        .await;
    
    // Start server in background
    let mut server = Command::new("cargo")
        .args(&["run", "--release", "--", "-p", &port.to_string(), "--in-memory", "--log-level", "error"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("Failed to start server");
    
    // Wait for server to start with retries
    let mut connected = false;
    for _ in 0..20 {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        if let Ok(_) = TcpStream::connect(format!("127.0.0.1:{}", port)).await {
            connected = true;
            break;
        }
    }
    
    if !connected {
        server.kill().await.unwrap();
        panic!("Failed to connect to server after 10 seconds");
    }
    
    // Connect to server
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("Failed to connect to server");
    
    // Disable Nagle's algorithm on client side too
    stream.set_nodelay(true).expect("Failed to set TCP_NODELAY");
    
    // Send startup message
    let mut startup = vec![];
    startup.extend_from_slice(&196608u32.to_be_bytes()); // Protocol version 3.0
    startup.extend_from_slice(b"user\0test\0database\0test\0\0");
    let len = ((startup.len() + 4) as u32).to_be_bytes();
    stream.write_all(&len).await.unwrap();
    stream.write_all(&startup).await.unwrap();
    
    // Read until ReadyForQuery
    let mut authenticated = false;
    for _ in 0..20 { // Limit iterations to prevent infinite loop
        let mut msg_type = [0u8; 1];
        if stream.read_exact(&mut msg_type).await.is_err() {
            break;
        }
        
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_be_bytes(len_buf) as usize - 4;
        
        let mut data = vec![0u8; len];
        stream.read_exact(&mut data).await.unwrap();
        
        if msg_type[0] == b'Z' {
            authenticated = true;
            break;
        }
    }
    
    assert!(authenticated, "Failed to authenticate with server");
    
    // Create table for testing
    let create_query = "CREATE TABLE test_table (id INTEGER)";
    let mut msg = vec![b'Q'];
    msg.extend_from_slice(&((create_query.len() + 5) as u32).to_be_bytes());
    msg.extend_from_slice(create_query.as_bytes());
    msg.push(0);
    stream.write_all(&msg).await.unwrap();
    
    // Read response for CREATE TABLE
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
    
    // Warm up with a few queries
    for _ in 0..5 {
        let query = "SELECT 1";
        let mut msg = vec![b'Q'];
        msg.extend_from_slice(&((query.len() + 5) as u32).to_be_bytes());
        msg.extend_from_slice(query.as_bytes());
        msg.push(0);
        stream.write_all(&msg).await.unwrap();
        
        // Read response
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
    
    // Measure SELECT 1 latency
    let mut times = Vec::new();
    for _ in 0..20 {
        let start = Instant::now();
        
        // Send Query
        let query = "SELECT 1";
        let mut msg = vec![b'Q'];
        msg.extend_from_slice(&((query.len() + 5) as u32).to_be_bytes());
        msg.extend_from_slice(query.as_bytes());
        msg.push(0);
        stream.write_all(&msg).await.unwrap();
        
        // Read response
        let mut message_count = 0;
        loop {
            let mut msg_type = [0u8; 1];
            stream.read_exact(&mut msg_type).await.unwrap();
            
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_be_bytes(len_buf) as usize - 4;
            
            let mut data = vec![0u8; len];
            stream.read_exact(&mut data).await.unwrap();
            
            message_count += 1;
            
            if msg_type[0] == b'Z' { // ReadyForQuery
                times.push(start.elapsed());
                break;
            }
            
            // Prevent infinite loop
            if message_count > 10 {
                panic!("Too many messages received without ReadyForQuery");
            }
        }
    }
    
    // Calculate statistics
    let avg_time = times.iter().sum::<std::time::Duration>() / times.len() as u32;
    let min_time = times.iter().min().unwrap();
    let max_time = times.iter().max().unwrap();
    
    println!("SELECT 1 latency statistics:");
    println!("  Average: {:?}", avg_time);
    println!("  Min:     {:?}", min_time);
    println!("  Max:     {:?}", max_time);
    println!("  Samples: {}", times.len());
    
    // With proper flushing, latency should be under 10ms
    // We use 10ms instead of 5ms to account for debug builds and CI environments
    let threshold_ms = if cfg!(debug_assertions) { 20 } else { 10 };
    
    assert!(
        avg_time.as_millis() < threshold_ms, 
        "SELECT 1 latency too high: {:?} (threshold: {}ms)", 
        avg_time,
        threshold_ms
    );
    
    // Also check that most queries are fast (not just average)
    let fast_queries = times.iter().filter(|t| t.as_millis() < threshold_ms).count();
    assert!(
        fast_queries >= times.len() * 8 / 10, // 80% should be fast
        "Too many slow queries: {}/{} were over {}ms",
        times.len() - fast_queries,
        times.len(),
        threshold_ms
    );
    
    // Kill server
    server.kill().await.unwrap();
    
    // Clean up
    let _ = tokio::process::Command::new("pkill")
        .args(&["-f", &format!("pgsqlite.*{}", port)])
        .output()
        .await;
}