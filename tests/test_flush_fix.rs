use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::Command;

#[tokio::test]
#[ignore] // Skip in normal test runs due to long execution time
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
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("Failed to start server");
    
    // Wait for server to start with retries
    let mut connected = false;
    let max_retries = if std::env::var("CI").is_ok() { 60 } else { 20 }; // 30s in CI, 10s locally
    for i in 0..max_retries {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
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
    // In CI environments, we need to be more lenient due to shared resources
    let is_ci = std::env::var("CI").is_ok();
    let threshold_ms = match (cfg!(debug_assertions), is_ci) {
        (true, true) => 100,   // Debug + CI: very lenient
        (true, false) => 20,   // Debug + local: moderate
        (false, true) => 50,   // Release + CI: lenient
        (false, false) => 10,  // Release + local: strict
    };
    
    // In CI, just warn instead of failing
    if is_ci && avg_time.as_millis() >= threshold_ms {
        println!("WARNING: SELECT 1 latency in CI: {:?} (threshold: {}ms)", avg_time, threshold_ms);
        println!("This is acceptable in CI environments due to shared resources");
    } else {
        assert!(
            avg_time.as_millis() < threshold_ms, 
            "SELECT 1 latency too high: {:?} (threshold: {}ms)", 
            avg_time,
            threshold_ms
        );
    }
    
    // Also check that most queries are fast (not just average)
    let fast_queries = times.iter().filter(|t| t.as_millis() < threshold_ms).count();
    let min_fast_ratio = if is_ci { 0.5 } else { 0.8 }; // 50% in CI, 80% locally
    
    if is_ci && fast_queries < times.len() * min_fast_ratio as usize {
        println!("WARNING: Only {}/{} queries were under {}ms in CI", 
                 fast_queries, times.len(), threshold_ms);
    } else {
        assert!(
            fast_queries >= (times.len() as f64 * min_fast_ratio) as usize,
            "Too many slow queries: {}/{} were over {}ms (minimum ratio: {})",
            times.len() - fast_queries,
            times.len(),
            threshold_ms,
            min_fast_ratio
        );
    }
    
    // Kill server
    server.kill().await.unwrap();
    
    // Clean up
    let _ = tokio::process::Command::new("pkill")
        .args(&["-f", &format!("pgsqlite.*{}", port)])
        .output()
        .await;
}