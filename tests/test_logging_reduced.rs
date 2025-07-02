use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Test that logging overhead has been reduced
#[tokio::test]
async fn test_logging_reduced() {
    // Start pgsqlite server
    let port = 25445;
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
    
    // Create test table
    send_query(&mut stream, 
        "CREATE TABLE test_log (id INTEGER PRIMARY KEY, name TEXT)"
    ).await;
    read_until_ready(&mut stream).await;
    
    // Test SELECT queries that would previously generate error logs
    let queries = vec![
        "SELECT * FROM test_log WHERE id = 1",
        "SELECT name FROM test_log",
        "SELECT COUNT(*) FROM test_log",
    ];
    
    println!("\nTesting queries that previously generated error logs:");
    
    for query in queries {
        let start = Instant::now();
        send_query(&mut stream, query).await;
        read_until_ready(&mut stream).await;
        let elapsed = start.elapsed();
        
        println!("Query: {} - Elapsed: {:?}", query, elapsed);
        
        // With reduced logging, queries should be faster
        assert!(elapsed < Duration::from_millis(10), 
                "Query took too long: {:?} (possible logging overhead)", elapsed);
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

async fn read_until_ready(stream: &mut TcpStream) {
    loop {
        let mut msg_type = [0u8; 1];
        stream.read_exact(&mut msg_type).await.unwrap();
        
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_be_bytes(len_buf) as usize - 4;
        
        let mut data = vec![0u8; len];
        stream.read_exact(&mut data).await.unwrap();
        
        if msg_type[0] == b'Z' {
            break; // ReadyForQuery
        }
    }
}