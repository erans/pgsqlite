use tokio::net::TcpListener;
use tokio::time::{timeout, Duration};


#[tokio::test]
async fn test_protocol_handshake() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use bytes::BytesMut;
    
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let (stream, addr) = listener.accept().await.unwrap();
        let _ = pgsqlite::handle_test_connection(stream, addr).await;
    });
    
    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Connect as client
    let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    
    // Send startup message
    let mut startup_msg = BytesMut::new();
    // Calculate length: 4 (length) + 4 (version) + "user\0testuser\0database\0test\0\0" (30 bytes)
    let params = b"user\0testuser\0database\0test\0\0";
    let total_len = 4 + 4 + params.len();
    startup_msg.extend_from_slice(&(total_len as i32).to_be_bytes()); // Length
    startup_msg.extend_from_slice(&196608i32.to_be_bytes()); // Protocol version 3.0
    startup_msg.extend_from_slice(params);
    
    client.write_all(&startup_msg).await.unwrap();
    
    // Read response
    let mut response = vec![0u8; 1024];
    let n = timeout(
        Duration::from_secs(1),
        client.read(&mut response)
    ).await.unwrap().unwrap();
    
    // Should receive authentication OK (R message with auth type 0)
    println!("Received {n} bytes");
    println!("Response: {:?}", &response[..n]);
    assert!(n > 5, "Expected at least 6 bytes, got {n}");
    assert_eq!(response[0], b'R'); // Authentication response
    
    // Wait for server to finish
    let _ = timeout(Duration::from_secs(1), server_handle).await;
}