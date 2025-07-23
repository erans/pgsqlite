use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::{BytesMut, BufMut};

#[tokio::test]
async fn test_manual_extended_protocol() {
    // Start test server
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        // Initialize test data
        db_handler.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)").await.unwrap();
        db_handler.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)").await.unwrap();
        db_handler.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)").await.unwrap();
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Connect as client
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).await.unwrap();
    
    // Send startup message
    let mut startup = BytesMut::new();
    startup.put_i32(0); // Length placeholder
    startup.put_i32(196608); // Protocol version 3.0
    startup.put(&b"user\0postgres\0database\0test\0\0"[..]);
    let len = startup.len() as i32;
    startup[0..4].copy_from_slice(&len.to_be_bytes());
    
    stream.write_all(&startup).await.unwrap();
    
    // Read authentication response and other startup messages
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    println!("Received {n} bytes in startup response");
    
    // Test 1: Parse a query with parameter
    println!("\nTest 1: Parse query with parameter");
    let mut parse_msg = BytesMut::new();
    parse_msg.put_u8(b'P'); // Parse
    parse_msg.put_i32(0); // Length placeholder
    parse_msg.put(&b"stmt1\0"[..]); // Statement name
    parse_msg.put(&b"SELECT id, name FROM users WHERE age > $1\0"[..]); // Query
    parse_msg.put_i16(0); // No parameter types
    let len = (parse_msg.len() - 1) as i32;
    parse_msg[1..5].copy_from_slice(&len.to_be_bytes());
    
    // Send Describe
    let mut describe_msg = BytesMut::new();
    describe_msg.put_u8(b'D'); // Describe
    describe_msg.put_i32(0); // Length placeholder
    describe_msg.put_u8(b'S'); // Statement
    describe_msg.put(&b"stmt1\0"[..]);
    let len = (describe_msg.len() - 1) as i32;
    describe_msg[1..5].copy_from_slice(&len.to_be_bytes());
    
    // Send Sync
    let mut sync_msg = BytesMut::new();
    sync_msg.put_u8(b'S'); // Sync
    sync_msg.put_i32(4); // Length
    
    stream.write_all(&parse_msg).await.unwrap();
    stream.write_all(&describe_msg).await.unwrap();
    stream.write_all(&sync_msg).await.unwrap();
    
    // Read Parse/Describe response
    let n = stream.read(&mut buf).await.unwrap();
    println!("Received {n} bytes for Parse/Describe");
    
    // Test 2: Bind and Execute
    println!("\nTest 2: Bind and Execute");
    let mut bind_msg = BytesMut::new();
    bind_msg.put_u8(b'B'); // Bind
    bind_msg.put_i32(0); // Length placeholder
    bind_msg.put(&b"portal1\0"[..]); // Portal name
    bind_msg.put(&b"stmt1\0"[..]); // Statement name
    bind_msg.put_i16(0); // No format codes
    bind_msg.put_i16(1); // One parameter
    bind_msg.put_i32(2); // Parameter length
    bind_msg.put(&b"20"[..]); // Parameter value "20"
    bind_msg.put_i16(0); // No result format codes
    let len = (bind_msg.len() - 1) as i32;
    bind_msg[1..5].copy_from_slice(&len.to_be_bytes());
    
    // Execute
    let mut execute_msg = BytesMut::new();
    execute_msg.put_u8(b'E'); // Execute
    execute_msg.put_i32(0); // Length placeholder
    execute_msg.put(&b"portal1\0"[..]); // Portal name
    execute_msg.put_i32(0); // No limit
    let len = (execute_msg.len() - 1) as i32;
    execute_msg[1..5].copy_from_slice(&len.to_be_bytes());
    
    stream.write_all(&bind_msg).await.unwrap();
    stream.write_all(&execute_msg).await.unwrap();
    stream.write_all(&sync_msg).await.unwrap();
    
    // Read Bind/Execute response
    let n = stream.read(&mut buf).await.unwrap();
    println!("Received {n} bytes for Bind/Execute");
    
    // Parse response to verify we got data
    let mut offset = 0;
    while offset < n {
        let msg_type = buf[offset];
        offset += 1;
        
        if offset + 4 > n { break; }
        let msg_len = i32::from_be_bytes([buf[offset], buf[offset+1], buf[offset+2], buf[offset+3]]) as usize;
        offset += 4;
        
        match msg_type {
            b'2' => println!("BindComplete"),
            b'T' => {
                println!("RowDescription");
                // Should have field descriptions
                assert!(msg_len > 4);
            }
            b'D' => {
                println!("DataRow");
                // Should have data
                assert!(msg_len > 2);
            }
            b'C' => {
                let tag = std::str::from_utf8(&buf[offset..offset+msg_len-5]).unwrap();
                println!("CommandComplete: {tag}");
                assert!(tag.starts_with("SELECT"));
            }
            b'Z' => println!("ReadyForQuery"),
            _ => println!("Message type: {} ({})", msg_type as char, msg_type),
        }
        
        offset += msg_len - 4;
    }
    
    // Send Terminate
    let mut terminate_msg = BytesMut::new();
    terminate_msg.put_u8(b'X'); // Terminate
    terminate_msg.put_i32(4); // Length
    stream.write_all(&terminate_msg).await.unwrap();
    
    server_handle.abort();
    
    println!("\nTest completed successfully!");
}