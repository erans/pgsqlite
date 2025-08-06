use tokio::net::{TcpListener, TcpStream};
use tokio::time::{timeout, Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::{BytesMut, BufMut};
use uuid::Uuid;

#[tokio::test]
async fn test_raw_protocol() {
    // Enable debug logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();
    
    // Use a temporary file instead of in-memory database
    let test_id = Uuid::new_v4().to_string().replace("-", "");
    let db_path = format!("/tmp/pgsqlite_test_{test_id}.db");
    let db_path_clone = db_path.clone();
    
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    println!("Test server listening on port {port}");
    
    let server_handle = tokio::spawn(async move {
        // Create database handler
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(&db_path_clone).unwrap()
        );
        
        // Initialize test data
        db_handler.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)").await.unwrap();
        db_handler.execute("INSERT INTO test (id, name) VALUES (1, 'Alice'), (2, 'Bob')").await.unwrap();
        
        println!("Test data initialized");
        
        // Accept connection
        let (stream, addr) = listener.accept().await.unwrap();
        println!("Accepted connection from {addr}");
        
        // Handle connection
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    // Give server time to start
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    // Connect directly using TCP
    println!("Connecting to test server on port {port}");
    let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    
    // Send startup message
    let mut startup_msg = BytesMut::new();
    let params = b"user\0testuser\0database\0test\0\0";
    let total_len = 4 + 4 + params.len();
    startup_msg.extend_from_slice(&(total_len as i32).to_be_bytes());
    startup_msg.extend_from_slice(&196608i32.to_be_bytes()); // Protocol 3.0
    startup_msg.extend_from_slice(params);
    
    client.write_all(&startup_msg).await.unwrap();
    println!("Sent startup message");
    
    // Read responses until ReadyForQuery
    let mut response = vec![0u8; 4096];
    let mut ready = false;
    
    while !ready {
        let n = timeout(Duration::from_secs(1), client.read(&mut response)).await.unwrap().unwrap();
        println!("Received {n} bytes");
        
        let mut offset = 0;
        while offset < n {
            let msg_type = response[offset];
            offset += 1;
            
            if offset + 4 > n { break; }
            let len = i32::from_be_bytes([response[offset], response[offset+1], response[offset+2], response[offset+3]]) as usize;
            offset += 4;
            
            println!("Message type: {} ({}), length: {}", msg_type as char, msg_type, len);
            
            if msg_type == b'Z' {
                ready = true;
                println!("Got ReadyForQuery");
                break;
            }
            
            offset += len - 4; // len includes the 4 length bytes
        }
    }
    
    // Send a simple query
    let query = "SELECT id, name FROM test ORDER BY id";
    let mut query_msg = BytesMut::new();
    query_msg.put_u8(b'Q'); // Query message
    query_msg.put_i32(4 + query.len() as i32 + 1); // Length
    query_msg.extend_from_slice(query.as_bytes());
    query_msg.put_u8(0); // Null terminator
    
    client.write_all(&query_msg).await.unwrap();
    println!("Sent query: {query}");
    
    // Read query response
    let mut got_complete = false;
    response.fill(0);
    
    while !got_complete {
        let n = timeout(Duration::from_secs(2), client.read(&mut response)).await.unwrap().unwrap();
        println!("Received {n} bytes for query response");
        
        let mut offset = 0;
        while offset < n {
            let msg_type = response[offset];
            offset += 1;
            
            if offset + 4 > n { break; }
            let len = i32::from_be_bytes([response[offset], response[offset+1], response[offset+2], response[offset+3]]) as usize;
            offset += 4;
            
            println!("Response message type: {} ({}), length: {}", msg_type as char, msg_type, len);
            
            match msg_type {
                b'T' => println!("Got RowDescription"),
                b'D' => println!("Got DataRow"),
                b'C' => {
                    println!("Got CommandComplete");
                    got_complete = true;
                }
                b'E' => {
                    println!("Got ErrorResponse");
                    // Print error details
                    let msg_end = offset + len - 4;
                    while offset < msg_end {
                        let field_type = response[offset];
                        if field_type == 0 { break; }
                        offset += 1;
                        
                        let mut field_end = offset;
                        while field_end < msg_end && response[field_end] != 0 {
                            field_end += 1;
                        }
                        
                        let field_value = String::from_utf8_lossy(&response[offset..field_end]);
                        println!("  Error field {}: {}", field_type as char, field_value);
                        offset = field_end + 1;
                    }
                    break;
                }
                b'Z' => {
                    println!("Got ReadyForQuery");
                    break;
                }
                _ => {}
            }
            
            if offset + len - 4 <= n {
                offset += len - 4;
            } else {
                break;
            }
        }
    }
    
    server_handle.abort();

    // Clean up
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{db_path}-journal"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
}