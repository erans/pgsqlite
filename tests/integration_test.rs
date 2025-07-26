use tokio::net::TcpListener;
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn test_basic_protocol() {
    // Enable debug logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();
    
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    println!("Test server listening on port {port}");
    
    let server_handle = tokio::spawn(async move {
        // Create database handler
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
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
    
    // Connect with tokio-postgres
    println!("Connecting to test server on port {port}");
    
    let connect_result = timeout(
        Duration::from_secs(5),
        tokio_postgres::connect(
            &format!("host=localhost port={port} dbname=test user=testuser"),
            tokio_postgres::NoTls,
        )
    ).await;
    
    match connect_result {
        Ok(Ok((client, connection))) => {
            println!("Connected successfully");
            
            // Spawn connection handler
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("Connection error: {e}");
                }
            });
            
            // Try a simple query using simple_query to avoid extended protocol
            println!("Executing query");
            let query_result = timeout(
                Duration::from_secs(2),
                client.simple_query("SELECT id, name FROM test ORDER BY id")
            ).await;
            
            match query_result {
                Ok(Ok(messages)) => {
                    println!("Query successful, got {} messages", messages.len());
                    
                    // simple_query returns SimpleQueryMessage enums
                    let mut row_count = 0;
                    for msg in &messages {
                        match msg {
                            tokio_postgres::SimpleQueryMessage::Row(row) => {
                                row_count += 1;
                                if row_count == 1 {
                                    assert_eq!(row.get(0).unwrap(), "1");
                                    assert_eq!(row.get(1).unwrap(), "Alice");
                                } else if row_count == 2 {
                                    assert_eq!(row.get(0).unwrap(), "2");
                                    assert_eq!(row.get(1).unwrap(), "Bob");
                                }
                            }
                            tokio_postgres::SimpleQueryMessage::CommandComplete(n) => {
                                println!("Command complete: {n}");
                            }
                            _ => {}
                        }
                    }
                    assert_eq!(row_count, 2);
                }
                Ok(Err(e)) => panic!("Query failed: {e}"),
                Err(_) => panic!("Query timed out"),
            }
        }
        Ok(Err(e)) => panic!("Connection failed: {e}"),
        Err(_) => panic!("Connection timed out"),
    }
    
    server_handle.abort();
}