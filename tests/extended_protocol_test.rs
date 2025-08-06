use tokio::net::TcpListener;
use tokio::time::{timeout, Duration};
use uuid::Uuid;

#[tokio::test]
async fn test_extended_protocol() {
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
        
        // Initialize test data - this will be persisted to the file
        db_handler.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)").await.unwrap();
        db_handler.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)").await.unwrap();
        db_handler.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)").await.unwrap();
        
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
            
            // Test 1: Query without parameters first
            println!("Test 1: Query without parameters");
            let query_result = timeout(
                Duration::from_secs(10),
                client.query("SELECT id, name, age FROM users WHERE age > 20 ORDER BY id", &[])
            ).await;
            
            match query_result {
                Ok(Ok(rows)) => {
                    println!("Query successful, got {} rows", rows.len());
                    assert_eq!(rows.len(), 2);
                    
                    let id1: i32 = rows[0].get(0);
                    let name1: &str = rows[0].get(1);
                    let age1: i32 = rows[0].get(2);
                    assert_eq!(id1, 1);
                    assert_eq!(name1, "Alice");
                    assert_eq!(age1, 30);
                    
                    let id2: i32 = rows[1].get(0);
                    let name2: &str = rows[1].get(1);
                    let age2: i32 = rows[1].get(2);
                    assert_eq!(id2, 2);
                    assert_eq!(name2, "Bob");
                    assert_eq!(age2, 25);
                }
                Ok(Err(e)) => panic!("Query failed: {e}"),
                Err(_) => panic!("Query timed out"),
            }
            
            // Test 2: Prepared statement
            println!("Test 2: Prepared statement");
            let stmt_result = timeout(
                Duration::from_secs(10),
                client.prepare("SELECT name FROM users WHERE id = $1::int4")
            ).await;
            
            match stmt_result {
                Ok(Ok(stmt)) => {
                    println!("Prepared statement successfully");
                    
                    // Execute prepared statement
                    let exec_result = timeout(
                        Duration::from_secs(10),
                        client.query(&stmt, &[&1i32])
                    ).await;
                    
                    match exec_result {
                        Ok(Ok(rows)) => {
                            println!("Prepared statement query successful, got {} rows", rows.len());
                            assert_eq!(rows.len(), 1);
                            assert_eq!(rows[0].get::<_, &str>(0), "Alice");
                        }
                        Ok(Err(e)) => panic!("Prepared statement execution failed: {e}"),
                        Err(_) => panic!("Prepared statement execution timed out"),
                    }
                }
                Ok(Err(e)) => panic!("Statement preparation failed: {e}"),
                Err(_) => panic!("Statement preparation timed out"),
            }
            
            // Test 3: Insert with parameters
            println!("Test 3: Insert with parameters");
            let insert_result = timeout(
                Duration::from_secs(10),
                client.execute("INSERT INTO users (id, name, age) VALUES ($1, $2, $3)", &[&3i32, &"Charlie", &35i32])
            ).await;
            
            match insert_result {
                Ok(Ok(count)) => {
                    println!("Insert successful, affected {count} rows");
                    assert_eq!(count, 1);
                    
                    // Verify insert
                    let verify = client.query_one("SELECT name, age FROM users WHERE id = 3", &[]).await.unwrap();
                    assert_eq!(verify.get::<_, &str>(0), "Charlie");
                    assert_eq!(verify.get::<_, i32>(1), 35);
                }
                Ok(Err(e)) => panic!("Insert failed: {e}"),
                Err(_) => panic!("Insert timed out"),
            }
        }
        Ok(Err(e)) => panic!("Connection failed: {e}"),
        Err(_) => panic!("Connection timed out"),
    }
    
    server_handle.abort();
    
    // Clean up the database file
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{db_path}-journal"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
}