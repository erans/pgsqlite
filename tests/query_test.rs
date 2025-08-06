use tokio::net::TcpListener;
use tokio::time::{timeout, Duration};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
async fn test_simple_select() {
    // Use a temporary file instead of in-memory database
    let test_id = Uuid::new_v4().to_string().replace("-", "");
    let db_path = format!("/tmp/pgsqlite_test_{test_id}.db");
    let db_path_clone = db_path.clone();
    
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        // Create SQLite database with test data
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(&db_path_clone).unwrap()
        );
        
        // Initialize test data
        db_handler.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)").await.unwrap();
        db_handler.execute("INSERT INTO test (id, name) VALUES (1, 'Alice'), (2, 'Bob')").await.unwrap();
        
        // Accept connection
        let (stream, addr) = listener.accept().await.unwrap();
        
        // Handle connection
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Connect with tokio-postgres
    let (client, connection) = timeout(
        Duration::from_secs(5),
        tokio_postgres::connect(
            &format!("host=localhost port={port} dbname=test user=testuser"),
            NoTls,
        )
    ).await.unwrap().unwrap();
    
    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    
    // Execute query
    let rows = client.query("SELECT id, name FROM test ORDER BY id", &[]).await.unwrap();
    
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, &str>(1), "Alice");
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, &str>(1), "Bob");
    
    server_handle.abort();
    
    // Clean up
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{db_path}-journal"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
}

#[tokio::test]
async fn test_create_insert_select() {
    // Use a temporary file instead of in-memory database
    let test_id = Uuid::new_v4().to_string().replace("-", "");
    let db_path = format!("/tmp/pgsqlite_test_{test_id}.db");
    let db_path_clone = db_path.clone();
    
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(&db_path_clone).unwrap()
        );
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Connect with tokio-postgres
    let (client, connection) = timeout(
        Duration::from_secs(5),
        tokio_postgres::connect(
            &format!("host=localhost port={port} dbname=test user=testuser"),
            NoTls,
        )
    ).await.unwrap().unwrap();
    
    tokio::spawn(connection);
    
    // Create table
    client.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)", &[])
        .await.unwrap();
    
    // Insert data
    let inserted = client.execute("INSERT INTO users (id, email) VALUES (1, 'test@example.com')", &[])
        .await.unwrap();
    assert_eq!(inserted, 1);
    
    // Query data
    let rows = client.query("SELECT * FROM users", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, &str>(1), "test@example.com");
    
    server_handle.abort();
    
    // Clean up
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{db_path}-journal"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
}

#[tokio::test]
async fn test_transactions() {
    // Use a temporary file instead of in-memory database
    let test_id = Uuid::new_v4().to_string().replace("-", "");
    let db_path = format!("/tmp/pgsqlite_test_{test_id}.db");
    let db_path_clone = db_path.clone();
    
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(&db_path_clone).unwrap()
        );
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Connect with tokio-postgres
    let (client, connection) = timeout(
        Duration::from_secs(5),
        tokio_postgres::connect(
            &format!("host=localhost port={port} dbname=test user=testuser"),
            NoTls,
        )
    ).await.unwrap().unwrap();
    
    tokio::spawn(connection);
    
    // Create table
    client.execute("CREATE TABLE counter (value INTEGER)", &[]).await.unwrap();
    client.execute("INSERT INTO counter VALUES (0)", &[]).await.unwrap();
    
    // Test transaction commit
    client.execute("BEGIN", &[]).await.unwrap();
    client.execute("UPDATE counter SET value = 1", &[]).await.unwrap();
    client.execute("COMMIT", &[]).await.unwrap();
    
    let rows = client.query("SELECT value FROM counter", &[]).await.unwrap();
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    
    // Test transaction rollback
    client.execute("BEGIN", &[]).await.unwrap();
    client.execute("UPDATE counter SET value = 2", &[]).await.unwrap();
    client.execute("ROLLBACK", &[]).await.unwrap();
    
    let rows = client.query("SELECT value FROM counter", &[]).await.unwrap();
    assert_eq!(rows[0].get::<_, i32>(0), 1); // Should still be 1
    
    server_handle.abort();
    
    // Clean up
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{db_path}-journal"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
}