use tokio::net::TcpListener;

#[tokio::test]
async fn test_create_index_with_varchar_pattern_ops() {
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        // Create database handler with a temporary file database
        let temp_db_path = format!("/tmp/pgsqlite_test_index_{}.db", std::process::id());
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(&temp_db_path).unwrap()
        );
        
        // Accept connection
        let (stream, addr) = listener.accept().await.unwrap();
        
        // Handle connection
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    
    // Connect with tokio-postgres
    let (client, connection) = tokio_postgres::connect(
        &format!("host=localhost port={port} dbname=test user=testuser"),
        tokio_postgres::NoTls,
    ).await.unwrap();
    
    // Spawn connection handler
    let connection_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });
    
    // Create a test table
    client.execute("CREATE TABLE test_users (id INTEGER PRIMARY KEY, email VARCHAR(255))", &[]).await.unwrap();
    
    // Insert some test data
    client.execute("INSERT INTO test_users (email) VALUES ('user@example.com'), ('admin@test.org')", &[]).await.unwrap();
    
    // Test CREATE INDEX with varchar_pattern_ops - this should be translated to COLLATE BINARY
    let create_index_result = client.execute(
        r#"CREATE INDEX "test_users_email_like_idx" ON "test_users" ("email" varchar_pattern_ops)"#,
        &[]
    ).await;
    
    // The index creation should succeed (no syntax error)
    assert!(create_index_result.is_ok(), "CREATE INDEX with varchar_pattern_ops failed: {:?}", create_index_result);
    
    // Test that LIKE queries work (which should benefit from the index)
    let like_result = client.query(
        "SELECT * FROM test_users WHERE email LIKE 'user%'",
        &[]
    ).await;
    
    assert!(like_result.is_ok());
    let rows = like_result.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(1), "user@example.com");
    
    // Cleanup
    server_handle.abort();
    connection_handle.abort();
}

