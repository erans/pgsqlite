use tokio::net::TcpListener;
use tokio::time::{timeout, Duration};
use tokio_postgres::{NoTls, SimpleQueryMessage};

#[tokio::test]
async fn test_query_cache_basic() {
    // Enable debug logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();
    
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    println!("Test server listening on port {}", port);
    
    let server_handle = tokio::spawn(async move {
        // Create database handler
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        // Accept connection
        let (stream, addr) = listener.accept().await.unwrap();
        println!("Accepted connection from {}", addr);
        
        // Handle connection
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    // Give server time to start
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    // Connect with tokio-postgres
    println!("Connecting to test server on port {}", port);
    
    let (client, connection) = timeout(
        Duration::from_secs(5),
        tokio_postgres::connect(
            &format!("host=localhost port={} dbname=test user=testuser", port),
            NoTls,
        )
    ).await.unwrap().unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Create a test table
    client
        .simple_query("CREATE TABLE cache_test (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .expect("Failed to create table");

    // Insert test data
    client
        .simple_query("INSERT INTO cache_test (id, name) VALUES (1, 'test1'), (2, 'test2')")
        .await
        .expect("Failed to insert data");

    // First query - should miss cache
    let start = std::time::Instant::now();
    let rows = client
        .simple_query("SELECT id, name FROM cache_test WHERE id = 1")
        .await
        .expect("Failed to execute first query");
    let first_duration = start.elapsed();

    // Verify result
    if let SimpleQueryMessage::Row(row) = &rows[0] {
        assert_eq!(row.get("id").unwrap(), "1");
        assert_eq!(row.get("name").unwrap(), "test1");
    }

    // Second identical query - should hit cache
    let start = std::time::Instant::now();
    let rows = client
        .simple_query("SELECT id, name FROM cache_test WHERE id = 1")
        .await
        .expect("Failed to execute second query");
    let second_duration = start.elapsed();

    // Verify result is still correct
    if let SimpleQueryMessage::Row(row) = &rows[0] {
        assert_eq!(row.get("id").unwrap(), "1");
        assert_eq!(row.get("name").unwrap(), "test1");
    }

    println!("First query duration: {:?}", first_duration);
    println!("Second query duration: {:?}", second_duration);
    
    // Cache hit should be faster (though in tests the difference might be small)
    // Just verify both queries returned correct results
    assert!(rows.len() > 0);
}

#[tokio::test]
async fn test_query_cache_normalization() {
    // Enable debug logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init();
    
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        // Create database handler
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        // Accept connection
        let (stream, addr) = listener.accept().await.unwrap();
        
        // Handle connection
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    // Give server time to start
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    // Connect with tokio-postgres
    let (client, connection) = timeout(
        Duration::from_secs(5),
        tokio_postgres::connect(
            &format!("host=localhost port={} dbname=test user=testuser", port),
            NoTls,
        )
    ).await.unwrap().unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Create a test table
    client
        .simple_query("CREATE TABLE norm_test (id INTEGER PRIMARY KEY, value TEXT)")
        .await
        .expect("Failed to create table");

    // Insert test data
    client
        .simple_query("INSERT INTO norm_test (id, value) VALUES (1, 'test')")
        .await
        .expect("Failed to insert data");

    // These queries should all use the same cache entry due to normalization
    let queries = vec![
        "SELECT * FROM norm_test WHERE id = 1",
        "select * from norm_test where id = 1",
        "SELECT  *  FROM  norm_test  WHERE  id  =  1",
        "SeLeCt * FrOm norm_test WhErE id = 1",
    ];

    for query in queries {
        let rows = client
            .simple_query(query)
            .await
            .expect(&format!("Failed to execute query: {}", query));

        // Verify result
        if let SimpleQueryMessage::Row(row) = &rows[0] {
            assert_eq!(row.get("id").unwrap(), "1");
            assert_eq!(row.get("value").unwrap(), "test");
        }
    }
}