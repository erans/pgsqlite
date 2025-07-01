use tokio::net::TcpListener;
use tokio::time::{timeout, Duration};
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_cache_status_query() {
    // Enable debug logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init();
    
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    println!("Test server listening on port {}", port);
    
    let _server_handle = tokio::spawn(async move {
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
    
    // Create test table and run some queries to populate cache
    client.simple_query("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)").await.unwrap();
    client.simple_query("INSERT INTO test VALUES (1, 'test')").await.unwrap();
    
    // Run the same query multiple times to get cache hits
    for _ in 0..5 {
        client.simple_query("SELECT * FROM test WHERE id = 1").await.unwrap();
    }
    
    // Query cache status
    let rows = client.simple_query("SELECT * FROM pgsqlite_cache_status").await.unwrap();
    
    println!("\nCache Status:");
    println!("-------------");
    
    // Process results
    for msg in rows {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let metric = row.get("metric").unwrap_or("unknown");
            let value = row.get("value").unwrap_or("0");
            println!("{}: {}", metric, value);
        }
    }
}