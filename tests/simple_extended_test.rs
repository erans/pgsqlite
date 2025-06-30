use tokio::net::TcpListener;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_simple_extended_protocol() {
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        // Create simple test table
        db_handler.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"
        ).await.unwrap();
        
        // Insert test data
        db_handler.execute(
            "INSERT INTO test (id, name) VALUES (1, 'Alice'), (2, 'Bob')"
        ).await.unwrap();
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Connect with tokio-postgres
    let config = format!("host=localhost port={} dbname=test user=testuser", port);
    let (client, connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // Use simple_query first to ensure the connection works
    println!("Running simple_query...");
    match client.simple_query("SELECT id, name FROM test").await {
        Ok(_messages) => {
            println!("Simple query successful");
        }
        Err(e) => {
            println!("Simple query failed: {:?}", e);
            panic!("Simple query failed");
        }
    }
    
    // Simple query without parameters - this should work
    println!("Running extended query...");
    match client.query("SELECT id, name FROM test", &[]).await {
        Ok(rows) => {
            println!("Query successful, got {} rows", rows.len());
            assert_eq!(rows.len(), 2);
        }
        Err(e) => {
            println!("Query failed: {:?}", e);
            panic!("First query failed");
        }
    }
    
    // Query with parameter - this triggers type lookups
    println!("Running query with parameter...");
    match client.query("SELECT id, name FROM test WHERE id = $1::int4", &[&1i32]).await {
        Ok(rows) => {
            println!("Query successful, got {} rows", rows.len());
            assert_eq!(rows.len(), 1);
        }
        Err(e) => {
            println!("Query with parameter failed: {:?}", e);
            panic!("Second query failed");
        }
    }
    
    println!("Simple extended protocol test passed!");
    
    server_handle.abort();
}