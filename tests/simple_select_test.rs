use tokio::net::TcpListener;
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
async fn test_simple_select() {
    // Enable debug logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pgsqlite::query::extended=debug")
        .try_init();
    
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
        
        // Create a simple table
        db_handler.execute("CREATE TABLE test (id INTEGER)").await.unwrap();
        db_handler.execute("INSERT INTO test VALUES (1)").await.unwrap();
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Connect with tokio-postgres
    let config = format!("host=localhost port={port} dbname=test user=testuser");
    let (client, connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });
    
    // Test simple query protocol first
    println!("Testing simple_query...");
    match client.simple_query("SELECT id FROM test").await {
        Ok(messages) => {
            println!("Simple query successful: {} messages", messages.len());
        }
        Err(e) => {
            println!("Simple query failed: {e:?}");
        }
    }
    
    // Test extended query protocol
    println!("\nTesting extended query...");
    match client.query("SELECT id FROM test", &[]).await {
        Ok(rows) => {
            println!("Extended query successful: {} rows", rows.len());
            assert_eq!(rows.len(), 1);
            let id: i32 = rows[0].get(0);
            assert_eq!(id, 1);
        }
        Err(e) => {
            println!("Extended query failed: {e:?}");
            panic!("Extended query should not fail");
        }
    }
    
    println!("\nTest passed!");
    server_handle.abort();

    // Clean up
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{db_path}-journal"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
}