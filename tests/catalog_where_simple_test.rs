use tokio::net::TcpListener;
use tokio_postgres::NoTls;
use pgsqlite::session::DbHandler;
use std::sync::Arc;

#[tokio::test]
async fn test_catalog_where_simple() {
    // Enable logging
    let _ = env_logger::builder().is_test(true).try_init();
    
    // Start test server without using the helper
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    eprintln!("Test server listening on port {}", port);
    
    let server_handle = tokio::spawn(async move {
        let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
        
        // Create test table
        db_handler.execute("CREATE TABLE test_table1 (id INTEGER PRIMARY KEY, name TEXT)").await.unwrap();
        eprintln!("Server: Created test table");
        
        let (stream, addr) = listener.accept().await.unwrap();
        eprintln!("Server: Accepted connection from {}", addr);
        if let Err(e) = pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await {
            eprintln!("Server error: {}", e);
        }
        eprintln!("Server: Connection handler finished");
    });
    
    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Connect with tokio-postgres
    let config = format!("host=localhost port={} dbname=test user=testuser", port);
    eprintln!("Connecting to {}", config);
    let (client, connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
    eprintln!("Client connected");
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // First test a simple non-catalog query
    eprintln!("\nTesting simple query...");
    match client.query("SELECT 1", &[]).await {
        Ok(rows) => eprintln!("✓ Simple query succeeded: {} rows", rows.len()),
        Err(e) => eprintln!("✗ Simple query failed: {:?}", e),
    }
    
    // Test with simple query protocol first
    eprintln!("\nTesting catalog query with simple protocol...");
    match client.simple_query(
        "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relkind = 'r'"
    ).await {
        Ok(messages) => {
            eprintln!("✓ Simple catalog query succeeded: {} messages", messages.len());
            let mut row_count = 0;
            for msg in &messages {
                match msg {
                    tokio_postgres::SimpleQueryMessage::Row(row) => {
                        row_count += 1;
                        if row_count == 1 {
                            eprintln!("  First row: relname={}, relkind={}", 
                                row.get(0).unwrap_or("?"), 
                                row.get(1).unwrap_or("?"));
                        }
                    }
                    tokio_postgres::SimpleQueryMessage::CommandComplete(n) => {
                        eprintln!("  CommandComplete: {}", n);
                    }
                    _ => {}
                }
            }
            eprintln!("  Total rows: {}", row_count);
        }
        Err(e) => {
            eprintln!("✗ Simple catalog query failed: {:?}", e);
        }
    }
    
    // Test the exact query that fails in catalog_where_test
    eprintln!("\nTesting catalog query with extended protocol...");
    match client.query(
        "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relkind = 'r'",
        &[]
    ).await {
        Ok(rows) => {
            eprintln!("✓ Extended catalog query succeeded: {} rows", rows.len());
            if rows.len() > 0 {
                let relname: &str = rows[0].get(0);
                eprintln!("  First row relname: {}", relname);
            }
        }
        Err(e) => {
            eprintln!("✗ Extended catalog query failed: {:?}", e);
            panic!("Test failed!");
        }
    }
    
    server_handle.abort();
}