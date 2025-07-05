use tokio::net::TcpListener;
use pgsqlite::session::DbHandler;
use std::sync::Arc;

#[tokio::test] 
async fn minimal_catalog_test() {
    let _ = env_logger::builder()
        .is_test(true)
        .try_init();
    
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    eprintln!("Test server on port {}", port);
    
    let server_handle = tokio::spawn(async move {
        let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
        
        // Create test table
        db_handler.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY)").await.unwrap();
        eprintln!("Created test table");
        
        let (stream, addr) = listener.accept().await.unwrap();
        eprintln!("Accepted connection from {}", addr);
        
        // Use pgsqlite's handler
        if let Err(e) = pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await {
            eprintln!("Handler error: {}", e);
        }
    });
    
    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Connect with tokio-postgres
    let config = format!("host=localhost port={} dbname=test user=testuser", port);
    let (client, connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // Test simple non-catalog query first
    eprintln!("\n=== Testing regular query ===");
    match client.query("SELECT 1 as num", &[]).await {
        Ok(rows) => {
            eprintln!("✓ Regular query works: {} rows", rows.len());
        }
        Err(e) => {
            eprintln!("✗ Regular query failed: {:?}", e);
        }
    }
    
    // Test catalog query with simple_query (simple protocol)
    eprintln!("\n=== Testing catalog query with simple protocol ===");
    match client.simple_query("SELECT relname FROM pg_catalog.pg_class WHERE relkind = 'r'").await {
        Ok(messages) => {
            eprintln!("✓ Simple protocol catalog query works: {} messages", messages.len());
            let mut row_count = 0;
            for msg in &messages {
                if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                    row_count += 1;
                    eprintln!("  Table: {}", row.get("relname").unwrap_or("?"));
                }
            }
            eprintln!("  Total rows: {}", row_count);
        }
        Err(e) => {
            eprintln!("✗ Simple protocol catalog query failed: {:?}", e);
        }
    }
    
    // Now test catalog query with extended protocol
    eprintln!("\n=== Testing catalog query with extended protocol ===");
    match client.query("SELECT relname FROM pg_catalog.pg_class WHERE relkind = 'r'", &[]).await {
        Ok(rows) => {
            eprintln!("✓ Extended protocol catalog query works: {} rows", rows.len());
            for row in &rows {
                let name: &str = row.get(0);
                eprintln!("  Table: {}", name);
            }
        }
        Err(e) => {
            eprintln!("✗ Extended protocol catalog query failed: {:?}", e);
            panic!("Catalog query should work!");
        }
    }
    
    server_handle.abort();
}