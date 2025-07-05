use tokio::net::TcpListener;
use tokio_postgres::{NoTls, SimpleQueryMessage};
use pgsqlite::session::DbHandler;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    env_logger::init();
    
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
        
        // Create test table
        db_handler.execute("CREATE TABLE test_table1 (id INTEGER PRIMARY KEY, name TEXT)").await.unwrap();
        
        let (stream, addr) = listener.accept().await.unwrap();
        if let Err(e) = pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await {
            eprintln!("Server error: {}", e);
        }
    });
    
    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    // Connect with tokio-postgres
    let config = format!("host=localhost port={} dbname=test user=testuser", port);
    let (client, connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // Try using simple query protocol instead of extended
    println!("Testing with simple_query protocol...");
    match client.simple_query(
        "SELECT * FROM pg_catalog.pg_class WHERE relkind = 'r'"
    ).await {
        Ok(messages) => {
            println!("✓ Simple query succeeded!");
            for msg in messages {
                match msg {
                    SimpleQueryMessage::Row(row) => {
                        println!("  Row with {} columns", row.len());
                    }
                    SimpleQueryMessage::CommandComplete(n) => {
                        println!("  Command complete: {}", n);
                    }
                    _ => {}
                }
            }
        }
        Err(e) => println!("✗ Simple query failed: {:?}", e),
    }
    
    println!("\nTesting with extended query protocol...");
    match client.query(
        "SELECT * FROM pg_catalog.pg_class WHERE relkind = 'r'",
        &[]
    ).await {
        Ok(rows) => println!("✓ Extended query succeeded: {} rows", rows.len()),
        Err(e) => println!("✗ Extended query failed: {:?}", e),
    }
    
    server_handle.abort();
}