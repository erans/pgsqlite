use tokio::net::TcpListener;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_catalog_query_handling() {
    // Enable debug logging for catalog module
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pgsqlite::catalog=debug,pgsqlite::query::extended=info")
        .try_init();
    
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
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
    
    // First, let's test simple query protocol with catalog
    println!("\n=== Testing simple_query with catalog ===");
    match client.simple_query("SELECT typname FROM pg_type WHERE oid = 23").await {
        Ok(messages) => {
            println!("Simple catalog query successful, got {} messages", messages.len());
        }
        Err(e) => {
            println!("Simple catalog query failed: {e:?}");
        }
    }
    
    // Now test extended query protocol
    println!("\n=== Testing direct catalog query ===");
    match client.query(
        "SELECT typname FROM pg_type WHERE oid = 23",
        &[]
    ).await {
        Ok(rows) => {
            println!("Direct catalog query successful, got {} rows", rows.len());
            if !rows.is_empty() {
                let typname: &str = rows[0].get(0);
                println!("Type name for OID 23: {typname}");
            }
        }
        Err(e) => {
            println!("Direct catalog query failed: {e:?}");
        }
    }
    
    // Now test what tokio-postgres does internally
    println!("\n=== Testing parameterized query ===");
    match tokio::time::timeout(
        tokio::time::Duration::from_secs(3),
        client.query("SELECT $1::int4", &[&42i32])
    ).await {
        Ok(Ok(rows)) => {
            println!("Parameterized query successful, got {} rows", rows.len());
        }
        Ok(Err(e)) => {
            println!("Parameterized query failed: {e:?}");
        }
        Err(_) => {
            println!("Parameterized query timed out - catalog query loop detected");
        }
    }
    
    server_handle.abort();
}