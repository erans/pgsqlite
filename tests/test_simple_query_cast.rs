use tokio::net::TcpListener;
use tokio_postgres::{NoTls, SimpleQueryMessage};

#[tokio::test]
async fn test_simple_query_text_format() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Connect with tokio-postgres
    let config = format!("host=localhost port={} dbname=test user=testuser", port);
    let (client, connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // Test simple query without casts first
    let messages = client.simple_query("SELECT 42").await.unwrap();
    
    // Debug: Print all messages
    println!("Got {} messages:", messages.len());
    for (i, msg) in messages.iter().enumerate() {
        match msg {
            SimpleQueryMessage::Row(row) => {
                println!("Message {}: Row with {} columns", i, row.len());
                for j in 0..row.len() {
                    println!("  Column {}: {:?}", j, row.get(j));
                }
            }
            SimpleQueryMessage::CommandComplete(cmd) => {
                println!("Message {}: CommandComplete({})", i, cmd);
            }
            SimpleQueryMessage::RowDescription(desc) => {
                println!("Message {}: RowDescription with {} columns", i, desc.len());
            }
            _ => println!("Message {}: Other", i),
        }
    }
    
    // Check we have a row with the correct value
    let mut found_row = false;
    for msg in &messages {
        if let SimpleQueryMessage::Row(row) = msg {
            let value = row.get(0).expect("Should have one column");
            assert_eq!(value, "42", "Value should be text '42', not binary encoded");
            found_row = true;
        }
    }
    assert!(found_row, "Should have found a row in the messages");
    
    // Test with cast function instead of :: syntax
    let messages = client.simple_query("SELECT CAST(42 AS TEXT)").await.unwrap();
    
    // Find the Row message
    let mut found_cast = false;
    for msg in &messages {
        if let SimpleQueryMessage::Row(row) = msg {
            assert_eq!(row.get(0).unwrap(), "42", "CAST(42 AS TEXT) should be '42'");
            found_cast = true;
        }
    }
    assert!(found_cast, "Should have found the cast result");
    
    server_handle.abort();
}

#[tokio::test]
async fn test_simple_query_numeric_values() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    let config = format!("host=localhost port={} dbname=test user=testuser", port);
    let (client, connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // Test various numeric types - all should return as text in simple query
    let messages = client.simple_query("SELECT 42, 3.14, -100, 1.23e10").await.unwrap();
    
    // Find the Row message
    let mut found_numeric = false;
    for msg in &messages {
        if let SimpleQueryMessage::Row(row) = msg {
            assert_eq!(row.get(0).unwrap(), "42", "Integer should be text '42'");
            assert_eq!(row.get(1).unwrap(), "3.14", "Float should be text '3.14'");
            assert_eq!(row.get(2).unwrap(), "-100", "Negative int should be text '-100'");
            // Scientific notation might be normalized by SQLite
            let sci_val = row.get(3).unwrap();
            assert!(sci_val == "12300000000" || sci_val == "1.23e10" || sci_val == "12300000000.0",
                "Scientific notation should be text, got: {}", sci_val);
            found_numeric = true;
        }
    }
    assert!(found_numeric, "Should have found numeric values");
    
    server_handle.abort();
}