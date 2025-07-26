use pgsqlite::session::DbHandler;
use std::sync::Arc;

#[tokio::test]
async fn test_catalog_extended_protocol() {
    // Enable logging
    let _ = env_logger::builder().is_test(true).try_init();
    
    // Start test server
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    eprintln!("Test server listening on port {port}");
    
    let server_handle = tokio::spawn(async move {
        let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
        
        // Create test table
        db_handler.execute("CREATE TABLE test_table1 (id INTEGER PRIMARY KEY, name TEXT)").await.unwrap();
        eprintln!("Server: Created test table");
        
        let (stream, addr) = listener.accept().await.unwrap();
        eprintln!("Server: Accepted connection from {addr}");
        
        // Use tokio_util codec
        use tokio_util::codec::Framed;
        use pgsqlite::protocol::{PostgresCodec, FrontendMessage, BackendMessage, AuthenticationMessage, TransactionStatus};
        use futures::{SinkExt, StreamExt};
        
        let codec = PostgresCodec::new();
        let mut framed = Framed::new(stream, codec);
        
        // Wait for startup message
        let _startup = match framed.next().await {
            Some(Ok(FrontendMessage::StartupMessage(msg))) => {
                eprintln!("Server: Received startup message");
                msg
            }
            other => {
                eprintln!("Server: Expected startup message, got: {other:?}");
                return;
            }
        };
        
        // Send authentication OK
        framed.send(BackendMessage::Authentication(AuthenticationMessage::Ok)).await.unwrap();
        eprintln!("Server: Sent AuthenticationOk");
        
        // Send parameter status messages
        framed.send(BackendMessage::ParameterStatus {
            name: "server_version".to_string(),
            value: "14.0".to_string(),
        }).await.unwrap();
        
        // Send backend key data
        framed.send(BackendMessage::BackendKeyData {
            process_id: 1234,
            secret_key: 5678,
        }).await.unwrap();
        
        // Send ready for query
        framed.send(BackendMessage::ReadyForQuery {
            status: TransactionStatus::Idle,
        }).await.unwrap();
        framed.flush().await.unwrap();
        eprintln!("Server: Sent ReadyForQuery");
        
        // Now handle the catalog query with extended protocol
        loop {
            match framed.next().await {
                Some(Ok(msg)) => {
                    eprintln!("Server: Received message: {msg:?}");
                    
                    match msg {
                        FrontendMessage::Parse { query, .. } => {
                            eprintln!("Server: Parse query: {query}");
                            
                            // Check if it's a catalog query
                            if query.contains("pg_catalog.pg_class") {
                                eprintln!("Server: Detected catalog query in Parse");
                                // For now, just send ParseComplete
                                framed.send(BackendMessage::ParseComplete).await.unwrap();
                                eprintln!("Server: Sent ParseComplete");
                            } else {
                                // Normal query
                                framed.send(BackendMessage::ParseComplete).await.unwrap();
                            }
                        }
                        FrontendMessage::Bind { .. } => {
                            eprintln!("Server: Bind");
                            framed.send(BackendMessage::BindComplete).await.unwrap();
                            eprintln!("Server: Sent BindComplete");
                        }
                        FrontendMessage::Describe { typ, name } => {
                            eprintln!("Server: Describe {} '{}'", 
                                if typ == b'S' { "statement" } else { "portal" }, 
                                name);
                            
                            // Send parameter description
                            framed.send(BackendMessage::ParameterDescription(vec![])).await.unwrap();
                            
                            // Send row description for the catalog query
                            use pgsqlite::protocol::FieldDescription;
                            use pgsqlite::types::PgType;
                            
                            let fields = vec![
                                FieldDescription {
                                    name: "relname".to_string(),
                                    table_oid: 0,
                                    column_id: 1,
                                    type_oid: PgType::Text.to_oid(),
                                    type_size: -1,
                                    type_modifier: -1,
                                    format: 0,
                                },
                                FieldDescription {
                                    name: "relkind".to_string(),
                                    table_oid: 0,
                                    column_id: 2,
                                    type_oid: PgType::Char.to_oid(),
                                    type_size: 1,
                                    type_modifier: -1,
                                    format: 0,
                                },
                            ];
                            framed.send(BackendMessage::RowDescription(fields)).await.unwrap();
                            eprintln!("Server: Sent RowDescription");
                        }
                        FrontendMessage::Execute { max_rows, .. } => {
                            eprintln!("Server: Execute (max_rows: {max_rows})");
                            
                            // Send data row
                            let row = vec![
                                Some(b"test_table1".to_vec()),
                                Some(b"r".to_vec()),
                            ];
                            framed.send(BackendMessage::DataRow(row)).await.unwrap();
                            eprintln!("Server: Sent DataRow");
                            
                            // Send command complete
                            framed.send(BackendMessage::CommandComplete {
                                tag: "SELECT 1".to_string(),
                            }).await.unwrap();
                            eprintln!("Server: Sent CommandComplete");
                        }
                        FrontendMessage::Sync => {
                            eprintln!("Server: Sync");
                            framed.send(BackendMessage::ReadyForQuery {
                                status: TransactionStatus::Idle,
                            }).await.unwrap();
                            framed.flush().await.unwrap();
                            eprintln!("Server: Sent ReadyForQuery");
                        }
                        FrontendMessage::Terminate => {
                            eprintln!("Server: Terminate");
                            break;
                        }
                        _ => {
                            eprintln!("Server: Unhandled message");
                        }
                    }
                }
                Some(Err(e)) => {
                    eprintln!("Server: Error reading message: {e}");
                    break;
                }
                None => {
                    eprintln!("Server: Connection closed");
                    break;
                }
            }
        }
    });
    
    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Connect with tokio-postgres
    let config = format!("host=localhost port={port} dbname=test user=testuser");
    eprintln!("Connecting to {config}");
    let (client, connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls).await.unwrap();
    eprintln!("Client connected");
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });
    
    // Test the catalog query
    eprintln!("\nTesting catalog query...");
    match client.query(
        "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relkind = 'r'",
        &[]
    ).await {
        Ok(rows) => {
            eprintln!("✓ Catalog query succeeded: {} rows", rows.len());
            assert_eq!(rows.len(), 1);
            let relname: &str = rows[0].get(0);
            assert_eq!(relname, "test_table1");
        }
        Err(e) => {
            eprintln!("✗ Catalog query failed: {e:?}");
            panic!("Test failed!");
        }
    }
    
    server_handle.abort();
}