use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::Framed;
use pgsqlite::protocol::{PostgresCodec, FrontendMessage, BackendMessage, FieldDescription};
use pgsqlite::session::DbHandler;
use pgsqlite::types::PgType;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;

#[tokio::test]
#[ignore = "Diagnostic test that intentionally panics to show trace output"]
async fn trace_catalog_protocol() {
    let _ = env_logger::builder().is_test(true).try_init();
    
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    eprintln!("Test server on port {}", port);
    
    let server_handle = tokio::spawn(async move {
        let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
        
        // Create test table
        db_handler.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY)").await.unwrap();
        
        let (stream, addr) = listener.accept().await.unwrap();
        eprintln!("Server: Accepted connection from {}", addr);
        
        let codec = PostgresCodec::new();
        let mut framed = Framed::new(stream, codec);
        
        // Handle startup
        let _ = framed.next().await;
        framed.send(BackendMessage::Authentication(pgsqlite::protocol::AuthenticationMessage::Ok)).await.unwrap();
        framed.send(BackendMessage::ParameterStatus {
            name: "server_version".to_string(),
            value: "14.0".to_string(),
        }).await.unwrap();
        framed.send(BackendMessage::BackendKeyData {
            process_id: 1234,
            secret_key: 5678,
        }).await.unwrap();
        framed.send(BackendMessage::ReadyForQuery {
            status: pgsqlite::protocol::TransactionStatus::Idle,
        }).await.unwrap();
        framed.flush().await.unwrap();
        
        // Handle extended protocol for catalog query
        loop {
            match framed.next().await {
                Some(Ok(msg)) => {
                    eprintln!("Server received: {:?}", msg);
                    
                    match msg {
                        FrontendMessage::Parse { query, .. } => {
                            eprintln!("PARSE: {}", query);
                            
                            // Check if it's a catalog query
                            if query.contains("pg_catalog.pg_class") {
                                eprintln!("  -> Detected catalog query");
                            }
                            
                            framed.send(BackendMessage::ParseComplete).await.unwrap();
                        }
                        FrontendMessage::Bind { .. } => {
                            framed.send(BackendMessage::BindComplete).await.unwrap();
                        }
                        FrontendMessage::Describe { typ, .. } => {
                            eprintln!("DESCRIBE type={}", if typ == b'S' { "Statement" } else { "Portal" });
                            
                            // Send parameter description
                            framed.send(BackendMessage::ParameterDescription(vec![])).await.unwrap();
                            
                            // Send row description for 3 columns
                            let fields = vec![
                                FieldDescription {
                                    name: "oid".to_string(),
                                    table_oid: 0,
                                    column_id: 1,
                                    type_oid: 26, // OID type
                                    type_size: 4,
                                    type_modifier: -1,
                                    format: 0,
                                },
                                FieldDescription {
                                    name: "relname".to_string(),
                                    table_oid: 0,
                                    column_id: 2,
                                    type_oid: PgType::Text.to_oid(),
                                    type_size: -1,
                                    type_modifier: -1,
                                    format: 0,
                                },
                                FieldDescription {
                                    name: "relkind".to_string(),
                                    table_oid: 0,
                                    column_id: 3,
                                    type_oid: PgType::Char.to_oid(),
                                    type_size: 1,
                                    type_modifier: -1,
                                    format: 0,
                                },
                            ];
                            eprintln!("  -> Sending RowDescription with {} fields", fields.len());
                            framed.send(BackendMessage::RowDescription(fields)).await.unwrap();
                        }
                        FrontendMessage::Execute { .. } => {
                            eprintln!("EXECUTE");
                            
                            // Send one data row with 3 columns
                            let row = vec![
                                Some(b"16384".to_vec()),      // oid
                                Some(b"test_table".to_vec()),  // relname
                                Some(b"r".to_vec()),           // relkind
                            ];
                            eprintln!("  -> Sending DataRow with {} values", row.len());
                            framed.send(BackendMessage::DataRow(row)).await.unwrap();
                            
                            framed.send(BackendMessage::CommandComplete {
                                tag: "SELECT 1".to_string(),
                            }).await.unwrap();
                        }
                        FrontendMessage::Sync => {
                            framed.send(BackendMessage::ReadyForQuery {
                                status: pgsqlite::protocol::TransactionStatus::Idle,
                            }).await.unwrap();
                            framed.flush().await.unwrap();
                        }
                        FrontendMessage::Terminate => {
                            break;
                        }
                        _ => {}
                    }
                }
                Some(Err(e)) => {
                    eprintln!("Server error: {}", e);
                    break;
                }
                None => break,
            }
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
    
    // Test the 3-column query
    eprintln!("\nClient: Executing query...");
    match client.query("SELECT oid, relname, relkind FROM pg_catalog.pg_class WHERE relkind = 'r'", &[]).await {
        Ok(rows) => {
            eprintln!("✓ Client: Success! Got {} rows", rows.len());
            if !rows.is_empty() {
                eprintln!("  First row has {} columns", rows[0].len());
            }
        }
        Err(e) => {
            eprintln!("✗ Client: Failed with: {:?}", e);
        }
    }
    
    // Always panic to see output
    panic!("End of trace");
}