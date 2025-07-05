use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use pgsqlite::protocol::{PostgresCodec, FrontendMessage, BackendMessage};
use pgsqlite::session::{DbHandler, SessionState};
use pgsqlite::catalog::CatalogInterceptor;
use std::sync::Arc;
use futures::{SinkExt, StreamExt};

#[tokio::test]
#[ignore = "Diagnostic test that intentionally panics to show trace output"]
async fn trace_extended_catalog() {
    let _ = env_logger::builder().is_test(true).try_init();
    
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    eprintln!("Test server on port {}", port);
    
    let _server_handle = tokio::spawn(async move {
        let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
        
        // Create test table
        db_handler.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY)").await.unwrap();
        
        let (stream, addr) = listener.accept().await.unwrap();
        eprintln!("Server: Accepted connection from {}", addr);
        
        let codec = PostgresCodec::new();
        let mut framed = Framed::new(stream, codec);
        let session = Arc::new(SessionState::new("test".to_string(), "testuser".to_string()));
        
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
        
        // Handle extended protocol
        let mut stored_query = String::new();
        
        loop {
            match framed.next().await {
                Some(Ok(msg)) => {
                    eprintln!("\nServer received: {:?}", msg);
                    
                    match msg {
                        FrontendMessage::Parse { query, name, .. } => {
                            stored_query = query.clone();
                            eprintln!("  PARSE '{}': {}", name, query);
                            
                            // Use the actual extended handler
                            if let Err(e) = pgsqlite::query::extended::ExtendedQueryHandler::handle_parse(
                                &mut framed, 
                                &db_handler, 
                                &session, 
                                name, 
                                query, 
                                vec![]
                            ).await {
                                eprintln!("  Parse error: {}", e);
                            }
                        }
                        FrontendMessage::Bind { statement, portal, .. } => {
                            eprintln!("  BIND statement '{}' to portal '{}'", statement, portal);
                            
                            if let Err(e) = pgsqlite::query::extended::ExtendedQueryHandler::handle_bind(
                                &mut framed,
                                &session,
                                portal,
                                statement,
                                vec![],
                                vec![],
                                vec![],
                            ).await {
                                eprintln!("  Bind error: {}", e);
                            }
                        }
                        FrontendMessage::Describe { typ, name } => {
                            eprintln!("  DESCRIBE {} '{}'", if typ == b'S' { "statement" } else { "portal" }, name);
                            
                            if let Err(e) = pgsqlite::query::extended::ExtendedQueryHandler::handle_describe(
                                &mut framed,
                                &session,
                                typ,
                                name,
                            ).await {
                                eprintln!("  Describe error: {}", e);
                            }
                        }
                        FrontendMessage::Execute { portal, max_rows } => {
                            eprintln!("  EXECUTE portal '{}' (max_rows: {})", portal, max_rows);
                            eprintln!("  Query is: {}", stored_query);
                            
                            // Check if it's a catalog query
                            if stored_query.contains("pg_catalog") {
                                eprintln!("  -> This is a catalog query!");
                                
                                // Try to intercept it
                                match CatalogInterceptor::intercept_query(&stored_query, db_handler.clone()).await {
                                    Some(Ok(response)) => {
                                        eprintln!("  -> Catalog interceptor returned {} columns, {} rows", 
                                            response.columns.len(), response.rows.len());
                                        eprintln!("  -> Columns: {:?}", response.columns);
                                    }
                                    Some(Err(e)) => {
                                        eprintln!("  -> Catalog interceptor error: {}", e);
                                    }
                                    None => {
                                        eprintln!("  -> Catalog interceptor returned None");
                                    }
                                }
                            }
                            
                            // Now use the actual execute handler
                            if let Err(e) = pgsqlite::query::extended::ExtendedQueryHandler::handle_execute(
                                &mut framed,
                                &db_handler,
                                &session,
                                portal,
                                max_rows,
                            ).await {
                                eprintln!("  Execute error: {}", e);
                            }
                        }
                        FrontendMessage::Sync => {
                            eprintln!("  SYNC");
                            framed.send(BackendMessage::ReadyForQuery {
                                status: *session.transaction_status.read().await,
                            }).await.unwrap();
                            framed.flush().await.unwrap();
                        }
                        FrontendMessage::Terminate => {
                            eprintln!("  TERMINATE");
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
    
    // Test catalog query
    eprintln!("\n=== Client: Testing catalog query ===");
    match client.query("SELECT relname, relkind FROM pg_catalog.pg_class WHERE relkind = 'r'", &[]).await {
        Ok(rows) => {
            eprintln!("✓ Client: Success! Got {} rows", rows.len());
        }
        Err(e) => {
            eprintln!("✗ Client: Failed with: {:?}", e);
        }
    }
    
    panic!("End of trace - check output above");
}