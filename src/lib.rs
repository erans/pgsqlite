pub mod protocol;
pub mod session;
pub mod translator;
pub mod types;
pub mod catalog;
pub mod functions;
pub mod query;
pub mod metadata;
pub mod rewriter;
pub mod cache;
pub mod config;

#[cfg(test)]
pub mod alloc_tracker;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum PgSqliteError {
    #[error("Protocol error: {0}")]
    Protocol(String),
    
    #[error("SQL parse error: {0}")]
    SqlParse(#[from] sqlparser::parser::ParserError),
    
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    
    #[error("Type conversion error: {0}")]
    TypeConversion(String),
    
    #[error("Feature not supported: {0}")]
    NotSupported(String),
    
    #[error("Authentication failed")]
    AuthenticationFailed,
    
    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, PgSqliteError>;

impl PgSqliteError {
    /// Get the PostgreSQL error code for this error
    pub fn pg_error_code(&self) -> &str {
        match self {
            PgSqliteError::Protocol(_) => "08P01", // protocol_violation
            PgSqliteError::SqlParse(_) => "42601", // syntax_error
            PgSqliteError::Sqlite(_) => "58000", // system_error
            PgSqliteError::TypeConversion(_) => "22P02", // invalid_text_representation
            PgSqliteError::NotSupported(_) => "0A000", // feature_not_supported
            PgSqliteError::AuthenticationFailed => "28000", // invalid_authorization_specification
            PgSqliteError::InvalidParameter(_) => "22023", // invalid_parameter_value
            PgSqliteError::Io(_) => "58030", // io_error
        }
    }
}

// Test helper to expose connection handler
#[doc(hidden)]
pub async fn handle_test_connection(
    stream: tokio::net::TcpStream,
    _addr: std::net::SocketAddr,
) -> anyhow::Result<()> {
    use session::DbHandler;
    let db_handler = std::sync::Arc::new(DbHandler::new(":memory:")?);
    handle_test_connection_with_pool(stream, _addr, db_handler).await
}

#[doc(hidden)]
pub async fn handle_test_connection_with_pool(
    stream: tokio::net::TcpStream,
    _addr: std::net::SocketAddr,
    db_handler: std::sync::Arc<session::DbHandler>,
) -> anyhow::Result<()> {
    use tokio_util::codec::Framed;
    use futures::{SinkExt, StreamExt};
    use std::sync::Arc;
    use protocol::{PostgresCodec, FrontendMessage, BackendMessage, AuthenticationMessage, TransactionStatus, ErrorResponse};
    use session::SessionState;
    use query::{QueryExecutor, ExtendedQueryHandler};
    use tracing::debug;
    
    let codec = PostgresCodec::new();
    let mut framed = Framed::new(stream, codec);
    
    // Wait for startup message
    let startup = match framed.next().await {
        Some(Ok(FrontendMessage::StartupMessage(msg))) => msg,
        _ => return Err(anyhow::anyhow!("Expected startup message")),
    };
    
    // Extract session parameters
    let mut database = "main".to_string();
    let mut user = "postgres".to_string();
    
    for (key, value) in &startup.parameters {
        match key.as_str() {
            "database" => database = value.clone(),
            "user" => user = value.clone(),
            _ => {}
        }
    }
    
    let session = Arc::new(SessionState::new(database, user));
    
    // Send authentication OK
    framed.send(BackendMessage::Authentication(AuthenticationMessage::Ok)).await?;
    
    // Send parameter status messages
    for (key, value) in session.parameters.read().await.iter() {
        framed.send(BackendMessage::ParameterStatus {
            name: key.clone(),
            value: value.clone(),
        }).await?;
    }
    
    // Send backend key data
    framed.send(BackendMessage::BackendKeyData {
        process_id: std::process::id() as i32,
        secret_key: 12345,
    }).await?;
    
    // Send ready for query
    framed.send(BackendMessage::ReadyForQuery {
        status: TransactionStatus::Idle,
    }).await?;
    
    // Main message loop
    while let Some(msg) = framed.next().await {
        let message = msg?;
        debug!("Received message: {:?}", message);
        match message {
            FrontendMessage::Query(sql) => {
                // Execute the query
                match QueryExecutor::execute_query(&mut framed, &db_handler, &sql).await {
                    Ok(()) => {
                        // Query executed successfully
                    }
                    Err(e) => {
                        let err = ErrorResponse::new(
                            "ERROR".to_string(),
                            "42000".to_string(),
                            format!("Query execution failed: {}", e),
                        );
                        framed.send(BackendMessage::ErrorResponse(err)).await?;
                    }
                }
                
                // Always send ReadyForQuery after handling the query
                framed.send(BackendMessage::ReadyForQuery {
                    status: *session.transaction_status.read().await,
                }).await?;
            }
            FrontendMessage::Parse { name, query, param_types } => {
                match ExtendedQueryHandler::handle_parse(&mut framed, &db_handler, &session, name, query, param_types).await {
                    Ok(()) => {},
                    Err(e) => {
                        let err = ErrorResponse::new(
                            "ERROR".to_string(),
                            "42000".to_string(),
                            format!("Parse failed: {}", e),
                        );
                        framed.send(BackendMessage::ErrorResponse(err)).await?;
                    }
                }
            }
            FrontendMessage::Bind { portal, statement, formats, values, result_formats } => {
                match ExtendedQueryHandler::handle_bind(&mut framed, &session, portal, statement, formats, values, result_formats).await {
                    Ok(()) => {},
                    Err(e) => {
                        let err = ErrorResponse::new(
                            "ERROR".to_string(),
                            "42000".to_string(),
                            format!("Bind failed: {}", e),
                        );
                        framed.send(BackendMessage::ErrorResponse(err)).await?;
                    }
                }
            }
            FrontendMessage::Execute { portal, max_rows } => {
                match ExtendedQueryHandler::handle_execute(&mut framed, &db_handler, &session, portal, max_rows).await {
                    Ok(()) => {},
                    Err(e) => {
                        let err = ErrorResponse::new(
                            "ERROR".to_string(),
                            "42000".to_string(),
                            format!("Execute failed: {}", e),
                        );
                        framed.send(BackendMessage::ErrorResponse(err)).await?;
                    }
                }
            }
            FrontendMessage::Describe { typ, name } => {
                match ExtendedQueryHandler::handle_describe(&mut framed, &session, typ, name).await {
                    Ok(()) => {},
                    Err(e) => {
                        let err = ErrorResponse::new(
                            "ERROR".to_string(),
                            "42000".to_string(),
                            format!("Describe failed: {}", e),
                        );
                        framed.send(BackendMessage::ErrorResponse(err)).await?;
                    }
                }
            }
            FrontendMessage::Close { typ, name } => {
                match ExtendedQueryHandler::handle_close(&mut framed, &session, typ, name).await {
                    Ok(()) => {},
                    Err(e) => {
                        let err = ErrorResponse::new(
                            "ERROR".to_string(),
                            "42000".to_string(),
                            format!("Close failed: {}", e),
                        );
                        framed.send(BackendMessage::ErrorResponse(err)).await?;
                    }
                }
            }
            FrontendMessage::Sync => {
                framed.send(BackendMessage::ReadyForQuery {
                    status: *session.transaction_status.read().await,
                }).await?;
                // Flush to ensure ReadyForQuery is sent immediately
                framed.flush().await?;
            }
            FrontendMessage::Flush => {
                framed.flush().await?;
            }
            FrontendMessage::Terminate => break,
            other => {
                eprintln!("Unhandled message: {:?}", other);
                let err = ErrorResponse::new(
                    "ERROR".to_string(),
                    "0A000".to_string(),
                    format!("Feature not supported: {:?}", other),
                );
                framed.send(BackendMessage::ErrorResponse(err)).await?;
                framed.send(BackendMessage::ReadyForQuery {
                    status: *session.transaction_status.read().await,
                }).await?;
            }
        }
    }
    
    Ok(())
}