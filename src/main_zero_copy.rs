use anyhow::Result;
use tokio::net::TcpListener;
use std::sync::Arc;
use tracing::{info, error, debug};

use crate::session::{SessionState, DbHandler};
use crate::protocol::{Connection, ConnectionExt, FrontendMessage, BackendMessage, TransactionStatus};
use crate::query::{QueryExecutor, ExtendedQueryHandler};

/// Example of how to use the new Connection type with zero-copy protocol
pub async fn handle_tcp_connection_zero_copy(
    stream: tokio::net::TcpStream,
    addr: std::net::SocketAddr,
    db_handler: Arc<DbHandler>,
) -> Result<()> {
    info!("Handling TCP connection from {} with zero-copy protocol", addr);
    
    // Create connection using the new Connection type
    let mut conn = Connection::new(stream);
    
    // Wait for startup message
    let startup = match conn.next().await {
        Some(Ok(FrontendMessage::StartupMessage(msg))) => msg,
        Some(Ok(other)) => {
            error!("Expected startup message, got {:?}", other);
            return Err(anyhow::anyhow!("Protocol error: expected startup message"));
        }
        Some(Err(e)) => return Err(e.into()),
        None => return Err(anyhow::anyhow!("Connection closed unexpectedly")),
    };
    
    info!("Received startup message: {:?}", startup);
    
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
    
    // Send authentication OK using the ConnectionExt trait
    conn.send_auth_ok().await?;
    
    // Send parameter status messages
    for (key, value) in session.parameters.read().await.iter() {
        conn.send_parameter_status(key, value).await?;
    }
    
    // Send backend key data
    conn.send_backend_key_data(std::process::id() as i32, 12345).await?;
    
    // Send ready for query
    conn.send_ready_for_query(TransactionStatus::Idle).await?;
    
    info!("Connection setup complete, entering main loop");
    
    // Main message loop
    loop {
        let message = match conn.next().await {
            Some(Ok(msg)) => msg,
            Some(Err(e)) => {
                error!("Error reading message: {}", e);
                break;
            }
            None => {
                info!("Client closed connection");
                break;
            }
        };
        
        debug!("Received message: {:?}", message);
        
        match message {
            FrontendMessage::Query(sql) => {
                // For now, we'll use the existing implementation
                // In the future, we'll modify QueryExecutor to use ProtocolWriter
                error!("Query execution not yet implemented for Connection type");
                conn.send(BackendMessage::ErrorResponse(
                    crate::protocol::ErrorResponse::new(
                        "ERROR".to_string(),
                        "0A000".to_string(),
                        "Query execution not yet implemented for zero-copy protocol".to_string(),
                    )
                )).await?;
                conn.send_ready_for_query(*session.transaction_status.read().await).await?;
            }
            
            FrontendMessage::Terminate => {
                info!("Client requested termination");
                break;
            }
            
            _ => {
                error!("Unhandled message type in zero-copy handler");
                conn.send(BackendMessage::ErrorResponse(
                    crate::protocol::ErrorResponse::new(
                        "ERROR".to_string(),
                        "0A000".to_string(),
                        "Feature not implemented in zero-copy protocol".to_string(),
                    )
                )).await?;
            }
        }
    }
    
    Ok(())
}

/// Start a server using the zero-copy protocol
pub async fn run_zero_copy_server(addr: &str, db_path: &str) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("Zero-copy server listening on {}", addr);
    
    let db_handler = Arc::new(DbHandler::new(db_path)?);
    
    loop {
        let (stream, addr) = listener.accept().await?;
        let db_handler = db_handler.clone();
        
        tokio::spawn(async move {
            if let Err(e) = handle_tcp_connection_zero_copy(stream, addr, db_handler).await {
                error!("Connection error: {}", e);
            }
        });
    }
}