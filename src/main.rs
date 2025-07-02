use anyhow::Result;
use clap::Parser;
use futures::SinkExt;
use futures::StreamExt;
use std::sync::Arc;
use std::path::PathBuf;
use tokio::net::{TcpListener, UnixListener};
use tokio_util::codec::Framed;
use tracing::{error, info};

use pgsqlite::protocol::{
    AuthenticationMessage, BackendMessage, ErrorResponse, FrontendMessage, PostgresCodec,
    TransactionStatus,
};
use pgsqlite::query::{ExtendedQueryHandler, QueryExecutor};
use pgsqlite::session::{DbHandler, SessionState};

#[derive(Parser, Debug)]
#[command(name = "pgsqlite")]
#[command(about = "pgsqlite - 🐘 PostgreSQL + 🪶 SQLite = ♥\nPostgreSQL wire protocol server on top of SQLite", long_about = None)]
struct Config {
    #[arg(short, long, default_value = "5432")]
    port: u16,

    #[arg(short, long, default_value = "sqlite.db")]
    database: String,

    #[arg(long, default_value = "info")]
    log_level: String,

    #[arg(long, help = "Use in-memory SQLite database (for testing/benchmarking only)")]
    in_memory: bool,

    #[arg(long, default_value = "/tmp", help = "Directory for Unix domain socket")]
    socket_dir: String,

    #[arg(long, help = "Disable TCP listener and use only Unix socket")]
    no_tcp: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(config.log_level)
        .init();

    // Determine database path based on --in-memory flag
    let db_path = if config.in_memory {
        info!("Using in-memory SQLite database (testing mode)");
        ":memory:".to_string()
    } else {
        config.database.clone()
    };

    // Initialize database handler with direct executor
    let db_handler = Arc::new(
        DbHandler::new(&db_path)
            .map_err(|e| anyhow::anyhow!("Failed to create database handler: {}", e))?,
    );

    // Build socket path
    let socket_path = PathBuf::from(&config.socket_dir).join(format!(".s.PGSQL.{}", config.port));
    
    // Remove existing socket file if it exists
    if socket_path.exists() {
        std::fs::remove_file(&socket_path)?;
    }

    // Create Unix socket listener
    let unix_listener = UnixListener::bind(&socket_path)?;
    info!("Unix socket created at: {}", socket_path.display());
    
    // Set socket permissions to 0777 for compatibility
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o777))?;
    }

    // Create TCP listener if not disabled
    let tcp_listener = if !config.no_tcp {
        let listener = TcpListener::bind(("0.0.0.0", config.port)).await?;
        info!("TCP server listening on port {}", config.port);
        Some(listener)
    } else {
        info!("TCP listener disabled, using Unix socket only");
        None
    };

    if config.in_memory {
        info!("Using in-memory database (for testing/benchmarking only)");
    } else {
        info!("Using database: {}", config.database);
    }

    // Handle cleanup on shutdown
    let socket_path_cleanup = socket_path.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        if socket_path_cleanup.exists() {
            let _ = std::fs::remove_file(&socket_path_cleanup);
            info!("Cleaned up Unix socket file");
        }
        std::process::exit(0);
    });
    
    // Start periodic cache metrics logging
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(300)); // Log every 5 minutes
        loop {
            interval.tick().await;
            pgsqlite::cache::log_cache_status();
        }
    });

    // Accept connections from both TCP and Unix sockets
    loop {
        let db_handler = db_handler.clone();
        
        tokio::select! {
            // Handle TCP connections
            result = async {
                if let Some(ref listener) = tcp_listener {
                    listener.accept().await
                } else {
                    std::future::pending::<Result<(tokio::net::TcpStream, std::net::SocketAddr), std::io::Error>>().await
                }
            } => {
                if let Ok((stream, addr)) = result {
                    info!("New TCP connection from {}", addr);
                    let db_handler = db_handler.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_tcp_connection(stream, addr, db_handler).await {
                            error!("TCP connection error from {}: {}", addr, e);
                        }
                    });
                }
            }
            
            // Handle Unix socket connections
            result = unix_listener.accept() => {
                if let Ok((stream, _addr)) = result {
                    info!("New Unix socket connection");
                    let db_handler = db_handler.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_unix_connection(stream, db_handler).await {
                            error!("Unix socket connection error: {}", e);
                        }
                    });
                }
            }
        }
    }
}

async fn handle_tcp_connection(
    stream: tokio::net::TcpStream,
    addr: std::net::SocketAddr,
    db_handler: Arc<DbHandler>,
) -> Result<()> {
    info!("Handling TCP connection from {}", addr);
    
    // Disable Nagle's algorithm for lower latency
    stream.set_nodelay(true)?;
    
    handle_connection_generic(stream, &addr.to_string(), db_handler).await
}

async fn handle_unix_connection(
    stream: tokio::net::UnixStream,
    db_handler: Arc<DbHandler>,
) -> Result<()> {
    info!("Handling Unix socket connection");
    handle_connection_generic(stream, "unix-socket", db_handler).await
}

async fn handle_connection_generic<S>(
    stream: S,
    connection_info: &str,
    db_handler: Arc<DbHandler>,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    let codec = PostgresCodec::new();
    let mut framed = Framed::new(stream, codec);

    // Wait for startup message
    let startup = match framed.next().await {
        Some(Ok(FrontendMessage::StartupMessage(msg))) => msg,
        Some(Ok(other)) => {
            error!("Expected startup message, got {:?}", other);
            return Err(anyhow::anyhow!("Protocol error: expected startup message"));
        }
        Some(Err(e)) => return Err(e.into()),
        None => return Err(anyhow::anyhow!("Connection closed unexpectedly")),
    };

    info!("Received startup message from {}: {:?}", connection_info, startup);

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
    let _session_id = uuid::Uuid::new_v4();

    // Send authentication OK
    framed
        .send(BackendMessage::Authentication(AuthenticationMessage::Ok))
        .await?;

    // Send parameter status messages
    for (key, value) in session.parameters.read().await.iter() {
        framed
            .send(BackendMessage::ParameterStatus {
                name: key.clone(),
                value: value.clone(),
            })
            .await?;
    }

    // Send backend key data
    framed
        .send(BackendMessage::BackendKeyData {
            process_id: std::process::id() as i32,
            secret_key: rand::random::<i32>(),
        })
        .await?;

    // Send ready for query
    framed
        .send(BackendMessage::ReadyForQuery {
            status: TransactionStatus::Idle,
        })
        .await?;

    info!("Sent authentication and ready response to {}", connection_info);

    // Main message loop
    while let Some(msg) = framed.next().await {
        match msg? {
            FrontendMessage::Query(sql) => {
                info!("Received query from {}: {}", connection_info, sql);

                // Execute the query
                match QueryExecutor::execute_query(&mut framed, &db_handler, &sql).await {
                    Ok(()) => {
                        // Query executed successfully
                    }
                    Err(e) => {
                        error!("Query execution error: {}", e);
                        let err = ErrorResponse::new(
                            "ERROR".to_string(),
                            "42000".to_string(),
                            format!("Query execution failed: {}", e),
                        );
                        framed.send(BackendMessage::ErrorResponse(err)).await?;
                    }
                }

                // Always send ReadyForQuery after handling the query
                framed
                    .send(BackendMessage::ReadyForQuery {
                        status: *session.transaction_status.read().await,
                    })
                    .await?;
                // Flush to ensure message is sent immediately
                framed.flush().await?;
            }
            FrontendMessage::Parse {
                name,
                query,
                param_types,
            } => {
                match ExtendedQueryHandler::handle_parse(
                    &mut framed,
                    &db_handler,
                    &session,
                    name,
                    query,
                    param_types,
                )
                .await
                {
                    Ok(()) => {}
                    Err(e) => {
                        error!("Parse error: {}", e);
                        let err = ErrorResponse::new(
                            "ERROR".to_string(),
                            "42000".to_string(),
                            format!("Parse failed: {}", e),
                        );
                        framed.send(BackendMessage::ErrorResponse(err)).await?;
                        framed
                            .send(BackendMessage::ReadyForQuery {
                                status: *session.transaction_status.read().await,
                            })
                            .await?;
                    }
                }
            }
            FrontendMessage::Bind {
                portal,
                statement,
                formats,
                values,
                result_formats,
            } => {
                match ExtendedQueryHandler::handle_bind(
                    &mut framed,
                    &session,
                    portal,
                    statement,
                    formats,
                    values,
                    result_formats,
                )
                .await
                {
                    Ok(()) => {}
                    Err(e) => {
                        error!("Bind error: {}", e);
                        let err = ErrorResponse::new(
                            "ERROR".to_string(),
                            "42000".to_string(),
                            format!("Bind failed: {}", e),
                        );
                        framed.send(BackendMessage::ErrorResponse(err)).await?;
                        framed
                            .send(BackendMessage::ReadyForQuery {
                                status: *session.transaction_status.read().await,
                            })
                            .await?;
                    }
                }
            }
            FrontendMessage::Execute { portal, max_rows } => {
                match ExtendedQueryHandler::handle_execute(
                    &mut framed,
                    &db_handler,
                    &session,
                    portal,
                    max_rows,
                )
                .await
                {
                    Ok(()) => {}
                    Err(e) => {
                        error!("Execute error: {}", e);
                        let err = ErrorResponse::new(
                            "ERROR".to_string(),
                            "42000".to_string(),
                            format!("Execute failed: {}", e),
                        );
                        framed.send(BackendMessage::ErrorResponse(err)).await?;
                        framed
                            .send(BackendMessage::ReadyForQuery {
                                status: *session.transaction_status.read().await,
                            })
                            .await?;
                    }
                }
            }
            FrontendMessage::Describe { typ, name } => {
                match ExtendedQueryHandler::handle_describe(&mut framed, &session, typ, name).await
                {
                    Ok(()) => {}
                    Err(e) => {
                        error!("Describe error: {}", e);
                        let err = ErrorResponse::new(
                            "ERROR".to_string(),
                            "42000".to_string(),
                            format!("Describe failed: {}", e),
                        );
                        framed.send(BackendMessage::ErrorResponse(err)).await?;
                        framed
                            .send(BackendMessage::ReadyForQuery {
                                status: *session.transaction_status.read().await,
                            })
                            .await?;
                    }
                }
            }
            FrontendMessage::Close { typ, name } => {
                match ExtendedQueryHandler::handle_close(&mut framed, &session, typ, name).await {
                    Ok(()) => {}
                    Err(e) => {
                        error!("Close error: {}", e);
                        let err = ErrorResponse::new(
                            "ERROR".to_string(),
                            "42000".to_string(),
                            format!("Close failed: {}", e),
                        );
                        framed.send(BackendMessage::ErrorResponse(err)).await?;
                        framed
                            .send(BackendMessage::ReadyForQuery {
                                status: *session.transaction_status.read().await,
                            })
                            .await?;
                    }
                }
            }
            FrontendMessage::Sync => {
                // Send ReadyForQuery to indicate we're ready for more commands
                framed
                    .send(BackendMessage::ReadyForQuery {
                        status: *session.transaction_status.read().await,
                    })
                    .await?;
            }
            FrontendMessage::Flush => {
                // Flush any pending messages
                framed.flush().await?;
            }
            FrontendMessage::Terminate => {
                info!("Client {} requested termination", connection_info);
                break;
            }
            other => {
                info!("Received unhandled message from {}: {:?}", connection_info, other);
            }
        }
    }

    info!("Connection from {} closed", connection_info);
    Ok(())
}
