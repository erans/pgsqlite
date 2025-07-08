use anyhow::Result;
use bytes::{Buf, BytesMut};
use futures::SinkExt;
use futures::StreamExt;
use std::sync::Arc;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio_util::codec::Framed;
use tracing::{error, info};
use tokio_rustls::TlsAcceptor;

use pgsqlite::config::Config;
use pgsqlite::protocol::{
    AuthenticationMessage, BackendMessage, ErrorResponse, FrontendMessage, PostgresCodec,
    TransactionStatus,
};
use pgsqlite::query::{ExtendedQueryHandler, QueryExecutor};
use pgsqlite::session::{DbHandler, SessionState};
use pgsqlite::ssl::CertificateManager;
use pgsqlite::migration::MigrationRunner;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(config.log_level.clone())
        .init();

    // Display version
    info!("pgsqlite v{}", env!("CARGO_PKG_VERSION"));

    // Determine database path based on --in-memory flag
    let db_path = if config.in_memory {
        info!("Using in-memory SQLite database (testing mode)");
        ":memory:".to_string()
    } else {
        config.database.clone()
    };

    // Handle migration command
    if config.migrate {
        info!("Running database migrations...");
        
        // Open connection directly for migration
        let conn = rusqlite::Connection::open(&db_path)
            .map_err(|e| anyhow::anyhow!("Failed to open database: {}", e))?;
        
        let mut runner = MigrationRunner::new(conn);
        match runner.run_pending_migrations() {
            Ok(applied) => {
                if applied.is_empty() {
                    info!("No pending migrations. Database is up to date.");
                } else {
                    info!("Successfully applied {} migrations: {:?}", applied.len(), applied);
                }
                std::process::exit(0);
            }
            Err(e) => {
                error!("Migration failed: {}", e);
                std::process::exit(1);
            }
        }
    }

    // Initialize database handler with direct executor
    let db_handler = Arc::new(
        DbHandler::new_with_config(&db_path, &config)
            .map_err(|e| anyhow::anyhow!("Failed to create database handler: {}", e))?,
    );

    // Unix socket setup (only on Unix platforms)
    #[cfg(unix)]
    let (socket_path, unix_listener) = {
        let socket_path = PathBuf::from(&config.socket_dir).join(format!(".s.PGSQL.{}", config.port));
        
        // Remove existing socket file if it exists
        if socket_path.exists() {
            std::fs::remove_file(&socket_path)?;
        }

        // Create Unix socket listener
        let unix_listener = UnixListener::bind(&socket_path)?;
        info!("Unix socket created at: {}", socket_path.display());
        
        // Set socket permissions to 0777 for compatibility
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o777))?;
        
        (socket_path, unix_listener)
    };

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

    // Initialize SSL if enabled
    let tls_acceptor = if config.ssl {
        if config.no_tcp {
            return Err(anyhow::anyhow!("SSL cannot be enabled when TCP is disabled"));
        }
        let cert_manager = CertificateManager::new(Arc::new(config.clone()));
        let (acceptor, _cert_source) = cert_manager.initialize().await?;
        Some(acceptor)
    } else {
        info!("SSL disabled - using unencrypted connections");
        None
    };

    // Handle cleanup on shutdown
    #[cfg(unix)]
    {
        let socket_path_cleanup = socket_path.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            if socket_path_cleanup.exists() {
                let _ = std::fs::remove_file(&socket_path_cleanup);
                info!("Cleaned up Unix socket file");
            }
            std::process::exit(0);
        });
    }
    
    #[cfg(not(unix))]
    {
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            std::process::exit(0);
        });
    }
    
    // Start periodic cache metrics logging
    let cache_metrics_interval = config.cache_metrics_interval_duration();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(cache_metrics_interval);
        loop {
            interval.tick().await;
            pgsqlite::cache::log_cache_status();
        }
    });

    // Accept connections from both TCP and Unix sockets
    #[cfg(unix)]
    {
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
                        let tls_acceptor = tls_acceptor.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_tcp_connection(stream, addr, db_handler, tls_acceptor).await {
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
    
    #[cfg(not(unix))]
    {
        // Windows/non-Unix: only handle TCP connections
        loop {
            let db_handler = db_handler.clone();
            
            if let Some(ref listener) = tcp_listener {
                if let Ok((stream, addr)) = listener.accept().await {
                    info!("New TCP connection from {}", addr);
                    let db_handler = db_handler.clone();
                    let tls_acceptor = tls_acceptor.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_tcp_connection(stream, addr, db_handler, tls_acceptor).await {
                            error!("TCP connection error from {}: {}", addr, e);
                        }
                    });
                }
            } else {
                // No TCP listener and no Unix sockets on Windows
                error!("No listeners available on Windows when TCP is disabled");
                return Err(anyhow::anyhow!("Cannot run without TCP on Windows"));
            }
        }
    }
}

async fn handle_tcp_connection(
    stream: tokio::net::TcpStream,
    addr: std::net::SocketAddr,
    db_handler: Arc<DbHandler>,
    tls_acceptor: Option<TlsAcceptor>,
) -> Result<()> {
    info!("Handling TCP connection from {}", addr);
    
    // Disable Nagle's algorithm for lower latency
    stream.set_nodelay(true)?;
    
    // Always handle potential SSL requests, even if SSL is disabled
    handle_ssl_negotiation(stream, addr, db_handler, tls_acceptor).await
}

async fn handle_ssl_negotiation(
    mut stream: tokio::net::TcpStream,
    addr: std::net::SocketAddr,
    db_handler: Arc<DbHandler>,
    tls_acceptor: Option<TlsAcceptor>,
) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    
    // Read the first message to check if it's an SSL request
    let mut buf = vec![0u8; 8];
    stream.read_exact(&mut buf).await?;
    
    // Check if this is an SSL request
    let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let code = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    
    if len == 8 && code == 80877103 {
        // This is an SSL request
        if let Some(tls_acceptor) = tls_acceptor {
            // SSL is enabled, send 'S' to indicate SSL is available
            stream.write_all(b"S").await?;
            stream.flush().await?;
            
            // Perform TLS handshake
            let tls_stream = tls_acceptor.accept(stream).await?;
            info!("SSL connection established with {}", addr);
            
            // Handle the connection with TLS
            handle_connection_generic(tls_stream, &addr.to_string(), db_handler).await
        } else {
            // SSL is disabled, send 'N' to indicate SSL is not available
            stream.write_all(b"N").await?;
            stream.flush().await?;
            info!("Rejected SSL request from {} (SSL disabled)", addr);
            
            // Continue with non-SSL connection
            handle_connection_generic(stream, &addr.to_string(), db_handler).await
        }
    } else {
        // Not an SSL request, we need to handle this as a regular startup message
        // Create a new buffer with the data we already read
        let initial_data = BytesMut::from(&buf[..]);
        
        // Create a custom stream that will first return our buffered data
        let stream_with_buffer = StreamWithBuffer::new(stream, initial_data);
        handle_connection_generic(stream_with_buffer, &addr.to_string(), db_handler).await
    }
}

#[cfg(unix)]
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
                match QueryExecutor::execute_query(&mut framed, &db_handler, &session, &sql).await {
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

// Helper struct to handle streams with pre-read data
struct StreamWithBuffer<S> {
    stream: S,
    buffer: BytesMut,
}

impl<S> StreamWithBuffer<S> {
    fn new(stream: S, buffer: BytesMut) -> Self {
        Self { stream, buffer }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for StreamWithBuffer<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // First, drain any buffered data
        if !self.buffer.is_empty() {
            let len = std::cmp::min(buf.remaining(), self.buffer.len());
            buf.put_slice(&self.buffer[..len]);
            self.buffer.advance(len);
            return Poll::Ready(Ok(()));
        }
        
        // Then read from the underlying stream
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for StreamWithBuffer<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
