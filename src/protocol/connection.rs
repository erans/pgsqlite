use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use futures::{StreamExt, Stream, SinkExt};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::protocol::{PostgresCodec, FrontendMessage, BackendMessage, ProtocolWriter, FramedWriter, DirectWriter, WriterType, TransactionStatus};
use crate::PgSqliteError;

/// A unified connection type that provides a migration path from Framed to ProtocolWriter
/// 
/// This allows us to gradually migrate from the current Framed-based implementation
/// to the new ProtocolWriter-based implementation without breaking existing code.
pub enum Connection {
    /// Traditional Framed connection (current implementation)
    Framed(Framed<TcpStream, PostgresCodec>),
    /// New protocol writer based connection
    Writer {
        stream: TcpStream,
        writer: Box<dyn ProtocolWriter + Send>,
        codec: PostgresCodec,
    },
}

impl Connection {
    /// Create a new connection using the configuration
    pub fn new(stream: TcpStream) -> Self {
        let writer_type = WriterType::from_config();
        Self::with_writer_type(stream, writer_type)
    }
    
    /// Create a new connection with a specific writer type
    pub fn with_writer_type(stream: TcpStream, writer_type: WriterType) -> Self {
        match writer_type {
            WriterType::Framed => {
                // Use traditional Framed approach
                let codec = PostgresCodec::new();
                Connection::Framed(Framed::new(stream, codec))
            }
            WriterType::Direct => {
                // For now, we'll use FramedWriter with DirectWriter later
                // TODO: Implement proper stream splitting for DirectWriter
                let codec = PostgresCodec::new();
                Connection::Framed(Framed::new(stream, codec))
            }
        }
    }
    
    /// Create from an existing Framed (for backward compatibility)
    pub fn from_framed(framed: Framed<TcpStream, PostgresCodec>) -> Self {
        Connection::Framed(framed)
    }
    
    /// Convert to use ProtocolWriter (for migration)
    pub fn into_writer_mode(self) -> Self {
        match self {
            Connection::Framed(framed) => {
                // Convert Framed to Writer mode
                let writer = Box::new(FramedWriter::new(framed));
                // Note: This is a simplified conversion - in practice we'd need to handle
                // the stream splitting properly
                Connection::Framed(writer.into_framed())
            }
            Connection::Writer { .. } => self, // Already in writer mode
        }
    }
    
    /// Send a backend message
    pub async fn send(&mut self, msg: BackendMessage) -> Result<(), PgSqliteError> {
        match self {
            Connection::Framed(framed) => {
                framed.send(msg).await?;
                Ok(())
            }
            Connection::Writer { writer, .. } => {
                // Convert BackendMessage to appropriate ProtocolWriter calls
                match msg {
                    BackendMessage::Authentication(auth) => {
                        match auth {
                            crate::protocol::AuthenticationMessage::Ok => writer.send_auth_ok().await,
                            _ => unimplemented!("Other auth types not yet implemented"),
                        }
                    }
                    BackendMessage::ParameterStatus { name, value } => {
                        writer.send_parameter_status(&name, &value).await
                    }
                    BackendMessage::BackendKeyData { process_id, secret_key } => {
                        writer.send_backend_key_data(process_id, secret_key).await
                    }
                    BackendMessage::ReadyForQuery { status } => {
                        writer.send_ready_for_query(status).await
                    }
                    BackendMessage::RowDescription(fields) => {
                        writer.send_row_description(&fields).await
                    }
                    BackendMessage::DataRow(values) => {
                        writer.send_data_row(&values).await
                    }
                    BackendMessage::CommandComplete { tag } => {
                        writer.send_command_complete(&tag).await
                    }
                    BackendMessage::ErrorResponse(err) => {
                        // Convert ErrorResponse to PgSqliteError
                        let error = PgSqliteError::Protocol(err.message.clone());
                        writer.send_error(&error).await
                    }
                    BackendMessage::ParseComplete => writer.send_parse_complete().await,
                    BackendMessage::BindComplete => writer.send_bind_complete().await,
                    BackendMessage::CloseComplete => writer.send_close_complete().await,
                    BackendMessage::NoData => writer.send_no_data().await,
                    BackendMessage::PortalSuspended => writer.send_portal_suspended().await,
                    _ => unimplemented!("Message type not yet implemented for ProtocolWriter"),
                }
            }
        }
    }
    
    /// Receive the next message
    pub async fn next(&mut self) -> Option<Result<FrontendMessage, PgSqliteError>> {
        match self {
            Connection::Framed(framed) => framed.next().await,
            Connection::Writer { .. } => {
                // TODO: Implement proper reading for Writer mode
                unimplemented!("Reading in Writer mode not yet implemented")
            }
        }
    }
    
    /// Flush any pending data
    pub async fn flush(&mut self) -> Result<(), PgSqliteError> {
        match self {
            Connection::Framed(framed) => {
                framed.flush().await?;
                Ok(())
            }
            Connection::Writer { writer, .. } => {
                writer.flush().await
            }
        }
    }
    
    /// Get access to the writer (if in writer mode)
    pub fn writer(&mut self) -> Option<&mut dyn ProtocolWriter> {
        match self {
            Connection::Framed(_) => None,
            Connection::Writer { writer, .. } => Some(&mut **writer),
        }
    }
}

/// Helper trait to make Connection easier to use
pub trait ConnectionExt {
    async fn send_auth_ok(&mut self) -> Result<(), PgSqliteError>;
    async fn send_parameter_status(&mut self, name: &str, value: &str) -> Result<(), PgSqliteError>;
    async fn send_backend_key_data(&mut self, process_id: i32, secret_key: i32) -> Result<(), PgSqliteError>;
    async fn send_ready_for_query(&mut self, status: TransactionStatus) -> Result<(), PgSqliteError>;
}

impl ConnectionExt for Connection {
    async fn send_auth_ok(&mut self) -> Result<(), PgSqliteError> {
        self.send(BackendMessage::Authentication(crate::protocol::AuthenticationMessage::Ok)).await
    }
    
    async fn send_parameter_status(&mut self, name: &str, value: &str) -> Result<(), PgSqliteError> {
        self.send(BackendMessage::ParameterStatus {
            name: name.to_string(),
            value: value.to_string(),
        }).await
    }
    
    async fn send_backend_key_data(&mut self, process_id: i32, secret_key: i32) -> Result<(), PgSqliteError> {
        self.send(BackendMessage::BackendKeyData { process_id, secret_key }).await
    }
    
    async fn send_ready_for_query(&mut self, status: TransactionStatus) -> Result<(), PgSqliteError> {
        self.send(BackendMessage::ReadyForQuery { status }).await
    }
}

/// Make Connection work as a Stream for compatibility
impl Stream for Connection {
    type Item = Result<FrontendMessage, PgSqliteError>;
    
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Delegate to the async next() method
        let fut = self.next();
        futures::pin_mut!(fut);
        fut.poll(cx)
    }
}