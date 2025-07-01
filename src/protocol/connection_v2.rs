use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use futures::{StreamExt, Stream};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::protocol::{PostgresCodec, FrontendMessage, BackendMessage, ProtocolWriter, FramedWriter, WriterType, TransactionStatus};
use crate::PgSqliteError;

#[cfg(feature = "zero-copy-protocol")]
use crate::protocol::connection_direct::DirectConnection;

/// A unified connection type that provides a migration path from Framed to ProtocolWriter
/// 
/// This V2 version properly supports both Framed and Direct connections
pub enum ConnectionV2 {
    /// Traditional Framed connection (current implementation)
    Framed(Framed<TcpStream, PostgresCodec>),
    
    /// Zero-copy DirectConnection
    #[cfg(feature = "zero-copy-protocol")]
    Direct(DirectConnection),
}

impl ConnectionV2 {
    /// Create a new connection using the configuration
    pub fn new(stream: TcpStream) -> Self {
        let writer_type = WriterType::from_config();
        Self::with_writer_type(stream, writer_type)
    }
    
    /// Create a new connection with a specific writer type
    pub fn with_writer_type(stream: TcpStream, writer_type: WriterType) -> Self {
        match writer_type {
            WriterType::Framed => {
                let codec = PostgresCodec::new();
                ConnectionV2::Framed(Framed::new(stream, codec))
            }
            WriterType::Direct => {
                #[cfg(feature = "zero-copy-protocol")]
                {
                    ConnectionV2::Direct(DirectConnection::new(stream))
                }
                #[cfg(not(feature = "zero-copy-protocol"))]
                {
                    let codec = PostgresCodec::new();
                    ConnectionV2::Framed(Framed::new(stream, codec))
                }
            }
        }
    }
    
    /// Create from an existing Framed (for backward compatibility)
    pub fn from_framed(framed: Framed<TcpStream, PostgresCodec>) -> Self {
        ConnectionV2::Framed(framed)
    }
    
    /// Get access to the protocol writer
    pub async fn with_writer<F, R>(&mut self, f: F) -> Result<R, PgSqliteError>
    where
        F: FnOnce(&mut dyn ProtocolWriter) -> futures::future::BoxFuture<'_, Result<R, PgSqliteError>>,
    {
        match self {
            ConnectionV2::Framed(framed) => {
                // Create a temporary FramedWriter
                let mut writer = FramedWriter::new_ref(framed);
                f(&mut writer).await
            }
            #[cfg(feature = "zero-copy-protocol")]
            ConnectionV2::Direct(direct) => {
                f(direct.writer()).await
            }
        }
    }
    
    /// Send a backend message (compatibility layer)
    pub async fn send(&mut self, msg: BackendMessage) -> Result<(), PgSqliteError> {
        self.with_writer(|writer| {
            Box::pin(async move {
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
            })
        }).await
    }
    
    /// Receive the next message
    pub async fn next(&mut self) -> Option<Result<FrontendMessage, PgSqliteError>> {
        match self {
            ConnectionV2::Framed(framed) => {
                match framed.next().await {
                    Some(Ok(msg)) => Some(Ok(msg)),
                    Some(Err(e)) => Some(Err(PgSqliteError::Io(e))),
                    None => None,
                }
            }
            #[cfg(feature = "zero-copy-protocol")]
            ConnectionV2::Direct(direct) => direct.next().await,
        }
    }
    
    /// Flush any pending data
    pub async fn flush(&mut self) -> Result<(), PgSqliteError> {
        match self {
            ConnectionV2::Framed(framed) => {
                futures::SinkExt::flush(framed).await?;
                Ok(())
            }
            #[cfg(feature = "zero-copy-protocol")]
            ConnectionV2::Direct(direct) => direct.flush().await,
        }
    }
}

/// Helper extension for FramedWriter
impl FramedWriter {
    /// Create a FramedWriter from a mutable reference to Framed
    pub fn new_ref<T>(framed: &mut Framed<T, PostgresCodec>) -> FramedWriterRef<'_, T> 
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send
    {
        FramedWriterRef { framed }
    }
}

/// A temporary writer that borrows a Framed connection
pub struct FramedWriterRef<'a, T> {
    framed: &'a mut Framed<T, PostgresCodec>,
}

#[async_trait::async_trait]
impl<'a, T> ProtocolWriter for FramedWriterRef<'a, T>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send
{
    async fn send_auth_ok(&mut self) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        self.framed.send(BackendMessage::Authentication(crate::protocol::AuthenticationMessage::Ok)).await?;
        Ok(())
    }
    
    async fn send_parameter_status(&mut self, param: &str, value: &str) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        self.framed.send(BackendMessage::ParameterStatus {
            name: param.to_string(),
            value: value.to_string(),
        }).await?;
        Ok(())
    }
    
    async fn send_backend_key_data(&mut self, process_id: i32, secret_key: i32) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        self.framed.send(BackendMessage::BackendKeyData { process_id, secret_key }).await?;
        Ok(())
    }
    
    async fn send_ready_for_query(&mut self, status: TransactionStatus) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        self.framed.send(BackendMessage::ReadyForQuery { status }).await?;
        Ok(())
    }
    
    async fn send_row_description(&mut self, fields: &[crate::protocol::FieldDescription]) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        self.framed.send(BackendMessage::RowDescription(fields.to_vec())).await?;
        Ok(())
    }
    
    async fn send_data_row(&mut self, values: &[Option<Vec<u8>>]) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        self.framed.send(BackendMessage::DataRow(values.to_vec())).await?;
        Ok(())
    }
    
    async fn send_data_row_raw(&mut self, values: &[Option<&[u8]>]) -> Result<(), PgSqliteError> {
        let values: Vec<Option<Vec<u8>>> = values.iter()
            .map(|opt| opt.map(|v| v.to_vec()))
            .collect();
        self.send_data_row(&values).await
    }
    
    async fn send_command_complete(&mut self, tag: &str) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        self.framed.send(BackendMessage::CommandComplete { tag: tag.to_string() }).await?;
        Ok(())
    }
    
    async fn send_error(&mut self, error: &PgSqliteError) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        let err_response = crate::protocol::ErrorResponse::new(
            "ERROR".to_string(),
            error.pg_error_code().to_string(),
            error.to_string()
        );
        self.framed.send(BackendMessage::ErrorResponse(err_response)).await?;
        Ok(())
    }
    
    async fn send_parse_complete(&mut self) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        self.framed.send(BackendMessage::ParseComplete).await?;
        Ok(())
    }
    
    async fn send_bind_complete(&mut self) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        self.framed.send(BackendMessage::BindComplete).await?;
        Ok(())
    }
    
    async fn send_close_complete(&mut self) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        self.framed.send(BackendMessage::CloseComplete).await?;
        Ok(())
    }
    
    async fn send_no_data(&mut self) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        self.framed.send(BackendMessage::NoData).await?;
        Ok(())
    }
    
    async fn send_portal_suspended(&mut self) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        self.framed.send(BackendMessage::PortalSuspended).await?;
        Ok(())
    }
    
    async fn flush(&mut self) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        self.framed.flush().await?;
        Ok(())
    }
}

/// Make ConnectionV2 work as a Stream for compatibility
impl Stream for ConnectionV2 {
    type Item = Result<FrontendMessage, PgSqliteError>;
    
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut *self {
            ConnectionV2::Framed(framed) => {
                Pin::new(framed).poll_next(cx)
                    .map(|opt| opt.map(|res| res.map_err(PgSqliteError::Io)))
            }
            #[cfg(feature = "zero-copy-protocol")]
            ConnectionV2::Direct(direct) => {
                Pin::new(direct).poll_next(cx)
            }
        }
    }
}

/// Helper trait for easy migration
pub trait ConnectionExt {
    async fn send_auth_ok(&mut self) -> Result<(), PgSqliteError>;
    async fn send_parameter_status(&mut self, name: &str, value: &str) -> Result<(), PgSqliteError>;
    async fn send_backend_key_data(&mut self, process_id: i32, secret_key: i32) -> Result<(), PgSqliteError>;
    async fn send_ready_for_query(&mut self, status: TransactionStatus) -> Result<(), PgSqliteError>;
}

impl ConnectionExt for ConnectionV2 {
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