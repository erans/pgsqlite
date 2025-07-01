use bytes::{BytesMut, BufMut};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::protocol::{BackendMessage, PostgresCodec, FieldDescription, TransactionStatus, AuthenticationMessage, ErrorResponse};
use crate::PgSqliteError;

/// Trait for writing PostgreSQL protocol messages
/// 
/// This trait abstracts over different implementations:
/// - FramedWriter: Uses the existing Framed codec (current implementation)
/// - DirectWriter: Writes directly to socket for zero-copy operation
#[async_trait::async_trait]
pub trait ProtocolWriter: Send {
    /// Send an authentication OK message
    async fn send_auth_ok(&mut self) -> Result<(), PgSqliteError>;
    
    /// Send parameter status messages
    async fn send_parameter_status(&mut self, param: &str, value: &str) -> Result<(), PgSqliteError>;
    
    /// Send backend key data
    async fn send_backend_key_data(&mut self, process_id: i32, secret_key: i32) -> Result<(), PgSqliteError>;
    
    /// Send ready for query message
    async fn send_ready_for_query(&mut self, status: TransactionStatus) -> Result<(), PgSqliteError>;
    
    /// Send row description
    async fn send_row_description(&mut self, fields: &[FieldDescription]) -> Result<(), PgSqliteError>;
    
    /// Send a data row
    async fn send_data_row(&mut self, values: &[Option<Vec<u8>>]) -> Result<(), PgSqliteError>;
    
    /// Send a data row with pre-encoded values (for zero-copy)
    async fn send_data_row_raw(&mut self, values: &[Option<&[u8]>]) -> Result<(), PgSqliteError>;
    
    /// Send command complete
    async fn send_command_complete(&mut self, tag: &str) -> Result<(), PgSqliteError>;
    
    /// Send error response
    async fn send_error(&mut self, error: &PgSqliteError) -> Result<(), PgSqliteError>;
    
    /// Send parse complete
    async fn send_parse_complete(&mut self) -> Result<(), PgSqliteError>;
    
    /// Send bind complete
    async fn send_bind_complete(&mut self) -> Result<(), PgSqliteError>;
    
    /// Send close complete
    async fn send_close_complete(&mut self) -> Result<(), PgSqliteError>;
    
    /// Send no data
    async fn send_no_data(&mut self) -> Result<(), PgSqliteError>;
    
    /// Send portal suspended
    async fn send_portal_suspended(&mut self) -> Result<(), PgSqliteError>;
    
    /// Flush any buffered data
    async fn flush(&mut self) -> Result<(), PgSqliteError>;
}

/// Implementation using the existing Framed codec
pub struct FramedWriter {
    framed: Framed<TcpStream, PostgresCodec>,
}

impl FramedWriter {
    pub fn new(framed: Framed<TcpStream, PostgresCodec>) -> Self {
        Self { framed }
    }
    
    /// Get a mutable reference to the framed stream
    pub fn framed_mut(&mut self) -> &mut Framed<TcpStream, PostgresCodec> {
        &mut self.framed
    }
    
    /// Take ownership of the framed stream
    pub fn into_framed(self) -> Framed<TcpStream, PostgresCodec> {
        self.framed
    }
}

#[async_trait::async_trait]
impl ProtocolWriter for FramedWriter {
    async fn send_auth_ok(&mut self) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        self.framed.send(BackendMessage::Authentication(AuthenticationMessage::Ok)).await?;
        Ok(())
    }
    
    async fn send_parameter_status(&mut self, param: &str, value: &str) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        self.framed.send(BackendMessage::ParameterStatus { 
            name: param.to_string(), 
            value: value.to_string() 
        }).await?;
        Ok(())
    }
    
    async fn send_backend_key_data(&mut self, process_id: i32, secret_key: i32) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        self.framed.send(BackendMessage::BackendKeyData { 
            process_id, 
            secret_key 
        }).await?;
        Ok(())
    }
    
    async fn send_ready_for_query(&mut self, status: TransactionStatus) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        self.framed.send(BackendMessage::ReadyForQuery { status }).await?;
        Ok(())
    }
    
    async fn send_row_description(&mut self, fields: &[FieldDescription]) -> Result<(), PgSqliteError> {
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
        // For framed writer, we need to convert to owned data
        let owned_values: Vec<Option<Vec<u8>>> = values.iter()
            .map(|v| v.map(|bytes| bytes.to_vec()))
            .collect();
        self.send_data_row(&owned_values).await
    }
    
    async fn send_command_complete(&mut self, tag: &str) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        self.framed.send(BackendMessage::CommandComplete { 
            tag: tag.to_string() 
        }).await?;
        Ok(())
    }
    
    async fn send_error(&mut self, error: &PgSqliteError) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        let err_response = ErrorResponse::new(
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

/// Direct socket writer for zero-copy operation
pub struct DirectWriter {
    socket: Box<dyn AsyncWrite + Unpin + Send>,
    write_buffer: BytesMut,
    /// Pre-allocated buffer for building messages
    message_buffer: crate::protocol::zero_copy::ZeroCopyMessageBuilder,
}

impl DirectWriter {
    pub fn new(socket: TcpStream) -> Self {
        Self {
            socket: Box::new(socket),
            write_buffer: BytesMut::with_capacity(8192),
            message_buffer: crate::protocol::zero_copy::ZeroCopyMessageBuilder::with_capacity(4096),
        }
    }
    
    /// Create a DirectWriter from any AsyncWrite
    pub fn new_from_writer<W: AsyncWrite + Unpin + Send + 'static>(writer: W) -> Self {
        Self {
            socket: Box::new(writer),
            write_buffer: BytesMut::with_capacity(8192),
            message_buffer: crate::protocol::zero_copy::ZeroCopyMessageBuilder::with_capacity(4096),
        }
    }
    
    /// Write a message directly to the socket
    async fn write_message(&mut self, message_bytes: &[u8]) -> Result<(), PgSqliteError> {
        self.socket.write_all(message_bytes).await?;
        Ok(())
    }
    
    /// Encode authentication message directly
    fn encode_auth_ok(&mut self) {
        self.write_buffer.clear();
        self.write_buffer.extend_from_slice(b"R");
        self.write_buffer.extend_from_slice(&8i32.to_be_bytes()); // Length
        self.write_buffer.extend_from_slice(&0i32.to_be_bytes()); // Auth OK
    }
    
    /// Encode parameter status directly
    fn encode_parameter_status(&mut self, param: &str, value: &str) {
        self.write_buffer.clear();
        self.write_buffer.extend_from_slice(b"S");
        
        // Calculate length: 4 (length field) + param + 1 (null) + value + 1 (null)
        let len = 4 + param.len() + 1 + value.len() + 1;
        self.write_buffer.extend_from_slice(&(len as i32).to_be_bytes());
        
        self.write_buffer.extend_from_slice(param.as_bytes());
        self.write_buffer.put_u8(0);
        self.write_buffer.extend_from_slice(value.as_bytes());
        self.write_buffer.put_u8(0);
    }
    
    /// Encode backend key data directly
    fn encode_backend_key_data(&mut self, process_id: i32, secret_key: i32) {
        self.write_buffer.clear();
        self.write_buffer.extend_from_slice(b"K");
        self.write_buffer.extend_from_slice(&12i32.to_be_bytes()); // Length
        self.write_buffer.extend_from_slice(&process_id.to_be_bytes());
        self.write_buffer.extend_from_slice(&secret_key.to_be_bytes());
    }
}

#[async_trait::async_trait]
impl ProtocolWriter for DirectWriter {
    async fn send_auth_ok(&mut self) -> Result<(), PgSqliteError> {
        self.encode_auth_ok();
        self.socket.write_all(&self.write_buffer).await?;
        Ok(())
    }
    
    async fn send_parameter_status(&mut self, param: &str, value: &str) -> Result<(), PgSqliteError> {
        self.encode_parameter_status(param, value);
        self.socket.write_all(&self.write_buffer).await?;
        Ok(())
    }
    
    async fn send_backend_key_data(&mut self, process_id: i32, secret_key: i32) -> Result<(), PgSqliteError> {
        self.encode_backend_key_data(process_id, secret_key);
        self.socket.write_all(&self.write_buffer).await?;
        Ok(())
    }
    
    async fn send_ready_for_query(&mut self, status: TransactionStatus) -> Result<(), PgSqliteError> {
        self.message_buffer.clear();
        self.message_buffer.build_ready_for_query(status);
        self.socket.write_all(self.message_buffer.as_bytes()).await?;
        Ok(())
    }
    
    async fn send_row_description(&mut self, fields: &[FieldDescription]) -> Result<(), PgSqliteError> {
        self.message_buffer.clear();
        self.message_buffer.build_row_description(fields);
        self.socket.write_all(self.message_buffer.as_bytes()).await?;
        Ok(())
    }
    
    async fn send_data_row(&mut self, values: &[Option<Vec<u8>>]) -> Result<(), PgSqliteError> {
        // Convert to borrowed slices for zero-copy builder
        let borrowed_values: Vec<Option<&[u8]>> = values.iter()
            .map(|v| v.as_ref().map(|vec| vec.as_slice()))
            .collect();
        self.send_data_row_raw(&borrowed_values).await
    }
    
    async fn send_data_row_raw(&mut self, values: &[Option<&[u8]>]) -> Result<(), PgSqliteError> {
        self.message_buffer.clear();
        self.message_buffer.build_data_row(values);
        self.socket.write_all(self.message_buffer.as_bytes()).await?;
        Ok(())
    }
    
    async fn send_command_complete(&mut self, tag: &str) -> Result<(), PgSqliteError> {
        self.message_buffer.clear();
        self.message_buffer.build_command_complete(tag);
        self.socket.write_all(self.message_buffer.as_bytes()).await?;
        Ok(())
    }
    
    async fn send_error(&mut self, error: &PgSqliteError) -> Result<(), PgSqliteError> {
        // For now, encode error messages directly
        self.write_buffer.clear();
        self.write_buffer.extend_from_slice(b"E");
        
        let len_pos = self.write_buffer.len();
        self.write_buffer.extend_from_slice(&0i32.to_be_bytes()); // Placeholder
        
        // Severity
        self.write_buffer.put_u8(b'S');
        self.write_buffer.extend_from_slice(b"ERROR");
        self.write_buffer.put_u8(0);
        
        // Error code
        self.write_buffer.put_u8(b'C');
        self.write_buffer.extend_from_slice(error.pg_error_code().as_bytes());
        self.write_buffer.put_u8(0);
        
        // Message
        self.write_buffer.put_u8(b'M');
        self.write_buffer.extend_from_slice(error.to_string().as_bytes());
        self.write_buffer.put_u8(0);
        
        // Terminator
        self.write_buffer.put_u8(0);
        
        // Update length
        let total_len = (self.write_buffer.len() - len_pos) as i32;
        self.write_buffer[len_pos..len_pos + 4].copy_from_slice(&total_len.to_be_bytes());
        
        self.socket.write_all(&self.write_buffer).await?;
        Ok(())
    }
    
    async fn send_parse_complete(&mut self) -> Result<(), PgSqliteError> {
        self.write_message(b"1\0\0\0\x04").await
    }
    
    async fn send_bind_complete(&mut self) -> Result<(), PgSqliteError> {
        self.write_message(b"2\0\0\0\x04").await
    }
    
    async fn send_close_complete(&mut self) -> Result<(), PgSqliteError> {
        self.write_message(b"3\0\0\0\x04").await
    }
    
    async fn send_no_data(&mut self) -> Result<(), PgSqliteError> {
        self.write_message(b"n\0\0\0\x04").await
    }
    
    async fn send_portal_suspended(&mut self) -> Result<(), PgSqliteError> {
        self.write_message(b"s\0\0\0\x04").await
    }
    
    async fn flush(&mut self) -> Result<(), PgSqliteError> {
        self.socket.flush().await?;
        Ok(())
    }
}

/// Factory for creating protocol writers based on configuration
pub enum WriterType {
    Framed,
    Direct,
}

impl WriterType {
    /// Create a protocol writer from the current configuration
    #[cfg(feature = "zero-copy-protocol")]
    pub fn from_config() -> Self {
        if std::env::var("PGSQLITE_ZERO_COPY").unwrap_or_default() == "1" {
            WriterType::Direct
        } else {
            WriterType::Framed
        }
    }
    
    #[cfg(not(feature = "zero-copy-protocol"))]
    pub fn from_config() -> Self {
        WriterType::Framed
    }
}

#[cfg(test)]
#[path = "writer_test.rs"]
mod writer_test;