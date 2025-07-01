use bytes::BufMut;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use std::collections::VecDeque;
use tracing::debug;

use crate::protocol::{
    FieldDescription, TransactionStatus, MappedValue, ProtocolWriter
};
use crate::protocol::buffer_pool::{BufferPool, PooledBytesMut, BufferPoolConfig};
use crate::PgSqliteError;

/// Enhanced DirectWriter that uses buffer pooling for reduced allocations
pub struct PooledDirectWriter {
    socket: Box<dyn AsyncWrite + Unpin + Send>,
    /// Local buffer pool for this writer
    buffer_pool: BufferPool,
    /// Queue of prepared messages ready for batch sending
    message_queue: VecDeque<PooledBytesMut>,
    /// Configuration for batching behavior
    batch_config: BatchConfig,
    /// Statistics for performance monitoring
    stats: WriterStats,
}

/// Configuration for message batching behavior
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum number of messages to batch before flushing
    pub max_batch_size: usize,
    /// Maximum total bytes to buffer before flushing
    pub max_batch_bytes: usize,
    /// Enable automatic batching optimization
    pub enable_batching: bool,
    /// Flush automatically on certain message types
    pub auto_flush_messages: Vec<MessageType>,
}

/// Types of messages for batching control
#[derive(Debug, Clone, PartialEq)]
pub enum MessageType {
    DataRow,
    CommandComplete,
    ReadyForQuery,
    ErrorResponse,
    RowDescription,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 50,
            max_batch_bytes: 32 * 1024, // 32KB
            enable_batching: std::env::var("PGSQLITE_ENABLE_BATCHING").unwrap_or_default() == "1",
            auto_flush_messages: vec![
                MessageType::CommandComplete,
                MessageType::ReadyForQuery,
                MessageType::ErrorResponse,
            ],
        }
    }
}

impl BatchConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();
        
        if let Ok(val) = std::env::var("PGSQLITE_BATCH_SIZE") {
            if let Ok(size) = val.parse::<usize>() {
                config.max_batch_size = size;
            }
        }
        
        if let Ok(val) = std::env::var("PGSQLITE_BATCH_BYTES") {
            if let Ok(bytes) = val.parse::<usize>() {
                config.max_batch_bytes = bytes;
            }
        }
        
        config
    }
}

/// Statistics for writer performance monitoring
#[derive(Debug, Clone, Default)]
pub struct WriterStats {
    /// Total messages written
    pub messages_written: u64,
    /// Total bytes written
    pub bytes_written: u64,
    /// Number of batch flushes
    pub batch_flushes: u64,
    /// Total messages batched
    pub messages_batched: u64,
    /// Number of buffer pool hits
    pub buffer_pool_hits: u64,
    /// Number of buffer allocations
    pub buffer_allocations: u64,
}

impl WriterStats {
    pub fn batch_efficiency(&self) -> f64 {
        if self.batch_flushes == 0 {
            0.0
        } else {
            self.messages_batched as f64 / self.batch_flushes as f64
        }
    }
    
    pub fn buffer_pool_hit_rate(&self) -> f64 {
        let total_requests = self.buffer_pool_hits + self.buffer_allocations;
        if total_requests == 0 {
            0.0
        } else {
            (self.buffer_pool_hits as f64 / total_requests as f64) * 100.0
        }
    }
}

impl PooledDirectWriter {
    /// Create a new pooled writer with default configuration
    pub fn new<W: AsyncWrite + Unpin + Send + 'static>(socket: W) -> Self {
        Self::with_config(socket, BufferPoolConfig::default(), BatchConfig::default())
    }
    
    /// Create a new pooled writer with custom configuration
    pub fn with_config<W: AsyncWrite + Unpin + Send + 'static>(
        socket: W,
        pool_config: BufferPoolConfig,
        batch_config: BatchConfig,
    ) -> Self {
        Self {
            socket: Box::new(socket),
            buffer_pool: BufferPool::with_config(pool_config),
            message_queue: VecDeque::new(),
            batch_config,
            stats: WriterStats::default(),
        }
    }
    
    /// Create from TCP stream with environment-based configuration
    pub fn from_tcp_stream(stream: TcpStream) -> Self {
        let pool_config = BufferPoolConfig::from_env();
        let batch_config = BatchConfig::from_env();
        Self::with_config(stream, pool_config, batch_config)
    }
    
    /// Get a pooled buffer for message construction
    fn get_buffer(&mut self) -> PooledBytesMut {
        let buffer = self.buffer_pool.get_buffer();
        self.stats.buffer_pool_hits += 1;
        buffer
    }
    
    /// Queue a message for batched sending
    fn queue_message(&mut self, message: PooledBytesMut, message_type: MessageType) -> Result<(), PgSqliteError> {
        let _message_size = message.len();
        
        if self.batch_config.enable_batching {
            self.message_queue.push_back(message);
            self.stats.messages_batched += 1;
            
            // Check if we should auto-flush
            let should_flush = self.batch_config.auto_flush_messages.contains(&message_type) ||
                              self.message_queue.len() >= self.batch_config.max_batch_size ||
                              self.get_queued_bytes() >= self.batch_config.max_batch_bytes;
            
            if should_flush {
                return self.flush_queue();
            }
        } else {
            // Send immediately if batching is disabled
            return self.write_buffer_immediately(message);
        }
        
        Ok(())
    }
    
    /// Get total bytes in the message queue
    fn get_queued_bytes(&self) -> usize {
        self.message_queue.iter().map(|buf| buf.len()).sum()
    }
    
    /// Flush all queued messages
    fn flush_queue(&mut self) -> Result<(), PgSqliteError> {
        if self.message_queue.is_empty() {
            return Ok(());
        }
        
        let total_bytes: usize = self.message_queue.iter().map(|buf| buf.len()).sum();
        let message_count = self.message_queue.len();
        
        debug!("Flushing {} messages ({} bytes)", message_count, total_bytes);
        
        // Collect messages first to avoid borrow checker issues
        let messages: Vec<_> = self.message_queue.drain(..).collect();
        
        // Write all queued messages
        for message in messages {
            self.write_buffer_sync(message)?;
        }
        
        self.stats.batch_flushes += 1;
        self.stats.messages_written += message_count as u64;
        self.stats.bytes_written += total_bytes as u64;
        
        Ok(())
    }
    
    /// Write a buffer immediately without queueing
    fn write_buffer_immediately(&mut self, buffer: PooledBytesMut) -> Result<(), PgSqliteError> {
        let bytes_written = buffer.len();
        self.write_buffer_sync(buffer)?;
        
        self.stats.messages_written += 1;
        self.stats.bytes_written += bytes_written as u64;
        
        Ok(())
    }
    
    /// Synchronously write a buffer to the socket
    fn write_buffer_sync(&mut self, buffer: PooledBytesMut) -> Result<(), PgSqliteError> {
        // For now, we need to use the async write in a blocking way
        // In a real implementation, this would be restructured to be fully async
        let data = buffer.buffer().clone();
        
        // This is a simplified sync implementation - in practice, the entire
        // trait would need to be restructured for proper async batching
        futures::executor::block_on(async {
            self.socket.write_all(&data).await
        })?;
        
        Ok(())
    }
    
    /// Build a DataRow message in a pooled buffer
    fn build_data_row(&mut self, values: &[Option<&[u8]>]) -> PooledBytesMut {
        let mut buffer = self.get_buffer();
        let buf = buffer.buffer_mut();
        
        buf.put_u8(b'D'); // Message type
        let len_pos = buf.len();
        buf.put_i32(0); // Placeholder for length
        
        buf.put_i16(values.len() as i16); // Number of columns
        
        for value in values {
            match value {
                Some(data) => {
                    buf.put_i32(data.len() as i32);
                    buf.extend_from_slice(data);
                }
                None => {
                    buf.put_i32(-1); // NULL value
                }
            }
        }
        
        // Update length
        let total_len = (buf.len() - len_pos) as i32;
        buf[len_pos..len_pos + 4].copy_from_slice(&total_len.to_be_bytes());
        
        buffer
    }
    
    /// Build a DataRow message with mapped values
    fn build_data_row_mapped(&mut self, values: &[Option<&MappedValue>]) -> PooledBytesMut {
        let mut buffer = self.get_buffer();
        let buf = buffer.buffer_mut();
        
        buf.put_u8(b'D'); // Message type
        let len_pos = buf.len();
        buf.put_i32(0); // Placeholder for length
        
        buf.put_i16(values.len() as i16); // Number of columns
        
        for value in values {
            match value {
                Some(mapped_value) => {
                    let data = mapped_value.as_slice();
                    buf.put_i32(data.len() as i32);
                    buf.extend_from_slice(data);
                }
                None => {
                    buf.put_i32(-1); // NULL value
                }
            }
        }
        
        // Update length
        let total_len = (buf.len() - len_pos) as i32;
        buf[len_pos..len_pos + 4].copy_from_slice(&total_len.to_be_bytes());
        
        buffer
    }
    
    /// Build a CommandComplete message
    fn build_command_complete(&mut self, tag: &str) -> PooledBytesMut {
        let mut buffer = self.get_buffer();
        let buf = buffer.buffer_mut();
        
        buf.put_u8(b'C'); // Message type
        let len_pos = buf.len();
        buf.put_i32(0); // Placeholder for length
        
        buf.extend_from_slice(tag.as_bytes());
        buf.put_u8(0); // Null terminator
        
        // Update length
        let total_len = (buf.len() - len_pos) as i32;
        buf[len_pos..len_pos + 4].copy_from_slice(&total_len.to_be_bytes());
        
        buffer
    }
    
    /// Build a RowDescription message
    fn build_row_description(&mut self, fields: &[FieldDescription]) -> PooledBytesMut {
        let mut buffer = self.get_buffer();
        let buf = buffer.buffer_mut();
        
        buf.put_u8(b'T'); // Message type
        let len_pos = buf.len();
        buf.put_i32(0); // Placeholder for length
        
        buf.put_i16(fields.len() as i16); // Number of fields
        
        for field in fields {
            buf.extend_from_slice(field.name.as_bytes());
            buf.put_u8(0); // Null terminator
            buf.put_i32(field.table_oid);
            buf.put_i16(field.column_id);
            buf.put_i32(field.type_oid);
            buf.put_i16(field.type_size);
            buf.put_i32(field.type_modifier);
            buf.put_i16(field.format);
        }
        
        // Update length
        let total_len = (buf.len() - len_pos) as i32;
        buf[len_pos..len_pos + 4].copy_from_slice(&total_len.to_be_bytes());
        
        buffer
    }
    
    /// Get performance statistics
    pub fn get_stats(&self) -> WriterStats {
        self.stats.clone()
    }
    
    /// Get buffer pool statistics
    pub fn get_buffer_pool_stats(&self) -> crate::protocol::buffer_pool::BufferPoolStats {
        self.buffer_pool.get_stats()
    }
    
    /// Force cleanup of buffer pool
    pub fn cleanup_buffers(&self) {
        self.buffer_pool.cleanup();
    }
}

#[async_trait::async_trait]
impl ProtocolWriter for PooledDirectWriter {
    async fn send_auth_ok(&mut self) -> Result<(), PgSqliteError> {
        let mut buffer = self.get_buffer();
        buffer.buffer_mut().extend_from_slice(b"R\0\0\0\x08\0\0\0\0");
        self.queue_message(buffer, MessageType::ReadyForQuery)?;
        Ok(())
    }
    
    async fn send_parameter_status(&mut self, param: &str, value: &str) -> Result<(), PgSqliteError> {
        let mut buffer = self.get_buffer();
        let buf = buffer.buffer_mut();
        
        buf.put_u8(b'S'); // Message type
        let len_pos = buf.len();
        buf.put_i32(0); // Placeholder for length
        
        buf.extend_from_slice(param.as_bytes());
        buf.put_u8(0);
        buf.extend_from_slice(value.as_bytes());
        buf.put_u8(0);
        
        // Update length
        let total_len = (buf.len() - len_pos) as i32;
        buf[len_pos..len_pos + 4].copy_from_slice(&total_len.to_be_bytes());
        
        self.queue_message(buffer, MessageType::ReadyForQuery)?;
        Ok(())
    }
    
    async fn send_backend_key_data(&mut self, process_id: i32, secret_key: i32) -> Result<(), PgSqliteError> {
        let mut buffer = self.get_buffer();
        let buf = buffer.buffer_mut();
        
        buf.put_u8(b'K'); // Message type
        buf.put_i32(12); // Length
        buf.put_i32(process_id);
        buf.put_i32(secret_key);
        
        self.queue_message(buffer, MessageType::ReadyForQuery)?;
        Ok(())
    }
    
    async fn send_ready_for_query(&mut self, status: TransactionStatus) -> Result<(), PgSqliteError> {
        let mut buffer = self.get_buffer();
        let buf = buffer.buffer_mut();
        
        buf.put_u8(b'Z'); // Message type
        buf.put_i32(5); // Length
        buf.put_u8(match status {
            TransactionStatus::Idle => b'I',
            TransactionStatus::InTransaction => b'T',
            TransactionStatus::InFailedTransaction => b'E',
        });
        
        self.queue_message(buffer, MessageType::ReadyForQuery)?;
        Ok(())
    }
    
    async fn send_row_description(&mut self, fields: &[FieldDescription]) -> Result<(), PgSqliteError> {
        let buffer = self.build_row_description(fields);
        self.queue_message(buffer, MessageType::RowDescription)?;
        Ok(())
    }
    
    async fn send_data_row(&mut self, values: &[Option<Vec<u8>>]) -> Result<(), PgSqliteError> {
        let borrowed_values: Vec<Option<&[u8]>> = values.iter()
            .map(|v| v.as_ref().map(|vec| vec.as_slice()))
            .collect();
        self.send_data_row_raw(&borrowed_values).await
    }
    
    async fn send_data_row_raw(&mut self, values: &[Option<&[u8]>]) -> Result<(), PgSqliteError> {
        let buffer = self.build_data_row(values);
        self.queue_message(buffer, MessageType::DataRow)?;
        Ok(())
    }
    
    async fn send_data_row_mapped(&mut self, values: &[Option<&MappedValue>]) -> Result<(), PgSqliteError> {
        let buffer = self.build_data_row_mapped(values);
        self.queue_message(buffer, MessageType::DataRow)?;
        Ok(())
    }
    
    async fn send_command_complete(&mut self, tag: &str) -> Result<(), PgSqliteError> {
        let buffer = self.build_command_complete(tag);
        self.queue_message(buffer, MessageType::CommandComplete)?;
        Ok(())
    }
    
    async fn send_error(&mut self, error: &PgSqliteError) -> Result<(), PgSqliteError> {
        let mut buffer = self.get_buffer();
        let buf = buffer.buffer_mut();
        
        buf.put_u8(b'E'); // Message type
        let len_pos = buf.len();
        buf.put_i32(0); // Placeholder for length
        
        // Severity
        buf.put_u8(b'S');
        buf.extend_from_slice(b"ERROR");
        buf.put_u8(0);
        
        // Error code
        buf.put_u8(b'C');
        buf.extend_from_slice(error.pg_error_code().as_bytes());
        buf.put_u8(0);
        
        // Message
        buf.put_u8(b'M');
        buf.extend_from_slice(error.to_string().as_bytes());
        buf.put_u8(0);
        
        // Terminator
        buf.put_u8(0);
        
        // Update length
        let total_len = (buf.len() - len_pos) as i32;
        buf[len_pos..len_pos + 4].copy_from_slice(&total_len.to_be_bytes());
        
        self.queue_message(buffer, MessageType::ErrorResponse)?;
        Ok(())
    }
    
    async fn send_parse_complete(&mut self) -> Result<(), PgSqliteError> {
        let mut buffer = self.get_buffer();
        buffer.buffer_mut().extend_from_slice(b"1\0\0\0\x04");
        self.queue_message(buffer, MessageType::ReadyForQuery)?;
        Ok(())
    }
    
    async fn send_bind_complete(&mut self) -> Result<(), PgSqliteError> {
        let mut buffer = self.get_buffer();
        buffer.buffer_mut().extend_from_slice(b"2\0\0\0\x04");
        self.queue_message(buffer, MessageType::ReadyForQuery)?;
        Ok(())
    }
    
    async fn send_close_complete(&mut self) -> Result<(), PgSqliteError> {
        let mut buffer = self.get_buffer();
        buffer.buffer_mut().extend_from_slice(b"3\0\0\0\x04");
        self.queue_message(buffer, MessageType::ReadyForQuery)?;
        Ok(())
    }
    
    async fn send_no_data(&mut self) -> Result<(), PgSqliteError> {
        let mut buffer = self.get_buffer();
        buffer.buffer_mut().extend_from_slice(b"n\0\0\0\x04");
        self.queue_message(buffer, MessageType::ReadyForQuery)?;
        Ok(())
    }
    
    async fn send_portal_suspended(&mut self) -> Result<(), PgSqliteError> {
        let mut buffer = self.get_buffer();
        buffer.buffer_mut().extend_from_slice(b"s\0\0\0\x04");
        self.queue_message(buffer, MessageType::ReadyForQuery)?;
        Ok(())
    }
    
    async fn flush(&mut self) -> Result<(), PgSqliteError> {
        self.flush_queue()?;
        self.socket.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    
    #[tokio::test]
    async fn test_pooled_writer_creation() {
        let cursor = Cursor::new(Vec::new());
        let writer = PooledDirectWriter::new(cursor);
        
        let stats = writer.get_stats();
        assert_eq!(stats.messages_written, 0);
        assert_eq!(stats.bytes_written, 0);
    }
    
    #[tokio::test]
    async fn test_buffer_pooling() {
        let cursor = Cursor::new(Vec::new());
        let mut writer = PooledDirectWriter::new(cursor);
        
        // Get multiple buffers
        {
            let _buf1 = writer.get_buffer();
            let _buf2 = writer.get_buffer();
        } // Buffers should be returned to pool
        
        let pool_stats = writer.get_buffer_pool_stats();
        assert!(pool_stats.buffers_returned >= 2);
        
        // Reuse a buffer
        {
            let _buf3 = writer.get_buffer();
        }
        
        let pool_stats = writer.get_buffer_pool_stats();
        assert!(pool_stats.buffers_reused >= 1);
    }
    
    #[test]
    fn test_batch_config() {
        let config = BatchConfig::default();
        assert_eq!(config.max_batch_size, 50);
        assert!(config.auto_flush_messages.contains(&MessageType::CommandComplete));
    }
    
    #[test]
    fn test_writer_stats() {
        let stats = WriterStats {
            messages_written: 100,
            batch_flushes: 10,
            messages_batched: 100,
            buffer_pool_hits: 80,
            buffer_allocations: 20,
            ..Default::default()
        };
        
        assert_eq!(stats.batch_efficiency(), 10.0); // 100 messages / 10 flushes
        assert_eq!(stats.buffer_pool_hit_rate(), 80.0); // 80% hit rate
    }
}