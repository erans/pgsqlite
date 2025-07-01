use tokio::net::TcpStream;
use tokio::io::ReadHalf;
use tokio_util::codec::FramedRead;
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::protocol::{PostgresCodec, FrontendMessage, ProtocolWriter, DirectWriter};
use crate::PgSqliteError;

/// A connection that uses DirectWriter for zero-copy sends
/// and FramedRead for receiving messages
pub struct DirectConnection {
    reader: FramedRead<ReadHalf<TcpStream>, PostgresCodec>,
    writer: DirectWriter,
}

impl DirectConnection {
    /// Create a new DirectConnection by splitting the TcpStream
    pub fn new(stream: TcpStream) -> Self {
        let (read_half, write_half) = tokio::io::split(stream);
        
        // Create reader with codec for receiving messages
        let codec = PostgresCodec::new();
        let reader = FramedRead::new(read_half, codec);
        
        // Create DirectWriter for zero-copy sending
        let writer = DirectWriter::new_from_writer(write_half);
        
        Self { reader, writer }
    }
    
    /// Get a mutable reference to the writer
    pub fn writer(&mut self) -> &mut DirectWriter {
        &mut self.writer
    }
    
    /// Receive the next message
    pub async fn next(&mut self) -> Option<Result<FrontendMessage, PgSqliteError>> {
        match self.reader.next().await {
            Some(Ok(msg)) => Some(Ok(msg)),
            Some(Err(e)) => Some(Err(PgSqliteError::Io(e))),
            None => None,
        }
    }
    
    /// Flush any pending data
    pub async fn flush(&mut self) -> Result<(), PgSqliteError> {
        self.writer.flush().await
    }
}


/// Make DirectConnection work as a Stream for compatibility
impl Stream for DirectConnection {
    type Item = Result<FrontendMessage, PgSqliteError>;
    
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.reader).poll_next(cx)
            .map(|opt| opt.map(|res| res.map_err(PgSqliteError::Io)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::BackendMessage;
    use tokio::net::TcpListener;
    
    #[tokio::test]
    async fn test_direct_connection_basic() {
        // Create a test socket pair
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream
        });
        
        let client = TcpStream::connect(addr).await.unwrap();
        let server = server_task.await.unwrap();
        
        // Create DirectConnection
        let mut conn = DirectConnection::new(server);
        
        // Test sending a message
        conn.writer().send_auth_ok().await.unwrap();
        conn.writer().flush().await.unwrap();
        
        // Verify we can still use the writer
        conn.writer().send_ready_for_query(crate::protocol::TransactionStatus::Idle).await.unwrap();
        conn.writer().flush().await.unwrap();
        
        drop(client); // Close connection
    }
    
    #[tokio::test]
    async fn test_direct_connection_split_operation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        
        // Client task that sends a message
        let client_task = tokio::spawn(async move {
            let client = TcpStream::connect(addr).await.unwrap();
            let codec = PostgresCodec::new();
            let mut framed = tokio_util::codec::Framed::new(client, codec);
            
            // Send a simple query
            framed.send(FrontendMessage::Query("SELECT 1".to_string())).await.unwrap();
            framed.flush().await.unwrap();
        });
        
        let (stream, _) = listener.accept().await.unwrap();
        let mut conn = DirectConnection::new(stream);
        
        // Receive the message
        let msg = conn.next().await.unwrap().unwrap();
        match msg {
            FrontendMessage::Query(sql) => assert_eq!(sql, "SELECT 1"),
            _ => panic!("Expected Query message"),
        }
        
        // Send response using zero-copy writer
        conn.writer().send_row_description(&[]).await.unwrap();
        conn.writer().send_command_complete("SELECT 1").await.unwrap();
        conn.writer().flush().await.unwrap();
        
        client_task.await.unwrap();
    }
}