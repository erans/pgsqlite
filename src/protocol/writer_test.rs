#[cfg(test)]
mod tests {
    use super::super::*;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_util::codec::Framed;
    use crate::protocol::{TransactionStatus, FieldDescription};
    
    async fn create_test_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        
        let client_future = TcpStream::connect(addr);
        let server_future = async {
            listener.accept().await.unwrap().0
        };
        
        let (client, server) = tokio::join!(client_future, server_future);
        (client.unwrap(), server)
    }
    
    #[tokio::test]
    async fn test_framed_writer_basic_messages() {
        let (_client, server) = create_test_pair().await;
        
        // Create writer
        let framed = Framed::new(server, PostgresCodec::new());
        let mut writer = FramedWriter::new(framed);
        
        // Test sending auth OK
        writer.send_auth_ok().await.unwrap();
        
        // Test sending parameter status
        writer.send_parameter_status("server_version", "pgsqlite-0.1").await.unwrap();
        
        // Test sending ready for query
        writer.send_ready_for_query(TransactionStatus::Idle).await.unwrap();
        
        writer.flush().await.unwrap();
    }
    
    #[tokio::test]
    async fn test_direct_writer_basic_messages() {
        let (mut client, server) = create_test_pair().await;
        
        // Create writer
        let mut writer = DirectWriter::new(server);
        
        // Test sending auth OK
        writer.send_auth_ok().await.unwrap();
        
        // Test sending parameter status
        writer.send_parameter_status("server_version", "pgsqlite-0.1").await.unwrap();
        
        // Test sending ready for query
        writer.send_ready_for_query(TransactionStatus::Idle).await.unwrap();
        
        writer.flush().await.unwrap();
        
        // Verify we can read the messages on the client side
        use tokio::io::AsyncReadExt;
        let mut buf = vec![0u8; 1024];
        let n = client.read(&mut buf).await.unwrap();
        assert!(n > 0);
        
        // Check auth OK message
        assert_eq!(buf[0], b'R'); // Auth message
        let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
        assert_eq!(len, 8);
        
        // Check parameter status
        let offset = 9;
        assert_eq!(buf[offset], b'S'); // Parameter status
    }
    
    #[tokio::test]
    async fn test_data_row_zero_copy() {
        let (_client, server) = create_test_pair().await;
        
        let mut writer = DirectWriter::new(server);
        
        // Send row description
        let fields = vec![
            FieldDescription {
                name: "id".to_string(),
                table_oid: 0,
                column_id: 1,
                type_oid: 23, // INT4
                type_size: 4,
                type_modifier: -1,
                format: 0,
            },
            FieldDescription {
                name: "name".to_string(),
                table_oid: 0,
                column_id: 2,
                type_oid: 25, // TEXT
                type_size: -1,
                type_modifier: -1,
                format: 0,
            },
        ];
        
        writer.send_row_description(&fields).await.unwrap();
        
        // Send data rows using zero-copy
        let row1: Vec<Option<&[u8]>> = vec![
            Some(b"1"),
            Some(b"Alice"),
        ];
        writer.send_data_row_raw(&row1).await.unwrap();
        
        let row2: Vec<Option<&[u8]>> = vec![
            Some(b"2"),
            None, // NULL value
        ];
        writer.send_data_row_raw(&row2).await.unwrap();
        
        writer.send_command_complete("SELECT 2").await.unwrap();
        writer.flush().await.unwrap();
    }
    
    #[tokio::test]
    async fn test_protocol_writer_trait() {
        async fn send_query_result<W: ProtocolWriter>(writer: &mut W) -> Result<(), crate::PgSqliteError> {
            // This demonstrates using the trait generically
            let fields = vec![
                FieldDescription {
                    name: "result".to_string(),
                    table_oid: 0,
                    column_id: 1,
                    type_oid: 23,
                    type_size: 4,
                    type_modifier: -1,
                    format: 0,
                },
            ];
            
            writer.send_row_description(&fields).await?;
            
            let row: Vec<Option<Vec<u8>>> = vec![Some(b"42".to_vec())];
            writer.send_data_row(&row).await?;
            
            writer.send_command_complete("SELECT 1").await?;
            writer.send_ready_for_query(TransactionStatus::Idle).await?;
            
            Ok(())
        }
        
        // Test with both implementations
        let (client1, server1) = create_test_pair().await;
        let framed = Framed::new(server1, PostgresCodec::new());
        let mut framed_writer = FramedWriter::new(framed);
        send_query_result(&mut framed_writer).await.unwrap();
        drop(client1);
        
        let (_client2, server2) = create_test_pair().await;
        let mut direct_writer = DirectWriter::new(server2);
        send_query_result(&mut direct_writer).await.unwrap();
    }
}