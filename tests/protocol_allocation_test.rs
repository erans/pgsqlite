#[cfg(test)]
mod tests {
    use pgsqlite::protocol::{ProtocolWriter, FramedWriter, DirectWriter, PostgresCodec, FieldDescription, TransactionStatus};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_util::codec::Framed;
    
    // Note: This would need to be set as global allocator in a separate test binary
    // For now, this is a demonstration of how we would measure allocations
    
    async fn create_socket_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        
        let client_future = TcpStream::connect(addr);
        let server_future = async {
            listener.accept().await.unwrap().0
        };
        
        let (client, server) = tokio::join!(client_future, server_future);
        (client.unwrap(), server)
    }
    
    fn create_test_fields() -> Vec<FieldDescription> {
        vec![
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
            FieldDescription {
                name: "email".to_string(),
                table_oid: 0,
                column_id: 3,
                type_oid: 25, // TEXT
                type_size: -1,
                type_modifier: -1,
                format: 0,
            },
        ]
    }
    
    async fn send_query_result<W: ProtocolWriter>(writer: &mut W, rows: usize) {
        let fields = create_test_fields();
        writer.send_row_description(&fields).await.unwrap();
        
        for i in 0..rows {
            let row = vec![
                Some(format!("{}", i).into_bytes()),
                Some(format!("User {}", i).into_bytes()),
                Some(format!("user{}@example.com", i).into_bytes()),
            ];
            writer.send_data_row(&row).await.unwrap();
        }
        
        writer.send_command_complete(&format!("SELECT {}", rows)).await.unwrap();
        writer.send_ready_for_query(TransactionStatus::Idle).await.unwrap();
    }
    
    #[tokio::test]
    async fn test_allocation_comparison() {
        // This test demonstrates how we would compare allocations
        // In practice, this would need a custom test harness with global allocator
        
        println!("\n=== Protocol Writer Allocation Comparison ===\n");
        
        // Test with Framed writer
        {
            let (_client, server) = create_socket_pair().await;
            let framed = Framed::new(server, PostgresCodec::new());
            let mut writer = FramedWriter::new(framed);
            
            // In real test with tracking allocator:
            // let before = TrackingAllocator::stats();
            // send_query_result(&mut writer, 100).await;
            // let after = TrackingAllocator::stats();
            // let diff = after.diff(&before);
            // println!("Framed writer allocations: {}", diff);
            
            // For now, just run the operation
            send_query_result(&mut writer, 10).await;
            println!("Framed writer: Complete (allocation tracking would show here)");
        }
        
        // Test with Direct writer
        {
            let (_client, server) = create_socket_pair().await;
            let mut writer = DirectWriter::new(server);
            
            // In real test with tracking allocator:
            // let before = TrackingAllocator::stats();
            // send_query_result(&mut writer, 100).await;
            // let after = TrackingAllocator::stats();
            // let diff = after.diff(&before);
            // println!("Direct writer allocations: {}", diff);
            
            // For now, just run the operation
            send_query_result(&mut writer, 10).await;
            println!("Direct writer: Complete (allocation tracking would show here)");
        }
        
        println!("\nNote: To see actual allocation counts, this test would need");
        println!("to be run in a separate binary with a global allocator set.");
    }
    
    #[tokio::test]
    async fn test_zero_copy_data_row() {
        let (_client, server) = create_socket_pair().await;
        let mut writer = DirectWriter::new(server);
        
        // Pre-encoded data that could come directly from SQLite
        let pre_encoded_values: Vec<&[u8]> = vec![
            b"123",
            b"John Doe",
            b"john@example.com",
        ];
        
        let values: Vec<Option<&[u8]>> = pre_encoded_values.iter()
            .map(|v| Some(*v))
            .collect();
        
        // This should perform zero allocations for the actual data
        writer.send_data_row_raw(&values).await.unwrap();
        
        println!("Zero-copy data row sent successfully");
    }
}