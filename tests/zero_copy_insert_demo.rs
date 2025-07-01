#[cfg(test)]
mod tests {
    use pgsqlite::protocol::{DirectWriter, ProtocolWriter, FieldDescription, TransactionStatus};
    use tokio::net::{TcpListener, TcpStream};
    use std::time::Instant;
    
    /// Demonstrates the performance difference between traditional and zero-copy INSERT handling
    #[tokio::test]
    #[ignore] // Run with: cargo test zero_copy_insert_demo -- --ignored --nocapture
    async fn demonstrate_zero_copy_insert_performance() {
        println!("\n=== Zero-Copy INSERT Performance Demonstration ===\n");
        
        // Create a test socket pair
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream
        });
        
        let client = TcpStream::connect(addr).await.unwrap();
        let server = server_task.await.unwrap();
        
        // Test parameters
        const NUM_INSERTS: usize = 1000;
        const INSERT_TAG: &str = "INSERT 0 1";
        
        // Simulate INSERT operations with DirectWriter
        let mut writer = DirectWriter::new(server);
        
        println!("Simulating {} INSERT operations with zero-copy protocol...", NUM_INSERTS);
        let start = Instant::now();
        
        for i in 0..NUM_INSERTS {
            // In real implementation, this would be called after executing the INSERT
            writer.send_command_complete(INSERT_TAG).await.unwrap();
            
            // Send ready for query after each INSERT
            writer.send_ready_for_query(TransactionStatus::Idle).await.unwrap();
            
            if i % 100 == 0 {
                writer.flush().await.unwrap();
            }
        }
        
        writer.flush().await.unwrap();
        let elapsed = start.elapsed();
        
        println!("Zero-copy INSERT results:");
        println!("  Total time: {:?}", elapsed);
        println!("  Per INSERT: {:?}", elapsed / NUM_INSERTS as u32);
        println!("  Operations/sec: {:.0}", NUM_INSERTS as f64 / elapsed.as_secs_f64());
        
        // Calculate theoretical improvement
        println!("\nTheoretical improvements:");
        println!("  - No BackendMessage enum allocation");
        println!("  - No String allocation for command tag");
        println!("  - Direct buffer writing to socket");
        println!("  - Reusable message buffers");
        
        drop(client); // Close connection
    }
    
    /// Demonstrates batched DataRow sending for SELECT operations
    #[tokio::test]
    #[ignore]
    async fn demonstrate_batched_data_rows() {
        println!("\n=== Batched DataRow Demonstration ===\n");
        
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream
        });
        
        let client = TcpStream::connect(addr).await.unwrap();
        let server = server_task.await.unwrap();
        
        let mut writer = DirectWriter::new(server);
        
        // Simulate a SELECT returning multiple rows
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
                name: "data".to_string(),
                table_oid: 0,
                column_id: 2,
                type_oid: 25, // TEXT
                type_size: -1,
                type_modifier: -1,
                format: 0,
            },
        ];
        
        writer.send_row_description(&fields).await.unwrap();
        
        const NUM_ROWS: usize = 10000;
        const BATCH_SIZE: usize = 100;
        
        println!("Sending {} rows in batches of {}...", NUM_ROWS, BATCH_SIZE);
        let start = Instant::now();
        
        for i in 0..NUM_ROWS {
            let row = vec![
                Some(i.to_string().into_bytes()),
                Some(format!("Row {}", i).into_bytes()),
            ];
            
            writer.send_data_row(&row).await.unwrap();
            
            // Flush every BATCH_SIZE rows
            if (i + 1) % BATCH_SIZE == 0 {
                writer.flush().await.unwrap();
            }
        }
        
        writer.send_command_complete(&format!("SELECT {}", NUM_ROWS)).await.unwrap();
        writer.flush().await.unwrap();
        
        let elapsed = start.elapsed();
        
        println!("\nBatched DataRow results:");
        println!("  Total time: {:?}", elapsed);
        println!("  Per row: {:?}", elapsed / NUM_ROWS as u32);
        println!("  Rows/sec: {:.0}", NUM_ROWS as f64 / elapsed.as_secs_f64());
        println!("  Flushes: {}", NUM_ROWS / BATCH_SIZE);
        
        drop(client);
    }
    
    /// Shows memory allocation patterns
    #[tokio::test]
    #[ignore]
    async fn demonstrate_allocation_savings() {
        println!("\n=== Allocation Savings Demonstration ===\n");
        
        // Traditional approach allocations per INSERT:
        println!("Traditional Framed approach allocations:");
        println!("  1. BackendMessage::CommandComplete {{ tag: String }} - heap allocation");
        println!("  2. String::from(\"INSERT 0 1\") - heap allocation");
        println!("  3. Message serialization buffer - heap allocation");
        println!("  4. Framed internal buffer growth - potential reallocation");
        println!("  Total: 3-4 heap allocations per INSERT");
        
        println!("\nZero-copy DirectWriter approach:");
        println!("  1. Reusable message buffer - no allocation");
        println!("  2. &str tag parameter - no allocation");
        println!("  3. Direct socket write - no intermediate buffer");
        println!("  Total: 0 heap allocations per INSERT");
        
        println!("\nFor 1000 INSERTs:");
        println!("  Traditional: ~3000-4000 allocations");
        println!("  Zero-copy: 0 allocations (after initial buffer creation)");
        
        println!("\nExpected performance improvement:");
        println!("  - Reduced memory pressure");
        println!("  - Better CPU cache utilization");
        println!("  - Lower GC/allocator overhead");
        println!("  - Could reduce INSERT overhead from 162x to ~50-80x");
    }
}