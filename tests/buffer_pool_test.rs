use pgsqlite::protocol::{
    BufferPool, BufferPoolConfig, PooledDirectWriter, BatchConfig, 
    MemoryMonitor, MemoryMonitorConfig, MemoryPressure,
    global_buffer_pool, global_memory_monitor, ProtocolWriter
};
use std::time::Duration;

#[tokio::test]
async fn test_buffer_pool_basic_functionality() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pgsqlite=debug")
        .try_init();

    println!("Testing basic buffer pool functionality...");
    
    let config = BufferPoolConfig {
        max_pool_size: 10,
        initial_buffer_capacity: 1024,
        max_buffer_capacity: 8192,
        enable_monitoring: true,
        ..Default::default()
    };
    
    let pool = BufferPool::with_config(config);
    
    // Test buffer allocation and reuse
    {
        let mut buffer1 = pool.get_buffer();
        buffer1.buffer_mut().extend_from_slice(b"test data 1");
        println!("Buffer 1 length: {} bytes", buffer1.len());
        assert_eq!(buffer1.len(), 11);
        
        let mut buffer2 = pool.get_buffer();
        buffer2.buffer_mut().extend_from_slice(b"test data 2");
        println!("Buffer 2 length: {} bytes", buffer2.len());
        assert_eq!(buffer2.len(), 11);
    } // Buffers should be returned to pool here
    
    let stats = pool.get_stats();
    println!("Pool stats after allocation:");
    println!("  Buffers allocated: {}", stats.buffers_allocated);
    println!("  Buffers returned: {}", stats.buffers_returned);
    println!("  Current pool size: {}", stats.current_pool_size);
    println!("  Reuse rate: {:.1}%", stats.reuse_rate());
    
    assert_eq!(stats.buffers_allocated, 2);
    assert_eq!(stats.buffers_returned, 2);
    assert_eq!(stats.current_pool_size, 2);
    
    // Test buffer reuse
    {
        let mut buffer3 = pool.get_buffer();
        assert!(buffer3.is_empty()); // Should be cleared when reused
        buffer3.buffer_mut().extend_from_slice(b"reused buffer");
        println!("Reused buffer length: {} bytes", buffer3.len());
    }
    
    let stats_after_reuse = pool.get_stats();
    println!("Pool stats after reuse:");
    println!("  Buffers reused: {}", stats_after_reuse.buffers_reused);
    println!("  Reuse rate: {:.1}%", stats_after_reuse.reuse_rate());
    
    assert_eq!(stats_after_reuse.buffers_reused, 1);
    assert!(stats_after_reuse.reuse_rate() > 0.0);
}

#[tokio::test]
async fn test_memory_monitor_integration() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pgsqlite=debug")
        .try_init();

    println!("Testing memory monitor integration...");
    
    let config = MemoryMonitorConfig {
        memory_threshold: 8192, // 8KB threshold for testing
        high_memory_threshold: 16384, // 16KB high threshold
        enable_auto_cleanup: true,
        enable_detailed_monitoring: true,
        ..Default::default()
    };
    
    let monitor = MemoryMonitor::with_config(config);
    monitor.reset_stats(); // Reset to avoid interference from other tests
    
    // Register a cleanup callback
    let cleanup_called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cleanup_called_clone = std::sync::Arc::clone(&cleanup_called);
    
    monitor.register_cleanup_callback(move || {
        cleanup_called_clone.store(true, std::sync::atomic::Ordering::Relaxed);
        println!("Cleanup callback executed!");
    });
    
    // Simulate memory allocations
    println!("Simulating memory allocations...");
    
    monitor.record_buffer_allocation(2048); // 2KB
    let stats1 = monitor.get_stats();
    println!("After 2KB allocation: pressure={:?}, total={}B", 
             stats1.pressure_level, stats1.total_bytes());
    assert_eq!(stats1.pressure_level, MemoryPressure::Low);
    
    monitor.record_buffer_allocation(4096); // +4KB = 6KB total
    let stats2 = monitor.get_stats();
    println!("After 6KB total: pressure={:?}, total={}B", 
             stats2.pressure_level, stats2.total_bytes());
    assert_eq!(stats2.pressure_level, MemoryPressure::Low);
    
    monitor.record_buffer_allocation(4096); // +4KB = 10KB total
    let stats3 = monitor.get_stats();
    println!("After 10KB total: pressure={:?}, total={}B", 
             stats3.pressure_level, stats3.total_bytes());
    assert_eq!(stats3.pressure_level, MemoryPressure::Medium);
    
    monitor.record_buffer_allocation(8192); // +8KB = 18KB total
    let stats4 = monitor.get_stats();
    println!("After 18KB total: pressure={:?}, total={}B", 
             stats4.pressure_level, stats4.total_bytes());
    assert_eq!(stats4.pressure_level, MemoryPressure::High);
    
    // Check if cleanup was triggered
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    println!("Cleanup callback called: {}", 
             cleanup_called.load(std::sync::atomic::Ordering::Relaxed));
    
    // Test deallocation
    monitor.record_buffer_deallocation(8192); // -8KB = 10KB total
    let stats5 = monitor.get_stats();
    println!("After deallocation: pressure={:?}, total={}B", 
             stats5.pressure_level, stats5.total_bytes());
    assert_eq!(stats5.pressure_level, MemoryPressure::Medium);
}

#[tokio::test]
async fn test_pooled_direct_writer() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pgsqlite=debug")
        .try_init();

    println!("Testing pooled direct writer...");
    
    // Create a mock socket (in-memory buffer)
    let socket = std::io::Cursor::new(Vec::new());
    
    let pool_config = BufferPoolConfig {
        max_pool_size: 5,
        initial_buffer_capacity: 512,
        enable_monitoring: true,
        ..Default::default()
    };
    
    let batch_config = BatchConfig {
        max_batch_size: 3,
        max_batch_bytes: 2048,
        enable_batching: true,
        ..Default::default()
    };
    
    let mut writer = PooledDirectWriter::with_config(socket, pool_config, batch_config);
    
    // Test message batching
    println!("Testing message batching...");
    
    // Send some data rows (should be batched)
    let test_values = vec![
        vec![Some(b"value1".to_vec()), Some(b"value2".to_vec())],
        vec![Some(b"value3".to_vec()), Some(b"value4".to_vec())],
        vec![Some(b"value5".to_vec()), Some(b"value6".to_vec())],
    ];
    
    for values in &test_values {
        let borrowed_values: Vec<Option<&[u8]>> = values.iter()
            .map(|v| v.as_ref().map(|vec| vec.as_slice()))
            .collect();
        writer.send_data_row_raw(&borrowed_values).await.unwrap();
    }
    
    // Send command complete (should trigger flush)
    writer.send_command_complete("SELECT 3").await.unwrap();
    
    let writer_stats = writer.get_stats();
    println!("Writer stats:");
    println!("  Messages written: {}", writer_stats.messages_written);
    println!("  Messages batched: {}", writer_stats.messages_batched);
    println!("  Batch flushes: {}", writer_stats.batch_flushes);
    println!("  Batch efficiency: {:.1}", writer_stats.batch_efficiency());
    
    assert!(writer_stats.messages_written > 0);
    assert!(writer_stats.batch_flushes > 0);
    
    let pool_stats = writer.get_buffer_pool_stats();
    println!("Buffer pool stats:");
    println!("  Buffers allocated: {}", pool_stats.buffers_allocated);
    println!("  Buffers reused: {}", pool_stats.buffers_reused);
    println!("  Current pool size: {}", pool_stats.current_pool_size);
    println!("  Reuse rate: {:.1}%", pool_stats.reuse_rate());
    
    assert!(pool_stats.buffers_allocated > 0);
}

#[tokio::test]
async fn test_global_buffer_pool_integration() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pgsqlite=debug")
        .try_init();

    println!("Testing global buffer pool integration...");
    
    // Test global buffer pool access
    let pool = global_buffer_pool();
    
    {
        let mut buffer1 = pool.get_buffer();
        buffer1.buffer_mut().extend_from_slice(b"global buffer test");
        println!("Global buffer 1 length: {} bytes", buffer1.len());
        
        let mut buffer2 = pool.get_buffer();
        buffer2.buffer_mut().extend_from_slice(b"another global buffer");
        println!("Global buffer 2 length: {} bytes", buffer2.len());
    } // Buffers returned to global pool
    
    let global_stats = pool.get_stats();
    println!("Global pool stats:");
    println!("  Total allocations: {}", global_stats.buffers_allocated);
    println!("  Total returns: {}", global_stats.buffers_returned);
    println!("  Current pool size: {}", global_stats.current_pool_size);
    
    // Test global memory monitor
    let monitor = global_memory_monitor();
    let memory_stats = monitor.get_stats();
    
    println!("Global memory stats:");
    println!("  Total memory: {} bytes", memory_stats.total_bytes());
    println!("  Buffer pool memory: {} bytes", memory_stats.buffer_pool_bytes);
    println!("  Pressure level: {:?}", memory_stats.pressure_level);
}

#[tokio::test]
async fn test_buffer_pool_size_limits() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pgsqlite=debug")
        .try_init();

    println!("Testing buffer pool size limits...");
    
    let config = BufferPoolConfig {
        max_pool_size: 2, // Very small pool for testing
        initial_buffer_capacity: 512,
        max_buffer_capacity: 1024, // Small capacity limit
        enable_monitoring: true,
        ..Default::default()
    };
    
    let pool = BufferPool::with_config(config);
    
    // Allocate more buffers than pool size
    {
        let _b1 = pool.get_buffer();
        let _b2 = pool.get_buffer();
        let _b3 = pool.get_buffer();
        let _b4 = pool.get_buffer();
    } // All buffers returned, but only 2 should be kept
    
    let stats = pool.get_stats();
    println!("Pool stats after exceeding size limit:");
    println!("  Buffers allocated: {}", stats.buffers_allocated);
    println!("  Buffers returned: {}", stats.buffers_returned);
    println!("  Buffers discarded: {}", stats.buffers_discarded);
    println!("  Current pool size: {}", stats.current_pool_size);
    
    assert_eq!(stats.current_pool_size, 2); // Only 2 should be kept
    assert_eq!(stats.buffers_discarded, 2); // 2 should be discarded
    
    // Test capacity limit
    {
        let mut large_buffer = pool.get_buffer();
        large_buffer.buffer_mut().resize(2048, 0); // Grow beyond capacity limit
    } // Should be discarded when returned
    
    let stats_after_large = pool.get_stats();
    println!("Pool stats after large buffer:");
    println!("  Buffers discarded: {}", stats_after_large.buffers_discarded);
    
    assert!(stats_after_large.buffers_discarded > stats.buffers_discarded);
}

#[test]
fn test_buffer_pool_configuration() {
    // Test environment variable configuration
    unsafe {
        std::env::set_var("PGSQLITE_BUFFER_POOL_SIZE", "25");
        std::env::set_var("PGSQLITE_BUFFER_INITIAL_CAPACITY", "2048");
        std::env::set_var("PGSQLITE_BUFFER_MONITORING", "1");
    }
    
    let config = BufferPoolConfig::from_env();
    assert_eq!(config.max_pool_size, 25);
    assert_eq!(config.initial_buffer_capacity, 2048);
    assert!(config.enable_monitoring);
    
    // Clean up
    unsafe {
        std::env::remove_var("PGSQLITE_BUFFER_POOL_SIZE");
        std::env::remove_var("PGSQLITE_BUFFER_INITIAL_CAPACITY");
        std::env::remove_var("PGSQLITE_BUFFER_MONITORING");
    }
    
    println!("Buffer pool configuration test passed");
}