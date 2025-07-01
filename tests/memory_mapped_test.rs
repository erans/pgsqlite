mod common;

use common::*;
use pgsqlite::protocol::ValueHandlerConfig;
use pgsqlite::query::MemoryMappedQueryExecutor;

#[tokio::test]
async fn test_memory_mapped_large_blob() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pgsqlite=debug")
        .try_init();

    // Create executor with memory mapping enabled
    let mut config = ValueHandlerConfig::default();
    config.enable_mmap = true;
    config.large_value_threshold = 1024; // 1KB threshold for testing
    config.mmap_config.min_size_for_mmap = 512; // 512 bytes minimum
    
    let executor = MemoryMappedQueryExecutor::with_config(config);
    let stats = executor.get_stats();
    
    println!("Memory mapping enabled: {}", stats.mmap_enabled);
    println!("Large value threshold: {} bytes", stats.mmap_threshold);
    println!("Memory mapping minimum size: {} bytes", stats.mmap_min_size);
    
    // Set up test server with custom executor
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create table with BLOB column
            db.execute(
                "CREATE TABLE large_data (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    large_blob BLOB
                )"
            ).await?;
            
            // Insert a large blob (2KB of data)
            let large_data = vec![0xAB; 2048]; // 2KB of 0xAB bytes
            let hex_data = hex::encode(&large_data);
            
            db.execute(&format!(
                "INSERT INTO large_data (id, name, large_blob) VALUES (1, 'large_test', X'{}')",
                hex_data
            )).await?;
            
            // Insert a small blob for comparison
            let small_data = vec![0xCD; 256]; // 256 bytes
            let hex_small = hex::encode(&small_data);
            
            db.execute(&format!(
                "INSERT INTO large_data (id, name, large_blob) VALUES (2, 'small_test', X'{}')",
                hex_small
            )).await?;
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    println!("Testing memory-mapped BLOB retrieval...");
    
    // Query for the large blob
    let rows = client.query("SELECT id, name, large_blob FROM large_data WHERE id = 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    
    let row = &rows[0];
    let id: i32 = row.get(0);
    let name: String = row.get(1);
    let blob_data: Vec<u8> = row.get(2);
    
    println!("Retrieved large blob: id={}, name={}, size={} bytes", id, name, blob_data.len());
    assert_eq!(id, 1);
    assert_eq!(name, "large_test");
    assert_eq!(blob_data.len(), 2048);
    assert!(blob_data.iter().all(|&b| b == 0xAB));
    
    // Query for the small blob
    let rows = client.query("SELECT id, name, large_blob FROM large_data WHERE id = 2", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    
    let row = &rows[0];
    let id: i32 = row.get(0);
    let name: String = row.get(1);
    let blob_data: Vec<u8> = row.get(2);
    
    println!("Retrieved small blob: id={}, name={}, size={} bytes", id, name, blob_data.len());
    assert_eq!(id, 2);
    assert_eq!(name, "small_test");
    assert_eq!(blob_data.len(), 256);
    assert!(blob_data.iter().all(|&b| b == 0xCD));
    
    // Query all data to test batch handling
    let rows = client.query("SELECT id, name, LENGTH(large_blob) FROM large_data ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    
    println!("All data summary:");
    for row in &rows {
        let id: i32 = row.get(0);
        let name: String = row.get(1);
        let blob_size: i32 = row.get(2);
        println!("  Row {}: name={}, blob_size={} bytes", id, name, blob_size);
    }
    
    server.abort();
}

#[tokio::test]
async fn test_memory_mapped_disabled() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pgsqlite=debug")
        .try_init();

    // Create executor with memory mapping disabled (default)
    let executor = MemoryMappedQueryExecutor::new();
    let stats = executor.get_stats();
    
    println!("Memory mapping enabled: {}", stats.mmap_enabled);
    assert!(!stats.mmap_enabled);
    
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(
                "CREATE TABLE test_data (
                    id INTEGER PRIMARY KEY,
                    content TEXT
                )"
            ).await?;
            
            // Insert large text data
            let large_text = "x".repeat(50000); // 50KB of text
            db.execute(&format!(
                "INSERT INTO test_data (id, content) VALUES (1, '{}')",
                large_text
            )).await?;
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    println!("Testing large text with memory mapping disabled...");
    
    let rows = client.query("SELECT id, LENGTH(content) FROM test_data WHERE id = 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    
    let row = &rows[0];
    let id: i32 = row.get(0);
    let content_length: i32 = row.get(1);
    
    println!("Retrieved large text: id={}, length={} characters", id, content_length);
    assert_eq!(id, 1);
    assert_eq!(content_length, 50000);
    
    server.abort();
}

#[tokio::test]
async fn test_memory_mapped_configuration() {
    // Test environment variable configuration
    unsafe {
        std::env::set_var("PGSQLITE_ENABLE_MMAP", "1");
        std::env::set_var("PGSQLITE_MMAP_MIN_SIZE", "2048");
    }
    
    let config = ValueHandlerConfig::default();
    assert!(config.enable_mmap);
    assert_eq!(config.mmap_config.min_size_for_mmap, 2048);
    
    // Clean up environment
    unsafe {
        std::env::remove_var("PGSQLITE_ENABLE_MMAP");
        std::env::remove_var("PGSQLITE_MMAP_MIN_SIZE");
    }
    
    // Test custom configuration
    let mut custom_config = ValueHandlerConfig::default();
    custom_config.enable_mmap = true;
    custom_config.large_value_threshold = 4096;
    custom_config.mmap_config.min_size_for_mmap = 1024;
    custom_config.mmap_config.max_memory_size = 64 * 1024;
    
    let executor = MemoryMappedQueryExecutor::with_config(custom_config);
    let stats = executor.get_stats();
    
    assert!(stats.mmap_enabled);
    assert_eq!(stats.mmap_threshold, 4096);
    assert_eq!(stats.mmap_min_size, 1024);
}