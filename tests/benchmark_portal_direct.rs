use pgsqlite::session::{DbHandler, SessionState, Portal, CachedQueryResult};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Direct Portal Management Performance Benchmark
/// Tests portal performance using internal APIs to avoid network protocol issues
#[tokio::test]
async fn benchmark_portal_performance_direct() {
    println!("\n🚀 === Direct Portal Performance Benchmark ===");
    println!("Testing portal management performance using internal APIs\n");

    // Setup temporary file database
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let db_path = format!("/tmp/benchmark_portal_direct_{}.db", timestamp);
    let db_handler = Arc::new(DbHandler::new(&db_path).expect("Failed to create database"));
    let session = Arc::new(SessionState::new("bench_user".to_string(), "bench_db".to_string()));

    // Create test data
    setup_test_data(&db_handler).await;
    
    println!("📊 === Memory Efficiency Benchmark ===\n");
    
    // Test 1: Traditional Full Fetch
    test_traditional_fetch(&db_handler).await;
    
    println!();
    
    // Test 2: Portal-based Chunked Fetch  
    test_portal_chunked_fetch(&db_handler, &session).await;
    
    println!();
    
    // Test 3: Portal Resource Management
    test_portal_resource_management(&session).await;
    
    println!();
    
    // Test 4: Concurrent Portal Operations
    test_concurrent_portals(&session).await;
    
    println!("\n✅ === Direct Portal Benchmark Complete ===\n");
    
    // Cleanup
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
}

async fn setup_test_data(db_handler: &DbHandler) {
    println!("🔧 Setting up test dataset...");
    
    // Create table
    db_handler.execute("CREATE TABLE large_table (
        id INTEGER PRIMARY KEY,
        data TEXT,
        value INTEGER,
        category TEXT
    )").await.expect("Failed to create table");
    
    // Insert test data
    let record_count = 10_000;
    let batch_size = 1000;
    
    let start = Instant::now();
    
    for batch in 0..(record_count / batch_size) {
        let mut values = Vec::new();
        
        for i in 0..batch_size {
            let record_id = batch * batch_size + i;
            values.push(format!(
                "({}, 'Data record {}', {}, 'Category {}')",
                record_id,
                record_id,
                record_id * 2,
                record_id % 10
            ));
        }
        
        let query = format!("INSERT INTO large_table (id, data, value, category) VALUES {}", 
            values.join(", "));
        
        db_handler.execute(&query).await.expect("Failed to insert batch");
    }
    
    let setup_time = start.elapsed();
    println!("  ✅ Created {record_count} records in {setup_time:?}\n");
}

async fn test_traditional_fetch(db_handler: &DbHandler) {
    println!("📋 Test 1: Traditional Full Fetch");
    
    let start = Instant::now();
    
    // Execute query and get all results at once
    let result = db_handler.query("SELECT * FROM large_table ORDER BY id").await
        .expect("Failed to execute query");
    
    let fetch_time = start.elapsed();
    let row_count = result.rows.len();
    let estimated_memory = row_count * 150; // ~150 bytes per row estimate
    
    println!("  ⏱️  Full fetch time: {fetch_time:?}");
    println!("  📊 Rows retrieved: {row_count}");
    println!("  💾 Estimated memory: ~{:.2} MB", estimated_memory as f64 / 1_000_000.0);
    println!("  🚀 Throughput: {:.0} rows/sec", 
        row_count as f64 / fetch_time.as_secs_f64());
}

async fn test_portal_chunked_fetch(db_handler: &DbHandler, session: &Arc<SessionState>) {
    println!("📋 Test 2: Portal-based Chunked Fetch");
    
    // Create portal
    let portal = Portal {
        statement_name: "large_query".to_string(),
        query: "SELECT * FROM large_table ORDER BY id".to_string(),
        translated_query: None,
        bound_values: vec![],
        param_formats: vec![],
        result_formats: vec![],
        inferred_param_types: None,
        client_param_types: vec![],
    };
    
    let portal_name = "bench_portal".to_string();
    session.portal_manager.create_portal(portal_name.clone(), portal)
        .expect("Failed to create portal");
    
    let chunk_size = 1000;
    let start = Instant::now();
    
    // Simulate fetching in chunks
    let mut total_rows = 0;
    let mut chunk_count = 0;
    let peak_memory = chunk_size * 150; // Memory for one chunk
    
    // First chunk - simulate initial query execution
    let first_chunk_start = Instant::now();
    let first_result = db_handler.query(&format!(
        "SELECT * FROM large_table ORDER BY id LIMIT {chunk_size}"
    )).await.expect("Failed to execute first chunk");
    let first_chunk_time = first_chunk_start.elapsed();
    
    total_rows += first_result.rows.len();
    chunk_count += 1;
    
    // Cache the result in portal (simulating portal state management)
    let cached_result = CachedQueryResult {
        rows: first_result.rows.clone(),
        field_descriptions: vec![],
        command_tag: format!("SELECT {chunk_size}"),
    };
    
    session.portal_manager.update_execution_state(
        &portal_name,
        total_rows,
        false,
        Some(cached_result),
    ).expect("Failed to update portal state");
    
    println!("  📦 First chunk: {} rows in {:?}", first_result.rows.len(), first_chunk_time);
    
    // Subsequent chunks - simulate portal Resume operations  
    let mut subsequent_time = std::time::Duration::ZERO;
    
    while total_rows < 10_000 {
        let chunk_start = Instant::now();
        
        // Simulate fetching next chunk using OFFSET
        let offset_result = db_handler.query(&format!(
            "SELECT * FROM large_table ORDER BY id LIMIT {chunk_size} OFFSET {total_rows}"
        )).await.expect("Failed to execute chunk");
        
        let chunk_time = chunk_start.elapsed();
        subsequent_time += chunk_time;
        
        if offset_result.rows.is_empty() {
            break;
        }
        
        total_rows += offset_result.rows.len();
        chunk_count += 1;
        
        // Update portal state
        session.portal_manager.update_execution_state(
            &portal_name,
            total_rows,
            offset_result.rows.len() < chunk_size,
            None,
        ).expect("Failed to update portal state");
        
        if chunk_count <= 5 || chunk_count % 3 == 0 {
            println!("  📦 Chunk {}: {} rows in {:?}", chunk_count, offset_result.rows.len(), chunk_time);
        }
    }
    
    let total_time = start.elapsed();
    
    // Close portal
    session.portal_manager.close_portal(&portal_name);
    
    println!("\n  📊 Portal Chunked Fetch Results:");
    println!("  ⏱️  Total time: {total_time:?}");
    println!("  📦 Chunks processed: {chunk_count}");
    println!("  📊 Total rows: {total_rows}");
    println!("  💾 Peak memory: ~{:.2} MB (vs full fetch)", peak_memory as f64 / 1_000_000.0);
    println!("  🚀 Throughput: {:.0} rows/sec", 
        total_rows as f64 / total_time.as_secs_f64());
    
    // Calculate memory efficiency
    let full_memory = total_rows * 150;
    let memory_savings = ((full_memory - peak_memory) as f64 / full_memory as f64) * 100.0;
    println!("  💡 Memory savings: {:.1}% ({:.2}MB vs {:.2}MB)", 
        memory_savings,
        full_memory as f64 / 1_000_000.0,
        peak_memory as f64 / 1_000_000.0);
}

async fn test_portal_resource_management(session: &Arc<SessionState>) {
    println!("📋 Test 3: Portal Resource Management");
    
    let portal_count = 100;
    let start = Instant::now();
    
    // Create many portals to test resource management
    for i in 0..portal_count {
        let portal = Portal {
            statement_name: format!("stmt_{i}"),
            query: format!("SELECT * FROM large_table WHERE category = '{}'", i % 10),
            translated_query: None,
            bound_values: vec![],
            param_formats: vec![],
            result_formats: vec![],
            inferred_param_types: None,
            client_param_types: vec![],
        };
        
        session.portal_manager.create_portal(format!("portal_{i}"), portal)
            .expect("Failed to create portal");
    }
    
    let creation_time = start.elapsed();
    let active_portals = session.portal_manager.portal_count();
    
    println!("  ⏱️  Portal creation time: {creation_time:?}");
    println!("  📊 Active portals: {active_portals}");
    println!("  🚀 Creation rate: {:.0} portals/sec", 
        portal_count as f64 / creation_time.as_secs_f64());
    
    // Test portal retrieval performance
    let retrieval_start = Instant::now();
    let mut successful_retrievals = 0;
    
    for i in 0..portal_count {
        if session.portal_manager.get_portal(&format!("portal_{i}")).is_some() {
            successful_retrievals += 1;
        }
    }
    
    let retrieval_time = retrieval_start.elapsed();
    
    println!("  ⏱️  Portal retrieval time: {retrieval_time:?}");
    println!("  📊 Successful retrievals: {successful_retrievals}");
    println!("  🚀 Retrieval rate: {:.0} lookups/sec", 
        successful_retrievals as f64 / retrieval_time.as_secs_f64());
    
    // Test cleanup performance
    let cleanup_start = Instant::now();
    let removed = session.portal_manager.cleanup_stale_portals(std::time::Duration::from_secs(0));
    let cleanup_time = cleanup_start.elapsed();
    
    println!("  ⏱️  Cleanup time: {cleanup_time:?}");
    println!("  📊 Portals cleaned up: {removed}");
    println!("  📊 Remaining portals: {}", session.portal_manager.portal_count());
}

async fn test_concurrent_portals(session: &Arc<SessionState>) {
    println!("📋 Test 4: Concurrent Portal Operations");
    
    let concurrent_count = 10;
    let operations_per_portal = 100;
    
    let start = Instant::now();
    
    // Create concurrent portal operations
    let mut handles = Vec::new();
    
    for portal_id in 0..concurrent_count {
        let session_clone = Arc::clone(session);
        
        let handle = tokio::spawn(async move {
            let portal_start = Instant::now();
            
            // Create portal
            let portal = Portal {
                statement_name: format!("concurrent_stmt_{portal_id}"),
                query: format!("SELECT count(*) FROM large_table WHERE id % {} = 0", portal_id + 1),
                translated_query: None,
                bound_values: vec![],
                param_formats: vec![],
                result_formats: vec![],
                inferred_param_types: None,
                client_param_types: vec![],
            };
            
            let portal_name = format!("concurrent_portal_{portal_id}");
            session_clone.portal_manager.create_portal(portal_name.clone(), portal)
                .expect("Failed to create concurrent portal");
            
            // Perform operations on the portal
            let mut operations = 0;
            for op_id in 0..operations_per_portal {
                // Simulate portal state updates (like Execute operations)
                let _result = session_clone.portal_manager.update_execution_state(
                    &portal_name,
                    op_id,
                    op_id == operations_per_portal - 1,
                    None,
                );
                
                operations += 1;
            }
            
            // Clean up
            session_clone.portal_manager.close_portal(&portal_name);
            
            let portal_time = portal_start.elapsed();
            (portal_id, operations, portal_time)
        });
        
        handles.push(handle);
    }
    
    // Wait for all concurrent operations to complete
    let mut results = Vec::new();
    for handle in handles {
        let result = handle.await.expect("Concurrent portal task failed");
        results.push(result);
    }
    
    let total_time = start.elapsed();
    
    // Calculate results
    let total_operations: usize = results.iter().map(|(_, ops, _)| *ops).sum();
    let avg_portal_time: std::time::Duration = results.iter()
        .map(|(_, _, time)| *time)
        .sum::<std::time::Duration>() / concurrent_count as u32;
    
    println!("  ⏱️  Total concurrent time: {total_time:?}");
    println!("  ⏱️  Average portal time: {avg_portal_time:?}");
    println!("  📊 Concurrent portals: {concurrent_count}");
    println!("  📊 Operations per portal: {operations_per_portal}");
    println!("  📊 Total operations: {total_operations}");
    println!("  🚀 Operations/sec: {:.0}", 
        total_operations as f64 / total_time.as_secs_f64());
    println!("  🚀 Concurrency efficiency: {:.1}x", 
        avg_portal_time.as_secs_f64() / total_time.as_secs_f64() * concurrent_count as f64);
}