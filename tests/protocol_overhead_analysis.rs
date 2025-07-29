use pgsqlite::session::DbHandler;
use std::time::Instant;
use std::sync::Arc;
use tokio::time::Duration;
use uuid::Uuid;

#[tokio::test]
async fn test_protocol_overhead_breakdown() {
    println!("\n=== PROTOCOL OVERHEAD BREAKDOWN ===\n");
    
    // Use a temporary file instead of in-memory database
    let test_id = Uuid::new_v4().to_string().replace("-", "");
    let db_path = format!("/tmp/pgsqlite_test_{}.db", test_id);
    
    let db = std::sync::Arc::new(DbHandler::new(&db_path).expect("Failed to create database"));
    
    // Create a session for testing
    let session_id = Uuid::new_v4();
    db.create_session_connection(session_id).await.expect("Failed to create session connection");
    
    // Create test table
    db.execute_with_session("CREATE TABLE protocol_test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)", &session_id)
        .await
        .expect("Failed to create table");
    
    let iterations = 1000;
    
    // Test 1: Direct DbHandler execution (no protocol)
    println!("1. Direct DbHandler execution (no protocol):");
    let start = Instant::now();
    for i in 0..iterations {
        let query = format!("INSERT INTO protocol_test (name, value) VALUES ('direct{i}', {i})");
        db.execute_with_session(&query, &session_id).await.expect("Failed to execute INSERT");
    }
    let direct_time = start.elapsed();
    let direct_avg = direct_time / iterations as u32;
    println!("  Total: {direct_time:?}, Average: {direct_avg:?}");
    
    // Test 2: Measure components of INSERT execution
    println!("\n2. Component timing for single INSERT:");
    
    let test_query = "INSERT INTO protocol_test (name, value) VALUES ('component', 999)";
    
    // Measure fast path detection
    let start = Instant::now();
    for _ in 0..100 {
        let _ = pgsqlite::query::fast_path::can_use_fast_path_enhanced(test_query);
    }
    let fast_path_time = start.elapsed() / 100;
    println!("  Fast path detection: {fast_path_time:?}");
    
    // Measure schema cache lookup
    let start = Instant::now();
    for _ in 0..100 {
        let _ = db.get_table_schema("protocol_test").await;
    }
    let schema_time = start.elapsed() / 100;
    println!("  Schema cache lookup: {schema_time:?}");
    
    // Test 3: Batch execution to identify per-query vs per-connection overhead
    println!("\n3. Batch execution analysis:");
    
    // Execute multiple INSERTs in a loop to simulate batch
    let start = Instant::now();
    for batch in 0..100 {
        for i in 0..10 {
            let query = format!("INSERT INTO protocol_test (name, value) VALUES ('batch{}', {})", batch * 10 + i, i * 100);
            db.execute_with_session(&query, &session_id).await.expect("Failed to execute INSERT");
        }
    }
    let batch_time = start.elapsed();
    println!("  1000 INSERTs (100 batches of 10): {batch_time:?}");
    println!("  Average per INSERT: {:?}", batch_time / 1000);
    
    // Test 4: Transaction overhead
    println!("\n4. Transaction overhead:");
    
    // Without transaction
    let start = Instant::now();
    for i in 0..100 {
        let query = format!("INSERT INTO protocol_test (name, value) VALUES ('notxn{i}', {i})");
        db.execute(&query).await.expect("Failed to execute INSERT");
    }
    let no_txn = start.elapsed();
    
    // With transaction
    let start = Instant::now();
    db.begin_with_session(&session_id).await.expect("Failed to begin");
    for i in 0..100 {
        let query = format!("INSERT INTO protocol_test (name, value) VALUES ('txn{i}', {i})");
        db.execute_with_session(&query, &session_id).await.expect("Failed to execute INSERT");
    }
    db.commit_with_session(&session_id).await.expect("Failed to commit");
    let with_txn = start.elapsed();
    
    println!("  100 INSERTs without transaction: {:?}, avg: {:?}", no_txn, no_txn / 100);
    println!("  100 INSERTs with transaction: {:?}, avg: {:?}", with_txn, with_txn / 100);
    
    // Test 5: Async overhead
    println!("\n5. Async overhead analysis:");
    
    // Measure tokio task spawn overhead
    let start = Instant::now();
    for _ in 0..1000 {
        tokio::task::yield_now().await;
    }
    let yield_time = start.elapsed();
    println!("  1000 yield_now calls: {:?}, avg: {:?}", yield_time, yield_time / 1000);
    
    // Summary
    println!("\n=== SUMMARY ===");
    println!("Direct DbHandler INSERT: {direct_avg:?}");
    println!("Fast path detection: {fast_path_time:?}");
    println!("Schema lookup: {schema_time:?}");
    println!("Estimated protocol overhead: ~{:?}", Duration::from_micros(200) - direct_avg);
    
    // Clean up session
    db.remove_session_connection(&session_id);
    
    // Clean up database file
    drop(db);
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{}-journal", db_path));
    let _ = std::fs::remove_file(format!("{}-wal", db_path));
    let _ = std::fs::remove_file(format!("{}-shm", db_path));
}

#[tokio::test]
async fn test_connection_handling_overhead() {
    println!("\n=== CONNECTION HANDLING OVERHEAD ===\n");
    
    // Test connection creation overhead
    let iterations = 10;
    
    let start = Instant::now();
    for _ in 0..iterations {
        let _db = DbHandler::new(":memory:").expect("Failed to create database");
    }
    let create_time = start.elapsed() / iterations as u32;
    println!("Database creation overhead: {create_time:?}");
    
    // Test mutex contention with concurrent access
    // Use a temporary file instead of in-memory database for shared access
    let test_id2 = Uuid::new_v4().to_string().replace("-", "");
    let db_path2 = format!("/tmp/pgsqlite_test_concurrent_{}.db", test_id2);
    let db = std::sync::Arc::new(DbHandler::new(&db_path2).expect("Failed to create database"));
    
    // Create session for single-threaded test
    let session_id = Uuid::new_v4();
    db.create_session_connection(session_id).await.expect("Failed to create session connection");
    
    db.execute_with_session("CREATE TABLE concurrent_test (id INTEGER PRIMARY KEY, value INTEGER)", &session_id)
        .await
        .expect("Failed to create table");
    
    println!("\nConcurrent access test:");
    
    // Single-threaded baseline
    let start = Instant::now();
    for i in 0..100 {
        db.execute_with_session(&format!("INSERT INTO concurrent_test (value) VALUES ({i})"), &session_id)
            .await
            .expect("Failed to execute");
    }
    let single_time = start.elapsed();
    
    // Multi-threaded test
    let start = Instant::now();
    let mut handles = vec![];
    
    for i in 0..10 {
        let db = Arc::clone(&db);
        let handle = tokio::spawn(async move {
            // Each thread needs its own session
            let thread_session_id = Uuid::new_v4();
            db.create_session_connection(thread_session_id).await.expect("Failed to create session connection");
            
            for j in 0..10 {
                db.execute_with_session(&format!("INSERT INTO concurrent_test (value) VALUES ({})", i * 10 + j), &thread_session_id)
                    .await
                    .expect("Failed to execute");
            }
            
            // Clean up session
            db.remove_session_connection(&thread_session_id);
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.await.expect("Task failed");
    }
    let multi_time = start.elapsed();
    
    println!("  Single-threaded (100 INSERTs): {:?}, avg: {:?}", single_time, single_time / 100);
    println!("  Multi-threaded (10x10 INSERTs): {:?}, avg: {:?}", multi_time, multi_time / 100);
    println!("  Contention factor: {:.2}x", multi_time.as_secs_f64() / single_time.as_secs_f64());
    
    // Clean up session
    db.remove_session_connection(&session_id);
    
    // Clean up database file
    drop(db);
    let _ = std::fs::remove_file(&db_path2);
    let _ = std::fs::remove_file(format!("{}-journal", db_path2));
    let _ = std::fs::remove_file(format!("{}-wal", db_path2));
    let _ = std::fs::remove_file(format!("{}-shm", db_path2));
}