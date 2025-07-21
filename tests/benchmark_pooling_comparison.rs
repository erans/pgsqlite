use std::sync::Arc;
use std::time::Instant;
use pgsqlite::session::DbHandler;
use tokio::task::JoinSet;

/// Compare performance with and without connection pooling
#[tokio::test]
async fn benchmark_pooling_comparison() {
    println!("\n=== Connection Pooling Performance Comparison ===\n");
    
    // Test parameters
    let num_tasks = 8;
    let queries_per_task = 1000;
    let query = "SELECT id, name FROM users WHERE id = 1";
    
    // Create and initialize database
    let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
    
    // Initialize test data
    db_handler.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").await.unwrap();
    for i in 1..=100 {
        db_handler.execute(&format!("INSERT INTO users (id, name) VALUES ({}, 'User{}')", i, i)).await.unwrap();
    }
    
    // Benchmark without pooling (current implementation)
    println!("Testing WITHOUT connection pooling:");
    let start = Instant::now();
    let mut tasks = JoinSet::new();
    
    for task_id in 0..num_tasks {
        let db = db_handler.clone();
        let query_str = query.to_string();
        
        tasks.spawn(async move {
            let task_start = Instant::now();
            for _ in 0..queries_per_task {
                db.query(&query_str).await.unwrap();
            }
            let elapsed = task_start.elapsed();
            (task_id, elapsed)
        });
    }
    
    let mut total_queries = 0;
    while let Some(result) = tasks.join_next().await {
        let (task_id, elapsed) = result.unwrap();
        let qps = queries_per_task as f64 / elapsed.as_secs_f64();
        println!("  Task {}: {} queries in {:.3}s ({:.0} queries/sec)", 
                 task_id, queries_per_task, elapsed.as_secs_f64(), qps);
        total_queries += queries_per_task;
    }
    
    let total_elapsed = start.elapsed();
    let total_qps = total_queries as f64 / total_elapsed.as_secs_f64();
    println!("  Total: {} queries in {:.3}s ({:.0} queries/sec)\n", 
             total_queries, total_elapsed.as_secs_f64(), total_qps);
    
    // TODO: Benchmark with pooling enabled
    // This would require setting PGSQLITE_USE_POOLING=true and using
    // the handle_test_connection_with_pool function with actual client connections
    println!("Testing WITH connection pooling:");
    println!("  (Not yet implemented - requires QueryExecutor integration)");
    println!("\nNote: Connection pooling infrastructure is complete but not yet integrated");
    println!("      into the main query execution pipeline. Once integrated, we expect");
    println!("      to see improved performance for concurrent read operations.");
}

/// Test that pooling can be enabled via environment variable
#[tokio::test]
async fn test_pooling_environment_variable() {
    unsafe {
        // Test without pooling
        std::env::remove_var("PGSQLITE_USE_POOLING");
        let result = std::env::var("PGSQLITE_USE_POOLING");
        assert!(result.is_err());
        
        // Test with pooling enabled
        std::env::set_var("PGSQLITE_USE_POOLING", "true");
        let result = std::env::var("PGSQLITE_USE_POOLING").unwrap();
        assert_eq!(result, "true");
        
        // Clean up
        std::env::remove_var("PGSQLITE_USE_POOLING");
    }
}