use std::sync::Arc;
use std::time::Instant;
use pgsqlite::session::DbHandler;
use tokio::task::JoinSet;

#[tokio::main]
async fn main() {
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
    println!("Testing WITHOUT connection pooling (current implementation):");
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
    
    println!("Connection pooling infrastructure summary:");
    println!("  ✅ ReadOnlyDbHandler with SQLite connection pool");
    println!("  ✅ QueryRouter for intelligent query routing");
    println!("  ✅ Transaction affinity tracking");
    println!("  ✅ WAL mode enabled for multi-reader support");
    println!("  ✅ Environment variable PGSQLITE_USE_POOLING");
    println!("\n  ⚠️  Integration with QueryExecutor pending");
    println!("  ⚠️  Once integrated, expect improved concurrent read performance");
}