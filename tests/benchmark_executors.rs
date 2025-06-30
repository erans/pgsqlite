use pgsqlite::session::DbHandler;
use std::sync::Arc;
use std::time::Instant;
use std::collections::HashMap;
use rusqlite::Connection;

#[tokio::test]
#[ignore] // Run with: cargo test benchmark_executor_comparison -- --ignored --nocapture
async fn benchmark_executor_comparison() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Executor Performance Comparison ===\n");
    
    let mut all_results = HashMap::new();
    
    // === 1. RAW SQLITE BASELINE ===
    println!("--- 1. Raw SQLite Performance (baseline) ---");
    let sqlite_results = benchmark_raw_sqlite()?;
    all_results.insert("raw_sqlite", sqlite_results);
    
    // === 2. MUTEX-BASED DB HANDLER ===
    println!("\n--- 2. Mutex-based DbHandler ---");
    let mutex_results = benchmark_mutex_handler().await?;
    all_results.insert("mutex", mutex_results);
    
    // === SUMMARY ===
    print_comparison_summary(&all_results);
    
    Ok(())
}

fn benchmark_raw_sqlite() -> Result<HashMap<String, f64>, Box<dyn std::error::Error>> {
    let mut results = HashMap::new();
    let conn = Connection::open_in_memory()?;
    
    // Create table
    conn.execute("CREATE TABLE bench_users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)", [])?;
    
    // Benchmark INSERT
    let start = Instant::now();
    for i in 0..1000 {
        conn.execute(
            &format!("INSERT INTO bench_users (name, email, age) VALUES ('user{}', 'user{}@example.com', {})",
                i, i, 20 + (i % 50)), 
            []
        )?;
    }
    let insert_time = start.elapsed();
    let insert_avg = insert_time.as_secs_f64() * 1000.0 / 1000.0;
    println!("INSERT: 1000 operations in {:?} ({:.3}ms avg)", insert_time, insert_avg);
    results.insert("insert".to_string(), insert_avg);
    
    // Benchmark SELECT
    let start = Instant::now();
    for i in 0..1000 {
        let mut stmt = conn.prepare(&format!("SELECT * FROM bench_users WHERE id = {}", i + 1))?;
        let _rows: Vec<(i32, String, String, i32)> = stmt.query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?.collect::<Result<Vec<_>, _>>()?;
    }
    let select_time = start.elapsed();
    let select_avg = select_time.as_secs_f64() * 1000.0 / 1000.0;
    println!("SELECT: 1000 operations in {:?} ({:.3}ms avg)", select_time, select_avg);
    results.insert("select".to_string(), select_avg);
    
    // Benchmark UPDATE
    let start = Instant::now();
    for i in 0..1000 {
        conn.execute(
            &format!("UPDATE bench_users SET age = {} WHERE id = {}", 30 + (i % 40), i + 1),
            []
        )?;
    }
    let update_time = start.elapsed();
    let update_avg = update_time.as_secs_f64() * 1000.0 / 1000.0;
    println!("UPDATE: 1000 operations in {:?} ({:.3}ms avg)", update_time, update_avg);
    results.insert("update".to_string(), update_avg);
    
    // Benchmark DELETE
    let start = Instant::now();
    for i in 0..1000 {
        conn.execute(
            &format!("DELETE FROM bench_users WHERE id = {}", i + 1),
            []
        )?;
    }
    let delete_time = start.elapsed();
    let delete_avg = delete_time.as_secs_f64() * 1000.0 / 1000.0;
    println!("DELETE: 1000 operations in {:?} ({:.3}ms avg)", delete_time, delete_avg);
    results.insert("delete".to_string(), delete_avg);
    
    Ok(results)
}

async fn benchmark_mutex_handler() -> Result<HashMap<String, f64>, Box<dyn std::error::Error>> {
    let mut results = HashMap::new();
    let handler = Arc::new(DbHandler::new(":memory:")?);
    
    // Create table
    handler.execute("CREATE TABLE bench_users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)").await?;
    
    // Benchmark INSERT
    let start = Instant::now();
    for i in 0..1000 {
        handler.execute(&format!(
            "INSERT INTO bench_users (name, email, age) VALUES ('user{}', 'user{}@example.com', {})",
            i, i, 20 + (i % 50)
        )).await?;
    }
    let insert_time = start.elapsed();
    let insert_avg = insert_time.as_secs_f64() * 1000.0 / 1000.0;
    println!("INSERT: 1000 operations in {:?} ({:.3}ms avg)", insert_time, insert_avg);
    results.insert("insert".to_string(), insert_avg);
    
    // Benchmark SELECT
    let start = Instant::now();
    for i in 0..1000 {
        let result = handler.query(&format!("SELECT * FROM bench_users WHERE id = {}", i + 1)).await?;
        assert_eq!(result.rows.len(), 1);
    }
    let select_time = start.elapsed();
    let select_avg = select_time.as_secs_f64() * 1000.0 / 1000.0;
    println!("SELECT: 1000 operations in {:?} ({:.3}ms avg)", select_time, select_avg);
    results.insert("select".to_string(), select_avg);
    
    // Benchmark UPDATE
    let start = Instant::now();
    for i in 0..1000 {
        handler.execute(&format!(
            "UPDATE bench_users SET age = {} WHERE id = {}", 
            30 + (i % 40), i + 1
        )).await?;
    }
    let update_time = start.elapsed();
    let update_avg = update_time.as_secs_f64() * 1000.0 / 1000.0;
    println!("UPDATE: 1000 operations in {:?} ({:.3}ms avg)", update_time, update_avg);
    results.insert("update".to_string(), update_avg);
    
    // Benchmark DELETE
    let start = Instant::now();
    for i in 0..1000 {
        handler.execute(&format!("DELETE FROM bench_users WHERE id = {}", i + 1)).await?;
    }
    let delete_time = start.elapsed();
    let delete_avg = delete_time.as_secs_f64() * 1000.0 / 1000.0;
    println!("DELETE: 1000 operations in {:?} ({:.3}ms avg)", delete_time, delete_avg);
    results.insert("delete".to_string(), delete_avg);
    
    Ok(results)
}

fn print_comparison_summary(results: &HashMap<&str, HashMap<String, f64>>) {
    println!("\n=== PERFORMANCE COMPARISON SUMMARY ===\n");
    
    let sqlite = &results["raw_sqlite"];
    let mutex = &results["mutex"];
    
    println!("Average time per operation (ms):");
    println!("┌─────────┬──────────┬──────────┐");
    println!("│ Op      │ SQLite   │ Mutex    │");
    println!("├─────────┼──────────┼──────────┤");
    
    for op in ["insert", "select", "update", "delete"] {
        println!("│ {:7} │ {:8.3} │ {:8.3} │",
            op.to_uppercase(),
            sqlite[op],
            mutex[op]
        );
    }
    println!("└─────────┴──────────┴──────────┘");
    
    println!("\nOverhead vs Raw SQLite:");
    println!("┌─────────┬──────────┐");
    println!("│ Op      │ Mutex    │");
    println!("├─────────┼──────────┤");
    
    for op in ["insert", "select", "update", "delete"] {
        let mutex_overhead = mutex[op] / sqlite[op];
        
        println!("│ {:7} │ {:7.1}x │",
            op.to_uppercase(),
            mutex_overhead
        );
    }
    println!("└─────────┴──────────┘");
}