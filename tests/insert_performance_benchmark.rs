use pgsqlite::session::DbHandler;
use std::time::Instant;
use rusqlite::Connection;

#[tokio::test] 
async fn test_insert_performance_comparison() {
    println!("\n=== INSERT PERFORMANCE COMPARISON ===\n");
    
    let iterations = 1000;
    
    // Test 1: Direct SQLite (baseline)
    println!("1. Direct SQLite (baseline):");
    let conn = Connection::open_in_memory().expect("Failed to create SQLite connection");
    conn.execute("CREATE TABLE baseline_test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)", [])
        .expect("Failed to create table");
    
    let start = Instant::now();
    for i in 0..iterations {
        conn.execute(
            "INSERT INTO baseline_test (name, value) VALUES (?1, ?2)", 
            rusqlite::params![format!("test{}", i), i]
        ).expect("Failed to execute INSERT");
    }
    let sqlite_time = start.elapsed();
    let sqlite_avg = sqlite_time / iterations as u32;
    println!("  Total: {:?}, Average: {:?}", sqlite_time, sqlite_avg);
    
    // Test 2: pgsqlite with non-decimal table (fast path)
    println!("\n2. pgsqlite - Non-decimal table (fast path):");
    let db = DbHandler::new(":memory:").expect("Failed to create database");
    db.execute("CREATE TABLE fast_test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)")
        .await
        .expect("Failed to create table");
    
    let start = Instant::now();
    for i in 0..iterations {
        let query = format!("INSERT INTO fast_test (name, value) VALUES ('test{}', {})", i, i);
        db.execute(&query).await.expect("Failed to execute INSERT");
    }
    let fast_time = start.elapsed();
    let fast_avg = fast_time / iterations as u32;
    println!("  Total: {:?}, Average: {:?}", fast_time, fast_avg);
    println!("  Overhead vs SQLite: {:.1}x", fast_avg.as_secs_f64() / sqlite_avg.as_secs_f64());
    
    // Test 3: pgsqlite with decimal table (slow path)
    println!("\n3. pgsqlite - Decimal table (slow path):");
    db.execute("CREATE TABLE decimal_test (id INTEGER PRIMARY KEY, price DECIMAL(10,2), name TEXT)")
        .await
        .expect("Failed to create table");
    
    let start = Instant::now();
    for i in 0..iterations {
        let query = format!("INSERT INTO decimal_test (price, name) VALUES ({}.99, 'test{}')", i, i);
        db.execute(&query).await.expect("Failed to execute INSERT");
    }
    let decimal_time = start.elapsed();
    let decimal_avg = decimal_time / iterations as u32;
    println!("  Total: {:?}, Average: {:?}", decimal_time, decimal_avg);
    println!("  Overhead vs SQLite: {:.1}x", decimal_avg.as_secs_f64() / sqlite_avg.as_secs_f64());
    println!("  Overhead vs fast path: {:.1}x", decimal_avg.as_secs_f64() / fast_avg.as_secs_f64());
    
    // Test 4: pgsqlite with parameterized queries
    println!("\n4. pgsqlite - Parameterized INSERT:");
    let start = Instant::now();
    for i in 0..iterations {
        db.try_execute_fast_path_with_params(
            "INSERT INTO fast_test (name, value) VALUES ($1, $2)",
            &[
                rusqlite::types::Value::Text(format!("param{}", i)),
                rusqlite::types::Value::Integer(i as i64),
            ],
        )
        .await
        .expect("Failed to execute parameterized INSERT");
    }
    let param_time = start.elapsed();
    let param_avg = param_time / iterations as u32;
    println!("  Total: {:?}, Average: {:?}", param_time, param_avg);
    println!("  Overhead vs SQLite: {:.1}x", param_avg.as_secs_f64() / sqlite_avg.as_secs_f64());
    
    // Test 5: pgsqlite with statement pool
    println!("\n5. pgsqlite - Statement pool:");
    let start = Instant::now();
    for i in 0..iterations {
        db.execute_with_statement_pool_params(
            "INSERT INTO fast_test (name, value) VALUES ($1, $2)",
            &[
                rusqlite::types::Value::Text(format!("pool{}", i)),
                rusqlite::types::Value::Integer(i as i64),
            ],
        )
        .await
        .expect("Failed to execute INSERT with statement pool");
    }
    let pool_time = start.elapsed();
    let pool_avg = pool_time / iterations as u32;
    println!("  Total: {:?}, Average: {:?}", pool_time, pool_avg);
    println!("  Overhead vs SQLite: {:.1}x", pool_avg.as_secs_f64() / sqlite_avg.as_secs_f64());
    
    // Summary
    println!("\n=== SUMMARY ===");
    println!("SQLite baseline:        {:?} per INSERT", sqlite_avg);
    println!("Fast path (no decimal): {:?} per INSERT ({:.1}x overhead)", fast_avg, fast_avg.as_secs_f64() / sqlite_avg.as_secs_f64());
    println!("Slow path (decimal):    {:?} per INSERT ({:.1}x overhead)", decimal_avg, decimal_avg.as_secs_f64() / sqlite_avg.as_secs_f64());
    println!("Parameterized:          {:?} per INSERT ({:.1}x overhead)", param_avg, param_avg.as_secs_f64() / sqlite_avg.as_secs_f64());
    println!("Statement pool:         {:?} per INSERT ({:.1}x overhead)", pool_avg, pool_avg.as_secs_f64() / sqlite_avg.as_secs_f64());
    
    // Calculate theoretical best case
    let mutex_overhead = fast_avg.as_secs_f64() - sqlite_avg.as_secs_f64();
    println!("\nMutex + protocol overhead: ~{:.1}µs per operation", mutex_overhead * 1_000_000.0);
}