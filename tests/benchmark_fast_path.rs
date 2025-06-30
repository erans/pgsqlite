use pgsqlite::session::DbHandler;
use std::sync::Arc;
use std::time::Instant;

#[tokio::test]
#[ignore] // Run with: cargo test benchmark_fast_path -- --ignored --nocapture
async fn benchmark_fast_path() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Fast Path Performance Benchmark ===\n");
    
    let db_handler = Arc::new(DbHandler::new(":memory:")?);
    
    // Create two tables - one with DECIMAL, one without
    db_handler.execute("CREATE TABLE simple_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)").await?;
    db_handler.execute("CREATE TABLE decimal_table (id INTEGER PRIMARY KEY, name TEXT, price DECIMAL(10,2))").await?;
    
    // Warm up with some data
    for i in 0..100 {
        db_handler.execute(&format!("INSERT INTO simple_table (name, value) VALUES ('item{}', {})", i, i * 10)).await?;
        db_handler.execute(&format!("INSERT INTO decimal_table (name, price) VALUES ('item{}', {}.99)", i, i * 10)).await?;
    }
    
    println!("--- Testing Simple Table (Fast Path Eligible) ---");
    
    // Benchmark simple table operations (should use fast path)
    let start = Instant::now();
    for i in 100..600 {
        db_handler.execute(&format!("INSERT INTO simple_table (name, value) VALUES ('item{}', {})", i, i * 10)).await?;
    }
    let simple_insert_time = start.elapsed();
    println!("500 simple INSERTs: {:?} ({:.3}ms avg)", simple_insert_time, simple_insert_time.as_secs_f64() * 1000.0 / 500.0);
    
    let start = Instant::now();
    for _ in 0..500 {
        let _result = db_handler.query("SELECT * FROM simple_table WHERE value > 1000").await?;
    }
    let simple_select_time = start.elapsed();
    println!("500 simple SELECTs: {:?} ({:.3}ms avg)", simple_select_time, simple_select_time.as_secs_f64() * 1000.0 / 500.0);
    
    let start = Instant::now();
    for i in 0..500 {
        db_handler.execute(&format!("UPDATE simple_table SET value = {} WHERE id = {}", i * 20, i + 1)).await?;
    }
    let simple_update_time = start.elapsed();
    println!("500 simple UPDATEs: {:?} ({:.3}ms avg)", simple_update_time, simple_update_time.as_secs_f64() * 1000.0 / 500.0);
    
    println!("\n--- Testing Decimal Table (No Fast Path) ---");
    
    // Benchmark decimal table operations (should NOT use fast path)
    let start = Instant::now();
    for i in 100..600 {
        db_handler.execute(&format!("INSERT INTO decimal_table (name, price) VALUES ('item{}', {}.99)", i, i * 10)).await?;
    }
    let decimal_insert_time = start.elapsed();
    println!("500 decimal INSERTs: {:?} ({:.3}ms avg)", decimal_insert_time, decimal_insert_time.as_secs_f64() * 1000.0 / 500.0);
    
    let start = Instant::now();
    for _ in 0..500 {
        let _result = db_handler.query("SELECT * FROM decimal_table WHERE price > 100.00").await?;
    }
    let decimal_select_time = start.elapsed();
    println!("500 decimal SELECTs: {:?} ({:.3}ms avg)", decimal_select_time, decimal_select_time.as_secs_f64() * 1000.0 / 500.0);
    
    let start = Instant::now();
    for i in 0..500 {
        db_handler.execute(&format!("UPDATE decimal_table SET price = {}.99 WHERE id = {}", i * 20, i + 1)).await?;
    }
    let decimal_update_time = start.elapsed();
    println!("500 decimal UPDATEs: {:?} ({:.3}ms avg)", decimal_update_time, decimal_update_time.as_secs_f64() * 1000.0 / 500.0);
    
    // Calculate speedup
    println!("\n--- Performance Comparison ---");
    println!("INSERT speedup: {:.2}x", decimal_insert_time.as_secs_f64() / simple_insert_time.as_secs_f64());
    println!("SELECT speedup: {:.2}x", decimal_select_time.as_secs_f64() / simple_select_time.as_secs_f64());
    println!("UPDATE speedup: {:.2}x", decimal_update_time.as_secs_f64() / simple_update_time.as_secs_f64());
    
    // Verify fast path is providing significant benefit
    assert!(simple_insert_time < decimal_insert_time, 
        "Fast path should be faster for INSERTs");
    assert!(simple_select_time < decimal_select_time, 
        "Fast path should be faster for SELECTs");
    assert!(simple_update_time < decimal_update_time, 
        "Fast path should be faster for UPDATEs");
    
    Ok(())
}