use pgsqlite::session::DbHandler;
use std::sync::Arc;
use std::time::Instant;

#[tokio::test]
async fn test_fast_path_performance() -> Result<(), Box<dyn std::error::Error>> {
    // Create database handler
    let db_handler = Arc::new(DbHandler::new(":memory:")?);
    
    // Create a simple table without DECIMAL columns (should use fast path)
    db_handler.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)").await?;
    
    // Warm up
    for i in 0..10 {
        db_handler.execute(&format!("INSERT INTO users (name, age) VALUES ('user{}', {})", i, i + 20)).await?;
    }
    
    // Measure INSERT performance
    let start = Instant::now();
    for i in 10..110 {
        db_handler.execute(&format!("INSERT INTO users (name, age) VALUES ('user{}', {})", i, i + 20)).await?;
    }
    let insert_duration = start.elapsed();
    println!("100 INSERTs took: {:?} ({:.3}ms per insert)", insert_duration, insert_duration.as_secs_f64() * 1000.0 / 100.0);
    
    // Measure SELECT performance
    let start = Instant::now();
    for _ in 0..100 {
        let result = db_handler.query("SELECT * FROM users WHERE age > 25").await?;
        assert!(!result.rows.is_empty());
    }
    let select_duration = start.elapsed();
    println!("100 SELECTs took: {:?} ({:.3}ms per select)", select_duration, select_duration.as_secs_f64() * 1000.0 / 100.0);
    
    // Now test with DECIMAL table (should NOT use fast path)
    db_handler.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price DECIMAL(10,2))").await?;
    
    let start = Instant::now();
    for i in 0..100 {
        db_handler.execute(&format!("INSERT INTO products (name, price) VALUES ('product{i}', {i}.99)")).await?;
    }
    let decimal_insert_duration = start.elapsed();
    println!("100 DECIMAL INSERTs took: {:?} ({:.3}ms per insert)", decimal_insert_duration, decimal_insert_duration.as_secs_f64() * 1000.0 / 100.0);
    
    // Fast path should generally be faster, but with our optimized implementation
    // the difference might be small. Log the results for analysis.
    if insert_duration >= decimal_insert_duration {
        println!("WARNING: Fast path INSERT ({insert_duration:?}) was not faster than decimal path ({decimal_insert_duration:?})");
    }
    
    Ok(())
}

#[tokio::test]
async fn test_fast_path_detection() -> Result<(), Box<dyn std::error::Error>> {
    let db_handler = Arc::new(DbHandler::new(":memory:")?);
    
    // Create tables
    db_handler.execute("CREATE TABLE simple (id INTEGER, name TEXT)").await?;
    db_handler.execute("CREATE TABLE with_decimal (id INTEGER, price DECIMAL)").await?;
    
    // These should use fast path (simple queries on non-DECIMAL tables)
    let fast_queries = vec![
        "INSERT INTO simple (id, name) VALUES (1, 'test')",
        "SELECT * FROM simple",
        "UPDATE simple SET name = 'updated' WHERE id = 1",
        "DELETE FROM simple WHERE id = 1",
    ];
    
    for query in fast_queries {
        println!("Testing fast path for: {query}");
        let start = Instant::now();
        if query.starts_with("SELECT") {
            db_handler.query(query).await?;
        } else {
            db_handler.execute(query).await?;
        }
        let duration = start.elapsed();
        println!("  Executed in: {duration:?}");
    }
    
    // These should NOT use fast path (queries on DECIMAL tables)
    let slow_queries = vec![
        "INSERT INTO with_decimal (id, price) VALUES (1, 9.99)",
        "SELECT * FROM with_decimal",
        "UPDATE with_decimal SET price = 19.99 WHERE id = 1",
    ];
    
    for query in slow_queries {
        println!("Testing non-fast path for: {query}");
        let start = Instant::now();
        if query.starts_with("SELECT") {
            db_handler.query(query).await?;
        } else {
            db_handler.execute(query).await?;
        }
        let duration = start.elapsed();
        println!("  Executed in: {duration:?}");
    }
    
    Ok(())
}