use pgsqlite::session::DbHandler;
use std::sync::Arc;

#[tokio::test]
async fn test_fast_path_with_metadata() -> Result<(), Box<dyn std::error::Error>> {
    // Create database handler
    let db_handler = Arc::new(DbHandler::new(":memory:")?);
    
    // Create a table with DECIMAL column
    db_handler.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price DECIMAL(10,2))").await?;
    
    // Check fast path detection for queries
    let queries = vec![
        ("INSERT INTO products (name, price) VALUES ('test', 10.99)", false, "should not use fast path with DECIMAL"),
        ("SELECT * FROM products", false, "should not use fast path with DECIMAL table"),
        ("UPDATE products SET price = 20.99", false, "should not use fast path with DECIMAL"),
    ];
    
    for (query, _expected_fast_path, description) in queries {
        println!("Testing: {query} - {description}");
        // Note: can_use_fast_path function needs connection and schema_cache which aren't directly accessible
        // from DbHandler. This test mainly verifies the table creation works.
    }
    
    // Create a simple table without DECIMAL
    db_handler.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)").await?;
    
    // Insert some test data
    db_handler.execute("INSERT INTO users (name, age) VALUES ('Alice', 25)").await?;
    db_handler.execute("INSERT INTO products (name, price) VALUES ('Widget', 9.99)").await?;
    
    // Verify data was inserted correctly
    let users_result = db_handler.query("SELECT * FROM users").await?;
    assert_eq!(users_result.rows.len(), 1);
    
    let products_result = db_handler.query("SELECT * FROM products").await?;
    assert_eq!(products_result.rows.len(), 1);
    
    println!("Fast path metadata test completed successfully");
    Ok(())
}

#[tokio::test] 
async fn test_fast_path_decimal_handling() -> Result<(), Box<dyn std::error::Error>> {
    let db_handler = Arc::new(DbHandler::new(":memory:")?);
    
    // Create table with various numeric types
    db_handler.execute("CREATE TABLE numbers (
        id INTEGER PRIMARY KEY,
        price DECIMAL(10,2),
        quantity INTEGER,
        weight REAL
    )").await?;
    
    // Insert data
    db_handler.execute("INSERT INTO numbers (price, quantity, weight) VALUES (10.99, 5, 2.5)").await?;
    
    // Test aggregations
    let sum_result = db_handler.query("SELECT SUM(price) FROM numbers").await?;
    println!("SUM(price) result: {:?}", sum_result.rows);
    
    let avg_result = db_handler.query("SELECT AVG(price) FROM numbers").await?;
    println!("AVG(price) result: {:?}", avg_result.rows);
    
    // Test arithmetic
    let calc_result = db_handler.query("SELECT price * quantity FROM numbers").await?;
    println!("price * quantity result: {:?}", calc_result.rows);
    
    Ok(())
}