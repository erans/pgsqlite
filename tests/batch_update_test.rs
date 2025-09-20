use pgsqlite::session::DbHandler;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Integration tests for batch UPDATE operations
#[tokio::test]
async fn test_batch_update_with_values() -> Result<(), Box<dyn std::error::Error>> {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let db_path = format!("/tmp/batch_update_test_{timestamp}_1.db");
    let db_handler = Arc::new(DbHandler::new(&db_path)?);
    
    // Create test table
    db_handler.execute("CREATE TABLE batch_users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)").await?;
    
    // Insert test data
    for i in 1..=5 {
        db_handler.execute(&format!("INSERT INTO batch_users (id, name, age) VALUES ({}, 'User{}', {})", i, i, i * 10)).await?;
    }
    
    // Test batch UPDATE with VALUES syntax
    let query = r#"
        UPDATE batch_users AS u 
        SET name = v.new_name, age = v.new_age 
        FROM (VALUES 
            (1, 'Alice', 25), 
            (2, 'Bob', 30), 
            (3, 'Charlie', 35)
        ) AS v(id, new_name, new_age) 
        WHERE u.id = v.id
    "#;
    
    let result = db_handler.execute(query).await?;
    println!("Batch update affected {} rows", result.rows_affected);
    
    // Verify the updates worked
    let select_result = db_handler.query("SELECT id, name, age FROM batch_users ORDER BY id").await?;
    
    // Check that rows 1-3 were updated
    let expected_data = [
        (1, "Alice", 25),
        (2, "Bob", 30),
        (3, "Charlie", 35),
        (4, "User4", 40),  // Unchanged
        (5, "User5", 50),  // Unchanged
    ];
    
    for (i, row) in select_result.rows.iter().enumerate() {
        let id: i32 = String::from_utf8(row[0].as_ref().unwrap().clone())?.parse()?;
        let name = String::from_utf8(row[1].as_ref().unwrap().clone())?;
        let age: i32 = String::from_utf8(row[2].as_ref().unwrap().clone())?.parse()?;
        
        let (expected_id, expected_name, expected_age) = expected_data[i];
        assert_eq!(id, expected_id, "ID mismatch at row {i}");
        assert_eq!(name, expected_name, "Name mismatch at row {i}");
        assert_eq!(age, expected_age, "Age mismatch at row {i}");
    }
    
    println!("✅ Batch UPDATE with VALUES test passed");
    
    // Cleanup
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    
    Ok(())
}

#[tokio::test]
async fn test_batch_update_single_column() -> Result<(), Box<dyn std::error::Error>> {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let db_path = format!("/tmp/batch_update_test_{timestamp}_2.db");
    let db_handler = Arc::new(DbHandler::new(&db_path)?);
    
    // Create test table
    db_handler.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price DECIMAL(10,2))").await?;
    
    // Insert test data
    db_handler.execute("INSERT INTO products (id, name, price) VALUES (1, 'Widget', 10.99)").await?;
    db_handler.execute("INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 25.50)").await?;
    db_handler.execute("INSERT INTO products (id, name, price) VALUES (3, 'Tool', 15.75)").await?;
    
    // Test batch UPDATE with single column
    let query = r#"
        UPDATE products AS p 
        SET price = v.new_price 
        FROM (VALUES 
            (1, 12.99), 
            (2, 27.99),
            (3, 17.99)
        ) AS v(id, new_price) 
        WHERE p.id = v.id
    "#;
    
    let result = db_handler.execute(query).await?;
    println!("Single column batch update affected {} rows", result.rows_affected);
    
    // Verify the updates
    let select_result = db_handler.query("SELECT id, price FROM products ORDER BY id").await?;
    
    let expected_prices = [12.99, 27.99, 17.99];
    for (i, row) in select_result.rows.iter().enumerate() {
        let price_str = String::from_utf8(row[1].as_ref().unwrap().clone())?;
        let price: f64 = price_str.parse()?;
        assert!((price - expected_prices[i]).abs() < 0.01, 
            "Price mismatch at row {}: expected {}, got {}", i, expected_prices[i], price);
    }
    
    println!("✅ Single column batch UPDATE test passed");
    
    // Cleanup
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    
    Ok(())
}

#[tokio::test]
async fn test_batch_update_with_quotes() -> Result<(), Box<dyn std::error::Error>> {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let db_path = format!("/tmp/batch_update_test_{timestamp}_3.db");
    let db_handler = Arc::new(DbHandler::new(&db_path)?);
    
    // Create test table
    db_handler.execute("CREATE TABLE quotes_test (id INTEGER PRIMARY KEY, description TEXT)").await?;
    
    // Insert test data
    db_handler.execute("INSERT INTO quotes_test (id, description) VALUES (1, 'Simple text')").await?;
    db_handler.execute("INSERT INTO quotes_test (id, description) VALUES (2, 'Another text')").await?;
    
    // Test batch UPDATE with quoted strings containing commas and apostrophes
    let query = r#"
        UPDATE quotes_test AS q 
        SET description = v.new_desc 
        FROM (VALUES 
            (1, 'Text with, comma'), 
            (2, 'Text with ''apostrophe''')
        ) AS v(id, new_desc) 
        WHERE q.id = v.id
    "#;
    
    let result = db_handler.execute(query).await?;
    println!("Quoted batch update affected {} rows", result.rows_affected);
    
    // Verify the updates
    let select_result = db_handler.query("SELECT id, description FROM quotes_test ORDER BY id").await?;
    
    let expected_descriptions = ["Text with, comma",
        "Text with 'apostrophe'"];
    
    for (i, row) in select_result.rows.iter().enumerate() {
        let description = String::from_utf8(row[1].as_ref().unwrap().clone())?;
        assert_eq!(description, expected_descriptions[i], 
            "Description mismatch at row {i}");
    }
    
    println!("✅ Quoted string batch UPDATE test passed");
    
    // Cleanup
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    
    Ok(())
}

#[tokio::test]
async fn test_batch_update_no_alias() -> Result<(), Box<dyn std::error::Error>> {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let db_path = format!("/tmp/batch_update_test_{timestamp}_4.db");
    let db_handler = Arc::new(DbHandler::new(&db_path)?);
    
    // Create test table
    db_handler.execute("CREATE TABLE simple_table (id INTEGER PRIMARY KEY, value INTEGER)").await?;
    
    // Insert test data
    db_handler.execute("INSERT INTO simple_table VALUES (1, 100), (2, 200), (3, 300)").await?;
    
    // Test batch UPDATE without table alias
    let query = r#"
        UPDATE simple_table 
        SET value = v.new_value 
        FROM (VALUES (1, 150), (2, 250)) AS v(id, new_value) 
        WHERE simple_table.id = v.id
    "#;
    
    let result = db_handler.execute(query).await?;
    println!("No alias batch update affected {} rows", result.rows_affected);
    
    // Verify the updates
    let select_result = db_handler.query("SELECT id, value FROM simple_table ORDER BY id").await?;
    
    let expected_values = [150, 250, 300]; // Only first two updated
    for (i, row) in select_result.rows.iter().enumerate() {
        let value: i32 = String::from_utf8(row[1].as_ref().unwrap().clone())?.parse()?;
        assert_eq!(value, expected_values[i], "Value mismatch at row {i}");
    }
    
    println!("✅ No alias batch UPDATE test passed");
    
    // Cleanup
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    
    Ok(())
}

#[tokio::test]
async fn test_batch_update_performance() -> Result<(), Box<dyn std::error::Error>> {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let db_path = format!("/tmp/batch_update_test_{timestamp}_5.db");
    let db_handler = Arc::new(DbHandler::new(&db_path)?);
    
    // Create test table
    db_handler.execute("CREATE TABLE perf_test (id INTEGER PRIMARY KEY, value INTEGER)").await?;
    
    // Insert test data
    for i in 1..=1000 {
        db_handler.execute(&format!("INSERT INTO perf_test VALUES ({}, {})", i, i * 10)).await?;
    }
    
    // Build a large batch UPDATE
    let mut values_clause = String::new();
    for i in 1..=100 {
        if i > 1 {
            values_clause.push_str(", ");
        }
        values_clause.push_str(&format!("({}, {})", i, i * 20));
    }
    
    let query = format!(r#"
        UPDATE perf_test AS p 
        SET value = v.new_value 
        FROM (VALUES {values_clause}) AS v(id, new_value) 
        WHERE p.id = v.id
    "#);
    
    let start = std::time::Instant::now();
    let result = db_handler.execute(&query).await?;
    let elapsed = start.elapsed();
    
    println!("Batch UPDATE of 100 rows took: {elapsed:?}");
    println!("Affected {} rows", result.rows_affected);
    
    // Verify some of the updates
    let verify_result = db_handler.query("SELECT value FROM perf_test WHERE id IN (1, 50, 100)").await?;
    let expected = [20, 1000, 2000]; // New values for rows 1, 50, 100
    
    for (i, row) in verify_result.rows.iter().enumerate() {
        let value: i32 = String::from_utf8(row[0].as_ref().unwrap().clone())?.parse()?;
        let row_id = if i == 0 { 1 } else if i == 1 { 50 } else { 100 };
        assert_eq!(value, expected[i], "Value mismatch for row {row_id}");
    }
    
    println!("✅ Performance batch UPDATE test passed");
    
    // Cleanup
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    
    Ok(())
}

#[tokio::test] 
async fn test_regular_update_still_works() -> Result<(), Box<dyn std::error::Error>> {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let db_path = format!("/tmp/batch_update_test_{timestamp}_6.db");
    let db_handler = Arc::new(DbHandler::new(&db_path)?);
    
    // Create test table
    db_handler.execute("CREATE TABLE regular_test (id INTEGER PRIMARY KEY, name TEXT)").await?;
    
    // Insert test data
    db_handler.execute("INSERT INTO regular_test VALUES (1, 'Original')").await?;
    
    // Test regular UPDATE (should not be affected by batch translator)
    let query = "UPDATE regular_test SET name = 'Updated' WHERE id = 1";
    let result = db_handler.execute(query).await?;
    println!("Regular update affected {} rows", result.rows_affected);
    
    // Verify the update
    let select_result = db_handler.query("SELECT name FROM regular_test WHERE id = 1").await?;
    let name = String::from_utf8(select_result.rows[0][0].as_ref().unwrap().clone())?;
    assert_eq!(name, "Updated");
    
    println!("✅ Regular UPDATE still works test passed");
    
    // Cleanup
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    
    Ok(())
}