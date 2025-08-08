use pgsqlite::session::DbHandler;
use std::sync::Arc;

/// Integration tests for batch DELETE operations
#[tokio::test]
async fn test_batch_delete_single_column() -> Result<(), Box<dyn std::error::Error>> {
    let db_handler = {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let db_path = format!("/tmp/test_batch_delete_{timestamp}.db");
        Arc::new(DbHandler::new(&db_path)?)
    };
    
    // Create test table
    db_handler.execute("CREATE TABLE batch_delete_users (id INTEGER PRIMARY KEY, name TEXT, status TEXT)").await?;
    
    // Insert test data
    for i in 1..=10 {
        db_handler.execute(&format!(
            "INSERT INTO batch_delete_users (id, name, status) VALUES ({i}, 'User{i}', 'active')"
        )).await?;
    }
    
    // Test batch DELETE with single column using PostgreSQL USING syntax
    let query = r#"
        DELETE FROM batch_delete_users AS u 
        USING (VALUES 
            (2), 
            (4), 
            (6)
        ) AS v(id) 
        WHERE u.id = v.id
    "#;
    
    let result = db_handler.execute(query).await?;
    println!("Batch delete affected {} rows", result.rows_affected);
    
    // Should have deleted 3 rows
    assert_eq!(result.rows_affected, 3);
    
    // Verify remaining rows
    let select_result = db_handler.query("SELECT COUNT(*) FROM batch_delete_users").await?;
    let count: i32 = String::from_utf8(select_result.rows[0][0].as_ref().unwrap().clone())?.parse()?;
    assert_eq!(count, 7); // 10 - 3 = 7 remaining
    
    // Verify specific rows were deleted
    let remaining_result = db_handler.query("SELECT id FROM batch_delete_users WHERE id IN (2, 4, 6)").await?;
    assert_eq!(remaining_result.rows.len(), 0);
    
    println!("✅ Single column batch DELETE test passed");
    Ok(())
}

#[tokio::test]
async fn test_batch_delete_multi_column() -> Result<(), Box<dyn std::error::Error>> {
    // Use tempfile to ensure unique database file
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_batch_delete_multi_column.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap())?);
    
    // Create test table
    db_handler.execute("CREATE TABLE batch_delete_products (id INTEGER PRIMARY KEY, category TEXT, status TEXT)").await?;
    
    // Insert test data
    let test_data = vec![
        (1, "electronics", "active"),
        (2, "books", "active"),
        (3, "electronics", "inactive"),
        (4, "clothing", "active"),
        (5, "books", "inactive"),
        (6, "electronics", "active"),
    ];
    
    for (id, category, status) in &test_data {
        db_handler.execute(&format!(
            "INSERT INTO batch_delete_products (id, category, status) VALUES ({id}, '{category}', '{status}')"
        )).await?;
    }
    
    // Test batch DELETE with multiple columns
    let query = r#"
        DELETE FROM batch_delete_products AS p 
        USING (VALUES 
            (1, 'electronics', 'active'),
            (5, 'books', 'inactive')
        ) AS v(id, category, status) 
        WHERE p.id = v.id AND p.category = v.category AND p.status = v.status
    "#;
    
    let result = db_handler.execute(query).await?;
    println!("Multi-column batch delete affected {} rows", result.rows_affected);
    
    // Should have deleted 2 rows
    assert_eq!(result.rows_affected, 2);
    
    // Verify remaining rows
    let count_result = db_handler.query("SELECT COUNT(*) FROM batch_delete_products").await?;
    let count: i32 = String::from_utf8(count_result.rows[0][0].as_ref().unwrap().clone())?.parse()?;
    assert_eq!(count, 4); // 6 - 2 = 4 remaining
    
    // Verify specific rows were deleted
    let check_result = db_handler.query("SELECT id FROM batch_delete_products WHERE id IN (1, 5) ORDER BY id").await?;
    assert_eq!(check_result.rows.len(), 0);
    
    println!("✅ Multi-column batch DELETE test passed");
    Ok(())
}

#[tokio::test]
async fn test_batch_delete_with_quotes() -> Result<(), Box<dyn std::error::Error>> {
    let db_handler = {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let db_path = format!("/tmp/test_batch_delete_{timestamp}.db");
        Arc::new(DbHandler::new(&db_path)?)
    };
    
    // Create test table
    db_handler.execute("CREATE TABLE batch_delete_quotes (id INTEGER PRIMARY KEY, description TEXT)").await?;
    
    // Insert test data with special characters
    let test_data = vec![
        (1, "Simple text"),
        (2, "Text with, comma"),
        (3, "Text with 'apostrophe'"),
        (4, "Normal text"),
        (5, "Another text"),
    ];
    
    for (id, description) in &test_data {
        db_handler.execute(&format!(
            "INSERT INTO batch_delete_quotes (id, description) VALUES ({}, '{}')", 
            id, description.replace("'", "''")
        )).await?;
    }
    
    // Test batch DELETE with quoted strings containing commas and apostrophes
    let query = r#"
        DELETE FROM batch_delete_quotes AS q 
        USING (VALUES 
            (2, 'Text with, comma'),
            (3, 'Text with ''apostrophe''')
        ) AS v(id, description) 
        WHERE q.id = v.id AND q.description = v.description
    "#;
    
    let result = db_handler.execute(query).await?;
    println!("Quoted batch delete affected {} rows", result.rows_affected);
    
    // Should have deleted 2 rows
    assert_eq!(result.rows_affected, 2);
    
    // Verify remaining rows
    let remaining_result = db_handler.query("SELECT COUNT(*) FROM batch_delete_quotes").await?;
    let count: i32 = String::from_utf8(remaining_result.rows[0][0].as_ref().unwrap().clone())?.parse()?;
    assert_eq!(count, 3); // 5 - 2 = 3 remaining
    
    println!("✅ Quoted string batch DELETE test passed");
    Ok(())
}

#[tokio::test]
async fn test_batch_delete_no_alias() -> Result<(), Box<dyn std::error::Error>> {
    let db_handler = {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let db_path = format!("/tmp/test_batch_delete_{timestamp}.db");
        Arc::new(DbHandler::new(&db_path)?)
    };
    
    // Create test table
    db_handler.execute("CREATE TABLE batch_delete_simple (id INTEGER PRIMARY KEY, value INTEGER)").await?;
    
    // Insert test data
    for i in 1..=5 {
        db_handler.execute(&format!("INSERT INTO batch_delete_simple VALUES ({}, {})", i, i * 10)).await?;
    }
    
    // Test batch DELETE without table alias
    let query = r#"
        DELETE FROM batch_delete_simple 
        USING (VALUES (2), (4)) AS v(id) 
        WHERE batch_delete_simple.id = v.id
    "#;
    
    let result = db_handler.execute(query).await?;
    println!("No alias batch delete affected {} rows", result.rows_affected);
    
    // Should have deleted 2 rows
    assert_eq!(result.rows_affected, 2);
    
    // Verify remaining rows
    let remaining_result = db_handler.query("SELECT id FROM batch_delete_simple ORDER BY id").await?;
    assert_eq!(remaining_result.rows.len(), 3); // Should have 1, 3, 5
    
    let expected_ids = [1, 3, 5];
    for (i, row) in remaining_result.rows.iter().enumerate() {
        let id: i32 = String::from_utf8(row[0].as_ref().unwrap().clone())?.parse()?;
        assert_eq!(id, expected_ids[i]);
    }
    
    println!("✅ No alias batch DELETE test passed");
    Ok(())
}

#[tokio::test]
async fn test_batch_delete_performance() -> Result<(), Box<dyn std::error::Error>> {
    let db_handler = {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let db_path = format!("/tmp/test_batch_delete_{timestamp}.db");
        Arc::new(DbHandler::new(&db_path)?)
    };
    
    // Create test table
    db_handler.execute("CREATE TABLE batch_delete_perf (id INTEGER PRIMARY KEY, value INTEGER)").await?;
    
    // Insert test data
    for i in 1..=1000 {
        db_handler.execute(&format!("INSERT INTO batch_delete_perf VALUES ({}, {})", i, i * 10)).await?;
    }
    
    // Build a large batch DELETE for the first 100 rows
    let mut values_clause = String::new();
    for i in 1..=100 {
        if i > 1 {
            values_clause.push_str(", ");
        }
        values_clause.push_str(&format!("({i})"));
    }
    
    let query = format!(r#"
        DELETE FROM batch_delete_perf AS p 
        USING (VALUES {values_clause}) AS v(id) 
        WHERE p.id = v.id
    "#);
    
    let start = std::time::Instant::now();
    let result = db_handler.execute(&query).await?;
    let elapsed = start.elapsed();
    
    println!("Batch DELETE of 100 rows took: {elapsed:?}");
    println!("Affected {} rows", result.rows_affected);
    
    // Should have deleted exactly 100 rows
    assert_eq!(result.rows_affected, 100);
    
    // Verify remaining count
    let count_result = db_handler.query("SELECT COUNT(*) FROM batch_delete_perf").await?;
    let count: i32 = String::from_utf8(count_result.rows[0][0].as_ref().unwrap().clone())?.parse()?;
    assert_eq!(count, 900); // 1000 - 100 = 900
    
    // Verify specific rows were deleted
    let check_result = db_handler.query("SELECT COUNT(*) FROM batch_delete_perf WHERE id <= 100").await?;
    let deleted_check: i32 = String::from_utf8(check_result.rows[0][0].as_ref().unwrap().clone())?.parse()?;
    assert_eq!(deleted_check, 0);
    
    println!("✅ Performance batch DELETE test passed");
    Ok(())
}

#[tokio::test] 
async fn test_regular_delete_still_works() -> Result<(), Box<dyn std::error::Error>> {
    let db_handler = {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let db_path = format!("/tmp/test_batch_delete_{timestamp}.db");
        Arc::new(DbHandler::new(&db_path)?)
    };
    
    // Create test table
    db_handler.execute("CREATE TABLE regular_delete_test (id INTEGER PRIMARY KEY, name TEXT)").await?;
    
    // Insert test data
    db_handler.execute("INSERT INTO regular_delete_test VALUES (1, 'Keep'), (2, 'Delete')").await?;
    
    // Test regular DELETE (should not be affected by batch translator)
    let query = "DELETE FROM regular_delete_test WHERE name = 'Delete'";
    let result = db_handler.execute(query).await?;
    println!("Regular delete affected {} rows", result.rows_affected);
    
    // Should have deleted 1 row
    assert_eq!(result.rows_affected, 1);
    
    // Verify remaining row
    let select_result = db_handler.query("SELECT name FROM regular_delete_test").await?;
    assert_eq!(select_result.rows.len(), 1);
    let name = String::from_utf8(select_result.rows[0][0].as_ref().unwrap().clone())?;
    assert_eq!(name, "Keep");
    
    println!("✅ Regular DELETE still works test passed");
    Ok(())
}

#[tokio::test]
async fn test_batch_delete_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    let db_handler = {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let db_path = format!("/tmp/test_batch_delete_{timestamp}.db");
        Arc::new(DbHandler::new(&db_path)?)
    };
    
    // Create test table
    db_handler.execute("CREATE TABLE batch_delete_edge (id INTEGER PRIMARY KEY, data TEXT)").await?;
    
    // Insert test data
    for i in 1..=5 {
        db_handler.execute(&format!("INSERT INTO batch_delete_edge VALUES ({i}, 'data{i}')")).await?;
    }
    
    // Test batch DELETE with non-existent values (should affect 0 rows)
    let query = r#"
        DELETE FROM batch_delete_edge AS e
        USING (VALUES (99), (100), (101)) AS v(id) 
        WHERE e.id = v.id
    "#;
    
    let result = db_handler.execute(query).await?;
    println!("Edge case batch delete affected {} rows", result.rows_affected);
    
    // Should have deleted 0 rows
    assert_eq!(result.rows_affected, 0);
    
    // Verify all original rows remain
    let count_result = db_handler.query("SELECT COUNT(*) FROM batch_delete_edge").await?;
    let count: i32 = String::from_utf8(count_result.rows[0][0].as_ref().unwrap().clone())?.parse()?;
    assert_eq!(count, 5);
    
    println!("✅ Edge case batch DELETE test passed");
    Ok(())
}