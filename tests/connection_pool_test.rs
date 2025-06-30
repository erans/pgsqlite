use pgsqlite::session::DbHandler;
use std::sync::Arc;

#[tokio::test]
async fn test_db_handler_basic() -> Result<(), Box<dyn std::error::Error>> {
    let db_handler = Arc::new(DbHandler::new(":memory:")?);
    
    // Test basic operations
    db_handler.execute("CREATE TABLE test_basic (id INTEGER PRIMARY KEY, name TEXT)").await?;
    db_handler.execute("INSERT INTO test_basic (name) VALUES ('test1')").await?;
    
    let result = db_handler.query("SELECT * FROM test_basic").await?;
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.columns, vec!["id", "name"]);
    
    Ok(())
}

#[tokio::test]
async fn test_db_handler_transactions() -> Result<(), Box<dyn std::error::Error>> {
    let db_handler = Arc::new(DbHandler::new(":memory:")?);
    
    // Create test table
    db_handler.execute("CREATE TABLE test_tx (id INTEGER PRIMARY KEY, value INTEGER)").await?;
    
    // Test successful transaction
    db_handler.begin().await?;
    db_handler.execute("INSERT INTO test_tx (value) VALUES (100)").await?;
    db_handler.execute("INSERT INTO test_tx (value) VALUES (200)").await?;
    db_handler.commit().await?;
    
    let result = db_handler.query("SELECT COUNT(*) FROM test_tx").await?;
    assert_eq!(result.rows.len(), 1);
    // Check that we have 2 rows
    let count_bytes = result.rows[0][0].as_ref().unwrap();
    let count_str = std::str::from_utf8(count_bytes).unwrap();
    let count: i64 = count_str.parse().unwrap();
    assert_eq!(count, 2);
    
    // Test rollback
    db_handler.begin().await?;
    db_handler.execute("INSERT INTO test_tx (value) VALUES (300)").await?;
    db_handler.rollback().await?;
    
    let result = db_handler.query("SELECT COUNT(*) FROM test_tx").await?;
    let count_bytes = result.rows[0][0].as_ref().unwrap();
    let count_str = std::str::from_utf8(count_bytes).unwrap();
    let count: i64 = count_str.parse().unwrap();
    assert_eq!(count, 2); // Should still be 2
    
    Ok(())
}

#[tokio::test]
async fn test_db_handler_concurrent_access() -> Result<(), Box<dyn std::error::Error>> {
    let db_handler = Arc::new(DbHandler::new(":memory:")?);
    
    // Create test table with AUTOINCREMENT to avoid conflicts
    db_handler.execute("CREATE TABLE test_concurrent (id INTEGER PRIMARY KEY AUTOINCREMENT, thread_id INTEGER)").await?;
    
    // Test concurrent reads (these should work fine)
    let mut handles = vec![];
    
    // First insert some test data
    for i in 0..10 {
        db_handler.execute(&format!("INSERT INTO test_concurrent (thread_id) VALUES ({})", i)).await?;
    }
    
    // Spawn multiple tasks for concurrent reads
    for i in 0..5 {
        let db = db_handler.clone();
        let handle = tokio::spawn(async move {
            for _ in 0..10 {
                let result = db.query("SELECT COUNT(*) FROM test_concurrent")
                    .await
                    .expect("Read should not fail");
                assert!(result.rows.len() > 0);
            }
            i
        });
        handles.push(handle);
    }
    
    // Wait for all read tasks to complete
    let mut results = vec![];
    for handle in handles {
        results.push(handle.await?);
    }
    assert_eq!(results.len(), 5);
    
    // Test sequential writes (as SQLite doesn't handle concurrent writes well)
    for i in 0..5 {
        for j in 0..10 {
            db_handler.execute(&format!("INSERT INTO test_concurrent (thread_id) VALUES ({})", i * 100 + j)).await?;
        }
    }
    
    // Verify total count
    let result = db_handler.query("SELECT COUNT(*) FROM test_concurrent").await?;
    let count_bytes = result.rows[0][0].as_ref().unwrap();
    let count_str = std::str::from_utf8(count_bytes).unwrap();
    let count: i64 = count_str.parse().unwrap();
    assert_eq!(count, 60); // 10 initial + 50 sequential inserts
    
    Ok(())
}

#[tokio::test]
async fn test_db_handler_schema_cache() -> Result<(), Box<dyn std::error::Error>> {
    let db_handler = Arc::new(DbHandler::new(":memory:")?);
    
    // Create a table with various types
    db_handler.execute("CREATE TABLE test_schema (
        id INTEGER PRIMARY KEY,
        name TEXT,
        age INTEGER,
        salary DECIMAL(10,2),
        active BOOLEAN
    )").await?;
    
    // Get schema should use cache on second call
    let schema1 = db_handler.get_table_schema("test_schema").await?;
    let _schema2 = db_handler.get_table_schema("test_schema").await?; // Should hit cache
    
    // Verify schema has correct column count
    assert_eq!(schema1.columns.len(), 5);
    
    // Verify column names
    assert_eq!(schema1.columns[0].name, "id");
    assert_eq!(schema1.columns[1].name, "name");
    assert_eq!(schema1.columns[2].name, "age");
    assert_eq!(schema1.columns[3].name, "salary");
    assert_eq!(schema1.columns[4].name, "active");
    
    Ok(())
}