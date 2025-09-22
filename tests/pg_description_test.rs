use pgsqlite::session::db_handler::DbHandler;
use std::sync::Arc;

#[tokio::test]
async fn test_pg_description_view_exists() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_description.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Test basic pg_description view query
    let result = db_handler.query("SELECT COUNT(*) FROM pg_description").await.unwrap();
    assert!(!result.rows.is_empty(), "Should get a count result");

    let count_bytes = result.rows[0][0].as_ref().unwrap();
    let count_str = String::from_utf8(count_bytes.clone()).unwrap();
    let count: i32 = count_str.parse().unwrap();

    // Initially should be 0 since no comments exist
    assert_eq!(count, 0, "Should have no descriptions initially, got {}", count);
    println!("✅ pg_description view contains {} descriptions (expected 0)", count);

    // Test column structure
    let result = db_handler.query("SELECT objoid, classoid, objsubid, description FROM pg_description LIMIT 1").await.unwrap();
    // Should have the correct columns even if no data
    assert_eq!(result.columns.len(), 4, "Should have 4 columns");
    assert_eq!(result.columns, vec!["objoid", "classoid", "objsubid", "description"]);
    println!("✅ pg_description has correct column structure");

    println!("🎉 pg_description SQLite view test passed!");
}

#[tokio::test]
async fn test_pg_description_with_comments() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_description_comments.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create a test table and add a comment
    db_handler.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)").await.unwrap();

    // Add a table comment
    db_handler.execute("COMMENT ON TABLE test_table IS 'This is a test table'").await.unwrap();

    // Add a column comment
    db_handler.execute("COMMENT ON COLUMN test_table.name IS 'Name column comment'").await.unwrap();

    // Test that comments are visible in pg_description
    let result = db_handler.query("SELECT COUNT(*) FROM pg_description").await.unwrap();
    let count_bytes = result.rows[0][0].as_ref().unwrap();
    let count_str = String::from_utf8(count_bytes.clone()).unwrap();
    let count: i32 = count_str.parse().unwrap();

    assert!(count > 0, "Should have at least one description after adding comments, got {}", count);
    println!("✅ pg_description contains {} descriptions after adding comments", count);

    // Test specific comment retrieval
    let result = db_handler.query("SELECT description FROM pg_description WHERE description LIKE '%test table%'").await.unwrap();
    assert!(!result.rows.is_empty(), "Should find table comment");

    let desc_bytes = result.rows[0][0].as_ref().unwrap();
    let description = String::from_utf8(desc_bytes.clone()).unwrap();
    assert!(description.contains("test table"), "Description should contain 'test table'");
    println!("✅ Found table comment: {}", description);

    println!("🎉 pg_description with comments test passed!");
}