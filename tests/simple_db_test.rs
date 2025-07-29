#[tokio::test]
async fn test_db_handler() {
    use pgsqlite::session::DbHandler;
    use uuid::Uuid;
    
    // Use a temporary file instead of in-memory database
    let test_id = Uuid::new_v4().to_string().replace("-", "");
    let db_path = format!("/tmp/pgsqlite_test_{}.db", test_id);
    
    let db = DbHandler::new(&db_path).unwrap();
    
    // Create table
    db.execute("CREATE TABLE test (id INTEGER, name TEXT)").await.unwrap();
    
    // Insert data
    db.execute("INSERT INTO test VALUES (1, 'Alice')").await.unwrap();
    
    // Query data
    let result = db.query("SELECT * FROM test").await.unwrap();
    
    assert_eq!(result.columns, vec!["id", "name"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Some(b"1".to_vec()));
    assert_eq!(result.rows[0][1], Some(b"Alice".to_vec()));
    
    // Clean up
    drop(db);
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{}-journal", db_path));
    let _ = std::fs::remove_file(format!("{}-wal", db_path));
    let _ = std::fs::remove_file(format!("{}-shm", db_path));
}