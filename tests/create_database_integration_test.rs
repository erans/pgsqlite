use std::sync::Arc;
use pgsqlite::session::DbHandler;
use pgsqlite::session::SessionState;

#[tokio::test]
async fn test_create_database_execution() {
    // Create a temporary file database for the test
    let temp_file = format!("/tmp/test_create_database_execution_{}.db", uuid::Uuid::new_v4());
    let db = Arc::new(DbHandler::new(&temp_file).expect("Failed to create database"));
    let session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));

    // Test CREATE DATABASE command by executing it directly on the database
    let result = db.query("CREATE DATABASE testdb").await;

    assert!(result.is_ok(), "CREATE DATABASE should succeed: {:?}", result);

    // Test different variations
    let variations = vec![
        "CREATE DATABASE mydb",
        "create database mydb",
        "Create Database mydb",
        "CREATE DATABASE mydb WITH ENCODING 'UTF8'",
        "CREATE DATABASE mydb WITH OWNER 'postgres' ENCODING 'UTF8'"
    ];

    for query in variations {
        let result = db.query(query).await;
        assert!(result.is_ok(), "CREATE DATABASE variation should succeed: {} - {:?}", query, result);
    }

    // Clean up
    std::fs::remove_file(&temp_file).ok();
}