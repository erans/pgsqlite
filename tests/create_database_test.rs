use std::sync::Arc;
use pgsqlite::session::DbHandler;
use pgsqlite::query::QueryExecutor;
use pgsqlite::session::SessionState;
use pgsqlite::protocol::PostgresCodec;
use tokio_util::codec::Framed;
use std::io::Cursor;

#[tokio::test]
async fn test_create_database_success() {
    // Create a temporary file database for the test
    let temp_file = format!("/tmp/test_create_database_{}.db", uuid::Uuid::new_v4());
    let db = Arc::new(DbHandler::new(&temp_file).expect("Failed to create database"));
    let session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));

    // Create a mock stream for testing
    let mock_data: Vec<u8> = vec![];
    let cursor = Cursor::new(mock_data);
    let mut framed = Framed::new(cursor, PostgresCodec::new());

    // Test CREATE DATABASE command
    let result = QueryExecutor::execute_query(
        &mut framed,
        &db,
        &session,
        "CREATE DATABASE testdb",
        None
    ).await;

    assert!(result.is_ok(), "CREATE DATABASE should succeed");

    // Clean up
    std::fs::remove_file(&temp_file).ok();
}

#[tokio::test]
async fn test_create_database_with_options() {
    // Create a temporary file database for the test
    let temp_file = format!("/tmp/test_create_database_options_{}.db", uuid::Uuid::new_v4());
    let db = Arc::new(DbHandler::new(&temp_file).expect("Failed to create database"));
    let session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));

    // Create a mock stream for testing
    let mock_data: Vec<u8> = vec![];
    let cursor = Cursor::new(mock_data);
    let mut framed = Framed::new(cursor, PostgresCodec::new());

    // Test CREATE DATABASE command with options
    let result = QueryExecutor::execute_query(
        &mut framed,
        &db,
        &session,
        "CREATE DATABASE testdb WITH ENCODING 'UTF8'",
        None
    ).await;

    assert!(result.is_ok(), "CREATE DATABASE with options should succeed");

    // Clean up
    std::fs::remove_file(&temp_file).ok();
}

#[tokio::test]
async fn test_create_database_case_insensitive() {
    // Create a temporary file database for the test
    let temp_file = format!("/tmp/test_create_database_case_{}.db", uuid::Uuid::new_v4());
    let db = Arc::new(DbHandler::new(&temp_file).expect("Failed to create database"));
    let session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));

    // Create a mock stream for testing
    let mock_data: Vec<u8> = vec![];
    let cursor = Cursor::new(mock_data);
    let mut framed = Framed::new(cursor, PostgresCodec::new());

    // Test various case combinations
    let test_cases = vec![
        "CREATE DATABASE testdb",
        "create database testdb",
        "Create Database testdb",
        "CREATE database testdb",
        "create DATABASE testdb",
    ];

    for query in test_cases {
        let result = QueryExecutor::execute_query(
            &mut framed,
            &db,
            &session,
            query,
            None
        ).await;

        assert!(result.is_ok(), "CREATE DATABASE should succeed for query: {}", query);
    }

    // Clean up
    std::fs::remove_file(&temp_file).ok();
}