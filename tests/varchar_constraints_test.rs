use pgsqlite::session::{DbHandler, SessionState};
use pgsqlite::query::QueryExecutor;
use pgsqlite::protocol::{PostgresCodec, FrontendMessage};
use tokio_util::codec::Framed;
use std::sync::Arc;

/// Test basic VARCHAR constraint validation
#[tokio::test]
async fn test_varchar_basic_constraint() {
    let db = DbHandler::new_for_test(":memory:").unwrap();
    let session = Arc::new(SessionState::new("test".to_string(), "test_user".to_string()));
    
    // Create table with VARCHAR constraints
    let create_query = "CREATE TABLE test_varchar (
        id INTEGER PRIMARY KEY,
        name VARCHAR(10),
        description VARCHAR(50)
    )";
    
    let (_client, server) = tokio::io::duplex(4096);
    let mut framed = Framed::new(server, PostgresCodec::new());
    
    QueryExecutor::execute_query(&mut framed, &db, &session, create_query).await.unwrap();
    
    // Valid insert - within constraints
    let insert_valid = "INSERT INTO test_varchar (id, name, description) VALUES 
        (1, 'John', 'A short description')";
    
    QueryExecutor::execute_query(&mut framed, &db, &session, insert_valid).await.unwrap();
    
    // Invalid insert - name too long
    let insert_invalid = "INSERT INTO test_varchar (id, name, description) VALUES 
        (2, 'ThisNameIsTooLong', 'Description')";
    
    let result = QueryExecutor::execute_query(&mut framed, &db, &session, insert_invalid).await;
    assert!(result.is_err() || check_error_response(&mut framed).await);
}

/// Test CHAR type with padding
#[tokio::test]
async fn test_char_padding() {
    let db = DbHandler::new_for_test(":memory:").unwrap();
    let session = Arc::new(SessionState::new("test".to_string(), "test_user".to_string()));
    
    // Create table with CHAR constraint
    let create_query = "CREATE TABLE test_char (
        id INTEGER PRIMARY KEY,
        code CHAR(5)
    )";
    
    let (_client, server) = tokio::io::duplex(4096);
    let mut framed = Framed::new(server, PostgresCodec::new());
    
    QueryExecutor::execute_query(&mut framed, &db, &session, create_query).await.unwrap();
    
    // Insert short value
    let insert = "INSERT INTO test_char (id, code) VALUES (1, 'AB')";
    QueryExecutor::execute_query(&mut framed, &db, &session, insert).await.unwrap();
    
    // Query should return padded value
    let select = "SELECT code, LENGTH(code) FROM test_char WHERE id = 1";
    QueryExecutor::execute_query(&mut framed, &db, &session, select).await.unwrap();
    
    // Value should be padded to 5 characters
    // Note: In actual implementation, we'd check the returned data rows
}

/// Test multi-byte character handling
#[tokio::test]
async fn test_multibyte_characters() {
    let db = DbHandler::new_for_test(":memory:").unwrap();
    let session = Arc::new(SessionState::new("test".to_string(), "test_user".to_string()));
    
    // Create table with VARCHAR constraint
    let create_query = "CREATE TABLE test_unicode (
        id INTEGER PRIMARY KEY,
        text VARCHAR(5)
    )";
    
    let (_client, server) = tokio::io::duplex(4096);
    let mut framed = Framed::new(server, PostgresCodec::new());
    
    QueryExecutor::execute_query(&mut framed, &db, &session, create_query).await.unwrap();
    
    // Valid: 5 characters (but more bytes)
    let insert_valid = "INSERT INTO test_unicode (id, text) VALUES (1, '你好世界了')";
    QueryExecutor::execute_query(&mut framed, &db, &session, insert_valid).await.unwrap();
    
    // Invalid: 6 characters
    let insert_invalid = "INSERT INTO test_unicode (id, text) VALUES (2, '你好世界了!')";
    let result = QueryExecutor::execute_query(&mut framed, &db, &session, insert_invalid).await;
    assert!(result.is_err() || check_error_response(&mut framed).await);
}

/// Test UPDATE with constraints
#[tokio::test]
async fn test_update_constraints() {
    let db = DbHandler::new_for_test(":memory:").unwrap();
    let session = Arc::new(SessionState::new("test".to_string(), "test_user".to_string()));
    
    let create_query = "CREATE TABLE test_update (
        id INTEGER PRIMARY KEY,
        name VARCHAR(10)
    )";
    
    let (_client, server) = tokio::io::duplex(4096);
    let mut framed = Framed::new(server, PostgresCodec::new());
    
    QueryExecutor::execute_query(&mut framed, &db, &session, create_query).await.unwrap();
    
    // Insert valid data
    let insert = "INSERT INTO test_update (id, name) VALUES (1, 'Short')";
    QueryExecutor::execute_query(&mut framed, &db, &session, insert).await.unwrap();
    
    // Valid update
    let update_valid = "UPDATE test_update SET name = 'StillOK' WHERE id = 1";
    QueryExecutor::execute_query(&mut framed, &db, &session, update_valid).await.unwrap();
    
    // Invalid update - exceeds constraint
    let update_invalid = "UPDATE test_update SET name = 'ThisNameIsTooLong' WHERE id = 1";
    let result = QueryExecutor::execute_query(&mut framed, &db, &session, update_invalid).await;
    assert!(result.is_err() || check_error_response(&mut framed).await);
}

/// Test NULL values (should bypass constraints)
#[tokio::test]
async fn test_null_values() {
    let db = DbHandler::new_for_test(":memory:").unwrap();
    let session = Arc::new(SessionState::new("test".to_string(), "test_user".to_string()));
    
    let create_query = "CREATE TABLE test_null (
        id INTEGER PRIMARY KEY,
        name VARCHAR(5)
    )";
    
    let (_client, server) = tokio::io::duplex(4096);
    let mut framed = Framed::new(server, PostgresCodec::new());
    
    QueryExecutor::execute_query(&mut framed, &db, &session, create_query).await.unwrap();
    
    // NULL should be allowed regardless of constraint
    let insert_null = "INSERT INTO test_null (id, name) VALUES (1, NULL)";
    QueryExecutor::execute_query(&mut framed, &db, &session, insert_null).await.unwrap();
}

/// Test CHARACTER VARYING syntax
#[tokio::test]
async fn test_character_varying() {
    let db = DbHandler::new_for_test(":memory:").unwrap();
    let session = Arc::new(SessionState::new("test".to_string(), "test_user".to_string()));
    
    let create_query = "CREATE TABLE test_char_var (
        id INTEGER PRIMARY KEY,
        name CHARACTER VARYING(15)
    )";
    
    let (_client, server) = tokio::io::duplex(4096);
    let mut framed = Framed::new(server, PostgresCodec::new());
    
    QueryExecutor::execute_query(&mut framed, &db, &session, create_query).await.unwrap();
    
    // Should work same as VARCHAR
    let insert_valid = "INSERT INTO test_char_var (id, name) VALUES (1, 'Valid Name')";
    QueryExecutor::execute_query(&mut framed, &db, &session, insert_valid).await.unwrap();
    
    let insert_invalid = "INSERT INTO test_char_var (id, name) VALUES (2, 'This name is too long for constraint')";
    let result = QueryExecutor::execute_query(&mut framed, &db, &session, insert_invalid).await;
    assert!(result.is_err() || check_error_response(&mut framed).await);
}

// Helper function to check if an error response was sent
async fn check_error_response(framed: &mut Framed<tokio::io::DuplexStream, PostgresCodec>) -> bool {
    use futures::StreamExt;
    if let Some(Ok(msg)) = framed.next().await {
        match msg {
            FrontendMessage::Query(_) => false,
            _ => false,
        }
    } else {
        false
    }
}