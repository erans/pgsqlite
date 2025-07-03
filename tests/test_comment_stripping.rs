mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_comment_stripping_simple_query() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test 1: Single-line comment in simple query
    let result = client
        .simple_query("SELECT 42 -- this is a comment")
        .await
        .unwrap();
    
    // Debug print to see what messages we get (comment out when test passes)
    // println!("Result messages: {}", result.len());
    // for (i, msg) in result.iter().enumerate() {
    //     println!("  Message {}: {:?}", i, msg);
    // }
    
    // Find the row message
    let mut found_row = false;
    for msg in &result {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            assert_eq!(row.get(0), Some("42"));
            found_row = true;
            break;
        }
    }
    assert!(found_row, "Expected to find a row result");
    
    // Test 2: Multi-line comment
    let result = client
        .simple_query("SELECT /* multi\nline\ncomment */ 123")
        .await
        .unwrap();
    
    // Find the row message
    let mut found_row = false;
    for msg in &result {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            assert_eq!(row.get(0), Some("123"));
            found_row = true;
            break;
        }
    }
    assert!(found_row, "Expected to find a row result");
    
    // Test 3: Comments with string literals
    let result = client
        .simple_query(r#"
-- This is a comment
SELECT 
    'not -- a comment' as col1,  -- but this is
    '/* also not a comment */' as col2  /* and this is */
"#)
        .await
        .unwrap();
    
    // Find the row message
    let mut found_row = false;
    for msg in &result {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            assert_eq!(row.get(0), Some("not -- a comment"));
            assert_eq!(row.get(1), Some("/* also not a comment */"));
            found_row = true;
            break;
        }
    }
    assert!(found_row, "Expected to find a row result");
    
    // Test 4: Comments in DDL
    let result = client
        .simple_query(r#"
-- Create a test table
CREATE TABLE comment_test (
    id INTEGER PRIMARY KEY, -- primary key
    data TEXT /* nullable text column */
)"#)
        .await
        .unwrap();
    
    // Should have at least one CommandComplete message
    assert!(!result.is_empty());
    if let tokio_postgres::SimpleQueryMessage::CommandComplete(_) = &result[0] {
        // Success
    } else {
        panic!("Expected command complete");
    }
    
    // Test 5: Comments in DML
    let result = client
        .simple_query(r#"
INSERT INTO comment_test (id, data) 
VALUES 
    (1, 'test'), -- first row
    (2, 'data')  /* second row */
-- end of insert
"#)
        .await
        .unwrap();
    
    // Should have at least one CommandComplete message
    assert!(!result.is_empty());
    let mut found_complete = false;
    for msg in &result {
        if let tokio_postgres::SimpleQueryMessage::CommandComplete(tag) = msg {
            let tag_str = format!("{:?}", tag);
            assert!(tag_str.contains("INSERT"));
            found_complete = true;
            break;
        }
    }
    assert!(found_complete, "Expected command complete");
    
    server.abort();
}

#[tokio::test]
async fn test_comment_stripping_extended_protocol() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test 1: Comments with parameters
    let row = client
        .query_one(
            "SELECT $1::int4 -- cast parameter to int4",
            &[&42i32],
        )
        .await
        .unwrap();
    
    let value: i32 = row.get(0);
    assert_eq!(value, 42);
    
    // Test 2: Multi-line comments with parameters
    let row = client
        .query_one(
            r#"
/* This query selects a parameter
   with type casting */
SELECT $1::text
-- end of query
"#,
            &[&"hello"],
        )
        .await
        .unwrap();
    
    let value: String = row.get(0);
    assert_eq!(value, "hello");
    
    // Test 3: Create table with comments, then use parameters
    client
        .execute(
            r#"
-- Test table for parameters
CREATE TABLE param_test (
    id INTEGER PRIMARY KEY,
    name TEXT /* person name */,
    age INTEGER -- person age
)"#,
            &[],
        )
        .await
        .unwrap();
    
    // Insert with comments and parameters
    let rows_affected = client
        .execute(
            r#"
INSERT INTO param_test (id, name, age) 
VALUES ($1, $2, $3) -- insert person
"#,
            &[&1i32, &"Alice", &30i32],
        )
        .await
        .unwrap();
    
    assert_eq!(rows_affected, 1);
    
    // Query with comments and parameters
    let row = client
        .query_one(
            r#"
-- Query person by id
SELECT name, age 
FROM param_test 
WHERE id = $1 /* parameter: person id */
"#,
            &[&1i32],
        )
        .await
        .unwrap();
    
    let name: String = row.get(0);
    let age: i32 = row.get(1);
    assert_eq!(name, "Alice");
    assert_eq!(age, 30);
    
    server.abort();
}

#[tokio::test]
async fn test_comment_edge_cases() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test 1: Empty query after comment stripping should fail
    let result = client.simple_query("-- just a comment").await;
    assert!(result.is_err(), "Empty query should fail");
    
    // Test 2: Query with only multi-line comment should fail
    let result = client.simple_query("/* only comment */").await;
    assert!(result.is_err(), "Empty query should fail");
    
    // Test 3: Nested comment syntax (PostgreSQL doesn't support nested comments)
    // Our comment stripper will produce "SELECT  still in comment */ 42" which should fail
    let result = client
        .simple_query("SELECT /* outer /* inner */ still in comment */ 42")
        .await;
    
    // This should fail to parse
    assert!(result.is_err(), "Nested comments should cause a parse error");
    
    // Test 4: String with escaped quotes and comments
    let result = client
        .simple_query(r#"SELECT 'It''s a -- test' -- with comment"#)
        .await
        .unwrap();
    
    // Find the row message
    let mut found_row = false;
    for msg in &result {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            assert_eq!(row.get(0), Some("It's a -- test"));
            found_row = true;
            break;
        }
    }
    assert!(found_row, "Expected to find a row result");
    
    // Test 5: Comment-like operators (should not be stripped)
    let result = client
        .simple_query(r#"SELECT '{"key": "value"}'::jsonb->>'key' -- json operator"#)
        .await
        .unwrap();
    
    // Find the row message
    let mut found_row = false;
    for msg in &result {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            assert_eq!(row.get(0), Some("value"));
            found_row = true;
            break;
        }
    }
    assert!(found_row, "Expected to find a row result");
    
    server.abort();
}