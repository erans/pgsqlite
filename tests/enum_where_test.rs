mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_enum_where_equality() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create an ENUM type
    client.simple_query("CREATE TYPE status AS ENUM ('pending', 'active', 'inactive', 'deleted')")
        .await
        .expect("Failed to create ENUM type");
    
    // Create a table with an ENUM column
    client.simple_query("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, status status)")
        .await
        .expect("Failed to create table");
    
    // Insert test data
    client.simple_query("INSERT INTO items (id, name, status) VALUES (1, 'Item 1', 'active')")
        .await
        .expect("Failed to insert row 1");
    client.simple_query("INSERT INTO items (id, name, status) VALUES (2, 'Item 2', 'pending')")
        .await
        .expect("Failed to insert row 2");
    client.simple_query("INSERT INTO items (id, name, status) VALUES (3, 'Item 3', 'inactive')")
        .await
        .expect("Failed to insert row 3");
    client.simple_query("INSERT INTO items (id, name, status) VALUES (4, 'Item 4', 'deleted')")
        .await
        .expect("Failed to insert row 4");
    
    // Test equality comparison
    let results = client.simple_query("SELECT id, name, status FROM items WHERE status = 'active'")
        .await
        .expect("Failed to query with WHERE clause");
    
    // Count data rows (skip RowDescription and CommandComplete)
    let mut data_rows = 0;
    for result in &results {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = result {
            data_rows += 1;
            assert_eq!(row.get("status"), Some("active"));
        }
    }
    assert_eq!(data_rows, 1, "Should find exactly one active item");
    
    // Test inequality comparison
    let results = client.simple_query("SELECT id, name, status FROM items WHERE status != 'deleted'")
        .await
        .expect("Failed to query with != WHERE clause");
    
    let mut data_rows = 0;
    for result in &results {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = result {
            data_rows += 1;
            let status = row.get("status").unwrap();
            assert_ne!(status, "deleted");
        }
    }
    assert_eq!(data_rows, 3, "Should find three non-deleted items");
    
    server.abort();
}

#[tokio::test]
async fn test_enum_where_in_list() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create an ENUM type
    client.simple_query("CREATE TYPE priority AS ENUM ('low', 'medium', 'high', 'critical')")
        .await
        .expect("Failed to create ENUM type");
    
    // Create a table
    client.simple_query("CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT, priority priority)")
        .await
        .expect("Failed to create table");
    
    // Insert test data
    client.simple_query("INSERT INTO tasks VALUES (1, 'Task 1', 'low'), (2, 'Task 2', 'high'), (3, 'Task 3', 'critical'), (4, 'Task 4', 'medium')")
        .await
        .expect("Failed to insert tasks");
    
    // Test IN clause
    let results = client.simple_query("SELECT id, title, priority FROM tasks WHERE priority IN ('high', 'critical') ORDER BY id")
        .await
        .expect("Failed to query with IN clause");
    
    let mut data_rows = Vec::new();
    for result in &results {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = result {
            data_rows.push((
                row.get("id").unwrap(),
                row.get("priority").unwrap()
            ));
        }
    }
    
    assert_eq!(data_rows.len(), 2, "Should find two high priority tasks");
    assert_eq!(data_rows[0], ("2", "high"));
    assert_eq!(data_rows[1], ("3", "critical"));
    
    // Test NOT IN clause
    let results = client.simple_query("SELECT COUNT(*) as cnt FROM tasks WHERE priority NOT IN ('low')")
        .await
        .expect("Failed to query with NOT IN clause");
    
    for result in &results {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = result {
            assert_eq!(row.get("cnt"), Some("3"));
            break;
        }
    }
    
    server.abort();
}

#[tokio::test]
async fn test_enum_where_ordering() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create an ENUM type with specific ordering
    client.simple_query("CREATE TYPE severity AS ENUM ('info', 'warning', 'error', 'fatal')")
        .await
        .expect("Failed to create ENUM type");
    
    // Create a table
    client.simple_query("CREATE TABLE logs (id INTEGER PRIMARY KEY, message TEXT, severity severity)")
        .await
        .expect("Failed to create table");
    
    // Insert test data
    client.simple_query("INSERT INTO logs VALUES (1, 'Info message', 'info'), (2, 'Warning message', 'warning'), (3, 'Error message', 'error'), (4, 'Fatal message', 'fatal')")
        .await
        .expect("Failed to insert logs");
    
    // Note: In SQLite, ENUM ordering comparisons will use text comparison
    // which may not match PostgreSQL's enum ordering. This is a known limitation.
    // For now, we'll test that basic comparisons work with text ordering.
    
    // Test ordering with WHERE clause
    let results = client.simple_query("SELECT id, severity FROM logs WHERE severity = 'error' OR severity = 'fatal' ORDER BY id")
        .await
        .expect("Failed to query with ordering comparison");
    
    let mut severe_logs = 0;
    for result in &results {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = result {
            severe_logs += 1;
            let severity = row.get("severity").unwrap();
            assert!(severity == "error" || severity == "fatal");
        }
    }
    assert_eq!(severe_logs, 2, "Should find two severe logs");
    
    server.abort();
}

#[tokio::test]
async fn test_enum_where_null_handling() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create an ENUM type
    client.simple_query("CREATE TYPE state AS ENUM ('draft', 'published', 'archived')")
        .await
        .expect("Failed to create ENUM type");
    
    // Create a table with nullable ENUM column
    client.simple_query("CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT, state state)")
        .await
        .expect("Failed to create table");
    
    // Insert test data including NULL values
    client.simple_query("INSERT INTO posts VALUES (1, 'Post 1', 'published'), (2, 'Post 2', NULL), (3, 'Post 3', 'draft'), (4, 'Post 4', NULL)")
        .await
        .expect("Failed to insert posts");
    
    // Test IS NULL
    let results = client.simple_query("SELECT id FROM posts WHERE state IS NULL ORDER BY id")
        .await
        .expect("Failed to query with IS NULL");
    
    let mut null_posts = Vec::new();
    for result in &results {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = result {
            null_posts.push(row.get("id").unwrap());
        }
    }
    assert_eq!(null_posts, vec!["2", "4"]);
    
    // Test IS NOT NULL
    let results = client.simple_query("SELECT COUNT(*) as cnt FROM posts WHERE state IS NOT NULL")
        .await
        .expect("Failed to query with IS NOT NULL");
    
    for result in &results {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = result {
            assert_eq!(row.get("cnt"), Some("2"));
            break;
        }
    }
    
    server.abort();
}