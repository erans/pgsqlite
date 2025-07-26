use tokio::net::TcpListener;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_json_object_agg_basic() {
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Connect with tokio-postgres
    let (client, connection) = tokio_postgres::connect(
        &format!("host=localhost port={port} dbname=test user=testuser"),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });
    
    // Test basic json_object_agg functionality
    let rows = client.query(
        "SELECT json_object_agg(key, value) AS result FROM (SELECT 'name' as key, 'John' as value UNION SELECT 'age', '30' UNION SELECT 'city', 'NYC') AS t",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 1);
    let result: String = rows[0].get("result");
    
    // Parse the result JSON to verify it contains the expected key-value pairs
    let json: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(json.get("name").unwrap(), "John");
    assert_eq!(json.get("age").unwrap(), "30");
    assert_eq!(json.get("city").unwrap(), "NYC");
    
    server_handle.abort();
}

#[tokio::test]
async fn test_jsonb_object_agg_basic() {
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Connect with tokio-postgres
    let (client, connection) = tokio_postgres::connect(
        &format!("host=localhost port={port} dbname=test user=testuser"),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });
    
    // Test basic jsonb_object_agg functionality
    let rows = client.query(
        "SELECT jsonb_object_agg(key, value) AS result FROM (SELECT 'name' as key, 'Alice' as value UNION SELECT 'active', 'true' UNION SELECT 'score', '95.5') AS t",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 1);
    let result: String = rows[0].get("result");
    
    // Parse the result JSON to verify it contains the expected key-value pairs
    let json: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(json.get("name").unwrap(), "Alice");
    // jsonb_object_agg should try to parse JSON values, so "true" becomes boolean true
    assert_eq!(json.get("active").unwrap(), true);
    assert_eq!(json.get("score").unwrap(), 95.5);
    
    server_handle.abort();
}

#[tokio::test]
async fn test_json_object_agg_with_table() {
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Connect with tokio-postgres
    let (client, connection) = tokio_postgres::connect(
        &format!("host=localhost port={port} dbname=test user=testuser"),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });
    
    // Create test table
    client.simple_query(
        "CREATE TABLE test_users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            department TEXT,
            salary INTEGER
        )"
    ).await.unwrap();
    
    // Insert test data
    client.simple_query(
        "INSERT INTO test_users (name, department, salary) VALUES 
        ('John', 'Engineering', 85000),
        ('Alice', 'Marketing', 72000),
        ('Bob', 'Engineering', 90000)"
    ).await.unwrap();
    
    // Test json_object_agg with table data
    let rows = client.query(
        "SELECT json_object_agg(name, salary) AS result FROM test_users WHERE department = 'Engineering'",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 1);
    let result: String = rows[0].get("result");
    
    // Parse the result JSON to verify it contains the expected key-value pairs
    let json: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(json.get("John").unwrap(), 85000);
    assert_eq!(json.get("Bob").unwrap(), 90000);
    assert!(!json.as_object().unwrap().contains_key("Alice")); // Alice is not in Engineering
    
    server_handle.abort();
}

#[tokio::test]
async fn test_json_object_agg_empty_result() {
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Connect with tokio-postgres
    let (client, connection) = tokio_postgres::connect(
        &format!("host=localhost port={port} dbname=test user=testuser"),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });
    
    // Test empty result set
    let rows = client.query(
        "SELECT json_object_agg(key, value) AS result FROM (SELECT 'key' as key, 'value' as value WHERE 1=0) AS t",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 1);
    let result: String = rows[0].get("result");
    
    // Should return empty object for no rows
    assert_eq!(result, "{}");
    
    server_handle.abort();
}

#[tokio::test]
async fn test_json_object_agg_mixed_types() {
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Connect with tokio-postgres
    let (client, connection) = tokio_postgres::connect(
        &format!("host=localhost port={port} dbname=test user=testuser"),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });
    
    // Test with mixed data types
    let rows = client.query(
        "SELECT json_object_agg(key, value) AS result FROM (SELECT 'str' as key, 'hello' as value UNION SELECT 'num', '42' UNION SELECT 'bool', 'true' UNION SELECT 'null_val', NULL) AS t",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 1);
    let result: String = rows[0].get("result");
    
    // Parse the result JSON to verify it contains the expected key-value pairs
    let json: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(json.get("str").unwrap(), "hello");
    assert_eq!(json.get("num").unwrap(), "42"); // json_object_agg treats text as literal strings
    assert_eq!(json.get("bool").unwrap(), "true"); // json_object_agg treats text as literal strings
    assert_eq!(json.get("null_val").unwrap(), &serde_json::Value::Null);
    
    server_handle.abort();
}

#[tokio::test]
async fn test_json_object_agg_duplicate_keys() {
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Connect with tokio-postgres
    let (client, connection) = tokio_postgres::connect(
        &format!("host=localhost port={port} dbname=test user=testuser"),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });
    
    // Test with duplicate keys - UNION removes duplicates, so only one 'name' survives
    let rows = client.query(
        "SELECT json_object_agg(key, value) AS result FROM (SELECT 'name' as key, 'John' as value UNION SELECT 'age', '30' UNION SELECT 'name', 'Jane') AS t",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 1);
    let result: String = rows[0].get("result");
    
    // Parse the result JSON to verify duplicate key handling
    let json: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(json.get("name").unwrap(), "John"); // First value survives UNION
    assert_eq!(json.get("age").unwrap(), "30");
    
    server_handle.abort();
}