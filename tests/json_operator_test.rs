use tokio::net::TcpListener;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_json_operators() {
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
        "CREATE TABLE test_json_ops (
            id INTEGER PRIMARY KEY,
            data JSON,
            config JSONB
        )"
    ).await.unwrap();
    
    // Insert test data
    client.simple_query(
        r#"INSERT INTO test_json_ops (id, data, config) VALUES 
        (1, '{"name": "Alice", "age": 30, "active": true}', '{"role": "admin", "perms": ["read", "write"]}'),
        (2, '{"name": "Bob", "items": [1, 2, 3]}', '{"role": "user"}'),
        (3, '{"nested": {"level1": {"level2": 42}}}', '{"complex": true}')"#
    ).await.unwrap();
    
    // Test ->> operator (text extraction)
    let rows = client.simple_query("SELECT data->>'name' AS name FROM test_json_ops WHERE id = 1").await.unwrap();
    let name = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    assert_eq!(name, "Alice");
    
    // Test -> operator (JSON extraction)
    let rows = client.simple_query("SELECT data->'items' AS items FROM test_json_ops WHERE id = 2").await.unwrap();
    let items = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    assert_eq!(items, "[1,2,3]");
    
    // Test nested -> operators
    let rows = client.simple_query("SELECT data->'nested'->'level1'->>'level2' AS value FROM test_json_ops WHERE id = 3").await.unwrap();
    let value = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    assert_eq!(value, "42");
    
    // Test #>> operator (path extraction as text)
    let rows = client.simple_query("SELECT data#>>'{nested,level1,level2}' AS value FROM test_json_ops WHERE id = 3").await.unwrap();
    let value = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    assert_eq!(value, "42");
    
    // Test @> operator (contains)
    let rows = client.simple_query("SELECT id FROM test_json_ops WHERE data @> '{\"name\": \"Alice\"}'").await.unwrap();
    let count = rows.iter()
        .filter(|msg| matches!(msg, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(count, 1);
    
    // Test <@ operator (is contained by) - reversed operands
    let rows = client.simple_query("SELECT id FROM test_json_ops WHERE '{\"name\": \"Bob\"}' <@ data").await.unwrap();
    let count = rows.iter()
        .filter(|msg| matches!(msg, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(count, 1);
    
    // Test complex query with multiple operators
    let rows = client.simple_query(
        "SELECT 
            id,
            data->>'name' AS name,
            config->>'role' AS role
        FROM test_json_ops 
        WHERE data @> '{\"active\": true}' 
           OR config->>'role' = 'user'"
    ).await.unwrap();
    let count = rows.iter()
        .filter(|msg| matches!(msg, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(count, 2); // Alice (active) and Bob (user role)
    
    println!("All JSON operator tests passed!");
    
    server_handle.abort();
}

#[tokio::test]
async fn test_json_functions() {
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
    
    // Test json_array_length
    let rows = client.simple_query("SELECT json_array_length('[1, 2, 3, 4, 5]') AS len").await.unwrap();
    let len = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    assert_eq!(len, "5");
    
    // Test json_typeof
    let rows = client.simple_query("SELECT json_typeof('{\"key\": \"value\"}') AS type").await.unwrap();
    let json_type = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    assert_eq!(json_type, "object");
    
    // Test jsonb_object_keys
    let rows = client.simple_query("SELECT jsonb_object_keys('{\"a\": 1, \"b\": 2, \"c\": 3}') AS keys").await.unwrap();
    let keys = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    assert!(keys.contains("a"));
    assert!(keys.contains("b"));
    assert!(keys.contains("c"));
    
    // Test json_strip_nulls
    let rows = client.simple_query("SELECT json_strip_nulls('{\"a\": 1, \"b\": null, \"c\": 3}') AS stripped").await.unwrap();
    let stripped = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    assert!(stripped.contains("\"a\":1"));
    assert!(!stripped.contains("\"b\":null"));
    assert!(stripped.contains("\"c\":3"));
    
    // Test jsonb_set
    let rows = client.simple_query("SELECT jsonb_set('{\"a\": 1, \"b\": 2}', '{b}', '99') AS updated").await.unwrap();
    let updated = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    assert!(updated.contains("\"b\":99"));
    
    // Test json_extract_path
    let rows = client.simple_query("SELECT json_extract_path('{\"a\": {\"b\": {\"c\": 42}}}', 'a.b.c') AS value").await.unwrap();
    let value = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    assert_eq!(value, "42");
    
    // Test json_extract_path_text
    let rows = client.simple_query("SELECT json_extract_path_text('{\"name\": \"John\", \"age\": 30}', 'name') AS name").await.unwrap();
    let name = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    assert_eq!(name, "John");
    
    println!("All JSON function tests passed!");
    
    server_handle.abort();
}