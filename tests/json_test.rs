use tokio::net::TcpListener;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_json_support() {
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
    
    // Test 1: Create table with JSON and JSONB columns
    client.simple_query(
        "CREATE TABLE test_json (
            id INTEGER PRIMARY KEY,
            config JSON,
            settings JSONB,
            metadata TEXT
        )"
    ).await.unwrap();
    
    // Test 2: Insert JSON data
    client.simple_query(
        r#"INSERT INTO test_json (id, config, settings, metadata) VALUES 
        (1, '{"name": "test", "value": 42}', '{"enabled": true, "options": ["a", "b"]}', 'plain text')"#
    ).await.unwrap();
    
    // Test 3: Test json_valid function
    let result = client.simple_query(
        r#"SELECT json_valid('{"valid": true}') as is_valid"#
    ).await.unwrap();
    let valid = result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap() == "1"),
            _ => None,
        })
        .expect("Expected to find a row");
    assert!(valid);
    
    // Test invalid JSON
    let result = client.simple_query(
        r#"SELECT json_valid('{invalid json}') as is_valid"#
    ).await.unwrap();
    let invalid = result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap() == "0"),
            _ => None,
        })
        .expect("Expected to find a row");
    assert!(invalid);
    
    // Test 4: Test json_typeof function
    let result = client.simple_query(
        r#"SELECT json_typeof('{"key": "value"}') as type"#
    ).await.unwrap();
    let json_type = result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().to_string()),
            _ => None,
        })
        .expect("Expected to find a row");
    assert_eq!(json_type, "object");
    
    // Test 5: Test json_array_length
    let result = client.simple_query(
        r#"SELECT json_array_length('[1, 2, 3, 4, 5]') as length"#
    ).await.unwrap();
    let length = result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().parse::<i32>().unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    assert_eq!(length, 5);
    
    // Test 6: Test jsonb_object_keys
    let result = client.simple_query(
        r#"SELECT jsonb_object_keys('{"name": "John", "age": 30, "city": "NYC"}') as keys"#
    ).await.unwrap();
    let keys = result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().to_string()),
            _ => None,
        })
        .expect("Expected to find a row");
    assert!(keys.contains("name"));
    assert!(keys.contains("age"));
    assert!(keys.contains("city"));
    
    // Test 7: Test to_json function
    let result = client.simple_query(
        r#"SELECT to_json('hello world') as json_str"#
    ).await.unwrap();
    let json_str = result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().to_string()),
            _ => None,
        })
        .expect("Expected to find a row");
    println!("to_json result: {json_str}");
    assert_eq!(json_str, r#""hello world""#);
    
    // Test 8: Test json_extract_scalar
    let result = client.simple_query(
        r#"SELECT json_extract_scalar('{"name": "Alice", "age": 25}', 'name') as name"#
    ).await.unwrap();
    let name = result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().to_string()),
            _ => None,
        })
        .expect("Expected to find a row");
    assert_eq!(name, "Alice");
    
    // Test 9: Test jsonb_contains
    let result = client.simple_query(
        r#"SELECT jsonb_contains('{"a": 1, "b": 2, "c": 3}', '{"a": 1, "b": 2}') as contains"#
    ).await.unwrap();
    let contains = result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap() == "1"),
            _ => None,
        })
        .expect("Expected to find a row");
    assert!(contains);
    
    // Test 10: Query the inserted data
    let result = client.simple_query(
        "SELECT id, config, settings FROM test_json WHERE id = 1"
    ).await.unwrap();
    result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                assert_eq!(row.get(0).unwrap(), "1");
                assert!(row.get(1).unwrap().contains("name"));
                assert!(row.get(2).unwrap().contains("enabled"));
                Some(())
            },
            _ => None,
        })
        .expect("Expected to find a row");
    
    println!("All JSON tests passed!");
    
    server_handle.abort();
}