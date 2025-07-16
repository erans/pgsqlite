use tokio::net::TcpListener;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_jsonb_delete_object_integration() {
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
        &format!("host=localhost port={} dbname=test user=testuser", port),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // Test deleting from JSON object
    let rows = client.simple_query(
        "SELECT jsonb_delete('{\"name\": \"John\", \"age\": 30, \"email\": \"john@example.com\"}', '{email}') AS result"
    ).await.unwrap();
    
    let result = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get("result").unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    
    // Parse the result to verify the deletion
    let json_value: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(json_value["name"], "John");
    assert_eq!(json_value["age"], 30);
    assert_eq!(json_value.get("email"), None); // Should be deleted
    
    server_handle.abort();
}

#[tokio::test]
async fn test_jsonb_delete_nested_object_integration() {
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
        &format!("host=localhost port={} dbname=test user=testuser", port),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // Test deleting from nested JSON object
    let rows = client.simple_query(
        "SELECT jsonb_delete('{\"user\": {\"name\": \"Alice\", \"email\": \"alice@example.com\"}, \"active\": true}', '{user,email}') AS result"
    ).await.unwrap();
    
    let result = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get("result").unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    
    // Parse the result to verify the deletion
    let json_value: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(json_value["user"]["name"], "Alice");
    assert_eq!(json_value["user"].get("email"), None); // Should be deleted
    assert_eq!(json_value["active"], true);
    
    server_handle.abort();
}

#[tokio::test]
async fn test_jsonb_delete_array_integration() {
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
        &format!("host=localhost port={} dbname=test user=testuser", port),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // Test deleting from JSON array
    let rows = client.simple_query(
        "SELECT jsonb_delete('[\"apple\", \"banana\", \"cherry\", \"date\"]', '{1}') AS result"
    ).await.unwrap();
    
    let result = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get("result").unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    
    // Parse the result to verify the deletion
    let json_value: serde_json::Value = serde_json::from_str(&result).unwrap();
    let array = json_value.as_array().unwrap();
    assert_eq!(array.len(), 3);
    assert_eq!(array[0], "apple");
    assert_eq!(array[1], "cherry"); // banana was deleted
    assert_eq!(array[2], "date");
    
    server_handle.abort();
}

#[tokio::test]
async fn test_jsonb_delete_path_integration() {
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
        &format!("host=localhost port={} dbname=test user=testuser", port),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // Test jsonb_delete_path function (should behave identically to jsonb_delete)
    let rows = client.simple_query(
        "SELECT jsonb_delete_path('{\"name\": \"John\", \"age\": 30, \"email\": \"john@example.com\"}', '{age}') AS result"
    ).await.unwrap();
    
    let result = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get("result").unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    
    // Parse the result to verify the deletion
    let json_value: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(json_value["name"], "John");
    assert_eq!(json_value.get("age"), None); // Should be deleted
    assert_eq!(json_value["email"], "john@example.com");
    
    server_handle.abort();
}

#[tokio::test]
async fn test_jsonb_delete_nonexistent_key_integration() {
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
        &format!("host=localhost port={} dbname=test user=testuser", port),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // Test deleting non-existent key (should return original)
    let rows = client.simple_query(
        "SELECT jsonb_delete('{\"name\": \"John\", \"age\": 30}', '{email}') AS result"
    ).await.unwrap();
    
    let result = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get("result").unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    
    // Parse the result to verify no change
    let json_value: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(json_value["name"], "John");
    assert_eq!(json_value["age"], 30);
    assert_eq!(json_value.as_object().unwrap().len(), 2); // Still 2 keys
    
    server_handle.abort();
}

#[tokio::test]
async fn test_jsonb_delete_with_table_data_integration() {
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
        &format!("host=localhost port={} dbname=test user=testuser", port),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // Create test table with JSON data
    client.simple_query(
        "CREATE TABLE test_jsonb_delete (
            id INTEGER PRIMARY KEY,
            data TEXT
        )"
    ).await.unwrap();
    
    // Insert test data
    client.simple_query(
        r#"INSERT INTO test_jsonb_delete (id, data) VALUES 
        (1, '{"name": "Alice", "age": 25, "email": "alice@example.com"}'),
        (2, '{"name": "Bob", "skills": ["coding", "testing", "debugging"], "active": true}');"#
    ).await.unwrap();
    
    // Test jsonb_delete with table data
    let rows = client.simple_query(
        "SELECT id, jsonb_delete(data, '{email}') AS updated_data FROM test_jsonb_delete WHERE id = 1"
    ).await.unwrap();
    
    let (id, updated_data) = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((row.get("id").unwrap(), row.get("updated_data").unwrap())),
            _ => None,
        })
        .expect("Expected to find a row");
    
    assert_eq!(id, "1");
    
    // Parse the result to verify the deletion
    let json_value: serde_json::Value = serde_json::from_str(&updated_data).unwrap();
    assert_eq!(json_value["name"], "Alice");
    assert_eq!(json_value["age"], 25);
    assert_eq!(json_value.get("email"), None); // Should be deleted
    
    // Test deleting from array in table data
    let rows = client.simple_query(
        "SELECT id, jsonb_delete(data, '{skills,1}') AS updated_data FROM test_jsonb_delete WHERE id = 2"
    ).await.unwrap();
    
    let (id, updated_data) = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((row.get("id").unwrap(), row.get("updated_data").unwrap())),
            _ => None,
        })
        .expect("Expected to find a row");
    
    assert_eq!(id, "2");
    
    // Parse the result to verify the array deletion
    let json_value: serde_json::Value = serde_json::from_str(&updated_data).unwrap();
    assert_eq!(json_value["name"], "Bob");
    assert_eq!(json_value["active"], true);
    let skills = json_value["skills"].as_array().unwrap();
    assert_eq!(skills.len(), 2);
    assert_eq!(skills[0], "coding");
    assert_eq!(skills[1], "debugging"); // "testing" was deleted
    
    server_handle.abort();
}