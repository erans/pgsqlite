use tokio::net::TcpListener;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_jsonb_insert_object_integration() {
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
    
    // Test inserting into JSON object
    let rows = client.simple_query(
        "SELECT jsonb_insert('{\"name\": \"John\", \"age\": 30}', '{email}', '\"john@example.com\"') AS result"
    ).await.unwrap();
    
    let result = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get("result").unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    
    // Parse the result to verify the insertion
    let json_value: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(json_value["name"], "John");
    assert_eq!(json_value["age"], 30);
    assert_eq!(json_value["email"], "john@example.com");
    
    server_handle.abort();
}

#[tokio::test]
async fn test_jsonb_insert_nested_object_integration() {
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
    
    // Test inserting into nested JSON object
    let rows = client.simple_query(
        "SELECT jsonb_insert('{\"user\": {\"name\": \"Alice\"}, \"active\": true}', '{user,email}', '\"alice@example.com\"') AS result"
    ).await.unwrap();
    
    let result = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get("result").unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    
    // Parse the result to verify the insertion
    let json_value: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(json_value["user"]["name"], "Alice");
    assert_eq!(json_value["user"]["email"], "alice@example.com");
    assert_eq!(json_value["active"], true);
    
    server_handle.abort();
}

#[tokio::test]
async fn test_jsonb_insert_array_integration() {
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
    
    // Test inserting into JSON array (before index 1)
    let rows = client.simple_query(
        "SELECT jsonb_insert('[\"apple\", \"banana\", \"cherry\"]', '{1}', '\"orange\"') AS result"
    ).await.unwrap();
    
    let result = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get("result").unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    
    // Parse the result to verify the insertion
    let json_value: serde_json::Value = serde_json::from_str(&result).unwrap();
    let array = json_value.as_array().unwrap();
    assert_eq!(array.len(), 4);
    assert_eq!(array[0], "apple");
    assert_eq!(array[1], "orange");
    assert_eq!(array[2], "banana");
    assert_eq!(array[3], "cherry");
    
    server_handle.abort();
}

#[tokio::test]
async fn test_jsonb_insert_array_after_integration() {
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
    
    // Test inserting into JSON array (after index 1)
    let rows = client.simple_query(
        "SELECT jsonb_insert('[\"apple\", \"banana\", \"cherry\"]', '{1}', '\"orange\"', true) AS result"
    ).await.unwrap();
    
    let result = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get("result").unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    
    // Parse the result to verify the insertion
    let json_value: serde_json::Value = serde_json::from_str(&result).unwrap();
    let array = json_value.as_array().unwrap();
    assert_eq!(array.len(), 4);
    assert_eq!(array[0], "apple");
    assert_eq!(array[1], "banana");
    assert_eq!(array[2], "orange");
    assert_eq!(array[3], "cherry");
    
    server_handle.abort();
}

#[tokio::test]
async fn test_jsonb_insert_existing_key_integration() {
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
    
    // Test inserting with existing key (should not modify)
    let rows = client.simple_query(
        "SELECT jsonb_insert('{\"name\": \"John\", \"age\": 30}', '{name}', '\"Jane\"') AS result"
    ).await.unwrap();
    
    let result = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get("result").unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    
    // Parse the result to verify no change
    let json_value: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(json_value["name"], "John"); // Should still be John
    assert_eq!(json_value["age"], 30);
    
    server_handle.abort();
}

#[tokio::test]
async fn test_jsonb_insert_with_table_data_integration() {
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
        "CREATE TABLE test_jsonb_insert (
            id INTEGER PRIMARY KEY,
            data TEXT
        )"
    ).await.unwrap();
    
    // Insert test data
    client.simple_query(
        r#"INSERT INTO test_jsonb_insert (id, data) VALUES 
        (1, '{"name": "Alice", "age": 25}'),
        (2, '{"name": "Bob", "skills": ["coding", "testing"]}')"#
    ).await.unwrap();
    
    // Test jsonb_insert with table data
    let rows = client.simple_query(
        "SELECT id, jsonb_insert(data, '{email}', '\"user@example.com\"') AS updated_data FROM test_jsonb_insert WHERE id = 1"
    ).await.unwrap();
    
    let (id, updated_data) = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((row.get("id").unwrap(), row.get("updated_data").unwrap())),
            _ => None,
        })
        .expect("Expected to find a row");
    
    assert_eq!(id, "1");
    
    // Parse the result to verify the insertion
    let json_value: serde_json::Value = serde_json::from_str(&updated_data).unwrap();
    assert_eq!(json_value["name"], "Alice");
    assert_eq!(json_value["age"], 25);
    assert_eq!(json_value["email"], "user@example.com");
    
    // Test inserting into array in table data
    let rows = client.simple_query(
        "SELECT id, jsonb_insert(data, '{skills,1}', '\"debugging\"') AS updated_data FROM test_jsonb_insert WHERE id = 2"
    ).await.unwrap();
    
    let (id, updated_data) = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((row.get("id").unwrap(), row.get("updated_data").unwrap())),
            _ => None,
        })
        .expect("Expected to find a row");
    
    assert_eq!(id, "2");
    
    // Parse the result to verify the array insertion
    let json_value: serde_json::Value = serde_json::from_str(&updated_data).unwrap();
    assert_eq!(json_value["name"], "Bob");
    let skills = json_value["skills"].as_array().unwrap();
    assert_eq!(skills.len(), 3);
    assert_eq!(skills[0], "coding");
    assert_eq!(skills[1], "debugging");
    assert_eq!(skills[2], "testing");
    
    server_handle.abort();
}