use tokio::net::TcpListener;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_json_each_integration() {
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
    
    // Test json_each() with object
    let rows = client.query(
        "SELECT key, value FROM json_each('{\"name\": \"Alice\", \"age\": 30, \"active\": true}') AS t ORDER BY key",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 3);
    
    // Check first row (active)
    let key: String = rows[0].get("key");
    let value: String = rows[0].get("value");
    assert_eq!(key, "active");
    assert_eq!(value, "true");
    
    // Check second row (age)
    let key: String = rows[1].get("key");
    let value: String = rows[1].get("value");
    assert_eq!(key, "age");
    assert_eq!(value, "30");
    
    // Check third row (name)
    let key: String = rows[2].get("key");
    let value: String = rows[2].get("value");
    assert_eq!(key, "name");
    assert_eq!(value, "Alice");
    
    server_handle.abort();
}

#[tokio::test]
async fn test_jsonb_each_integration() {
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
    
    // Test jsonb_each() with object (should behave identically to json_each)
    let rows = client.query(
        "SELECT key, value FROM jsonb_each('{\"x\": 1, \"y\": 2}') AS t ORDER BY key",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 2);
    
    // Check first row (x)
    let key: String = rows[0].get("key");
    let value: String = rows[0].get("value");
    assert_eq!(key, "x");
    assert_eq!(value, "1");
    
    // Check second row (y)
    let key: String = rows[1].get("key");
    let value: String = rows[1].get("value");
    assert_eq!(key, "y");
    assert_eq!(value, "2");
    
    server_handle.abort();
}

#[tokio::test]
async fn test_json_each_with_array() {
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
    
    // Test json_each() with array
    let rows = client.query(
        r#"SELECT key, value FROM json_each('["apple", "banana", "cherry"]') AS t ORDER BY key"#,
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 3);
    
    // Check array indices as keys
    let key: String = rows[0].get("key");
    let value: String = rows[0].get("value");
    assert_eq!(key, "0");
    assert_eq!(value, "apple");
    
    let key: String = rows[1].get("key");
    let value: String = rows[1].get("value");
    assert_eq!(key, "1");
    assert_eq!(value, "banana");
    
    let key: String = rows[2].get("key");
    let value: String = rows[2].get("value");
    assert_eq!(key, "2");
    assert_eq!(value, "cherry");
    
    server_handle.abort();
}

#[tokio::test]
async fn test_json_each_with_table_data() {
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
        "CREATE TABLE test_json (
            id INTEGER PRIMARY KEY,
            data TEXT
        )"
    ).await.unwrap();
    
    // Insert test data
    client.simple_query(
        r#"INSERT INTO test_json (id, data) VALUES 
        (1, '{"name": "Alice", "age": 25}'),
        (2, '{"name": "Bob", "score": 95}')
        "#
    ).await.unwrap();
    
    // Test json_each() with table data
    let rows = client.query(
        "SELECT id, t.key, t.value FROM test_json, json_each(data) AS t WHERE id = 1 ORDER BY t.key",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 2);
    
    // Check first result
    let id: i32 = rows[0].get("id");
    let key: String = rows[0].get("key");
    let value: String = rows[0].get("value");
    assert_eq!(id, 1);
    assert_eq!(key, "age");
    assert_eq!(value, "25");
    
    // Check second result
    let id: i32 = rows[1].get("id");
    let key: String = rows[1].get("key");
    let value: String = rows[1].get("value");
    assert_eq!(id, 1);
    assert_eq!(key, "name");
    assert_eq!(value, "Alice");
    
    server_handle.abort();
}

#[tokio::test]
async fn test_json_each_empty_object() {
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
    
    // Test json_each() with empty object
    let rows = client.query(
        "SELECT key, value FROM json_each('{}') AS t",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 0);
    
    // Test json_each() with empty array
    let rows = client.query(
        "SELECT key, value FROM json_each('[]') AS t",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 0);
    
    server_handle.abort();
}