use tokio::net::TcpListener;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_jsonb_pretty_simple_object_integration() {
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
    
    // Test pretty-printing a simple object
    let rows = client.simple_query(
        r#"SELECT jsonb_pretty('{"name":"John","age":30,"active":true}') AS result"#
    ).await.unwrap();
    
    let result = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get("result").unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    
    // Verify the output is pretty-printed
    assert!(result.contains("{\n"));
    assert!(result.contains("  \"name\": \"John\""));
    assert!(result.contains("  \"age\": 30"));
    assert!(result.contains("  \"active\": true"));
    assert!(result.contains("\n}"));
    
    server_handle.abort();
}

#[tokio::test]
async fn test_jsonb_pretty_nested_structure_integration() {
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
    
    // Test pretty-printing nested structure
    let rows = client.simple_query(
        r#"SELECT jsonb_pretty('{"user":{"name":"Alice","contacts":{"email":"alice@example.com","phone":"555-1234"}},"tags":["vip","premium"]}') AS result"#
    ).await.unwrap();
    
    let result = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get("result").unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    
    // Verify nested structure is properly indented
    assert!(result.contains("  \"user\": {"));
    assert!(result.contains("    \"name\": \"Alice\""));
    assert!(result.contains("    \"contacts\": {"));
    assert!(result.contains("      \"email\": \"alice@example.com\""));
    assert!(result.contains("      \"phone\": \"555-1234\""));
    assert!(result.contains("  \"tags\": ["));
    assert!(result.contains("    \"vip\","));
    assert!(result.contains("    \"premium\""));
    
    server_handle.abort();
}

#[tokio::test]
async fn test_jsonb_pretty_array_of_objects_integration() {
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
    
    // Test pretty-printing array of objects
    let rows = client.simple_query(
        r#"SELECT jsonb_pretty('[{"id":1,"name":"Item 1"},{"id":2,"name":"Item 2"},{"id":3,"name":"Item 3"}]') AS result"#
    ).await.unwrap();
    
    let result = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get("result").unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    
    // Verify array formatting
    assert!(result.contains("[\n"));
    assert!(result.contains("  {\n"));
    assert!(result.contains("    \"id\": 1,"));
    assert!(result.contains("    \"name\": \"Item 1\""));
    assert!(result.contains("  },"));
    assert!(result.contains("    \"id\": 2,"));
    assert!(result.contains("    \"name\": \"Item 2\""));
    assert!(result.contains("    \"id\": 3,"));
    assert!(result.contains("    \"name\": \"Item 3\""));
    assert!(result.contains("\n]"));
    
    server_handle.abort();
}

#[tokio::test]
async fn test_jsonb_pretty_with_table_data_integration() {
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
        "CREATE TABLE test_jsonb_pretty (
            id INTEGER PRIMARY KEY,
            data TEXT
        )"
    ).await.unwrap();
    
    // Insert test data with compact JSON
    client.simple_query(
        r#"INSERT INTO test_jsonb_pretty (id, data) VALUES 
        (1, '{"product":{"name":"Widget","price":19.99,"specs":{"weight":"100g","dimensions":[10,5,2]}},"inStock":true}'),
        (2, '["compact","array","without","formatting"]')"#
    ).await.unwrap();
    
    // Test pretty-printing table data
    let rows = client.simple_query(
        "SELECT id, jsonb_pretty(data) AS pretty_data FROM test_jsonb_pretty WHERE id = 1"
    ).await.unwrap();
    
    let (id, pretty_data) = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => 
                Some((row.get("id").unwrap(), row.get("pretty_data").unwrap())),
            _ => None,
        })
        .expect("Expected to find a row");
    
    assert_eq!(id, "1");
    
    // Verify the complex JSON is properly formatted
    assert!(pretty_data.contains("  \"product\": {"));
    assert!(pretty_data.contains("    \"name\": \"Widget\""));
    assert!(pretty_data.contains("    \"price\": 19.99"));
    assert!(pretty_data.contains("    \"specs\": {"));
    assert!(pretty_data.contains("      \"weight\": \"100g\""));
    assert!(pretty_data.contains("      \"dimensions\": ["));
    assert!(pretty_data.contains("        10,"));
    assert!(pretty_data.contains("        5,"));
    assert!(pretty_data.contains("        2"));
    assert!(pretty_data.contains("  \"inStock\": true"));
    
    // Test pretty-printing array from table
    let rows = client.simple_query(
        "SELECT id, jsonb_pretty(data) AS pretty_data FROM test_jsonb_pretty WHERE id = 2"
    ).await.unwrap();
    
    let (id, pretty_data) = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => 
                Some((row.get("id").unwrap(), row.get("pretty_data").unwrap())),
            _ => None,
        })
        .expect("Expected to find a row");
    
    assert_eq!(id, "2");
    
    // Verify array is formatted
    assert!(pretty_data.contains("[\n"));
    assert!(pretty_data.contains("  \"compact\","));
    assert!(pretty_data.contains("  \"array\","));
    assert!(pretty_data.contains("  \"without\","));
    assert!(pretty_data.contains("  \"formatting\""));
    assert!(pretty_data.contains("\n]"));
    
    server_handle.abort();
}

#[tokio::test]
async fn test_jsonb_pretty_edge_cases_integration() {
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
    
    // Test empty object
    let rows = client.simple_query(
        "SELECT jsonb_pretty('{}') AS result"
    ).await.unwrap();
    
    let result = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get("result").unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    
    assert_eq!(result, "{}");
    
    // Test empty array
    let rows = client.simple_query(
        "SELECT jsonb_pretty('[]') AS result"
    ).await.unwrap();
    
    let result = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get("result").unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    
    assert_eq!(result, "[]");
    
    // Test simple string
    let rows = client.simple_query(
        r#"SELECT jsonb_pretty('"hello world"') AS result"#
    ).await.unwrap();
    
    let result = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get("result").unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    
    assert_eq!(result, "\"hello world\"");
    
    // Test number
    let rows = client.simple_query(
        "SELECT jsonb_pretty('42') AS result"
    ).await.unwrap();
    
    let result = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get("result").unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    
    assert_eq!(result, "42");
    
    // Test null
    let rows = client.simple_query(
        "SELECT jsonb_pretty('null') AS result"
    ).await.unwrap();
    
    let result = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get("result").unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    
    assert_eq!(result, "null");
    
    // Test invalid JSON (should return original)
    let rows = client.simple_query(
        "SELECT jsonb_pretty('{not valid json}') AS result"
    ).await.unwrap();
    
    let result = rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get("result").unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    
    assert_eq!(result, "{not valid json}");
    
    server_handle.abort();
}