use tokio::net::TcpListener;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_json_each_text_basic() {
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
    
    // Test json_each_text with basic types
    let rows = client.query(
        r#"SELECT key, value FROM json_each_text('{"name": "Alice", "age": 30, "active": true, "score": null}') AS t ORDER BY key"#,
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 4);
    
    // Check that all values are text
    assert_eq!(rows[0].get::<_, &str>("key"), "active");
    assert_eq!(rows[0].get::<_, &str>("value"), "true"); // boolean as text
    
    assert_eq!(rows[1].get::<_, &str>("key"), "age");
    assert_eq!(rows[1].get::<_, &str>("value"), "30"); // number as text
    
    assert_eq!(rows[2].get::<_, &str>("key"), "name");
    assert_eq!(rows[2].get::<_, &str>("value"), "Alice"); // string remains string
    
    assert_eq!(rows[3].get::<_, &str>("key"), "score");
    assert_eq!(rows[3].get::<_, &str>("value"), "null"); // null as text
    
    server_handle.abort();
}

#[tokio::test]
async fn test_jsonb_each_text_with_nested() {
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
    
    // Test jsonb_each_text with nested structures - they should be returned as JSON strings
    let rows = client.query(
        r#"SELECT key, value FROM jsonb_each_text('{"items": [1, 2, 3], "meta": {"version": 1.0}}') AS t ORDER BY key"#,
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 2);
    
    // Arrays and objects should be returned as JSON strings
    assert_eq!(rows[0].get::<_, &str>("key"), "items");
    assert_eq!(rows[0].get::<_, &str>("value"), "[1,2,3]"); // array as JSON string
    
    assert_eq!(rows[1].get::<_, &str>("key"), "meta");
    assert_eq!(rows[1].get::<_, &str>("value"), "{\"version\":1.0}"); // object as JSON string
    
    server_handle.abort();
}

#[tokio::test]
async fn test_json_each_text_with_table() {
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
    
    // Create test table
    client.simple_query(
        "CREATE TABLE test_data (
            id INTEGER PRIMARY KEY,
            data JSONB
        )"
    ).await.unwrap();
    
    // Insert test data
    client.simple_query(
        r#"INSERT INTO test_data (id, data) VALUES 
        (1, '{"quantity": 10, "price": 29.99, "inStock": true}'),
        (2, '{"quantity": 0, "price": 15.50, "inStock": false}')"#
    ).await.unwrap();
    
    // Test json_each_text with table data
    // For cross joins, we need to manually cast the value to text
    let rows = client.query(
        "SELECT td.id, e.key, json_each_text_value(td.data, e.key) AS value 
         FROM test_data td,
              json_each_text(td.data) AS e
         WHERE e.key = 'inStock'
         ORDER BY td.id",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 2);
    
    // Both boolean values should be returned as text
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, &str>(1), "inStock");
    assert_eq!(rows[0].get::<_, &str>(2), "true");
    
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, &str>(1), "inStock");
    assert_eq!(rows[1].get::<_, &str>(2), "false");
    
    server_handle.abort();
}

#[tokio::test]
async fn test_json_each_text_with_filter() {
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
    
    // Test filtering on text values
    let rows = client.query(
        r#"SELECT key, value 
           FROM json_each_text('{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}') AS t
           WHERE CAST(value AS INTEGER) > 2
           ORDER BY key"#,
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 3);
    
    assert_eq!(rows[0].get::<_, &str>("key"), "c");
    assert_eq!(rows[0].get::<_, &str>("value"), "3");
    
    assert_eq!(rows[1].get::<_, &str>("key"), "d");
    assert_eq!(rows[1].get::<_, &str>("value"), "4");
    
    assert_eq!(rows[2].get::<_, &str>("key"), "e");
    assert_eq!(rows[2].get::<_, &str>("value"), "5");
    
    server_handle.abort();
}

#[tokio::test]
async fn test_json_each_text_empty_and_null() {
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
    let rows = client.query(
        r#"SELECT key, value FROM json_each_text('{}') AS t"#,
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 0);
    
    // Test empty array (should return no rows since arrays don't have string keys)
    let rows = client.query(
        r#"SELECT key, value FROM json_each_text('[]') AS t"#,
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 0);
    
    server_handle.abort();
}