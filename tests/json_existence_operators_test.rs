use tokio::net::TcpListener;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_json_existence_operators() {
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
        "CREATE TABLE test_existence (
            id INTEGER PRIMARY KEY,
            data JSONB
        )"
    ).await.unwrap();
    
    // Insert test data
    client.simple_query(
        r#"INSERT INTO test_existence (id, data) VALUES 
        (1, '{"name": "Alice", "age": 30, "active": true}'),
        (2, '{"name": "Bob", "email": "bob@example.com", "role": "admin"}'),
        (3, '{"title": "Manager", "department": "IT", "clearance": "high"}'),
        (4, '{"items": ["laptop", "mouse"], "budget": 1000}')
        "#
    ).await.unwrap();
    
    // Test ? operator (key exists) - use direct function call to test
    let rows = client.query(
        "SELECT id FROM test_existence WHERE pgsqlite_json_has_key(data, 'name') ORDER BY id",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    
    // Test ? operator - key doesn't exist  
    let rows = client.query(
        "SELECT id FROM test_existence WHERE pgsqlite_json_has_key(data, 'missing_key')",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 0);
    
    // Test ?| operator (any key exists) - use direct function call
    let rows = client.query(
        "SELECT id FROM test_existence WHERE pgsqlite_json_has_any_key(data, 'email,age,department') ORDER BY id",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 3); // Records 1 (age), 2 (email), 3 (department)
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[2].get::<_, i32>(0), 3);
    
    // Test ?| operator - no keys exist
    let rows = client.query(
        "SELECT id FROM test_existence WHERE pgsqlite_json_has_any_key(data, 'missing1,missing2,missing3')",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 0);
    
    // Test ?& operator (all keys exist)
    let rows = client.query(
        "SELECT id FROM test_existence WHERE pgsqlite_json_has_all_keys(data, 'name,age')",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 1); // Only record 1 has both name and age
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    
    // Test ?& operator - missing one key
    let rows = client.query(
        "SELECT id FROM test_existence WHERE pgsqlite_json_has_all_keys(data, 'name,email')",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 1); // Record 2 has both name and email
    
    // Test combined operators
    let rows = client.query(
        "SELECT id FROM test_existence WHERE pgsqlite_json_has_key(data, 'name') AND pgsqlite_json_has_any_key(data, 'role,department') ORDER BY id",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 1); // Record 2 has 'name' and 'role'
    assert_eq!(rows[0].get::<_, i32>(0), 2);
    
    // Test with non-object JSON (should return false)
    client.simple_query(
        "INSERT INTO test_existence (id, data) VALUES (5, '[1, 2, 3]')"
    ).await.unwrap();
    
    let rows = client.query(
        "SELECT id FROM test_existence WHERE pgsqlite_json_has_key(data, 'name') ORDER BY id",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 2); // Still only records 1 and 2, not the array
    
    server_handle.abort();
}

#[tokio::test]
async fn test_json_existence_operators_with_table_alias() {
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
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            profile JSONB
        )"
    ).await.unwrap();
    
    // Insert test data
    client.simple_query(
        r#"INSERT INTO users (id, profile) VALUES 
        (1, '{"name": "Alice", "email": "alice@example.com"}'),
        (2, '{"username": "bob123", "phone": "555-1234"}')
        "#
    ).await.unwrap();
    
    // Test with table alias
    let rows = client.query(
        "SELECT u.id FROM users u WHERE pgsqlite_json_has_key(u.profile, 'email') ORDER BY u.id",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    
    // Test ?| with table alias
    let rows = client.query(
        "SELECT u.id FROM users u WHERE pgsqlite_json_has_any_key(u.profile, 'email,phone') ORDER BY u.id",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 2); // Both records have either email or phone
    
    server_handle.abort();
}