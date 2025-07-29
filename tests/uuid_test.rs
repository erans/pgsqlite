use tokio::net::TcpListener;
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
async fn test_uuid_support() {
    // Use a temporary file instead of in-memory database
    let test_id = Uuid::new_v4().to_string().replace("-", "");
    let db_path = format!("/tmp/pgsqlite_test_{}.db", test_id);
    let db_path_clone = db_path.clone();
    
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(&db_path_clone).unwrap()
        );
        
        // Create table with UUID column
        db_handler.execute(
            "CREATE TABLE users (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )"
        ).await.unwrap();
        
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
    
    // Test 1: Generate UUID with gen_random_uuid() using simple_query
    let result = client.simple_query("SELECT gen_random_uuid() as uuid").await.unwrap();
    let uuid1 = result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().to_string()),
            _ => None,
        })
        .expect("Expected to find a row");
    println!("Generated UUID 1: {uuid1}");
    assert_eq!(uuid1.len(), 36);
    assert!(uuid1.contains('-'));
    
    // Test 2: Generate another UUID - should be different
    let result = client.simple_query("SELECT gen_random_uuid() as uuid").await.unwrap();
    let uuid2 = result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().to_string()),
            _ => None,
        })
        .expect("Expected to find a row");
    println!("Generated UUID 2: {uuid2}");
    assert_ne!(uuid1, uuid2);
    
    // Test 3: Test uuid_generate_v4() function
    let result = client.simple_query("SELECT uuid_generate_v4() as uuid").await.unwrap();
    let uuid3 = result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().to_string()),
            _ => None,
        })
        .expect("Expected to find a row");
    println!("Generated UUID 3: {uuid3}");
    assert_eq!(uuid3.len(), 36);
    
    // Test 4: Validate UUID function
    let result = client.simple_query(&format!("SELECT is_valid_uuid('{uuid1}') as valid")).await.unwrap();
    let valid = result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap() == "1"),
            _ => None,
        })
        .expect("Expected to find a row");
    assert!(valid);
    
    let result = client.simple_query("SELECT is_valid_uuid('not-a-uuid') as valid").await.unwrap();
    let invalid = result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap() == "1"),
            _ => None,
        })
        .expect("Expected to find a row");
    assert!(!invalid);
    
    // Test 5: UUID normalization
    let upper_uuid = "550E8400-E29B-41D4-A716-446655440000";
    let result = client.simple_query(&format!("SELECT uuid_normalize('{upper_uuid}') as normalized")).await.unwrap();
    let normalized = result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().to_string()),
            _ => None,
        })
        .expect("Expected to find a row");
    assert_eq!(normalized, "550e8400-e29b-41d4-a716-446655440000");
    
    // Test 6: Insert and retrieve UUID
    client.simple_query(&format!(
        "INSERT INTO users (id, name) VALUES ('{uuid1}', 'Alice')"
    )).await.unwrap();
    
    let result = client.simple_query(&format!(
        "SELECT id, name FROM users WHERE id = '{uuid1}'"
    )).await.unwrap();
    result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                assert_eq!(row.get(0).unwrap(), uuid1);
                assert_eq!(row.get(1).unwrap(), "Alice");
                Some(())
            },
            _ => None,
        })
        .expect("Expected to find a row");
    
    // Test 7: Use gen_random_uuid() in INSERT
    client.simple_query(
        "INSERT INTO users (id, name) VALUES (gen_random_uuid(), 'Bob')"
    ).await.unwrap();
    
    let result = client.simple_query(
        "SELECT COUNT(*) as count FROM users"
    ).await.unwrap();
    let count = result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().parse::<i64>().unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    assert_eq!(count, 2);
    
    // Test 8: UUID comparison (case insensitive with collation)
    client.simple_query(
        "CREATE TABLE uuid_test (id TEXT COLLATE uuid)"
    ).await.unwrap();
    
    client.simple_query(&format!(
        "INSERT INTO uuid_test VALUES ('{}'), ('{}')",
        upper_uuid, "550e8400-e29b-41d4-a716-446655440000"
    )).await.unwrap();
    
    let result = client.simple_query(
        "SELECT COUNT(DISTINCT id) as count FROM uuid_test"
    ).await.unwrap();
    let count = result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().parse::<i64>().unwrap()),
            _ => None,
        })
        .expect("Expected to find a row");
    assert_eq!(count, 1);
    
    println!("All UUID tests passed!");
    
    server_handle.abort();
    
    // Clean up
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{}-journal", db_path));
    let _ = std::fs::remove_file(format!("{}-wal", db_path));
    let _ = std::fs::remove_file(format!("{}-shm", db_path));
}