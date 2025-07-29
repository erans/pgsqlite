use tokio::net::TcpListener;
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
async fn test_returning_clause() {
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
        
        // Create test table
        db_handler.execute(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE,
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
    
    // Test 1: INSERT with RETURNING *
    let result = client.simple_query(
        "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com') RETURNING *"
    ).await.unwrap();
    
    let returned_row = result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("Expected to find a returned row");
    
    assert_eq!(returned_row.get("name").unwrap(), "Alice");
    assert_eq!(returned_row.get("email").unwrap(), "alice@example.com");
    let user_id = returned_row.get("id").unwrap();
    println!("Inserted user with id: {user_id}");
    
    // Test 2: INSERT with RETURNING specific columns
    let result = client.simple_query(
        "INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com') RETURNING id, name"
    ).await.unwrap();
    
    let returned_row = result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("Expected to find a returned row");
    
    assert_eq!(returned_row.get("name").unwrap(), "Bob");
    let bob_id = returned_row.get("id").unwrap();
    println!("Inserted Bob with id: {bob_id}");
    
    // Test 3: UPDATE with RETURNING
    let result = client.simple_query(
        &format!("UPDATE users SET email = 'alice.smith@example.com' WHERE id = {user_id} RETURNING id, name, email")
    ).await.unwrap();
    
    let returned_row = result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("Expected to find a returned row");
    
    assert_eq!(returned_row.get("id").unwrap(), user_id);
    assert_eq!(returned_row.get("name").unwrap(), "Alice");
    assert_eq!(returned_row.get("email").unwrap(), "alice.smith@example.com");
    
    // Test 4: DELETE with RETURNING
    let result = client.simple_query(
        &format!("DELETE FROM users WHERE id = {bob_id} RETURNING name, email")
    ).await.unwrap();
    
    let returned_row = result.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("Expected to find a returned row");
    
    assert_eq!(returned_row.get("name").unwrap(), "Bob");
    assert_eq!(returned_row.get("email").unwrap(), "bob@example.com");
    
    // Test 5: Multiple row UPDATE with RETURNING
    // First insert more users
    client.simple_query(
        "INSERT INTO users (name, email) VALUES 
         ('Charlie', 'charlie@example.com'),
         ('David', 'david@example.com'),
         ('Eve', 'eve@example.com')"
    ).await.unwrap();
    
    let result = client.simple_query(
        "UPDATE users SET email = name || '@updated.com' WHERE name LIKE '%e' RETURNING name, email"
    ).await.unwrap();
    
    let mut updated_count = 0;
    for msg in &result {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            updated_count += 1;
            let name = row.get("name").unwrap();
            let email = row.get("email").unwrap();
            println!("Updated: {name} -> {email}");
            assert!(email.ends_with("@updated.com"));
        }
    }
    assert_eq!(updated_count, 3); // Alice, Charlie, Eve
    
    // Test 6: DELETE with no matches and RETURNING
    let result = client.simple_query(
        "DELETE FROM users WHERE name = 'NonExistent' RETURNING *"
    ).await.unwrap();
    
    let has_rows = result.iter().any(|msg| matches!(msg, tokio_postgres::SimpleQueryMessage::Row(_)));
    assert!(!has_rows, "Should not return any rows for non-existent user");
    
    println!("All RETURNING tests passed!");
    
    server_handle.abort();

    // Clean up
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{}-journal", db_path));
    let _ = std::fs::remove_file(format!("{}-wal", db_path));
    let _ = std::fs::remove_file(format!("{}-shm", db_path));
}