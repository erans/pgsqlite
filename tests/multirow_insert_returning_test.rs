use tokio::net::TcpListener;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_multirow_insert_returning() {
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        // Create test table
        db_handler.execute(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE
            )"
        ).await.unwrap();
        
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
    
    // Test 1: Multi-row INSERT with RETURNING - VALUES syntax
    let result = client.simple_query(
        "INSERT INTO users (name, email) VALUES 
         ('Alice', 'alice@example.com'),
         ('Bob', 'bob@example.com'),
         ('Charlie', 'charlie@example.com')
         RETURNING id, name"
    ).await.unwrap();
    
    let mut returned_rows = Vec::new();
    for msg in &result {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let id = row.get("id").unwrap();
            let name = row.get("name").unwrap();
            returned_rows.push((id.to_string(), name.to_string()));
            println!("Inserted user: id={}, name={}", id, name);
        }
    }
    
    assert_eq!(returned_rows.len(), 3, "Should return 3 rows");
    assert_eq!(returned_rows[0].1, "Alice");
    assert_eq!(returned_rows[1].1, "Bob");
    assert_eq!(returned_rows[2].1, "Charlie");
    
    // Test 2: INSERT SELECT with RETURNING
    // First create a source table
    client.execute(
        "CREATE TABLE temp_users (name TEXT, email TEXT)",
        &[]
    ).await.unwrap();
    
    client.execute(
        "INSERT INTO temp_users VALUES ('David', 'david@example.com'), ('Eve', 'eve@example.com')",
        &[]
    ).await.unwrap();
    
    // Now test INSERT SELECT with RETURNING
    let result = client.simple_query(
        "INSERT INTO users (name, email) 
         SELECT name, email FROM temp_users
         RETURNING id, name, email"
    ).await.unwrap();
    
    let mut select_returned_rows = Vec::new();
    for msg in &result {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let id = row.get("id").unwrap();
            let name = row.get("name").unwrap();
            let email = row.get("email").unwrap();
            select_returned_rows.push((id.to_string(), name.to_string(), email.to_string()));
            println!("Inserted via SELECT: id={}, name={}, email={}", id, name, email);
        }
    }
    
    assert_eq!(select_returned_rows.len(), 2, "Should return 2 rows from INSERT SELECT");
    assert_eq!(select_returned_rows[0].1, "David");
    assert_eq!(select_returned_rows[0].2, "david@example.com");
    assert_eq!(select_returned_rows[1].1, "Eve");
    assert_eq!(select_returned_rows[1].2, "eve@example.com");
    
    // Test 3: Verify all users are in the table
    let all_users = client.query("SELECT * FROM users ORDER BY id", &[]).await.unwrap();
    assert_eq!(all_users.len(), 5, "Should have 5 users total");
    
    println!("All multi-row INSERT RETURNING tests passed!");
    
    server_handle.abort();
}

#[tokio::test]
async fn test_sqlalchemy_style_insert_returning() {
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        // Create test table
        db_handler.execute(
            "CREATE TABLE categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT,
                created_at TEXT
            )"
        ).await.unwrap();
        
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
    
    // Test SQLAlchemy-style INSERT SELECT with RETURNING
    let result = client.simple_query(
        "INSERT INTO categories (name, description, created_at) 
         SELECT p0::VARCHAR, p1::TEXT, p2::TIMESTAMP WITHOUT TIME ZONE 
         FROM (VALUES 
           ('Technology', 'Posts about technology and programming', '2025-01-25 12:00:00'::timestamp, 0), 
           ('Lifestyle', 'Posts about lifestyle and personal development', '2025-01-25 12:00:01'::timestamp, 1)
         ) AS imp_sen(p0, p1, p2, sen_counter) 
         ORDER BY sen_counter 
         RETURNING categories.id, categories.id AS id__1"
    ).await.unwrap();
    
    let mut returned_rows = Vec::new();
    for msg in &result {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let id = row.get("id").unwrap();
            let id_alias = row.get("id__1").unwrap();
            returned_rows.push((id.to_string(), id_alias.to_string()));
            println!("Inserted category: id={}, id__1={}", id, id_alias);
        }
    }
    
    assert_eq!(returned_rows.len(), 2, "Should return 2 rows from SQLAlchemy-style INSERT");
    assert_eq!(returned_rows[0].0, returned_rows[0].1, "id and id__1 should match");
    assert_eq!(returned_rows[1].0, returned_rows[1].1, "id and id__1 should match");
    
    // Verify data was inserted correctly
    let categories = client.query("SELECT * FROM categories ORDER BY id", &[]).await.unwrap();
    assert_eq!(categories.len(), 2, "Should have 2 categories");
    
    println!("SQLAlchemy-style INSERT RETURNING test passed!");
    
    server_handle.abort();
}