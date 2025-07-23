use tokio::net::TcpListener;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_json_agg_integration() {
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
        "CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name TEXT,
            department TEXT,
            salary INTEGER
        )"
    ).await.unwrap();
    
    // Insert test data
    client.simple_query(
        r#"INSERT INTO employees (id, name, department, salary) VALUES 
        (1, 'Alice', 'Engineering', 95000),
        (2, 'Bob', 'Engineering', 87000),
        (3, 'Charlie', 'Sales', 75000),
        (4, 'Diana', 'Sales', 82000),
        (5, 'Eve', 'Marketing', 70000)
        "#
    ).await.unwrap();
    
    // Test json_agg with simple values
    let rows = client.query(
        "SELECT json_agg(name) AS names FROM employees",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 1);
    let json_result: String = rows[0].get("names");
    
    // Parse and verify it's a valid JSON array
    let parsed: serde_json::Value = serde_json::from_str(&json_result).unwrap();
    match parsed {
        serde_json::Value::Array(arr) => {
            assert_eq!(arr.len(), 5);
            // Check that all names are present (order might vary)
            let names: Vec<String> = arr.iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect();
            assert!(names.contains(&"Alice".to_string()));
            assert!(names.contains(&"Bob".to_string()));
            assert!(names.contains(&"Charlie".to_string()));
            assert!(names.contains(&"Diana".to_string()));
            assert!(names.contains(&"Eve".to_string()));
        }
        _ => panic!("Expected JSON array"),
    }
    
    // Test json_agg with numbers
    let rows = client.query(
        "SELECT json_agg(salary) AS salaries FROM employees WHERE department = 'Engineering'",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 1);
    let json_result: String = rows[0].get("salaries");
    let parsed: serde_json::Value = serde_json::from_str(&json_result).unwrap();
    
    match parsed {
        serde_json::Value::Array(arr) => {
            assert_eq!(arr.len(), 2);
            let salaries: Vec<i64> = arr.iter()
                .map(|v| v.as_i64().unwrap())
                .collect();
            assert!(salaries.contains(&95000));
            assert!(salaries.contains(&87000));
        }
        _ => panic!("Expected JSON array"),
    }
    
    // Test jsonb_agg (should behave identically)
    let rows = client.query(
        "SELECT jsonb_agg(name) AS names FROM employees WHERE department = 'Sales'",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 1);
    let json_result: String = rows[0].get("names");
    let parsed: serde_json::Value = serde_json::from_str(&json_result).unwrap();
    
    match parsed {
        serde_json::Value::Array(arr) => {
            assert_eq!(arr.len(), 2);
            let names: Vec<String> = arr.iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect();
            assert!(names.contains(&"Charlie".to_string()));
            assert!(names.contains(&"Diana".to_string()));
        }
        _ => panic!("Expected JSON array"),
    }
    
    // Test empty result
    let rows = client.query(
        "SELECT json_agg(name) AS names FROM employees WHERE department = 'NonExistent'",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 1);
    let json_result: String = rows[0].get("names");
    assert_eq!(json_result, "[]");
    
    // Test with GROUP BY
    let rows = client.query(
        "SELECT department, json_agg(name) AS names FROM employees GROUP BY department ORDER BY department",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 3);
    
    // Check Engineering department
    let eng_row = &rows[0];
    let dept: String = eng_row.get("department");
    let names: String = eng_row.get("names");
    assert_eq!(dept, "Engineering");
    
    let parsed: serde_json::Value = serde_json::from_str(&names).unwrap();
    match parsed {
        serde_json::Value::Array(arr) => {
            assert_eq!(arr.len(), 2);
        }
        _ => panic!("Expected JSON array"),
    }
    
    server_handle.abort();
}

#[tokio::test]
async fn test_json_agg_with_nulls() {
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
    
    // Create test table with nullable column
    client.simple_query(
        "CREATE TABLE test_nulls (
            id INTEGER PRIMARY KEY,
            value TEXT
        )"
    ).await.unwrap();
    
    // Insert test data with NULLs
    client.simple_query(
        r#"INSERT INTO test_nulls (id, value) VALUES 
        (1, 'A'),
        (2, NULL),
        (3, 'B'),
        (4, NULL)
        "#
    ).await.unwrap();
    
    // Test json_agg with NULL values
    let rows = client.query(
        "SELECT json_agg(value) AS value_list FROM test_nulls ORDER BY id",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 1);
    let json_result: String = rows[0].get("value_list");
    let parsed: serde_json::Value = serde_json::from_str(&json_result).unwrap();
    
    match parsed {
        serde_json::Value::Array(arr) => {
            assert_eq!(arr.len(), 4);
            // Check that NULL values are properly represented
            assert_eq!(arr[0], serde_json::Value::String("A".to_string()));
            assert_eq!(arr[1], serde_json::Value::Null);
            assert_eq!(arr[2], serde_json::Value::String("B".to_string()));
            assert_eq!(arr[3], serde_json::Value::Null);
        }
        _ => panic!("Expected JSON array"),
    }
    
    server_handle.abort();
}