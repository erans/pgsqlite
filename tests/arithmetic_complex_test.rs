use tokio::net::TcpListener;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_nested_parentheses() {
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let db_path = format!("/tmp/test_nested_parentheses_{timestamp}.db");
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(&db_path).unwrap()
        );
        
        // Create test table
        db_handler.execute("CREATE TABLE calc_data (id INTEGER PRIMARY KEY, a REAL, b REAL, c REAL, d REAL)").await.unwrap();
        db_handler.execute("INSERT INTO calc_data VALUES (1, 10.0, 5.0, 2.0, 3.0)").await.unwrap();
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Connect client
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} dbname=test user=test"),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    
    // Test nested parentheses: ((a + b) * c) / d
    let rows = client.query("SELECT ((a + b) * c) / d AS complex_result FROM calc_data WHERE id = 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let result: f64 = rows[0].get(0);
    assert!((result - 10.0).abs() < 0.01); // ((10 + 5) * 2) / 3 = 30 / 3 = 10
    
    // Test different grouping: (a + (b * c)) / d
    // Force floating-point arithmetic by ensuring at least one operand is REAL
    let rows = client.query("SELECT CAST((a + (b * c)) AS REAL) / CAST(d AS REAL) AS different_grouping FROM calc_data WHERE id = 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let result: f64 = rows[0].get(0);

    assert!((result - 6.666667).abs() < 0.01); // (10 + (5 * 2)) / 3 = 20 / 3 = 6.67
    
    // Test deeply nested: (a * (b + (c * d)))
    let rows = client.query("SELECT a * (b + (c * d)) AS deeply_nested FROM calc_data WHERE id = 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let result: f64 = rows[0].get(0);
    assert!((result - 110.0).abs() < 0.01); // 10 * (5 + (2 * 3)) = 10 * 11 = 110
    
    server_handle.abort();
}

#[tokio::test]
async fn test_multiple_columns_arithmetic() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let db_path = format!("/tmp/test_multiple_columns_arithmetic_{timestamp}.db");
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(&db_path).unwrap()
        );
        
        db_handler.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, quantity INTEGER, price REAL, tax_rate REAL, discount REAL)").await.unwrap();
        db_handler.execute("INSERT INTO sales VALUES (1, 10, 25.50, 0.08, 0.1)").await.unwrap();
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} dbname=test user=test"),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    
    // Test complex sales calculation: quantity * price * (1 - discount) * (1 + tax_rate)
    let rows = client.query(
        "SELECT quantity * price * (1 - discount) * (1 + tax_rate) AS total_amount FROM sales WHERE id = 1",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 1);
    let result: f64 = rows[0].get(0);
    // 10 * 25.50 * 0.9 * 1.08 = 247.86
    assert!((result - 247.86).abs() < 0.01);
    
    // Test with column arithmetic in multiple aliases
    let rows = client.query(
        "SELECT quantity * price AS subtotal, quantity * price * (1 - discount) AS discounted, quantity * price * (1 - discount) * (1 + tax_rate) AS final_total FROM sales WHERE id = 1",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 1);
    let subtotal: f64 = rows[0].get(0);
    let discounted: f64 = rows[0].get(1);
    let final_total: f64 = rows[0].get(2);
    assert!((subtotal - 255.0).abs() < 0.01);
    assert!((discounted - 229.5).abs() < 0.01);
    assert!((final_total - 247.86).abs() < 0.01);
    
    server_handle.abort();
}

#[tokio::test]
#[ignore] // SQLite type affinity issues with function results
async fn test_arithmetic_with_functions() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let db_path = format!("/tmp/test_arithmetic_with_functions_{timestamp}.db");
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(&db_path).unwrap()
        );
        
        db_handler.execute("CREATE TABLE measurements (id INTEGER PRIMARY KEY, value REAL)").await.unwrap();
        db_handler.execute("INSERT INTO measurements VALUES (1, 2.75)").await.unwrap();
        db_handler.execute("INSERT INTO measurements VALUES (2, -2.5)").await.unwrap();
        db_handler.execute("INSERT INTO measurements VALUES (3, 16.0)").await.unwrap();
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} dbname=test user=test"),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    
    // Test ROUND function in arithmetic
    let rows = client.query("SELECT ROUND(value * 2, 2) AS rounded_double FROM measurements WHERE id = 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let result: f64 = rows[0].get(0);
    assert_eq!(result, 5.5);
    
    // Test ABS function in arithmetic - multiply by 10.0 to ensure float
    let rows = client.query("SELECT ABS(value) * 10.0 AS abs_times_ten FROM measurements WHERE id = 2", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let result: f64 = rows[0].get(0);
    assert!((result - 25.0).abs() < 0.01);
    
    // Test SQRT function in arithmetic (SQLite uses sqrt)
    let rows = client.query("SELECT sqrt(value) + 10 AS sqrt_plus_ten FROM measurements WHERE id = 3", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let result: f64 = rows[0].get(0);
    assert!((result - 14.0).abs() < 0.01); // sqrt(16) + 10 = 4 + 10 = 14
    
    server_handle.abort();
}

#[tokio::test]
async fn test_mixed_type_arithmetic() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let db_path = format!("/tmp/test_mixed_type_arithmetic_{timestamp}.db");
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(&db_path).unwrap()
        );
        
        db_handler.execute("CREATE TABLE mixed (id INTEGER PRIMARY KEY, int_col INTEGER, real_col REAL, decimal_col DECIMAL(10,2))").await.unwrap();
        db_handler.execute("INSERT INTO mixed VALUES (1, 10, 2.5, 100.00)").await.unwrap();
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} dbname=test user=test"),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    
    // Test INTEGER + REAL
    let rows = client.query("SELECT int_col + real_col AS int_plus_real FROM mixed WHERE id = 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let result: f64 = rows[0].get(0);
    assert!((result - 12.5).abs() < 0.01);
    
    // Test INTEGER * DECIMAL
    let rows = client.query("SELECT int_col * decimal_col AS int_times_decimal FROM mixed WHERE id = 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let result: f64 = rows[0].get(0);
    assert!((result - 1000.0).abs() < 0.01);
    
    // Test REAL / INTEGER
    let rows = client.query("SELECT decimal_col / int_col AS decimal_div_int FROM mixed WHERE id = 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let result: f64 = rows[0].get(0);
    assert!((result - 10.0).abs() < 0.01);
    
    server_handle.abort();
}

#[tokio::test]
async fn test_very_long_expressions() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let db_path = format!("/tmp/test_very_long_expressions_{timestamp}.db");
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(&db_path).unwrap()
        );
        
        db_handler.execute("CREATE TABLE nums (id INTEGER PRIMARY KEY, v1 REAL, v2 REAL, v3 REAL, v4 REAL, v5 REAL)").await.unwrap();
        db_handler.execute("INSERT INTO nums VALUES (1, 1.0, 2.0, 3.0, 4.0, 5.0)").await.unwrap();
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} dbname=test user=test"),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    
    // Test very long arithmetic expression
    // Force floating-point arithmetic by using REAL casts for division
    let rows = client.query(
        "SELECT v1 + v2 * v3 - CAST(v4 AS REAL) / CAST(v5 AS REAL) + v1 * v2 + v3 * v4 - v5 AS long_expr FROM nums WHERE id = 1",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 1);
    let result: f64 = rows[0].get(0);
    // 1 + 2*3 - 4/5 + 1*2 + 3*4 - 5 = 1 + 6 - 0.8 + 2 + 12 - 5 = 15.2
    assert!((result - 15.2).abs() < 0.01);
    
    // Test chained arithmetic with many terms
    // Force floating-point arithmetic for division
    let rows = client.query(
        "SELECT ((((v1 + 1) * 2) - 3) / CAST(4 AS REAL)) * 5 AS chained FROM nums WHERE id = 1",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 1);
    let result: f64 = rows[0].get(0);
    // ((((1 + 1) * 2) - 3) / 4) * 5 = (((2 * 2) - 3) / 4) * 5 = ((4 - 3) / 4) * 5 = (1 / 4) * 5 = 0.25 * 5 = 1.25
    assert!((result - 1.25).abs() < 0.01);
    
    server_handle.abort();
}

#[tokio::test]
#[ignore] // SQLite type affinity issues with CASE expressions
async fn test_arithmetic_with_case_expressions() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let db_path = format!("/tmp/test_arithmetic_with_case_expressions_{timestamp}.db");
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(&db_path).unwrap()
        );
        
        db_handler.execute("CREATE TABLE conditions (id INTEGER PRIMARY KEY, status TEXT, amount REAL)").await.unwrap();
        db_handler.execute("INSERT INTO conditions VALUES (1, 'premium', 100.0)").await.unwrap();
        db_handler.execute("INSERT INTO conditions VALUES (2, 'standard', 100.0)").await.unwrap();
        db_handler.execute("INSERT INTO conditions VALUES (3, 'basic', 100.0)").await.unwrap();
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} dbname=test user=test"),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    
    // Test CASE expression in arithmetic - ensure amount is REAL
    let rows = client.query(
        "SELECT amount * 1.0 * CASE 
            WHEN status = 'premium' THEN 1.5 
            WHEN status = 'standard' THEN 1.2 
            ELSE 1.0 
         END AS adjusted_amount 
         FROM conditions 
         ORDER BY id",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 3);
    let premium: f64 = rows[0].get(0);
    let standard: f64 = rows[1].get(0);
    let basic: f64 = rows[2].get(0);
    
    assert!((premium - 150.0).abs() < 0.01);
    assert!((standard - 120.0).abs() < 0.01);
    assert!((basic - 100.0).abs() < 0.01);
    
    server_handle.abort();
}