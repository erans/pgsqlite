mod common;
use common::*;

#[tokio::test]
async fn test_numeric_literal_validation() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with NUMERIC constraints
    client.execute(
        "CREATE TABLE prepared_test (
            id INTEGER PRIMARY KEY,
            amount NUMERIC(10,2),
            rate NUMERIC(5,4)
        )",
        &[]
    ).await.unwrap();
    
    // Test direct INSERT with literal values (our validator handles these)
    client.execute(
        "INSERT INTO prepared_test (id, amount, rate) VALUES (1, 123.45, 0.9999)",
        &[]
    ).await.unwrap();
    
    // Invalid precision - should fail
    let result = client.execute(
        "INSERT INTO prepared_test (id, amount, rate) VALUES (2, 99999999.999, 0.5)",
        &[]
    ).await;
    assert!(result.is_err(), "Expected precision overflow to fail");
    
    // Invalid scale - should fail
    let result = client.execute(
        "INSERT INTO prepared_test (id, amount, rate) VALUES (3, 100.00, 1.23456)",
        &[]
    ).await;
    assert!(result.is_err(), "Expected scale overflow to fail");
    
    // Test UPDATE with literal values
    client.execute(
        "UPDATE prepared_test SET amount = 999.99 WHERE id = 1",
        &[]
    ).await.unwrap();
    
    // Invalid update - should fail
    let result = client.execute(
        "UPDATE prepared_test SET amount = 999.999 WHERE id = 1",
        &[]
    ).await;
    assert!(result.is_err(), "Expected scale overflow in UPDATE to fail");
    
    server.abort();
}

#[tokio::test]
async fn test_numeric_transaction_behavior() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table
    client.execute(
        "CREATE TABLE tx_test (
            id INTEGER PRIMARY KEY,
            balance NUMERIC(10,2)
        )",
        &[]
    ).await.unwrap();
    
    // Insert initial data
    client.execute("INSERT INTO tx_test VALUES (1, 1000.00)", &[]).await.unwrap();
    
    // Test atomic behavior with batch insert
    let result = client.execute(
        "INSERT INTO tx_test VALUES 
         (2, 2000.00),
         (3, 3000.999),  -- This should fail
         (4, 4000.00)",
        &[]
    ).await;
    
    // Should fail due to constraint violation
    assert!(result.is_err());
    
    // Verify no partial insert occurred
    let count = client.query_one("SELECT COUNT(*) FROM tx_test", &[]).await.unwrap();
    assert_eq!(count.get::<_, i64>(0), 1); // Only the initial row
    
    server.abort();
}

#[tokio::test]
async fn test_numeric_error_messages() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table
    client.execute(
        "CREATE TABLE error_test (
            id INTEGER PRIMARY KEY,
            amount NUMERIC(5,2)
        )",
        &[]
    ).await.unwrap();
    
    // Test precision overflow error
    let result = client.execute(
        "INSERT INTO error_test VALUES (1, 9999.99)", // Max valid value is 999.99
        &[]
    ).await;
    
    assert!(result.is_err());
    let err = result.unwrap_err();
    
    // Check error code
    assert_eq!(err.code(), Some(&tokio_postgres::error::SqlState::NUMERIC_VALUE_OUT_OF_RANGE));
    
    // Check error contains column information
    let err_str = err.to_string();
    assert!(err_str.contains("numeric") || err_str.contains("NUMERIC"), 
            "Error should mention numeric type: {err_str}");
    
    server.abort();
}

#[tokio::test]
async fn test_numeric_mixed_operations() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table
    client.execute(
        "CREATE TABLE mixed_test (
            id INTEGER PRIMARY KEY,
            counter NUMERIC(5,0)
        )",
        &[]
    ).await.unwrap();
    
    // Test mixed valid and invalid operations
    
    // Valid insert
    client.execute("INSERT INTO mixed_test VALUES (0, 100)", &[]).await.unwrap();
    
    // Invalid insert - should fail
    let result = client.execute("INSERT INTO mixed_test VALUES (1, 100000)", &[]).await;
    assert!(result.is_err());
    
    // Valid insert
    client.execute("INSERT INTO mixed_test VALUES (2, 200)", &[]).await.unwrap();
    
    // Invalid insert - should fail
    let result = client.execute("INSERT INTO mixed_test VALUES (3, 100000)", &[]).await;
    assert!(result.is_err());
    
    // Valid insert
    client.execute("INSERT INTO mixed_test VALUES (4, 400)", &[]).await.unwrap();
    
    // Verify only valid inserts succeeded
    let count = client.query_one(
        "SELECT COUNT(*) FROM mixed_test WHERE counter <= 99999",
        &[]
    ).await.unwrap();
    assert_eq!(count.get::<_, i64>(0), 3); // Should have 3 valid rows
    
    server.abort();
}

#[tokio::test]
async fn test_numeric_with_computed_expressions() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create tables
    client.execute(
        "CREATE TABLE prices (
            id INTEGER PRIMARY KEY,
            base_price NUMERIC(10,2),
            tax_rate NUMERIC(3,2)
        )",
        &[]
    ).await.unwrap();
    
    client.execute(
        "CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            price_id INTEGER,
            quantity INTEGER,
            total NUMERIC(12,2)
        )",
        &[]
    ).await.unwrap();
    
    // Insert test data
    client.execute("INSERT INTO prices VALUES (1, 100.00, 0.15)", &[]).await.unwrap();
    
    // Test computed expression that should respect NUMERIC constraints
    // total = base_price * (1 + tax_rate) * quantity
    // For base=100, tax=0.15, qty=10: total = 100 * 1.15 * 10 = 1150.00
    let result = client.execute(
        "INSERT INTO orders 
         SELECT 1, 1, 10, p.base_price * (1 + p.tax_rate) * 10
         FROM prices p WHERE p.id = 1",
        &[]
    ).await;
    
    // Should succeed as 1150.00 fits in NUMERIC(12,2)
    assert!(result.is_ok());
    
    // Verify the computed value
    let row = client.query_one(
        "SELECT total::text FROM orders WHERE id = 1",
        &[]
    ).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "1150.00");
    
    // Test expression that would exceed constraints
    // Note: Our implementation validates literals at INSERT time, not computed expressions
    // This is a limitation - computed values are not validated against NUMERIC constraints
    client.execute(
        "INSERT INTO orders 
         SELECT 2, 1, 100000, p.base_price * (1 + p.tax_rate) * 100000
         FROM prices p WHERE p.id = 1",
        &[]
    ).await.unwrap();
    
    // The computed value may exceed constraints but SQLite will store it
    
    server.abort();
}

#[tokio::test]
async fn test_numeric_special_values() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table
    client.execute(
        "CREATE TABLE special_test (
            id INTEGER PRIMARY KEY,
            value NUMERIC(10,2)
        )",
        &[]
    ).await.unwrap();
    
    // Test various special numeric formats
    
    // Scientific notation
    let result = client.execute(
        "INSERT INTO special_test VALUES (1, 1.23e2)", // 123.00
        &[]
    ).await;
    assert!(result.is_ok());
    
    // Leading zeros
    client.execute(
        "INSERT INTO special_test VALUES (2, 00123.45)",
        &[]
    ).await.unwrap();
    
    // Positive sign
    client.execute(
        "INSERT INTO special_test VALUES (3, +456.78)",
        &[]
    ).await.unwrap();
    
    // Very small number
    client.execute(
        "INSERT INTO special_test VALUES (4, 0.01)",
        &[]
    ).await.unwrap();
    
    // Verify all values
    let rows = client.query(
        "SELECT id, value::text FROM special_test ORDER BY id",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows[0].get::<_, String>(1), "123.00");
    assert_eq!(rows[1].get::<_, String>(1), "123.45");
    assert_eq!(rows[2].get::<_, String>(1), "456.78");
    assert_eq!(rows[3].get::<_, String>(1), "0.01");
    
    server.abort();
}

#[tokio::test]
async fn test_numeric_with_default_values() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with DEFAULT values
    client.execute(
        "CREATE TABLE defaults_test (
            id INTEGER PRIMARY KEY,
            price NUMERIC(10,2) DEFAULT 99.99,
            quantity NUMERIC(5,0) DEFAULT 1
        )",
        &[]
    ).await.unwrap();
    
    // Insert using defaults
    client.execute(
        "INSERT INTO defaults_test (id) VALUES (1)",
        &[]
    ).await.unwrap();
    
    // Insert overriding one default
    client.execute(
        "INSERT INTO defaults_test (id, price) VALUES (2, 123.45)",
        &[]
    ).await.unwrap();
    
    // Verify defaults were applied
    let rows = client.query(
        "SELECT id, price::text, quantity::text FROM defaults_test ORDER BY id",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows[0].get::<_, String>(1), "99.99");
    assert_eq!(rows[0].get::<_, String>(2), "1");
    assert_eq!(rows[1].get::<_, String>(1), "123.45");
    assert_eq!(rows[1].get::<_, String>(2), "1");
    
    server.abort();
}

#[tokio::test]
async fn test_numeric_cast_operations() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table
    client.execute(
        "CREATE TABLE cast_test (
            id INTEGER PRIMARY KEY,
            int_val INTEGER,
            text_val TEXT,
            numeric_val NUMERIC(10,2)
        )",
        &[]
    ).await.unwrap();
    
    // Insert test data
    client.execute(
        "INSERT INTO cast_test VALUES (1, 123, '456.789', 789.01)",
        &[]
    ).await.unwrap();
    
    // Also insert a value that would exceed NUMERIC(10,2) when cast
    client.execute(
        "INSERT INTO cast_test VALUES (4, 999, '99999999.999', 100.00)",
        &[]
    ).await.unwrap();
    
    // Test casting from different types to NUMERIC
    client.execute(
        "INSERT INTO cast_test (id, numeric_val) 
         SELECT 2, CAST(int_val AS NUMERIC(10,2)) FROM cast_test WHERE id = 1",
        &[]
    ).await.unwrap();
    
    // Cast from text - PostgreSQL rounds the value
    client.execute(
        "INSERT INTO cast_test (id, numeric_val) 
         SELECT 3, CAST(text_val AS NUMERIC(10,2)) FROM cast_test WHERE id = 1",
        &[]
    ).await.unwrap();
    
    // Verify the value was rounded to 2 decimal places
    let row = client.query_one(
        "SELECT numeric_val::text FROM cast_test WHERE id = 3",
        &[]
    ).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "456.79");
    
    // Test CAST with precision overflow
    // Note: Our implementation validates literals at INSERT time, not computed values
    // This is a limitation compared to full PostgreSQL behavior
    client.execute(
        "INSERT INTO cast_test (id, numeric_val) 
         SELECT 5, CAST(text_val AS NUMERIC(10,2)) FROM cast_test WHERE id = 4",
        &[]
    ).await.unwrap();
    
    // The value gets stored but may be truncated/rounded by SQLite
    
    // Verify successful cast
    let row = client.query_one(
        "SELECT numeric_val::text FROM cast_test WHERE id = 2",
        &[]
    ).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "123.00");
    
    server.abort();
}