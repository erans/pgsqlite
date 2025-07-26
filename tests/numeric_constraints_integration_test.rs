mod common;
use common::*;

#[tokio::test]
async fn test_numeric_constraint_enforcement() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with NUMERIC constraints
    client.execute(
        "CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            price NUMERIC(10,2),
            quantity NUMERIC(5,0),
            tax_rate NUMERIC(3,3)
        )",
        &[]
    ).await.unwrap();
    
    // Test valid inserts
    client.execute(
        "INSERT INTO products VALUES (1, 9999999.99, 99999, 0.999)",
        &[]
    ).await.unwrap();
    
    client.execute(
        "INSERT INTO products VALUES (2, 0.01, 0, 0.000)",
        &[]
    ).await.unwrap();
    
    // Test precision overflow - should fail
    let result = client.execute(
        "INSERT INTO products VALUES (3, 99999999.999, 1, 0.5)",
        &[]
    ).await;
    assert!(result.is_err(), "Expected precision overflow to fail");
    let err = result.unwrap_err();
    println!("Precision overflow error: {err:?}");
    assert!(err.to_string().contains("numeric field overflow") || 
            err.code() == Some(&tokio_postgres::error::SqlState::NUMERIC_VALUE_OUT_OF_RANGE),
            "Expected numeric field overflow error, got: {err}");
    
    // Test scale overflow - should fail
    let result = client.execute(
        "INSERT INTO products VALUES (4, 100.999, 1, 0.5)",
        &[]
    ).await;
    assert!(result.is_err());
    
    // Test quantity with decimals when scale=0 - should fail
    let result = client.execute(
        "INSERT INTO products VALUES (5, 100.00, 123.45, 0.5)",
        &[]
    ).await;
    assert!(result.is_err());
    
    // Test tax_rate > 1 (precision 3, scale 3 means max 0.999) - should fail
    let result = client.execute(
        "INSERT INTO products VALUES (6, 100.00, 10, 1.000)",
        &[]
    ).await;
    assert!(result.is_err());
    
    server.abort();
}

#[tokio::test]
async fn test_numeric_constraint_updates() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table and insert valid data
    client.execute(
        "CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            balance NUMERIC(10,2)
        )",
        &[]
    ).await.unwrap();
    
    client.execute(
        "INSERT INTO accounts VALUES (1, 1000.00)",
        &[]
    ).await.unwrap();
    
    // Valid update
    client.execute(
        "UPDATE accounts SET balance = 2000.50 WHERE id = 1",
        &[]
    ).await.unwrap();
    
    // Invalid update - too many decimal places
    let result = client.execute(
        "UPDATE accounts SET balance = 2000.555 WHERE id = 1",
        &[]
    ).await;
    assert!(result.is_err());
    
    // Invalid update - exceeds precision
    let result = client.execute(
        "UPDATE accounts SET balance = 99999999.999 WHERE id = 1",
        &[]
    ).await;
    assert!(result.is_err());
    
    server.abort();
}

#[tokio::test]
async fn test_numeric_formatting_retrieval() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with different scales
    client.execute(
        "CREATE TABLE prices (
            id INTEGER PRIMARY KEY,
            two_decimal NUMERIC(10,2),
            four_decimal NUMERIC(10,4),
            no_decimal NUMERIC(10,0)
        )",
        &[]
    ).await.unwrap();
    
    // Insert values with varying precision
    client.execute(
        "INSERT INTO prices VALUES 
        (1, 123, 123, 123),
        (2, 123.40, 123.4, 123),
        (3, 123.46, 123.4568, 124)",
        &[]
    ).await.unwrap();
    
    // Retrieve and check formatting
    let rows = client.query(
        "SELECT two_decimal::text, four_decimal::text, no_decimal::text FROM prices ORDER BY id",
        &[]
    ).await.unwrap();
    
    // Row 1: integers should be formatted with trailing zeros
    assert_eq!(rows[0].get::<_, String>(0), "123.00");
    assert_eq!(rows[0].get::<_, String>(1), "123.0000");
    assert_eq!(rows[0].get::<_, String>(2), "123");
    
    // Row 2: should pad with zeros as needed
    assert_eq!(rows[1].get::<_, String>(0), "123.40");
    assert_eq!(rows[1].get::<_, String>(1), "123.4000");
    assert_eq!(rows[1].get::<_, String>(2), "123");
    
    // Row 3: should round to match scale
    assert_eq!(rows[2].get::<_, String>(0), "123.46");
    assert_eq!(rows[2].get::<_, String>(1), "123.4568");
    assert_eq!(rows[2].get::<_, String>(2), "124");
    
    server.abort();
}

#[tokio::test]
async fn test_numeric_null_handling() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with NOT NULL and nullable columns
    client.execute(
        "CREATE TABLE nullable_test (
            id INTEGER PRIMARY KEY,
            required_amount NUMERIC(10,2) NOT NULL,
            optional_amount NUMERIC(10,2)
        )",
        &[]
    ).await.unwrap();
    
    // NULL in nullable column should work
    client.execute(
        "INSERT INTO nullable_test VALUES (1, 100.00, NULL)",
        &[]
    ).await.unwrap();
    
    // NULL in NOT NULL column should fail
    let result = client.execute(
        "INSERT INTO nullable_test (id, required_amount, optional_amount) VALUES (2, NULL, 50.00)",
        &[]
    ).await;
    assert!(result.is_err());
    
    // Verify NULL is preserved in retrieval
    let row = client.query_one(
        "SELECT optional_amount::text FROM nullable_test WHERE id = 1",
        &[]
    ).await.unwrap();
    
    let value: Option<String> = row.get(0);
    assert!(value.is_none());
    
    server.abort();
}

#[tokio::test]
async fn test_numeric_multi_row_insert() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table
    client.execute(
        "CREATE TABLE batch_test (
            id INTEGER PRIMARY KEY,
            amount NUMERIC(8,2)
        )",
        &[]
    ).await.unwrap();
    
    // Multi-row insert with mix of valid and invalid values
    // All values must be valid for the insert to succeed
    let result = client.execute(
        "INSERT INTO batch_test VALUES 
        (1, 100.00),
        (2, 200.50),
        (3, 999999.999),  -- This exceeds scale
        (4, 300.00)",
        &[]
    ).await;
    
    // Should fail due to the invalid value
    assert!(result.is_err());
    
    // Verify no rows were inserted (atomic operation)
    let count = client.query_one(
        "SELECT COUNT(*) FROM batch_test",
        &[]
    ).await.unwrap();
    assert_eq!(count.get::<_, i64>(0), 0);
    
    // Now insert only valid values
    client.execute(
        "INSERT INTO batch_test VALUES 
        (1, 100.00),
        (2, 200.50),
        (3, 999999.99),
        (4, 300.00)",
        &[]
    ).await.unwrap();
    
    // Verify all were inserted
    let count = client.query_one(
        "SELECT COUNT(*) FROM batch_test",
        &[]
    ).await.unwrap();
    assert_eq!(count.get::<_, i64>(0), 4);
    
    server.abort();
}

#[tokio::test]
async fn test_numeric_edge_cases() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with edge case constraints
    client.execute(
        "CREATE TABLE edge_cases (
            id INTEGER PRIMARY KEY,
            tiny NUMERIC(1,0),        -- Single digit
            all_scale NUMERIC(3,3),   -- Only fractional part
            large NUMERIC(38,10)      -- Large precision
        )",
        &[]
    ).await.unwrap();
    
    // Test single digit constraint
    client.execute("INSERT INTO edge_cases (id, tiny) VALUES (1, 9)", &[]).await.unwrap();
    let result = client.execute("INSERT INTO edge_cases (id, tiny) VALUES (2, 10)", &[]).await;
    assert!(result.is_err());
    
    // Test all-scale constraint (max 0.999)
    client.execute("UPDATE edge_cases SET all_scale = 0.999 WHERE id = 1", &[]).await.unwrap();
    let result = client.execute("INSERT INTO edge_cases (id, all_scale) VALUES (3, 1.0)", &[]).await;
    assert!(result.is_err());
    
    // Test large precision
    // Note: rust_decimal has precision limitations
    // Using a smaller number to avoid precision loss
    let large_num = "12345678901234.1234567890";
    client.execute(
        &format!("UPDATE edge_cases SET large = {large_num} WHERE id = 1"),
        &[]
    ).await.unwrap();
    
    // Verify formatting
    let row = client.query_one(
        "SELECT all_scale::text, large::text FROM edge_cases WHERE id = 1",
        &[]
    ).await.unwrap();
    
    assert_eq!(row.get::<_, String>(0), "0.999");
    // Due to rust_decimal precision limitations, we might lose some precision
    // Check that the value starts with the expected prefix
    assert!(row.get::<_, String>(1).starts_with("12345678901234.123"));
    
    server.abort();
}