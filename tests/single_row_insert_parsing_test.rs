mod common;
use common::*;

#[tokio::test]
async fn test_single_row_insert_with_semicolon() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table
    client.execute(
        "CREATE TABLE test_insert (
            id INTEGER PRIMARY KEY,
            text_col TEXT,
            date_col DATE
        )",
        &[]
    ).await.unwrap();
    
    // Test single-row INSERT with semicolon (as it appears in test_queries.sql)
    let result = client.simple_query(
        "INSERT INTO test_insert (text_col, date_col) VALUES ('Test 1', '2025-01-01');"
    ).await;
    
    assert!(result.is_ok(), "Should handle INSERT with trailing semicolon");
    
    // Verify row was inserted
    let row = client.query_one("SELECT text_col, date_col FROM test_insert WHERE text_col = 'Test 1'", &[]).await.unwrap();
    let text: &str = row.get(0);
    assert_eq!(text, "Test 1");
    
    // Also test without semicolon
    let result2 = client.simple_query(
        "INSERT INTO test_insert (text_col, date_col) VALUES ('Test 2', '2025-01-02')"
    ).await;
    
    assert!(result2.is_ok(), "Should handle INSERT without trailing semicolon");
    
    server.abort();
}

#[tokio::test]
async fn test_single_row_insert_edge_cases() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table
    client.execute(
        "CREATE TABLE edge_test (
            id INTEGER PRIMARY KEY,
            text_col TEXT,
            date_col DATE
        )",
        &[]
    ).await.unwrap();
    
    // Test with spaces before semicolon
    client.simple_query(
        "INSERT INTO edge_test (text_col, date_col) VALUES ('Test 1', '2025-01-01')  ;"
    ).await.unwrap();
    
    // Test with newline before semicolon
    client.simple_query(
        "INSERT INTO edge_test (text_col, date_col) VALUES ('Test 2', '2025-01-02')\n;"
    ).await.unwrap();
    
    // Test with parenthesis in text value
    client.simple_query(
        "INSERT INTO edge_test (text_col, date_col) VALUES ('Test (with parens)', '2025-01-03');"
    ).await.unwrap();
    
    // Verify all rows were inserted
    let count = client.query_one("SELECT COUNT(*) FROM edge_test", &[]).await.unwrap();
    let count_val: i64 = count.get(0);
    assert_eq!(count_val, 3, "Should have inserted 3 rows");
    
    server.abort();
}