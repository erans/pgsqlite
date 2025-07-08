mod common;
use common::*;

#[tokio::test]
async fn test_multirow_insert_with_dates() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table
    client.execute(
        "CREATE TABLE orders (
            order_id SERIAL PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            order_date DATE,
            total_amount NUMERIC(10,2)
        )",
        &[]
    ).await.unwrap();
    
    // Multi-row INSERT with date values
    let result = client.simple_query(
        "INSERT INTO orders (customer_id, product_id, quantity, order_date, total_amount) VALUES
            (1, 1, 1, '2025-01-01', 999.99),
            (1, 2, 2, '2025-01-01', 59.98),
            (2, 3, 1, '2025-01-02', 79.99)"
    ).await;
    
    // Check if INSERT succeeded
    assert!(result.is_ok(), "Multi-row INSERT should succeed: {:?}", result.err());
    
    // Verify data was inserted correctly
    let check = client.query(
        "SELECT customer_id, order_date FROM orders ORDER BY customer_id, product_id",
        &[]
    ).await.unwrap();
    
    assert_eq!(check.len(), 3, "Should have 3 rows");
    
    // Check that dates are properly stored and retrieved
    let date1: chrono::NaiveDate = check[0].get(1);
    let date2: chrono::NaiveDate = check[2].get(1);
    
    assert_eq!(date1.to_string(), "2025-01-01");
    assert_eq!(date2.to_string(), "2025-01-02");
    
    server.abort();
}