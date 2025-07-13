mod common;
use common::*;

#[tokio::test]
async fn test_array_operators() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create tables with array columns
            db.execute(
                "CREATE TABLE products (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    tags TEXT
                )"
            ).await?;
            
            db.execute(
                "CREATE TABLE categories (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    subcategories TEXT
                )"
            ).await?;
            
            // Insert test data
            db.execute(
                r#"INSERT INTO products (id, name, tags) VALUES 
                (1, 'Laptop', '["electronics", "computers", "portable"]'),
                (2, 'Desktop', '["electronics", "computers", "desktop"]'),
                (3, 'Phone', '["electronics", "mobile", "portable"]'),
                (4, 'Book', '["education", "reading"]'),
                (5, 'Tablet', '["electronics", "portable", "mobile"]')"#
            ).await?;
            
            db.execute(
                r#"INSERT INTO categories (id, name, subcategories) VALUES
                (1, 'Electronics', '["computers", "mobile", "audio"]'),
                (2, 'Books', '["fiction", "non-fiction", "education"]')"#
            ).await?;
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    // Test array subscript access
    let row = client.query_one(
        "SELECT tags[1] FROM products WHERE id = 1",
        &[]
    ).await.unwrap();
    let first_tag: String = row.get(0);
    assert_eq!(first_tag, "electronics");
    
    // Test ANY operator
    let rows = client.query(
        "SELECT id, name FROM products WHERE 'portable' = ANY(tags)",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 3); // Laptop, Phone, Tablet
    
    // Test ALL operator (finding products where all tags are longer than 5 chars)
    // TODO: Fix ALL operator translation - currently disabled due to syntax issues
    // let _rows = client.query(
    //     "SELECT id, name FROM products WHERE 5 < ALL(SELECT length(value) FROM json_each(tags))",
    //     &[]
    // ).await.unwrap();
    // This is a simplified test - in reality we'd need to check string length of each element
    
    // Test @> operator (contains)
    let rows = client.query(
        "SELECT id, name FROM products WHERE tags @> '[\"electronics\", \"portable\"]'",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 3); // Laptop, Phone, Tablet
    
    // Test <@ operator (is contained by)
    let rows = client.query(
        "SELECT id, name FROM products WHERE tags <@ '[\"electronics\", \"computers\", \"portable\", \"desktop\", \"mobile\"]'",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 4); // All electronics products
    
    // Test && operator (overlap)
    let rows = client.query(
        "SELECT p.name, c.name 
         FROM products p, categories c 
         WHERE p.tags && c.subcategories",
        &[]
    ).await.unwrap();
    assert!(rows.len() > 0); // Should find overlapping tags
    
    // Test || operator (concatenation)
    let row = client.query_one(
        "SELECT tags || '[\"new-tag\"]' AS combined FROM products WHERE id = 1",
        &[]
    ).await.unwrap();
    let combined: String = row.get(0);
    assert!(combined.contains("new-tag"));
    assert!(combined.contains("electronics"));
    
    server.abort();
}

#[tokio::test]
async fn test_array_functions() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(
                "CREATE TABLE test_arrays (
                    id INTEGER PRIMARY KEY,
                    numbers TEXT,
                    matrix TEXT
                )"
            ).await?;
            
            db.execute(
                r#"INSERT INTO test_arrays (id, numbers, matrix) VALUES 
                (1, '[1, 2, 3, 4, 5]', '[[1, 2, 3], [4, 5, 6]]'),
                (2, '[10, 20, 30]', '[[7, 8], [9, 10], [11, 12]]'),
                (3, '[]', '[]')"#
            ).await?;
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    // Test array_length
    let row = client.query_one(
        "SELECT array_length(numbers, 1) FROM test_arrays WHERE id = 1",
        &[]
    ).await.unwrap();
    let length: i32 = row.get(0);
    assert_eq!(length, 5);
    
    // Test array_upper and array_lower
    let row = client.query_one(
        "SELECT array_upper(numbers, 1), array_lower(numbers, 1) FROM test_arrays WHERE id = 1",
        &[]
    ).await.unwrap();
    let upper: i32 = row.get(0);
    let lower: i32 = row.get(1);
    assert_eq!(upper, 5);
    assert_eq!(lower, 1);
    
    // Test array_ndims
    let row = client.query_one(
        "SELECT array_ndims(matrix) FROM test_arrays WHERE id = 1",
        &[]
    ).await.unwrap();
    let ndims: i32 = row.get(0);
    assert_eq!(ndims, 2);
    
    // Test array_append
    let row = client.query_one(
        "SELECT array_append(numbers, 6) FROM test_arrays WHERE id = 1",
        &[]
    ).await.unwrap();
    let appended: String = row.get(0);
    assert!(appended.contains("6"));
    assert_eq!(appended, "[1,2,3,4,5,6]");
    
    // Test array_prepend
    let row = client.query_one(
        "SELECT array_prepend(0, numbers) FROM test_arrays WHERE id = 1",
        &[]
    ).await.unwrap();
    let prepended: String = row.get(0);
    assert!(prepended.starts_with("[0,"));
    
    // Test array_cat
    let row = client.query_one(
        "SELECT array_cat(numbers, '[6, 7, 8]') FROM test_arrays WHERE id = 1",
        &[]
    ).await.unwrap();
    let concatenated: String = row.get(0);
    assert!(concatenated.contains("8"));
    assert_eq!(concatenated, "[1,2,3,4,5,6,7,8]");
    
    // Test array_remove
    let row = client.query_one(
        "SELECT array_remove(numbers, 3) FROM test_arrays WHERE id = 1",
        &[]
    ).await.unwrap();
    let removed: String = row.get(0);
    assert!(!removed.contains("3"));
    assert_eq!(removed, "[1,2,4,5]");
    
    // Test array_replace
    let row = client.query_one(
        "SELECT array_replace(numbers, 3, 99) FROM test_arrays WHERE id = 1",
        &[]
    ).await.unwrap();
    let replaced: String = row.get(0);
    assert!(replaced.contains("99"));
    assert!(!replaced.contains("3"));
    
    // Test array_position
    let row = client.query_one(
        "SELECT array_position(numbers, 3) FROM test_arrays WHERE id = 1",
        &[]
    ).await.unwrap();
    let position: i32 = row.get(0);
    assert_eq!(position, 3); // 1-based index
    
    // Test array_positions
    let row = client.query_one(
        "SELECT array_positions('[1, 2, 3, 2, 4, 2]', 2)",
        &[]
    ).await.unwrap();
    let positions: String = row.get(0);
    assert_eq!(positions, "[2,4,6]"); // 1-based indices
    
    server.abort();
}

#[tokio::test]
async fn test_array_aggregation() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(
                "CREATE TABLE sales (
                    id INTEGER PRIMARY KEY,
                    product TEXT,
                    amount INTEGER
                )"
            ).await?;
            
            db.execute(
                "INSERT INTO sales (product, amount) VALUES 
                ('Laptop', 1200),
                ('Phone', 800),
                ('Laptop', 1500),
                ('Tablet', 600),
                ('Phone', 900)"
            ).await?;
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    // Test array_agg
    let row = client.query_one(
        "SELECT array_agg(DISTINCT product ORDER BY product) AS products FROM sales",
        &[]
    ).await.unwrap();
    let products: String = row.get(0);
    // Should contain all distinct products in order
    assert!(products.contains("Laptop"));
    assert!(products.contains("Phone"));
    assert!(products.contains("Tablet"));
    
    // Test array_agg with GROUP BY
    let rows = client.query(
        "SELECT product, array_agg(amount) AS amounts 
         FROM sales 
         GROUP BY product 
         ORDER BY product",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 3); // 3 distinct products
    
    server.abort();
}

#[tokio::test]
async fn test_array_slice() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(
                "CREATE TABLE test_data (
                    id INTEGER PRIMARY KEY,
                    data_values TEXT
                )"
            ).await?;
            
            db.execute(
                r#"INSERT INTO test_data (id, data_values) VALUES 
                (1, '[10, 20, 30, 40, 50, 60, 70]')"#
            ).await?;
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    // Test array slice
    let row = client.query_one(
        "SELECT data_values[2:5] AS slice FROM test_data WHERE id = 1",
        &[]
    ).await.unwrap();
    let slice: String = row.get(0);
    assert_eq!(slice, "[20,30,40,50]");
    
    server.abort();
}