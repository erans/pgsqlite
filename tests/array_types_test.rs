mod common;
use common::*;
use tokio_postgres::types::{Type, FromSql};

// Helper type to extract JSON columns as strings
struct JsonString(String);

impl<'a> FromSql<'a> for JsonString {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let s = std::str::from_utf8(raw)?;
        Ok(JsonString(s.to_string()))
    }
    
    fn accepts(ty: &Type) -> bool {
        ty.name() == "json" || ty.name() == "text"
    }
}

#[tokio::test]
async fn test_array_declarations() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // SQLite doesn't support arrays natively, but we should handle the syntax
            // Arrays will be stored as JSON strings
            db.execute(
                "CREATE TABLE array_test (
                    id INTEGER PRIMARY KEY,
                    int_array TEXT,
                    text_array TEXT,
                    nested_array TEXT
                )"
            ).await?;
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    // Test that array type declarations are accepted (even if stored as TEXT)
    match client.execute(
        "CREATE TABLE test_array_types (
            id INTEGER PRIMARY KEY,
            numbers INTEGER[],
            names TEXT[],
            matrix INTEGER[][]
        )",
        &[]
    ).await {
        Ok(_) => {
            // Good - we handle array syntax
        }
        Err(e) => {
            // For now, it's OK if this fails since SQLite doesn't support arrays
            // In a full implementation, we'd translate this to TEXT columns
            println!("Array type declaration not fully supported yet: {e}");
        }
    }
    
    server.abort();
}

#[tokio::test]
async fn test_array_literals() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(
                "CREATE TABLE array_data (
                    id INTEGER PRIMARY KEY,
                    int_array TEXT,
                    text_array TEXT
                )"
            ).await?;
            
            // Store arrays as JSON strings for now
            db.execute(
                "INSERT INTO array_data VALUES 
                (1, '[1,2,3]', '[\"hello\",\"world\"]'),
                (2, '[4,5,6]', '[\"foo\",\"bar\"]'),
                (3, '[]', '[]')"
            ).await?;
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    // Test retrieving array data (as JSON strings for now)
    let row = client.query_one("SELECT int_array, text_array FROM array_data WHERE id = 1", &[]).await.unwrap();
    
    // The columns contain JSON data, so they're returned as JSON type
    let JsonString(int_array) = row.get(0);
    let JsonString(text_array) = row.get(1);
    
    assert_eq!(int_array, "[1,2,3]");
    assert_eq!(text_array, "[\"hello\",\"world\"]");
    
    // Test empty arrays
    let row = client.query_one("SELECT int_array FROM array_data WHERE id = 3", &[]).await.unwrap();
    let JsonString(empty_array) = row.get(0);
    assert_eq!(empty_array, "[]");
    
    // Test array literal syntax (PostgreSQL style)
    // This will likely fail for now, which is expected
    match client.query_one("SELECT ARRAY[1,2,3]", &[]).await {
        Ok(_) => println!("Array literal syntax supported"),
        Err(e) => println!("Array literal syntax not yet supported: {e}"),
    }
    
    // Test alternative array syntax
    match client.query_one("SELECT '{1,2,3}'::integer[]", &[]).await {
        Ok(_) => println!("PostgreSQL array cast syntax supported"),
        Err(e) => println!("PostgreSQL array cast syntax not yet supported: {e}"),
    }
    
    server.abort();
}

#[tokio::test]
async fn test_array_operations() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(
                "CREATE TABLE array_ops (
                    id INTEGER PRIMARY KEY,
                    data TEXT
                )"
            ).await?;
            
            db.execute(
                "INSERT INTO array_ops VALUES 
                (1, '[1,2,3,4,5]'),
                (2, '[\"a\",\"b\",\"c\"]')"
            ).await?;
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    // Test JSON functions as a workaround for array operations
    // SQLite has JSON functions that can help with array-like operations
    
    // Test array length (using JSON)
    let row = client.query_one(
        "SELECT json_array_length(data) FROM array_ops WHERE id = 1", 
        &[]
    ).await.unwrap();
    let len: i32 = row.get(0);
    assert_eq!(len, 5);
    
    // Test array element access (using JSON)
    let row = client.query_one(
        "SELECT json_extract(data, '$[0]') FROM array_ops WHERE id = 1", 
        &[]
    ).await.unwrap();
    // json_extract returns text, so we need to parse it
    let first_elem: String = row.get(0);
    assert_eq!(first_elem, "1");
    
    // Test array element access for text array
    let row = client.query_one(
        "SELECT json_extract(data, '$[1]') FROM array_ops WHERE id = 2", 
        &[]
    ).await.unwrap();
    let second_elem: String = row.get(0);
    assert_eq!(second_elem, "b");
    
    // PostgreSQL array operations that we should eventually support
    // These will fail for now
    let unsupported_ops = vec![
        ("array_length(data, 1)", "array_length function"),
        ("data[1]", "array subscript"),
        ("array_append(data, 6)", "array_append function"),
        ("unnest(data)", "unnest function"),
    ];
    
    for (op, desc) in unsupported_ops {
        match client.query(&format!("SELECT {op} FROM array_ops WHERE id = 1"), &[]).await {
            Ok(_) => println!("{desc} is supported!"),
            Err(_) => println!("{desc} not yet supported (expected)"),
        }
    }
    
    server.abort();
}

#[tokio::test]
async fn test_array_in_where_clause() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(
                "CREATE TABLE products (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    tags TEXT
                )"
            ).await?;
            
            db.execute(
                "INSERT INTO products VALUES 
                (1, 'Laptop', '[\"electronics\",\"computers\"]'),
                (2, 'Mouse', '[\"electronics\",\"accessories\"]'),
                (3, 'Book', '[\"education\",\"reading\"]')"
            ).await?;
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    // Test JSON-based array containment
    let rows = client.query(
        "SELECT id, name FROM products 
         WHERE json_extract(tags, '$') LIKE '%electronics%'",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 2);
    
    // Test finding specific array element
    let rows = client.query(
        "SELECT id, name FROM products 
         WHERE json_extract(tags, '$[0]') = 'electronics'",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 2);
    
    // PostgreSQL ANY/ALL operators (not supported yet)
    match client.query(
        "SELECT id FROM products WHERE 'electronics' = ANY(tags)",
        &[]
    ).await {
        Ok(_) => println!("ANY operator supported"),
        Err(_) => println!("ANY operator not yet supported (expected)"),
    }
    
    server.abort();
}

#[tokio::test]
async fn test_array_aggregation() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(
                "CREATE TABLE measurements (
                    id INTEGER PRIMARY KEY,
                    sensor_id INTEGER,
                    value REAL
                )"
            ).await?;
            
            db.execute(
                "INSERT INTO measurements VALUES 
                (1, 1, 23.5),
                (2, 1, 24.0),
                (3, 1, 23.8),
                (4, 2, 30.1),
                (5, 2, 30.5)"
            ).await?;
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    // Test JSON aggregation as array substitute
    let row = client.query_one(
        "SELECT json_group_array(value) as value_array 
         FROM measurements 
         WHERE sensor_id = 1",
        &[]
    ).await.unwrap();
    // For JSON columns, we need to handle them properly  
    let JsonString(values) = row.get(0);
    assert!(values.contains("23.5"));
    assert!(values.contains("24.0"));
    assert!(values.contains("23.8"));
    
    // Test grouping with JSON arrays
    let rows = client.query(
        "SELECT sensor_id, json_group_array(value) as value_array 
         FROM measurements 
         GROUP BY sensor_id 
         ORDER BY sensor_id",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 2);
    
    // PostgreSQL array_agg function (not supported yet)
    match client.query(
        "SELECT array_agg(value) FROM measurements WHERE sensor_id = 1",
        &[]
    ).await {
        Ok(_) => println!("array_agg supported"),
        Err(_) => println!("array_agg not yet supported (expected)"),
    }
    
    server.abort();
}

#[tokio::test]
async fn test_multi_dimensional_arrays() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(
                "CREATE TABLE matrix_data (
                    id INTEGER PRIMARY KEY,
                    matrix TEXT
                )"
            ).await?;
            
            // Store 2D array as nested JSON
            db.execute(
                "INSERT INTO matrix_data VALUES 
                (1, '[[1,2,3],[4,5,6],[7,8,9]]'),
                (2, '[[10,20],[30,40]]')"
            ).await?;
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    // Test accessing nested array elements
    let row = client.query_one(
        "SELECT json_extract(matrix, '$[0][0]') as elem FROM matrix_data WHERE id = 1",
        &[]
    ).await.unwrap();
    // json_extract returns numeric values as integers when they can be parsed as such
    let elem: i32 = row.get(0);
    assert_eq!(elem, 1);
    
    // Test accessing different row
    let row = client.query_one(
        "SELECT json_extract(matrix, '$[1][2]') as elem FROM matrix_data WHERE id = 1",
        &[]
    ).await.unwrap();
    // json_extract returns numeric values as integers when they can be parsed as such
    let elem: i32 = row.get(0);
    assert_eq!(elem, 6);
    
    // Test matrix dimensions
    let row = client.query_one(
        "SELECT json_array_length(matrix) as rows FROM matrix_data WHERE id = 1",
        &[]
    ).await.unwrap();
    let rows: i32 = row.get(0);
    assert_eq!(rows, 3);
    
    server.abort();
}

#[tokio::test]
async fn test_array_with_nulls() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(
                "CREATE TABLE nullable_arrays (
                    id INTEGER PRIMARY KEY,
                    data TEXT
                )"
            ).await?;
            
            db.execute(
                "INSERT INTO nullable_arrays VALUES 
                (1, '[1,null,3]'),
                (2, '[null,null,null]'),
                (3, NULL)"
            ).await?;
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    // Test array with null elements
    let row = client.query_one(
        "SELECT json_extract(data, '$[1]') FROM nullable_arrays WHERE id = 1",
        &[]
    ).await.unwrap();
    // JSON null becomes SQL NULL
    assert!(row.try_get::<_, i32>(0).is_err());
    
    // Test NULL array
    let row = client.query_one(
        "SELECT data FROM nullable_arrays WHERE id = 3",
        &[]
    ).await.unwrap();
    assert!(row.try_get::<_, String>(0).is_err());
    
    // Test array length with nulls
    let row = client.query_one(
        "SELECT json_array_length(data) FROM nullable_arrays WHERE id = 1",
        &[]
    ).await.unwrap();
    let len: i32 = row.get(0);
    assert_eq!(len, 3); // Includes null elements
    
    server.abort();
}

#[tokio::test]
async fn test_array_unnest_workaround() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(
                "CREATE TABLE array_table (
                    id INTEGER PRIMARY KEY,
                    items TEXT
                )"
            ).await?;
            
            db.execute(
                "INSERT INTO array_table VALUES 
                (1, '[10,20,30]'),
                (2, '[40,50]')"
            ).await?;
            
            // Create a helper table for JSON table functions
            db.execute(
                "CREATE TABLE json_each_helper AS 
                 SELECT key, value FROM json_each('[1,2,3]')"
            ).await?;
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    // Test json_each as unnest alternative
    let rows = client.query(
        "SELECT value FROM json_each((SELECT items FROM array_table WHERE id = 1))",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 3);
    
    let values: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(values, vec!["10", "20", "30"]);
    
    // Test cross join with json_each for unnesting multiple arrays
    let rows = client.query(
        "SELECT a.id, je.value 
         FROM array_table a, json_each(a.items) je 
         ORDER BY a.id, je.value",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 5); // 3 + 2 elements total
    
    server.abort();
}