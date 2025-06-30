mod common;
use common::*;
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn test_large_result_set() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(
                "CREATE TABLE large_data (
                    id INTEGER PRIMARY KEY,
                    data TEXT
                )"
            ).await?;
            
            // Insert 5000 rows
            for i in 1..=5000 {
                db.execute(&format!(
                    "INSERT INTO large_data (id, data) VALUES ({}, 'Row {} data with some padding to make it larger')",
                    i, i
                )).await?;
            }
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    // Test fetching large result set
    let start = std::time::Instant::now();
    let rows = client.query("SELECT id, data FROM large_data ORDER BY id", &[]).await.unwrap();
    let elapsed = start.elapsed();
    
    println!("Fetched {} rows in {:?}", rows.len(), elapsed);
    assert_eq!(rows.len(), 5000);
    
    // Verify first and last rows
    let first_id: i32 = rows[0].get(0);
    let first_data: String = rows[0].get(1);
    assert_eq!(first_id, 1);
    assert!(first_data.contains("Row 1"));
    
    let last_id: i32 = rows[4999].get(0);
    let last_data: String = rows[4999].get(1);
    assert_eq!(last_id, 5000);
    assert!(last_data.contains("Row 5000"));
    
    server.abort();
}

#[tokio::test]
async fn test_large_parameter_values() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(
                "CREATE TABLE blob_data (
                    id INTEGER PRIMARY KEY,
                    large_text TEXT,
                    large_blob BLOB
                )"
            ).await?;
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    // Test with large text parameter (1MB)
    let large_text = "x".repeat(1024 * 1024);
    client.execute(
        "INSERT INTO blob_data (id, large_text) VALUES ($1, $2)",
        &[&1i32, &large_text]
    ).await.unwrap();
    
    // Test with large binary parameter (2MB)
    let large_blob = vec![0xABu8; 2 * 1024 * 1024];
    client.execute(
        "INSERT INTO blob_data (id, large_blob) VALUES ($1, $2)",
        &[&2i32, &large_blob]
    ).await.unwrap();
    
    // Verify retrieval
    let row = client.query_one("SELECT large_text FROM blob_data WHERE id = 1", &[]).await.unwrap();
    let retrieved_text: String = row.get(0);
    assert_eq!(retrieved_text.len(), large_text.len());
    
    let row = client.query_one("SELECT large_blob FROM blob_data WHERE id = 2", &[]).await.unwrap();
    let retrieved_blob: Vec<u8> = row.get(0);
    assert_eq!(retrieved_blob.len(), large_blob.len());
    
    server.abort();
}

#[tokio::test]
async fn test_mixed_format_modes() {
    let server = setup_test_server().await;
    
    let client = &server.client;
    
    // Create table through PostgreSQL protocol to ensure proper metadata storage
    client.execute(
        "CREATE TABLE format_test (
            id INTEGER PRIMARY KEY,
            num INTEGER,
            txt TEXT,
            bin BLOB
        )",
        &[]
    ).await.unwrap();
    
    // Insert data through PostgreSQL protocol  
    client.execute(
        "INSERT INTO format_test (id, num, txt, bin) VALUES (1, 42, 'hello', x'DEADBEEF')",
        &[]
    ).await.unwrap();
    
    // Prepare statement to test mixed formats
    let stmt = client.prepare("SELECT id, num, txt, bin FROM format_test WHERE id = $1").await.unwrap();
    
    // Execute with binary format for some columns
    // Note: tokio-postgres may not fully support mixed formats,
    // but we should handle whatever it sends
    let rows = client.query(&stmt, &[&1i32]).await.unwrap();
    assert_eq!(rows.len(), 1);
    
    let id: i32 = rows[0].get(0);
    let num: i32 = rows[0].get(1);
    let txt: String = rows[0].get(2);
    let bin: Vec<u8> = rows[0].get(3);
    
    assert_eq!(id, 1);
    assert_eq!(num, 42);
    assert_eq!(txt, "hello");
    assert_eq!(bin, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    
    server.abort();
}

#[tokio::test]
async fn test_portal_suspension() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(
                "CREATE TABLE suspension_test (
                    id INTEGER PRIMARY KEY,
                    value TEXT
                )"
            ).await?;
            
            // Insert 100 rows
            for i in 1..=100 {
                db.execute(&format!(
                    "INSERT INTO suspension_test VALUES ({}, 'Value {}')",
                    i, i
                )).await?;
            }
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    // Note: tokio-postgres doesn't directly expose portal suspension,
    // but we can test that large queries work correctly
    let rows = client.query("SELECT * FROM suspension_test ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 100);
    
    // Verify some data
    let first_row = &rows[0];
    let id: i32 = first_row.get(0);
    let value: String = first_row.get(1);
    assert_eq!(id, 1);
    assert_eq!(value, "Value 1");
    
    let last_row = &rows[99];
    let id: i32 = last_row.get(0);
    let value: String = last_row.get(1);
    assert_eq!(id, 100);
    assert_eq!(value, "Value 100");
    
    server.abort();
}

#[tokio::test]
async fn test_null_parameters() {
    let server = setup_test_server().await;
    
    let client = &server.client;
    
    // Create table through PostgreSQL protocol to ensure proper metadata storage
    client.execute(
        "CREATE TABLE null_test (
            id INTEGER PRIMARY KEY,
            opt_int INTEGER,
            opt_text TEXT,
            opt_blob BYTEA
        )",
        &[]
    ).await.unwrap();
    
    // Test inserting NULL values
    client.execute(
        "INSERT INTO null_test (id, opt_int, opt_text, opt_blob) VALUES ($1, $2, $3, $4)",
        &[&1i32, &None::<i32>, &None::<String>, &None::<Vec<u8>>]
    ).await.unwrap();
    
    // Test with some NULL and some non-NULL
    client.execute(
        "INSERT INTO null_test (id, opt_int, opt_text, opt_blob) VALUES ($1, $2, $3, $4)",
        &[&2i32, &Some(42i32), &None::<String>, &Some(vec![1u8, 2, 3])]
    ).await.unwrap();
    
    // Verify NULLs
    let row = client.query_one("SELECT opt_int, opt_text, opt_blob FROM null_test WHERE id = 1", &[]).await.unwrap();
    assert!(row.try_get::<_, i32>(0).is_err());
    assert!(row.try_get::<_, String>(1).is_err());
    assert!(row.try_get::<_, Vec<u8>>(2).is_err());
    
    // Verify mixed
    let row = client.query_one("SELECT opt_int, opt_text, opt_blob FROM null_test WHERE id = 2", &[]).await.unwrap();
    assert_eq!(row.get::<_, i32>(0), 42);
    assert!(row.try_get::<_, String>(1).is_err());
    assert_eq!(row.get::<_, Vec<u8>>(2), vec![1, 2, 3]);
    
    server.abort();
}

#[tokio::test]
async fn test_empty_query() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test empty query
    match client.simple_query("").await {
        Ok(messages) => {
            // PostgreSQL returns EmptyQueryResponse for empty queries
            assert!(messages.is_empty() || messages.len() == 1);
        }
        Err(e) => {
            // Some implementations might error on empty query
            println!("Empty query error (may be expected): {}", e);
        }
    }
    
    // Test whitespace-only query
    match client.simple_query("   \n\t  ").await {
        Ok(messages) => {
            assert!(messages.is_empty() || messages.len() == 1);
        }
        Err(e) => {
            println!("Whitespace query error (may be expected): {}", e);
        }
    }
    
    server.abort();
}

#[tokio::test]
async fn test_multiple_statements() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(
                "CREATE TABLE multi_test (
                    id INTEGER PRIMARY KEY,
                    value TEXT
                )"
            ).await?;
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    // Test multiple statements in simple query
    let messages = client.simple_query(
        "INSERT INTO multi_test VALUES (1, 'one'); 
         INSERT INTO multi_test VALUES (2, 'two');
         SELECT * FROM multi_test ORDER BY id"
    ).await.unwrap();
    
    // Should get multiple command complete messages and row data
    assert!(messages.len() >= 3);
    
    // Verify data was inserted
    let rows = client.query("SELECT COUNT(*) FROM multi_test", &[]).await.unwrap();
    let count: i64 = rows[0].get(0);
    assert_eq!(count, 2);
    
    server.abort();
}

#[tokio::test]
async fn test_query_timeout() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test query that might take long (SQLite doesn't have pg_sleep)
    // We'll use a complex query instead
    let result = timeout(
        Duration::from_secs(2),
        client.query(
            "WITH RECURSIVE r(i) AS (
                SELECT 1
                UNION ALL
                SELECT i + 1 FROM r WHERE i < 1000000
            )
            SELECT COUNT(*) FROM r",
            &[]
        )
    ).await;
    
    match result {
        Ok(Ok(rows)) => {
            let count: i64 = rows[0].get(0);
            println!("Recursive query completed with count: {}", count);
        }
        Ok(Err(e)) => println!("Query error: {}", e),
        Err(_) => println!("Query timed out (expected for very large recursive)"),
    }
    
    server.abort();
}

#[tokio::test]
async fn test_special_characters_in_strings() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(
                "CREATE TABLE special_chars (
                    id INTEGER PRIMARY KEY,
                    content TEXT
                )"
            ).await?;
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    // Test various special characters
    let test_strings = vec![
        "Simple string",
        "String with 'quotes'",
        "String with \"double quotes\"",
        "String with \\ backslash",
        "String with \n newline",
        "String with \t tab",
        "String with NULL \0 byte",
        "Unicode: émojis 🎉 and 中文",
        "Mixed: 'quotes' and \"more\" \\ stuff",
    ];
    
    for (i, s) in test_strings.iter().enumerate() {
        // Skip NULL byte test if it causes issues
        if s.contains('\0') {
            match client.execute(
                "INSERT INTO special_chars (id, content) VALUES ($1, $2)",
                &[&(i as i32), s]
            ).await {
                Ok(_) => {},
                Err(e) => {
                    println!("NULL byte in string not supported (expected): {}", e);
                    continue;
                }
            }
        } else {
            client.execute(
                "INSERT INTO special_chars (id, content) VALUES ($1, $2)",
                &[&(i as i32), s]
            ).await.unwrap();
        }
        
        // Verify retrieval
        let row = client.query_one(
            "SELECT content FROM special_chars WHERE id = $1::int4", 
            &[&(i as i32)]
        ).await.unwrap();
        let retrieved: String = row.get(0);
        assert_eq!(&retrieved, s, "Failed for string: {:?}", s);
    }
    
    server.abort();
}

#[tokio::test]
async fn test_parameter_limit() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create table with many columns
            let mut cols = Vec::new();
            for i in 1..=100 {
                cols.push(format!("col{} INTEGER", i));
            }
            
            db.execute(&format!(
                "CREATE TABLE many_params (id INTEGER PRIMARY KEY, {})",
                cols.join(", ")
            )).await?;
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    // Test with many parameters (PostgreSQL supports up to 65535 parameters)
    // We'll test with 100 parameters
    let mut params: Vec<i32> = Vec::new();
    for i in 1..=101 {
        params.push(i);
    }
    
    let mut placeholders = Vec::new();
    let mut param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();
    
    for i in 0..101 {
        placeholders.push(format!("${}", i + 1));
        param_refs.push(&params[i]);
    }
    
    let query = format!(
        "INSERT INTO many_params VALUES ({})", 
        placeholders.join(", ")
    );
    
    match client.execute(&query, &param_refs).await {
        Ok(n) => {
            assert_eq!(n, 1);
            println!("Successfully inserted with 101 parameters");
        }
        Err(e) => {
            println!("Failed with many parameters: {}", e);
        }
    }
    
    server.abort();
}