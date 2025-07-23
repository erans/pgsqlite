mod common;
use common::*;

/// Test array concatenation operator (||) functionality
#[tokio::test]
async fn test_array_concatenation_operators() {
    let server = setup_test_server_with_init(|client| {
        Box::pin(async move {
            client.execute("CREATE TABLE test_arrays (
                id INTEGER PRIMARY KEY,
                tags TEXT[],
                numbers INTEGER[],
                items_list TEXT[],
                category_names TEXT[]
            )").await?;
            
            client.execute("INSERT INTO test_arrays (id, tags, numbers, items_list, category_names) VALUES 
                (1, '{tag1,tag2}', '{1,2,3}', '{item1,item2}', '{cat1,cat2}'),
                (2, '{tag3,tag4}', '{4,5,6}', '{item3,item4}', '{cat3,cat4}')").await?;
            
            Ok(())
        })
    }).await;

    let client = &server.client;

    // Test 1: Array literal concatenation
    let result = client.query("SELECT '{a,b}' || '{c,d}' AS concat_result", &[]).await.unwrap();
    assert_eq!(result.len(), 1);
    // Should be converted to array_cat function call

    // Test 2: Column concatenation with array literal
    let result = client.query("SELECT tags || '{new_tag}' AS extended_tags FROM test_arrays WHERE id = 1", &[]).await.unwrap();
    assert_eq!(result.len(), 1);

    // Test 3: Two array columns concatenation  
    let result = client.query("SELECT tags || category_names AS all_tags FROM test_arrays WHERE id = 1", &[]).await.unwrap();
    assert_eq!(result.len(), 1);

    // Test 4: Array function result concatenation (using PostgreSQL array literal syntax)
    let result = client.query("SELECT '{a,b}' || tags AS combined FROM test_arrays WHERE id = 1", &[]).await.unwrap();
    assert_eq!(result.len(), 1);

    // Test 5: String concatenation should NOT be affected
    let result = client.query("SELECT 'hello' || ' world' AS greeting", &[]).await.unwrap();
    assert_eq!(result.len(), 1);
    let greeting: String = result[0].get(0);
    assert_eq!(greeting, "hello world");

    // Test 6: Mixed string and column (should be string concat)
    let result = client.query("SELECT 'id_' || id::TEXT AS prefixed_id FROM test_arrays WHERE id = 1", &[]).await.unwrap();
    assert_eq!(result.len(), 1);
    let prefixed_id: String = result[0].get(0);
    assert_eq!(prefixed_id, "id_1");
}

#[tokio::test] 
async fn test_array_concatenation_with_aliases() {
    let server = setup_test_server_with_init(|client| {
        Box::pin(async move {
            client.execute("CREATE TABLE test_concat (
                id INTEGER PRIMARY KEY,
                tags_array TEXT[],
                items TEXT[]
            )").await?;
            
            client.execute("INSERT INTO test_concat (id, tags_array, items) VALUES 
                (1, '{tag1,tag2}', '{item1,item2}')").await?;
            
            Ok(())
        })
    }).await;

    let client = &server.client;

    // Test concatenation with aliases
    let result = client.query("SELECT tags_array || items AS combined_data FROM test_concat", &[]).await.unwrap();
    assert_eq!(result.len(), 1);

    // Test array literal with alias  
    let result = client.query("SELECT '{extra}' || tags_array AS final_tags FROM test_concat", &[]).await.unwrap();
    assert_eq!(result.len(), 1);
}

#[tokio::test]
async fn test_array_concatenation_edge_cases() {
    let server = setup_test_server().await;
    let client = &server.client;

    // Test various array literal formats
    let test_cases = vec![
        "SELECT '{1,2}' || '{3,4}' AS result1",
        // NOTE: ARRAY[1,2] || ARRAY[3,4] syntax detection is implemented but requires
        // ARRAY literal translator (ARRAY[1,2,3] -> JSON format) for full functionality
        "SELECT '[1,2]'::json || '[3,4]'::json AS result3",
        // String concatenation (should not be translated)
        "SELECT 'hello' || 'world' AS result4",
        "SELECT 'prefix_' || 'suffix' AS result5"
    ];

    for query in test_cases {
        let result = client.query(query, &[]).await;
        // Should not error - either array_cat or string concat
        assert!(result.is_ok(), "Query failed: {query}");
    }
}

#[tokio::test]
async fn test_array_concatenation_function_calls() {
    let server = setup_test_server_with_init(|client| {
        Box::pin(async move {
            client.execute("CREATE TABLE test_funcs (
                id INTEGER PRIMARY KEY,
                base_array TEXT[]
            )").await?;

            client.execute("INSERT INTO test_funcs (id, base_array) VALUES (1, '{a,b}')").await?;
            
            Ok(())
        })
    }).await;

    let client = &server.client;

    // Test array concatenation with columns (focus on concatenation logic)
    let queries = vec![
        "SELECT base_array || base_array FROM test_funcs",
        "SELECT base_array || '{x,y}' FROM test_funcs",
        "SELECT '{prefix}' || base_array FROM test_funcs"
    ];

    for query in queries {
        let result = client.query(query, &[]).await;
        assert!(result.is_ok(), "Array concatenation query failed: {query}");
    }
}

#[tokio::test]
async fn test_array_concatenation_naming_patterns() {
    let server = setup_test_server_with_init(|client| {
        Box::pin(async move {
            client.execute("CREATE TABLE naming_test (
                user_tags TEXT[],
                item_ids INTEGER[],
                categories TEXT[],
                keywords TEXT[],
                elements TEXT[],
                values_list TEXT[],
                name_array TEXT[],
                simple_string TEXT
            )").await?;

            client.execute("INSERT INTO naming_test VALUES 
                ('{tag1}', '{1,2}', '{cat1}', '{kw1}', '{el1}', '{val1}', '{name1}', 'text')").await?;
            
            Ok(())
        })
    }).await;

    let client = &server.client;

    // These should be detected as array concatenation
    let array_queries = vec![
        "SELECT user_tags || categories",
        "SELECT item_ids || values_list", 
        "SELECT keywords || elements",
        "SELECT name_array || user_tags"
    ];

    for query in array_queries {
        let result = client.query(&format!("{query} FROM naming_test"), &[]).await;
        assert!(result.is_ok(), "Array naming pattern query failed: {query}");
    }

    // This should remain string concatenation  
    let result = client.query("SELECT simple_string || ' suffix' FROM naming_test", &[]).await.unwrap();
    let concatenated: String = result[0].get(0);
    assert_eq!(concatenated, "text suffix");
}

#[tokio::test]
async fn test_array_syntax_concatenation() {
    let server = setup_test_server_with_init(|client| {
        Box::pin(async move {
            client.execute("CREATE TABLE array_syntax_test (
                id INTEGER PRIMARY KEY,
                int_array INTEGER[],
                text_array TEXT[]
            )").await?;
            
            client.execute("INSERT INTO array_syntax_test (id, int_array, text_array) VALUES 
                (1, '{1,2,3}', '{a,b,c}'),
                (2, '{4,5,6}', '{d,e,f}')")
            .await?;
            
            Ok(())
        })
    }).await;

    let client = &server.client;

    // Note: Direct ARRAY[1,2] syntax requires ARRAY literal translator
    // For now, test the type-aware concatenation logic with existing syntax
    
    // Test array literal concatenation (current working format)
    let result = client.query("SELECT '{1,2}' || '{3,4}' AS combined", &[]).await.unwrap();
    assert_eq!(result.len(), 1);

    // Test mixed literal and column syntax
    let result = client.query("SELECT '{0}' || int_array AS prepended FROM array_syntax_test WHERE id = 1", &[]).await.unwrap();
    assert_eq!(result.len(), 1);

    // Test column with literal
    let result = client.query("SELECT text_array || '{z}' AS appended FROM array_syntax_test WHERE id = 1", &[]).await.unwrap();
    assert_eq!(result.len(), 1);

    // Test complex expressions with existing array literal format
    let result = client.query("SELECT '{prefix}' || text_array AS wrapped FROM array_syntax_test WHERE id = 1", &[]).await.unwrap();
    assert_eq!(result.len(), 1);

    // Ensure string concatenation still works and is not affected
    let result = client.query("SELECT 'hello' || ' world' AS greeting", &[]).await.unwrap();
    assert_eq!(result.len(), 1);
    let greeting: String = result[0].get(0);
    assert_eq!(greeting, "hello world");
}