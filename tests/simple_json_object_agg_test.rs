use rusqlite::Connection;
use pgsqlite::functions::json_functions::register_json_functions;

#[test]
fn test_json_object_agg_unit() {
    let conn = Connection::open_in_memory().unwrap();
    register_json_functions(&conn).unwrap();
    
    // Test if the function exists first
    match conn.query_row(
        "SELECT json_object_agg(key, value) FROM (SELECT 'name' as key, 'John' as value UNION SELECT 'age', '30') AS t",
        [],
        |row| row.get::<usize, String>(0)
    ) {
        Ok(result) => {
            println!("json_object_agg result: {}", result);
            
            // Parse the result JSON to verify it contains the expected key-value pairs
            match serde_json::from_str::<serde_json::Value>(&result) {
                Ok(json) => {
                    assert_eq!(json.get("name").unwrap(), "John");
                    assert_eq!(json.get("age").unwrap(), "30");
                    println!("json_object_agg test passed: {}", result);
                }
                Err(e) => {
                    panic!("Failed to parse JSON: {}", e);
                }
            }
        }
        Err(e) => {
            panic!("Failed to execute query: {}", e);
        }
    }
}

#[test]
fn test_jsonb_object_agg_unit() {
    let conn = Connection::open_in_memory().unwrap();
    register_json_functions(&conn).unwrap();
    
    // Test basic jsonb_object_agg functionality
    let result: String = conn.query_row(
        "SELECT jsonb_object_agg(key, value) FROM (SELECT 'active' as key, 'true' as value UNION SELECT 'score', '95.5') AS t",
        [],
        |row| row.get(0)
    ).unwrap();
    
    // Parse the result JSON to verify it contains the expected key-value pairs
    let json: serde_json::Value = serde_json::from_str(&result).unwrap();
    // jsonb_object_agg should try to parse JSON values, so "true" becomes boolean true
    assert_eq!(json.get("active").unwrap(), true);
    assert_eq!(json.get("score").unwrap(), 95.5);
    
    println!("jsonb_object_agg test passed: {}", result);
}

#[test]
fn test_json_object_agg_empty() {
    let conn = Connection::open_in_memory().unwrap();
    register_json_functions(&conn).unwrap();
    
    // Test empty result set
    let result: String = conn.query_row(
        "SELECT json_object_agg(key, value) FROM (SELECT 'test' as key, 'value' as value WHERE false) AS t",
        [],
        |row| row.get(0)
    ).unwrap();
    
    // Should return empty object for no rows
    assert_eq!(result, "{}");
    
    println!("json_object_agg empty test passed: {}", result);
}