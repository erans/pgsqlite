use rusqlite::{Connection, Result};

/// Register unnest support functions with SQLite
/// Note: The main unnest functionality is handled by UnnestTranslator
/// which converts unnest() calls to json_each() equivalents
pub fn register_unnest_vtab(conn: &Connection) -> Result<()> {
    // Register a helper function for array validation
    conn.create_scalar_function(
        "validate_array_for_unnest",
        1,
        rusqlite::functions::FunctionFlags::SQLITE_UTF8 | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let array_json: String = ctx.get(0)?;
            
            match serde_json::from_str::<serde_json::Value>(&array_json) {
                Ok(serde_json::Value::Array(_)) => {
                    // Return the JSON array as-is for use with json_each
                    Ok(Some(array_json))
                }
                _ => Ok(None),
            }
        },
    )?;
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn test_unnest_helper_functions() {
        let conn = Connection::open_in_memory().unwrap();
        register_unnest_vtab(&conn).unwrap();
        
        // Test array validation function
        let result: Option<String> = conn.query_row(
            "SELECT validate_array_for_unnest('[1,2,3,4]')",
            [],
            |row| row.get(0)
        ).unwrap();
        
        assert_eq!(result, Some("[1,2,3,4]".to_string()));
        
        // Test with invalid JSON
        let result: Option<String> = conn.query_row(
            "SELECT validate_array_for_unnest('not json')",
            [],
            |row| row.get(0)
        ).unwrap();
        
        assert_eq!(result, None);
        
        // Test with non-array JSON
        let result: Option<String> = conn.query_row(
            "SELECT validate_array_for_unnest('{\"key\": \"value\"}')",
            [],
            |row| row.get(0)
        ).unwrap();
        
        assert_eq!(result, None);
    }
    
    #[test] 
    fn test_json_each_equivalent() {
        let conn = Connection::open_in_memory().unwrap();
        register_unnest_vtab(&conn).unwrap();
        
        // Test that json_each works as expected for unnest replacement
        // Note: json_each returns different types for different values, so we convert to string
        let mut stmt = conn.prepare("SELECT CAST(value AS TEXT) FROM json_each('[1,2,3,4]')").unwrap();
        let rows: Vec<String> = stmt.query_map([], |row| {
            let value: String = row.get(0)?;
            Ok(value)
        }).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        
        assert_eq!(rows, vec!["1", "2", "3", "4"]);
        
        // Test with string array
        let mut stmt = conn.prepare("SELECT value FROM json_each('[\"a\",\"b\",\"c\"]')").unwrap();
        let rows: Vec<String> = stmt.query_map([], |row| {
            let value: String = row.get(0)?;
            Ok(value)
        }).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        
        assert_eq!(rows, vec!["a", "b", "c"]);
    }
}