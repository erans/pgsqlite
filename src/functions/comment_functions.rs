use rusqlite::Connection;
use crate::ddl::CommentDdlHandler;
use crate::PgSqliteError;
use regex::Regex;
use once_cell::sync::Lazy;
use tracing::debug;

// Regex patterns for detecting comment function calls in SQL
static OBJ_DESCRIPTION_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)obj_description\s*\(\s*([^,\s]+)\s*,\s*'([^']+)'\s*\)").unwrap()
});

static OBJ_DESCRIPTION_SINGLE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)obj_description\s*\(\s*([^)\s]+)\s*\)").unwrap()
});

static COL_DESCRIPTION_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)col_description\s*\(\s*([^,\s]+)\s*,\s*([^)\s]+)\s*\)").unwrap()
});

pub struct CommentFunctionHandler;

impl CommentFunctionHandler {
    /// Check if query contains comment functions that need processing
    pub fn contains_comment_functions(query: &str) -> bool {
        OBJ_DESCRIPTION_REGEX.is_match(query) ||
        OBJ_DESCRIPTION_SINGLE_REGEX.is_match(query) ||
        COL_DESCRIPTION_REGEX.is_match(query)
    }
    
    /// Replace comment function calls with actual values
    pub fn process_comment_functions(
        conn: &Connection,
        query: &str,
    ) -> Result<String, PgSqliteError> {
        let mut processed_query = query.to_string();
        
        // Process obj_description(oid, catalog) calls
        processed_query = Self::process_obj_description_two_param(conn, &processed_query)?;
        
        // Process obj_description(oid) calls  
        processed_query = Self::process_obj_description_single_param(conn, &processed_query)?;
        
        // Process col_description(oid, column_num) calls
        processed_query = Self::process_col_description(conn, &processed_query)?;
        
        debug!("Processed comment functions: {} -> {}", query, processed_query);
        Ok(processed_query)
    }
    
    /// Process obj_description(oid, catalog_name) function calls
    fn process_obj_description_two_param(
        conn: &Connection,
        query: &str,
    ) -> Result<String, PgSqliteError> {
        let mut result = query.to_string();
        
        for captures in OBJ_DESCRIPTION_REGEX.captures_iter(query) {
            let full_match = captures.get(0).unwrap().as_str();
            let oid_str = captures.get(1).unwrap().as_str();
            let catalog_name = captures.get(2).unwrap().as_str();
            
            // Only process if OID is a numeric literal, skip variables
            if let Ok(oid) = oid_str.parse::<i32>() {
                let comment = CommentDdlHandler::get_comment(conn, oid, catalog_name, 0)?;
                let replacement = match comment {
                    Some(text) => {
                        // Text from database already has proper escaping, just wrap in quotes
                        format!("'{}'", text)
                    },
                    None => "NULL".to_string(),
                };
                
                result = result.replace(full_match, &replacement);
            }
            // If it's a variable (like c.oid), leave it unchanged
        }
        
        Ok(result)
    }
    
    /// Process obj_description(oid) function calls (single parameter, deprecated)
    fn process_obj_description_single_param(
        conn: &Connection,
        query: &str,
    ) -> Result<String, PgSqliteError> {
        let mut result = query.to_string();
        
        for captures in OBJ_DESCRIPTION_SINGLE_REGEX.captures_iter(query) {
            let full_match = captures.get(0).unwrap().as_str();
            let oid_str = captures.get(1).unwrap().as_str();
            
            // Only process if OID is a numeric literal, skip variables
            if let Ok(oid) = oid_str.parse::<i32>() {
                // For single parameter, try pg_class first (most common)
                let comment = CommentDdlHandler::get_comment(conn, oid, "pg_class", 0)?
                    .or_else(|| CommentDdlHandler::get_comment(conn, oid, "pg_proc", 0).unwrap_or(None));
                    
                let replacement = match comment {
                    Some(text) => {
                        // Text from database already has proper escaping, just wrap in quotes
                        format!("'{}'", text)
                    },
                    None => "NULL".to_string(),
                };
                
                result = result.replace(full_match, &replacement);
            }
            // If it's a variable, leave it unchanged
        }
        
        Ok(result)
    }
    
    /// Process col_description(table_oid, column_num) function calls
    fn process_col_description(
        conn: &Connection,
        query: &str,
    ) -> Result<String, PgSqliteError> {
        let mut result = query.to_string();
        
        for captures in COL_DESCRIPTION_REGEX.captures_iter(query) {
            let full_match = captures.get(0).unwrap().as_str();
            let table_oid_str = captures.get(1).unwrap().as_str();
            let column_num_str = captures.get(2).unwrap().as_str();
            
            // Only process if both are numeric literals
            if let (Ok(table_oid), Ok(column_num)) = (table_oid_str.parse::<i32>(), column_num_str.parse::<i32>()) {
                let comment = CommentDdlHandler::get_comment(conn, table_oid, "pg_class", column_num)?;
                let replacement = match comment {
                    Some(text) => {
                        // Text from database already has proper escaping, just wrap in quotes
                        format!("'{}'", text)
                    },
                    None => "NULL".to_string(),
                };
                
                result = result.replace(full_match, &replacement);
            }
            // If either is a variable, leave it unchanged
        }
        
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use crate::ddl::CommentDdlHandler;
    use crate::metadata::ObjectResolver;
    
    fn setup_test_db() -> Connection {
        let mut conn = Connection::open_in_memory().unwrap();
        
        // Create comments table
        conn.execute(
            "CREATE TABLE __pgsqlite_comments (
                object_oid INTEGER NOT NULL,
                catalog_name TEXT NOT NULL,
                subobject_id INTEGER DEFAULT 0,
                comment_text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (object_oid, catalog_name, subobject_id)
            )",
            [],
        ).unwrap();
        
        // Create test table
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)", []).unwrap();
        
        // Add some comments using DDL handler
        CommentDdlHandler::handle_comment_ddl(&mut conn, "COMMENT ON TABLE users IS 'User information table'").unwrap();
        CommentDdlHandler::handle_comment_ddl(&mut conn, "COMMENT ON COLUMN users.name IS 'User''s full name'").unwrap();
        CommentDdlHandler::handle_comment_ddl(&mut conn, "COMMENT ON COLUMN users.email IS 'User''s email address'").unwrap();
        
        conn
    }
    
    #[test]
    fn test_contains_comment_functions() {
        assert!(CommentFunctionHandler::contains_comment_functions(
            "SELECT obj_description(123, 'pg_class')"
        ));
        assert!(CommentFunctionHandler::contains_comment_functions(
            "SELECT obj_description(123)"
        ));
        assert!(CommentFunctionHandler::contains_comment_functions(
            "SELECT col_description(123, 1)"
        ));
        assert!(CommentFunctionHandler::contains_comment_functions(
            "SELECT c.relname, obj_description(c.oid, 'pg_class') FROM pg_class c"
        ));
        
        assert!(!CommentFunctionHandler::contains_comment_functions(
            "SELECT * FROM users"
        ));
    }
    
    #[test]
    fn test_process_obj_description_two_param() {
        let conn = setup_test_db();
        let table_oid = ObjectResolver::resolve_table_oid("users");
        
        let query = format!("SELECT obj_description({}, 'pg_class')", table_oid);
        let result = CommentFunctionHandler::process_comment_functions(&conn, &query).unwrap();
        assert_eq!(result, "SELECT 'User information table'");
        
        // Test with non-existent comment
        let query = "SELECT obj_description(99999, 'pg_class')";
        let result = CommentFunctionHandler::process_comment_functions(&conn, &query).unwrap();
        assert_eq!(result, "SELECT NULL");
    }
    
    #[test]
    fn test_process_col_description() {
        let conn = setup_test_db();
        let table_oid = ObjectResolver::resolve_table_oid("users");
        
        let query = format!("SELECT col_description({}, 2)", table_oid);
        let result = CommentFunctionHandler::process_comment_functions(&conn, &query).unwrap();
        assert_eq!(result, "SELECT 'User''s full name'"); // Note escaped quote
        
        // Test with non-existent comment
        let query = format!("SELECT col_description({}, 1)", table_oid); // id column has no comment
        let result = CommentFunctionHandler::process_comment_functions(&conn, &query).unwrap();
        assert_eq!(result, "SELECT NULL");
    }
    
    #[test]
    fn test_process_single_param_obj_description() {
        let conn = setup_test_db();
        let table_oid = ObjectResolver::resolve_table_oid("users");
        
        let query = format!("SELECT obj_description({})", table_oid);
        let result = CommentFunctionHandler::process_comment_functions(&conn, &query).unwrap();
        assert_eq!(result, "SELECT 'User information table'");
    }
    
    #[test]
    fn test_complex_query_processing() {
        let conn = setup_test_db();
        let table_oid = ObjectResolver::resolve_table_oid("users");
        
        let query = format!(
            "SELECT c.relname, obj_description({}, 'pg_class'), col_description({}, 2) FROM pg_class c",
            table_oid, table_oid
        );
        let result = CommentFunctionHandler::process_comment_functions(&conn, &query).unwrap();
        assert_eq!(result, "SELECT c.relname, 'User information table', 'User''s full name' FROM pg_class c");
    }
    
    #[test]
    fn test_quote_escaping() {
        let mut conn = setup_test_db();
        let table_oid = ObjectResolver::resolve_table_oid("users");
        
        // Add comment with single quote using DDL handler (which handles escaping)
        CommentDdlHandler::handle_comment_ddl(
            &mut conn,
            "COMMENT ON TABLE users IS 'User''s information table'"
        ).unwrap();
        
        let query = format!("SELECT obj_description({}, 'pg_class')", table_oid);
        let result = CommentFunctionHandler::process_comment_functions(&conn, &query).unwrap();
        assert_eq!(result, "SELECT 'User''s information table'"); // Quote escaping preserved from storage
    }
}