use rusqlite::{Connection, OptionalExtension};
use crate::metadata::ObjectResolver;
use crate::PgSqliteError;
use tracing::{debug, info};
use once_cell::sync::Lazy;
use regex::Regex;

// Pre-compiled regex patterns for COMMENT ON statements
static COMMENT_ON_TABLE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)COMMENT\s+ON\s+TABLE\s+(\w+)\s+IS\s+(?:'((?:''|[^'])*)'|NULL)").unwrap()
});

static COMMENT_ON_COLUMN_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)COMMENT\s+ON\s+COLUMN\s+(\w+)\.(\w+)\s+IS\s+(?:'((?:''|[^'])*)'|NULL)").unwrap()
});

static COMMENT_ON_FUNCTION_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)COMMENT\s+ON\s+FUNCTION\s+(\w+)\s*\([^)]*\)\s+IS\s+(?:'((?:''|[^'])*)'|NULL)").unwrap()
});

pub struct CommentDdlHandler;

impl CommentDdlHandler {
    /// Check if a query is a COMMENT ON statement
    pub fn is_comment_ddl(query: &str) -> bool {
        let trimmed = query.trim().to_uppercase();
        trimmed.starts_with("COMMENT ON")
    }
    
    /// Handle COMMENT ON statements
    pub fn handle_comment_ddl(
        conn: &mut Connection,
        query: &str,
    ) -> Result<(), PgSqliteError> {
        let trimmed = query.trim().to_uppercase();
        
        if trimmed.contains("COMMENT ON TABLE") {
            Self::handle_comment_on_table(conn, query)
        } else if trimmed.contains("COMMENT ON COLUMN") {
            Self::handle_comment_on_column(conn, query)
        } else if trimmed.contains("COMMENT ON FUNCTION") {
            Self::handle_comment_on_function(conn, query)
        } else {
            Err(PgSqliteError::Protocol(
                "Unsupported COMMENT ON statement type".to_string()
            ))
        }
    }
    
    /// Handle COMMENT ON TABLE statements
    fn handle_comment_on_table(
        conn: &mut Connection,
        query: &str,
    ) -> Result<(), PgSqliteError> {
        debug!("Parsing COMMENT ON TABLE: {}", query);
        
        let captures = COMMENT_ON_TABLE_REGEX
            .captures(query)
            .ok_or_else(|| PgSqliteError::Protocol("Invalid COMMENT ON TABLE syntax".to_string()))?;
            
        let table_name = captures.get(1).unwrap().as_str();
        let comment_text = captures.get(2).map(|m| m.as_str());
        
        info!("Setting comment on table '{}': {:?}", table_name, comment_text);
        
        // Validate table exists
        if !ObjectResolver::table_exists(conn, table_name)? {
            return Err(PgSqliteError::Protocol(format!(
                "Table '{}' does not exist", 
                table_name
            )));
        }
        
        let table_oid = ObjectResolver::resolve_table_oid(table_name);
        
        if let Some(comment) = comment_text {
            // Set or update comment
            Self::set_comment(conn, table_oid, "pg_class", 0, Some(comment))?;
        } else {
            // Remove comment (IS NULL)
            Self::set_comment(conn, table_oid, "pg_class", 0, None)?;
        }
        
        Ok(())
    }
    
    /// Handle COMMENT ON COLUMN statements  
    fn handle_comment_on_column(
        conn: &mut Connection,
        query: &str,
    ) -> Result<(), PgSqliteError> {
        debug!("Parsing COMMENT ON COLUMN: {}", query);
        
        let captures = COMMENT_ON_COLUMN_REGEX
            .captures(query)
            .ok_or_else(|| PgSqliteError::Protocol("Invalid COMMENT ON COLUMN syntax".to_string()))?;
            
        let table_name = captures.get(1).unwrap().as_str();
        let column_name = captures.get(2).unwrap().as_str();
        let comment_text = captures.get(3).map(|m| m.as_str());
        
        info!("Setting comment on column '{}.{}': {:?}", table_name, column_name, comment_text);
        
        // Validate table and column exist
        let (table_oid, column_number) = ObjectResolver::resolve_column_oid(conn, table_name, column_name)?;
        
        if let Some(comment) = comment_text {
            // Set or update comment
            Self::set_comment(conn, table_oid, "pg_class", column_number, Some(comment))?;
        } else {
            // Remove comment (IS NULL)
            Self::set_comment(conn, table_oid, "pg_class", column_number, None)?;
        }
        
        Ok(())
    }
    
    /// Handle COMMENT ON FUNCTION statements (basic support)
    fn handle_comment_on_function(
        conn: &mut Connection,
        query: &str,
    ) -> Result<(), PgSqliteError> {
        debug!("Parsing COMMENT ON FUNCTION: {}", query);
        
        let captures = COMMENT_ON_FUNCTION_REGEX
            .captures(query)
            .ok_or_else(|| PgSqliteError::Protocol("Invalid COMMENT ON FUNCTION syntax".to_string()))?;
            
        let function_name = captures.get(1).unwrap().as_str();
        let comment_text = captures.get(2).map(|m| m.as_str());
        
        info!("Setting comment on function '{}': {:?}", function_name, comment_text);
        
        // Generate function OID (simplified - just hash the name)
        let function_oid = ObjectResolver::resolve_table_oid(&format!("function_{}", function_name));
        
        if let Some(comment) = comment_text {
            // Set or update comment
            Self::set_comment(conn, function_oid, "pg_proc", 0, Some(comment))?;
        } else {
            // Remove comment (IS NULL)  
            Self::set_comment(conn, function_oid, "pg_proc", 0, None)?;
        }
        
        Ok(())
    }
    
    /// Set or remove a comment in the database
    pub fn set_comment(
        conn: &mut Connection,
        object_oid: i32,
        catalog_name: &str,
        subobject_id: i32,
        comment_text: Option<&str>,
    ) -> Result<(), PgSqliteError> {
        if let Some(comment) = comment_text {
            // Insert or update comment
            debug!("Setting comment for OID {} in catalog {}: '{}'", object_oid, catalog_name, comment);
            conn.execute(
                "INSERT OR REPLACE INTO __pgsqlite_comments 
                 (object_oid, catalog_name, subobject_id, comment_text, updated_at) 
                 VALUES (?1, ?2, ?3, ?4, CURRENT_TIMESTAMP)",
                rusqlite::params![object_oid, catalog_name, subobject_id, comment],
            )?;
        } else {
            // Remove comment
            debug!("Removing comment for OID {} in catalog {}", object_oid, catalog_name);
            conn.execute(
                "DELETE FROM __pgsqlite_comments 
                 WHERE object_oid = ?1 AND catalog_name = ?2 AND subobject_id = ?3",
                rusqlite::params![object_oid, catalog_name, subobject_id],
            )?;
        }
        Ok(())
    }
    
    /// Get a comment from the database
    pub fn get_comment(
        conn: &Connection,
        object_oid: i32,
        catalog_name: &str,
        subobject_id: i32,
    ) -> Result<Option<String>, PgSqliteError> {
        let comment: Option<String> = conn.query_row(
            "SELECT comment_text FROM __pgsqlite_comments 
             WHERE object_oid = ?1 AND catalog_name = ?2 AND subobject_id = ?3",
            rusqlite::params![object_oid, catalog_name, subobject_id],
            |row| row.get(0)
        ).optional()?;
        Ok(comment)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    
    fn setup_test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        
        // Create comments table (simulate migration)
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
        
        conn
    }
    
    #[test]
    fn test_is_comment_ddl() {
        assert!(CommentDdlHandler::is_comment_ddl("COMMENT ON TABLE users IS 'User table'"));
        assert!(CommentDdlHandler::is_comment_ddl("comment on column users.name is 'User name'"));
        assert!(CommentDdlHandler::is_comment_ddl("  COMMENT ON FUNCTION foo() IS NULL  "));
        
        assert!(!CommentDdlHandler::is_comment_ddl("SELECT * FROM users"));
        assert!(!CommentDdlHandler::is_comment_ddl("CREATE TABLE test (id INT)"));
    }
    
    #[test]
    fn test_comment_on_table() {
        let mut conn = setup_test_db();
        
        // Set table comment
        CommentDdlHandler::handle_comment_ddl(
            &mut conn,
            "COMMENT ON TABLE users IS 'This is the user table'"
        ).unwrap();
        
        // Verify comment was stored
        let table_oid = ObjectResolver::resolve_table_oid("users");
        let comment = CommentDdlHandler::get_comment(&conn, table_oid, "pg_class", 0).unwrap();
        assert_eq!(comment, Some("This is the user table".to_string()));
        
        // Remove comment
        CommentDdlHandler::handle_comment_ddl(
            &mut conn,
            "COMMENT ON TABLE users IS NULL"
        ).unwrap();
        
        // Verify comment was removed
        let comment = CommentDdlHandler::get_comment(&conn, table_oid, "pg_class", 0).unwrap();
        assert_eq!(comment, None);
    }
    
    #[test]
    fn test_comment_on_column() {
        let mut conn = setup_test_db();
        
        // Set column comment
        CommentDdlHandler::handle_comment_ddl(
            &mut conn,
            "COMMENT ON COLUMN users.email IS 'User email address'"
        ).unwrap();
        
        // Verify comment was stored
        let (table_oid, column_num) = ObjectResolver::resolve_column_oid(&conn, "users", "email").unwrap();
        let comment = CommentDdlHandler::get_comment(&conn, table_oid, "pg_class", column_num).unwrap();
        assert_eq!(comment, Some("User email address".to_string()));
        
        // Test case insensitive
        CommentDdlHandler::handle_comment_ddl(
            &mut conn,
            "COMMENT ON COLUMN users.EMAIL IS 'Updated email comment'"
        ).unwrap();
        
        let comment = CommentDdlHandler::get_comment(&conn, table_oid, "pg_class", column_num).unwrap();
        assert_eq!(comment, Some("Updated email comment".to_string()));
    }
    
    #[test]
    fn test_comment_on_function() {
        let mut conn = setup_test_db();
        
        // Set function comment
        CommentDdlHandler::handle_comment_ddl(
            &mut conn,
            "COMMENT ON FUNCTION calculate_age(date) IS 'Calculates age from birth date'"
        ).unwrap();
        
        // Verify comment was stored
        let function_oid = ObjectResolver::resolve_table_oid("function_calculate_age");
        let comment = CommentDdlHandler::get_comment(&conn, function_oid, "pg_proc", 0).unwrap();
        assert_eq!(comment, Some("Calculates age from birth date".to_string()));
    }
    
    #[test]
    fn test_error_cases() {
        let mut conn = setup_test_db();
        
        // Test non-existent table
        let result = CommentDdlHandler::handle_comment_ddl(
            &mut conn,
            "COMMENT ON TABLE nonexistent IS 'Should fail'"
        );
        assert!(result.is_err());
        
        // Test non-existent column
        let result = CommentDdlHandler::handle_comment_ddl(
            &mut conn,
            "COMMENT ON COLUMN users.nonexistent IS 'Should fail'"
        );
        assert!(result.is_err());
        
        // Test invalid syntax
        let result = CommentDdlHandler::handle_comment_ddl(
            &mut conn,
            "COMMENT ON INVALID SYNTAX"
        );
        assert!(result.is_err());
    }
}