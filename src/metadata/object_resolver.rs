use rusqlite::Connection;
use crate::PgSqliteError;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tracing::debug;

/// Resolves database object names to their corresponding OIDs for comment storage
pub struct ObjectResolver;

impl ObjectResolver {
    /// Resolve table name to OID using the same algorithm as pg_class view
    pub fn resolve_table_oid(table_name: &str) -> i32 {
        generate_table_oid(table_name)
    }
    
    /// Resolve column to (table_oid, column_number)
    pub fn resolve_column_oid(
        conn: &Connection, 
        table_name: &str, 
        column_name: &str
    ) -> Result<(i32, i32), PgSqliteError> {
        let table_oid = Self::resolve_table_oid(table_name);
        let column_number = Self::get_column_number(conn, table_name, column_name)?;
        Ok((table_oid, column_number))
    }
    
    /// Get column number (1-based) for a column in a table
    fn get_column_number(
        conn: &Connection,
        table_name: &str,
        column_name: &str,
    ) -> Result<i32, PgSqliteError> {
        debug!("Resolving column number for {}.{}", table_name, column_name);
        
        // Query PRAGMA table_info to get column info
        // Note: PRAGMA statements don't support parameters, so we need to format the query
        let pragma_query = format!("PRAGMA table_info('{}')", table_name);
        let mut stmt = conn.prepare(&pragma_query)?;
        let column_info: Vec<(i32, String)> = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i32>("cid")? + 1, // Convert to 1-based indexing
                row.get::<_, String>("name")?,
            ))
        })?.collect::<Result<Vec<_>, _>>()?;
        
        // Find the column by name
        for (column_num, col_name) in column_info {
            if col_name.eq_ignore_ascii_case(column_name) {
                debug!("Found column {} at position {}", column_name, column_num);
                return Ok(column_num);
            }
        }
        
        Err(PgSqliteError::Protocol(format!(
            "Column '{}' not found in table '{}'", 
            column_name, 
            table_name
        )))
    }
    
    /// Validate that a table exists
    pub fn table_exists(conn: &Connection, table_name: &str) -> Result<bool, PgSqliteError> {
        let count: i32 = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ? COLLATE NOCASE",
            [table_name],
            |row| row.get(0)
        )?;
        Ok(count > 0)
    }
    
    /// Get all tables in the database
    pub fn get_all_tables(conn: &Connection) -> Result<Vec<String>, PgSqliteError> {
        let mut stmt = conn.prepare(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '__pgsqlite_%'"
        )?;
        let tables: Vec<String> = stmt.query_map([], |row| {
            row.get::<_, String>(0)
        })?.collect::<Result<Vec<_>, _>>()?;
        Ok(tables)
    }
}

/// Generate a stable OID from table name using the same algorithm as pg_class view
fn generate_table_oid(name: &str) -> i32 {
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    // Keep it positive and in a reasonable range for OIDs
    // Use same algorithm as existing catalog system
    ((hasher.finish() & 0x7FFFFFFF) % 1000000 + 16384) as i32
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    
    #[test]
    fn test_generate_table_oid() {
        // Test deterministic OID generation
        let oid1 = ObjectResolver::resolve_table_oid("users");
        let oid2 = ObjectResolver::resolve_table_oid("users");
        assert_eq!(oid1, oid2);
        
        // Different names should produce different OIDs
        let oid3 = ObjectResolver::resolve_table_oid("posts");
        assert_ne!(oid1, oid3);
        
        // OIDs should be in expected range
        assert!(oid1 >= 16384);
        assert!(oid1 < 1016384);
    }
    
    #[test]
    fn test_table_exists() {
        let conn = Connection::open_in_memory().unwrap();
        
        // Create test table
        conn.execute("CREATE TABLE test_table (id INTEGER, name TEXT)", []).unwrap();
        
        // Test table existence
        assert!(ObjectResolver::table_exists(&conn, "test_table").unwrap());
        assert!(!ObjectResolver::table_exists(&conn, "nonexistent_table").unwrap());
        
        // Test case insensitive
        assert!(ObjectResolver::table_exists(&conn, "TEST_TABLE").unwrap());
    }
    
    #[test]
    fn test_resolve_column_oid() {
        let conn = Connection::open_in_memory().unwrap();
        
        // Create test table
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)", []).unwrap();
        
        // Test column resolution
        let (_table_oid, col_num) = ObjectResolver::resolve_column_oid(&conn, "users", "id").unwrap();
        assert_eq!(col_num, 1); // First column (1-based)
        
        let (_, col_num2) = ObjectResolver::resolve_column_oid(&conn, "users", "name").unwrap();
        assert_eq!(col_num2, 2); // Second column
        
        let (_, col_num3) = ObjectResolver::resolve_column_oid(&conn, "users", "email").unwrap();
        assert_eq!(col_num3, 3); // Third column
        
        // Test case insensitive
        let (_, col_num4) = ObjectResolver::resolve_column_oid(&conn, "users", "EMAIL").unwrap();
        assert_eq!(col_num4, 3);
        
        // Test error for non-existent column
        assert!(ObjectResolver::resolve_column_oid(&conn, "users", "nonexistent").is_err());
    }
    
    #[test] 
    fn test_get_all_tables() {
        let conn = Connection::open_in_memory().unwrap();
        
        // Create test tables
        conn.execute("CREATE TABLE users (id INTEGER)", []).unwrap();
        conn.execute("CREATE TABLE posts (id INTEGER)", []).unwrap();
        conn.execute("CREATE TABLE __pgsqlite_metadata (key TEXT)", []).unwrap(); // Should be filtered out
        
        let tables = ObjectResolver::get_all_tables(&conn).unwrap();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"posts".to_string()));
        assert!(!tables.contains(&"__pgsqlite_metadata".to_string()));
    }
}