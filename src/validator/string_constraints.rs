use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use rusqlite::Connection;
use crate::error::PgError;

/// Represents a string constraint for a column
#[derive(Debug, Clone)]
pub struct StringConstraint {
    pub table_name: String,
    pub column_name: String,
    pub max_length: i32,
    pub is_char_type: bool,  // true for CHAR (needs padding), false for VARCHAR
}

/// Cache for string constraints to avoid repeated database queries
pub struct StringConstraintValidator {
    constraints: Arc<RwLock<HashMap<String, HashMap<String, StringConstraint>>>>,
}

impl Default for StringConstraintValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl StringConstraintValidator {
    pub fn new() -> Self {
        Self {
            constraints: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Load constraints for a table from the database
    pub fn load_table_constraints(&self, conn: &Connection, table_name: &str) -> Result<(), rusqlite::Error> {
        // First check if we have the string constraints table (migration v6)
        let has_constraints_table = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='__pgsqlite_string_constraints'",
            [],
            |row| row.get::<_, i32>(0)
        )? > 0;
        
        if !has_constraints_table {
            // No constraints table, nothing to load
            return Ok(());
        }
        
        // Query string constraints
        let mut stmt = conn.prepare(
            "SELECT column_name, max_length, is_char_type 
             FROM __pgsqlite_string_constraints 
             WHERE table_name = ?1"
        )?;
        
        let constraints_result = stmt.query_map([table_name], |row| {
            Ok(StringConstraint {
                table_name: table_name.to_string(),
                column_name: row.get(0)?,
                max_length: row.get(1)?,
                is_char_type: row.get(2)?,
            })
        })?;
        
        let mut table_constraints = HashMap::new();
        for constraint in constraints_result {
            let constraint = constraint?;
            table_constraints.insert(constraint.column_name.clone(), constraint);
        }
        
        // Update cache
        if !table_constraints.is_empty() {
            let mut cache = self.constraints.write().unwrap();
            cache.insert(table_name.to_string(), table_constraints);
        }
        
        Ok(())
    }
    
    /// Populate the string constraints table from __pgsqlite_schema
    pub fn populate_constraints_from_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
        // Check if type_modifier column exists
        let has_type_modifier = conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('__pgsqlite_schema') WHERE name = 'type_modifier'",
            [],
            |row| row.get::<_, i32>(0)
        )? > 0;
        
        if !has_type_modifier {
            return Ok(());  // Old schema, no constraints to populate
        }
        
        // Get all string types with modifiers
        let mut stmt = conn.prepare(
            "SELECT table_name, column_name, pg_type, type_modifier 
             FROM __pgsqlite_schema 
             WHERE type_modifier IS NOT NULL 
             AND pg_type IN ('varchar', 'char', 'character varying', 'character', 'nvarchar')"
        )?;
        
        let constraints = stmt.query_map([], |row| {
            let pg_type: String = row.get(2)?;
            let is_char = pg_type.to_lowercase() == "char" || pg_type.to_lowercase() == "character";
            
            Ok((
                row.get::<_, String>(0)?,  // table_name
                row.get::<_, String>(1)?,  // column_name
                row.get::<_, i32>(3)?,     // type_modifier (max_length)
                is_char,
            ))
        })?;
        
        // Insert into string constraints table
        for constraint in constraints {
            let (table_name, column_name, max_length, is_char) = constraint?;
            conn.execute(
                "INSERT OR REPLACE INTO __pgsqlite_string_constraints 
                 (table_name, column_name, max_length, is_char_type) 
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![table_name, column_name, max_length, is_char as i32],
            )?;
        }
        
        Ok(())
    }
    
    /// Validate a value against constraints
    pub fn validate_value(
        &self,
        table_name: &str,
        column_name: &str,
        value: &str,
    ) -> Result<(), PgError> {
        // Check cache
        let cache = self.constraints.read().unwrap();
        
        if let Some(table_constraints) = cache.get(table_name) {
            if let Some(constraint) = table_constraints.get(column_name) {
                // Count characters (not bytes) for PostgreSQL compatibility
                let char_count = value.chars().count() as i32;
                
                if char_count > constraint.max_length {
                    let type_name = if constraint.is_char_type {
                        format!("character({})", constraint.max_length)
                    } else {
                        format!("character varying({})", constraint.max_length)
                    };
                    
                    return Err(PgError::StringDataRightTruncation {
                        type_name,
                        column_name: column_name.to_string(),
                        actual_length: char_count,
                        max_length: constraint.max_length,
                    });
                }
            }
        }
        
        Ok(())
    }
    
    /// Pad CHAR values to their defined length
    pub fn pad_char_value(&self, table_name: &str, column_name: &str, value: &str) -> String {
        let cache = self.constraints.read().unwrap();
        
        if let Some(table_constraints) = cache.get(table_name) {
            if let Some(constraint) = table_constraints.get(column_name) {
                if constraint.is_char_type {
                    let char_count = value.chars().count() as i32;
                    if char_count < constraint.max_length {
                        // Pad with spaces to reach the required length
                        let padding = constraint.max_length - char_count;
                        return format!("{}{}", value, " ".repeat(padding as usize));
                    }
                }
            }
        }
        
        value.to_string()
    }
    
    /// Check if a table has any string constraints
    pub fn table_has_constraints(&self, table_name: &str) -> bool {
        let cache = self.constraints.read().unwrap();
        cache.contains_key(table_name)
    }
    
    /// Clear cache for a specific table (e.g., after ALTER TABLE)
    pub fn invalidate_table(&self, table_name: &str) {
        let mut cache = self.constraints.write().unwrap();
        cache.remove(table_name);
    }
    
    /// Clear entire cache
    pub fn clear_cache(&self) {
        let mut cache = self.constraints.write().unwrap();
        cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    
    fn setup_test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        
        // Create the constraints table
        conn.execute(
            "CREATE TABLE __pgsqlite_string_constraints (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                max_length INTEGER NOT NULL,
                is_char_type BOOLEAN NOT NULL DEFAULT 0,
                PRIMARY KEY (table_name, column_name)
            )",
            [],
        ).unwrap();
        
        conn
    }
    
    #[test]
    fn test_validate_value_within_constraint() {
        let validator = StringConstraintValidator::new();
        let conn = setup_test_db();
        
        // Add a constraint
        conn.execute(
            "INSERT INTO __pgsqlite_string_constraints (table_name, column_name, max_length, is_char_type) 
             VALUES ('users', 'name', 10, 0)",
            [],
        ).unwrap();
        
        // Load constraints
        validator.load_table_constraints(&conn, "users").unwrap();
        
        // Valid value
        assert!(validator.validate_value("users", "name", "John").is_ok());
        assert!(validator.validate_value("users", "name", "1234567890").is_ok()); // Exactly 10
    }
    
    #[test]
    fn test_validate_value_exceeds_constraint() {
        let validator = StringConstraintValidator::new();
        let conn = setup_test_db();
        
        // Add a constraint
        conn.execute(
            "INSERT INTO __pgsqlite_string_constraints (table_name, column_name, max_length, is_char_type) 
             VALUES ('users', 'name', 5, 0)",
            [],
        ).unwrap();
        
        // Load constraints
        validator.load_table_constraints(&conn, "users").unwrap();
        
        // Invalid value
        let result = validator.validate_value("users", "name", "TooLong");
        assert!(result.is_err());
        
        if let Err(PgError::StringDataRightTruncation { type_name, column_name, actual_length, max_length }) = result {
            assert_eq!(type_name, "character varying(5)");
            assert_eq!(column_name, "name");
            assert_eq!(actual_length, 7);
            assert_eq!(max_length, 5);
        } else {
            panic!("Expected StringDataRightTruncation error");
        }
    }
    
    #[test]
    fn test_multibyte_character_counting() {
        let validator = StringConstraintValidator::new();
        let conn = setup_test_db();
        
        // Add a constraint
        conn.execute(
            "INSERT INTO __pgsqlite_string_constraints (table_name, column_name, max_length, is_char_type) 
             VALUES ('test', 'text', 3, 0)",
            [],
        ).unwrap();
        
        // Load constraints
        validator.load_table_constraints(&conn, "test").unwrap();
        
        // Test with multi-byte characters
        assert!(validator.validate_value("test", "text", "你好世").is_ok()); // 3 characters
        assert!(validator.validate_value("test", "text", "你好世界").is_err()); // 4 characters
        assert!(validator.validate_value("test", "text", "😀😀😀").is_ok()); // 3 emojis
        assert!(validator.validate_value("test", "text", "café").is_err()); // 4 characters
    }
    
    #[test]
    fn test_char_padding() {
        let validator = StringConstraintValidator::new();
        let conn = setup_test_db();
        
        // Add a CHAR constraint
        conn.execute(
            "INSERT INTO __pgsqlite_string_constraints (table_name, column_name, max_length, is_char_type) 
             VALUES ('test', 'code', 5, 1)",
            [],
        ).unwrap();
        
        // Load constraints
        validator.load_table_constraints(&conn, "test").unwrap();
        
        // Test padding
        assert_eq!(validator.pad_char_value("test", "code", "AB"), "AB   ");
        assert_eq!(validator.pad_char_value("test", "code", "12345"), "12345");
        assert_eq!(validator.pad_char_value("test", "code", ""), "     ");
    }
    
    #[test]
    fn test_no_constraint() {
        let validator = StringConstraintValidator::new();
        let conn = setup_test_db();
        
        // No constraints loaded
        validator.load_table_constraints(&conn, "users").unwrap();
        
        // Should always pass validation
        assert!(validator.validate_value("users", "name", "Any length string should be OK").is_ok());
    }
    
    #[test]
    fn test_cache_invalidation() {
        let validator = StringConstraintValidator::new();
        let conn = setup_test_db();
        
        // Add a constraint
        conn.execute(
            "INSERT INTO __pgsqlite_string_constraints (table_name, column_name, max_length, is_char_type) 
             VALUES ('users', 'name', 10, 0)",
            [],
        ).unwrap();
        
        // Load constraints
        validator.load_table_constraints(&conn, "users").unwrap();
        assert!(validator.table_has_constraints("users"));
        
        // Invalidate
        validator.invalidate_table("users");
        assert!(!validator.table_has_constraints("users"));
    }
}