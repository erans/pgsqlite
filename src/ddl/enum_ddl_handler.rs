use rusqlite::Connection;
use crate::metadata::EnumMetadata;
use crate::cache::global_enum_cache;
use crate::PgSqliteError;
use tracing::info;
use once_cell::sync::Lazy;
use regex::Regex;

// Pre-compiled regex patterns
static CREATE_TYPE_ENUM_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)CREATE\s+TYPE\s+(\w+)\s+AS\s+ENUM\s*\((.*)\)").unwrap()
});

static ALTER_TYPE_ADD_VALUE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)ALTER\s+TYPE\s+(\w+)\s+ADD\s+VALUE\s+(?:IF\s+NOT\s+EXISTS\s+)?'([^']+)'(?:\s+(BEFORE|AFTER)\s+'([^']+)')?").unwrap()
});

static DROP_TYPE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)DROP\s+TYPE\s+(?:IF\s+EXISTS\s+)?(\w+)(?:\s+CASCADE)?").unwrap()
});

pub struct EnumDdlHandler;

impl EnumDdlHandler {
    /// Check if a query is an ENUM-related DDL statement
    pub fn is_enum_ddl(query: &str) -> bool {
        let upper = query.trim().to_uppercase();
        
        // CREATE TYPE ... AS ENUM
        if upper.starts_with("CREATE TYPE") && upper.contains("AS ENUM") {
            return true;
        }
        
        // ALTER TYPE
        if upper.starts_with("ALTER TYPE") {
            return true;
        }
        
        // DROP TYPE
        if upper.starts_with("DROP TYPE") {
            return true;
        }
        
        false
    }
    
    /// Handle ENUM-related DDL statements
    pub fn handle_enum_ddl(
        conn: &mut Connection,
        query: &str,
    ) -> Result<(), PgSqliteError> {
        let upper = query.trim().to_uppercase();
        
        if upper.starts_with("CREATE TYPE") && upper.contains("AS ENUM") {
            Self::handle_create_type_enum(conn, query)
        } else if upper.starts_with("ALTER TYPE") {
            Self::handle_alter_type(conn, query)
        } else if upper.starts_with("DROP TYPE") {
            Self::handle_drop_type(conn, query)
        } else {
            Err(PgSqliteError::Protocol("Not an ENUM DDL statement".to_string()))
        }
    }
    
    /// Handle CREATE TYPE ... AS ENUM
    fn handle_create_type_enum(
        conn: &mut Connection,
        query: &str,
    ) -> Result<(), PgSqliteError> {
        // Parse the CREATE TYPE statement
        let (type_name, values) = Self::parse_create_type_enum(query)?;
        
        info!("Creating ENUM type '{}' with {} values", type_name, values.len());
        
        // Create the ENUM type
        let type_oid = EnumMetadata::create_enum_type(
            conn,
            &type_name,
            &values.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            None, // Use default namespace
        ).map_err(|e| PgSqliteError::Protocol(format!("Failed to create ENUM type: {e}")))?;
        
        // Clear the cache
        global_enum_cache().invalidate_type(type_oid);
        
        info!("Successfully created ENUM type '{}' with OID {}", type_name, type_oid);
        Ok(())
    }
    
    /// Parse CREATE TYPE ... AS ENUM statement
    fn parse_create_type_enum(query: &str) -> Result<(String, Vec<String>), PgSqliteError> {
        // Simple regex-based parsing for CREATE TYPE
        // Format: CREATE TYPE name AS ENUM ('value1', 'value2', ...)
        
        let captures = CREATE_TYPE_ENUM_REGEX.captures(query)
            .ok_or_else(|| PgSqliteError::Protocol("Invalid CREATE TYPE AS ENUM syntax".to_string()))?;
        
        let type_name = captures.get(1)
            .ok_or_else(|| PgSqliteError::Protocol("Missing type name".to_string()))?
            .as_str()
            .to_string();
        
        let values_str = captures.get(2)
            .ok_or_else(|| PgSqliteError::Protocol("Missing enum values".to_string()))?
            .as_str();
        
        // Parse the values list
        let values = Self::parse_enum_values(values_str)?;
        
        if values.is_empty() {
            return Err(PgSqliteError::Protocol("ENUM type must have at least one value".to_string()));
        }
        
        Ok((type_name, values))
    }
    
    /// Parse enum values from a comma-separated list of quoted strings
    fn parse_enum_values(values_str: &str) -> Result<Vec<String>, PgSqliteError> {
        let mut values = Vec::new();
        let mut current_value = String::new();
        let mut in_quotes = false;
        let mut chars = values_str.chars().peekable();
        
        while let Some(ch) = chars.next() {
            match ch {
                '\'' => {
                    if in_quotes {
                        // Check if next char is also a quote (escaped quote)
                        if chars.peek() == Some(&'\'') {
                            current_value.push('\'');
                            chars.next(); // consume the second quote
                        } else {
                            // End of quoted value
                            in_quotes = false;
                            values.push(current_value.clone());
                            current_value.clear();
                        }
                    } else {
                        // Start of quoted value
                        in_quotes = true;
                    }
                }
                ',' if !in_quotes => {
                    // Comma outside quotes, ignore
                }
                _ if in_quotes => {
                    current_value.push(ch);
                }
                _ => {
                    // Ignore whitespace outside quotes
                    if !ch.is_whitespace() {
                        return Err(PgSqliteError::Protocol(
                            format!("Invalid character '{ch}' outside quotes")
                        ));
                    }
                }
            }
        }
        
        if in_quotes {
            return Err(PgSqliteError::Protocol("Unterminated quoted string".to_string()));
        }
        
        Ok(values)
    }
    
    /// Handle ALTER TYPE statements
    fn handle_alter_type(
        conn: &mut Connection,
        query: &str,
    ) -> Result<(), PgSqliteError> {
        // Parse ALTER TYPE for ADD VALUE
        if let Some(captures) = ALTER_TYPE_ADD_VALUE_REGEX.captures(query) {
            let type_name = captures.get(1).unwrap().as_str();
            let new_value = captures.get(2).unwrap().as_str();
            let position = captures.get(3).map(|m| m.as_str());
            let relative_value = captures.get(4).map(|m| m.as_str());
            
            info!("Adding value '{}' to ENUM type '{}'", new_value, type_name);
            
            // Get type OID for cache invalidation
            let type_oid = EnumMetadata::get_enum_type(conn, type_name)
                .map_err(|e| PgSqliteError::Protocol(format!("Failed to get ENUM type: {e}")))?
                .ok_or_else(|| PgSqliteError::Protocol(format!("Type '{type_name}' does not exist")))?
                .type_oid;
            
            // Add the value
            match position {
                Some("BEFORE") => {
                    EnumMetadata::add_enum_value(conn, type_name, new_value, relative_value, None)
                }
                Some("AFTER") => {
                    EnumMetadata::add_enum_value(conn, type_name, new_value, None, relative_value)
                }
                _ => {
                    EnumMetadata::add_enum_value(conn, type_name, new_value, None, None)
                }
            }.map_err(|e| PgSqliteError::Protocol(format!("Failed to add ENUM value: {e}")))?;
            
            // Clear the cache
            global_enum_cache().invalidate_type(type_oid);
            
            info!("Successfully added value '{}' to ENUM type '{}'", new_value, type_name);
            return Ok(());
        }
        
        // TODO: Handle RENAME VALUE, RENAME TO, etc.
        Err(PgSqliteError::Protocol("Unsupported ALTER TYPE operation".to_string()))
    }
    
    /// Handle DROP TYPE statements
    fn handle_drop_type(
        conn: &mut Connection,
        query: &str,
    ) -> Result<(), PgSqliteError> {
        // Parse DROP TYPE
        let captures = DROP_TYPE_REGEX.captures(query)
            .ok_or_else(|| PgSqliteError::Protocol("Invalid DROP TYPE syntax".to_string()))?;
        
        let type_name = captures.get(1).unwrap().as_str();
        let if_exists = query.to_uppercase().contains("IF EXISTS");
        let cascade = query.to_uppercase().contains("CASCADE");
        
        info!("Dropping ENUM type '{}'", type_name);
        
        // Check if type exists
        let enum_type = EnumMetadata::get_enum_type(conn, type_name)
            .map_err(|e| PgSqliteError::Protocol(format!("Failed to get ENUM type: {e}")))?;
        
        if let Some(et) = enum_type {
            // Check if type is used in any tables (unless CASCADE)
            if !cascade {
                // Check for dependencies in __pgsqlite_enum_usage table
                let check_sql = "
                    SELECT table_name 
                    FROM __pgsqlite_enum_usage 
                    WHERE enum_type = ?1
                ";
                
                let mut stmt = conn.prepare(check_sql)
                    .map_err(|e| PgSqliteError::Protocol(format!("Failed to prepare dependency check: {e}")))?;
                
                let dependent_tables: Vec<String> = stmt.query_map([type_name], |row| {
                    row.get::<_, String>(0)
                }).map_err(|e| PgSqliteError::Protocol(format!("Failed to check dependencies: {e}")))?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| PgSqliteError::Protocol(format!("Failed to collect dependencies: {e}")))?;
                
                if !dependent_tables.is_empty() {
                    return Err(PgSqliteError::Protocol(
                        format!("cannot drop type {type_name} because other objects depend on it")
                    ));
                }
            } else {
                // CASCADE specified - drop all triggers and clean up usage
                let usage_sql = "
                    SELECT table_name, column_name 
                    FROM __pgsqlite_enum_usage 
                    WHERE enum_type = ?1
                ";
                
                let mut stmt = conn.prepare(usage_sql)
                    .map_err(|e| PgSqliteError::Protocol(format!("Failed to prepare usage query: {e}")))?;
                
                let usages: Vec<(String, String)> = stmt.query_map([type_name], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                }).map_err(|e| PgSqliteError::Protocol(format!("Failed to query usage: {e}")))?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| PgSqliteError::Protocol(format!("Failed to collect usage: {e}")))?;
                
                // Drop all triggers
                for (table_name, column_name) in usages {
                    crate::metadata::EnumTriggers::drop_enum_validation_triggers(
                        conn, &table_name, &column_name, type_name
                    ).ok(); // Ignore errors on trigger drop
                }
                
                // Clean up enum usage records
                conn.execute("DELETE FROM __pgsqlite_enum_usage WHERE enum_type = ?1", [type_name])
                    .map_err(|e| PgSqliteError::Protocol(format!("Failed to clean enum usage: {e}")))?;
            }
            
            // Drop the type
            EnumMetadata::drop_enum_type(conn, type_name)
                .map_err(|e| PgSqliteError::Protocol(format!("Failed to drop ENUM type: {e}")))?;
            
            // Clear the cache
            global_enum_cache().invalidate_type(et.type_oid);
            
            info!("Successfully dropped ENUM type '{}'", type_name);
        } else if !if_exists {
            return Err(PgSqliteError::Protocol(format!("Type '{type_name}' does not exist")));
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_enum_values() {
        let values_str = "'happy', 'sad', 'angry'";
        let values = EnumDdlHandler::parse_enum_values(values_str).unwrap();
        assert_eq!(values, vec!["happy", "sad", "angry"]);
        
        // Test with spaces
        let values_str = "  'one'  ,  'two'  ,  'three'  ";
        let values = EnumDdlHandler::parse_enum_values(values_str).unwrap();
        assert_eq!(values, vec!["one", "two", "three"]);
        
        // Test with escaped quotes
        let values_str = "'it''s', 'quote\"test'";
        let values = EnumDdlHandler::parse_enum_values(values_str).unwrap();
        assert_eq!(values, vec!["it's", "quote\"test"]);
    }
    
    #[test]
    fn test_parse_create_type_enum() {
        let query = "CREATE TYPE mood AS ENUM ('happy', 'sad', 'angry')";
        let (type_name, values) = EnumDdlHandler::parse_create_type_enum(query).unwrap();
        assert_eq!(type_name, "mood");
        assert_eq!(values, vec!["happy", "sad", "angry"]);
        
        // Test with whitespace
        let query = "CREATE TYPE   status   AS   ENUM   (  'pending'  ,  'active'  )";
        let (type_name, values) = EnumDdlHandler::parse_create_type_enum(query).unwrap();
        assert_eq!(type_name, "status");
        assert_eq!(values, vec!["pending", "active"]);
    }
    
    #[test]
    fn test_is_enum_ddl() {
        assert!(EnumDdlHandler::is_enum_ddl("CREATE TYPE mood AS ENUM ('happy')"));
        assert!(EnumDdlHandler::is_enum_ddl("create type status as enum ('pending')"));
        assert!(EnumDdlHandler::is_enum_ddl("ALTER TYPE mood ADD VALUE 'neutral'"));
        assert!(EnumDdlHandler::is_enum_ddl("DROP TYPE mood"));
        assert!(EnumDdlHandler::is_enum_ddl("drop type if exists mood cascade"));
        
        assert!(!EnumDdlHandler::is_enum_ddl("CREATE TABLE test (id INT)"));
        assert!(!EnumDdlHandler::is_enum_ddl("CREATE TYPE point AS (x INT, y INT)"));
        assert!(!EnumDdlHandler::is_enum_ddl("SELECT * FROM test"));
    }
}