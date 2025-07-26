use rusqlite::Connection;
use crate::metadata::EnumMetadata;
use crate::PgSqliteError;
use regex::Regex;

pub struct EnumValidator;

impl EnumValidator {
    /// Validate INSERT statement for ENUM values
    pub fn validate_insert(
        conn: &Connection,
        query: &str,
    ) -> Result<(), PgSqliteError> {
        // Parse INSERT statement to extract table and values
        let insert_regex = Regex::new(
            r"(?i)INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)"
        ).map_err(|e| PgSqliteError::Protocol(format!("Regex error: {e}")))?;
        
        if let Some(captures) = insert_regex.captures(query) {
            let table_name = captures.get(1).unwrap().as_str();
            let columns_str = captures.get(2).unwrap().as_str();
            let values_str = captures.get(3).unwrap().as_str();
            
            // Parse columns
            let columns: Vec<&str> = columns_str.split(',')
                .map(|c| c.trim())
                .collect();
            
            // Parse values (simple parsing, doesn't handle all cases)
            let values: Vec<&str> = values_str.split(',')
                .map(|v| v.trim())
                .collect();
            
            if columns.len() != values.len() {
                return Err(PgSqliteError::Protocol("Column count doesn't match value count".to_string()));
            }
            
            // Check each column for ENUM types
            for (i, column) in columns.iter().enumerate() {
                if let Ok(Some(pg_type)) = Self::get_column_type(conn, table_name, column) {
                    // Check if it's an ENUM type
                    if let Ok(Some(enum_type)) = EnumMetadata::get_enum_type(conn, &pg_type) {
                        // Extract the value (remove quotes)
                        let value = values[i].trim_matches('\'').trim_matches('"');
                        
                        // Validate the value
                        if !EnumMetadata::is_valid_enum_value(conn, enum_type.type_oid, value)? {
                            return Err(PgSqliteError::Protocol(
                                format!("invalid input value for enum {pg_type}: \"{value}\"")
                            ));
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Get the PostgreSQL type of a column from __pgsqlite_schema
    fn get_column_type(conn: &Connection, table_name: &str, column_name: &str) -> Result<Option<String>, rusqlite::Error> {
        let mut stmt = conn.prepare(
            "SELECT pg_type FROM __pgsqlite_schema WHERE table_name = ?1 AND column_name = ?2"
        )?;
        
        match stmt.query_row([table_name, column_name], |row| {
            row.get::<_, String>(0)
        }) {
            Ok(pg_type) => Ok(Some(pg_type)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
    
    /// Validate UPDATE statement for ENUM values
    pub fn validate_update(
        conn: &Connection,
        query: &str,
    ) -> Result<(), PgSqliteError> {
        // Parse UPDATE statement
        let update_regex = Regex::new(
            r"(?i)UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE|$)"
        ).map_err(|e| PgSqliteError::Protocol(format!("Regex error: {e}")))?;
        
        if let Some(captures) = update_regex.captures(query) {
            let table_name = captures.get(1).unwrap().as_str();
            let set_clause = captures.get(2).unwrap().as_str();
            
            // Parse SET assignments (simple parsing)
            let assignments: Vec<&str> = set_clause.split(',')
                .map(|a| a.trim())
                .collect();
            
            for assignment in assignments {
                if let Some(eq_pos) = assignment.find('=') {
                    let column = assignment[..eq_pos].trim();
                    let value_part = assignment[eq_pos + 1..].trim();
                    
                    // Check if column is ENUM type
                    if let Ok(Some(pg_type)) = Self::get_column_type(conn, table_name, column) {
                        if let Ok(Some(enum_type)) = EnumMetadata::get_enum_type(conn, &pg_type) {
                            // Extract value (handle quoted strings)
                            let value = value_part.trim_matches('\'').trim_matches('"');
                            
                            // Skip parameter placeholders
                            if value.starts_with('$') || value == "?" {
                                continue;
                            }
                            
                            // Validate the value
                            if !EnumMetadata::is_valid_enum_value(conn, enum_type.type_oid, value)? {
                                return Err(PgSqliteError::Protocol(
                                    format!("invalid input value for enum {pg_type}: \"{value}\"")
                                ));
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
}