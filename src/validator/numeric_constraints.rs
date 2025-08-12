use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use rusqlite::Connection;
use crate::error::PgError;
use rust_decimal::prelude::*;

/// Represents a numeric constraint for a column
#[derive(Debug, Clone)]
pub struct NumericConstraint {
    pub table_name: String,
    pub column_name: String,
    pub precision: i32,  // Total number of digits
    pub scale: i32,      // Number of digits after decimal point
}

/// Cache for numeric constraints to avoid repeated database queries
pub struct NumericConstraintValidator {
    constraints: Arc<RwLock<HashMap<String, HashMap<String, NumericConstraint>>>>,
}

impl Default for NumericConstraintValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl NumericConstraintValidator {
    pub fn new() -> Self {
        Self {
            constraints: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Load constraints for a table from the database
    pub fn load_table_constraints(&self, conn: &Connection, table_name: &str) -> Result<(), rusqlite::Error> {
        // First check if we have the numeric constraints table (migration v7)
        let has_constraints_table = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='__pgsqlite_numeric_constraints'",
            [],
            |row| row.get::<_, i32>(0)
        )? > 0;
        
        if !has_constraints_table {
            // No constraints table, nothing to load
            return Ok(());
        }
        
        // Query numeric constraints
        let mut stmt = conn.prepare(
            "SELECT column_name, precision, scale 
             FROM __pgsqlite_numeric_constraints 
             WHERE table_name = ?1"
        )?;
        
        let constraints_result = stmt.query_map([table_name], |row| {
            Ok(NumericConstraint {
                table_name: table_name.to_string(),
                column_name: row.get(0)?,
                precision: row.get(1)?,
                scale: row.get(2)?,
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
    
    /// Populate the numeric constraints table from __pgsqlite_schema
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
        
        // Get all numeric types with modifiers
        let mut stmt = conn.prepare(
            "SELECT table_name, column_name, pg_type, type_modifier 
             FROM __pgsqlite_schema 
             WHERE type_modifier IS NOT NULL 
             AND pg_type IN ('numeric', 'decimal')"
        )?;
        
        let constraints = stmt.query_map([], |row| {
            let type_modifier: i32 = row.get(3)?;
            // Decode precision and scale from modifier
            let tmp_typmod = type_modifier - 4; // Remove VARHDRSZ
            let precision = (tmp_typmod >> 16) & 0xFFFF;
            let scale = tmp_typmod & 0xFFFF;
            
            Ok((
                row.get::<_, String>(0)?,  // table_name
                row.get::<_, String>(1)?,  // column_name
                precision,
                scale,
            ))
        })?;
        
        // Insert into numeric constraints table
        for constraint in constraints {
            let (table_name, column_name, precision, scale) = constraint?;
            conn.execute(
                "INSERT OR REPLACE INTO __pgsqlite_numeric_constraints 
                 (table_name, column_name, precision, scale) 
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![table_name, column_name, precision, scale],
            )?;
        }
        
        Ok(())
    }
    
    /// Validate a numeric value against constraints
    pub fn validate_value(
        &self,
        table_name: &str,
        column_name: &str,
        value: &str,
    ) -> Result<(), PgError> {
        // NULL values bypass constraints
        if value.is_empty() || value.to_uppercase() == "NULL" {
            return Ok(());
        }
        
        // Check cache
        let cache = self.constraints.read().unwrap();
        
        if let Some(table_constraints) = cache.get(table_name)
            && let Some(constraint) = table_constraints.get(column_name) {
                // Parse the value as a decimal
                let decimal = match Decimal::from_str(value) {
                    Ok(d) => d,
                    Err(_) => {
                        // If we can't parse it as a decimal, it will fail at database level
                        return Ok(());
                    }
                };
                
                // Count total digits and scale
                let (integer_digits, fractional_digits) = self.count_digits(&decimal);
                let total_digits = integer_digits + fractional_digits;
                
                // Check precision (total digits)
                if total_digits > constraint.precision {
                    return Err(PgError::NumericValueOutOfRange {
                        type_name: format!("numeric({},{})", constraint.precision, constraint.scale),
                        column_name: column_name.to_string(),
                        value: value.to_string(),
                    });
                }
                
                // Check scale (fractional digits)
                if fractional_digits > constraint.scale {
                    return Err(PgError::NumericValueOutOfRange {
                        type_name: format!("numeric({},{})", constraint.precision, constraint.scale),
                        column_name: column_name.to_string(),
                        value: value.to_string(),
                    });
                }
            }
        
        Ok(())
    }
    
    /// Count the number of integer and fractional digits in a decimal
    fn count_digits(&self, decimal: &Decimal) -> (i32, i32) {
        let s = decimal.abs().to_string();
        
        // Remove leading zeros and handle special cases
        let s = s.trim_start_matches('0');
        
        if let Some(dot_pos) = s.find('.') {
            let integer_part = &s[..dot_pos];
            let fractional_part = &s[dot_pos + 1..];
            
            // Count non-zero integer digits (or 1 if all zeros)
            let integer_digits = if integer_part.is_empty() { 
                1 
            } else { 
                integer_part.len() as i32 
            };
            
            // Count fractional digits
            let fractional_digits = fractional_part.len() as i32;
            
            (integer_digits, fractional_digits)
        } else {
            // No decimal point
            let integer_digits = if s.is_empty() { 1 } else { s.len() as i32 };
            (integer_digits, 0)
        }
    }
    
    /// Format a decimal value according to scale
    pub fn format_value(&self, table_name: &str, column_name: &str, value: &str) -> String {
        // NULL values pass through
        if value.is_empty() || value.to_uppercase() == "NULL" {
            return value.to_string();
        }
        
        let cache = self.constraints.read().unwrap();
        
        if let Some(table_constraints) = cache.get(table_name)
            && let Some(constraint) = table_constraints.get(column_name) {
                // Parse the value as a decimal
                if let Ok(mut decimal) = Decimal::from_str(value) {
                    // Round to the specified scale
                    decimal = decimal.round_dp(constraint.scale as u32);
                    
                    // Format with the exact scale
                    let formatted = format!("{:.prec$}", decimal, prec = constraint.scale as usize);
                    return formatted;
                }
            }
        
        value.to_string()
    }
    
    /// Check if a table has any numeric constraints
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
        
        // Create the numeric constraints table
        conn.execute(
            "CREATE TABLE __pgsqlite_numeric_constraints (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                precision INTEGER NOT NULL,
                scale INTEGER NOT NULL,
                PRIMARY KEY (table_name, column_name)
            )",
            [],
        ).unwrap();
        
        // Add test constraints
        conn.execute(
            "INSERT INTO __pgsqlite_numeric_constraints VALUES ('test_table', 'amount', 10, 2)",
            [],
        ).unwrap();
        
        conn
    }
    
    #[test]
    fn test_load_constraints() {
        let conn = setup_test_db();
        let validator = NumericConstraintValidator::new();
        
        validator.load_table_constraints(&conn, "test_table").unwrap();
        assert!(validator.table_has_constraints("test_table"));
    }
    
    #[test]
    fn test_validate_valid_values() {
        let conn = setup_test_db();
        let validator = NumericConstraintValidator::new();
        validator.load_table_constraints(&conn, "test_table").unwrap();
        
        // Valid values
        assert!(validator.validate_value("test_table", "amount", "123.45").is_ok());
        assert!(validator.validate_value("test_table", "amount", "0.99").is_ok());
        assert!(validator.validate_value("test_table", "amount", "99999999.99").is_ok());
        assert!(validator.validate_value("test_table", "amount", "").is_ok()); // NULL
    }
    
    #[test]
    fn test_validate_invalid_precision() {
        let conn = setup_test_db();
        let validator = NumericConstraintValidator::new();
        validator.load_table_constraints(&conn, "test_table").unwrap();
        
        // Too many digits total
        let result = validator.validate_value("test_table", "amount", "99999999.999");
        assert!(result.is_err());
        match result.unwrap_err() {
            PgError::NumericValueOutOfRange { .. } => (),
            _ => panic!("Expected NumericValueOutOfRange error"),
        }
    }
    
    #[test]
    fn test_validate_invalid_scale() {
        let conn = setup_test_db();
        let validator = NumericConstraintValidator::new();
        validator.load_table_constraints(&conn, "test_table").unwrap();
        
        // Too many decimal places
        let result = validator.validate_value("test_table", "amount", "123.456");
        assert!(result.is_err());
        match result.unwrap_err() {
            PgError::NumericValueOutOfRange { .. } => (),
            _ => panic!("Expected NumericValueOutOfRange error"),
        }
    }
    
    #[test]
    fn test_format_value() {
        let conn = setup_test_db();
        let validator = NumericConstraintValidator::new();
        validator.load_table_constraints(&conn, "test_table").unwrap();
        
        // Should format to 2 decimal places
        assert_eq!(validator.format_value("test_table", "amount", "123"), "123.00");
        assert_eq!(validator.format_value("test_table", "amount", "123.4"), "123.40");
        assert_eq!(validator.format_value("test_table", "amount", "123.456"), "123.46"); // Rounded
    }
}