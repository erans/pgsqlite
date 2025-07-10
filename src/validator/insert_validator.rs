use regex::Regex;
use crate::error::PgError;
use crate::validator::StringConstraintValidator;
use rusqlite::Connection;

pub struct InsertValidator;

impl InsertValidator {
    /// Check if query needs validation (has string literals)
    pub fn needs_validation(query: &str) -> bool {
        query.contains('\'')
    }
    
    /// Extract table name from INSERT query
    pub fn extract_table_name(query: &str) -> Option<String> {
        let re = Regex::new(r"(?i)INSERT\s+INTO\s+(\w+)").ok()?;
        re.captures(query)
            .and_then(|cap| cap.get(1))
            .map(|m| m.as_str().to_string())
    }
    
    /// Note: Full validation would be implemented with proper SQL parsing
    /// For now, we rely on SQLite's CHECK constraints or trigger-based validation
    pub fn validate_insert(
        _query: &str,
        _validator: &StringConstraintValidator,
        _conn: &Connection,
    ) -> Result<(), PgError> {
        // TODO: Implement proper INSERT validation with SQL parsing
        // For now, constraints are enforced by SQLite
        Ok(())
    }
}

pub struct UpdateValidator;

impl UpdateValidator {
    /// Check if query needs validation (has string literals)
    pub fn needs_validation(query: &str) -> bool {
        query.contains('\'')
    }
    
    /// Extract table name from UPDATE query
    pub fn extract_table_name(query: &str) -> Option<String> {
        let re = Regex::new(r"(?i)UPDATE\s+(\w+)").ok()?;
        re.captures(query)
            .and_then(|cap| cap.get(1))
            .map(|m| m.as_str().to_string())
    }
    
    /// Note: Full validation would be implemented with proper SQL parsing
    /// For now, we rely on SQLite's CHECK constraints or trigger-based validation
    pub fn validate_update(
        _query: &str,
        _validator: &StringConstraintValidator,
        _conn: &Connection,
    ) -> Result<(), PgError> {
        // TODO: Implement proper UPDATE validation with SQL parsing
        // For now, constraints are enforced by SQLite
        Ok(())
    }
}