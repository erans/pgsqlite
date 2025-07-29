use crate::PgSqliteError;
use regex::Regex;
use once_cell::sync::Lazy;

// Pre-compiled regex patterns
static RETURNING_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(.+?)\s+RETURNING\s+(.+)$").unwrap()
});

static INSERT_TABLE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)INSERT\s+INTO\s+([^\s(]+)").unwrap()
});

static UPDATE_TABLE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)UPDATE\s+([^\s]+)").unwrap()
});

static DELETE_TABLE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)DELETE\s+FROM\s+([^\s]+)").unwrap()
});

static WHERE_CLAUSE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\sWHERE\s+(.+?)(?:\s+RETURNING|$)").unwrap()
});

static UPDATE_WHERE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)WHERE\s+(.+?)(?:\s+RETURNING|$)").unwrap()
});

/// Translates PostgreSQL RETURNING clause to SQLite-compatible operations
pub struct ReturningTranslator;

impl ReturningTranslator {
    /// Check if a query contains a RETURNING clause
    pub fn has_returning_clause(sql: &str) -> bool {
        let upper_sql = sql.to_uppercase();
        upper_sql.contains(" RETURNING ")
    }
    
    /// Extract RETURNING clause from a query
    pub fn extract_returning_clause(sql: &str) -> Option<(String, String)> {
        if let Some(captures) = RETURNING_REGEX.captures(sql) {
            let base_query = captures.get(1)?.as_str().trim().to_string();
            let returning_clause = captures.get(2)?.as_str().trim().to_string();
            Some((base_query, returning_clause))
        } else {
            None
        }
    }
    
    /// Generate follow-up SELECT for INSERT with RETURNING
    pub fn generate_insert_returning_query(
        table_name: &str,
        returning_columns: &str,
        rowid: i64,
    ) -> String {
        format!("SELECT {returning_columns} FROM {table_name} WHERE rowid = {rowid}")
    }
    
    /// Extract table name from INSERT statement
    pub fn extract_table_from_insert(sql: &str) -> Option<String> {
        if let Some(captures) = INSERT_TABLE_REGEX.captures(sql) {
            Some(captures.get(1)?.as_str().to_string())
        } else {
            None
        }
    }
    
    /// Extract table name from UPDATE statement
    pub fn extract_table_from_update(sql: &str) -> Option<String> {
        if let Some(captures) = UPDATE_TABLE_REGEX.captures(sql) {
            Some(captures.get(1)?.as_str().to_string())
        } else {
            None
        }
    }
    
    /// Extract table name from DELETE statement
    pub fn extract_table_from_delete(sql: &str) -> Option<String> {
        if let Some(captures) = DELETE_TABLE_REGEX.captures(sql) {
            Some(captures.get(1)?.as_str().to_string())
        } else {
            None
        }
    }
    
    /// Extract WHERE clause from a query
    pub fn extract_where_clause(sql: &str) -> String {
        if let Some(captures) = WHERE_CLAUSE_REGEX.captures(sql) {
            format!("WHERE {}", captures.get(1).map(|m| m.as_str()).unwrap_or("1=1"))
        } else {
            String::new()
        }
    }
    
    /// Generate a query to capture affected rows before UPDATE/DELETE
    pub fn generate_capture_query(
        sql: &str,
        table_name: &str,
        returning_columns: &str,
    ) -> Result<String, PgSqliteError> {
        let upper_sql = sql.to_uppercase();
        
        if upper_sql.starts_with("UPDATE") {
            // Extract WHERE clause from UPDATE
            let where_clause = UPDATE_WHERE_REGEX.captures(sql)
                .and_then(|c| c.get(1))
                .map(|m| m.as_str())
                .unwrap_or("1=1");
            
            Ok(format!("SELECT rowid, {returning_columns} FROM {table_name} WHERE {where_clause}"))
        } else if upper_sql.starts_with("DELETE") {
            // Extract WHERE clause from DELETE
            let where_clause = UPDATE_WHERE_REGEX.captures(sql)
                .and_then(|c| c.get(1))
                .map(|m| m.as_str())
                .unwrap_or("1=1");
            
            Ok(format!("SELECT rowid, {returning_columns} FROM {table_name} WHERE {where_clause}"))
        } else {
            Err(PgSqliteError::Protocol("Unsupported operation for RETURNING".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_has_returning_clause() {
        assert!(ReturningTranslator::has_returning_clause("INSERT INTO users (name) VALUES ('John') RETURNING id"));
        assert!(ReturningTranslator::has_returning_clause("UPDATE users SET name = 'Jane' WHERE id = 1 returning *"));
        assert!(!ReturningTranslator::has_returning_clause("INSERT INTO users (name) VALUES ('John')"));
    }
    
    #[test]
    fn test_extract_returning_clause() {
        let (base, returning) = ReturningTranslator::extract_returning_clause(
            "INSERT INTO users (name) VALUES ('John') RETURNING id, name"
        ).unwrap();
        assert_eq!(base, "INSERT INTO users (name) VALUES ('John')");
        assert_eq!(returning, "id, name");
    }
    
    #[test]
    fn test_extract_table_names() {
        assert_eq!(
            ReturningTranslator::extract_table_from_insert("INSERT INTO users (name) VALUES ('John')"),
            Some("users".to_string())
        );
        
        assert_eq!(
            ReturningTranslator::extract_table_from_update("UPDATE users SET name = 'Jane' WHERE id = 1"),
            Some("users".to_string())
        );
        
        assert_eq!(
            ReturningTranslator::extract_table_from_delete("DELETE FROM users WHERE id = 1"),
            Some("users".to_string())
        );
    }
    
    #[test]
    fn test_generate_capture_query() {
        let capture = ReturningTranslator::generate_capture_query(
            "UPDATE users SET name = 'Jane' WHERE id = 1 RETURNING *",
            "users",
            "*"
        ).unwrap();
        assert_eq!(capture, "SELECT rowid, * FROM users WHERE id = 1");
    }
}