use once_cell::sync::Lazy;
use regex::Regex;

/// Regular expressions for detecting truly simple queries that need no processing
static SIMPLE_SELECT_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*SELECT\s+\*?\s*FROM\s+\w+\s*(WHERE\s+\w+\s*=\s*('[^']*'|\d+))?\s*(LIMIT\s+\d+)?\s*;?\s*$").unwrap()
});

static SIMPLE_INSERT_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*INSERT\s+INTO\s+\w+\s*\([^)]+\)\s*VALUES\s*\([^)]+\)\s*;?\s*$").unwrap()
});

static SIMPLE_UPDATE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*UPDATE\s+\w+\s+SET\s+\w+\s*=\s*('[^']*'|\d+)\s*(WHERE\s+\w+\s*=\s*('[^']*'|\d+))?\s*;?\s*$").unwrap()
});

static SIMPLE_DELETE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*DELETE\s+FROM\s+\w+\s+(WHERE\s+\w+\s*=\s*('[^']*'|\d+))?\s*;?\s*$").unwrap()
});

/// Detects if a query is simple enough to bypass all translation and processing
pub fn is_ultra_simple_query(query: &str) -> bool {
    // Quick checks to exclude complex queries
    if query.contains("::") || // PostgreSQL casts
       query.contains("JOIN") ||
       query.contains("UNION") ||
       query.contains("(SELECT") || // Subqueries
       query.contains("CASE") ||
       query.contains("GROUP BY") ||
       query.contains("HAVING") ||
       query.contains("CURRENT_") || // DateTime functions
       query.contains("NOW()") ||
       query.contains("EXTRACT") ||
       query.contains("DATE_") ||
       query.contains("AT TIME ZONE") ||
       query.contains("||") || // String concatenation
       query.contains("~") || // Pattern matching
       query.contains("->") || // JSON operators
       query.contains("@") || // Array/range operators
       query.contains("DECIMAL") || // May need rewriting
       query.contains("NUMERIC") {
        return false;
    }
    
    // Check if it matches one of our simple patterns
    SIMPLE_SELECT_REGEX.is_match(query) ||
    SIMPLE_INSERT_REGEX.is_match(query) ||
    SIMPLE_UPDATE_REGEX.is_match(query) ||
    SIMPLE_DELETE_REGEX.is_match(query)
}

/// Extract table name from a simple query
pub fn extract_simple_table_name(query: &str) -> Option<String> {
    // Try to extract table name using simple regex
    if let Some(caps) = Regex::new(r"(?i)FROM\s+(\w+)").unwrap().captures(query) {
        return Some(caps.get(1).unwrap().as_str().to_string());
    }
    
    if let Some(caps) = Regex::new(r"(?i)INTO\s+(\w+)").unwrap().captures(query) {
        return Some(caps.get(1).unwrap().as_str().to_string());
    }
    
    if let Some(caps) = Regex::new(r"(?i)UPDATE\s+(\w+)").unwrap().captures(query) {
        return Some(caps.get(1).unwrap().as_str().to_string());
    }
    
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_ultra_simple_detection() {
        // Simple queries that should pass
        assert!(is_ultra_simple_query("SELECT * FROM users"));
        assert!(is_ultra_simple_query("SELECT * FROM users WHERE id = 1"));
        assert!(is_ultra_simple_query("SELECT * FROM users LIMIT 10"));
        assert!(is_ultra_simple_query("INSERT INTO users (name) VALUES ('test')"));
        assert!(is_ultra_simple_query("UPDATE users SET name = 'test' WHERE id = 1"));
        assert!(is_ultra_simple_query("DELETE FROM users WHERE id = 1"));
        
        // Complex queries that should fail
        assert!(!is_ultra_simple_query("SELECT * FROM users WHERE created_at > NOW()"));
        assert!(!is_ultra_simple_query("SELECT id::text FROM users"));
        assert!(!is_ultra_simple_query("SELECT * FROM users JOIN orders"));
        assert!(!is_ultra_simple_query("SELECT (SELECT COUNT(*) FROM orders)"));
        assert!(!is_ultra_simple_query("SELECT * FROM users WHERE name ~ 'test'"));
    }
    
    #[test]
    fn test_table_extraction() {
        assert_eq!(extract_simple_table_name("SELECT * FROM users"), Some("users".to_string()));
        assert_eq!(extract_simple_table_name("INSERT INTO products (name) VALUES ('test')"), Some("products".to_string()));
        assert_eq!(extract_simple_table_name("UPDATE customers SET name = 'test'"), Some("customers".to_string()));
        assert_eq!(extract_simple_table_name("DELETE FROM orders"), Some("orders".to_string()));
    }
}