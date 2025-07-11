use once_cell::sync::Lazy;
use regex::Regex;

/// Regular expressions for detecting truly simple queries that need no processing
static SIMPLE_SELECT_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*SELECT\s+\*?\s*FROM\s+\w+\s*(WHERE\s+\w+\s*=\s*('[^']*'|\d+))?\s*(LIMIT\s+\d+)?\s*;?\s*$").unwrap()
});

static SIMPLE_INSERT_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*INSERT\s+INTO\s+\w+\s*\([^)]+\)\s*VALUES\s*\([^)]+\)\s*;?\s*$").unwrap()
});

static BATCH_INSERT_REGEX: Lazy<Regex> = Lazy::new(|| {
    // Matches multi-row INSERT: INSERT INTO table (cols) VALUES (row1), (row2), ...
    Regex::new(r"(?i)^\s*INSERT\s+INTO\s+\w+\s*\([^)]+\)\s*VALUES\s*\([^)]+\)(?:\s*,\s*\([^)]+\))+\s*;?\s*$").unwrap()
});

static SIMPLE_UPDATE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*UPDATE\s+\w+\s+SET\s+(?:\w+\s*=\s*(?:'[^']*'|\d+(?:\.\d+)?|NULL)\s*,?\s*)+\s*(WHERE\s+\w+\s*=\s*(?:'[^']*'|\d+(?:\.\d+)?|NULL))?\s*;?\s*$").unwrap()
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
    
    // Additional check for INSERT statements with datetime patterns
    if query.to_uppercase().starts_with("INSERT") {
        // Exclude if it contains date/time patterns that need conversion
        if (query.contains("'") && query.contains('-')) || // Date patterns like '2024-01-01'
           (query.contains("'") && query.contains(':')) {   // Time patterns like '14:30:00'
            return false;
        }
    }
    
    // Check if it matches one of our simple patterns
    SIMPLE_SELECT_REGEX.is_match(query) ||
    SIMPLE_INSERT_REGEX.is_match(query) ||
    BATCH_INSERT_REGEX.is_match(query) ||
    SIMPLE_UPDATE_REGEX.is_match(query) ||
    SIMPLE_DELETE_REGEX.is_match(query)
}

/// Check if a batch INSERT query is simple (no datetime/decimal values)
pub fn is_simple_batch_insert(query: &str) -> bool {
    // First check if it's a batch INSERT
    if !BATCH_INSERT_REGEX.is_match(query) {
        return false;
    }
    
    // Check for patterns that require translation
    if query.contains("::") || // PostgreSQL casts
       query.contains("CURRENT_") || // DateTime functions
       query.contains("NOW()") ||
       query.contains("||") || // String concatenation
       query.contains("DECIMAL") || // May need rewriting
       query.contains("NUMERIC") {
        return false;
    }
    
    // Check for datetime patterns that need conversion
    if (query.contains("'") && query.contains('-')) || // Date patterns like '2024-01-01'
       (query.contains("'") && query.contains(':')) {   // Time patterns like '14:30:00'
        return false;
    }
    
    true
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
        assert!(is_ultra_simple_query("UPDATE users SET name = 'test', age = 25"));
        assert!(is_ultra_simple_query("UPDATE users SET price = 99.99, quantity = 5 WHERE id = 1"));
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
    
    #[test]
    fn test_batch_insert_detection() {
        // Simple batch INSERTs that should pass ultra-simple test
        assert!(is_ultra_simple_query("INSERT INTO users (id, name) VALUES (1, 'test'), (2, 'test2')"));
        assert!(is_ultra_simple_query("INSERT INTO products (id, price) VALUES (1, 99.99), (2, 149.99), (3, 199.99)"));
        
        // Batch INSERTs with datetime values should NOT pass
        assert!(!is_ultra_simple_query("INSERT INTO orders (id, date) VALUES (1, '2024-01-01'), (2, '2024-01-02')"));
        assert!(!is_ultra_simple_query("INSERT INTO logs (id, time) VALUES (1, '14:30:00'), (2, '15:45:00')"));
        
        // Test is_simple_batch_insert specifically
        assert!(is_simple_batch_insert("INSERT INTO users (id, name) VALUES (1, 'test'), (2, 'test2')"));
        assert!(!is_simple_batch_insert("INSERT INTO orders (id, date) VALUES (1, '2024-01-01'), (2, '2024-01-02')"));
        assert!(!is_simple_batch_insert("INSERT INTO users (id, name) VALUES (1, 'test')")); // Not a batch
    }
}