use once_cell::sync::Lazy;
use regex::Regex;
use tracing::debug;

/// Check if a query contains non-deterministic functions that should not be cached
pub fn contains_non_deterministic_functions(query: &str) -> bool {
    let query_lower = query.to_lowercase();
    query_lower.contains("gen_random_uuid") ||
    query_lower.contains("uuid_generate_v4") ||
    query_lower.contains("random()") ||
    query_lower.contains("now()") ||
    query_lower.contains("current_timestamp") ||
    query_lower.contains("current_date") ||
    query_lower.contains("current_time")
}

/// Regular expressions for detecting truly simple queries that need no processing
static SIMPLE_SELECT_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*SELECT\s+(\*|[\w\s,]+)\s*FROM\s+\w+\s*(WHERE\s+\w+\s*=\s*('[^']*'|\d+))?\s*(LIMIT\s+\d+)?\s*;?\s*$").unwrap()
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
    debug!("Checking if ultra-simple: {}", query);
    // Quick checks to exclude complex queries
    if query.contains("::") || // PostgreSQL casts
       query.contains("CAST") || // SQL standard casts (case-insensitive check below)
       query.contains("cast") || // SQL standard casts
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
       query.contains("NUMERIC") ||
       query.contains("unnest") || // unnest function calls need translation
       query.contains("UNNEST") {
        return false;
    }
    
    // Check for non-deterministic functions - these should not be treated as ultra simple
    if contains_non_deterministic_functions(query) {
        return false;
    }
    
    // Additional check for INSERT statements with datetime or array patterns
    if query.to_uppercase().starts_with("INSERT") {
        // Exclude if it contains date/time patterns that need conversion
        if (query.contains("'") && query.contains('-')) || // Date patterns like '2024-01-01'
           (query.contains("'") && query.contains(':')) ||  // Time patterns like '14:30:00'
           query.contains('{') ||                           // Array patterns like '{1,2,3}'
           query.contains("ARRAY[") {                       // Array constructor like ARRAY[1,2,3]
            debug!("INSERT query detected with special patterns - NOT ultra-simple: {}", query);
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

/// Optimized check for simple RETURNING clause - inline and minimal overhead
/// Returns true if RETURNING clause is simple or not present
#[inline(always)]
fn has_complex_returning(query_bytes: &[u8], returning_pos: usize) -> bool {
    // Get the part after RETURNING (9 chars)
    let after_returning = &query_bytes[returning_pos + 9..];
    
    // Find the actual content start (skip whitespace)
    let mut i = 0;
    while i < after_returning.len() && after_returning[i].is_ascii_whitespace() {
        i += 1;
    }
    
    if i >= after_returning.len() {
        return true; // Empty RETURNING clause is complex
    }
    
    let content = &after_returning[i..];
    
    // Special case: RETURNING * is simple
    if content.starts_with(b"*") {
        let after_star = &content[1..];
        // Check if it's just * followed by whitespace/semicolon
        for &b in after_star {
            if !b.is_ascii_whitespace() && b != b';' {
                return true; // Something after *, it's complex
            }
        }
        return false; // Just RETURNING *
    }
    
    // Fast scan for complex characters that indicate expressions
    // We only allow: alphanumeric, underscore, comma, whitespace, semicolon
    for &b in content {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_' | b',' | b' ' | b'\t' | b'\n' | b'\r' | b';' => {},
            _ => return true, // Any other character means it's complex
        }
    }
    
    false // Simple RETURNING with just column names
}

/// Fast byte-level check for simple queries that don't need LazyQueryProcessor
/// Heavily optimized for minimal overhead
#[inline(always)]
pub fn is_fast_path_simple_query(query: &str) -> bool {
    // Quick length check
    if query.len() < 10 || query.len() > 2000 {
        return false;
    }
    
    let query_bytes = query.as_bytes();
    
    // Quick check for first character to determine query type
    // This avoids expensive string operations for most queries
    let first_char = query_bytes[0].to_ascii_uppercase();
    
    let (is_dml, can_have_returning) = match first_char {
        b'S' => {
            // Check if it's SELECT (no RETURNING possible)
            if query_bytes.len() >= 7 {
                let prefix = &query_bytes[0..7];
                let is_select = prefix.eq_ignore_ascii_case(b"SELECT ");
                (is_select, false)
            } else {
                (false, false)
            }
        },
        b'I' => {
            // Check if it's INSERT INTO (can have RETURNING)
            if query_bytes.len() >= 12 {
                let prefix = &query_bytes[0..12];
                let is_insert = prefix.eq_ignore_ascii_case(b"INSERT INTO ");
                (is_insert, is_insert)
            } else {
                (false, false)
            }
        },
        b'U' => {
            // Check if it's UPDATE (can have RETURNING)
            if query_bytes.len() >= 7 {
                let prefix = &query_bytes[0..7];
                let is_update = prefix.eq_ignore_ascii_case(b"UPDATE ");
                (is_update, is_update)
            } else {
                (false, false)
            }
        },
        b'D' => {
            // Check if it's DELETE FROM (can have RETURNING)
            if query_bytes.len() >= 12 {
                let prefix = &query_bytes[0..12];
                let is_delete = prefix.eq_ignore_ascii_case(b"DELETE FROM ");
                (is_delete, is_delete)
            } else {
                (false, false)
            }
        },
        _ => (false, false),
    };
    
    if !is_dml {
        return false;
    }
    
    // Fast checks for features that need translation
    // Check for :: (type casts) - very common, check first
    if memchr::memmem::find(query_bytes, b"::").is_some() {
        return false;
    }
    
    // Check for regex operators
    if memchr::memmem::find(query_bytes, b" ~ ").is_some() ||
       memchr::memmem::find(query_bytes, b" !~ ").is_some() ||
       memchr::memmem::find(query_bytes, b" ~* ").is_some() ||
       memchr::memmem::find(query_bytes, b" !~* ").is_some() {
        return false;
    }
    
    // Check for schema prefixes
    if memchr::memmem::find(query_bytes, b"pg_catalog").is_some() ||
       memchr::memmem::find(query_bytes, b"PG_CATALOG").is_some() {
        return false;
    }
    
    // Check for array operations
    if memchr::memchr(b'[', query_bytes).is_some() ||
       memchr::memchr(b']', query_bytes).is_some() ||
       memchr::memmem::find(query_bytes, b"ANY(").is_some() ||
       memchr::memmem::find(query_bytes, b"ALL(").is_some() ||
       memchr::memmem::find(query_bytes, b" @> ").is_some() ||
       memchr::memmem::find(query_bytes, b" <@ ").is_some() ||
       memchr::memmem::find(query_bytes, b" && ").is_some() {
        return false;
    }
    
    // Check for special SQL features
    if memchr::memmem::find(query_bytes, b"USING").is_some() ||
       memchr::memmem::find(query_bytes, b"AT TIME ZONE").is_some() ||
       memchr::memmem::find(query_bytes, b"NOW()").is_some() ||
       memchr::memmem::find(query_bytes, b"CURRENT_").is_some() ||
       memchr::memmem::find(query_bytes, b"::NUMERIC").is_some() ||
       memchr::memmem::find(query_bytes, b"::DECIMAL").is_some() ||
       memchr::memmem::find(query_bytes, b"CAST").is_some() ||
       memchr::memmem::find(query_bytes, b"cast").is_some() {
        return false;
    }
    
    // RETURNING check - DISABLED FOR TESTING
    // // This is the key optimization - we skip this expensive check for SELECT queries
    // if can_have_returning {
    //     // Use SIMD-optimized memchr - check both cases but it's still fast
    //     if let Some(pos) = memchr::memmem::find(query_bytes, b"RETURNING") {
    //         if has_complex_returning(query_bytes, pos) {
    //             return false;
    //         }
    //     } else if let Some(pos) = memchr::memmem::find(query_bytes, b"returning") {
    //         if has_complex_returning(query_bytes, pos) {
    //             return false;
    //         }
    //     }
    // }
    
    // Check for UPDATE ... FROM pattern (only if UPDATE)
    if first_char == b'U' {
        if let Some(set_pos) = memchr::memmem::find(query_bytes, b" SET ") {
            if memchr::memmem::find(&query_bytes[set_pos..], b" FROM ").is_some() {
                return false;
            }
        }
    }
    
    // Check for datetime patterns in INSERT statements
    if first_char == b'I' {
        if memchr::memchr(b'\'', query_bytes).is_some() {
            // Check for date pattern YYYY-MM-DD or time pattern HH:MM:SS
            if memchr::memchr(b'-', query_bytes).is_some() ||
               memchr::memchr(b':', query_bytes).is_some() {
                return false;
            }
        }
        // Check for array literals
        if memchr::memchr(b'{', query_bytes).is_some() ||
           memchr::memmem::find(query_bytes, b"ARRAY[").is_some() {
            return false;
        }
    }
    
    // Check for JOIN, UNION, subqueries, etc
    if memchr::memmem::find(query_bytes, b"JOIN").is_some() ||
       memchr::memmem::find(query_bytes, b"UNION").is_some() ||
       memchr::memmem::find(query_bytes, b"(SELECT").is_some() ||
       memchr::memmem::find(query_bytes, b"CASE").is_some() ||
       memchr::memmem::find(query_bytes, b"GROUP BY").is_some() ||
       memchr::memmem::find(query_bytes, b"HAVING").is_some() ||
       memchr::memmem::find(query_bytes, b"EXTRACT").is_some() ||
       memchr::memmem::find(query_bytes, b"unnest").is_some() ||
       memchr::memmem::find(query_bytes, b"UNNEST").is_some() {
        return false;
    }
    
    true
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
    
    // Check for datetime or array patterns that need conversion
    if (query.contains("'") && query.contains('-')) || // Date patterns like '2024-01-01'
       (query.contains("'") && query.contains(':')) ||  // Time patterns like '14:30:00'
       query.contains('{') ||                           // Array patterns like '{1,2,3}'
       query.contains("ARRAY[") {                       // Array constructor like ARRAY[1,2,3]
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
        
        // Test the specific query from datetime_conversion_success_test
        assert!(is_ultra_simple_query("SELECT date_col, time_col FROM dt_test WHERE id = 1"));
        
        // Complex queries that should fail
        assert!(!is_ultra_simple_query("SELECT * FROM users WHERE created_at > NOW()"));
        assert!(!is_ultra_simple_query("SELECT id::text FROM users"));
        assert!(!is_ultra_simple_query("SELECT CAST('inactive' AS status)"));
        assert!(!is_ultra_simple_query("SELECT cast(id as text) FROM users"));
        assert!(!is_ultra_simple_query("SELECT * FROM users JOIN orders"));
        assert!(!is_ultra_simple_query("SELECT (SELECT COUNT(*) FROM orders)"));
        assert!(!is_ultra_simple_query("SELECT * FROM users WHERE name ~ 'test'"));
        assert!(!is_ultra_simple_query("SELECT value FROM unnest('[1,2,3]') AS t"));
        assert!(!is_ultra_simple_query("SELECT value FROM UNNEST('[1,2,3]') AS t"));
        
        // Test the exact query from the integration test
        assert!(!is_ultra_simple_query("SELECT value FROM unnest('[\"first\", \"second\", \"third\"]') AS t"));
        assert!(!is_ultra_simple_query("SELECT value, ordinality FROM unnest('[\"first\", \"second\", \"third\"]') WITH ORDINALITY AS t ORDER BY ordinality"));
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
    
    #[test]
    fn test_array_insert_detection() {
        // Array INSERTs should NOT be ultra-simple
        assert!(!is_ultra_simple_query("INSERT INTO test_arrays (int_array) VALUES ('{1,2,3}')"));
        assert!(!is_ultra_simple_query("INSERT INTO test_arrays (int_array, text_array) VALUES ('{1,2,3}', '{\"a\",\"b\"}')"));
        
        // The exact query from the failing test
        let failing_query = "INSERT INTO test_arrays (int_array, text_array, bool_array) VALUES\n    ('{1,2,3,4,5}', '{\"apple\",\"banana\",\"cherry\"}', '{true,false,true}'),\n    ('{}', '{}', '{}'),\n    (NULL, NULL, NULL);";
        assert!(!is_ultra_simple_query(failing_query));
    }
    
    #[test]
    fn test_fast_path_simple_query() {
        // Simple queries that should use fast path
        assert!(is_fast_path_simple_query("SELECT * FROM users"));
        assert!(is_fast_path_simple_query("SELECT id, name FROM users WHERE id = $1"));
        assert!(is_fast_path_simple_query("INSERT INTO users (name, email) VALUES ($1, $2)"));
        assert!(is_fast_path_simple_query("UPDATE users SET name = $1 WHERE id = $2"));
        assert!(is_fast_path_simple_query("DELETE FROM users WHERE id = $1"));
        assert!(is_fast_path_simple_query("SELECT id, name, email FROM users WHERE active = true"));
        
        // Benchmark queries - should be simple
        assert!(is_fast_path_simple_query("UPDATE benchmark_table_pg SET text_col = %s WHERE id = %s"));
        assert!(is_fast_path_simple_query("DELETE FROM benchmark_table_pg WHERE id = %s"));
        assert!(is_fast_path_simple_query("SELECT * FROM benchmark_table_pg WHERE int_col > %s"));
        
        // Simple RETURNING clauses should NOW use fast path!
        assert!(is_fast_path_simple_query("INSERT INTO users (name) VALUES ('test') RETURNING id"));
        assert!(is_fast_path_simple_query("INSERT INTO benchmark_table_pg (text_col, int_col, real_col, bool_col) VALUES (%s, %s, %s, %s) RETURNING id"));
        assert!(is_fast_path_simple_query("UPDATE users SET name = 'test' WHERE id = 1 RETURNING *"));
        assert!(is_fast_path_simple_query("DELETE FROM users WHERE id = 1 RETURNING id, name"));
        
        // Complex queries that should NOT use fast path
        assert!(!is_fast_path_simple_query("SELECT * FROM users WHERE created_at::date = $1"));
        assert!(!is_fast_path_simple_query("SELECT * FROM pg_catalog.pg_tables"));
        assert!(!is_fast_path_simple_query("SELECT * FROM users WHERE email ~ '@gmail.com'"));
        assert!(!is_fast_path_simple_query("SELECT * FROM users WHERE id = ANY($1)"));
        assert!(!is_fast_path_simple_query("DELETE FROM users USING orders WHERE users.id = orders.user_id"));
        assert!(!is_fast_path_simple_query("UPDATE users SET updated_at = NOW()"));
        assert!(!is_fast_path_simple_query("SELECT * FROM users WHERE tags @> ARRAY['admin']"));
        assert!(!is_fast_path_simple_query("SELECT CAST('active' AS status)"));
        assert!(!is_fast_path_simple_query("SELECT * FROM users JOIN orders ON users.id = orders.user_id"));
        assert!(!is_fast_path_simple_query("INSERT INTO logs (created) VALUES ('2024-01-01')"));
        assert!(!is_fast_path_simple_query("INSERT INTO logs (time) VALUES ('14:30:00')"));
        assert!(!is_fast_path_simple_query("SELECT * FROM unnest(ARRAY[1,2,3])"));
        
        // Complex RETURNING clauses should NOT use fast path
        assert!(!is_fast_path_simple_query("INSERT INTO users (name) VALUES ('test') RETURNING id::text"));
        assert!(!is_fast_path_simple_query("INSERT INTO users (name) VALUES ('test') RETURNING upper(name)"));
        assert!(!is_fast_path_simple_query("UPDATE users SET price = 10 WHERE id = 1 RETURNING price * 2"));
        assert!(!is_fast_path_simple_query("DELETE FROM users WHERE id = 1 RETURNING now()"));
        
        // Edge cases
        assert!(!is_fast_path_simple_query("SELECT")); // Too short
        assert!(!is_fast_path_simple_query("BEGIN")); // Not DML
        assert!(!is_fast_path_simple_query("COMMIT")); // Not DML
    }
    
    #[test]
    fn test_returning_optimization() {
        // SELECT queries should never check for RETURNING (optimization)
        assert!(is_fast_path_simple_query("SELECT * FROM users"));
        assert!(is_fast_path_simple_query("SELECT * FROM benchmark_table_pg WHERE int_col > %s"));
        
        // INSERT/UPDATE/DELETE with simple RETURNING should be fast path
        assert!(is_fast_path_simple_query("INSERT INTO users (name) VALUES ('test') RETURNING id"));
        assert!(is_fast_path_simple_query("UPDATE users SET name = 'test' RETURNING *"));
        assert!(is_fast_path_simple_query("DELETE FROM users WHERE id = 1 RETURNING id"));
        assert!(is_fast_path_simple_query("INSERT INTO test VALUES (1) RETURNING col1, col2, col3"));
        
        // Complex RETURNING should not be fast path
        assert!(!is_fast_path_simple_query("INSERT INTO users VALUES (1) RETURNING id + 1"));
        assert!(!is_fast_path_simple_query("INSERT INTO users VALUES (1) RETURNING upper(name)"));
        assert!(!is_fast_path_simple_query("INSERT INTO users VALUES (1) RETURNING id::text"));
    }
}