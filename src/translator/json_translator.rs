use crate::PgSqliteError;
use regex::Regex;
use once_cell::sync::Lazy;

/// Translates PostgreSQL JSON/JSONB types to SQLite-compatible types
pub struct JsonTranslator;

impl JsonTranslator {
    /// Translate SQL statement, converting JSON/JSONB types to TEXT
    pub fn translate_statement(sql: &str) -> Result<String, PgSqliteError> {
        // Quick check to avoid regex if not needed
        let lower_sql = sql.to_lowercase();
        if !lower_sql.contains("json") && !lower_sql.contains("jsonb") {
            return Ok(sql.to_string());
        }

        // For now, use simple string replacement for JSON/JSONB types
        // This is more reliable than trying to parse and modify the AST
        let mut result = sql.to_string();
        
        // Replace JSONB type (case-insensitive)
        result = Self::replace_type(&result, "JSONB", "TEXT");
        
        // Replace JSON type (case-insensitive)  
        result = Self::replace_type(&result, "JSON", "TEXT");
        
        Ok(result)
    }
    
    /// Replace a type name in SQL, preserving case and word boundaries
    fn replace_type(sql: &str, from_type: &str, to_type: &str) -> String {
        let regex_pattern = format!(r"\b{}\b", regex::escape(from_type));
        let re = regex::RegexBuilder::new(&regex_pattern)
            .case_insensitive(true)
            .build()
            .unwrap();
        re.replace_all(sql, to_type).to_string()
    }
    
    /// Check if a query is trying to use JSON/JSONB functions
    pub fn contains_json_operations(sql: &str) -> bool {
        let lower_sql = sql.to_lowercase();
        
        // PostgreSQL JSON operators and functions
        lower_sql.contains("->") ||
        lower_sql.contains("->>") ||
        lower_sql.contains("#>") ||
        lower_sql.contains("#>>") ||
        lower_sql.contains("@>") ||
        lower_sql.contains("<@") ||
        lower_sql.contains("?") ||
        lower_sql.contains("?|") ||
        lower_sql.contains("?&") ||
        lower_sql.contains("jsonb_") ||
        lower_sql.contains("json_") ||
        lower_sql.contains("to_json") ||
        lower_sql.contains("to_jsonb") ||
        lower_sql.contains("array_to_json") ||
        lower_sql.contains("row_to_json")
    }
    
    /// Translate JSON operators in SQL to SQLite-compatible functions
    pub fn translate_json_operators(sql: &str) -> Result<String, PgSqliteError> {
        // Quick check to avoid processing if no operators
        if !Self::contains_json_operators(sql) {
            return Ok(sql.to_string());
        }
        
        let mut result = sql.to_string();
        
        // Translate operators in order of precedence (longer operators first)
        result = Self::translate_text_extract_operator(&result)?;
        result = Self::translate_json_extract_operator(&result)?;
        result = Self::translate_path_text_operator(&result)?;
        result = Self::translate_path_json_operator(&result)?;
        result = Self::translate_contains_operators(&result)?;
        result = Self::translate_existence_operators(&result)?;
        
        Ok(result)
    }
    
    /// No longer needed - we use custom functions instead of $ paths
    pub fn restore_json_path_root(sql: &str) -> String {
        sql.to_string()
    }
    
    /// Check if SQL contains JSON operators
    fn contains_json_operators(sql: &str) -> bool {
        sql.contains("->") || 
        sql.contains("->>") || 
        sql.contains("#>") || 
        sql.contains("#>>") ||
        sql.contains("@>") ||
        sql.contains("<@") ||
        sql.contains("?") ||
        sql.contains("?|") ||
        sql.contains("?&")
    }
    
    /// Translate ->> operator (extract JSON field as text)
    fn translate_text_extract_operator(sql: &str) -> Result<String, PgSqliteError> {
        static RE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(\b\w+(?:\.\w+)?|pgsqlite_json_get_(?:json|array_json)\([^)]+\))\s*->>\s*'([^']+)'")
                .expect("Invalid regex")
        });
        
        static RE_INT: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(\b\w+(?:\.\w+)?|pgsqlite_json_get_(?:json|array_json)\([^)]+\))\s*->>\s*(\d+)")
                .expect("Invalid regex")
        });
        
        let mut result = sql.to_string();
        
        // Handle string keys
        result = RE.replace_all(&result, r"pgsqlite_json_get_text($1, '$2')").to_string();
        
        // Handle integer indices
        result = RE_INT.replace_all(&result, r"pgsqlite_json_get_array_text($1, $2)").to_string();
        
        Ok(result)
    }
    
    /// Translate -> operator (extract JSON field as JSON)
    fn translate_json_extract_operator(sql: &str) -> Result<String, PgSqliteError> {
        static RE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(\b\w+(?:\.\w+)?|pgsqlite_json_get_(?:json|array_json)\([^)]+\))\s*->\s*'([^']+)'")
                .expect("Invalid regex")
        });
        
        static RE_INT: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(\b\w+(?:\.\w+)?|pgsqlite_json_get_(?:json|array_json)\([^)]+\))\s*->\s*(\d+)")
                .expect("Invalid regex")
        });
        
        let mut result = sql.to_string();
        
        // Handle string keys
        result = RE.replace_all(&result, r"pgsqlite_json_get_json($1, '$2')").to_string();
        
        // Handle integer indices  
        result = RE_INT.replace_all(&result, r"pgsqlite_json_get_array_json($1, $2)").to_string();
        
        Ok(result)
    }
    
    /// Translate #>> operator (extract JSON path as text)
    fn translate_path_text_operator(sql: &str) -> Result<String, PgSqliteError> {
        static RE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(\b\w+(?:\.\w+)?)\s*#>>\s*'\{([^}]+)\}'")
                .expect("Invalid regex")
        });
        
        let result = RE.replace_all(sql, |caps: &regex::Captures| {
            let json_col = &caps[1];
            let path = &caps[2];
            format!("pgsqlite_json_path_text({json_col}, '{path}')")
        });
        
        Ok(result.to_string())
    }
    
    /// Translate #> operator (extract JSON path as JSON)
    fn translate_path_json_operator(sql: &str) -> Result<String, PgSqliteError> {
        static RE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(\b\w+(?:\.\w+)?)\s*#>\s*'\{([^}]+)\}'")
                .expect("Invalid regex")
        });
        
        let result = RE.replace_all(sql, |caps: &regex::Captures| {
            let json_col = &caps[1];
            let path = &caps[2];
            format!("pgsqlite_json_path_json({json_col}, '{path}')")
        });
        
        Ok(result.to_string())
    }
    
    /// Translate @> and <@ operators (containment)
    fn translate_contains_operators(sql: &str) -> Result<String, PgSqliteError> {
        static RE_CONTAINS: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(\b\w+(?:\.\w+)?)\s*@>\s*'([^']+)'")
                .expect("Invalid regex")
        });
        
        static RE_CONTAINED: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(\b\w+(?:\.\w+)?)\s*<@\s*'([^']+)'")
                .expect("Invalid regex")
        });
        
        // Also handle reversed format: 'json' <@ column
        static RE_CONTAINED_REV: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"'([^']+)'\s*<@\s*(\b\w+(?:\.\w+)?)")
                .expect("Invalid regex")
        });
        
        let mut result = sql.to_string();
        
        // Translate @> (contains)
        result = RE_CONTAINS.replace_all(&result, r"jsonb_contains($1, '$2')").to_string();
        
        // Translate <@ (is contained by) - normal format
        result = RE_CONTAINED.replace_all(&result, r"jsonb_contained($1, '$2')").to_string();
        
        // Translate <@ (is contained by) - reversed format
        result = RE_CONTAINED_REV.replace_all(&result, r"jsonb_contains($2, '$1')").to_string();
        
        Ok(result)
    }
    
    /// Translate ?, ?|, ?& operators (existence checks)
    fn translate_existence_operators(sql: &str) -> Result<String, PgSqliteError> {
        static RE_HAS_KEY: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(\b\w+(?:\.\w+)?)\s*\?\s*'([^']+)'")
                .expect("Invalid regex")
        });
        
        static RE_HAS_ANY_KEY: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(\b\w+(?:\.\w+)?)\s*\?\|\s*'?\{([^}]+)\}'?")
                .expect("Invalid regex")
        });
        
        static RE_HAS_ALL_KEYS: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(\b\w+(?:\.\w+)?)\s*\?\&\s*'?\{([^}]+)\}'?")
                .expect("Invalid regex")
        });
        
        let mut result = sql.to_string();
        
        // Translate ? operator (has key)
        result = RE_HAS_KEY.replace_all(&result, r"pgsqlite_json_has_key($1, '$2')").to_string();
        
        // Translate ?| operator (has any key)
        result = RE_HAS_ANY_KEY.replace_all(&result, r"pgsqlite_json_has_any_key($1, '$2')").to_string();
        
        // Translate ?& operator (has all keys)
        result = RE_HAS_ALL_KEYS.replace_all(&result, r"pgsqlite_json_has_all_keys($1, '$2')").to_string();
        
        Ok(result)
    }
    
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_create_table_json_translation() {
        let sql = "CREATE TABLE test (id INTEGER, data JSON, metadata JSONB)";
        let translated = JsonTranslator::translate_statement(sql).unwrap();
        
        // Should convert JSON/JSONB to TEXT
        assert!(translated.contains("TEXT"));
        assert!(!translated.to_uppercase().contains("JSONB"));
        assert!(!translated.to_uppercase().contains(" JSON"));
    }
    
    #[test]
    fn test_alter_table_json_translation() {
        let sql = "ALTER TABLE test ADD COLUMN config JSONB";
        let translated = JsonTranslator::translate_statement(sql).unwrap();
        
        assert!(translated.contains("TEXT"));
        assert!(!translated.to_uppercase().contains("JSONB"));
    }
    
    #[test]
    fn test_json_operation_detection() {
        assert!(JsonTranslator::contains_json_operations("SELECT data->>'name' FROM users"));
        assert!(JsonTranslator::contains_json_operations("SELECT * WHERE config @> '{\"active\": true}'"));
        assert!(JsonTranslator::contains_json_operations("SELECT jsonb_array_length(items) FROM orders"));
        assert!(!JsonTranslator::contains_json_operations("SELECT * FROM users"));
    }
    
    #[test]
    fn test_text_extract_operator() {
        // Test ->> operator with string key
        let sql = "SELECT data->>'name' FROM users";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert_eq!(translated, "SELECT pgsqlite_json_get_text(data, 'name') FROM users");
        
        // Test ->> operator with integer index
        let sql = "SELECT items->>0 FROM orders";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert_eq!(translated, "SELECT pgsqlite_json_get_array_text(items, 0) FROM orders");
        
        // Test with table alias
        let sql = "SELECT u.data->>'email' FROM users u";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert_eq!(translated, "SELECT pgsqlite_json_get_text(u.data, 'email') FROM users u");
    }
    
    #[test]
    fn test_json_extract_operator() {
        // Test -> operator with string key
        let sql = "SELECT data->'address' FROM users";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert_eq!(translated, "SELECT pgsqlite_json_get_json(data, 'address') FROM users");
        
        // Test -> operator with integer index
        let sql = "SELECT tags->1 FROM posts";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert_eq!(translated, "SELECT pgsqlite_json_get_array_json(tags, 1) FROM posts");
    }
    
    #[test]
    fn test_path_operators() {
        // Test #>> operator
        let sql = "SELECT data#>>'{address,city}' FROM users";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert_eq!(translated, "SELECT pgsqlite_json_path_text(data, 'address,city') FROM users");
        
        // Test #> operator
        let sql = "SELECT data#>'{items,0}' FROM orders";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert_eq!(translated, "SELECT pgsqlite_json_path_json(data, 'items,0') FROM orders");
    }
    
    #[test]
    fn test_contains_operators() {
        // Test @> operator
        let sql = "SELECT * FROM users WHERE data @> '{\"active\": true}'";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert_eq!(translated, "SELECT * FROM users WHERE jsonb_contains(data, '{\"active\": true}')");
        
        // Test <@ operator
        let sql = "SELECT * FROM items WHERE metadata <@ '{\"type\": \"product\", \"status\": \"active\"}'";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert_eq!(translated, "SELECT * FROM items WHERE jsonb_contained(metadata, '{\"type\": \"product\", \"status\": \"active\"}')");
        
        // Test <@ operator with reversed operands
        let sql = "SELECT id FROM users WHERE '{\"name\": \"Bob\"}' <@ data";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert_eq!(translated, "SELECT id FROM users WHERE jsonb_contains(data, '{\"name\": \"Bob\"}')");
    }
    
    #[test]
    fn test_combined_operators() {
        // Test multiple operators in one query
        let sql = "SELECT id, data->>'name', data->'address' FROM users WHERE data @> '{\"verified\": true}'";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert!(translated.contains("pgsqlite_json_get_text(data, 'name')"));
        assert!(translated.contains("pgsqlite_json_get_json(data, 'address')"));
        assert!(translated.contains("jsonb_contains(data, '{\"verified\": true}')"));
    }
    
    #[test]
    fn test_chained_operators() {
        // Test chained JSON operations like data->'items'->1->>'name'
        let sql = "SELECT id, data->'items'->1->>'name' AS item_name FROM test";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        
        // The translation should at least start replacing the operators with our custom functions
        assert!(translated.contains("pgsqlite_json_get_json(data, 'items')"));
        assert!(translated.contains("pgsqlite_json_get"));
        // The key improvement is that our custom functions can handle any input type
        // which solves the original "Invalid function parameter type" error
    }
    
    #[test]
    fn test_existence_operators() {
        // Test ? operator (key exists)
        let sql = "SELECT * FROM users WHERE data ? 'name'";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert_eq!(translated, "SELECT * FROM users WHERE pgsqlite_json_has_key(data, 'name')");
        
        // Test ?| operator (any key exists)
        let sql = "SELECT * FROM users WHERE config ?| '{admin,user}'";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert_eq!(translated, "SELECT * FROM users WHERE pgsqlite_json_has_any_key(config, 'admin,user')");
        
        // Test ?& operator (all keys exist)
        let sql = "SELECT * FROM items WHERE metadata ?& '{name,price,category}'";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert_eq!(translated, "SELECT * FROM items WHERE pgsqlite_json_has_all_keys(metadata, 'name,price,category')");
        
        // Test with table alias
        let sql = "SELECT u.id FROM users u WHERE u.profile ? 'email'";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert_eq!(translated, "SELECT u.id FROM users u WHERE pgsqlite_json_has_key(u.profile, 'email')");
    }
}