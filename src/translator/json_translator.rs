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
            Regex::new(r"(\b\w+(?:\.\w+)?)\s*->>\s*'([^']+)'")
                .expect("Invalid regex")
        });
        
        static RE_INT: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(\b\w+(?:\.\w+)?)\s*->>\s*(\d+)")
                .expect("Invalid regex")
        });
        
        let mut result = sql.to_string();
        
        // Handle string keys
        result = RE.replace_all(&result, r"json_extract($1, '$$.$2')").to_string();
        
        // Handle integer indices
        result = RE_INT.replace_all(&result, r"json_extract($1, '$$[$2]')").to_string();
        
        Ok(result)
    }
    
    /// Translate -> operator (extract JSON field as JSON)
    fn translate_json_extract_operator(sql: &str) -> Result<String, PgSqliteError> {
        static RE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(\b\w+(?:\.\w+)?)\s*->\s*'([^']+)'")
                .expect("Invalid regex")
        });
        
        static RE_INT: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(\b\w+(?:\.\w+)?)\s*->\s*(\d+)")
                .expect("Invalid regex")
        });
        
        let mut result = sql.to_string();
        
        // Handle string keys
        result = RE.replace_all(&result, r"json_extract($1, '$$.$2')").to_string();
        
        // Handle integer indices  
        result = RE_INT.replace_all(&result, r"json_extract($1, '$$[$2]')").to_string();
        
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
            let json_path = Self::array_to_json_path(path);
            format!("json_extract({}, '{}')", json_col, json_path)
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
            let json_path = Self::array_to_json_path(path);
            format!("json_extract({}, '{}')", json_col, json_path)
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
        // For now, we'll return the SQL as-is since these operators are complex
        // and would require custom functions in SQLite
        // TODO: Implement custom functions for these operators
        Ok(sql.to_string())
    }
    
    /// Convert PostgreSQL array path notation to JSON path
    fn array_to_json_path(path: &str) -> String {
        let parts: Vec<&str> = path.split(',').map(|s| s.trim()).collect();
        let mut json_path = String::from("$");
        
        for part in parts {
            if let Ok(index) = part.parse::<usize>() {
                json_path.push_str(&format!("[{}]", index));
            } else {
                json_path.push_str(&format!(".{}", part));
            }
        }
        
        json_path
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
        assert_eq!(translated, "SELECT json_extract(data, '$.name') FROM users");
        
        // Test ->> operator with integer index
        let sql = "SELECT items->>0 FROM orders";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert_eq!(translated, "SELECT json_extract(items, '$[0]') FROM orders");
        
        // Test with table alias
        let sql = "SELECT u.data->>'email' FROM users u";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert_eq!(translated, "SELECT json_extract(u.data, '$.email') FROM users u");
    }
    
    #[test]
    fn test_json_extract_operator() {
        // Test -> operator with string key
        let sql = "SELECT data->'address' FROM users";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert_eq!(translated, "SELECT json_extract(data, '$.address') FROM users");
        
        // Test -> operator with integer index
        let sql = "SELECT tags->1 FROM posts";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert_eq!(translated, "SELECT json_extract(tags, '$[1]') FROM posts");
    }
    
    #[test]
    fn test_path_operators() {
        // Test #>> operator
        let sql = "SELECT data#>>'{address,city}' FROM users";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert_eq!(translated, "SELECT json_extract(data, '$.address.city') FROM users");
        
        // Test #> operator
        let sql = "SELECT data#>'{items,0}' FROM orders";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert_eq!(translated, "SELECT json_extract(data, '$.items[0]') FROM orders");
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
        let sql = "SELECT id, data->>'name', data->'address'->>'city' FROM users WHERE data @> '{\"verified\": true}'";
        let translated = JsonTranslator::translate_json_operators(sql).unwrap();
        assert!(translated.contains("json_extract(data, '$.name')"));
        assert!(translated.contains("json_extract(data, '$.address')"));
        assert!(translated.contains("jsonb_contains(data, '{\"verified\": true}')"));
    }
}