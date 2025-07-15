use crate::PgSqliteError;
use crate::translator::{TranslationMetadata, ColumnTypeHint, ExpressionType};
use crate::types::PgType;
use regex::Regex;
use tracing::debug;


/// Translates PostgreSQL json_each()/jsonb_each() and json_each_text()/jsonb_each_text() function calls 
/// to SQLite json_each() equivalents with proper column selection for PostgreSQL compatibility
pub struct JsonEachTranslator;

impl JsonEachTranslator {
    /// Check if SQL contains json_each or jsonb_each function calls (including _text variants)
    pub fn contains_json_each(sql: &str) -> bool {
        // Fast path: check for json_each before any expensive operations
        if !sql.contains("json_each") && !sql.contains("jsonb_each") {
            return false;
        }
        
        // Only do lowercase conversion if json_each is present
        let sql_lower = sql.to_lowercase();
        sql_lower.contains("json_each(") || sql_lower.contains("jsonb_each(") ||
        sql_lower.contains("json_each_text(") || sql_lower.contains("jsonb_each_text(")
    }
    
    /// Translate json_each()/jsonb_each() and json_each_text()/jsonb_each_text() function calls to SQLite json_each() equivalents
    pub fn translate_json_each(sql: &str) -> Result<String, PgSqliteError> {
        if !Self::contains_json_each(sql) {
            return Ok(sql.to_string());
        }
        
        let mut result = sql.to_string();
        
        // Step 1: Replace jsonb_each variants with json_each
        result = result.replace("jsonb_each_text(", "json_each_text(");
        result = result.replace("jsonb_each(", "json_each(");
        
        // Step 2: Handle json_each_text() - converts all values to text, including nested objects/arrays
        // We handle these differently based on whether it's a FROM clause or cross join
        let json_each_text_from_regex = Regex::new(r"\bFROM\s+json_each_text\(([^)]+)\)\s+AS\s+(\w+)").unwrap();
        
        result = json_each_text_from_regex.replace_all(&result, |caps: &regex::Captures| {
            let json_expr = caps.get(1).unwrap().as_str();
            let alias = caps.get(2).unwrap().as_str();
            // For json_each_text, use json_each_text_value function
            let replacement = format!("FROM (SELECT ('' || key) AS key, json_each_text_value({}, key) AS value FROM json_each({})) AS {}", json_expr, json_expr, alias);
            debug!("JSON each_text FROM translation: {} -> {}", &caps[0], replacement);
            replacement
        }).to_string();
        
        // Handle cross join pattern for json_each_text
        // For cross joins, we can't use a subquery, so we just replace the function name
        // The columns will be accessed directly and need to be cast to text in the SELECT
        result = result.replace("json_each_text(", "json_each(");
        
        // Step 3: Handle json_each() in FROM clause (not cross joins)
        let json_each_regex = Regex::new(r"\bFROM\s+json_each\(([^)]+)\)\s+AS\s+(\w+)").unwrap();
        
        result = json_each_regex.replace_all(&result, |caps: &regex::Captures| {
            let json_expr = caps.get(1).unwrap().as_str();
            let alias = caps.get(2).unwrap().as_str();
            // Use custom json_each_value function to handle boolean conversion properly
            let replacement = format!("FROM (SELECT ('' || key) AS key, json_each_value({}, key) AS value FROM json_each({})) AS {}", json_expr, json_expr, alias);
            debug!("JSON each translation: {} -> {}", &caps[0], replacement);
            replacement
        }).to_string();
        
        Ok(result)
    }
    
    /// Translate json_each with metadata
    pub fn translate_with_metadata(sql: &str) -> Result<(String, TranslationMetadata), PgSqliteError> {
        if !Self::contains_json_each(sql) {
            return Ok((sql.to_string(), TranslationMetadata::new()));
        }
        
        let mut metadata = TranslationMetadata::new();
        
        // Use the same translation logic as translate_json_each
        let result = Self::translate_json_each(sql)?;
        
        // Extract metadata for aliased json_each functions
        Self::extract_json_each_metadata(&result, &mut metadata);
        
        Ok((result, metadata))
    }
    
    /// Extract metadata for aliased json_each functions
    fn extract_json_each_metadata(sql: &str, metadata: &mut TranslationMetadata) {
        // If the SQL contains json_each, add aggressive type hints for all possible column references
        if sql.contains("json_each") {
            let text_hint = ColumnTypeHint {
                source_column: None,
                suggested_type: Some(PgType::Text),
                datetime_subtype: None,
                is_expression: true,
                expression_type: Some(ExpressionType::Other),
            };
            
            // Add aggressive type hints for all possible json_each column references
            // These are the most common ways json_each columns are accessed
            metadata.add_hint("key".to_string(), text_hint.clone());
            metadata.add_hint("value".to_string(), text_hint.clone());
            
            // Common aliases used in json_each queries
            for alias in &["t", "j", "json", "data", "expanded", "item", "row"] {
                metadata.add_hint(format!("{}.key", alias), text_hint.clone());
                metadata.add_hint(format!("{}.value", alias), text_hint.clone());
            }
            
            debug!("Added aggressive type hints for json_each columns: key and value as TEXT");
        }
        
        // Also look for specific aliased json_each functions for additional context
        let alias_regex = Regex::new(r"(?i)json_each\s*\([^)]+\)\s+(?:AS\s+)?(\w+)").unwrap();
        
        for captures in alias_regex.captures_iter(sql) {
            let alias = captures[1].to_string();
            debug!("Found json_each alias: {}", alias);
            
            let text_hint = ColumnTypeHint {
                source_column: None,
                suggested_type: Some(PgType::Text),
                datetime_subtype: None,
                is_expression: true,
                expression_type: Some(ExpressionType::Other),
            };
            
            // Add specific hints for this alias
            metadata.add_hint(format!("{}.key", alias), text_hint.clone());
            metadata.add_hint(format!("{}.value", alias), text_hint.clone());
            
            debug!("Added specific type hints for json_each alias '{}'", alias);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_json_each_from_clause() {
        let sql = "SELECT key, value FROM json_each('{\"a\": 1, \"b\": 2}') AS t";
        let result = JsonEachTranslator::translate_json_each(sql).unwrap();
        assert!(result.contains("FROM (SELECT ('' || key) AS key, json_each_value('{\"a\": 1, \"b\": 2}', key) AS value FROM json_each('{\"a\": 1, \"b\": 2}')) AS t"));
    }
    
    #[test]
    fn test_jsonb_each_from_clause() {
        let sql = "SELECT key, value FROM jsonb_each('{\"a\": 1, \"b\": 2}') AS t";
        let result = JsonEachTranslator::translate_json_each(sql).unwrap();
        assert!(result.contains("FROM (SELECT ('' || key) AS key, json_each_value('{\"a\": 1, \"b\": 2}', key) AS value FROM json_each('{\"a\": 1, \"b\": 2}')) AS t"));
    }
    
    #[test]
    fn test_json_each_from_clause_with_alias() {
        let sql = "SELECT t.key, t.value FROM json_each('{\"name\": \"Alice\"}') AS t";
        let result = JsonEachTranslator::translate_json_each(sql).unwrap();
        assert!(result.contains("FROM (SELECT ('' || key) AS key, json_each_value('{\"name\": \"Alice\"}', key) AS value FROM json_each('{\"name\": \"Alice\"}')) AS t"));
    }
    
    #[test]
    fn test_json_each_select_clause() {
        let sql = "SELECT json_each(data) FROM table1";
        let result = JsonEachTranslator::translate_json_each(sql).unwrap();
        assert_eq!(result, sql); // Should be unchanged since it's already json_each
    }
    
    #[test]
    fn test_no_json_each() {
        let sql = "SELECT name FROM users";
        let result = JsonEachTranslator::translate_json_each(sql).unwrap();
        assert_eq!(result, "SELECT name FROM users");
    }
    
    #[test]
    fn test_contains_json_each() {
        assert!(JsonEachTranslator::contains_json_each("SELECT json_each(data) FROM table"));
        assert!(JsonEachTranslator::contains_json_each("FROM json_each(data) AS t"));
        assert!(JsonEachTranslator::contains_json_each("SELECT jsonb_each(data) FROM table"));
        assert!(JsonEachTranslator::contains_json_each("FROM jsonb_each(data) AS t"));
        assert!(JsonEachTranslator::contains_json_each("SELECT json_each_text(data) FROM table"));
        assert!(JsonEachTranslator::contains_json_each("FROM json_each_text(data) AS t"));
        assert!(JsonEachTranslator::contains_json_each("SELECT jsonb_each_text(data) FROM table"));
        assert!(JsonEachTranslator::contains_json_each("FROM jsonb_each_text(data) AS t"));
        assert!(!JsonEachTranslator::contains_json_each("SELECT name FROM users"));
    }
    
    #[test]
    fn test_json_each_with_metadata() {
        let sql = "SELECT key, value FROM json_each('{\"a\": 1}') AS expanded";
        let (result, _metadata) = JsonEachTranslator::translate_with_metadata(sql).unwrap();
        assert!(result.contains("FROM (SELECT ('' || key) AS key, json_each_value('{\"a\": 1}', key) AS value FROM json_each('{\"a\": 1}')) AS expanded"));
        // The metadata should contain hints for key and value columns
    }
    
    #[test]
    fn test_json_each_text_from_clause() {
        let sql = "SELECT key, value FROM json_each_text('{\"a\": 1, \"b\": true, \"c\": null}') AS t";
        let result = JsonEachTranslator::translate_json_each(sql).unwrap();
        assert!(result.contains("FROM (SELECT ('' || key) AS key, json_each_text_value('{\"a\": 1, \"b\": true, \"c\": null}', key) AS value FROM json_each('{\"a\": 1, \"b\": true, \"c\": null}')) AS t"));
    }
    
    #[test]
    fn test_jsonb_each_text_from_clause() {
        let sql = "SELECT key, value FROM jsonb_each_text('{\"x\": [1,2,3], \"y\": {\"nested\": true}}') AS t";
        let result = JsonEachTranslator::translate_json_each(sql).unwrap();
        assert!(result.contains("FROM (SELECT ('' || key) AS key, json_each_text_value('{\"x\": [1,2,3], \"y\": {\"nested\": true}}', key) AS value FROM json_each('{\"x\": [1,2,3], \"y\": {\"nested\": true}}')) AS t"));
    }
    
    #[test]
    fn test_json_each_text_with_alias() {
        let sql = "SELECT expanded.key, expanded.value FROM json_each_text(data) AS expanded";
        let result = JsonEachTranslator::translate_json_each(sql).unwrap();
        assert!(result.contains("FROM (SELECT ('' || key) AS key, json_each_text_value(data, key) AS value FROM json_each(data)) AS expanded"));
    }
    
    #[test]
    fn test_json_each_text_cross_join() {
        let sql = "SELECT t.id, e.key, e.value FROM test_table t, json_each_text(t.data) AS e";
        let result = JsonEachTranslator::translate_json_each(sql).unwrap();
        // For cross joins, json_each_text is just replaced with json_each
        assert!(result.contains("json_each(t.data) AS e"));
        assert!(!result.contains("json_each_text(t.data)"));
    }
}