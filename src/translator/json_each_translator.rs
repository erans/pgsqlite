use crate::PgSqliteError;
use crate::translator::{TranslationMetadata, ColumnTypeHint, ExpressionType};
use crate::types::PgType;
use regex::Regex;
use tracing::debug;


/// Translates PostgreSQL json_each()/jsonb_each() function calls to SQLite json_each() equivalents
/// with proper column selection for PostgreSQL compatibility
pub struct JsonEachTranslator;

impl JsonEachTranslator {
    /// Check if SQL contains json_each or jsonb_each function calls
    pub fn contains_json_each(sql: &str) -> bool {
        // Fast path: check for json_each before any expensive operations
        if !sql.contains("json_each") && !sql.contains("jsonb_each") {
            return false;
        }
        
        // Only do lowercase conversion if json_each is present
        let sql_lower = sql.to_lowercase();
        sql_lower.contains("json_each(") || sql_lower.contains("jsonb_each(")
    }
    
    /// Translate json_each()/jsonb_each() function calls to SQLite json_each() equivalents
    pub fn translate_json_each(sql: &str) -> Result<String, PgSqliteError> {
        if !Self::contains_json_each(sql) {
            return Ok(sql.to_string());
        }
        
        let mut result = sql.to_string();
        
        // Step 1: Replace jsonb_each with json_each
        result = result.replace("jsonb_each(", "json_each(");
        
        // Step 2: Replace json_each with a wrapped subquery that casts to TEXT
        // This ensures PostgreSQL compatibility for both key and value columns
        let json_each_regex = Regex::new(r"\bFROM\s+json_each\(([^)]+)\)\s+AS\s+(\w+)").unwrap();
        
        result = json_each_regex.replace_all(&result, |caps: &regex::Captures| {
            let json_expr = caps.get(1).unwrap().as_str();
            let alias = caps.get(2).unwrap().as_str();
            // Use substr to force TEXT type more reliably - substr always returns TEXT
            let replacement = format!("FROM (SELECT substr(key, 1) AS key, substr(value, 1) AS value FROM json_each({})) AS {}", json_expr, alias);
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
        // Look for aliased json_each functions
        let alias_regex = Regex::new(r"(?i)json_each\s*\([^)]+\)\s+(?:AS\s+)?(\w+)").unwrap();
        
        for captures in alias_regex.captures_iter(sql) {
            let alias = captures[1].to_string();
            debug!("Found json_each alias: {}", alias);
            
            // Add type hints for both aliased and non-aliased column access patterns
            // Many queries access columns as just "key" and "value" without the alias prefix
            
            // Non-aliased access (e.g., SELECT key, value FROM json_each(...) AS t)
            metadata.add_hint("key".to_string(), ColumnTypeHint {
                source_column: None,
                suggested_type: Some(PgType::Text),
                datetime_subtype: None,
                is_expression: true,
                expression_type: Some(ExpressionType::Other),
            });
            
            metadata.add_hint("value".to_string(), ColumnTypeHint {
                source_column: None,
                suggested_type: Some(PgType::Text),
                datetime_subtype: None,
                is_expression: true,
                expression_type: Some(ExpressionType::Other),
            });
            
            debug!("Added type hints for json_each columns: key and value as TEXT");
            
            // Aliased access (e.g., SELECT t.key, t.value FROM json_each(...) AS t)
            metadata.add_hint(format!("{}.key", alias), ColumnTypeHint {
                source_column: None,
                suggested_type: Some(PgType::Text),
                datetime_subtype: None,
                is_expression: true,
                expression_type: Some(ExpressionType::Other),
            });
            
            metadata.add_hint(format!("{}.value", alias), ColumnTypeHint {
                source_column: None,
                suggested_type: Some(PgType::Text),
                datetime_subtype: None,
                is_expression: true,
                expression_type: Some(ExpressionType::Other),
            });
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
        assert!(result.contains("FROM (SELECT substr(key, 1) AS key, substr(value, 1) AS value FROM json_each('{\"a\": 1, \"b\": 2}')) AS t"));
    }
    
    #[test]
    fn test_jsonb_each_from_clause() {
        let sql = "SELECT key, value FROM jsonb_each('{\"a\": 1, \"b\": 2}') AS t";
        let result = JsonEachTranslator::translate_json_each(sql).unwrap();
        assert!(result.contains("FROM (SELECT substr(key, 1) AS key, substr(value, 1) AS value FROM json_each('{\"a\": 1, \"b\": 2}')) AS t"));
    }
    
    #[test]
    fn test_json_each_from_clause_with_alias() {
        let sql = "SELECT t.key, t.value FROM json_each('{\"name\": \"Alice\"}') AS t";
        let result = JsonEachTranslator::translate_json_each(sql).unwrap();
        assert!(result.contains("FROM (SELECT substr(key, 1) AS key, substr(value, 1) AS value FROM json_each('{\"name\": \"Alice\"}')) AS t"));
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
        assert!(!JsonEachTranslator::contains_json_each("SELECT name FROM users"));
    }
    
    #[test]
    fn test_json_each_with_metadata() {
        let sql = "SELECT key, value FROM json_each('{\"a\": 1}') AS expanded";
        let (result, _metadata) = JsonEachTranslator::translate_with_metadata(sql).unwrap();
        assert!(result.contains("FROM (SELECT substr(key, 1) AS key, substr(value, 1) AS value FROM json_each('{\"a\": 1}')) AS expanded"));
        // The metadata should contain hints for key and value columns
    }
}