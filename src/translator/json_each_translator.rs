use crate::PgSqliteError;
use crate::translator::{TranslationMetadata, ColumnTypeHint, ExpressionType};
use crate::types::PgType;
use regex::Regex;
use once_cell::sync::Lazy;
use tracing::debug;

/// Regex patterns for json_each and jsonb_each function calls
static JSON_EACH_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(json_each|jsonb_each)\s*\(\s*([^)]+)\s*\)").unwrap()
});

static JSON_EACH_FROM_CLAUSE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\bFROM\s+(json_each|jsonb_each)\s*\(\s*([^)]+)\s*\)(?:\s+(?:AS\s+)?(\w+))?").unwrap()
});

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
        
        // Handle different patterns:
        // 1. FROM json_each(json_data) AS alias
        // 2. json_each(json_data) in SELECT clause
        
        result = Self::translate_from_clause(&result)?;
        result = Self::translate_select_clause(&result)?;
        
        Ok(result)
    }
    
    /// Translate json_each with metadata
    pub fn translate_with_metadata(sql: &str) -> Result<(String, TranslationMetadata), PgSqliteError> {
        if !Self::contains_json_each(sql) {
            return Ok((sql.to_string(), TranslationMetadata::new()));
        }
        
        let mut result = sql.to_string();
        let mut metadata = TranslationMetadata::new();
        
        // Translate json_each calls
        result = Self::translate_from_clause(&result)?;
        result = Self::translate_select_clause(&result)?;
        
        // Extract metadata for aliased json_each functions
        Self::extract_json_each_metadata(&result, &mut metadata);
        
        Ok((result, metadata))
    }
    
    /// Translate FROM json_each(json_data) AS alias to use only key and value columns
    fn translate_from_clause(sql: &str) -> Result<String, PgSqliteError> {
        let mut result = sql.to_string();
        
        // Collect replacements to avoid borrowing issues
        let mut replacements = Vec::new();
        for captures in JSON_EACH_FROM_CLAUSE_REGEX.captures_iter(&result) {
            let function_name = &captures[1]; // json_each or jsonb_each
            let json_expr = captures[2].trim();
            let alias = captures.get(3).map(|m| m.as_str()).unwrap_or("json_each_table");
            
            // Convert to SQLite json_each but only expose key and value columns
            // We wrap it in a subquery to ensure only key and value are available
            let replacement = format!(
                "(SELECT key, value FROM json_each({})) AS {}",
                json_expr, alias
            );
            
            debug!("Planning to translate FROM {}: {} -> {}", function_name, &captures[0], &replacement);
            replacements.push((captures[0].to_string(), replacement));
        }
        
        // Apply replacements
        for (original, replacement) in replacements {
            result = result.replace(&original, &replacement);
            debug!("Translated FROM json_each: {} -> {}", original, replacement);
        }
        
        Ok(result)
    }
    
    /// Translate json_each() calls in SELECT clause to subqueries
    fn translate_select_clause(sql: &str) -> Result<String, PgSqliteError> {
        let mut result = sql.to_string();
        
        // Handle json_each() in SELECT clause - convert to row-returning subquery
        let mut replacements = Vec::new();
        for captures in JSON_EACH_REGEX.captures_iter(&result) {
            let function_name = &captures[1]; // json_each or jsonb_each
            let json_expr = captures[2].trim();
            
            // Check if this is already part of a FROM clause (avoid double translation)
            let full_match = &captures[0];
            if result.contains(&format!("FROM {}", full_match)) {
                continue; // Skip this one, it's handled by translate_from_clause
            }
            
            // This is a simplified translation for json_each in SELECT
            // PostgreSQL json_each returns rows, so we need to handle this appropriately
            let replacement = format!(
                "(SELECT json_group_array(json_object('key', key, 'value', value)) FROM json_each({}))",
                json_expr
            );
            
            debug!("Planning to translate SELECT {}: {} -> {}", function_name, full_match, &replacement);
            replacements.push((full_match.to_string(), replacement));
        }
        
        // Apply replacements
        for (original, replacement) in replacements {
            result = result.replace(&original, &replacement);
            debug!("Translated SELECT json_each: {} -> {}", original, replacement);
        }
        
        Ok(result)
    }
    
    /// Extract metadata for aliased json_each functions
    fn extract_json_each_metadata(sql: &str, metadata: &mut TranslationMetadata) {
        // Look for aliased json_each functions
        let alias_regex = Regex::new(r"(?i)json_each\s*\([^)]+\)\s+(?:AS\s+)?(\w+)").unwrap();
        
        for captures in alias_regex.captures_iter(sql) {
            let alias = captures[1].to_string();
            debug!("Found json_each alias: {}", alias);
            
            // Add type hints for key and value columns
            metadata.add_hint(format!("{}.key", alias), ColumnTypeHint {
                source_column: None,
                suggested_type: Some(PgType::Text),
                datetime_subtype: None,
                is_expression: true,
                expression_type: Some(ExpressionType::Other),
            });
            
            metadata.add_hint(format!("{}.value", alias), ColumnTypeHint {
                source_column: None,
                suggested_type: Some(PgType::Json), // Could be any JSON type
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
        assert!(result.contains("(SELECT key, value FROM json_each("));
        assert!(result.contains(")) AS t"));
    }
    
    #[test]
    fn test_jsonb_each_from_clause() {
        let sql = "SELECT key, value FROM jsonb_each('{\"a\": 1, \"b\": 2}') AS t";
        let result = JsonEachTranslator::translate_json_each(sql).unwrap();
        assert!(result.contains("(SELECT key, value FROM json_each("));
        assert!(result.contains(")) AS t"));
    }
    
    #[test]
    fn test_json_each_from_clause_with_alias() {
        let sql = "SELECT t.key, t.value FROM json_each('{\"name\": \"Alice\"}') AS t";
        let result = JsonEachTranslator::translate_json_each(sql).unwrap();
        assert!(result.contains("(SELECT key, value FROM json_each('{\"name\": \"Alice\"}')) AS t"));
    }
    
    #[test]
    fn test_json_each_select_clause() {
        let sql = "SELECT json_each(data) FROM table1";
        let result = JsonEachTranslator::translate_json_each(sql).unwrap();
        assert!(result.contains("(SELECT json_group_array(json_object('key', key, 'value', value)) FROM json_each(data))"));
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
        let (result, metadata) = JsonEachTranslator::translate_with_metadata(sql).unwrap();
        assert!(result.contains("(SELECT key, value FROM json_each("));
        // The metadata should contain hints for key and value columns
    }
}