use crate::PgSqliteError;
use crate::translator::{TranslationMetadata, ColumnTypeHint, ExpressionType};
use crate::types::PgType;
use regex::Regex;
use once_cell::sync::Lazy;
use tracing::debug;

/// Regex patterns for unnest function calls
static UNNEST_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\bunnest\s*\(\s*([^)]+)\s*\)").unwrap()
});


static UNNEST_FROM_CLAUSE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\bFROM\s+unnest\s*\(\s*([^)]+)\s*\)(?:\s+(?:AS\s+)?(\w+))?").unwrap()
});

/// Translates PostgreSQL unnest() function calls to SQLite json_each() equivalents
pub struct UnnestTranslator;

impl UnnestTranslator {
    /// Check if SQL contains unnest function calls
    pub fn contains_unnest(sql: &str) -> bool {
        // Fast path: check for unnest before any expensive operations
        if !sql.contains("unnest") {
            return false;
        }
        
        // Only do lowercase conversion if unnest is present
        let sql_lower = sql.to_lowercase();
        sql_lower.contains("unnest(")
    }
    
    /// Translate unnest() function calls to json_each() equivalents
    pub fn translate_unnest(sql: &str) -> Result<String, PgSqliteError> {
        if !Self::contains_unnest(sql) {
            return Ok(sql.to_string());
        }
        
        let mut result = sql.to_string();
        
        // Handle different patterns:
        // 1. FROM unnest(array) AS alias
        // 2. unnest(array) in SELECT clause
        
        result = Self::translate_from_clause(&result)?;
        result = Self::translate_select_clause(&result)?;
        
        Ok(result)
    }
    
    /// Translate unnest with metadata
    pub fn translate_with_metadata(sql: &str) -> Result<(String, TranslationMetadata), PgSqliteError> {
        if !Self::contains_unnest(sql) {
            return Ok((sql.to_string(), TranslationMetadata::new()));
        }
        
        let mut result = sql.to_string();
        let mut metadata = TranslationMetadata::new();
        
        // Translate unnest calls
        result = Self::translate_from_clause(&result)?;
        result = Self::translate_select_clause(&result)?;
        
        // Extract metadata for aliased unnest functions
        Self::extract_unnest_metadata(&result, &mut metadata);
        
        Ok((result, metadata))
    }
    
    /// Translate FROM unnest(array) AS alias to FROM json_each(array) AS alias
    fn translate_from_clause(sql: &str) -> Result<String, PgSqliteError> {
        let mut result = sql.to_string();
        
        // Collect replacements to avoid borrowing issues
        let mut replacements = Vec::new();
        for captures in UNNEST_FROM_CLAUSE_REGEX.captures_iter(&result) {
            let array_expr = captures[1].trim();
            let alias = captures.get(2).map(|m| m.as_str()).unwrap_or("unnest_table");
            
            // Convert unnest(array) to json_each(array) with proper column selection
            let replacement = format!("json_each({}) AS {}", array_expr, alias);
            
            replacements.push((captures[0].to_string(), replacement));
        }
        
        // Apply replacements
        for (original, replacement) in replacements {
            result = result.replace(&original, &replacement);
            debug!("Translated FROM unnest: {} -> {}", original, replacement);
        }
        
        Ok(result)
    }
    
    /// Translate unnest() calls in SELECT clause to subqueries with json_each
    fn translate_select_clause(sql: &str) -> Result<String, PgSqliteError> {
        let mut result = sql.to_string();
        
        // Handle unnest() in SELECT clause - this is more complex
        // For now, we'll provide a basic translation
        let mut replacements = Vec::new();
        for captures in UNNEST_REGEX.captures_iter(&result) {
            let array_expr = captures[1].trim();
            
            // This is a simplified translation that works for basic cases
            // More complex cases might need different handling
            let replacement = format!("(SELECT value FROM json_each({}))", array_expr);
            
            replacements.push((captures[0].to_string(), replacement));
        }
        
        // Apply replacements
        for (original, replacement) in replacements {
            result = result.replace(&original, &replacement);
            debug!("Translated SELECT unnest: {} -> {}", original, replacement);
        }
        
        Ok(result)
    }
    
    /// Extract metadata for aliased unnest functions
    fn extract_unnest_metadata(sql: &str, metadata: &mut TranslationMetadata) {
        // Look for aliased unnest functions (now converted to json_each)
        let alias_regex = Regex::new(r"(?i)json_each\s*\([^)]+\)\s+(?:AS\s+)?(\w+)").unwrap();
        
        for captures in alias_regex.captures_iter(sql) {
            let alias = captures[1].to_string();
            debug!("Found json_each (unnest) alias: {}", alias);
            
            metadata.add_hint(alias, ColumnTypeHint {
                source_column: None,
                suggested_type: Some(PgType::Text), // json_each returns text values
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
    fn test_unnest_from_clause() {
        let sql = "SELECT value FROM unnest(ARRAY[1,2,3]) AS t";
        let result = UnnestTranslator::translate_unnest(sql).unwrap();
        assert!(result.contains("json_each"));
        assert!(!result.contains("unnest"));
    }
    
    #[test]
    fn test_unnest_from_clause_with_alias() {
        let sql = "SELECT t.value FROM unnest('[1,2,3]'::json) AS t";
        let result = UnnestTranslator::translate_unnest(sql).unwrap();
        assert!(result.contains("json_each('[1,2,3]'::json) AS t"));
    }
    
    #[test]
    fn test_unnest_select_clause() {
        let sql = "SELECT unnest(tags) FROM articles";
        let result = UnnestTranslator::translate_unnest(sql).unwrap();
        assert!(result.contains("(SELECT value FROM json_each(tags))"));
    }
    
    #[test]
    fn test_no_unnest() {
        let sql = "SELECT name FROM users";
        let result = UnnestTranslator::translate_unnest(sql).unwrap();
        assert_eq!(result, "SELECT name FROM users");
    }
    
    #[test]
    fn test_contains_unnest() {
        assert!(UnnestTranslator::contains_unnest("SELECT unnest(array) FROM table"));
        assert!(UnnestTranslator::contains_unnest("FROM unnest(array) AS t"));
        assert!(!UnnestTranslator::contains_unnest("SELECT name FROM users"));
    }
    
    #[test]
    fn test_unnest_with_metadata() {
        let sql = "SELECT value FROM unnest('[1,2,3]') AS expanded";
        let (result, _metadata) = UnnestTranslator::translate_with_metadata(sql).unwrap();
        assert!(result.contains("json_each"));
        // The metadata should contain hints for the alias if it's a table alias
    }
}