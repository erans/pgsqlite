use crate::PgSqliteError;
use crate::translator::{TranslationMetadata, ColumnTypeHint, ExpressionType};
use crate::types::PgType;
use regex::Regex;
use once_cell::sync::Lazy;
use tracing::debug;

/// Regex patterns for array_agg function variants
static ARRAY_AGG_DISTINCT_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)array_agg\s*\(\s*DISTINCT\s+([^)]+)\s*\)").unwrap()
});

static ARRAY_AGG_ORDER_BY_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)array_agg\s*\(\s*([^)]+?)\s+ORDER\s+BY\s+([^)]+)\s*\)").unwrap()
});

static ARRAY_AGG_DISTINCT_ORDER_BY_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)array_agg\s*\(\s*DISTINCT\s+([^)]+?)\s+ORDER\s+BY\s+([^)]+)\s*\)").unwrap()
});

/// Translates PostgreSQL array_agg functions with ORDER BY and DISTINCT
pub struct ArrayAggTranslator;

impl ArrayAggTranslator {
    /// Check if SQL contains array_agg with ORDER BY or DISTINCT
    pub fn contains_enhanced_array_agg(sql: &str) -> bool {
        // Fast path: check for array_agg before any expensive operations
        if !sql.contains("array_agg") {
            return false;
        }
        
        // Only do lowercase conversion if array_agg is present
        let sql_lower = sql.to_lowercase();
        
        // Check for ORDER BY or DISTINCT in array_agg context
        sql_lower.contains("order by") || sql_lower.contains("distinct")
    }
    
    /// Translate array_agg functions with ORDER BY and DISTINCT support
    pub fn translate_array_agg(sql: &str) -> Result<String, PgSqliteError> {
        if !Self::contains_enhanced_array_agg(sql) {
            return Ok(sql.to_string());
        }
        
        let mut result = sql.to_string();
        
        // Process in order of specificity:
        // 1. DISTINCT + ORDER BY (most specific)
        // 2. ORDER BY only
        // 3. DISTINCT only
        
        result = Self::translate_distinct_order_by(&result)?;
        result = Self::translate_order_by(&result)?;
        result = Self::translate_distinct(&result)?;
        
        Ok(result)
    }
    
    /// Translate array_agg functions and return metadata
    pub fn translate_with_metadata(sql: &str) -> Result<(String, TranslationMetadata), PgSqliteError> {
        if !Self::contains_enhanced_array_agg(sql) {
            return Ok((sql.to_string(), TranslationMetadata::new()));
        }
        
        let mut result = sql.to_string();
        let mut metadata = TranslationMetadata::new();
        
        // Process translations
        result = Self::translate_distinct_order_by(&result)?;
        result = Self::translate_order_by(&result)?;
        result = Self::translate_distinct(&result)?;
        
        // Extract metadata for aliased array_agg functions
        Self::extract_array_agg_metadata(&result, &mut metadata);
        
        Ok((result, metadata))
    }
    
    /// Translate array_agg(DISTINCT expr ORDER BY expr2)
    fn translate_distinct_order_by(sql: &str) -> Result<String, PgSqliteError> {
        let mut result = sql.to_string();
        
        // Collect replacements first to avoid borrowing issues
        let mut replacements = Vec::new();
        for captures in ARRAY_AGG_DISTINCT_ORDER_BY_REGEX.captures_iter(&result) {
            let expr = captures[1].trim();
            let _order_by = captures[2].trim();
            
            // For DISTINCT with ORDER BY, we need to use a subquery approach
            // Since SQLite doesn't support ORDER BY in aggregate functions directly,
            // we'll use a workaround with GROUP BY and MIN/MAX for ordering
            let _complex_replacement = format!(
                "(SELECT array_agg_distinct({})\n                 FROM (\n                     SELECT DISTINCT {} \n                     FROM (SELECT {} FROM {}) \n                     ORDER BY {}\n                 ))",
                expr, expr, expr, "?", _order_by
            );
            
            // Since we can't determine the table context here, we'll use a simpler approach
            // that relies on the outer query context
            let simple_replacement = format!("array_agg_distinct({expr})");
            
            replacements.push((captures[0].to_string(), simple_replacement));
        }
        
        // Apply replacements
        for (original, replacement) in replacements {
            result = result.replace(&original, &replacement);
            debug!("Translated DISTINCT ORDER BY array_agg: {} -> {}", original, replacement);
        }
        
        Ok(result)
    }
    
    /// Translate array_agg(expr ORDER BY expr2)
    fn translate_order_by(sql: &str) -> Result<String, PgSqliteError> {
        let mut result = sql.to_string();
        
        // Collect replacements first to avoid borrowing issues
        let mut replacements = Vec::new();
        for captures in ARRAY_AGG_ORDER_BY_REGEX.captures_iter(&result) {
            let expr = captures[1].trim();
            let _order_by = captures[2].trim();
            
            // For now, we'll just use the regular array_agg and rely on the outer query's ORDER BY
            // This is a limitation that matches what's documented in the TODO
            let replacement = format!("array_agg({expr})");
            
            replacements.push((captures[0].to_string(), replacement));
        }
        
        // Apply replacements
        for (original, replacement) in replacements {
            result = result.replace(&original, &replacement);
            debug!("Translated ORDER BY array_agg: {} -> {} (ORDER BY not fully supported)", original, replacement);
        }
        
        Ok(result)
    }
    
    /// Translate array_agg(DISTINCT expr)
    fn translate_distinct(sql: &str) -> Result<String, PgSqliteError> {
        let mut result = sql.to_string();
        
        // Collect replacements first to avoid borrowing issues
        let mut replacements = Vec::new();
        for captures in ARRAY_AGG_DISTINCT_REGEX.captures_iter(&result) {
            let expr = captures[1].trim();
            let replacement = format!("array_agg_distinct({expr})");
            replacements.push((captures[0].to_string(), replacement));
        }
        
        // Apply replacements
        for (original, replacement) in replacements {
            result = result.replace(&original, &replacement);
            debug!("Translated DISTINCT array_agg: {} -> {}", original, replacement);
        }
        
        Ok(result)
    }
    
    /// Extract metadata for aliased array_agg functions
    fn extract_array_agg_metadata(sql: &str, metadata: &mut TranslationMetadata) {
        // Look for aliased array_agg functions
        let alias_regex = Regex::new(r"(?i)array_agg(?:_distinct)?\s*\([^)]+\)\s+(?:AS\s+)?(\w+)").unwrap();
        
        for captures in alias_regex.captures_iter(sql) {
            let alias = captures[1].to_string();
            debug!("Found array_agg alias: {}", alias);
            
            metadata.add_hint(alias, ColumnTypeHint {
                source_column: None,
                suggested_type: Some(PgType::Text), // Array stored as JSON TEXT
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
    fn test_array_agg_distinct() {
        let sql = "SELECT array_agg(DISTINCT name) FROM users";
        let result = ArrayAggTranslator::translate_array_agg(sql).unwrap();
        assert_eq!(result, "SELECT array_agg_distinct(name) FROM users");
    }
    
    #[test]
    fn test_array_agg_order_by() {
        let sql = "SELECT array_agg(name ORDER BY name) FROM users";
        let result = ArrayAggTranslator::translate_array_agg(sql).unwrap();
        assert_eq!(result, "SELECT array_agg(name) FROM users");
    }
    
    #[test]
    fn test_array_agg_distinct_order_by() {
        let sql = "SELECT array_agg(DISTINCT name ORDER BY name) FROM users";
        let result = ArrayAggTranslator::translate_array_agg(sql).unwrap();
        assert_eq!(result, "SELECT array_agg_distinct(name) FROM users");
    }
    
    #[test]
    fn test_array_agg_with_alias() {
        let sql = "SELECT array_agg(DISTINCT name) AS unique_names FROM users";
        let (result, metadata) = ArrayAggTranslator::translate_with_metadata(sql).unwrap();
        assert_eq!(result, "SELECT array_agg_distinct(name) AS unique_names FROM users");
        assert!(metadata.get_hint("unique_names").is_some());
    }
    
    #[test]
    fn test_no_enhanced_array_agg() {
        let sql = "SELECT array_agg(name) FROM users";
        let result = ArrayAggTranslator::translate_array_agg(sql).unwrap();
        assert_eq!(result, "SELECT array_agg(name) FROM users");
    }
    
    #[test]
    fn test_contains_enhanced_array_agg() {
        assert!(ArrayAggTranslator::contains_enhanced_array_agg("SELECT array_agg(DISTINCT name) FROM users"));
        assert!(ArrayAggTranslator::contains_enhanced_array_agg("SELECT array_agg(name ORDER BY name) FROM users"));
        assert!(!ArrayAggTranslator::contains_enhanced_array_agg("SELECT array_agg(name) FROM users"));
        assert!(!ArrayAggTranslator::contains_enhanced_array_agg("SELECT name FROM users"));
    }
}