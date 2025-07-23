use crate::types::PgType;
use crate::translator::metadata::{TranslationMetadata, ColumnTypeHint};

/// Translator for PostgreSQL row_to_json() function calls
pub struct RowToJsonTranslator;

impl RowToJsonTranslator {
    /// Translate row_to_json(t) calls to JSON object construction
    pub fn translate_row_to_json(query: &str) -> (String, TranslationMetadata) {
        let mut metadata = TranslationMetadata::new();
        
        // Look for row_to_json patterns in the query
        let translated_query = if Self::contains_row_to_json_call(query) {
            Self::translate_query(query, &mut metadata)
        } else {
            query.to_string()
        };
        
        (translated_query, metadata)
    }
    
    /// Check if query contains row_to_json function calls
    fn contains_row_to_json_call(query: &str) -> bool {
        let query_lower = query.to_lowercase();
        query_lower.contains("row_to_json(")
    }
    
    /// Translate the query by converting row_to_json calls
    fn translate_query(query: &str, metadata: &mut TranslationMetadata) -> String {
        // For now, implement a simple translation that handles common patterns
        // This is a basic implementation - a full implementation would need
        // to parse the query AST and handle complex subqueries
        
        // Pattern: SELECT row_to_json(t) FROM (SELECT ...) t
        // This should be translated to JSON object construction
        
        // Look for the pattern and extract the subquery
        if let Some(translated) = Self::translate_subquery_pattern(query, metadata) {
            return translated;
        }
        
        // For simple cases, just return the original query
        // The SQLite function will handle basic conversions
        query.to_string()
    }
    
    /// Translate subquery patterns like SELECT row_to_json(t) FROM (SELECT ...) t
    fn translate_subquery_pattern(query: &str, metadata: &mut TranslationMetadata) -> Option<String> {
        use regex::Regex;
        
        // Pattern to match: row_to_json(alias) FROM (...) alias
        // We'll use a two-step approach since backreferences may not work as expected
        let pattern = r"(?i)row_to_json\s*\(\s*(\w+)\s*\)\s+FROM\s+\(\s*(.+?)\s*\)\s+(\w+)";
        
        if let Ok(re) = Regex::new(pattern) {
            if let Some(captures) = re.captures(query) {
                let alias1 = captures.get(1).map(|m| m.as_str()).unwrap_or("t");
                let subquery = captures.get(2).map(|m| m.as_str()).unwrap_or("");
                let alias2 = captures.get(3).map(|m| m.as_str()).unwrap_or("t");
                
                // Check if the aliases match (this is our backreference check)
                if alias1 != alias2 {
                    return None;
                }
                
                let alias = alias1;
                
                // Extract column names from the subquery
                if let Some(columns) = Self::extract_columns_from_select(subquery) {
                    // Build JSON object construction
                    let json_fields: Vec<String> = columns.iter()
                        .map(|col| format!("'{col}', {col}"))
                        .collect();
                    
                    let json_construction = format!("json_object({})", json_fields.join(", "));
                    
                    // Replace the row_to_json call with json_object
                    let result = query.replace(
                        &format!("row_to_json({alias})"),
                        &json_construction
                    );
                    
                    // Add metadata hint for the result type
                    let hint = ColumnTypeHint {
                        source_column: Some(alias.to_string()),
                        suggested_type: Some(PgType::Json),
                        datetime_subtype: None,
                        is_expression: true,
                        expression_type: None,
                    };
                    metadata.add_hint(alias.to_string(), hint);
                    
                    return Some(result);
                }
            }
        }
        
        None
    }
    
    /// Extract column names from a SELECT clause
    fn extract_columns_from_select(select_clause: &str) -> Option<Vec<String>> {
        // Simple extraction of column names from SELECT clause
        // This is a basic implementation - a full parser would be more robust
        
        if let Some(select_start) = select_clause.to_lowercase().find("select") {
            let after_select = &select_clause[select_start + 6..].trim();
            
            // Find the FROM clause to get the column part
            let columns_part = if let Some(from_pos) = after_select.to_lowercase().find(" from ") {
                &after_select[..from_pos]
            } else {
                after_select
            };
            
            // Split by comma and extract column names
            let columns: Vec<String> = columns_part
                .split(',')
                .map(|col| {
                    let col = col.trim();
                    // Handle aliases (col AS alias or col alias)
                    if let Some(as_pos) = col.to_lowercase().find(" as ") {
                        col[as_pos + 4..].trim().to_string()
                    } else {
                        // Simple column name (might have spaces for implicit alias)
                        let parts: Vec<&str> = col.split_whitespace().collect();
                        if parts.len() >= 2 {
                            parts[parts.len() - 1].to_string()
                        } else {
                            col.to_string()
                        }
                    }
                })
                .collect();
            
            return Some(columns);
        }
        
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_contains_row_to_json_call() {
        assert!(RowToJsonTranslator::contains_row_to_json_call("SELECT row_to_json(t) FROM users t"));
        assert!(RowToJsonTranslator::contains_row_to_json_call("SELECT ROW_TO_JSON(t) FROM users t"));
        assert!(!RowToJsonTranslator::contains_row_to_json_call("SELECT * FROM users"));
    }
    
    #[test]
    fn test_extract_columns_from_select() {
        let columns = RowToJsonTranslator::extract_columns_from_select("SELECT name, age FROM users");
        assert_eq!(columns, Some(vec!["name".to_string(), "age".to_string()]));
        
        let columns = RowToJsonTranslator::extract_columns_from_select("SELECT name AS user_name, age FROM users");
        assert_eq!(columns, Some(vec!["user_name".to_string(), "age".to_string()]));
    }
    
    #[test]
    fn test_translate_subquery_pattern() {
        let query = "SELECT row_to_json(t) FROM (SELECT name, age FROM users WHERE id = 1) t";
        let mut metadata = TranslationMetadata::new();
        
        let result = RowToJsonTranslator::translate_subquery_pattern(query, &mut metadata);
        assert!(result.is_some(), "Expected translation result for query: {query}");
        
        let translated = result.unwrap();
        assert!(translated.contains("json_object('name', name, 'age', age)"));
    }
}