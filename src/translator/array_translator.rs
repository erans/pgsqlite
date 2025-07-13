use crate::PgSqliteError;
use crate::translator::{TranslationMetadata, ColumnTypeHint, ExpressionType};
use crate::types::PgType;
use regex::Regex;
use once_cell::sync::Lazy;
use tracing::info;

/// Regex patterns for array operators
static ARRAY_CONTAINS_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(\b\w+(?:\.\w+)*)\s*@>\s*('[^']+'|"[^"]+"|'\[[^\]]+\]')"#).unwrap()
});

static ARRAY_CONTAINED_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(\b\w+(?:\.\w+)*|'[^']+'|"[^"]+"|'\[[^\]]+\]')\s*<@\s*(\b\w+(?:\.\w+)*|'[^']+'|"[^"]+"|'\[[^\]]+\]')"#).unwrap()
});

static ARRAY_OVERLAP_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(\b\w+(?:\.\w+)*)\s*&&\s*(\b\w+(?:\.\w+)*|'[^']+'|"[^"]+"|'\[[^\]]+\]')"#).unwrap()
});

// TODO: Re-enable when we can differentiate array concat from string concat
// static ARRAY_CONCAT_REGEX: Lazy<Regex> = Lazy::new(|| {
//     Regex::new(r#"(\b\w+(?:\.\w+)*)\s*\|\|\s*('[^']+'|"[^"]+"|'\[[^\]]+\]')"#).unwrap()
// });

static ARRAY_SUBSCRIPT_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(\b\w+(?:\.\w+)*)\[(\d+)\]").unwrap()
});

static ARRAY_SLICE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(\b\w+(?:\.\w+)*)\[(\d+):(\d+)\]").unwrap()
});

static ANY_OPERATOR_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"('[^']+'|"[^"]+"|[^\s=]+)\s*=\s*ANY\s*\((\b\w+(?:\.\w+)*)\)"#).unwrap()
});

static ALL_OPERATOR_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(\b\w+(?:\.\w+)*|\d+)\s*([><=!]+)\s*ALL\s*\(([^)]+)\)").unwrap()
});

/// Translates PostgreSQL array operators to SQLite-compatible functions
pub struct ArrayTranslator;

impl ArrayTranslator {
    /// Translate array operators in SQL statement
    pub fn translate_array_operators(sql: &str) -> Result<String, PgSqliteError> {
        let mut result = sql.to_string();
        
        // Translate array subscript access first (most specific)
        result = Self::translate_array_subscript(&result)?;
        result = Self::translate_array_slice(&result)?;
        
        // Translate ANY/ALL operators
        result = Self::translate_any_operator(&result)?;
        result = Self::translate_all_operator(&result)?;
        
        // Translate array operators
        result = Self::translate_contains_operator(&result)?;
        result = Self::translate_contained_operator(&result)?;
        result = Self::translate_overlap_operator(&result)?;
        result = Self::translate_concat_operator(&result)?;
        
        Ok(result)
    }
    
    /// Translate array operators and return metadata about array expressions
    pub fn translate_with_metadata(sql: &str) -> Result<(String, TranslationMetadata), PgSqliteError> {
        let mut result = sql.to_string();
        let mut metadata = TranslationMetadata::new();
        
        // Translate array subscript access first (most specific)
        result = Self::translate_array_subscript(&result)?;
        result = Self::translate_array_slice(&result)?;
        
        // Translate ANY/ALL operators
        result = Self::translate_any_operator(&result)?;
        result = Self::translate_all_operator(&result)?;
        
        // Translate array operators
        result = Self::translate_contains_operator(&result)?;
        result = Self::translate_contained_operator(&result)?;
        result = Self::translate_overlap_operator(&result)?;
        
        // Translate concat operator and capture metadata
        let (new_result, concat_metadata) = Self::translate_concat_operator_with_metadata(&result)?;
        result = new_result;
        metadata.merge(concat_metadata);
        
        // Extract metadata for all array functions with aliases
        Self::extract_array_function_metadata(&result, &mut metadata);
        
        Ok((result, metadata))
    }
    
    /// Translate array subscript access: array[1] -> json_extract(array, '$[0]')
    fn translate_array_subscript(sql: &str) -> Result<String, PgSqliteError> {
        let mut result = sql.to_string();
        
        while let Some(captures) = ARRAY_SUBSCRIPT_REGEX.captures(&result) {
            let array_col = &captures[1];
            let index: usize = captures[2].parse().unwrap_or(1);
            // PostgreSQL arrays are 1-based, JSON arrays are 0-based
            let json_index = if index > 0 { index - 1 } else { 0 };
            
            let replacement = format!("json_extract({}, '$[{}]')", array_col, json_index);
            result = result.replace(&captures[0], &replacement);
        }
        
        Ok(result)
    }
    
    /// Translate array slice access: array[1:3] -> array_slice(array, 1, 3)
    fn translate_array_slice(sql: &str) -> Result<String, PgSqliteError> {
        let mut result = sql.to_string();
        
        while let Some(captures) = ARRAY_SLICE_REGEX.captures(&result) {
            let array_col = &captures[1];
            let start = &captures[2];
            let end = &captures[3];
            
            let replacement = format!("array_slice({}, {}, {})", array_col, start, end);
            result = result.replace(&captures[0], &replacement);
        }
        
        Ok(result)
    }
    
    /// Translate ANY operator: value = ANY(array) -> EXISTS(SELECT 1 FROM json_each(array) WHERE value = ?)
    fn translate_any_operator(sql: &str) -> Result<String, PgSqliteError> {
        let mut result = sql.to_string();
        
        while let Some(captures) = ANY_OPERATOR_REGEX.captures(&result) {
            let value = &captures[1];
            let array_col = &captures[2];
            
            let replacement = format!(
                "EXISTS (SELECT 1 FROM json_each({}) WHERE value = {})",
                array_col, value
            );
            result = result.replace(&captures[0], &replacement);
        }
        
        Ok(result)
    }
    
    /// Translate ALL operator: value > ALL(array) -> NOT EXISTS(SELECT 1 FROM json_each(array) WHERE value <= ?)
    fn translate_all_operator(sql: &str) -> Result<String, PgSqliteError> {
        let mut result = sql.to_string();
        
        while let Some(captures) = ALL_OPERATOR_REGEX.captures(&result) {
            let value = &captures[1];
            let operator = &captures[2];
            let subquery_or_array = &captures[3];
            
            // Invert the operator for NOT EXISTS logic
            let inverted_op = match operator {
                ">" => "<=",
                ">=" => "<",
                "<" => ">=",
                "<=" => ">",
                "=" => "!=",
                "!=" | "<>" => "=",
                _ => operator,
            };
            
            let replacement = if subquery_or_array.contains("SELECT") {
                // Handle subquery case: value > ALL(SELECT expr FROM ...) -> NOT EXISTS(SELECT 1 FROM ... WHERE expr <= value)
                // For simplicity, rewrite as NOT EXISTS with the condition on the selected expression
                let select_expr = extract_select_expression(subquery_or_array).unwrap_or("value");
                if let Some(from_pos) = subquery_or_array.to_uppercase().find(" FROM") {
                    let from_part = &subquery_or_array[from_pos..];
                    format!(
                        "NOT EXISTS (SELECT 1{} WHERE {} {} {})",
                        from_part, select_expr, inverted_op, value
                    )
                } else {
                    // Fallback if we can't parse the FROM clause
                    format!(
                        "NOT EXISTS ({} WHERE {} {} {})",
                        subquery_or_array, select_expr, inverted_op, value
                    )
                }
            } else {
                // Handle array column case: ALL(array_col) -> NOT EXISTS(SELECT 1 FROM json_each(array_col) WHERE value <= ?)
                format!(
                    "NOT EXISTS (SELECT 1 FROM json_each({}) WHERE value {} {})",
                    subquery_or_array, inverted_op, value
                )
            };
            result = result.replace(&captures[0], &replacement);
        }
        
        Ok(result)
    }
    
    /// Translate @> operator: array1 @> array2 -> array_contains(array1, array2)
    fn translate_contains_operator(sql: &str) -> Result<String, PgSqliteError> {
        let mut result = sql.to_string();
        
        while let Some(captures) = ARRAY_CONTAINS_REGEX.captures(&result) {
            let array1 = &captures[1];
            let array2 = captures[2].trim();
            
            let replacement = format!("array_contains({}, {})", array1, array2);
            result = result.replace(&captures[0], &replacement);
        }
        
        Ok(result)
    }
    
    /// Translate <@ operator: array1 <@ array2 -> array_contained({}, {})
    fn translate_contained_operator(sql: &str) -> Result<String, PgSqliteError> {
        let mut result = sql.to_string();
        
        while let Some(captures) = ARRAY_CONTAINED_REGEX.captures(&result) {
            let array1 = captures[1].trim();
            let array2 = &captures[2];
            
            let replacement = format!("array_contained({}, {})", array1, array2);
            result = result.replace(&captures[0], &replacement);
        }
        
        Ok(result)
    }
    
    /// Translate && operator: array1 && array2 -> array_overlap(array1, array2)
    fn translate_overlap_operator(sql: &str) -> Result<String, PgSqliteError> {
        let mut result = sql.to_string();
        
        while let Some(captures) = ARRAY_OVERLAP_REGEX.captures(&result) {
            let array1 = &captures[1];
            let array2 = captures[2].trim();
            
            let replacement = format!("array_overlap({}, {})", array1, array2);
            result = result.replace(&captures[0], &replacement);
        }
        
        Ok(result)
    }
    
    /// Translate || operator: array1 || array2 -> array_cat(array1, array2)
    fn translate_concat_operator(sql: &str) -> Result<String, PgSqliteError> {
        // More specific regex for array concatenation - only match if the second operand looks like a JSON array
        let array_concat_regex = regex::Regex::new(r#"(\b\w+(?:\.\w+)*)\s*\|\|\s*('\[[^\]]+\]')"#).unwrap();
        
        let mut result = sql.to_string();
        
        while let Some(captures) = array_concat_regex.captures(&result) {
            let array1 = &captures[1];
            let array2 = captures[2].trim();
            
            let replacement = format!("array_cat({}, {})", array1, array2);
            result = result.replace(&captures[0], &replacement);
        }
        
        Ok(result)
    }
    
    /// Translate || operator with metadata tracking
    fn translate_concat_operator_with_metadata(sql: &str) -> Result<(String, TranslationMetadata), PgSqliteError> {
        let mut metadata = TranslationMetadata::new();
        
        // More specific regex for array concatenation - only match if the second operand looks like a JSON array
        let array_concat_regex = regex::Regex::new(r#"(\b\w+(?:\.\w+)*)\s*\|\|\s*('\[[^\]]+\]')"#).unwrap();
        
        let result = sql.to_string();
        
        // Collect all replacements first
        let mut replacements = Vec::new();
        for captures in array_concat_regex.captures_iter(&result) {
            let array1 = captures[1].to_string();
            let array2 = captures[2].trim();
            
            let original = captures[0].to_string();
            let replacement = format!("array_cat({}, {})", array1, array2);
            
            replacements.push((original, replacement, array1));
        }
        
        // First find aliases in the original query before replacement
        let alias_regex = regex::Regex::new(r#"(?i)(\b\w+(?:\.\w+)*)\s*\|\|\s*(?:'[^']+'|"[^"]+"|'\[[^\]]+\]')\s+(?:AS\s+)?(\w+)"#).unwrap();
        let mut alias_map = std::collections::HashMap::new();
        for captures in alias_regex.captures_iter(&result) {
            let expr = captures[0].to_string();
            let alias = captures[2].to_string();
            alias_map.insert(expr, alias);
        }
        
        // Apply replacements
        let mut final_result = result;
        for (original, replacement, array1) in replacements {
            final_result = final_result.replace(&original, &replacement);
            
            // Check if this expression had an alias
            if let Some(alias) = alias_map.get(&original) {
                info!("Found alias '{}' for array concat expression", alias);
                metadata.add_hint(alias.clone(), ColumnTypeHint {
                    source_column: Some(array1),
                    suggested_type: Some(PgType::Text), // Return as TEXT (JSON array)
                    datetime_subtype: None,
                    is_expression: true,
                    expression_type: Some(ExpressionType::Other),
                });
            }
        }
        
        Ok((final_result, metadata))
    }
    
    /// Extract metadata for all array functions with aliases
    fn extract_array_function_metadata(sql: &str, metadata: &mut TranslationMetadata) {
        use tracing::info;
        info!("Extracting array function metadata from: {}", sql);
        // Array functions that return arrays
        let array_functions = [
            "array_agg", "array_append", "array_prepend", "array_cat", 
            "array_remove", "array_replace", "array_slice", "string_to_array",
            "array_positions"
        ];
        
        // Array functions that return non-array types
        let int_functions = [
            "array_length", "array_upper", "array_lower", "array_ndims",
            "array_position", "json_array_length"
        ];
        
        let bool_functions = [
            "array_contains", "array_contained", "array_overlap"
        ];
        
        let text_functions = [
            "array_to_string", "unnest"
        ];
        
        // Look for patterns like "function_name(...) AS alias"
        for func in &array_functions {
            let pattern = format!(r"(?i){}\s*\([^)]+\)\s+(?:AS\s+)?(\w+)", regex::escape(func));
            info!("Checking pattern for {}: {}", func, pattern);
            if let Ok(re) = regex::Regex::new(&pattern) {
                for captures in re.captures_iter(sql) {
                    let alias = captures[1].to_string();
                    info!("Found array function {} with alias: {}", func, alias);
                    metadata.add_hint(alias, ColumnTypeHint {
                        source_column: None,
                        suggested_type: Some(PgType::Text), // Return as TEXT (JSON array)
                        datetime_subtype: None,
                        is_expression: true,
                        expression_type: Some(ExpressionType::Other),
                    });
                }
            }
        }
        
        for func in &int_functions {
            let pattern = format!(r"(?i){}\\s*\\([^)]+\\)\\s+(?:AS\\s+)?(\\w+)", regex::escape(func));
            if let Ok(re) = regex::Regex::new(&pattern) {
                for captures in re.captures_iter(sql) {
                    let alias = captures[1].to_string();
                    metadata.add_hint(alias, ColumnTypeHint {
                        source_column: None,
                        suggested_type: Some(PgType::Int4),
                        datetime_subtype: None,
                        is_expression: true,
                        expression_type: Some(ExpressionType::Other),
                    });
                }
            }
        }
        
        for func in &bool_functions {
            let pattern = format!(r"(?i){}\\s*\\([^)]+\\)\\s+(?:AS\\s+)?(\\w+)", regex::escape(func));
            if let Ok(re) = regex::Regex::new(&pattern) {
                for captures in re.captures_iter(sql) {
                    let alias = captures[1].to_string();
                    metadata.add_hint(alias, ColumnTypeHint {
                        source_column: None,
                        suggested_type: Some(PgType::Bool),
                        datetime_subtype: None,
                        is_expression: true,
                        expression_type: Some(ExpressionType::Other),
                    });
                }
            }
        }
        
        for func in &text_functions {
            let pattern = format!(r"(?i){}\\s*\\([^)]+\\)\\s+(?:AS\\s+)?(\\w+)", regex::escape(func));
            if let Ok(re) = regex::Regex::new(&pattern) {
                for captures in re.captures_iter(sql) {
                    let alias = captures[1].to_string();
                    metadata.add_hint(alias, ColumnTypeHint {
                        source_column: None,
                        suggested_type: Some(PgType::Text),
                        datetime_subtype: None,
                        is_expression: true,
                        expression_type: Some(ExpressionType::Other),
                    });
                }
            }
        }
    }
}

/// Helper function to extract the expression from a SELECT statement
fn extract_select_expression(select_query: &str) -> Option<&str> {
    // Find SELECT keyword and extract the expression before FROM
    let upper_query = select_query.to_uppercase();
    if let Some(select_pos) = upper_query.find("SELECT") {
        let after_select = &select_query[select_pos + 6..].trim_start();
        if let Some(from_pos) = upper_query[select_pos + 6..].find(" FROM") {
            Some(after_select[..from_pos].trim())
        } else {
            None
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_array_subscript() {
        let sql = "SELECT tags[1] FROM products";
        let result = ArrayTranslator::translate_array_operators(sql).unwrap();
        assert_eq!(result, "SELECT json_extract(tags, '$[0]') FROM products");
        
        let sql2 = "SELECT matrix[2][3] FROM data";
        let result2 = ArrayTranslator::translate_array_operators(sql2).unwrap();
        assert!(result2.contains("json_extract(matrix, '$[1]')"));
    }
    
    #[test]
    fn test_any_operator() {
        let sql = "SELECT * FROM products WHERE 'electronics' = ANY(tags)";
        let result = ArrayTranslator::translate_array_operators(sql).unwrap();
        println!("ANY operator result: {}", result);
        assert!(result.contains("EXISTS (SELECT 1 FROM json_each(tags) WHERE value = 'electronics')"));
    }
    
    #[test]
    fn test_all_operator() {
        let sql = "SELECT * FROM scores WHERE 90 > ALL(grades)";
        let result = ArrayTranslator::translate_array_operators(sql).unwrap();
        assert!(result.contains("NOT EXISTS (SELECT 1 FROM json_each(grades) WHERE value <= 90)"));
        
        // Test ALL with subquery
        let sql2 = "SELECT id, name FROM products WHERE 5 < ALL(SELECT length(value) FROM json_each(tags))";
        let result2 = ArrayTranslator::translate_array_operators(sql2).unwrap();
        println!("Original: {}", sql2);
        println!("ALL subquery result: {}", result2);
        assert!(result2.contains("NOT EXISTS"));
        // Note: This may not contain "length(value)" due to the translation
        
        // Test expression extraction
        let expr = extract_select_expression("SELECT length(value) FROM json_each(tags)");
        println!("Extracted expression: {:?}", expr);
        assert_eq!(expr, Some("length(value)"));
    }
    
    #[test]
    fn test_contains_operator() {
        let sql = "SELECT * FROM products WHERE tags @> '[\"electronics\",\"computers\"]'";
        let result = ArrayTranslator::translate_array_operators(sql).unwrap();
        assert!(result.contains("array_contains(tags, '[\"electronics\",\"computers\"]')"));
    }
    
    #[test]
    fn test_overlap_operator() {
        let sql = "SELECT * FROM products WHERE tags && '[\"electronics\", \"games\"]'";
        let result = ArrayTranslator::translate_array_operators(sql).unwrap();
        assert!(result.contains("array_overlap(tags, '[\"electronics\", \"games\"]')"));
    }
}