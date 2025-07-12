use crate::PgSqliteError;
use regex::Regex;
use once_cell::sync::Lazy;

/// Regex patterns for array operators
static ARRAY_CONTAINS_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(\b\w+(?:\.\w+)*)\s*@>\s*('[^']+'|"[^"]+"|'\[[^\]]+\]')"#).unwrap()
});

static ARRAY_CONTAINED_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"('[^']+'|"[^"]+"|'\[[^\]]+\]')\s*<@\s*(\b\w+(?:\.\w+)*)"#).unwrap()
});

static ARRAY_OVERLAP_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(\b\w+(?:\.\w+)*)\s*&&\s*(\b\w+(?:\.\w+)*|'[^']+'|"[^"]+"|'\[[^\]]+\]')"#).unwrap()
});

static ARRAY_CONCAT_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(\b\w+(?:\.\w+)*)\s*\|\|\s*('[^']+'|"[^"]+"|'\[[^\]]+\]')"#).unwrap()
});

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
    Regex::new(r"(\b\w+(?:\.\w+)*|\d+)\s*([><=!]+)\s*ALL\s*\((\b\w+(?:\.\w+)*)\)").unwrap()
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
        // TODO: || operator conflicts with string concatenation
        // Need smarter detection to differentiate array concat from string concat
        // result = Self::translate_concat_operator(&result)?;
        
        Ok(result)
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
            let array_col = &captures[3];
            
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
            
            let replacement = format!(
                "NOT EXISTS (SELECT 1 FROM json_each({}) WHERE value {} {})",
                array_col, inverted_op, value
            );
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
        let mut result = sql.to_string();
        
        while let Some(captures) = ARRAY_CONCAT_REGEX.captures(&result) {
            let array1 = &captures[1];
            let array2 = captures[2].trim();
            
            let replacement = format!("array_cat({}, {})", array1, array2);
            result = result.replace(&captures[0], &replacement);
        }
        
        Ok(result)
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