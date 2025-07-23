use rusqlite::Connection;
use regex::Regex;
use once_cell::sync::Lazy;

/// Translator for CAST operations to NUMERIC types with constraints
pub struct NumericCastTranslator;

static CAST_TO_NUMERIC_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)CAST\s*\(\s*([^)]+?)\s*AS\s*(?:NUMERIC|DECIMAL)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\s*\)").unwrap()
});

impl NumericCastTranslator {
    /// Check if the query needs numeric cast translation
    pub fn needs_translation(query: &str) -> bool {
        CAST_TO_NUMERIC_REGEX.is_match(query)
    }
    
    /// Translate CAST(expr AS NUMERIC(p,s)) to numeric_cast(expr, p, s)
    pub fn translate_query(query: &str, _conn: &Connection) -> String {
        let mut result = query.to_string();
        
        // Replace all CAST(expr AS NUMERIC(p,s)) with numeric_cast(expr, p, s)
        while let Some(captures) = CAST_TO_NUMERIC_REGEX.captures(&result) {
            let full_match = captures.get(0).unwrap();
            let expr = captures.get(1).unwrap().as_str().trim();
            let precision = captures.get(2).unwrap().as_str();
            let scale = captures.get(3).unwrap().as_str();
            
            tracing::info!("Translating CAST({} AS NUMERIC({},{})) to numeric_cast", expr, precision, scale);
            let replacement = format!("numeric_cast({expr}, {precision}, {scale})");
            result = result.replace(full_match.as_str(), &replacement);
        }
        
        if result != query {
            tracing::info!("NumericCastTranslator: {} -> {}", query, result);
        }
        
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_cast_to_numeric_detection() {
        assert!(NumericCastTranslator::needs_translation("SELECT CAST(text_val AS NUMERIC(10,2))"));
        assert!(NumericCastTranslator::needs_translation("SELECT CAST(123 AS DECIMAL(5,3))"));
        assert!(!NumericCastTranslator::needs_translation("SELECT CAST(val AS TEXT)"));
        assert!(!NumericCastTranslator::needs_translation("SELECT CAST(val AS NUMERIC)"));
    }
    
    #[test]
    fn test_cast_translation() {
        let conn = Connection::open_in_memory().unwrap();
        
        let query = "SELECT CAST(text_val AS NUMERIC(10,2)) FROM test";
        let translated = NumericCastTranslator::translate_query(query, &conn);
        assert_eq!(translated, "SELECT numeric_cast(text_val, 10, 2) FROM test");
        
        let query = "INSERT INTO t SELECT id, CAST(amount AS DECIMAL(5,2)) FROM s";
        let translated = NumericCastTranslator::translate_query(query, &conn);
        assert_eq!(translated, "INSERT INTO t SELECT id, numeric_cast(amount, 5, 2) FROM s");
    }
}