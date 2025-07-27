/// Translator that fixes PostgreSQL function syntax for SQLite
/// PostgreSQL allows functions without arguments to be called with or without parentheses
/// SQLite requires consistent syntax
pub struct FunctionParenthesesTranslator;

impl FunctionParenthesesTranslator {
    /// Check if translation is needed
    pub fn needs_translation(query: &str) -> bool {
        // Check for functions that might be called with empty parentheses
        query.contains("current_user()") || 
        query.contains("CURRENT_USER()") ||
        query.contains("session_user()") ||
        query.contains("SESSION_USER()")
    }
    
    /// Translate function calls with empty parentheses to no parentheses
    pub fn translate_query(query: &str) -> String {
        let mut result = query.to_string();
        
        // List of functions that should not have parentheses when called without arguments
        let functions = [
            ("current_user()", "current_user"),
            ("CURRENT_USER()", "CURRENT_USER"),
            ("session_user()", "session_user"),
            ("SESSION_USER()", "SESSION_USER"),
        ];
        
        for (from, to) in &functions {
            result = result.replace(from, to);
        }
        
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_current_user_translation() {
        let query = "SELECT current_user()";
        assert!(FunctionParenthesesTranslator::needs_translation(query));
        assert_eq!(
            FunctionParenthesesTranslator::translate_query(query),
            "SELECT current_user"
        );
    }
    
    #[test]
    fn test_uppercase_translation() {
        let query = "SELECT CURRENT_USER()";
        assert!(FunctionParenthesesTranslator::needs_translation(query));
        assert_eq!(
            FunctionParenthesesTranslator::translate_query(query),
            "SELECT CURRENT_USER"
        );
    }
    
    #[test]
    fn test_no_translation_needed() {
        let query = "SELECT version()";
        assert!(!FunctionParenthesesTranslator::needs_translation(query));
        assert_eq!(
            FunctionParenthesesTranslator::translate_query(query),
            query
        );
    }
}