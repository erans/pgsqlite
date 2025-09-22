/// Translator for PostgreSQL session identifiers that need to be converted to function calls
///
/// PostgreSQL supports special identifiers like `current_user`, `session_user` without parentheses
/// These need to be translated to function calls (`current_user()`, `session_user()`) for SQLite
pub struct SessionIdentifierTranslator;

impl SessionIdentifierTranslator {
    pub fn needs_translation(query: &str) -> bool {
        // Check for session identifiers that need function call conversion
        // Use word boundaries to avoid false positives (e.g., "current_user_id")
        let patterns = [
            (r"\bcurrent_user\b", r"\bcurrent_user\s*\("),
            (r"\bCURRENT_USER\b", r"\bCURRENT_USER\s*\("),
            (r"\bsession_user\b", r"\bsession_user\s*\("),
            (r"\bSESSION_USER\b", r"\bSESSION_USER\s*\("),
        ];

        for (pattern, func_pattern) in &patterns {
            if regex::Regex::new(pattern).unwrap().is_match(query) {
                // Make sure it's not already a function call (with parentheses)
                if !regex::Regex::new(func_pattern).unwrap().is_match(query) {
                    return true;
                }
            }
        }
        false
    }

    pub fn translate_query(query: &str) -> String {
        let mut result = query.to_string();

        // List of session identifiers that should become function calls
        let identifiers = [
            (r"\bcurrent_user\b", "current_user()"),
            (r"\bCURRENT_USER\b", "CURRENT_USER()"),
            (r"\bsession_user\b", "session_user()"),
            (r"\bSESSION_USER\b", "SESSION_USER()"),
        ];

        for (pattern, replacement) in &identifiers {
            // Check if the identifier is not already a function call
            let regex = regex::Regex::new(pattern).unwrap();
            let paren_check = regex::Regex::new(&format!("{}\\s*\\(", pattern)).unwrap();

            // Only replace if not already followed by parentheses
            if !paren_check.is_match(&result) {
                result = regex.replace_all(&result, *replacement).to_string();
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_current_user_translation() {
        let query = "SELECT current_user";
        assert!(SessionIdentifierTranslator::needs_translation(query));
        assert_eq!(
            SessionIdentifierTranslator::translate_query(query),
            "SELECT current_user()"
        );
    }

    #[test]
    fn test_current_user_uppercase_translation() {
        let query = "SELECT CURRENT_USER";
        assert!(SessionIdentifierTranslator::needs_translation(query));
        assert_eq!(
            SessionIdentifierTranslator::translate_query(query),
            "SELECT CURRENT_USER()"
        );
    }

    #[test]
    fn test_session_user_translation() {
        let query = "SELECT session_user";
        assert!(SessionIdentifierTranslator::needs_translation(query));
        assert_eq!(
            SessionIdentifierTranslator::translate_query(query),
            "SELECT session_user()"
        );
    }

    #[test]
    fn test_multiple_identifiers() {
        let query = "SELECT current_user, session_user, current_database()";
        assert!(SessionIdentifierTranslator::needs_translation(query));
        assert_eq!(
            SessionIdentifierTranslator::translate_query(query),
            "SELECT current_user(), session_user(), current_database()"
        );
    }

    #[test]
    fn test_already_function_call_not_translated() {
        let query = "SELECT current_user()";
        assert!(!SessionIdentifierTranslator::needs_translation(query));
        assert_eq!(
            SessionIdentifierTranslator::translate_query(query),
            "SELECT current_user()"
        );
    }

    #[test]
    fn test_function_call_with_spaces() {
        let query = "SELECT current_user  ()";
        assert!(!SessionIdentifierTranslator::needs_translation(query));
        assert_eq!(
            SessionIdentifierTranslator::translate_query(query),
            "SELECT current_user  ()"
        );
    }

    #[test]
    fn test_similar_names_not_affected() {
        let query = "SELECT current_user_id, session_user_name";
        assert!(!SessionIdentifierTranslator::needs_translation(query));
        assert_eq!(
            SessionIdentifierTranslator::translate_query(query),
            "SELECT current_user_id, session_user_name"
        );
    }

    #[test]
    fn test_complex_query_with_where_clause() {
        let query = "SELECT * FROM audit_log WHERE user_name = current_user AND db_name = current_database()";
        assert!(SessionIdentifierTranslator::needs_translation(query));
        assert_eq!(
            SessionIdentifierTranslator::translate_query(query),
            "SELECT * FROM audit_log WHERE user_name = current_user() AND db_name = current_database()"
        );
    }

    #[test]
    fn test_insert_values_with_session_identifiers() {
        let query = "INSERT INTO test (user_col, session_col) VALUES (current_user, session_user)";
        assert!(SessionIdentifierTranslator::needs_translation(query));
        assert_eq!(
            SessionIdentifierTranslator::translate_query(query),
            "INSERT INTO test (user_col, session_col) VALUES (current_user(), session_user())"
        );
    }

    #[test]
    fn test_no_translation_needed() {
        let query = "SELECT id FROM users WHERE name = 'test'";
        assert!(!SessionIdentifierTranslator::needs_translation(query));
        assert_eq!(
            SessionIdentifierTranslator::translate_query(query),
            "SELECT id FROM users WHERE name = 'test'"
        );
    }
}