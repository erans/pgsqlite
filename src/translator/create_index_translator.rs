use regex::Regex;
use once_cell::sync::Lazy;

/// Regex to match PostgreSQL CREATE INDEX with operator class syntax
static OPERATOR_CLASS_PATTERN: Lazy<Regex> = Lazy::new(|| {
    // Matches: column_name operator_class (e.g., "email" varchar_pattern_ops)
    // Group 1: column name (with quotes)
    // Group 2: operator class name
    Regex::new(r#"(?i)("[^"]+"|[a-zA-Z_][a-zA-Z0-9_]*)\s+([a-zA-Z_][a-zA-Z0-9_]*_ops)\b"#).unwrap()
});

/// Translates PostgreSQL CREATE INDEX statements with operator classes to SQLite
pub struct CreateIndexTranslator;

impl CreateIndexTranslator {
    /// Check if the query needs translation
    pub fn needs_translation(query: &str) -> bool {
        query.to_uppercase().contains("CREATE INDEX") && 
        OPERATOR_CLASS_PATTERN.is_match(query)
    }
    
    /// Translate CREATE INDEX statement with operator classes
    pub fn translate(query: &str) -> String {
        if !Self::needs_translation(query) {
            return query.to_string();
        }
        
        let mut result = query.to_string();
        
        // Replace operator class syntax with appropriate COLLATE clause
        result = OPERATOR_CLASS_PATTERN.replace_all(&result, |caps: &regex::Captures| {
            let column_name = caps.get(1).unwrap().as_str();
            let operator_class = caps.get(2).unwrap().as_str().to_lowercase();
            
            // Map PostgreSQL operator classes to SQLite COLLATE clauses
            match operator_class.as_str() {
                "varchar_pattern_ops" | "text_pattern_ops" | "bpchar_pattern_ops" => {
                    // Pattern ops use character-by-character comparison (like C locale)
                    format!("{} COLLATE BINARY", column_name)
                }
                "varchar_ops" | "text_ops" | "bpchar_ops" => {
                    // Regular ops use default collation
                    column_name.to_string()
                }
                _ => {
                    // Unknown operator class, remove it but keep column
                    column_name.to_string()
                }
            }
        }).to_string();
        
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_needs_translation() {
        assert!(CreateIndexTranslator::needs_translation(
            r#"CREATE INDEX "foo_user_email_c390bbc3_like" ON "foo_user" ("email" varchar_pattern_ops)"#
        ));
        
        assert!(CreateIndexTranslator::needs_translation(
            "CREATE INDEX idx_name ON table_name (column text_pattern_ops)"
        ));
        
        assert!(!CreateIndexTranslator::needs_translation(
            "CREATE INDEX idx_name ON table_name (column)"
        ));
        
        assert!(!CreateIndexTranslator::needs_translation(
            "SELECT * FROM table"
        ));
    }

    #[test]
    fn test_translate_varchar_pattern_ops() {
        let input = r#"CREATE INDEX "foo_user_email_c390bbc3_like" ON "foo_user" ("email" varchar_pattern_ops)"#;
        let expected = r#"CREATE INDEX "foo_user_email_c390bbc3_like" ON "foo_user" ("email" COLLATE BINARY)"#;
        
        assert_eq!(CreateIndexTranslator::translate(input), expected);
    }

    #[test]
    fn test_translate_text_pattern_ops() {
        let input = "CREATE INDEX idx_name ON table_name (name text_pattern_ops)";
        let expected = "CREATE INDEX idx_name ON table_name (name COLLATE BINARY)";
        
        assert_eq!(CreateIndexTranslator::translate(input), expected);
    }

    #[test]
    fn test_translate_bpchar_pattern_ops() {
        let input = "CREATE INDEX idx_code ON products (code bpchar_pattern_ops)";
        let expected = "CREATE INDEX idx_code ON products (code COLLATE BINARY)";
        
        assert_eq!(CreateIndexTranslator::translate(input), expected);
    }

    #[test]
    fn test_translate_regular_ops() {
        let input = "CREATE INDEX idx_name ON table_name (name varchar_ops)";
        let expected = "CREATE INDEX idx_name ON table_name (name)";
        
        assert_eq!(CreateIndexTranslator::translate(input), expected);
    }

    #[test]
    fn test_translate_unknown_ops() {
        let input = "CREATE INDEX idx_name ON table_name (name unknown_ops)";
        let expected = "CREATE INDEX idx_name ON table_name (name)";
        
        assert_eq!(CreateIndexTranslator::translate(input), expected);
    }

    #[test]
    fn test_translate_multiple_columns() {
        let input = r#"CREATE INDEX idx_multi ON users ("email" varchar_pattern_ops, "name" text_pattern_ops)"#;
        let expected = r#"CREATE INDEX idx_multi ON users ("email" COLLATE BINARY, "name" COLLATE BINARY)"#;
        
        assert_eq!(CreateIndexTranslator::translate(input), expected);
    }

    #[test]
    fn test_translate_no_operator_class() {
        let input = "CREATE INDEX idx_simple ON table_name (column)";
        let expected = "CREATE INDEX idx_simple ON table_name (column)";
        
        assert_eq!(CreateIndexTranslator::translate(input), expected);
    }

    #[test]
    fn test_case_insensitive_matching() {
        let input = "create index idx_name on table_name (name VARCHAR_PATTERN_OPS)";
        let expected = "create index idx_name on table_name (name COLLATE BINARY)";
        
        assert_eq!(CreateIndexTranslator::translate(input), expected);
    }
}