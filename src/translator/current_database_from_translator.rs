use once_cell::sync::Lazy;
use regex::Regex;

pub struct CurrentDatabaseFromTranslator;

static CURRENT_DATABASE_FROM_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?is)(^|;)\s*select\s+\*\s+from\s+current_database\s*\(\s*\)\s*(?:as\s+([A-Za-z_][A-Za-z0-9_]*)\s*)?(;|$)"#,
    )
    .expect("regex compiles")
});

impl CurrentDatabaseFromTranslator {
    pub fn needs_translation(query: &str) -> bool {
        CURRENT_DATABASE_FROM_PATTERN.is_match(query)
    }

    pub fn translate_query(query: &str) -> String {
        if !Self::needs_translation(query) {
            return query.to_string();
        }

        CURRENT_DATABASE_FROM_PATTERN
            .replace_all(query, |caps: &regex::Captures| {
                let prefix = caps.get(1).map(|m| m.as_str()).unwrap_or("");
                let alias = caps
                    .get(2)
                    .map(|m| m.as_str())
                    .unwrap_or("current_database");
                let suffix = caps.get(3).map(|m| m.as_str()).unwrap_or("");
                if prefix.is_empty() {
                    format!("SELECT current_database() AS {alias}{suffix}")
                } else {
                    format!("{prefix} SELECT current_database() AS {alias}{suffix}")
                }
            })
            .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::CurrentDatabaseFromTranslator;

    #[test]
    fn rewrites_select_star_from_current_database() {
        let query = "SELECT * FROM current_database()";
        assert!(CurrentDatabaseFromTranslator::needs_translation(query));
        assert_eq!(
            CurrentDatabaseFromTranslator::translate_query(query),
            "SELECT current_database() AS current_database"
        );
    }

    #[test]
    fn preserves_semicolon() {
        let query = "select * from current_database();";
        assert_eq!(
            CurrentDatabaseFromTranslator::translate_query(query),
            "SELECT current_database() AS current_database;"
        );
    }

    #[test]
    fn rewrites_when_not_first_statement() {
        let query = "set search_path=foo; select * from current_database();";
        assert_eq!(
            CurrentDatabaseFromTranslator::translate_query(query),
            "set search_path=foo; SELECT current_database() AS current_database;"
        );
    }

    #[test]
    fn ignores_non_matching_query() {
        let query = "SELECT current_database()";
        assert!(!CurrentDatabaseFromTranslator::needs_translation(query));
        assert_eq!(CurrentDatabaseFromTranslator::translate_query(query), query);
    }
}
