use once_cell::sync::Lazy;
use regex::Regex;

pub struct CurrentSchemaFromTranslator;

static CURRENT_SCHEMA_FROM_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?is)(^|;)\s*select\s+\*\s+from\s+current_schema\s*\(\s*\)\s*(?:as\s+([A-Za-z_][A-Za-z0-9_]*)\s*)?(;|$)"#,
    )
    .expect("regex compiles")
});

impl CurrentSchemaFromTranslator {
    pub fn needs_translation(query: &str) -> bool {
        CURRENT_SCHEMA_FROM_PATTERN.is_match(query)
    }

    pub fn translate_query(query: &str) -> String {
        if !Self::needs_translation(query) {
            return query.to_string();
        }

        CURRENT_SCHEMA_FROM_PATTERN
            .replace_all(query, |caps: &regex::Captures| {
                let prefix = caps.get(1).map(|m| m.as_str()).unwrap_or("");
                let alias = caps.get(2).map(|m| m.as_str()).unwrap_or("current_schema");
                let suffix = caps.get(3).map(|m| m.as_str()).unwrap_or("");
                if prefix.is_empty() {
                    format!("SELECT current_schema() AS {alias}{suffix}")
                } else {
                    format!("{prefix} SELECT current_schema() AS {alias}{suffix}")
                }
            })
            .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::CurrentSchemaFromTranslator;

    #[test]
    fn rewrites_select_star_from_current_schema() {
        let query = "SELECT * FROM current_schema()";
        assert!(CurrentSchemaFromTranslator::needs_translation(query));
        assert_eq!(
            CurrentSchemaFromTranslator::translate_query(query),
            "SELECT current_schema() AS current_schema"
        );
    }

    #[test]
    fn preserves_semicolon() {
        let query = "select * from current_schema();";
        assert_eq!(
            CurrentSchemaFromTranslator::translate_query(query),
            "SELECT current_schema() AS current_schema;"
        );
    }

    #[test]
    fn rewrites_when_not_first_statement() {
        let query = "set search_path=foo; select * from current_schema();";
        assert_eq!(
            CurrentSchemaFromTranslator::translate_query(query),
            "set search_path=foo; SELECT current_schema() AS current_schema;"
        );
    }

    #[test]
    fn ignores_non_matching_query() {
        let query = "SELECT current_schema()";
        assert!(!CurrentSchemaFromTranslator::needs_translation(query));
        assert_eq!(CurrentSchemaFromTranslator::translate_query(query), query);
    }
}
