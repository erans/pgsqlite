use once_cell::sync::Lazy;
use regex::Regex;

pub struct CurrentSchemaFromTranslator;

static CURRENT_SCHEMA_FROM_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?is)^\s*select\s+\*\s+from\s+current_schema\s*\(\s*\)\s*(?:as\s+([A-Za-z_][A-Za-z0-9_]*)\s*)?(?:;)?\s*$"#,
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

        let trimmed = query.trim_end();
        let has_semicolon = trimmed.ends_with(';');
        let suffix = if has_semicolon { ";" } else { "" };
        let alias = CURRENT_SCHEMA_FROM_PATTERN
            .captures(query)
            .and_then(|caps| caps.get(1))
            .map(|m| m.as_str())
            .unwrap_or("current_schema");

        format!("SELECT current_schema() AS {alias}{suffix}")
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
    fn ignores_non_matching_query() {
        let query = "SELECT current_schema()";
        assert!(!CurrentSchemaFromTranslator::needs_translation(query));
        assert_eq!(CurrentSchemaFromTranslator::translate_query(query), query);
    }
}
