use once_cell::sync::Lazy;
use regex::Regex;

pub struct VersionSelectTranslator;

static VERSION_SELECT_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?is)(^|;)\s*select\s+version\s*\(\s*\)\s*(?:as\s+([A-Za-z_][A-Za-z0-9_]*)\s*)?(;|$)"#,
    )
    .expect("regex compiles")
});

impl VersionSelectTranslator {
    pub fn needs_translation(query: &str) -> bool {
        VERSION_SELECT_PATTERN.is_match(query)
    }

    pub fn translate_query(query: &str) -> String {
        if !Self::needs_translation(query) {
            return query.to_string();
        }

        VERSION_SELECT_PATTERN
            .replace_all(query, |caps: &regex::Captures| {
                let prefix = caps.get(1).map(|m| m.as_str()).unwrap_or("");
                let alias = caps.get(2).map(|m| m.as_str()).unwrap_or("version");
                let suffix = caps.get(3).map(|m| m.as_str()).unwrap_or("");
                if prefix.is_empty() {
                    format!("SELECT version() AS {alias}{suffix}")
                } else {
                    format!("{prefix} SELECT version() AS {alias}{suffix}")
                }
            })
            .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::VersionSelectTranslator;

    #[test]
    fn rewrites_select_version() {
        let query = "SELECT version()";
        assert!(VersionSelectTranslator::needs_translation(query));
        assert_eq!(
            VersionSelectTranslator::translate_query(query),
            "SELECT version() AS version"
        );
    }

    #[test]
    fn preserves_semicolon() {
        let query = "select version();";
        assert_eq!(
            VersionSelectTranslator::translate_query(query),
            "SELECT version() AS version;"
        );
    }

    #[test]
    fn rewrites_when_not_first_statement() {
        let query = "set search_path=foo; select version();";
        assert_eq!(
            VersionSelectTranslator::translate_query(query),
            "set search_path=foo; SELECT version() AS version;"
        );
    }

    #[test]
    fn ignores_non_matching_query() {
        let query = "SELECT version(), now()";
        assert!(!VersionSelectTranslator::needs_translation(query));
        assert_eq!(VersionSelectTranslator::translate_query(query), query);
    }
}
