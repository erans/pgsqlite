use regex::Regex;
use tracing::debug;

/// Translator for queries containing pg_table_is_visible function
/// 
/// SQLAlchemy generates queries that use pg_table_is_visible to check if tables
/// are in the search path. In SQLite, all tables are visible, so we can
/// simplify these queries by removing the pg_table_is_visible condition.
pub struct PgTableIsVisibleTranslator;

impl PgTableIsVisibleTranslator {
    /// Translate queries containing pg_table_is_visible
    pub fn translate(query: &str) -> String {
        // Pattern to match pg_table_is_visible calls
        // Matches both pg_catalog.pg_table_is_visible and plain pg_table_is_visible
        let pattern = Regex::new(r"(?i)\s+AND\s+(?:pg_catalog\.)?pg_table_is_visible\s*\([^)]+\)").unwrap();
        
        if pattern.is_match(query) {
            let result = pattern.replace_all(query, "").to_string();
            debug!("Removed pg_table_is_visible condition from query");
            result
        } else {
            query.to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remove_pg_table_is_visible() {
        let query = "SELECT pg_class.relname FROM pg_class WHERE pg_class.relname = 'test' AND pg_table_is_visible(pg_class.oid)";
        let translated = PgTableIsVisibleTranslator::translate(query);
        assert_eq!(translated, "SELECT pg_class.relname FROM pg_class WHERE pg_class.relname = 'test'");
    }

    #[test]
    fn test_remove_pg_catalog_pg_table_is_visible() {
        let query = "SELECT * FROM pg_class WHERE relkind = 'r' AND pg_catalog.pg_table_is_visible(oid) AND relname = 'test'";
        let translated = PgTableIsVisibleTranslator::translate(query);
        assert_eq!(translated, "SELECT * FROM pg_class WHERE relkind = 'r' AND relname = 'test'");
    }

    #[test]
    fn test_complex_sqlalchemy_query() {
        let query = r"SELECT pg_catalog.pg_class.relname 
FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
WHERE pg_catalog.pg_class.relname = $1 AND pg_catalog.pg_class.relkind = ANY (ARRAY[$2, $3, $4, $5, $6]) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != $7";
        let translated = PgTableIsVisibleTranslator::translate(query);
        let expected = r"SELECT pg_catalog.pg_class.relname 
FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
WHERE pg_catalog.pg_class.relname = $1 AND pg_catalog.pg_class.relkind = ANY (ARRAY[$2, $3, $4, $5, $6]) AND pg_catalog.pg_namespace.nspname != $7";
        assert_eq!(translated, expected);
    }
}