use tracing::debug;

/// Translator for PostgreSQL catalog functions
/// Removes pg_catalog schema prefix from function calls since SQLite doesn't support schema-qualified functions
pub struct CatalogFunctionTranslator;

impl CatalogFunctionTranslator {
    /// Translate catalog function calls by removing pg_catalog prefix
    pub fn translate(query: &str) -> String {
        // Simple text replacement for common patterns
        let mut result = query.to_string();
        
        // Replace pg_catalog.pg_table_is_visible with pg_table_is_visible
        result = result.replace("pg_catalog.pg_table_is_visible", "pg_table_is_visible");
        
        // Replace other common catalog functions
        result = result.replace("pg_catalog.format_type", "format_type");
        result = result.replace("pg_catalog.pg_get_expr", "pg_get_expr");
        result = result.replace("pg_catalog.pg_get_constraintdef", "pg_get_constraintdef");
        result = result.replace("pg_catalog.pg_get_userbyid", "pg_get_userbyid");
        result = result.replace("pg_catalog.to_regtype", "to_regtype");
        
        if result != query {
            debug!("Translated catalog functions in query");
        }
        
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_catalog_function_translation() {
        let query = "SELECT * FROM pg_class WHERE pg_catalog.pg_table_is_visible(oid)";
        let translated = CatalogFunctionTranslator::translate(query);
        assert_eq!(translated, "SELECT * FROM pg_class WHERE pg_table_is_visible(oid)");
    }
    
    #[test]
    fn test_multiple_functions() {
        let query = "SELECT pg_catalog.format_type(t.oid, NULL), pg_catalog.pg_table_is_visible(c.oid)";
        let translated = CatalogFunctionTranslator::translate(query);
        assert_eq!(translated, "SELECT format_type(t.oid, NULL), pg_table_is_visible(c.oid)");
    }
}