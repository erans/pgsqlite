use sqlparser::ast::{Statement, Query, SetExpr, TableFactor, ObjectName, ObjectNamePart};
use tracing::debug;

/// Translator that removes schema prefixes from table names
/// PostgreSQL queries often use schema.table syntax (e.g., pg_catalog.pg_class)
/// but SQLite doesn't support schemas, so we need to strip the prefix
pub struct SchemaPrefixTranslator;

impl SchemaPrefixTranslator {
    /// Translate a query string by removing schema prefixes
    pub fn translate_query(query: &str) -> String {
        // Simple string replacement approach for known pg_catalog tables
        let mut result = query.to_string();
        
        // List of known pg_catalog tables that we have views for
        let catalog_tables = [
            "pg_class", "pg_namespace", "pg_attribute", "pg_type", 
            "pg_constraint", "pg_index", "pg_attrdef", "pg_am",
            "pg_enum", "pg_range"
        ];
        
        for table in &catalog_tables {
            // Replace pg_catalog.table with just table
            result = result.replace(&format!("pg_catalog.{table}"), table);
            // Also handle uppercase
            result = result.replace(&format!("PG_CATALOG.{}", table.to_uppercase()), table);
        }
        
        // Also remove schema prefix from functions
        let catalog_functions = [
            "pg_table_is_visible", "pg_get_userbyid", "pg_get_constraintdef",
            "format_type", "pg_get_expr", "pg_get_indexdef"
        ];
        
        for func in &catalog_functions {
            result = result.replace(&format!("pg_catalog.{func}"), func);
            result = result.replace(&format!("PG_CATALOG.{}", func.to_uppercase()), func);
        }
        
        debug!("Schema prefix translation: {} -> {}", query, result);
        result
    }
    
    /// Translate an AST by removing schema prefixes
    pub fn translate_statement(stmt: &mut Statement) -> Result<(), sqlparser::parser::ParserError> {
        match stmt {
            Statement::Query(query) => Self::translate_query_ast(query),
            _ => Ok(()),
        }
    }
    
    fn translate_query_ast(query: &mut Query) -> Result<(), sqlparser::parser::ParserError> {
        if let SetExpr::Select(select) = &mut *query.body {
            // Translate table names in FROM clause
            for table_ref in &mut select.from {
                Self::translate_table_factor(&mut table_ref.relation)?;
                
                // Also handle JOINs
                for join in &mut table_ref.joins {
                    Self::translate_table_factor(&mut join.relation)?;
                }
            }
        }
        Ok(())
    }
    
    fn translate_table_factor(factor: &mut TableFactor) -> Result<(), sqlparser::parser::ParserError> {
        if let TableFactor::Table { name, .. } = factor {
            Self::translate_object_name(name);
        }
        Ok(())
    }
    
    fn translate_object_name(name: &mut ObjectName) {
        // If the name has 2 parts (schema.table), remove the schema part
        if name.0.len() == 2 {
            let schema = &name.0[0];
            let table = &name.0[1];
            
            // Check if it's a pg_catalog schema
            let schema_name = match schema {
                ObjectNamePart::Identifier(ident) => ident.value.to_lowercase(),
            };
            
            if schema_name == "pg_catalog" {
                // Replace with just the table name
                name.0 = vec![table.clone()];
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_schema_prefix_removal() {
        let query = "SELECT * FROM pg_catalog.pg_class WHERE relname = 'test'";
        let translated = SchemaPrefixTranslator::translate_query(query);
        assert_eq!(translated, "SELECT * FROM pg_class WHERE relname = 'test'");
    }
    
    #[test]
    fn test_function_prefix_removal() {
        let query = "SELECT pg_catalog.pg_table_is_visible(oid) FROM pg_catalog.pg_class";
        let translated = SchemaPrefixTranslator::translate_query(query);
        assert_eq!(translated, "SELECT pg_table_is_visible(oid) FROM pg_class");
    }
    
    #[test]
    fn test_join_prefix_removal() {
        let query = "SELECT * FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid";
        let translated = SchemaPrefixTranslator::translate_query(query);
        assert_eq!(translated, "SELECT * FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid");
    }
}