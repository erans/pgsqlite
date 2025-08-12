use sqlparser::ast::{
    Expr, BinaryOperator
};
use rusqlite::Connection;
use crate::metadata::EnumMetadata;
use std::collections::HashMap;

/// Rewrites queries to properly handle ENUM type comparisons
pub struct EnumQueryRewriter<'a> {
    conn: &'a Connection,
    enum_types_cache: HashMap<String, bool>,
}

impl<'a> EnumQueryRewriter<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self {
            conn,
            enum_types_cache: HashMap::new(),
        }
    }
    
    /// Check if a column is an ENUM type
    fn is_enum_column(&mut self, table: &str, column: &str) -> bool {
        // First check if __pgsqlite_schema exists
        let schema_exists = self.conn.query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='__pgsqlite_schema'",
            [],
            |_| Ok(())
        ).is_ok();
        
        if !schema_exists {
            return false;
        }
        
        // Check if this column is stored with a known ENUM type
        let query = "SELECT pg_type FROM __pgsqlite_schema 
                     WHERE table_name = ?1 AND column_name = ?2";
        
        match self.conn.query_row(query, [table, column], |row| {
            row.get::<_, String>(0)
        }) {
            Ok(pg_type) => {
                // Check if this type is an ENUM
                let cache_key = pg_type.clone();
                if let Some(&is_enum) = self.enum_types_cache.get(&cache_key) {
                    return is_enum;
                }
                
                // Check if this type exists in our ENUM metadata
                let is_enum = EnumMetadata::get_enum_type(self.conn, &pg_type)
                    .unwrap_or(None)
                    .is_some();
                
                self.enum_types_cache.insert(cache_key, is_enum);
                is_enum
            }
            Err(_) => false,
        }
    }
    
    /// Rewrite expressions that involve ENUM comparisons
    pub fn rewrite_expression(&mut self, expr: &mut Expr, context: &QueryContext) -> Result<(), String> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // First recursively rewrite sub-expressions
                self.rewrite_expression(left, context)?;
                self.rewrite_expression(right, context)?;
                
                // Check if this is a comparison involving an ENUM column
                if matches!(op, BinaryOperator::Eq | BinaryOperator::NotEq | 
                           BinaryOperator::Lt | BinaryOperator::Gt | 
                           BinaryOperator::LtEq | BinaryOperator::GtEq) {
                    
                    // Check if left side is an ENUM column
                    if let Expr::Identifier(ident) = &**left {
                        let column_name = ident.value.clone();
                        
                        // Try to find the table for this column
                        if let Some(table) = context.find_table_for_column(&column_name)
                            && self.is_enum_column(&table, &column_name) {
                                // For ordering comparisons, we need to use the enum sort order
                                if matches!(op, BinaryOperator::Lt | BinaryOperator::Gt | 
                                           BinaryOperator::LtEq | BinaryOperator::GtEq) {
                                    return self.rewrite_enum_ordering_comparison(expr, &table, &column_name);
                                }
                            }
                    }
                    
                    // Check if left side is a compound identifier (table.column)
                    if let Expr::CompoundIdentifier(parts) = &**left
                        && parts.len() == 2 {
                            let table = parts[0].value.clone();
                            let column = parts[1].value.clone();
                            
                            if self.is_enum_column(&table, &column) {
                                // For ordering comparisons, we need to use the enum sort order
                                if matches!(op, BinaryOperator::Lt | BinaryOperator::Gt | 
                                           BinaryOperator::LtEq | BinaryOperator::GtEq) {
                                    return self.rewrite_enum_ordering_comparison(expr, &table, &column);
                                }
                            }
                        }
                }
            }
            
            Expr::InList { expr: inner_expr, list, negated: _ } => {
                // Recursively rewrite the inner expression
                self.rewrite_expression(inner_expr, context)?;
                
                // Rewrite each expression in the list
                for item in list.iter_mut() {
                    self.rewrite_expression(item, context)?;
                }
            }
            
            Expr::InSubquery { expr: inner_expr, subquery: _, negated: _ } => {
                // Recursively rewrite the inner expression
                self.rewrite_expression(inner_expr, context)?;
                // Note: Subquery rewriting would be handled at a higher level
            }
            
            Expr::Case { .. } => {
                // Case expressions are complex and would require deep AST traversal
                // For now, we don't rewrite case expressions with ENUMs
            }
            
            // Other expression types - recursively process
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                self.rewrite_expression(inner, context)?;
            }
            
            Expr::Between { expr: inner_expr, negated: _, low, high } => {
                self.rewrite_expression(inner_expr, context)?;
                self.rewrite_expression(low, context)?;
                self.rewrite_expression(high, context)?;
            }
            
            Expr::Nested(inner) => {
                self.rewrite_expression(inner, context)?;
            }
            
            _ => {
                // Other expression types don't need rewriting
            }
        }
        
        Ok(())
    }
    
    /// Rewrite ENUM ordering comparisons to use sort order
    fn rewrite_enum_ordering_comparison(
        &self, 
        _expr: &mut Expr, 
        _table: &str, 
        _column: &str
    ) -> Result<(), String> {
        // For ENUM ordering comparisons, we need to join with the enum values table
        // to get the sort order. However, this is complex to do in the AST.
        // For now, we'll document this as a limitation.
        
        // In a full implementation, we would:
        // 1. Get the enum type name from the schema
        // 2. Rewrite the comparison to use a subquery that joins with __pgsqlite_enum_values
        // 3. Compare based on sort_order instead of the text value
        
        // For now, ENUMs will use text comparison which may not match PostgreSQL ordering
        Ok(())
    }
}

/// Context for query rewriting (reused from decimal_rewriter)
pub use super::expression_type_resolver::QueryContext;