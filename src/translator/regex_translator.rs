use sqlparser::ast::{Expr, BinaryOperator, UnaryOperator, Function, FunctionArg, FunctionArgExpr, ObjectName, ObjectNamePart, Ident, Statement, Query, SetExpr, Select};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use crate::PgSqliteError;
use tracing::{debug, trace};

/// Translates PostgreSQL regex operators (~, !~, ~*, !~*) to SQLite-compatible REGEXP function calls
pub struct RegexTranslator;

impl RegexTranslator {
    /// Translate a query containing PostgreSQL regex operators to SQLite-compatible syntax
    pub fn translate_query(query: &str) -> Result<String, PgSqliteError> {
        // Quick check to avoid parsing if no regex operators are present
        if !Self::contains_regex_operators(query) {
            return Ok(query.to_string());
        }

        debug!("Translating regex operators in query: {}", query);
        
        // Parse the SQL query
        let dialect = PostgreSqlDialect {};
        let mut statements = Parser::parse_sql(&dialect, query)?;
        
        // Translate each statement
        for statement in &mut statements {
            Self::translate_statement(statement)?;
        }
        
        // Convert back to SQL string
        let result = statements.iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join("; ");
            
        debug!("Translated query: {}", result);
        Ok(result)
    }
    
    /// Quick check if query contains regex operators
    fn contains_regex_operators(query: &str) -> bool {
        // Look for regex operators with word boundaries
        query.contains(" ~ ") || 
        query.contains(" !~ ") || 
        query.contains(" ~* ") || 
        query.contains(" !~* ") ||
        // Also check for operators at line boundaries
        query.contains("\n~") || 
        query.contains("\n!~") ||
        // Check for OPERATOR syntax
        query.contains("OPERATOR(") && query.contains("~")
    }
    
    /// Translate a statement
    fn translate_statement(statement: &mut Statement) -> Result<(), PgSqliteError> {
        match statement {
            Statement::Query(query) => Self::translate_query_box(query)?,
            // For now, just handle SELECT queries which are the main use case for catalog queries
            _ => {}
        }
        Ok(())
    }
    
    /// Translate a boxed query
    fn translate_query_box(query: &mut Box<Query>) -> Result<(), PgSqliteError> {
        if let SetExpr::Select(select) = &mut *query.body {
            Self::translate_select(select)?;
        }
        Ok(())
    }
    
    /// Translate a SELECT statement
    fn translate_select(select: &mut Box<Select>) -> Result<(), PgSqliteError> {
        // Translate WHERE clause
        if let Some(selection) = &mut select.selection {
            Self::translate_expression(selection)?;
        }
        
        // Translate projections
        for projection in &mut select.projection {
            match projection {
                sqlparser::ast::SelectItem::UnnamedExpr(expr) |
                sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                    Self::translate_expression(expr)?;
                }
                _ => {}
            }
        }
        
        // Translate HAVING clause
        if let Some(having) = &mut select.having {
            Self::translate_expression(having)?;
        }
        
        Ok(())
    }
    
    /// Translate an expression, converting regex operators to REGEXP function calls
    fn translate_expression(expr: &mut Expr) -> Result<(), PgSqliteError> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    // Handle standard regex operators
                    BinaryOperator::PGRegexMatch => {
                        trace!("Translating ~ operator");
                        *expr = Self::create_regexp_function(left.as_ref().clone(), right.as_ref().clone(), false);
                    }
                    BinaryOperator::PGRegexNotMatch => {
                        trace!("Translating !~ operator");
                        *expr = Self::create_regexp_function(left.as_ref().clone(), right.as_ref().clone(), true);
                    }
                    BinaryOperator::PGRegexIMatch => {
                        trace!("Translating ~* operator");
                        *expr = Self::create_regexpi_function(left.as_ref().clone(), right.as_ref().clone(), false);
                    }
                    BinaryOperator::PGRegexNotIMatch => {
                        trace!("Translating !~* operator");
                        *expr = Self::create_regexpi_function(left.as_ref().clone(), right.as_ref().clone(), true);
                    }
                    _ => {
                        // Recursively process other operators
                        Self::translate_expression(left)?;
                        Self::translate_expression(right)?;
                    }
                }
            }
            // Handle OPERATOR(pg_catalog.~) syntax
            Expr::Function(func) if func.name.to_string().contains("OPERATOR") => {
                if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                    if arg_list.args.len() >= 2 {
                        // Check if this is a regex operator
                        let func_str = func.name.to_string();
                        if func_str.contains("~") && !func_str.contains("!~") {
                            trace!("Translating OPERATOR(pg_catalog.~) syntax");
                            let pattern = Self::extract_function_arg(&arg_list.args[1]);
                            let text = Self::extract_function_arg(&arg_list.args[0]);
                            *expr = Self::create_regexp_function(text, pattern, false);
                        }
                    }
                }
            }
            // Recursively process nested expressions
            Expr::Nested(nested) => Self::translate_expression(nested)?,
            Expr::UnaryOp { expr: inner, .. } => Self::translate_expression(inner)?,
            Expr::Cast { expr: inner, .. } => Self::translate_expression(inner)?,
            Expr::Case { operand, conditions, else_result, .. } => {
                if let Some(op) = operand {
                    Self::translate_expression(op)?;
                }
                for condition in conditions {
                    Self::translate_expression(&mut condition.condition)?;
                    Self::translate_expression(&mut condition.result)?;
                }
                if let Some(else_expr) = else_result {
                    Self::translate_expression(else_expr)?;
                }
            }
            Expr::InList { expr: inner, list, .. } => {
                Self::translate_expression(inner)?;
                for item in list {
                    Self::translate_expression(item)?;
                }
            }
            Expr::InSubquery { expr: inner, .. } => Self::translate_expression(inner)?,
            Expr::Between { expr: inner, low, high, .. } => {
                Self::translate_expression(inner)?;
                Self::translate_expression(low)?;
                Self::translate_expression(high)?;
            }
            _ => {}
        }
        Ok(())
    }
    
    /// Extract expression from function argument
    fn extract_function_arg(arg: &FunctionArg) -> Expr {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) |
            FunctionArg::Named { arg: FunctionArgExpr::Expr(e), .. } => e.clone(),
            _ => Expr::Value(sqlparser::ast::Value::Null.into()),
        }
    }
    
    /// Create a REGEXP function call
    fn create_regexp_function(text: Expr, pattern: Expr, negate: bool) -> Expr {
        let regexp_call = Expr::Function(Function {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("regexp"))]),
            args: sqlparser::ast::FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
                args: vec![
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(pattern)),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(text)),
                ],
                duplicate_treatment: None,
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: sqlparser::ast::FunctionArguments::None,
            uses_odbc_syntax: false,
        });
        
        if negate {
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(regexp_call),
            }
        } else {
            regexp_call
        }
    }
    
    /// Create a case-insensitive REGEXP function call
    fn create_regexpi_function(text: Expr, pattern: Expr, negate: bool) -> Expr {
        let regexpi_call = Expr::Function(Function {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("regexpi"))]),
            args: sqlparser::ast::FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
                args: vec![
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(pattern)),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(text)),
                ],
                duplicate_treatment: None,
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: sqlparser::ast::FunctionArguments::None,
            uses_odbc_syntax: false,
        });
        
        if negate {
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(regexpi_call),
            }
        } else {
            regexpi_call
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_regex_match_operator() {
        let query = "SELECT * FROM users WHERE email ~ '@gmail\\.com$'";
        let result = RegexTranslator::translate_query(query).unwrap();
        assert!(result.contains("regexp"));
        assert!(result.contains("@gmail\\.com$"));
    }
    
    #[test]
    fn test_regex_not_match_operator() {
        let query = "SELECT * FROM pg_namespace WHERE nspname !~ '^pg_'";
        let result = RegexTranslator::translate_query(query).unwrap();
        assert!(result.contains("NOT"));
        assert!(result.contains("regexp"));
        assert!(result.contains("^pg_"));
    }
    
    #[test]
    fn test_case_insensitive_match() {
        let query = "SELECT * FROM products WHERE name ~* 'laptop'";
        let result = RegexTranslator::translate_query(query).unwrap();
        assert!(result.contains("regexpi"));
        assert!(result.contains("laptop"));
    }
    
    #[test]
    fn test_complex_where_clause() {
        let query = "SELECT * FROM test WHERE col1 ~ 'pattern1' AND col2 !~ 'pattern2'";
        let result = RegexTranslator::translate_query(query).unwrap();
        assert!(result.contains("regexp"));
        assert!(result.contains("NOT"));
        assert!(result.contains("pattern1"));
        assert!(result.contains("pattern2"));
    }
    
    #[test]
    fn test_no_regex_operators() {
        let query = "SELECT * FROM users WHERE id = 1";
        let result = RegexTranslator::translate_query(query).unwrap();
        assert_eq!(result, query);
    }
}