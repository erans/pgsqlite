use sqlparser::ast::{Expr, BinaryOperator, Value, ValueWithSpan, DataType};
use crate::types::PgType;

/// Detects and handles implicit casts in PostgreSQL-style queries
pub struct ImplicitCastDetector;

impl ImplicitCastDetector {
    /// Check if an expression needs implicit casting based on its context
    pub fn needs_implicit_cast(
        left_expr: &Expr,
        left_type: PgType,
        op: &BinaryOperator,
        right_expr: &Expr,
        right_type: PgType,
    ) -> Option<ImplicitCast> {
        use BinaryOperator::*;
        
        // Check for comparison operations that need implicit casts
        match op {
            Eq | NotEq | Lt | LtEq | Gt | GtEq => {
                Self::check_comparison_cast(left_expr, left_type, right_expr, right_type)
            }
            // Arithmetic operations may also need implicit casts
            Plus | Minus | Multiply | Divide => {
                Self::check_arithmetic_cast(left_expr, left_type, right_expr, right_type)
            }
            _ => None,
        }
    }
    
    /// Check if comparison needs implicit cast
    fn check_comparison_cast(
        left_expr: &Expr,
        left_type: PgType,
        right_expr: &Expr,
        right_type: PgType,
    ) -> Option<ImplicitCast> {
        // Case 1: Integer column = 'decimal_string' (e.g., integer_col = '123.45')
        if Self::is_integer_type(left_type) && Self::is_decimal_string_literal(right_expr) {
            return Some(ImplicitCast::StringToDecimal { 
                expr: right_expr.clone(),
                target_type: PgType::Numeric,
            });
        }
        
        // Case 2: 'decimal_string' = Integer column
        if Self::is_decimal_string_literal(left_expr) && Self::is_integer_type(right_type) {
            return Some(ImplicitCast::StringToDecimal {
                expr: left_expr.clone(),
                target_type: PgType::Numeric,
            });
        }
        
        // Case 3: Numeric column compared with integer literal
        if Self::is_numeric_type(left_type) && Self::is_integer_literal(right_expr) {
            return Some(ImplicitCast::IntegerToDecimal {
                expr: right_expr.clone(),
            });
        }
        
        // Case 4: Integer literal compared with numeric column
        if Self::is_integer_literal(left_expr) && Self::is_numeric_type(right_type) {
            return Some(ImplicitCast::IntegerToDecimal {
                expr: left_expr.clone(),
            });
        }
        
        None
    }
    
    /// Check if arithmetic operation needs implicit cast
    fn check_arithmetic_cast(
        left_expr: &Expr,
        left_type: PgType,
        right_expr: &Expr,
        right_type: PgType,
    ) -> Option<ImplicitCast> {
        // Type promotion in arithmetic: integer + decimal -> decimal
        if Self::is_integer_type(left_type) && Self::is_numeric_type(right_type) {
            return Some(ImplicitCast::IntegerToDecimal {
                expr: left_expr.clone(),
            });
        }
        
        if Self::is_numeric_type(left_type) && Self::is_integer_type(right_type) {
            return Some(ImplicitCast::IntegerToDecimal {
                expr: right_expr.clone(),
            });
        }
        
        None
    }
    
    /// Check if expression is a string literal containing a decimal number
    fn is_decimal_string_literal(expr: &Expr) -> bool {
        match expr {
            Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }) => {
                // Check if string looks like a decimal number
                // Allow both '123.45' and '5' to trigger implicit casts
                s.parse::<f64>().is_ok()
            }
            _ => false,
        }
    }
    
    /// Check if expression is an integer literal
    fn is_integer_literal(expr: &Expr) -> bool {
        match expr {
            Expr::Value(ValueWithSpan { value: Value::Number(n, _), .. }) => {
                !n.contains('.')
            }
            _ => false,
        }
    }
    
    /// Check if type is an integer type
    fn is_integer_type(pg_type: PgType) -> bool {
        matches!(pg_type, PgType::Int2 | PgType::Int4 | PgType::Int8)
    }
    
    /// Check if type is a numeric/decimal type
    fn is_numeric_type(pg_type: PgType) -> bool {
        matches!(pg_type, PgType::Numeric)
    }
    
    /// Detect implicit casts in function arguments
    pub fn check_function_arg_cast(
        func_name: &str,
        arg_position: usize,
        arg_expr: &Expr,
        arg_type: PgType,
    ) -> Option<ImplicitCast> {
        // Check known functions that expect specific types
        match func_name.to_uppercase().as_str() {
            "ROUND" | "TRUNC" | "CEIL" | "FLOOR" => {
                if arg_position == 0 && !Self::is_numeric_type(arg_type) {
                    // These functions expect numeric input
                    return Some(ImplicitCast::ToDecimal {
                        expr: arg_expr.clone(),
                        source_type: arg_type,
                    });
                }
            }
            // These math functions return floats and don't need decimal arguments
            // They can work with regular numeric types without conversion
            "POW" | "POWER" | "SQRT" | "EXP" | "LN" | "LOG" | 
            "SIN" | "COS" | "TAN" | "ASIN" | "ACOS" | "ATAN" => {
                // Don't apply implicit casts for float-returning math functions
                // They work fine with integer/float/decimal inputs
                // and always return float values
            }
            _ => {}
        }
        
        None
    }
    
    /// Detect implicit casts in INSERT/UPDATE assignments
    pub fn check_assignment_cast(
        target_type: PgType,
        value_expr: &Expr,
        value_type: PgType,
    ) -> Option<ImplicitCast> {
        // If target is numeric and value is not, need implicit cast
        if Self::is_numeric_type(target_type) && !Self::is_numeric_type(value_type) {
            // Check for string literals that look like numbers
            if let Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }) = value_expr {
                if s.parse::<f64>().is_ok() {
                    return Some(ImplicitCast::StringToDecimal {
                        expr: value_expr.clone(),
                        target_type,
                    });
                }
            }
            
            // Integer values can be implicitly cast to decimal
            if Self::is_integer_type(value_type) {
                return Some(ImplicitCast::IntegerToDecimal {
                    expr: value_expr.clone(),
                });
            }
        }
        
        None
    }
}

/// Represents an implicit cast that needs to be applied
#[derive(Debug, Clone)]
pub enum ImplicitCast {
    /// Cast string literal to decimal (e.g., '123.45' -> decimal)
    StringToDecimal { 
        expr: Expr,
        target_type: PgType,
    },
    /// Cast integer to decimal for arithmetic/comparison
    IntegerToDecimal {
        expr: Expr,
    },
    /// General cast to decimal type
    ToDecimal {
        expr: Expr,
        source_type: PgType,
    },
}

impl ImplicitCast {
    /// Apply the implicit cast by wrapping the expression
    pub fn apply(self) -> Expr {
        match self {
            ImplicitCast::StringToDecimal { expr, .. } |
            ImplicitCast::IntegerToDecimal { expr } |
            ImplicitCast::ToDecimal { expr, .. } => {
                // Wrap in decimal_from_text function
                let text_expr = match &expr {
                    Expr::Value(ValueWithSpan { value: Value::Number(_, _), .. }) => {
                        // Cast number to text first
                        Expr::Cast {
                            expr: Box::new(expr),
                            data_type: DataType::Text,
                            format: None,
                            kind: sqlparser::ast::CastKind::Cast,
                        }
                    }
                    _ => expr,
                };
                
                Expr::Function(sqlparser::ast::Function {
                    name: sqlparser::ast::ObjectName(vec![
                        sqlparser::ast::ObjectNamePart::Identifier(
                            sqlparser::ast::Ident::new("decimal_from_text")
                        )
                    ]),
                    args: sqlparser::ast::FunctionArguments::List(
                        sqlparser::ast::FunctionArgumentList {
                            duplicate_treatment: None,
                            args: vec![
                                sqlparser::ast::FunctionArg::Unnamed(
                                    sqlparser::ast::FunctionArgExpr::Expr(text_expr)
                                )
                            ],
                            clauses: vec![],
                        }
                    ),
                    over: None,
                    uses_odbc_syntax: false,
                    parameters: sqlparser::ast::FunctionArguments::None,
                    filter: None,
                    null_treatment: None,
                    within_group: vec![],
                })
            }
        }
    }
}