use sqlparser::ast::{Expr, BinaryOperator, Value as SqlValue, UnaryOperator};
use std::collections::HashMap;
use tracing::debug;

/// Evaluates WHERE clauses against catalog row data
pub struct WhereEvaluator;

impl WhereEvaluator {
    /// Evaluate a WHERE clause expression against a row of data
    pub fn evaluate(
        expr: &Expr,
        row_data: &HashMap<String, String>,
        column_mapping: &HashMap<String, usize>,
    ) -> bool {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                Self::evaluate_binary_op(left, op, right, row_data, column_mapping)
            }
            Expr::UnaryOp { op, expr } => {
                Self::evaluate_unary_op(op, expr, row_data, column_mapping)
            }
            Expr::InList { expr, list, negated } => {
                Self::evaluate_in_list(expr, list, *negated, row_data, column_mapping)
            }
            Expr::IsNull(expr) => {
                let value = Self::get_column_value(expr, row_data);
                value.is_none()
            }
            Expr::IsNotNull(expr) => {
                let value = Self::get_column_value(expr, row_data);
                value.is_some()
            }
            Expr::Like { expr, pattern, negated, .. } => {
                Self::evaluate_like(expr, pattern, *negated, row_data, column_mapping)
            }
            Expr::ILike { expr, pattern, negated, .. } => {
                Self::evaluate_ilike(expr, pattern, *negated, row_data, column_mapping)
            }
            Expr::Value(val) => {
                // A literal value evaluates to its boolean interpretation
                match &val.value {
                    SqlValue::Boolean(b) => *b,
                    SqlValue::SingleQuotedString(s) => s == "t" || s == "true",
                    _ => false,
                }
            }
            Expr::Function(func) => {
                // Handle function calls in WHERE clause
                let func_name = func.name.to_string().to_lowercase();
                if func_name == "pg_table_is_visible" || func_name == "pg_catalog.pg_table_is_visible" {
                    // pg_table_is_visible always returns true for all tables in SQLite
                    true
                } else {
                    debug!("Unsupported function in WHERE clause: {}", func_name);
                    true
                }
            }
            _ => {
                debug!("Unsupported WHERE expression type: {:?}", expr);
                true // Default to including the row if we can't evaluate
            }
        }
    }

    fn evaluate_binary_op(
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
        row_data: &HashMap<String, String>,
        column_mapping: &HashMap<String, usize>,
    ) -> bool {
        match op {
            BinaryOperator::Eq => {
                let left_val = Self::get_expression_value(left, row_data);
                let right_val = Self::get_expression_value(right, row_data);
                left_val == right_val
            }
            BinaryOperator::NotEq => {
                let left_val = Self::get_expression_value(left, row_data);
                let right_val = Self::get_expression_value(right, row_data);
                left_val != right_val
            }
            BinaryOperator::Lt => {
                if let (Some(left_val), Some(right_val)) = (
                    Self::get_expression_value(left, row_data),
                    Self::get_expression_value(right, row_data),
                ) {
                    Self::compare_values(&left_val, &right_val) < 0
                } else {
                    false
                }
            }
            BinaryOperator::LtEq => {
                if let (Some(left_val), Some(right_val)) = (
                    Self::get_expression_value(left, row_data),
                    Self::get_expression_value(right, row_data),
                ) {
                    Self::compare_values(&left_val, &right_val) <= 0
                } else {
                    false
                }
            }
            BinaryOperator::Gt => {
                if let (Some(left_val), Some(right_val)) = (
                    Self::get_expression_value(left, row_data),
                    Self::get_expression_value(right, row_data),
                ) {
                    Self::compare_values(&left_val, &right_val) > 0
                } else {
                    false
                }
            }
            BinaryOperator::GtEq => {
                if let (Some(left_val), Some(right_val)) = (
                    Self::get_expression_value(left, row_data),
                    Self::get_expression_value(right, row_data),
                ) {
                    Self::compare_values(&left_val, &right_val) >= 0
                } else {
                    false
                }
            }
            BinaryOperator::And => {
                Self::evaluate(left, row_data, column_mapping)
                    && Self::evaluate(right, row_data, column_mapping)
            }
            BinaryOperator::Or => {
                Self::evaluate(left, row_data, column_mapping)
                    || Self::evaluate(right, row_data, column_mapping)
            }
            BinaryOperator::PGRegexMatch => {
                Self::evaluate_regex_match(left, right, false, row_data)
            }
            BinaryOperator::PGRegexNotMatch => {
                Self::evaluate_regex_match(left, right, true, row_data)
            }
            _ => {
                debug!("Unsupported binary operator: {:?}", op);
                true
            }
        }
    }

    fn evaluate_unary_op(
        op: &UnaryOperator,
        expr: &Expr,
        row_data: &HashMap<String, String>,
        column_mapping: &HashMap<String, usize>,
    ) -> bool {
        match op {
            UnaryOperator::Not => !Self::evaluate(expr, row_data, column_mapping),
            _ => {
                debug!("Unsupported unary operator: {:?}", op);
                true
            }
        }
    }

    fn evaluate_in_list(
        expr: &Expr,
        list: &[Expr],
        negated: bool,
        row_data: &HashMap<String, String>,
        _column_mapping: &HashMap<String, usize>,
    ) -> bool {
        let value = Self::get_expression_value(expr, row_data);
        if value.is_none() {
            return false;
        }
        let value = value.unwrap();

        let mut found = false;
        for item in list {
            if let Some(item_val) = Self::get_expression_value(item, row_data) {
                if value == item_val {
                    found = true;
                    break;
                }
            }
        }

        if negated {
            !found
        } else {
            found
        }
    }

    fn evaluate_like(
        expr: &Expr,
        pattern: &Expr,
        negated: bool,
        row_data: &HashMap<String, String>,
        _column_mapping: &HashMap<String, usize>,
    ) -> bool {
        if let (Some(value), Some(pattern_str)) = (
            Self::get_expression_value(expr, row_data),
            Self::get_expression_value(pattern, row_data),
        ) {
            let matches = Self::like_match(&value, &pattern_str);
            debug!("LIKE evaluation: '{}' LIKE '{}' = {}", value, pattern_str, matches);
            if negated {
                !matches
            } else {
                matches
            }
        } else {
            debug!("LIKE evaluation failed to get values from expressions");
            false
        }
    }

    fn evaluate_ilike(
        expr: &Expr,
        pattern: &Expr,
        negated: bool,
        row_data: &HashMap<String, String>,
        _column_mapping: &HashMap<String, usize>,
    ) -> bool {
        if let (Some(value), Some(pattern_str)) = (
            Self::get_expression_value(expr, row_data),
            Self::get_expression_value(pattern, row_data),
        ) {
            let matches = Self::like_match(&value.to_lowercase(), &pattern_str.to_lowercase());
            if negated {
                !matches
            } else {
                matches
            }
        } else {
            false
        }
    }

    fn evaluate_regex_match(
        expr: &Expr,
        pattern: &Expr,
        negated: bool,
        row_data: &HashMap<String, String>,
    ) -> bool {
        if let (Some(value), Some(pattern_str)) = (
            Self::get_expression_value(expr, row_data),
            Self::get_expression_value(pattern, row_data),
        ) {
            // PostgreSQL ~ operator uses POSIX regex
            // For now, use simple pattern matching as a placeholder
            let matches = if pattern_str.starts_with('^') && pattern_str.ends_with('$') {
                // Exact match pattern
                let pattern_content = &pattern_str[1..pattern_str.len()-1];
                value == pattern_content
            } else if pattern_str.starts_with('^') {
                // Starts with pattern
                let pattern_content = &pattern_str[1..];
                value.starts_with(pattern_content)
            } else if pattern_str.ends_with('$') {
                // Ends with pattern
                let pattern_content = &pattern_str[..pattern_str.len()-1];
                value.ends_with(pattern_content)
            } else {
                // Contains pattern
                value.contains(&pattern_str)
            };

            if negated {
                !matches
            } else {
                matches
            }
        } else {
            false
        }
    }

    fn get_column_value(expr: &Expr, row_data: &HashMap<String, String>) -> Option<String> {
        match expr {
            Expr::Identifier(ident) => {
                row_data.get(&ident.value.to_lowercase()).cloned()
            }
            Expr::CompoundIdentifier(parts) => {
                // For compound identifiers like t.column, use just the column name
                if let Some(last) = parts.last() {
                    row_data.get(&last.value.to_lowercase()).cloned()
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn get_expression_value(expr: &Expr, row_data: &HashMap<String, String>) -> Option<String> {
        match expr {
            Expr::Value(val) => match &val.value {
                SqlValue::SingleQuotedString(s) => Some(s.clone()),
                SqlValue::Number(n, _) => Some(n.clone()),
                SqlValue::Boolean(b) => Some(if *b { "t".to_string() } else { "f".to_string() }),
                SqlValue::Null => None,
                _ => None,
            },
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                Self::get_column_value(expr, row_data)
            }
            _ => None,
        }
    }

    fn compare_values(left: &str, right: &str) -> i32 {
        // Try to compare as numbers first
        if let (Ok(left_num), Ok(right_num)) = (left.parse::<i64>(), right.parse::<i64>()) {
            left_num.cmp(&right_num) as i32
        } else if let (Ok(left_num), Ok(right_num)) = (left.parse::<f64>(), right.parse::<f64>()) {
            if left_num < right_num {
                -1
            } else if left_num > right_num {
                1
            } else {
                0
            }
        } else {
            // Compare as strings
            left.cmp(right) as i32
        }
    }

    fn like_match(value: &str, pattern: &str) -> bool {
        // Convert SQL LIKE pattern to simple pattern matching
        // % matches any sequence of characters
        // _ matches any single character
        
        // For simplicity, we'll use a basic implementation
        // that handles common cases
        if pattern == "%" {
            return true;
        }

        let pattern_chars: Vec<char> = pattern.chars().collect();
        let value_chars: Vec<char> = value.chars().collect();
        
        Self::like_match_recursive(&value_chars, &pattern_chars, 0, 0)
    }

    fn like_match_recursive(
        value: &[char],
        pattern: &[char],
        val_idx: usize,
        pat_idx: usize,
    ) -> bool {
        if pat_idx >= pattern.len() {
            return val_idx >= value.len();
        }

        if val_idx >= value.len() {
            // Only match if remaining pattern is all %
            return pattern[pat_idx..].iter().all(|&c| c == '%');
        }

        match pattern[pat_idx] {
            '%' => {
                // Try matching 0 or more characters
                for i in val_idx..=value.len() {
                    if Self::like_match_recursive(value, pattern, i, pat_idx + 1) {
                        return true;
                    }
                }
                false
            }
            '_' => {
                // Match exactly one character
                Self::like_match_recursive(value, pattern, val_idx + 1, pat_idx + 1)
            }
            c => {
                // Match literal character
                if value[val_idx] == c {
                    Self::like_match_recursive(value, pattern, val_idx + 1, pat_idx + 1)
                } else {
                    false
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_equality() {
        let mut row_data = HashMap::new();
        row_data.insert("relkind".to_string(), "r".to_string());
        row_data.insert("relname".to_string(), "users".to_string());

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(sqlparser::ast::Ident::new("relkind"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(sqlparser::ast::ValueWithSpan {
                value: SqlValue::SingleQuotedString("r".to_string()),
                span: sqlparser::tokenizer::Span {
                    start: sqlparser::tokenizer::Location { line: 1, column: 1 },
                    end: sqlparser::tokenizer::Location { line: 1, column: 1 },
                },
            })),
        };

        let column_mapping = HashMap::new();
        assert!(WhereEvaluator::evaluate(&expr, &row_data, &column_mapping));
    }

    #[test]
    fn test_in_list() {
        let mut row_data = HashMap::new();
        row_data.insert("relkind".to_string(), "r".to_string());

        let expr = Expr::InList {
            expr: Box::new(Expr::Identifier(sqlparser::ast::Ident::new("relkind"))),
            list: vec![
                Expr::Value(sqlparser::ast::ValueWithSpan {
                    value: SqlValue::SingleQuotedString("r".to_string()),
                    span: sqlparser::tokenizer::Span {
                    start: sqlparser::tokenizer::Location { line: 1, column: 1 },
                    end: sqlparser::tokenizer::Location { line: 1, column: 1 },
                },
                }),
                Expr::Value(sqlparser::ast::ValueWithSpan {
                    value: SqlValue::SingleQuotedString("p".to_string()),
                    span: sqlparser::tokenizer::Span {
                    start: sqlparser::tokenizer::Location { line: 1, column: 1 },
                    end: sqlparser::tokenizer::Location { line: 1, column: 1 },
                },
                }),
            ],
            negated: false,
        };

        let column_mapping = HashMap::new();
        assert!(WhereEvaluator::evaluate(&expr, &row_data, &column_mapping));
    }

    #[test]
    fn test_like_match_function() {
        // Test basic like patterns
        assert!(WhereEvaluator::like_match("pgclass_test_table1", "pgclass_test_%"));
        assert!(WhereEvaluator::like_match("pgclass_test_table2", "pgclass_test_%"));
        assert!(!WhereEvaluator::like_match("other_table", "pgclass_test_%"));
        assert!(WhereEvaluator::like_match("test", "test"));
        assert!(WhereEvaluator::like_match("test", "te%"));
        assert!(WhereEvaluator::like_match("test", "%st"));
        assert!(WhereEvaluator::like_match("test", "t_st"));
    }
    
    #[test]
    fn test_like_pattern() {
        let mut row_data = HashMap::new();
        row_data.insert("nspname".to_string(), "pg_toast".to_string());

        let expr = Expr::Like {
            expr: Box::new(Expr::Identifier(sqlparser::ast::Ident::new("nspname"))),
            pattern: Box::new(Expr::Value(sqlparser::ast::ValueWithSpan {
                value: SqlValue::SingleQuotedString("pg_%".to_string()),
                span: sqlparser::tokenizer::Span {
                    start: sqlparser::tokenizer::Location { line: 1, column: 1 },
                    end: sqlparser::tokenizer::Location { line: 1, column: 1 },
                },
            })),
            negated: false,
            escape_char: None,
            any: false,
        };

        let column_mapping = HashMap::new();
        assert!(WhereEvaluator::evaluate(&expr, &row_data, &column_mapping));
        
        // Test our specific case
        let mut row_data2 = HashMap::new();
        row_data2.insert("relname".to_string(), "pgclass_test_table1".to_string());
        
        let expr2 = Expr::Like {
            expr: Box::new(Expr::Identifier(sqlparser::ast::Ident::new("relname"))),
            pattern: Box::new(Expr::Value(sqlparser::ast::ValueWithSpan {
                value: SqlValue::SingleQuotedString("pgclass_test_%".to_string()),
                span: sqlparser::tokenizer::Span {
                    start: sqlparser::tokenizer::Location { line: 1, column: 1 },
                    end: sqlparser::tokenizer::Location { line: 1, column: 1 },
                },
            })),
            negated: false,
            escape_char: None,
            any: false,
        };
        
        assert!(WhereEvaluator::evaluate(&expr2, &row_data2, &column_mapping),
            "pgclass_test_table1 should match pgclass_test_%");
    }
}