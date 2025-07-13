use regex::Regex;
use once_cell::sync::Lazy;
use super::TranslationMetadata;
use tracing::{debug, info};

/// Analyzes arithmetic expressions in SQL queries to generate type metadata
pub struct ArithmeticAnalyzer;

// Regex patterns for detecting arithmetic expressions in SELECT clauses
static SELECT_CLAUSE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    // Match SELECT ... FROM pattern to extract the projection list
    Regex::new(r"(?i)\bSELECT\s+(.*?)\s+FROM\b").unwrap()
});

static ARITHMETIC_EXPR_PATTERN: Lazy<Regex> = Lazy::new(|| {
    // Match arithmetic expressions with explicit AS alias
    // This pattern matches complex arithmetic expressions including nested parentheses
    // Captures: (1) full expression, (2) alias
    // The expression part now matches any combination of:
    // - identifiers (column names)
    // - numbers (integers or decimals)
    // - arithmetic operators (+, -, *, /)
    // - parentheses for grouping
    // - whitespace
    Regex::new(r"(?i)([\w\.\s\+\-\*/\(\)]+[\+\-\*/][\w\.\s\+\-\*/\(\)]+)\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*)").unwrap()
});

static COLUMN_IN_EXPR_PATTERN: Lazy<Regex> = Lazy::new(|| {
    // Match column names within arithmetic expressions
    // Look for identifiers that are not pure numbers
    Regex::new(r"(?i)\b([a-zA-Z_][a-zA-Z0-9_\.]*)").unwrap()
});

impl ArithmeticAnalyzer {
    /// Check if the query contains arithmetic expressions that need analysis
    pub fn needs_analysis(query: &str) -> bool {
        // Quick check for arithmetic operators in SELECT clause
        if let Some(caps) = SELECT_CLAUSE_PATTERN.captures(query) {
            let select_list = &caps[1];
            select_list.contains('+') || select_list.contains('-') || 
            select_list.contains('*') || select_list.contains('/')
        } else {
            false
        }
    }
    
    /// Analyze query and extract metadata for arithmetic expressions
    pub fn analyze_query(query: &str) -> TranslationMetadata {
        let mut metadata = TranslationMetadata::new();
        
        // Extract SELECT clause
        if let Some(select_caps) = SELECT_CLAUSE_PATTERN.captures(query) {
            let select_list = &select_caps[1];
            debug!("Analyzing SELECT clause for arithmetic: {}", select_list);
            
            // Find all arithmetic expressions with aliases
            for caps in ARITHMETIC_EXPR_PATTERN.captures_iter(select_list) {
                let expression = &caps[1];
                let alias = &caps[2];
                
                debug!("Found arithmetic expression '{}' aliased as '{}'", expression, alias);
                
                // Extract all columns from the expression
                let mut columns = Vec::new();
                for column_match in COLUMN_IN_EXPR_PATTERN.captures_iter(expression) {
                    let col = column_match[1].to_string();
                    if !col.chars().all(|c| c.is_numeric() || c == '.') {
                        columns.push(col);
                    }
                }
                
                if !columns.is_empty() {
                    // Use the first column found as the source
                    let source_column = columns[0].clone();
                    info!("Detected arithmetic on columns {:?} aliased as '{}', using '{}' as source", columns, alias, source_column);
                    
                    // Create metadata hint for arithmetic on float
                    // The actual type will be determined by looking up the source column
                    let hint = super::ColumnTypeHint::arithmetic_on_float(source_column);
                    metadata.add_hint(alias.to_string(), hint);
                }
            }
            
            // Also check for expressions without explicit AS keyword
            // Pattern: column +/- number, alias (with comma or FROM following)
            let implicit_alias_pattern = Regex::new(
                r"(?i)([a-zA-Z_][a-zA-Z0-9_\.]*\s*[\+\-\*/]\s*[0-9.]+)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:,|FROM|$)"
            ).unwrap();
            
            for caps in implicit_alias_pattern.captures_iter(select_list) {
                let expression = &caps[1];
                let alias = &caps[2];
                
                // Skip if this alias is actually a SQL keyword
                if is_sql_keyword(alias) {
                    continue;
                }
                
                debug!("Found arithmetic expression '{}' with implicit alias '{}'", expression, alias);
                
                // Extract all columns from the expression
                let mut columns = Vec::new();
                for column_match in COLUMN_IN_EXPR_PATTERN.captures_iter(expression) {
                    let col = column_match[1].to_string();
                    if !col.chars().all(|c| c.is_numeric() || c == '.') {
                        columns.push(col);
                    }
                }
                
                if !columns.is_empty() {
                    // Use the first column found as the source
                    let source_column = columns[0].clone();
                    info!("Detected arithmetic on columns {:?} implicitly aliased as '{}', using '{}' as source", columns, alias, source_column);
                    
                    let hint = super::ColumnTypeHint::arithmetic_on_float(source_column);
                    metadata.add_hint(alias.to_string(), hint);
                }
            }
        }
        
        metadata
    }
}

/// Check if a string is a SQL keyword (to avoid false positives in implicit alias detection)
fn is_sql_keyword(s: &str) -> bool {
    matches!(s.to_uppercase().as_str(), 
        "FROM" | "WHERE" | "GROUP" | "ORDER" | "HAVING" | "LIMIT" | 
        "OFFSET" | "UNION" | "INTERSECT" | "EXCEPT" | "AS" | "ON" | 
        "AND" | "OR" | "NOT" | "IN" | "EXISTS" | "BETWEEN" | "LIKE" |
        "JOIN" | "LEFT" | "RIGHT" | "INNER" | "OUTER" | "CROSS"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_needs_analysis() {
        assert!(ArithmeticAnalyzer::needs_analysis("SELECT price * 1.1 AS total FROM items"));
        assert!(ArithmeticAnalyzer::needs_analysis("SELECT col1 + 5 FROM table"));
        assert!(!ArithmeticAnalyzer::needs_analysis("SELECT col1, col2 FROM table"));
        assert!(!ArithmeticAnalyzer::needs_analysis("INSERT INTO table VALUES (1 + 2)"));
    }
    
    #[test]
    fn test_analyze_arithmetic_with_explicit_alias() {
        let query = "SELECT price * 1.1 AS price_with_tax FROM products";
        let metadata = ArithmeticAnalyzer::analyze_query(query);
        
        let hint = metadata.get_hint("price_with_tax").expect("Should have hint for price_with_tax");
        assert_eq!(hint.source_column.as_ref().unwrap(), "price");
        assert!(hint.is_expression);
        assert_eq!(hint.expression_type, Some(crate::translator::ExpressionType::ArithmeticOnFloat));
    }
    
    #[test]
    fn test_analyze_arithmetic_with_implicit_alias() {
        let query = "SELECT price * 1.1 total FROM products";
        let metadata = ArithmeticAnalyzer::analyze_query(query);
        
        let hint = metadata.get_hint("total").expect("Should have hint for total");
        assert_eq!(hint.source_column.as_ref().unwrap(), "price");
    }
    
    #[test]
    fn test_analyze_multiple_arithmetic_expressions() {
        let query = "SELECT price * 1.1 AS with_tax, cost + 10 AS adjusted_cost FROM products";
        let metadata = ArithmeticAnalyzer::analyze_query(query);
        
        assert!(metadata.get_hint("with_tax").is_some());
        assert!(metadata.get_hint("adjusted_cost").is_some());
        
        let tax_hint = metadata.get_hint("with_tax").unwrap();
        assert_eq!(tax_hint.source_column.as_ref().unwrap(), "price");
        
        let cost_hint = metadata.get_hint("adjusted_cost").unwrap();
        assert_eq!(cost_hint.source_column.as_ref().unwrap(), "cost");
    }
    
    #[test]
    fn test_analyze_complex_expressions() {
        let query = "SELECT quantity * price + shipping AS total_cost FROM orders";
        let metadata = ArithmeticAnalyzer::analyze_query(query);
        
        let hint = metadata.get_hint("total_cost").expect("Should have hint for total_cost");
        // Should extract the first column in the expression
        assert_eq!(hint.source_column.as_ref().unwrap(), "quantity");
    }
    
    #[test]
    fn test_no_false_positives_for_keywords() {
        let query = "SELECT col1 + 5 FROM table WHERE col2 > 10";
        let metadata = ArithmeticAnalyzer::analyze_query(query);
        
        // Should not create a hint for "FROM" or "WHERE"
        assert!(metadata.get_hint("FROM").is_none());
        assert!(metadata.get_hint("WHERE").is_none());
    }
}