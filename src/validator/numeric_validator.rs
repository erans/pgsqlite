use rusqlite::Connection;
use std::collections::HashMap;
use once_cell::sync::Lazy;
use std::sync::RwLock;
use regex::Regex;
use crate::error::PgError;

/// Cache for numeric constraints by table
static CONSTRAINT_CACHE: Lazy<RwLock<HashMap<String, HashMap<String, (i32, i32)>>>> = 
    Lazy::new(|| RwLock::new(HashMap::new()));

pub struct NumericValidator;

impl NumericValidator {
    /// Load numeric constraints for a table into cache
    pub fn load_table_constraints(conn: &Connection, table_name: &str) -> Result<(), rusqlite::Error> {
        // Check if constraints table exists
        let exists: bool = conn.query_row(
            "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='__pgsqlite_numeric_constraints'",
            [],
            |row| row.get(0)
        ).unwrap_or(false);
        
        if !exists {
            return Ok(());
        }
        
        // Load constraints
        let mut stmt = conn.prepare(
            "SELECT column_name, precision, scale FROM __pgsqlite_numeric_constraints WHERE table_name = ?1"
        )?;
        
        let constraints = stmt.query_map([table_name], |row| {
            Ok((
                row.get::<_, String>(0)?,
                (row.get::<_, i32>(1)?, row.get::<_, i32>(2)?)
            ))
        })?;
        
        let mut table_constraints = HashMap::new();
        for constraint in constraints {
            let (col_name, (precision, scale)) = constraint?;
            table_constraints.insert(col_name, (precision, scale));
        }
        
        // Update cache
        let mut cache = CONSTRAINT_CACHE.write().unwrap();
        cache.insert(table_name.to_string(), table_constraints);
        
        Ok(())
    }
    
    /// Validate a numeric value against constraints
    pub fn validate_value(value: &str, precision: i32, scale: i32) -> Result<(), PgError> {
        // Handle NULL/empty
        if value.is_empty() || value.eq_ignore_ascii_case("null") {
            return Ok(());
        }
        
        // Parse the numeric value
        let value = value.trim();
        
        // First, validate the format (must be a valid numeric string)
        let (integer_part, decimal_part) = Self::parse_numeric_parts(value)?;
        
        // Validate scale (decimal places)
        if decimal_part.len() > scale as usize {
            return Err(PgError::NumericValueOutOfRange {
                type_name: format!("numeric({precision},{scale})"),
                column_name: String::new(),
                value: value.to_string(),
            });
        }
        
        // Validate precision (total significant digits)
        // For NUMERIC(p,s), the maximum integer part has (p-s) digits
        let max_integer_digits = (precision - scale) as usize;
        
        // Count significant digits in integer part (excluding leading zeros and sign)
        let integer_without_sign = integer_part.trim_start_matches('-').trim_start_matches('+');
        let significant_integer = integer_without_sign.trim_start_matches('0');
        if significant_integer.len() > max_integer_digits {
            return Err(PgError::NumericValueOutOfRange {
                type_name: format!("numeric({precision},{scale})"),
                column_name: String::new(),
                value: value.to_string(),
            });
        }
        
        // Special case: if integer part is exactly max digits, check if it's not too large
        if significant_integer.len() == max_integer_digits && max_integer_digits > 0 {
            // For very large precision, use string comparison
            if precision > 15 {
                // Create a string of all 9s for the maximum value
                let max_value_str = "9".repeat(max_integer_digits);
                if significant_integer > max_value_str.as_str() {
                    return Err(PgError::NumericValueOutOfRange {
                        type_name: format!("numeric({precision},{scale})"),
                        column_name: String::new(),
                        value: value.to_string(),
                    });
                }
            } else {
                // For smaller precision, we can still use f64
                if let Ok(num_value) = value.parse::<f64>() {
                    let max_value = 10f64.powi(precision - scale);
                    if num_value.abs() >= max_value {
                        return Err(PgError::NumericValueOutOfRange {
                            type_name: format!("numeric({precision},{scale})"),
                            column_name: String::new(),
                            value: value.to_string(),
                        });
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Parse a numeric string into integer and decimal parts
    fn parse_numeric_parts(value: &str) -> Result<(String, String), PgError> {
        let value = value.trim();
        
        // First check for scientific notation
        if value.contains('e') || value.contains('E') {
            // Convert scientific notation to decimal
            match value.parse::<f64>() {
                Ok(num) => {
                    // Format to string without scientific notation
                    let formatted = format!("{num:.10}");
                    // Remove trailing zeros after decimal
                    let trimmed = formatted.trim_end_matches('0').trim_end_matches('.');
                    return Self::parse_numeric_parts(trimmed);
                }
                Err(_) => {
                    return Err(PgError::NumericValueOutOfRange {
                        type_name: "numeric".to_string(),
                        column_name: String::new(),
                        value: value.to_string(),
                    });
                }
            }
        }
        
        // Check for valid numeric format
        let mut chars = value.chars();
        let mut _has_sign = false;
        let mut has_dot = false;
        let mut integer_part = String::new();
        let mut decimal_part = String::new();
        let mut is_after_dot = false;
        
        // Handle optional sign
        if let Some(first) = chars.next() {
            match first {
                '+' | '-' => {
                    _has_sign = true;
                    if first == '-' {
                        integer_part.push('-');
                    }
                }
                '0'..='9' => {
                    integer_part.push(first);
                }
                '.' => {
                    has_dot = true;
                    is_after_dot = true;
                    integer_part.push('0'); // Leading dot means 0.xxx
                }
                _ => {
                    return Err(PgError::NumericValueOutOfRange {
                        type_name: "numeric".to_string(),
                        column_name: String::new(),
                        value: value.to_string(),
                    });
                }
            }
        } else {
            return Err(PgError::NumericValueOutOfRange {
                type_name: "numeric".to_string(),
                column_name: String::new(),
                value: "empty".to_string(),
            });
        }
        
        // Process remaining characters
        for ch in chars {
            match ch {
                '0'..='9' => {
                    if is_after_dot {
                        decimal_part.push(ch);
                    } else {
                        integer_part.push(ch);
                    }
                }
                '.' => {
                    if has_dot {
                        return Err(PgError::NumericValueOutOfRange {
                            type_name: "numeric".to_string(),
                            column_name: String::new(),
                            value: value.to_string(),
                        });
                    }
                    has_dot = true;
                    is_after_dot = true;
                }
                _ => {
                    return Err(PgError::NumericValueOutOfRange {
                        type_name: "numeric".to_string(),
                        column_name: String::new(),
                        value: value.to_string(),
                    });
                }
            }
        }
        
        // If no integer part after sign, add zero
        if integer_part.is_empty() || integer_part == "-" {
            integer_part.push('0');
        }
        
        // Remove trailing zeros from decimal part for scale validation
        let decimal_trimmed = decimal_part.trim_end_matches('0');
        
        Ok((integer_part, decimal_trimmed.to_string()))
    }
    
    /// Validate INSERT statement values
    pub fn validate_insert(
        conn: &Connection,
        sql: &str,
        table_name: &str
    ) -> Result<(), PgError> {
        // First ensure constraints are loaded
        let _ = Self::load_table_constraints(conn, table_name);
        
        // Get constraints from cache
        let cache = CONSTRAINT_CACHE.read().unwrap();
        let constraints = match cache.get(table_name) {
            Some(c) => {
                c
            },
            None => {
                return Ok(()); // No constraints for this table
            }
        };
        
        if constraints.is_empty() {
            return Ok(()); // No numeric constraints
        }
        
        // Parse the INSERT statement to extract column names and values
        let insert_data = match parse_insert_statement(sql, conn, table_name) {
            Some(data) => data,
            None => {
                return Ok(()); // Couldn't parse, let it through
            }
        };
        
        // Validate each value against its constraint
        for (col_name, value) in insert_data.iter() {
            if let Some((precision, scale)) = constraints.get(col_name) {
                Self::validate_value(value, *precision, *scale)
                    .map_err(|mut e| {
                        // Add column name to error
                        if let PgError::NumericValueOutOfRange { column_name, .. } = &mut e {
                            *column_name = col_name.clone();
                        }
                        e
                    })?;
            }
        }
        
        Ok(())
    }
    
    /// Validate UPDATE statement values
    pub fn validate_update(
        conn: &Connection,
        sql: &str,
        table_name: &str
    ) -> Result<(), PgError> {
        // First ensure constraints are loaded
        let _ = Self::load_table_constraints(conn, table_name);
        
        // Get constraints from cache
        let cache = CONSTRAINT_CACHE.read().unwrap();
        let constraints = match cache.get(table_name) {
            Some(c) => c,
            None => {
                return Ok(()); // No constraints for this table
            }
        };
        
        if constraints.is_empty() {
            return Ok(()); // No numeric constraints
        }
        
        // Parse the UPDATE statement to extract column assignments
        let update_data = match parse_update_statement(sql) {
            Some(data) => data,
            None => {
                return Ok(()); // Couldn't parse, let it through
            }
        };
        
        // Validate each value against its constraint
        for (col_name, value) in update_data.iter() {
            if let Some((precision, scale)) = constraints.get(col_name) {
                Self::validate_value(value, *precision, *scale)
                    .map_err(|mut e| {
                        // Add column name to error
                        if let PgError::NumericValueOutOfRange { column_name, .. } = &mut e {
                            *column_name = col_name.clone();
                        }
                        e
                    })?;
            }
        }
        
        Ok(())
    }
    
    /// Clear constraint cache for a table
    pub fn invalidate_cache(table_name: &str) {
        let mut cache = CONSTRAINT_CACHE.write().unwrap();
        cache.remove(table_name);
    }
}

/// Parse INSERT statement to extract column names and values
fn parse_insert_statement(sql: &str, conn: &Connection, table_name: &str) -> Option<Vec<(String, String)>> {
    // Try multi-row INSERT first (with or without column names)
    
    // Multi-row INSERT with column names: INSERT INTO table (col1, col2) VALUES (val1, val2), (val3, val4)
    static MULTI_INSERT_WITH_COLS_REGEX: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?si)INSERT\s+INTO\s+\w+\s*\(([^)]+)\)\s*VALUES\s*(.+)").unwrap()
    });
    
    if let Some(caps) = MULTI_INSERT_WITH_COLS_REGEX.captures(sql) {
        let columns_str = caps.get(1)?.as_str();
        let all_values_str = caps.get(2)?.as_str();
        
        let columns: Vec<String> = columns_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        
        // Parse all value sets
        let value_sets = parse_multi_row_values(all_values_str);
        if !value_sets.is_empty() {
            let mut all_data = Vec::new();
            
            for values in value_sets {
                if columns.len() != values.len() {
                    continue;
                }
                
                for (col, val) in columns.iter().zip(values.iter()) {
                    all_data.push((col.clone(), val.clone()));
                }
            }
            
            if !all_data.is_empty() {
                return Some(all_data);
            }
        }
    }
    
    // Multi-row INSERT without column names: INSERT INTO table VALUES (val1, val2), (val3, val4)
    static MULTI_INSERT_NO_COLS_REGEX: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?si)INSERT\s+INTO\s+\w+\s+VALUES\s+(.+)").unwrap()
    });
    
    if let Some(caps) = MULTI_INSERT_NO_COLS_REGEX.captures(sql) {
        let all_values_str = caps.get(1)?.as_str();
        
        // Get column names from table schema
        let mut stmt = conn.prepare(&format!("PRAGMA table_info({table_name})")).ok()?;
        let column_info = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i32>(0)?, // cid
                row.get::<_, String>(1)? // name
            ))
        }).ok()?;
        
        let mut columns: Vec<(i32, String)> = Vec::new();
        for col in column_info {
            let (cid, name) = col.ok()?;
            columns.push((cid, name));
        }
        
        // Sort by column ID to get correct order
        columns.sort_by_key(|(cid, _)| *cid);
        
        let column_names: Vec<String> = columns.into_iter()
            .map(|(_, name)| name)
            .collect();
        
        // Parse all value sets
        let value_sets = parse_multi_row_values(all_values_str);
        if !value_sets.is_empty() {
            let mut all_data = Vec::new();
            
            for values in value_sets {
                if column_names.len() != values.len() {
                    continue;
                }
                
                for (col, val) in column_names.iter().zip(values.iter()) {
                    all_data.push((col.clone(), val.clone()));
                }
            }
            
            if !all_data.is_empty() {
                return Some(all_data);
            }
        }
    }
    
    None
}

/// Parse multi-row VALUES clause like (val1, val2), (val3, val4)
fn parse_multi_row_values(values_str: &str) -> Vec<Vec<String>> {
    let mut result = Vec::new();
    let mut current_set = Vec::new();
    let mut current_value = String::new();
    let mut in_quotes = false;
    let mut quote_char = ' ';
    let mut paren_depth = 0;
    let mut in_value_set = false;
    let mut in_comment = false;
    
    let chars: Vec<char> = values_str.chars().collect();
    let mut i = 0;
    
    while i < chars.len() {
        let ch = chars[i];
        
        // Handle SQL comments
        if !in_quotes && !in_comment && ch == '-' && i + 1 < chars.len() && chars[i + 1] == '-' {
            // Start of comment, skip to end of line
            in_comment = true;
            i += 2;
            continue;
        }
        
        if in_comment {
            if ch == '\n' {
                in_comment = false;
            }
            i += 1;
            continue;
        }
        
        match ch {
            '(' if !in_quotes => {
                paren_depth += 1;
                if paren_depth == 1 {
                    in_value_set = true;
                } else {
                    current_value.push(ch);
                }
            }
            ')' if !in_quotes => {
                paren_depth -= 1;
                if paren_depth == 0 && in_value_set {
                    // End of value set
                    if !current_value.is_empty() {
                        current_set.push(current_value.trim().trim_matches('\'').trim_matches('"').to_string());
                        current_value.clear();
                    }
                    if !current_set.is_empty() {
                        result.push(current_set.clone());
                        current_set.clear();
                    }
                    in_value_set = false;
                } else {
                    current_value.push(ch);
                }
            }
            '\'' | '"' if !in_quotes => {
                in_quotes = true;
                quote_char = ch;
                current_value.push(ch);
            }
            ch if ch == quote_char && in_quotes => {
                in_quotes = false;
                current_value.push(ch);
            }
            ',' if !in_quotes && in_value_set && paren_depth == 1 => {
                // Value separator within a set
                current_set.push(current_value.trim().trim_matches('\'').trim_matches('"').to_string());
                current_value.clear();
            }
            ',' if !in_quotes && !in_value_set && paren_depth == 0 => {
                // Separator between value sets - ignore
            }
            _ => {
                if in_value_set {
                    current_value.push(ch);
                }
            }
        }
        
        i += 1;
    }
    
    result
}

/// Parse UPDATE statement to extract column assignments
fn parse_update_statement(sql: &str) -> Option<Vec<(String, String)>> {
    // Simple regex-based parser for UPDATE statements
    // This handles: UPDATE table SET col1 = val1, col2 = val2
    static UPDATE_REGEX: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)UPDATE\s+\w+\s+SET\s+(.+?)(?:\s+WHERE|$)").unwrap()
    });
    
    let caps = UPDATE_REGEX.captures(sql)?;
    let assignments_str = caps.get(1)?.as_str();
    
    let mut result = Vec::new();
    
    // Split by comma but handle values that might contain commas
    let assignments = split_assignments(assignments_str);
    
    for assignment in assignments {
        let parts: Vec<&str> = assignment.splitn(2, '=').collect();
        if parts.len() == 2 {
            let col_name = parts[0].trim().to_string();
            let value_with_quotes = parts[1].trim().to_string();
            
            // Skip validation for computed expressions or column references
            if is_computed_expression(&value_with_quotes) {
                continue; // Don't validate computed expressions
            }
            
            // Only remove quotes after validation check
            let value = value_with_quotes.trim_matches('\'').to_string();
            result.push((col_name, value));
        }
    }
    
    Some(result)
}


/// Check if a value is a computed expression that shouldn't be validated
fn is_computed_expression(value: &str) -> bool {
    let trimmed = value.trim();
    
    // Skip validation for:
    // 1. Expressions with arithmetic operators (but not leading minus for negative numbers)
    // 2. Function calls
    // 3. Column references
    // 4. CASE expressions
    // 5. Subqueries
    
    // Check for arithmetic operators, but handle negative numbers correctly
    if trimmed.contains('+') || 
       trimmed.contains('*') || 
       trimmed.contains('/') || 
       trimmed.contains('%') {
        return true;
    }
    
    // Check for minus sign that's not at the beginning (indicating subtraction)
    if let Some(minus_pos) = trimmed.find('-') {
        if minus_pos > 0 {
            return true; // It's subtraction, not a negative number
        }
    }
    
    // Check for function calls, subqueries, or CASE expressions
    if trimmed.contains('(') || 
       trimmed.contains("CASE") ||
       trimmed.contains("SELECT") {
        return true;
    }
    
    // Check if it's a literal value (quoted string or simple number)
    if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        return false; // It's a quoted string literal
    }
    
    // Check if it's a simple number (including negative numbers)
    if trimmed.chars().all(|c| c.is_ascii_digit() || c == '.' || c == '-') {
        // Make sure there's at most one minus sign and it's at the beginning
        let minus_count = trimmed.chars().filter(|&c| c == '-').count();
        if minus_count <= 1 && (minus_count == 0 || trimmed.starts_with('-')) {
            return false; // It's a simple number literal
        }
    }
    
    // Everything else is considered a computed expression or column reference
    true
}

/// Split comma-separated assignments, handling quoted strings
fn split_assignments(assignments_str: &str) -> Vec<String> {
    let mut assignments = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut quote_char = ' ';
    let mut paren_depth = 0;
    
    for ch in assignments_str.chars() {
        match ch {
            '\'' | '"' if !in_quotes => {
                in_quotes = true;
                quote_char = ch;
                current.push(ch);
            }
            ch if ch == quote_char && in_quotes => {
                in_quotes = false;
                current.push(ch);
            }
            '(' if !in_quotes => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' if !in_quotes => {
                paren_depth -= 1;
                current.push(ch);
            }
            ',' if !in_quotes && paren_depth == 0 => {
                assignments.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    
    if !current.is_empty() {
        assignments.push(current.trim().to_string());
    }
    
    assignments
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_is_computed_expression() {
        // Computed expressions that should be skipped
        assert!(is_computed_expression("amount * 1.1"));
        assert!(is_computed_expression("price + 100"));
        assert!(is_computed_expression("quantity / 2"));
        assert!(is_computed_expression("SUM(amount)"));
        assert!(is_computed_expression("CASE WHEN amount > 100 THEN 'high' ELSE 'low' END"));
        assert!(is_computed_expression("column_name")); // Column reference
        assert!(is_computed_expression("(SELECT COUNT(*) FROM table)"));
        
        // Literal values that should be validated
        assert!(!is_computed_expression("123.45"));
        assert!(!is_computed_expression("'quoted string'"));
        assert!(!is_computed_expression("0"));
        assert!(!is_computed_expression("-456.78"));
    }
    
    #[test]
    fn test_parse_update_statement_with_computed_expressions() {
        // Test that computed expressions are filtered out
        let sql = "UPDATE table SET amount = amount * 1.1, name = 'test', quantity = 5";
        let result = parse_update_statement(sql).unwrap();
        
        // Should only contain literal assignments, not computed expressions
        // Now that we preserve quotes for detection, 'test' should be recognized as a literal
        assert_eq!(result.len(), 2);
        assert!(result.contains(&("name".to_string(), "test".to_string())));
        assert!(result.contains(&("quantity".to_string(), "5".to_string())));
        
        // Should not contain the computed expression
        assert!(!result.iter().any(|(col, _)| col == "amount"));
    }
}