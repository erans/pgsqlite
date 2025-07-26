/// Proper SQL parameter parsing that respects string literal boundaries
use std::collections::HashSet;

/// Parse SQL query to find parameter placeholders ($1, $2, etc. and %(name)s) while respecting string literals
pub struct ParameterParser;

impl ParameterParser {
    /// Count the number of unique parameters in a SQL query, ignoring $ characters inside string literals
    pub fn count_parameters(sql: &str) -> usize {
        Self::find_parameters(sql).len()
    }
    
    /// Count the number of unique Python-style parameters (%(name)s) in a SQL query
    pub fn count_python_parameters(sql: &str) -> usize {
        Self::find_python_parameters(sql).len()
    }
    
    /// Find all parameter placeholders in a SQL query, ignoring $ characters inside string literals
    pub fn find_parameters(sql: &str) -> Vec<usize> {
        let mut parameters = HashSet::new();
        let mut chars = sql.char_indices().peekable();
        
        while let Some((i, ch)) = chars.next() {
            match ch {
                // Handle single-quoted string literals
                '\'' => {
                    // Skip everything until the closing quote (handling escaped quotes)
                    while let Some((_, inner_ch)) = chars.next() {
                        if inner_ch == '\'' {
                            // Check if this is an escaped quote ('')
                            if let Some((_, next_ch)) = chars.peek() {
                                if *next_ch == '\'' {
                                    // Escaped quote, skip it and continue in string
                                    chars.next();
                                    continue;
                                }
                            }
                            // End of string literal
                            break;
                        }
                    }
                }
                // Handle double-quoted identifiers (should not contain parameters anyway)
                '"' => {
                    // Skip everything until the closing quote (handling escaped quotes)
                    while let Some((_, inner_ch)) = chars.next() {
                        if inner_ch == '"' {
                            // Check if this is an escaped quote ("")
                            if let Some((_, next_ch)) = chars.peek() {
                                if *next_ch == '"' {
                                    // Escaped quote, skip it and continue in identifier
                                    chars.next();
                                    continue;
                                }
                            }
                            // End of quoted identifier
                            break;
                        }
                    }
                }
                // Handle potential parameter placeholder
                '$' => {
                    // Look ahead to see if this is followed by digits
                    let mut param_num = String::new();
                    let _start_pos = i + 1;
                    
                    // Peek at following characters to collect digits
                    while let Some((_, next_ch)) = chars.peek() {
                        if next_ch.is_ascii_digit() {
                            param_num.push(*next_ch);
                            chars.next(); // consume the digit
                        } else {
                            break;
                        }
                    }
                    
                    // If we found digits, this is a parameter
                    if !param_num.is_empty() {
                        if let Ok(param_number) = param_num.parse::<usize>() {
                            if param_number > 0 && param_number <= 99 {
                                parameters.insert(param_number);
                            }
                        }
                    }
                }
                _ => {
                    // Regular character, continue
                }
            }
        }
        
        let mut sorted_params: Vec<usize> = parameters.into_iter().collect();
        sorted_params.sort();
        sorted_params
    }
    
    /// Replace parameter placeholders with values, respecting string literal boundaries
    pub fn substitute_parameters(sql: &str, values: &[String]) -> Result<String, String> {
        let mut result = String::new();
        let mut chars = sql.char_indices().peekable();
        
        while let Some((_i, ch)) = chars.next() {
            match ch {
                // Handle single-quoted string literals - copy verbatim
                '\'' => {
                    result.push(ch);
                    // Copy everything until the closing quote (handling escaped quotes)
                    while let Some((_, inner_ch)) = chars.next() {
                        result.push(inner_ch);
                        if inner_ch == '\'' {
                            // Check if this is an escaped quote ('')
                            if let Some((_, next_ch)) = chars.peek() {
                                if *next_ch == '\'' {
                                    // Escaped quote, copy it and continue in string
                                    result.push(chars.next().unwrap().1);
                                    continue;
                                }
                            }
                            // End of string literal
                            break;
                        }
                    }
                }
                // Handle double-quoted identifiers - copy verbatim
                '"' => {
                    result.push(ch);
                    // Copy everything until the closing quote (handling escaped quotes)
                    while let Some((_, inner_ch)) = chars.next() {
                        result.push(inner_ch);
                        if inner_ch == '"' {
                            // Check if this is an escaped quote ("")
                            if let Some((_, next_ch)) = chars.peek() {
                                if *next_ch == '"' {
                                    // Escaped quote, copy it and continue in identifier
                                    result.push(chars.next().unwrap().1);
                                    continue;
                                }
                            }
                            // End of quoted identifier
                            break;
                        }
                    }
                }
                // Handle potential parameter placeholder
                '$' => {
                    // Look ahead to see if this is followed by digits
                    let mut param_num = String::new();
                    let mut collected_chars = Vec::new();
                    
                    // Peek at following characters to collect digits
                    while let Some((_, next_ch)) = chars.peek() {
                        if next_ch.is_ascii_digit() {
                            collected_chars.push(*next_ch);
                            param_num.push(*next_ch);
                            chars.next(); // consume the digit
                        } else {
                            break;
                        }
                    }
                    
                    // If we found digits, this might be a parameter
                    if !param_num.is_empty() {
                        if let Ok(param_number) = param_num.parse::<usize>() {
                            if param_number > 0 && param_number <= values.len() {
                                // Valid parameter number, substitute with value
                                result.push_str(&values[param_number - 1]);
                                continue;
                            }
                        }
                    }
                    
                    // Not a valid parameter, copy the $ and digits as-is
                    result.push('$');
                    for collected_char in collected_chars {
                        result.push(collected_char);
                    }
                }
                _ => {
                    // Regular character, copy as-is
                    result.push(ch);
                }
            }
        }
        
        Ok(result)
    }
    
    /// Find all Python-style parameter placeholders (%(name)s) in a SQL query, ignoring them inside string literals
    pub fn find_python_parameters(sql: &str) -> Vec<String> {
        let mut parameters = HashSet::new();
        let mut chars = sql.char_indices().peekable();
        
        while let Some((_i, ch)) = chars.next() {
            match ch {
                // Handle single-quoted string literals
                '\'' => {
                    // Skip everything until the closing quote (handling escaped quotes)
                    while let Some((_, inner_ch)) = chars.next() {
                        if inner_ch == '\'' {
                            // Check if this is an escaped quote ('')
                            if let Some((_, next_ch)) = chars.peek() {
                                if *next_ch == '\'' {
                                    // Escaped quote, skip it and continue in string
                                    chars.next();
                                    continue;
                                }
                            }
                            // End of string literal
                            break;
                        }
                    }
                }
                // Handle double-quoted identifiers
                '"' => {
                    // Skip everything until the closing quote (handling escaped quotes)
                    while let Some((_, inner_ch)) = chars.next() {
                        if inner_ch == '"' {
                            // Check if this is an escaped quote ("")
                            if let Some((_, next_ch)) = chars.peek() {
                                if *next_ch == '"' {
                                    // Escaped quote, skip it and continue in identifier
                                    chars.next();
                                    continue;
                                }
                            }
                            // End of quoted identifier
                            break;
                        }
                    }
                }
                // Handle potential Python-style parameter %(name)s
                '%' => {
                    // Look ahead to see if this is followed by (name)s pattern
                    if let Some((_, next_ch)) = chars.peek() {
                        if *next_ch == '(' {
                            chars.next(); // consume the '('
                            
                            // Collect parameter name
                            let mut param_name = String::new();
                            let mut found_closing = false;
                            
                            while let Some((_, name_ch)) = chars.next() {
                                if name_ch == ')' {
                                    found_closing = true;
                                    break;
                                } else if name_ch.is_alphanumeric() || name_ch == '_' {
                                    param_name.push(name_ch);
                                } else {
                                    // Invalid character in parameter name
                                    break;
                                }
                            }
                            
                            // Check if we have )s after the parameter name
                            if found_closing && !param_name.is_empty() {
                                if let Some((_, s_ch)) = chars.peek() {
                                    if *s_ch == 's' {
                                        chars.next(); // consume the 's'
                                        parameters.insert(param_name);
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {
                    // Regular character, continue
                }
            }
        }
        
        let mut sorted_params: Vec<String> = parameters.into_iter().collect();
        sorted_params.sort();
        sorted_params
    }
    
    /// Replace Python-style parameter placeholders with values, respecting string literal boundaries
    pub fn substitute_python_parameters(sql: &str, values: &std::collections::HashMap<String, String>) -> Result<String, String> {
        let mut result = String::new();
        let mut chars = sql.char_indices().peekable();
        
        while let Some((_i, ch)) = chars.next() {
            match ch {
                // Handle single-quoted string literals - copy verbatim
                '\'' => {
                    result.push(ch);
                    // Copy everything until the closing quote (handling escaped quotes)
                    while let Some((_, inner_ch)) = chars.next() {
                        result.push(inner_ch);
                        if inner_ch == '\'' {
                            // Check if this is an escaped quote ('')
                            if let Some((_, next_ch)) = chars.peek() {
                                if *next_ch == '\'' {
                                    // Escaped quote, copy it and continue in string
                                    result.push(chars.next().unwrap().1);
                                    continue;
                                }
                            }
                            // End of string literal
                            break;
                        }
                    }
                }
                // Handle double-quoted identifiers - copy verbatim
                '"' => {
                    result.push(ch);
                    // Copy everything until the closing quote (handling escaped quotes)
                    while let Some((_, inner_ch)) = chars.next() {
                        result.push(inner_ch);
                        if inner_ch == '"' {
                            // Check if this is an escaped quote ("")
                            if let Some((_, next_ch)) = chars.peek() {
                                if *next_ch == '"' {
                                    // Escaped quote, copy it and continue in identifier
                                    result.push(chars.next().unwrap().1);
                                    continue;
                                }
                            }
                            // End of quoted identifier
                            break;
                        }
                    }
                }
                // Handle potential Python-style parameter %(name)s
                '%' => {
                    // Look ahead to see if this is followed by (name)s pattern
                    if let Some((_, next_ch)) = chars.peek() {
                        if *next_ch == '(' {
                            chars.next(); // consume the '('
                            
                            // Collect parameter name
                            let mut param_name = String::new();
                            let mut found_closing = false;
                            let mut collected_chars = Vec::new();
                            
                            while let Some((_, name_ch)) = chars.next() {
                                collected_chars.push(name_ch);
                                if name_ch == ')' {
                                    found_closing = true;
                                    break;
                                } else if name_ch.is_alphanumeric() || name_ch == '_' {
                                    param_name.push(name_ch);
                                } else {
                                    // Invalid character in parameter name
                                    break;
                                }
                            }
                            
                            // Check if we have )s after the parameter name
                            if found_closing && !param_name.is_empty() {
                                if let Some((_, s_ch)) = chars.peek() {
                                    if *s_ch == 's' {
                                        chars.next(); // consume the 's'
                                        
                                        // Try to substitute with value
                                        if let Some(value) = values.get(&param_name) {
                                            result.push_str(value);
                                            continue;
                                        }
                                    }
                                }
                            }
                            
                            // Not a valid parameter or no substitution, copy as-is
                            result.push('%');
                            result.push('(');
                            for collected_char in collected_chars {
                                result.push(collected_char);
                            }
                        } else {
                            // Just a regular %, copy as-is
                            result.push(ch);
                        }
                    } else {
                        // Just a regular %, copy as-is
                        result.push(ch);
                    }
                }
                _ => {
                    // Regular character, copy as-is
                    result.push(ch);
                }
            }
        }
        
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parameter_counting() {
        // Simple parameter counting
        assert_eq!(ParameterParser::count_parameters("SELECT * FROM users WHERE id = $1"), 1);
        assert_eq!(ParameterParser::count_parameters("SELECT * FROM users WHERE id = $1 AND name = $2"), 2);
        
        // Parameters in string literals should be ignored
        assert_eq!(ParameterParser::count_parameters("SELECT data->>'$.items[0]' FROM users"), 0);
        assert_eq!(ParameterParser::count_parameters("SELECT data->>'$1' FROM users"), 0);
        
        // Mixed case: real parameter and $ in string literal
        assert_eq!(ParameterParser::count_parameters("SELECT data->>'$.items[0]' FROM users WHERE id = $1"), 1);
        assert_eq!(ParameterParser::count_parameters("SELECT data->>'$1' FROM users WHERE id = $2"), 1);
    }
    
    #[test]
    fn test_parameter_finding() {
        let params = ParameterParser::find_parameters("SELECT * FROM users WHERE id = $1 AND name = $3");
        assert_eq!(params, vec![1, 3]);
        
        // Parameters in string literals should be ignored
        let params = ParameterParser::find_parameters("SELECT data->>'$1' FROM users WHERE id = $2");
        assert_eq!(params, vec![2]);
    }
    
    #[test]
    fn test_parameter_substitution() {
        // Simple substitution
        let result = ParameterParser::substitute_parameters(
            "SELECT * FROM users WHERE id = $1",
            &["42".to_string()]
        ).unwrap();
        assert_eq!(result, "SELECT * FROM users WHERE id = 42");
        
        // $ in string literals should NOT be substituted
        let result = ParameterParser::substitute_parameters(
            "SELECT data->>'$.items[0]' FROM users WHERE id = $1",
            &["42".to_string()]
        ).unwrap();
        assert_eq!(result, "SELECT data->>'$.items[0]' FROM users WHERE id = 42");
        
        // $ that looks like parameter in string should NOT be substituted
        let result = ParameterParser::substitute_parameters(
            "SELECT data->>'$1' FROM users WHERE id = $2",
            &["value1".to_string(), "42".to_string()]
        ).unwrap();
        assert_eq!(result, "SELECT data->>'$1' FROM users WHERE id = 42");
    }
    
    #[test]
    fn test_escaped_quotes() {
        // Handle escaped quotes in string literals
        let result = ParameterParser::substitute_parameters(
            "SELECT 'It''s a test with $1' FROM users WHERE id = $1",
            &["42".to_string()]
        ).unwrap();
        assert_eq!(result, "SELECT 'It''s a test with $1' FROM users WHERE id = 42");
    }
    
    #[test]
    fn test_double_quoted_identifiers() {
        // $ in double-quoted identifiers should not be treated as parameters
        let params = ParameterParser::find_parameters("SELECT \"column_$1\" FROM users WHERE id = $2");
        assert_eq!(params, vec![2]);
        
        let result = ParameterParser::substitute_parameters(
            "SELECT \"column_$1\" FROM users WHERE id = $2",
            &["unused".to_string(), "42".to_string()]
        ).unwrap();
        assert_eq!(result, "SELECT \"column_$1\" FROM users WHERE id = 42");
    }
    
    #[test]
    fn test_json_path_dollar_signs() {
        // This test specifically addresses the issue described in the task
        
        // Test 1: JSON path with $ should not be counted as parameter
        let count = ParameterParser::count_parameters("SELECT data->>'$.items[0]' FROM users");
        assert_eq!(count, 0, "JSON path '$.items[0]' should not be counted as parameter");
        
        // Test 2: JSON path that looks like parameter number should not be counted
        let count = ParameterParser::count_parameters("SELECT data->>'$1' FROM users");
        assert_eq!(count, 0, "JSON path '$1' should not be counted as parameter");
        
        // Test 3: Mix of real parameter and JSON path
        let count = ParameterParser::count_parameters("SELECT data->>'$.items[0]' FROM users WHERE id = $1");
        assert_eq!(count, 1, "Should count only the real parameter $1, not the JSON path");
        
        let params = ParameterParser::find_parameters("SELECT data->>'$.items[0]' FROM users WHERE id = $1");
        assert_eq!(params, vec![1], "Should find only parameter $1");
        
        // Test 4: Substitution should not affect JSON paths
        let result = ParameterParser::substitute_parameters(
            "SELECT data->>'$.items[0]' FROM users WHERE id = $1",
            &["42".to_string()]
        ).unwrap();
        assert_eq!(result, "SELECT data->>'$.items[0]' FROM users WHERE id = 42");
        
        // Test 5: Complex JSON path expressions
        let count = ParameterParser::count_parameters(
            "SELECT json_extract(config, '$.nested.array[0].value') FROM table WHERE json_extract(data, '$.id') = $1"
        );
        assert_eq!(count, 1, "Should count only the real parameter, not JSON paths");
        
        // Test 6: Multiple JSON paths with various $ patterns
        let count = ParameterParser::count_parameters(
            "SELECT json_extract(a, '$.x'), json_extract(b, '$[0]'), json_extract(c, '$.y.z') FROM t WHERE id = $1 AND status = $2"
        );
        assert_eq!(count, 2, "Should count only real parameters $1 and $2");
        
        // Test 7: JSON path substitution should preserve paths exactly
        let result = ParameterParser::substitute_parameters(
            "SELECT json_extract(config, '$.items[$1]') FROM users WHERE id = $2",
            &["should_not_replace".to_string(), "42".to_string()]
        ).unwrap();
        assert_eq!(result, "SELECT json_extract(config, '$.items[$1]') FROM users WHERE id = 42",
                  "JSON path should not be modified, only real parameter should be substituted");
    }
    
    #[test]
    fn test_python_parameter_parsing() {
        // Test finding Python-style parameters
        let params = ParameterParser::find_python_parameters("SELECT * FROM users WHERE id = %(user_id)s");
        assert_eq!(params, vec!["user_id"]);
        
        let params = ParameterParser::find_python_parameters("INSERT INTO users (name, email) VALUES (%(name)s, %(email)s)");
        assert_eq!(params, vec!["email", "name"]); // Sorted alphabetically
        
        // Test complex SQLAlchemy pattern
        let sql = "INSERT INTO categories (name, description, created_at) SELECT p0::VARCHAR, p1::TEXT, p2::TIMESTAMP WITHOUT TIME ZONE FROM (VALUES (%(name__0)s, %(description__0)s, %(created_at__0)s, 0), (%(name__1)s, %(description__1)s, %(created_at__1)s, 1)) AS imp_sen(p0, p1, p2, sen_counter) ORDER BY sen_counter";
        let params = ParameterParser::find_python_parameters(sql);
        assert_eq!(params, vec!["created_at__0", "created_at__1", "description__0", "description__1", "name__0", "name__1"]);
        
        // Test parameters in string literals should be ignored
        let params = ParameterParser::find_python_parameters("SELECT 'test %(param)s' FROM users WHERE id = %(real_param)s");
        assert_eq!(params, vec!["real_param"]);
    }
    
    #[test]
    fn test_python_parameter_substitution() {
        let mut values = std::collections::HashMap::new();
        values.insert("user_id".to_string(), "42".to_string());
        values.insert("name".to_string(), "'John Doe'".to_string());
        
        // Simple substitution
        let result = ParameterParser::substitute_python_parameters(
            "SELECT * FROM users WHERE id = %(user_id)s",
            &values
        ).unwrap();
        assert_eq!(result, "SELECT * FROM users WHERE id = 42");
        
        // Multiple parameters
        let result = ParameterParser::substitute_python_parameters(
            "INSERT INTO users (name, id) VALUES (%(name)s, %(user_id)s)",
            &values
        ).unwrap();
        assert_eq!(result, "INSERT INTO users (name, id) VALUES ('John Doe', 42)");
        
        // Parameters in string literals should NOT be substituted
        let result = ParameterParser::substitute_python_parameters(
            "SELECT 'test %(user_id)s' FROM users WHERE id = %(user_id)s",
            &values
        ).unwrap();
        assert_eq!(result, "SELECT 'test %(user_id)s' FROM users WHERE id = 42");
    }
    
    #[test]
    fn test_python_parameter_counting() {
        assert_eq!(ParameterParser::count_python_parameters("SELECT * FROM users WHERE id = %(user_id)s"), 1);
        assert_eq!(ParameterParser::count_python_parameters("INSERT INTO users (name, email) VALUES (%(name)s, %(email)s)"), 2);
        
        // Parameters in string literals should be ignored
        assert_eq!(ParameterParser::count_python_parameters("SELECT 'test %(param)s' FROM users"), 0);
        assert_eq!(ParameterParser::count_python_parameters("SELECT 'test %(param)s' FROM users WHERE id = %(real_param)s"), 1);
    }
}