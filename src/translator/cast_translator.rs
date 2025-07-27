use crate::metadata::EnumMetadata;
use rusqlite::Connection;
use super::SimdCastSearch;

/// Translates PostgreSQL cast syntax to SQLite-compatible syntax
pub struct CastTranslator;

impl CastTranslator {
    /// Quick check if translation is needed (using SIMD acceleration)
    #[inline]
    pub fn needs_translation(query: &str) -> bool {
        // Fast path: check for :: first using SIMD (most common cast syntax)
        if SimdCastSearch::has_cast_outside_strings(query) {
            return true;
        }
        
        // Slower path: check for CAST using SIMD (less common)
        SimdCastSearch::contains_cast_keyword(query)
    }
    
    /// Translate a query containing PostgreSQL cast syntax
    pub fn translate_query(query: &str, conn: Option<&Connection>) -> String {
        let result = Self::translate_query_with_depth(query, conn, 0);
        if query.contains("inactive") && query.contains("status") {
            eprintln!("CastTranslator: Translating enum cast query");
            eprintln!("  Original: {}", query);
            eprintln!("  Result: {}", result);
        }
        if query.contains("::mood::text") || query.contains("::text") {
            eprintln!("CastTranslator: Translating double cast query");
            eprintln!("  Original: {}", query);
            eprintln!("  Result: {}", result);
        }
        result
    }
    
    /// Internal translation method with recursion depth tracking
    fn translate_query_with_depth(query: &str, conn: Option<&Connection>, depth: usize) -> String {
        // Prevent infinite recursion
        const MAX_RECURSION_DEPTH: usize = 10;
        if depth >= MAX_RECURSION_DEPTH {
            return query.to_string();
        }
        
        // Check translation cache first
        if let Some(cached) = crate::cache::global_translation_cache().get(query) {
            return cached;
        }
        
        // Handle both :: and CAST syntax
        let mut result = query.to_string();
        
        // First handle CAST syntax
        result = Self::translate_cast_syntax(&result, conn);
        
        // Then handle :: cast syntax using SIMD to find all positions
        let cast_positions = SimdCastSearch::find_all_double_colons(&result);
        let mut iterations = 0;
        const MAX_ITERATIONS: usize = 100;
        
        for &cast_pos in &cast_positions {
            if iterations >= MAX_ITERATIONS {
                break;
            }
            iterations += 1;
            
            // Check if this :: is inside a string literal (for IPv6 addresses)
            if Self::is_inside_string(&result, cast_pos) {
                continue;
            }
            
            // Find the start of the expression before ::
            let before = &result[..cast_pos];
            let expr_start = Self::find_expression_start(before);
            
            // Find the end of the type after ::
            let after = &result[cast_pos + 2..];
            let type_end = Self::find_type_end(after);
            
            // Extract expression and type
            let mut expr = &result[expr_start..cast_pos];
            let type_name = &result[cast_pos + 2..cast_pos + 2 + type_end];
            
            // Fix for extra closing paren
            // This happens when we have (expr)::type and extract starting after the (
            let mut trimmed_paren = false;
            if expr.ends_with(')') && !expr.starts_with('(') {
                let open_count = expr.matches('(').count();
                let close_count = expr.matches(')').count();
                if close_count > open_count {
                    expr = &expr[..expr.len()-1];
                    trimmed_paren = true;
                }
            }
            
            // Check if this is an ENUM type cast
            let translated_cast = if let Some(conn) = conn {
                if Self::is_enum_type(conn, type_name) {
                    // For ENUM types, we validate the value
                    Self::translate_enum_cast(expr, type_name, conn)
                } else if type_name.eq_ignore_ascii_case("text") {
                    // For text cast, we need to handle parenthesized expressions carefully
                    // Remove outer parentheses if present to avoid (CAST(...))
                    let clean_expr = if expr.starts_with('(') && expr.ends_with(')') {
                        &expr[1..expr.len()-1]
                    } else {
                        expr
                    };
                    
                    // Always preserve cast for aggregate functions or complex expressions
                    if clean_expr.contains('(') || Self::is_aggregate_function(clean_expr) || Self::might_need_text_cast(clean_expr) {
                        // Check if this is a subquery that needs extra parentheses
                        if clean_expr.trim_start().starts_with("SELECT") {
                            format!("CAST(({clean_expr}) AS TEXT)")
                        } else {
                            format!("CAST({clean_expr} AS TEXT)")
                        }
                    } else {
                        clean_expr.to_string()
                    }
                } else {
                    // For other types, check if SQLite supports them
                    let sqlite_type = Self::postgres_to_sqlite_type(type_name);
                    
                    // Special handling for timestamp/date/time types
                    let upper_type = type_name.to_uppercase();
                    match upper_type.as_str() {
                        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => {
                            // Use pgsqlite's timestamp conversion function
                            format!("pg_timestamp_from_text({expr})")
                        }
                        "DATE" => {
                            // Use pgsqlite's date conversion function
                            format!("pg_date_from_text({expr})")
                        }
                        "TIME" | "TIME WITHOUT TIME ZONE" | "TIME WITH TIME ZONE" | "TIMETZ" => {
                            // Use pgsqlite's time conversion function
                            format!("pg_time_from_text({expr})")
                        }
                        _ => {
                            // For non-datetime types, use regular cast logic
                            // If postgres_to_sqlite_type returns TEXT and the original type is not a text type,
                            // it means SQLite doesn't know this type
                            if sqlite_type == "TEXT" && !matches!(upper_type.as_str(), "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER VARYING") {
                                // Unknown type, just return the expression
                                expr.to_string()
                            } else if sqlite_type == upper_type.as_str() {
                                // Same type name, use CAST
                                // Check if expression is a subquery
                                if expr.trim_start().starts_with("SELECT") {
                                    format!("CAST(({expr}) AS {type_name})")
                                } else {
                                    format!("CAST({expr} AS {type_name})")
                                }
                            } else {
                                // Use SQLite type
                                // Check if expression is a subquery
                                if expr.trim_start().starts_with("SELECT") {
                                    format!("CAST(({expr}) AS {sqlite_type})")
                                } else {
                                    format!("CAST({expr} AS {sqlite_type})")
                                }
                            }
                        }
                    }
                }
            } else {
                // No connection, use standard SQL cast
                if type_name.eq_ignore_ascii_case("text") {
                    // Remove outer parentheses if present
                    let clean_expr = if expr.starts_with('(') && expr.ends_with(')') {
                        &expr[1..expr.len()-1]
                    } else {
                        expr
                    };
                    // Keep CAST for expressions with function calls
                    if clean_expr.contains('(') && clean_expr.contains(')') {
                        // Check if this is a subquery that needs extra parentheses
                        if clean_expr.trim_start().starts_with("SELECT") {
                            format!("CAST(({clean_expr}) AS TEXT)")
                        } else {
                            format!("CAST({clean_expr} AS TEXT)")
                        }
                    } else {
                        clean_expr.to_string()
                    }
                } else {
                    // Special handling for timestamp/date/time types even without connection
                    let upper_type = type_name.to_uppercase();
                    match upper_type.as_str() {
                        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => {
                            format!("pg_timestamp_from_text({expr})")
                        }
                        "DATE" => {
                            format!("pg_date_from_text({expr})")
                        }
                        "TIME" | "TIME WITHOUT TIME ZONE" | "TIME WITH TIME ZONE" | "TIMETZ" => {
                            format!("pg_time_from_text({expr})")
                        }
                        _ => {
                            format!("CAST({expr} AS {type_name})")
                        }
                    }
                }
            };
            
            // If we trimmed a paren, add it back after the CAST
            let final_replacement = if trimmed_paren {
                format!("{translated_cast})")
            } else {
                translated_cast
            };
            
            // Build the original cast expression to replace
            let original_cast = format!("{}::{}", expr, type_name);
            
            // Debug log for RETURNING issue
            if query.contains("RETURNING") {
                eprintln!("DEBUG CastTranslator: Processing query with RETURNING");
                eprintln!("  Original: {}", query);
                eprintln!("  Current result: {}", result);
                eprintln!("  Looking to replace '{}' with '{}'", original_cast, final_replacement);
            }
            
            // Find and replace the exact cast expression
            if let Some(pos) = result.find(&original_cast) {
                if query.contains("RETURNING") {
                    eprintln!("DEBUG: Found exact match at position {}", pos);
                    eprintln!("  Replacing '{}' with '{}'", original_cast, final_replacement);
                }
                result.replace_range(pos..pos + original_cast.len(), &final_replacement);
                if query.contains("RETURNING") {
                    eprintln!("  Result after replacement: {}", result);
                }
            } else {
                // Fallback to the old method if exact match fails
                if query.contains("RETURNING") {
                    eprintln!("DEBUG: Fallback replacement");
                    eprintln!("  expr_start: {}, cast_pos: {}, type_end: {}", expr_start, cast_pos, type_end);
                    eprintln!("  Replacing range {}..{}", expr_start, cast_pos + 2 + type_end);
                    eprintln!("  Substring being replaced: '{}'", &result[expr_start..cast_pos + 2 + type_end]);
                }
                result.replace_range(expr_start..cast_pos + 2 + type_end, &final_replacement);
            }
            
            // Since we modified the string, we need to recalculate positions for the next iteration
            // Break and re-find positions (this is safe because we limit iterations)
            break;
        }
        
        // If we made changes, we might need to process more casts
        if iterations > 0 && iterations < MAX_ITERATIONS && result != query {
            // Recursively process any remaining casts with incremented depth
            return Self::translate_query_with_depth(&result, conn, depth + 1);
        }
        
        // Cache the translation if it changed
        if result != query {
            crate::cache::global_translation_cache().insert(query.to_string(), result.clone());
        }
        
        result
    }
    
    /// Check if a position is inside a string literal
    fn is_inside_string(query: &str, pos: usize) -> bool {
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut escaped = false;
        
        for (i, ch) in query.chars().enumerate() {
            if i >= pos {
                break;
            }
            
            if escaped {
                escaped = false;
                continue;
            }
            
            match ch {
                '\\' => escaped = true,
                '\'' if !in_double_quote => in_single_quote = !in_single_quote,
                '"' if !in_single_quote => in_double_quote = !in_double_quote,
                _ => {}
            }
        }
        
        in_single_quote || in_double_quote
    }
    
    /// Find the start of an expression before :: cast
    fn find_expression_start(before: &str) -> usize {
        let bytes = before.as_bytes();
        let mut paren_depth = 0;
        let mut quote_char = None;
        
        // Scan backwards to find expression start
        for i in (0..bytes.len()).rev() {
            let ch = bytes[i];
            
            // Handle quotes
            if quote_char.is_some() {
                if ch == quote_char.unwrap() && (i == 0 || bytes[i-1] != b'\\') {
                    quote_char = None;
                }
                continue;
            }
            
            if ch == b'\'' || ch == b'"' {
                quote_char = Some(ch);
                continue;
            }
            
            // Handle parentheses
            if ch == b')' {
                paren_depth += 1;
            } else if ch == b'(' {
                paren_depth -= 1;
                if paren_depth < 0 {
                    // Found unmatched opening paren - this is the start
                    // Return position after the '('
                    return i + 1;
                }
            }
            
            // If we're not in parentheses, look for expression boundaries
            if paren_depth == 0
                && (ch == b' ' || ch == b',' || ch == b'(' || ch == b'=' || ch == b'<' || ch == b'>') {
                    return i + 1;
                }
        }
        
        0
    }
    
    /// Find the end of a type name after ::
    fn find_type_end(after: &str) -> usize {
        let bytes = after.as_bytes();
        let mut paren_depth = 0;
        
        // Debug for RETURNING issue
        if after.contains("RETURNING") {
            eprintln!("DEBUG find_type_end: after = '{}'", after);
        }
        
        // Check if this starts with a multi-word type pattern
        let upper_after = after.to_uppercase();
        let multiword_types = [
            ("TIMESTAMP WITHOUT TIME ZONE", 27),
            ("TIMESTAMP WITH TIME ZONE", 24),
            ("TIME WITHOUT TIME ZONE", 22), 
            ("TIME WITH TIME ZONE", 19),
            ("DOUBLE PRECISION", 16),
            ("CHARACTER VARYING", 17),
            ("BIT VARYING", 11),
        ];
        
        for (pattern, len) in multiword_types {
            if upper_after.starts_with(pattern) {
                // Make sure this is followed by a word boundary or end of string
                if after.len() == len {
                    // Exact match - this is the end of the string
                    return len;
                } else if let Some(next_char) = after.as_bytes().get(len) {
                    // There's a character after the pattern - make sure it's a word boundary
                    if !next_char.is_ascii_alphanumeric() && *next_char != b'_' {
                        return len;
                    }
                }
            }
        }
        
        // Check for single-word types that might be followed by ) or other delimiters
        let single_word_types = [
            "TIMESTAMP", "DATE", "TIME", "TIMETZ", "TIMESTAMPTZ",
            "INTEGER", "INT", "INT4", "INT8", "INT2", 
            "BIGINT", "SMALLINT", "SERIAL", "BIGSERIAL",
            "TEXT", "VARCHAR", "CHAR", "BOOLEAN", "BOOL",
            "REAL", "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE",
            "NUMERIC", "DECIMAL", "MONEY", "UUID", "JSON", "JSONB",
            "BYTEA", "BIT", "VARBIT", "INTERVAL"
        ];
        
        for type_name in single_word_types {
            if upper_after.starts_with(type_name) {
                let len = type_name.len();
                // Make sure this is followed by a word boundary
                if after.len() == len {
                    if after.contains("RETURNING") {
                        eprintln!("DEBUG: Exact match for type '{}', returning {}", type_name, len);
                    }
                    return len;
                } else if let Some(next_char) = after.as_bytes().get(len) {
                    // Check if next character is not alphanumeric (word boundary)
                    if !next_char.is_ascii_alphanumeric() && *next_char != b'_' {
                        if after.contains("RETURNING") {
                            eprintln!("DEBUG: Type '{}' followed by non-alphanumeric '{}', returning {}", type_name, *next_char as char, len);
                        }
                        return len;
                    }
                }
            }
        }
        
        // Fall back to original logic for single-word types
        for i in 0..bytes.len() {
            let ch = bytes[i];
            
            // Handle parentheses for parameterized types like bit(8), varchar(10)
            if ch == b'(' {
                paren_depth += 1;
                continue;
            } else if ch == b')' {
                if paren_depth > 0 {
                    paren_depth -= 1;
                    continue;
                } else {
                    // Unmatched closing paren - end of type
                    return i;
                }
            }
            
            // If we're not inside parentheses, type name ends at these characters
            if paren_depth == 0 {
                if ch == b' ' || ch == b',' || ch == b';' || ch == b'=' || 
                   ch == b'<' || ch == b'>' || ch == b'+' || ch == b'-' || ch == b'*' || 
                   ch == b'/' || ch == b'|' || ch == b'&' {
                    return i;
                }
                
                // Handle chained casts like ::type1::type2
                if i > 0 && ch == b':' && i + 1 < bytes.len() && bytes[i + 1] == b':' {
                    return i;
                }
            }
        }
        
        after.len()
    }
    
    /// Check if a type name is an ENUM type
    fn is_enum_type(conn: &Connection, type_name: &str) -> bool {
        EnumMetadata::get_enum_type(conn, type_name)
            .unwrap_or(None)
            .is_some()
    }
    
    /// Translate an ENUM cast
    fn translate_enum_cast(expr: &str, type_name: &str, _conn: &Connection) -> String {
        // For ENUM casts, we need to validate the value exists
        // Use a CASE expression that will cause a constraint error for invalid values
        // We use CAST(NULL AS INTEGER) NOT NULL which will fail with "NOT NULL constraint failed"
        
        format!(
            r#"(CASE 
                WHEN EXISTS(SELECT 1 FROM __pgsqlite_enum_values ev JOIN __pgsqlite_enum_types et ON ev.type_oid = et.type_oid WHERE et.type_name = '{}' AND ev.label = {}) 
                THEN (SELECT label FROM __pgsqlite_enum_values ev JOIN __pgsqlite_enum_types et ON ev.type_oid = et.type_oid WHERE et.type_name = '{}' AND ev.label = {})
                ELSE (SELECT CASE WHEN 1=0 THEN 'dummy' ELSE CAST('invalid input value for enum {}: ' || {} AS INTEGER) END)
            END)"#,
            type_name, expr, type_name, expr, type_name, expr
        )
    }
    
    /// Check if an expression might need explicit TEXT casting
    fn might_need_text_cast(expr: &str) -> bool {
        // If it's a column name (not a literal), it might be a special type
        // that needs explicit casting
        !expr.starts_with('\'') && !expr.starts_with('"') && expr.parse::<f64>().is_err()
    }
    
    /// Check if an expression is an aggregate function
    fn is_aggregate_function(expr: &str) -> bool {
        let expr_upper = expr.to_uppercase();
        expr_upper.starts_with("SUM(") || 
        expr_upper.starts_with("AVG(") || 
        expr_upper.starts_with("COUNT(") || 
        expr_upper.starts_with("MIN(") || 
        expr_upper.starts_with("MAX(") ||
        expr_upper.starts_with("(SUM(") ||
        expr_upper.starts_with("(AVG(") ||
        expr_upper.starts_with("(COUNT(") ||
        expr_upper.starts_with("(MIN(") ||
        expr_upper.starts_with("(MAX(")
    }
    
    /// Convert PostgreSQL type names to SQLite type names
    fn postgres_to_sqlite_type(pg_type: &str) -> &'static str {
        // Fast path: check for common simple types first (case-insensitive)
        match pg_type {
            // Exact matches (common case)
            "text" | "TEXT" => return "TEXT",
            "integer" | "INTEGER" => return "INTEGER",
            "int" | "INT" => return "INTEGER",
            "bool" | "BOOL" | "boolean" | "BOOLEAN" => return "INTEGER",
            "real" | "REAL" | "float" | "FLOAT" => return "REAL",
            "bytea" | "BYTEA" => return "BLOB",
            "numeric" | "NUMERIC" | "decimal" | "DECIMAL" => return "TEXT",
            "bit" | "BIT" | "varbit" | "VARBIT" => return "TEXT",
            _ => {}
        }
        
        // Handle parameterized or complex types
        let upper_type = pg_type.to_uppercase();
        let base_type = if let Some(paren_pos) = upper_type.find('(') {
            &upper_type[..paren_pos]
        } else {
            &upper_type
        };
        
        match base_type {
            "INTEGER" | "INT" | "INT4" | "INT8" | "BIGINT" | "SMALLINT" | "INT2" => "INTEGER",
            "REAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => "REAL",
            "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER VARYING" => "TEXT",
            "BYTEA" => "BLOB",
            "BOOLEAN" | "BOOL" => "INTEGER", // SQLite uses 0/1 for boolean
            "NUMERIC" | "DECIMAL" => "TEXT", // Store as text for precision
            "BIT" | "VARBIT" | "BIT VARYING" => "TEXT", // BIT types stored as text strings
            // DateTime types - pgsqlite stores them as INTEGER (microseconds/days)
            "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => "INTEGER",
            "DATE" => "INTEGER",
            "TIME" | "TIME WITHOUT TIME ZONE" | "TIME WITH TIME ZONE" | "TIMETZ" => "INTEGER",
            "INTERVAL" => "INTEGER",
            _ => "TEXT", // Default to TEXT for unknown types
        }
    }
    
    /// Translate CAST(expr AS type) syntax
    fn translate_cast_syntax(query: &str, conn: Option<&Connection>) -> String {
        let mut result = query.to_string();
        
        // Use regex to find CAST expressions
        // Match CAST(expr AS type) pattern
        let mut search_from = 0;
        loop {
            // Find CAST( position (case-insensitive) starting from search_from
            let remaining = &result[search_from..];
            let cast_start_offset = remaining.chars()
                .collect::<String>()
                .to_uppercase()
                .find("CAST(");
            
            let cast_start = match cast_start_offset {
                Some(offset) => search_from + offset,
                None => break,
            };
            
            // Find matching closing parenthesis
            let mut paren_count = 1;
            let mut i = cast_start + 5; // Skip "CAST("
            let cast_content_start = i;
            let mut as_pos = None;
            
            while i < result.len() && paren_count > 0 {
                if result[i..].starts_with('(') {
                    paren_count += 1;
                } else if result[i..].starts_with(')') {
                    paren_count -= 1;
                } else if paren_count == 1 && as_pos.is_none() && result[i..].to_uppercase().starts_with(" AS ") {
                    as_pos = Some(i);
                }
                i += 1;
            }
            
            if paren_count != 0 || as_pos.is_none() {
                // Malformed CAST, skip it
                break;
            }
            
            let cast_end = i - 1; // Position of closing ')'
            let as_position = as_pos.unwrap();
            
            // Extract expression and type
            let expr = result[cast_content_start..as_position].trim();
            let type_name = result[as_position + 4..cast_end].trim();
            
            // Check if this is an ENUM type cast
            let translated = if let Some(conn) = conn {
                if Self::is_enum_type(conn, type_name) {
                    // For ENUM types, use the enum cast translator
                    Self::translate_enum_cast(expr, type_name, conn)
                } else if type_name.eq_ignore_ascii_case("text") {
                    // For text cast, we need to handle parenthesized expressions carefully
                    // Remove outer parentheses if present to avoid (CAST(...))
                    let clean_expr = if expr.starts_with('(') && expr.ends_with(')') {
                        &expr[1..expr.len()-1]
                    } else {
                        expr
                    };
                    
                    // Always preserve cast for aggregate functions or complex expressions
                    if clean_expr.contains('(') || Self::is_aggregate_function(clean_expr) || Self::might_need_text_cast(clean_expr) {
                        // Check if this is a subquery that needs extra parentheses
                        if clean_expr.trim_start().starts_with("SELECT") {
                            format!("CAST(({clean_expr}) AS TEXT)")
                        } else {
                            format!("CAST({clean_expr} AS TEXT)")
                        }
                    } else {
                        clean_expr.to_string()
                    }
                } else {
                    // Check if SQLite supports this type
                    let sqlite_type = Self::postgres_to_sqlite_type(type_name);
                    
                    // Special handling for timestamp/date/time types
                    let upper_type = type_name.to_uppercase();
                    match upper_type.as_str() {
                        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => {
                            // Use pgsqlite's timestamp conversion function
                            format!("pg_timestamp_from_text({expr})")
                        }
                        "DATE" => {
                            // Use pgsqlite's date conversion function
                            format!("pg_date_from_text({expr})")
                        }
                        "TIME" | "TIME WITHOUT TIME ZONE" | "TIME WITH TIME ZONE" | "TIMETZ" => {
                            // Use pgsqlite's time conversion function
                            format!("pg_time_from_text({expr})")
                        }
                        _ => {
                            // For non-datetime types, use regular cast logic
                            if sqlite_type == "TEXT" && !matches!(upper_type.as_str(), "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER VARYING") {
                                // Unknown type, just return the expression
                                expr.to_string()
                            } else {
                                // Keep the CAST with SQLite type
                                // Check if expression is a subquery
                                if expr.trim_start().starts_with("SELECT") {
                                    format!("CAST(({expr}) AS {sqlite_type})")
                                } else {
                                    format!("CAST({expr} AS {sqlite_type})")
                                }
                            }
                        }
                    }
                }
            } else {
                // No connection, keep the CAST
                format!("CAST({expr} AS {type_name})")
            };
            
            // Replace the CAST expression
            result.replace_range(cast_start..=cast_end, &translated);
            
            // Update search position to after the replacement
            search_from = cast_start + translated.len();
        }
        
        result
    }
}