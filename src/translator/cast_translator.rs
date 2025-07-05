use crate::metadata::EnumMetadata;
use rusqlite::Connection;

/// Translates PostgreSQL cast syntax to SQLite-compatible syntax
pub struct CastTranslator;

impl CastTranslator {
    /// Quick check if translation is needed (avoiding uppercase allocation)
    #[inline]
    pub fn needs_translation(query: &str) -> bool {
        // Fast path: check for :: first (most common cast syntax)
        if query.contains("::") {
            // Make sure it's not inside a string literal (for IPv6)
            return !Self::all_casts_in_strings(query);
        }
        
        // Slower path: check for CAST (less common, needs case-insensitive check)
        // Use manual case-insensitive search to avoid allocation
        Self::contains_cast_keyword(query)
    }
    
    /// Check if all :: occurrences are inside string literals
    #[inline]
    fn all_casts_in_strings(query: &str) -> bool {
        let mut in_string = false;
        let mut prev_char = '\0';
        
        for (i, ch) in query.chars().enumerate() {
            match ch {
                '\'' if prev_char != '\\' => in_string = !in_string,
                ':' if !in_string && query[i..].starts_with("::") => return false,
                _ => {}
            }
            prev_char = ch;
        }
        
        true
    }
    
    /// Case-insensitive check for CAST keyword without allocation
    #[inline]
    fn contains_cast_keyword(query: &str) -> bool {
        let bytes = query.as_bytes();
        let cast_bytes = b"CAST";
        
        if bytes.len() < cast_bytes.len() {
            return false;
        }
        
        for i in 0..=(bytes.len() - cast_bytes.len()) {
            if bytes[i..i + cast_bytes.len()].eq_ignore_ascii_case(cast_bytes) {
                // Check if it's followed by '(' to avoid matching words like "CASTLE"
                if i + cast_bytes.len() < bytes.len() && bytes[i + cast_bytes.len()] == b'(' {
                    return true;
                }
            }
        }
        
        false
    }
    
    /// Translate a query containing PostgreSQL cast syntax
    pub fn translate_query(query: &str, conn: Option<&Connection>) -> String {
        // Check translation cache first
        if let Some(cached) = crate::cache::global_translation_cache().get(query) {
            return cached;
        }
        
        // Handle both :: and CAST syntax
        let mut result = query.to_string();
        
        // First handle CAST syntax
        result = Self::translate_cast_syntax(&result, conn);
        
        // Then handle :: cast syntax
        let mut search_from = 0;
        let mut iterations = 0;
        const MAX_ITERATIONS: usize = 100;
        
        while search_from < result.len() && iterations < MAX_ITERATIONS {
            iterations += 1;
            let remaining = &result[search_from..];
            let cast_pos_offset = match remaining.find("::") {
                Some(pos) => pos,
                None => break,
            };
            let cast_pos = search_from + cast_pos_offset;
            
            // Check if this :: is inside a string literal (for IPv6 addresses)
            if Self::is_inside_string(&result, cast_pos) {
                search_from = cast_pos + 2;
                if search_from > result.len() {
                    break;
                }
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
                        format!("CAST({} AS TEXT)", clean_expr)
                    } else {
                        clean_expr.to_string()
                    }
                } else {
                    // For other types, check if SQLite supports them
                    let sqlite_type = Self::postgres_to_sqlite_type(type_name);
                    // If postgres_to_sqlite_type returns TEXT and the original type is not a text type,
                    // it means SQLite doesn't know this type
                    if sqlite_type == "TEXT" && !matches!(type_name.to_uppercase().as_str(), "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER VARYING") {
                        // Unknown type, just return the expression
                        expr.to_string()
                    } else if sqlite_type == type_name.to_uppercase().as_str() {
                        // Same type name, use CAST
                        format!("CAST({} AS {})", expr, type_name)
                    } else {
                        // Use SQLite type
                        format!("CAST({} AS {})", expr, sqlite_type)
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
                        format!("CAST({} AS TEXT)", clean_expr)
                    } else {
                        clean_expr.to_string()
                    }
                } else {
                    format!("CAST({} AS {})", expr, type_name)
                }
            };
            
            // If we trimmed a paren, add it back after the CAST
            let final_replacement = if trimmed_paren {
                format!("{})", translated_cast)
            } else {
                translated_cast
            };
            
            // Replace the PostgreSQL cast with the translated version
            result.replace_range(expr_start..cast_pos + 2 + type_end, &final_replacement);
            
            // Update search position to after the replacement
            let new_search_from = expr_start + final_replacement.len();
            
            // Ensure we always move forward to avoid infinite loops
            if new_search_from <= search_from {
                search_from = cast_pos + 2; // Move past the :: we just processed
            } else {
                search_from = new_search_from;
            }
            
            // Ensure we don't go past the end of the string
            if search_from >= result.len() {
                break;
            }
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
            if paren_depth == 0 {
                if ch == b' ' || ch == b',' || ch == b'(' || ch == b'=' || ch == b'<' || ch == b'>' {
                    return i + 1;
                }
            }
        }
        
        0
    }
    
    /// Find the end of a type name after ::
    fn find_type_end(after: &str) -> usize {
        let bytes = after.as_bytes();
        
        for i in 0..bytes.len() {
            let ch = bytes[i];
            
            // Type name ends at these characters
            if ch == b' ' || ch == b',' || ch == b')' || ch == b';' || ch == b'=' || 
               ch == b'<' || ch == b'>' || ch == b'+' || ch == b'-' || ch == b'*' || 
               ch == b'/' || ch == b'|' || ch == b'&' {
                return i;
            }
            
            // Handle chained casts like ::type1::type2
            if i > 0 && ch == b':' && i + 1 < bytes.len() && bytes[i + 1] == b':' {
                return i;
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
    fn translate_enum_cast(expr: &str, _type_name: &str, _conn: &Connection) -> String {
        // For ENUM casts, we just return the expression as-is
        // The CHECK constraint on the column will validate the value at runtime
        // This is consistent with how PostgreSQL handles ENUM casts
        expr.to_string()
    }
    
    /// Check if an expression might need explicit TEXT casting
    fn might_need_text_cast(expr: &str) -> bool {
        // If it's a column name (not a literal), it might be a special type
        // that needs explicit casting
        !expr.starts_with('\'') && !expr.starts_with('"') && !expr.parse::<f64>().is_ok()
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
        match pg_type.to_uppercase().as_str() {
            "INTEGER" | "INT" | "INT4" | "INT8" | "BIGINT" | "SMALLINT" | "INT2" => "INTEGER",
            "REAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => "REAL",
            "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER VARYING" => "TEXT",
            "BYTEA" => "BLOB",
            "BOOLEAN" | "BOOL" => "INTEGER", // SQLite uses 0/1 for boolean
            "NUMERIC" | "DECIMAL" => "TEXT", // Store as text for precision
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
                    // For ENUM types, just return the expression
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
                        format!("CAST({} AS TEXT)", clean_expr)
                    } else {
                        clean_expr.to_string()
                    }
                } else {
                    // Check if SQLite supports this type
                    let sqlite_type = Self::postgres_to_sqlite_type(type_name);
                    if sqlite_type == "TEXT" && !matches!(type_name.to_uppercase().as_str(), "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER VARYING") {
                        // Unknown type, just return the expression
                        expr.to_string()
                    } else {
                        // Keep the CAST with SQLite type
                        format!("CAST({} AS {})", expr, sqlite_type)
                    }
                }
            } else {
                // No connection, keep the CAST
                format!("CAST({} AS {})", expr, type_name)
            };
            
            // Replace the CAST expression
            result.replace_range(cast_start..=cast_end, &translated);
            
            // Update search position to after the replacement
            search_from = cast_start + translated.len();
        }
        
        result
    }
}