use crate::metadata::EnumMetadata;
use rusqlite::Connection;

/// Translates PostgreSQL cast syntax to SQLite-compatible syntax
pub struct CastTranslator;

impl CastTranslator {
    /// Translate a query containing PostgreSQL cast syntax
    pub fn translate_query(query: &str, conn: Option<&Connection>) -> String {
        
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
            let expr = &result[expr_start..cast_pos];
            let type_name = &result[cast_pos + 2..cast_pos + 2 + type_end];
            
            // Check if this is an ENUM type cast
            let translated_cast = if let Some(conn) = conn {
                if Self::is_enum_type(conn, type_name) {
                    // For ENUM types, we validate the value
                    Self::translate_enum_cast(expr, type_name, conn)
                } else if type_name.eq_ignore_ascii_case("text") {
                    // For text cast, keep it if the expression might be a special type
                    // that needs explicit casting (like MONEY, INET, etc.)
                    if Self::might_need_text_cast(expr) {
                        format!("CAST({} AS TEXT)", expr)
                    } else {
                        expr.to_string()
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
                    expr.to_string()
                } else {
                    format!("CAST({} AS {})", expr, type_name)
                }
            };
            
            // Replace the PostgreSQL cast with the translated version
            result.replace_range(expr_start..cast_pos + 2 + type_end, &translated_cast);
            
            // Update search position to after the replacement
            let new_search_from = expr_start + translated_cast.len();
            
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
                    return i + 1;  // Return position after the '('
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
                    // For text cast, keep it if the expression might be a special type
                    if Self::might_need_text_cast(expr) {
                        format!("CAST({} AS TEXT)", expr)
                    } else {
                        expr.to_string()
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