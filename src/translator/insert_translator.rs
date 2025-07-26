use regex::Regex;
use once_cell::sync::Lazy;
use crate::session::DbHandler;
use crate::types::ValueConverter;
use serde_json;
use tracing::debug;

/// Translates INSERT statements to convert datetime literals to INTEGER values
pub struct InsertTranslator;

// Pattern to match INSERT INTO table (...) VALUES (...)
static INSERT_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?si)INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*(.+?)(?:\s+RETURNING|;?\s*$)").unwrap()
});

// Pattern to match INSERT INTO table VALUES (...) without column list
static INSERT_NO_COLUMNS_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?si)INSERT\s+INTO\s+(\w+)\s+VALUES\s*(.+?)(?:\s+RETURNING|;?\s*$)").unwrap()
});

// Pattern to match INSERT INTO table (...) SELECT ...
static INSERT_SELECT_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?si)INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s+SELECT\s+(.+)").unwrap()
});

// Pattern to match INSERT INTO table SELECT ... (without column list)
static INSERT_SELECT_NO_COLUMNS_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?si)INSERT\s+INTO\s+(\w+)\s+SELECT\s+(.+)").unwrap()
});

impl InsertTranslator {
    /// Check if the query is an INSERT that might need datetime, array, or VALUES translation
    pub fn needs_translation(query: &str) -> bool {
        let is_insert = INSERT_PATTERN.is_match(query) || 
                       INSERT_NO_COLUMNS_PATTERN.is_match(query) ||
                       INSERT_SELECT_PATTERN.is_match(query) ||
                       INSERT_SELECT_NO_COLUMNS_PATTERN.is_match(query);
        
        let has_datetime_or_array = query.contains('-') ||  // Date patterns like '2024-01-01'
                                   query.contains(':') ||  // Time patterns like '14:30:00'
                                   query.contains('{') ||  // Array patterns like '{1,2,3}'
                                   query.contains("ARRAY[") || // Array constructor like ARRAY[1,2,3]
                                   query.contains("NOW()") ||  // PostgreSQL datetime functions
                                   query.contains("CURRENT_DATE") ||
                                   query.contains("CURRENT_TIME") ||
                                   query.contains("CURRENT_TIMESTAMP");
        
        // Also check for SQLAlchemy VALUES pattern
        let has_sqlalchemy_values = query.contains("FROM (VALUES") && query.contains(") AS ") && 
                                   (query.contains("imp_sen") || query.contains("(p0, p1"));
        
        is_insert && (has_datetime_or_array || has_sqlalchemy_values)
    }
    
    /// Translate INSERT statement to convert datetime values to INTEGER format
    pub async fn translate_query(query: &str, db: &DbHandler) -> Result<String, String> {
        // Try matching with explicit columns first
        if let Some(caps) = INSERT_PATTERN.captures(query) {
            let table_name = &caps[1];
            let columns_str = &caps[2];
            let values_str = &caps[3];
            
            // Parse column names
            let columns: Vec<&str> = columns_str.split(',')
                .map(|c| c.trim())
                .collect();
            
            // Get column types from __pgsqlite_schema
            let column_types = Self::get_column_types(db, table_name).await?;
            
            // Check if any columns are datetime or array types
            let needs_conversion = columns.iter().any(|col| {
                if let Some(pg_type) = column_types.get(&col.to_lowercase()) {
                    matches!(pg_type.as_str(),
                        "date" | "DATE" | 
                        "time" | "TIME" | 
                        "timestamp" | "TIMESTAMP" | 
                        "timestamptz" | "TIMESTAMPTZ" |
                        "timetz" | "TIMETZ" |
                        "interval" | "INTERVAL"
                    ) || pg_type.ends_with("[]") || pg_type.starts_with("_")
                } else {
                    false
                }
            });
            
            if !needs_conversion {
                // No datetime or array columns, return original query
                return Ok(query.to_string());
            }
            
            // Parse and convert VALUES
            let converted_values = Self::convert_values_clause(
                values_str,
                &columns,
                &column_types
            )?;
            
            // Reconstruct the INSERT query
            let result = format!(
                "INSERT INTO {} ({}) VALUES {}",
                table_name,
                columns_str,
                converted_values
            );
            Ok(result)
        } else if let Some(caps) = INSERT_NO_COLUMNS_PATTERN.captures(query) {
            // INSERT without explicit columns - need to get all columns from schema
            let table_name = &caps[1];
            let values_str = &caps[2];
            
            // Get all columns and types from __pgsqlite_schema
            let (columns, column_types) = Self::get_all_columns_and_types(db, table_name).await?;
            
            // Check if any columns are datetime or array types
            let needs_conversion = column_types.values().any(|pg_type| {
                matches!(pg_type.as_str(),
                    "date" | "DATE" | 
                    "time" | "TIME" | 
                    "timestamp" | "TIMESTAMP" | 
                    "timestamptz" | "TIMESTAMPTZ" |
                    "timetz" | "TIMETZ" |
                    "interval" | "INTERVAL"
                ) || pg_type.ends_with("[]") || pg_type.starts_with("_")
            });
            
            if !needs_conversion {
                // No datetime or array columns, return original query
                return Ok(query.to_string());
            }
            
            // Parse and convert VALUES
            let columns_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
            let converted_values = Self::convert_values_clause(
                values_str,
                &columns_refs,
                &column_types
            )?;
            
            // Reconstruct the INSERT query  
            Ok(format!(
                "INSERT INTO {} VALUES {}",
                table_name,
                converted_values
            ))
        } else if let Some(caps) = INSERT_SELECT_PATTERN.captures(query) {
            // Handle INSERT INTO table (...) SELECT ...
            let table_name = &caps[1];
            let columns_str = &caps[2];
            let select_clause = &caps[3];
            
            debug!("INSERT SELECT translation called for table: {}", table_name);
            debug!("SELECT clause: {}", select_clause);
            
            // Parse column names
            let columns: Vec<&str> = columns_str.split(',')
                .map(|c| c.trim())
                .collect();
            
            // Get column types from __pgsqlite_schema
            let column_types = Self::get_column_types(db, table_name).await?;
            
            // Check if this is the SQLAlchemy VALUES pattern FIRST
            let final_select = if Self::is_sqlalchemy_values_pattern(select_clause) {
                eprintln!("🎯 SQLAlchemy VALUES pattern detected, converting to UNION ALL");
                eprintln!("   Table: {}", table_name);
                eprintln!("   Columns: {:?}", columns);
                eprintln!("   Select clause: {}", select_clause);
                // Convert VALUES pattern to UNION ALL
                Self::convert_sqlalchemy_values_to_union(select_clause, &columns, &column_types)?
            } else {
                // Translate the SELECT clause normally
                let converted_select = Self::translate_select_clause(
                    select_clause,
                    &columns,
                    &column_types
                )?;
                eprintln!("   Converted SELECT: {}", converted_select);
                converted_select
            };
            
            eprintln!("   Final SELECT: {}", final_select);
            
            // Reconstruct the INSERT query
            Ok(format!(
                "INSERT INTO {} ({}) SELECT {}",
                table_name,
                columns_str,
                final_select
            ))
        } else if let Some(caps) = INSERT_SELECT_NO_COLUMNS_PATTERN.captures(query) {
            // Handle INSERT INTO table SELECT ... (without column list)
            let table_name = &caps[1];
            let select_clause = &caps[2];
            
            // Get all columns and types from __pgsqlite_schema, ordered by column position
            let (columns, column_types) = Self::get_all_columns_and_types(db, table_name).await?;
            
            // Translate the SELECT clause
            let columns_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
            let converted_select = Self::translate_select_clause(
                select_clause,
                &columns_refs,
                &column_types
            )?;
            
            // Reconstruct the INSERT query  
            Ok(format!(
                "INSERT INTO {} SELECT {}",
                table_name,
                converted_select
            ))
        } else {
            // Not a recognized INSERT pattern, return as-is
            Ok(query.to_string())
        }
    }
    
    /// Get column types from __pgsqlite_schema
    async fn get_column_types(db: &DbHandler, table_name: &str) -> Result<std::collections::HashMap<String, String>, String> {
        let query = format!(
            "SELECT column_name, pg_type FROM __pgsqlite_schema WHERE table_name = '{}'",
            table_name
        );
        
        match db.query(&query).await {
            Ok(response) => {
                let mut types = std::collections::HashMap::new();
                for row in response.rows {
                    if row.len() >= 2 {
                        if let (Some(col_name), Some(pg_type)) = (&row[0], &row[1]) {
                            let col_str = String::from_utf8_lossy(col_name).to_string();
                            let type_str = String::from_utf8_lossy(pg_type).to_string();
                            types.insert(col_str.to_lowercase(), type_str);
                        }
                    }
                }
                Ok(types)
            }
            Err(_) => {
                // __pgsqlite_schema might not exist or query failed
                Ok(std::collections::HashMap::new())
            }
        }
    }
    
    /// Get all columns and their types from __pgsqlite_schema, ordered by column position
    async fn get_all_columns_and_types(db: &DbHandler, table_name: &str) -> Result<(Vec<String>, std::collections::HashMap<String, String>), String> {
        // First get columns from PRAGMA table_info to ensure correct order
        let pragma_query = format!("PRAGMA table_info({})", table_name);
        let column_order = match db.query(&pragma_query).await {
            Ok(response) => {
                let mut columns = Vec::new();
                for row in response.rows {
                    if row.len() > 1 {
                        if let Some(col_name) = &row[1] {
                            let col_str = String::from_utf8_lossy(col_name).to_string();
                            columns.push(col_str);
                        }
                    }
                }
                columns
            }
            Err(e) => {
                return Err(format!("Failed to get table info: {}", e));
            }
        };
        
        // Then get types from __pgsqlite_schema
        let types = Self::get_column_types(db, table_name).await?;
        
        Ok((column_order, types))
    }
    
    /// Convert VALUES clause, transforming datetime literals to INTEGER
    fn convert_values_clause(
        values_str: &str,
        columns: &[&str],
        column_types: &std::collections::HashMap<String, String>
    ) -> Result<String, String> {
        let values_str = values_str.trim();
        
        // Check if this is a multi-row INSERT
        if values_str.contains("),(") || values_str.matches('(').count() > 1 {
            // Handle multi-row INSERT
            let mut result_rows = Vec::new();
            let mut current_row = String::new();
            let mut paren_depth = 0;
            let mut in_quotes = false;
            let mut chars = values_str.chars().peekable();
            
            while let Some(ch) = chars.next() {
                match ch {
                    '\'' => {
                        current_row.push(ch);
                        if in_quotes && chars.peek() == Some(&'\'') {
                            current_row.push('\'');
                            chars.next();
                        } else {
                            in_quotes = !in_quotes;
                        }
                    }
                    '(' if !in_quotes => {
                        if paren_depth == 0 && !current_row.trim().is_empty() {
                            // Start of a new row
                            current_row.clear();
                        }
                        paren_depth += 1;
                        current_row.push(ch);
                    }
                    ')' if !in_quotes => {
                        paren_depth -= 1;
                        current_row.push(ch);
                        
                        if paren_depth == 0 {
                            // End of a row
                            let row_content = current_row.trim();
                            if row_content.starts_with('(') && row_content.ends_with(')') {
                                let inner = &row_content[1..row_content.len()-1];
                                let values = Self::parse_values(inner)?;
                                
                                if values.len() != columns.len() {
                                    // For batch INSERTs, indicate which row has the problem
                                    let row_num = result_rows.len() + 1;
                                    return Err(format!("Column count mismatch in row {}: {} columns but {} values", row_num, columns.len(), values.len()));
                                }
                                
                                // Convert each value based on column type
                                let mut converted_values = Vec::new();
                                for (i, value) in values.iter().enumerate() {
                                    let column_name = columns[i];
                                    let converted = if let Some(pg_type) = column_types.get(&column_name.to_lowercase()) {
                                        Self::convert_value(value, pg_type)?
                                    } else {
                                        value.to_string()
                                    };
                                    converted_values.push(converted);
                                }
                                
                                result_rows.push(format!("({})", converted_values.join(", ")));
                                current_row.clear();
                            }
                        }
                    }
                    _ => {
                        current_row.push(ch);
                    }
                }
            }
            
            if result_rows.is_empty() {
                return Err("No valid rows found in multi-row INSERT".to_string());
            }
            
            let result = result_rows.join(", ");
            Ok(result)
        } else {
            // Handle single-row INSERT
            let values_content = if values_str.starts_with('(') && values_str.ends_with(')') {
                &values_str[1..values_str.len()-1]
            } else {
                values_str
            };
            
            // Parse individual values
            let values = Self::parse_values(values_content)?;
            
            if values.len() != columns.len() {
                return Err(format!("Column count mismatch: {} columns but {} values", columns.len(), values.len()));
            }
            
            // Convert each value based on column type
            let mut converted_values = Vec::new();
            for (i, value) in values.iter().enumerate() {
                let column_name = columns[i];
                let converted = if let Some(pg_type) = column_types.get(&column_name.to_lowercase()) {
                    Self::convert_value(value, pg_type)?
                } else {
                    value.to_string()
                };
                converted_values.push(converted);
            }
            
            Ok(format!("({})", converted_values.join(", ")))
        }
    }
    
    /// Parse comma-separated values, handling quoted strings
    fn parse_values(values_str: &str) -> Result<Vec<String>, String> {
        let mut values = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut escape_next = false;
        let mut chars = values_str.chars().peekable();
        
        while let Some(ch) = chars.next() {
            if escape_next {
                current.push(ch);
                escape_next = false;
                continue;
            }
            
            match ch {
                '\'' => {
                    current.push(ch);
                    // Check for escaped quote
                    if in_quotes && chars.peek() == Some(&'\'') {
                        current.push('\'');
                        chars.next();
                    } else {
                        in_quotes = !in_quotes;
                    }
                }
                '\\' if in_quotes => {
                    current.push(ch);
                    escape_next = true;
                }
                ',' if !in_quotes => {
                    values.push(current.trim().to_string());
                    current.clear();
                }
                _ => {
                    current.push(ch);
                }
            }
        }
        
        if !current.is_empty() {
            values.push(current.trim().to_string());
        }
        
        Ok(values)
    }
    
    /// Convert a single value based on PostgreSQL type
    fn convert_value(value: &str, pg_type: &str) -> Result<String, String> {
        // Skip NULL values
        if value.to_uppercase() == "NULL" {
            return Ok(value.to_string());
        }
        
        // Check for date/time function calls (not quoted)
        let value_upper = value.to_uppercase();
        if !value.starts_with('\'') {
            // Handle NOW() -> CURRENT_TIMESTAMP conversion for SQLite
            if value_upper == "NOW()" {
                return Ok("CURRENT_TIMESTAMP".to_string());
            }
            // Other date/time functions that SQLite handles natively
            if value_upper == "CURRENT_DATE" ||
               value_upper == "CURRENT_TIME" ||
               value_upper == "CURRENT_TIMESTAMP" ||
               value_upper.starts_with("CURRENT_") {
                return Ok(value.to_string());
            }
        }
        
        // Remove quotes if present
        let unquoted = if value.starts_with('\'') && value.ends_with('\'') && value.len() > 1 {
            &value[1..value.len()-1]
        } else {
            value
        };
        
        match pg_type.to_lowercase().as_str() {
            "date" => {
                match ValueConverter::convert_date_to_unix(unquoted) {
                    Ok(days) => Ok(days),
                    Err(e) => Err(format!("Invalid date value '{}': {}. Expected format: YYYY-MM-DD", unquoted, e))
                }
            }
            "time" => {
                match ValueConverter::convert_time_to_seconds(unquoted) {
                    Ok(micros) => Ok(micros),
                    Err(e) => Err(format!("Invalid time value '{}': {}. Expected format: HH:MM:SS[.ffffff]", unquoted, e))
                }
            }
            "timestamp" => {
                match ValueConverter::convert_timestamp_to_unix(unquoted) {
                    Ok(micros) => Ok(micros),
                    Err(e) => Err(format!("Invalid timestamp value '{}': {}. Expected format: YYYY-MM-DD HH:MM:SS[.ffffff]", unquoted, e))
                }
            }
            "timestamptz" | "timetz" | "interval" => {
                // TODO: Implement these conversions
                // For now, keep as quoted strings
                Ok(value.to_string())
            }
            _ => {
                // Check if it's an array type
                if pg_type.ends_with("[]") || pg_type.starts_with("_") {
                    // Convert PostgreSQL array literal to JSON
                    Self::convert_array_value(value)
                } else {
                    // Not a datetime or array type, keep original value
                    Ok(value.to_string())
                }
            }
        }
    }
    
    /// Convert PostgreSQL array literal to JSON format
    fn convert_array_value(value: &str) -> Result<String, String> {
        let value = value.trim();
        
        // Handle NULL
        if value.eq_ignore_ascii_case("NULL") {
            return Ok("NULL".to_string());
        }
        
        // Handle ARRAY[...] constructor
        if value.starts_with("ARRAY[") && value.ends_with(']') {
            let inner = &value[6..value.len()-1];
            let elements = Self::parse_array_elements(inner)?;
            let json_array = serde_json::to_string(&elements)
                .map_err(|e| format!("Failed to convert array to JSON: {}", e))?;
            return Ok(format!("'{}'", json_array));
        }
        
        // Handle '{...}' literal
        if value.starts_with("'{") && value.ends_with("}'") {
            let inner = &value[2..value.len()-2];
            let elements = Self::parse_pg_array_literal(inner)?;
            let json_array = serde_json::to_string(&elements)
                .map_err(|e| format!("Failed to convert array to JSON: {}", e))?;
            return Ok(format!("'{}'", json_array));
        }
        
        // If it's already a quoted value that doesn't look like an array, keep it
        Ok(value.to_string())
    }
    
    /// Parse elements from ARRAY[1,2,3] format
    fn parse_array_elements(inner: &str) -> Result<Vec<serde_json::Value>, String> {
        let mut elements = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut depth = 0;
        
        for ch in inner.chars() {
            match ch {
                '\'' if depth == 0 => {
                    in_quotes = !in_quotes;
                    current.push(ch);
                }
                ',' if !in_quotes && depth == 0 => {
                    let elem = current.trim();
                    elements.push(Self::parse_array_element(elem)?);
                    current.clear();
                }
                '[' => {
                    depth += 1;
                    current.push(ch);
                }
                ']' => {
                    depth -= 1;
                    current.push(ch);
                }
                _ => current.push(ch),
            }
        }
        
        // Don't forget the last element
        if !current.trim().is_empty() {
            elements.push(Self::parse_array_element(current.trim())?);
        }
        
        Ok(elements)
    }
    
    /// Parse elements from PostgreSQL array literal format {1,2,3}
    fn parse_pg_array_literal(inner: &str) -> Result<Vec<serde_json::Value>, String> {
        let mut elements = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        
        for ch in inner.chars() {
            match ch {
                '"' => {
                    // PostgreSQL uses double quotes for string elements
                    in_quotes = !in_quotes;
                }
                ',' if !in_quotes => {
                    let elem = current.trim();
                    if !elem.is_empty() {
                        elements.push(Self::parse_pg_array_element(elem)?);
                    }
                    current.clear();
                }
                _ => current.push(ch),
            }
        }
        
        // Don't forget the last element
        if !current.trim().is_empty() {
            elements.push(Self::parse_pg_array_element(current.trim())?);
        }
        
        Ok(elements)
    }
    
    /// Parse a single array element
    fn parse_array_element(elem: &str) -> Result<serde_json::Value, String> {
        let elem = elem.trim();
        
        // NULL
        if elem.eq_ignore_ascii_case("NULL") {
            return Ok(serde_json::Value::Null);
        }
        
        // Quoted string
        if elem.starts_with('\'') && elem.ends_with('\'') && elem.len() > 1 {
            let unquoted = &elem[1..elem.len()-1];
            return Ok(serde_json::Value::String(unquoted.to_string()));
        }
        
        // Number
        if let Ok(num) = elem.parse::<i64>() {
            return Ok(serde_json::json!(num));
        }
        if let Ok(num) = elem.parse::<f64>() {
            return Ok(serde_json::json!(num));
        }
        
        // Boolean
        if elem.eq_ignore_ascii_case("true") || elem.eq_ignore_ascii_case("false") {
            return Ok(serde_json::json!(elem.eq_ignore_ascii_case("true")));
        }
        
        // Default to string
        Ok(serde_json::Value::String(elem.to_string()))
    }
    
    /// Parse a single PostgreSQL array element (from {} format)
    fn parse_pg_array_element(elem: &str) -> Result<serde_json::Value, String> {
        let elem = elem.trim();
        
        // NULL
        if elem.eq_ignore_ascii_case("NULL") {
            return Ok(serde_json::Value::Null);
        }
        
        // Quoted string (PostgreSQL uses double quotes in array literals)
        if elem.starts_with('"') && elem.ends_with('"') && elem.len() > 1 {
            let unquoted = &elem[1..elem.len()-1];
            return Ok(serde_json::Value::String(unquoted.to_string()));
        }
        
        // Number
        if let Ok(num) = elem.parse::<i64>() {
            return Ok(serde_json::json!(num));
        }
        if let Ok(num) = elem.parse::<f64>() {
            return Ok(serde_json::json!(num));
        }
        
        // Boolean
        if elem.eq_ignore_ascii_case("true") || elem.eq_ignore_ascii_case("false") {
            return Ok(serde_json::json!(elem.eq_ignore_ascii_case("true")));
        }
        
        // Default to string
        Ok(serde_json::Value::String(elem.to_string()))
    }
    
    /// Translate SELECT clause expressions to convert datetime literals and functions
    fn translate_select_clause(
        select_clause: &str,
        columns: &[&str],
        column_types: &std::collections::HashMap<String, String>
    ) -> Result<String, String> {
        // SQLAlchemy VALUES pattern is now handled in the main translate_query method
        
        // Parse the SELECT clause to extract individual expressions
        let expressions = Self::parse_select_expressions(select_clause)?;
        
        if expressions.len() != columns.len() {
            return Err(format!(
                "Column count mismatch in SELECT: {} columns but {} expressions", 
                columns.len(), 
                expressions.len()
            ));
        }
        
        // Convert each expression based on the target column type
        let mut converted_expressions = Vec::new();
        for (i, expr) in expressions.iter().enumerate() {
            let column_name = columns[i];
            let converted_expr = if let Some(pg_type) = column_types.get(&column_name.to_lowercase()) {
                Self::convert_select_expression(expr, pg_type)?
            } else {
                expr.to_string()
            };
            converted_expressions.push(converted_expr);
        }
        
        Ok(converted_expressions.join(", "))
    }
    
    /// Check if this is the SQLAlchemy VALUES pattern with column aliases
    fn is_sqlalchemy_values_pattern(select_clause: &str) -> bool {
        // Look for pattern: p0::TYPE or CAST(p0 AS TYPE), ... FROM (VALUES ...) AS alias(p0, p1, p2, ...)
        // This works for both original queries and after CastTranslator has run
        select_clause.contains("FROM (VALUES") && 
        select_clause.contains(") AS ") && 
        (select_clause.contains("(p0, p1, p2") || 
         (select_clause.contains("CAST(p0") && select_clause.contains("imp_sen")))
    }
    
    /// Convert SQLAlchemy VALUES pattern to UNION ALL syntax
    fn convert_sqlalchemy_values_to_union(
        select_clause: &str,
        columns: &[&str],
        _column_types: &std::collections::HashMap<String, String>
    ) -> Result<String, String> {
        // Extract the VALUES rows from the pattern:
        // SELECT p0::TYPE, p1::TYPE FROM (VALUES (val1, val2, val3, idx), ...) AS alias(p0, p1, p2, sen_counter)
        
        // First, extract the SELECT expressions to understand what type casts are applied
        let select_start = 0;
        let from_pos = select_clause.find(" FROM ").ok_or("FROM not found")?;
        let select_expressions = &select_clause[select_start..from_pos];
        
        // Parse the SELECT expressions to get the type casts
        let type_casts = Self::parse_sqlalchemy_type_casts(select_expressions)?;
        eprintln!("   🔍 Type casts: {:?}", type_casts);
        
        // Find the VALUES clause
        let values_start = select_clause.find("VALUES").ok_or("VALUES not found")?;
        let values_end_search = &select_clause[values_start..];
        
        // Find the end of VALUES - look for ) AS
        let as_pos = values_end_search.find(") AS").ok_or("End of VALUES not found")?;
        let values_content = &values_end_search[6..as_pos + 1]; // Skip "VALUES" and include the last )
        
        // Parse the VALUES rows
        let rows = Self::parse_values_rows(values_content)?;
        eprintln!("   📦 Parsed {} rows from VALUES clause", rows.len());
        
        // Extract ORDER BY and RETURNING clauses if present
        let mut order_by = "";
        let mut returning = "";
        
        if let Some(order_pos) = select_clause.find(" ORDER BY ") {
            let order_end = select_clause[order_pos..].find(" RETURNING ")
                .map(|p| order_pos + p)
                .unwrap_or(select_clause.len());
            order_by = &select_clause[order_pos..order_end];
        }
        
        if let Some(ret_pos) = select_clause.find(" RETURNING ") {
            returning = &select_clause[ret_pos..];
        }
        
        // Build UNION ALL query
        let mut union_parts = Vec::new();
        
        for (row_idx, row_values) in rows.iter().enumerate() {
            let mut select_parts = Vec::new();
            
            for (col_idx, value) in row_values.iter().enumerate() {
                if col_idx >= columns.len() {
                    // This is probably the sen_counter column, skip it
                    continue;
                }
                
                let _column_name = columns[col_idx];
                
                // Apply type cast if specified in the original query
                let converted_value = if col_idx < type_casts.len() {
                    if let Some(ref cast_type) = type_casts[col_idx] {
                        // Apply the CAST
                        format!("CAST({} AS {})", value, cast_type)
                    } else {
                        // No cast found in query, just use the value
                        // For NUMERIC columns, we don't need special handling since SQLite stores them as-is
                        value.to_string()
                    }
                } else {
                    // Beyond the type_casts array, just use value as-is
                    value.to_string()
                };
                
                select_parts.push(converted_value);
            }
            
            // For the first row, don't include SELECT (it will be added by the caller)
            // For subsequent rows, we need SELECT for UNION ALL
            if row_idx == 0 {
                union_parts.push(select_parts.join(", "));
            } else {
                let select_stmt = format!("SELECT {}", select_parts.join(", "));
                union_parts.push(select_stmt);
            }
        }
        
        // Join with UNION ALL
        let union_query = union_parts.join(" UNION ALL ");
        
        // Add ORDER BY if we need to preserve row order (but skip sen_counter)
        let final_query = if !order_by.is_empty() && order_by.contains("sen_counter") {
            // Skip ORDER BY sen_counter as it doesn't exist in UNION ALL
            format!("{}{}", union_query, returning)
        } else {
            format!("{}{}{}", union_query, order_by, returning)
        };
        
        Ok(final_query)
    }
    
    /// Parse VALUES rows from a VALUES clause like ((val1, val2), (val3, val4))
    /// Parse type casts from SQLAlchemy SELECT expressions like "CAST(p0 AS INTEGER), p1::NUMERIC(10, 2)"
    fn parse_sqlalchemy_type_casts(select_expressions: &str) -> Result<Vec<Option<String>>, String> {
        let mut type_casts = Vec::new();
        
        // Split by comma but handle nested parentheses
        let expressions = Self::parse_select_expressions(select_expressions)?;
        
        for expr in expressions {
            let expr_trimmed = expr.trim();
            
            // Check for CAST(p{n} AS TYPE) pattern
            if expr_trimmed.starts_with("CAST(") {
                if let Some(as_pos) = expr_trimmed.find(" AS ") {
                    let after_as = &expr_trimmed[as_pos + 4..];
                    if let Some(close_pos) = after_as.rfind(')') {
                        let cast_type = after_as[..close_pos].trim();
                        type_casts.push(Some(cast_type.to_string()));
                        continue;
                    }
                }
            }
            
            // Check for p{n}::TYPE pattern
            if expr_trimmed.contains("::") {
                if let Some(cast_pos) = expr_trimmed.find("::") {
                    let cast_type = expr_trimmed[cast_pos + 2..].trim();
                    type_casts.push(Some(cast_type.to_string()));
                    continue;
                }
            }
            
            // No cast found
            type_casts.push(None);
        }
        
        Ok(type_casts)
    }
    
    fn parse_values_rows(values_content: &str) -> Result<Vec<Vec<String>>, String> {
        let mut rows = Vec::new();
        let mut current_row = Vec::new();
        let mut current_value = String::new();
        let mut in_quotes = false;
        let mut paren_depth = 0;
        let mut in_row = false;
        
        let chars: Vec<char> = values_content.chars().collect();
        let mut i = 0;
        
        while i < chars.len() {
            let ch = chars[i];
            
            match ch {
                '\'' => {
                    current_value.push(ch);
                    if in_quotes && i + 1 < chars.len() && chars[i + 1] == '\'' {
                        // Escaped quote
                        current_value.push('\'');
                        i += 1;
                    } else {
                        in_quotes = !in_quotes;
                    }
                }
                '(' if !in_quotes => {
                    if paren_depth == 0 {
                        in_row = true;
                    } else {
                        current_value.push(ch);
                    }
                    paren_depth += 1;
                }
                ')' if !in_quotes => {
                    paren_depth -= 1;
                    if paren_depth == 0 && in_row {
                        // End of row
                        if !current_value.trim().is_empty() {
                            current_row.push(current_value.trim().to_string());
                            current_value.clear();
                        }
                        rows.push(current_row.clone());
                        current_row.clear();
                        in_row = false;
                    } else {
                        current_value.push(ch);
                    }
                }
                ',' if !in_quotes && in_row => {
                    // End of value within row
                    current_row.push(current_value.trim().to_string());
                    current_value.clear();
                }
                _ => {
                    if in_row {
                        current_value.push(ch);
                    }
                }
            }
            i += 1;
        }
        
        Ok(rows)
    }
    
    /// Parse SELECT clause into individual expressions, handling commas within function calls
    fn parse_select_expressions(select_clause: &str) -> Result<Vec<String>, String> {
        // First, extract only the expressions part before FROM
        let expressions_part = if let Some(from_pos) = select_clause.find(" FROM ") {
            &select_clause[..from_pos]
        } else {
            select_clause
        }.trim();
        
        let mut expressions = Vec::new();
        let mut current = String::new();
        let mut paren_depth = 0;
        let mut in_quotes = false;
        let mut chars = expressions_part.chars().peekable();
        
        while let Some(ch) = chars.next() {
            match ch {
                '\'' => {
                    current.push(ch);
                    // Handle escaped quotes
                    if in_quotes && chars.peek() == Some(&'\'') {
                        current.push('\'');
                        chars.next();
                    } else {
                        in_quotes = !in_quotes;
                    }
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
                    // End of expression
                    expressions.push(current.trim().to_string());
                    current.clear();
                }
                _ => {
                    current.push(ch);
                }
            }
        }
        
        // Don't forget the last expression
        if !current.trim().is_empty() {
            expressions.push(current.trim().to_string());
        }
        
        Ok(expressions)
    }
    
    /// Convert a single SELECT expression to handle datetime literals and functions
    fn convert_select_expression(expr: &str, pg_type: &str) -> Result<String, String> {
        let expr_trimmed = expr.trim();
        
        // Check if this is a datetime/array type that needs conversion
        let needs_datetime_conversion = matches!(pg_type.to_lowercase().as_str(),
            "date" | "time" | "timestamp" | "timestamptz" | "timetz" | "interval"
        );
        
        let needs_array_conversion = pg_type.ends_with("[]") || pg_type.starts_with("_");
        
        // Handle PostgreSQL type cast expressions like "p0::VARCHAR" or "p1::TIMESTAMP"
        if expr_trimmed.contains("::") {
            // For cast expressions, we don't convert them - they're column references with type hints
            // SQLAlchemy uses these for parameter binding, not literal values
            return Ok(expr_trimmed.to_string());
        }
        
        if needs_datetime_conversion {
            // Handle PostgreSQL datetime functions
            let expr_upper = expr_trimmed.to_uppercase();
            if expr_upper == "NOW()" {
                return Ok("CURRENT_TIMESTAMP".to_string());
            }
            if expr_upper == "CURRENT_DATE" || 
               expr_upper == "CURRENT_TIME" || 
               expr_upper == "CURRENT_TIMESTAMP" {
                return Ok(expr_trimmed.to_string());
            }
            
            // Handle literal datetime values (quoted strings)
            if expr_trimmed.starts_with('\'') && expr_trimmed.ends_with('\'') && expr_trimmed.len() > 1 {
                let literal_value = &expr_trimmed[1..expr_trimmed.len()-1];
                return Self::convert_datetime_literal(literal_value, pg_type);
            }
        }
        
        if needs_array_conversion {
            // Handle array literals and constructors
            if expr_trimmed.starts_with("ARRAY[") || expr_trimmed.starts_with("'{") {
                return Self::convert_array_value(expr_trimmed);
            }
        }
        
        // No conversion needed, return as-is
        Ok(expr_trimmed.to_string())
    }
    
    /// Convert datetime literal to INTEGER format
    fn convert_datetime_literal(literal: &str, pg_type: &str) -> Result<String, String> {
        match pg_type.to_lowercase().as_str() {
            "date" => {
                match ValueConverter::convert_date_to_unix(literal) {
                    Ok(days) => Ok(days),
                    Err(e) => Err(format!("Invalid date value '{}': {}", literal, e))
                }
            }
            "time" => {
                match ValueConverter::convert_time_to_seconds(literal) {
                    Ok(micros) => Ok(micros),
                    Err(e) => Err(format!("Invalid time value '{}': {}", literal, e))
                }
            }
            "timestamp" => {
                match ValueConverter::convert_timestamp_to_unix(literal) {
                    Ok(micros) => Ok(micros),
                    Err(e) => Err(format!("Invalid timestamp value '{}': {}", literal, e))
                }
            }
            _ => {
                // For other types (timestamptz, timetz, interval), keep as quoted string for now
                Ok(format!("'{}'", literal))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_values() {
        let values = InsertTranslator::parse_values("1, 'hello', '2024-01-15', 'it''s fine'").unwrap();
        assert_eq!(values, vec!["1", "'hello'", "'2024-01-15'", "'it''s fine'"]);
    }
    
    #[test]
    fn test_convert_date_value() {
        let result = InsertTranslator::convert_value("'2024-01-15'", "date").unwrap();
        assert_eq!(result, "19737"); // Days since epoch
    }
    
    #[test]
    fn test_needs_translation() {
        assert!(InsertTranslator::needs_translation("INSERT INTO test (date_col) VALUES ('2024-01-15')"));
        assert!(InsertTranslator::needs_translation("INSERT INTO test (time_col) VALUES ('14:30:00')"));
        assert!(InsertTranslator::needs_translation("INSERT INTO test (arr_col) VALUES ('{1,2,3}')"));
        assert!(InsertTranslator::needs_translation("INSERT INTO test (arr_col) VALUES (ARRAY[1,2,3])"));
        assert!(!InsertTranslator::needs_translation("INSERT INTO test (id) VALUES (1)"));
        
        // Test INSERT SELECT patterns
        assert!(InsertTranslator::needs_translation("INSERT INTO test (date_col) SELECT '2024-01-15' FROM source"));
        assert!(InsertTranslator::needs_translation("INSERT INTO test SELECT '2024-01-15', NOW() FROM source"));
        assert!(InsertTranslator::needs_translation("INSERT INTO test (time_col) SELECT CURRENT_TIME FROM source"));
        assert!(!InsertTranslator::needs_translation("INSERT INTO test SELECT id, name FROM source"));
    }
    
    #[test]
    fn test_convert_array_value() {
        // Test ARRAY constructor
        let result = InsertTranslator::convert_array_value("ARRAY[1,2,3]").unwrap();
        assert_eq!(result, "'[1,2,3]'");
        
        // Test PostgreSQL array literal
        let result = InsertTranslator::convert_array_value("'{1,2,3}'").unwrap();
        assert_eq!(result, "'[1,2,3]'");
        
        // Test with strings
        let result = InsertTranslator::convert_array_value("ARRAY['a','b','c']").unwrap();
        assert_eq!(result, r#"'["a","b","c"]'"#);
        
        // Test with NULL
        let result = InsertTranslator::convert_array_value("NULL").unwrap();
        assert_eq!(result, "NULL");
    }
    
    #[test]
    fn test_parse_array_elements() {
        let elements = InsertTranslator::parse_array_elements("1,2,3").unwrap();
        assert_eq!(elements, vec![serde_json::json!(1), serde_json::json!(2), serde_json::json!(3)]);
        
        let elements = InsertTranslator::parse_array_elements("'a','b','c'").unwrap();
        assert_eq!(elements, vec![serde_json::json!("a"), serde_json::json!("b"), serde_json::json!("c")]);
        
        let elements = InsertTranslator::parse_array_elements("1,NULL,3").unwrap();
        assert_eq!(elements, vec![serde_json::json!(1), serde_json::Value::Null, serde_json::json!(3)]);
    }
    
    #[test]
    fn test_parse_select_expressions() {
        // Test simple expressions
        let expressions = InsertTranslator::parse_select_expressions("id, name, '2024-01-15'").unwrap();
        assert_eq!(expressions, vec!["id", "name", "'2024-01-15'"]);
        
        // Test expressions with function calls
        let expressions = InsertTranslator::parse_select_expressions("id, UPPER(name), NOW()").unwrap();
        assert_eq!(expressions, vec!["id", "UPPER(name)", "NOW()"]);
        
        // Test complex expressions with nested commas
        let expressions = InsertTranslator::parse_select_expressions("id, COALESCE(date_col, '2024-01-01'), name").unwrap();
        assert_eq!(expressions, vec!["id", "COALESCE(date_col, '2024-01-01')", "name"]);
        
        // Test SQLAlchemy pattern with FROM clause
        let expressions = InsertTranslator::parse_select_expressions("p0::VARCHAR, p1::TEXT, p2::TIMESTAMP WITHOUT TIME ZONE FROM (VALUES (...)) AS imp_sen ORDER BY sen_counter").unwrap();
        assert_eq!(expressions, vec!["p0::VARCHAR", "p1::TEXT", "p2::TIMESTAMP WITHOUT TIME ZONE"]);
    }
    
    #[test]
    fn test_convert_select_expression() {
        // Test datetime literal conversion
        let result = InsertTranslator::convert_select_expression("'2024-01-15'", "date").unwrap();
        assert_eq!(result, "19737"); // Days since epoch
        
        // Test function conversion
        let result = InsertTranslator::convert_select_expression("NOW()", "timestamp").unwrap();
        assert_eq!(result, "CURRENT_TIMESTAMP");
        
        // Test non-datetime expression (should pass through)
        let result = InsertTranslator::convert_select_expression("id + 1", "integer").unwrap();
        assert_eq!(result, "id + 1");
        
        // Test CURRENT_DATE (should pass through)
        let result = InsertTranslator::convert_select_expression("CURRENT_DATE", "date").unwrap();
        assert_eq!(result, "CURRENT_DATE");
        
        // Test PostgreSQL cast expressions (should pass through unchanged)
        let result = InsertTranslator::convert_select_expression("p0::VARCHAR", "varchar").unwrap();
        assert_eq!(result, "p0::VARCHAR");
        
        let result = InsertTranslator::convert_select_expression("p2::TIMESTAMP WITHOUT TIME ZONE", "timestamp").unwrap();
        assert_eq!(result, "p2::TIMESTAMP WITHOUT TIME ZONE");
    }
}