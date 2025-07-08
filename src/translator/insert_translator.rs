use regex::Regex;
use once_cell::sync::Lazy;
use crate::session::DbHandler;
use crate::types::ValueConverter;

/// Translates INSERT statements to convert datetime literals to INTEGER values
pub struct InsertTranslator;

// Pattern to match INSERT INTO table (...) VALUES (...)
static INSERT_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?si)INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*(.+)$").unwrap()
});

// Pattern to match INSERT INTO table VALUES (...) without column list
static INSERT_NO_COLUMNS_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?si)INSERT\s+INTO\s+(\w+)\s+VALUES\s*(.+)$").unwrap()
});

impl InsertTranslator {
    /// Check if the query is an INSERT that might need datetime translation
    pub fn needs_translation(query: &str) -> bool {
        let result = (INSERT_PATTERN.is_match(query) || INSERT_NO_COLUMNS_PATTERN.is_match(query)) && (
            query.contains('-') ||  // Date patterns like '2024-01-01'
            query.contains(':')     // Time patterns like '14:30:00'
        );
        result
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
            
            // Check if any columns are datetime types
            let has_datetime = columns.iter().any(|col| {
                if let Some(pg_type) = column_types.get(&col.to_lowercase()) {
                    matches!(pg_type.as_str(),
                        "date" | "DATE" | 
                        "time" | "TIME" | 
                        "timestamp" | "TIMESTAMP" | 
                        "timestamptz" | "TIMESTAMPTZ" |
                        "timetz" | "TIMETZ" |
                        "interval" | "INTERVAL"
                    )
                } else {
                    false
                }
            });
            
            if !has_datetime {
                // No datetime columns, return original query
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
            
            // Check if any columns are datetime types
            let has_datetime = column_types.values().any(|pg_type| {
                matches!(pg_type.as_str(),
                    "date" | "DATE" | 
                    "time" | "TIME" | 
                    "timestamp" | "TIMESTAMP" | 
                    "timestamptz" | "TIMESTAMPTZ" |
                    "timetz" | "TIMETZ" |
                    "interval" | "INTERVAL"
                )
            });
            
            if !has_datetime {
                // No datetime columns, return original query
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
        } else {
            // Not a simple INSERT statement, return as-is
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
                                    return Err(format!("Column count mismatch: {} columns but {} values in row", columns.len(), values.len()));
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
                    Err(e) => Err(format!("Failed to convert date '{}': {}", unquoted, e))
                }
            }
            "time" => {
                match ValueConverter::convert_time_to_seconds(unquoted) {
                    Ok(micros) => Ok(micros),
                    Err(e) => Err(format!("Failed to convert time '{}': {}", unquoted, e))
                }
            }
            "timestamp" => {
                match ValueConverter::convert_timestamp_to_unix(unquoted) {
                    Ok(micros) => Ok(micros),
                    Err(e) => Err(format!("Failed to convert timestamp '{}': {}", unquoted, e))
                }
            }
            "timestamptz" | "timetz" | "interval" => {
                // TODO: Implement these conversions
                // For now, keep as quoted strings
                Ok(value.to_string())
            }
            _ => {
                // Not a datetime type, keep original value
                Ok(value.to_string())
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
        assert!(!InsertTranslator::needs_translation("INSERT INTO test (id) VALUES (1)"));
    }
}