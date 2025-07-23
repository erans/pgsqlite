use rusqlite::Connection;
use regex::Regex;
use once_cell::sync::Lazy;
use std::collections::HashMap;

/// Translates NUMERIC column ::text casts to use numeric_format function
pub struct NumericFormatTranslator;

impl NumericFormatTranslator {
    /// Check if translation is needed
    pub fn needs_translation(query: &str) -> bool {
        query.contains("::text") || query.to_uppercase().contains("CAST") && query.to_uppercase().contains("AS TEXT")
    }
    
    /// Translate query to use numeric_format for NUMERIC columns cast to TEXT
    pub fn translate_query(query: &str, conn: &Connection) -> String {
        // First check if we need translation
        if !Self::needs_translation(query) {
            return query.to_string();
        }
        
        // Get numeric constraints for all tables
        let constraints = match Self::load_all_numeric_constraints(conn) {
            Ok(c) => c,
            Err(_) => return query.to_string(), // On error, return unchanged
        };
        
        if constraints.is_empty() {
            return query.to_string();
        }
        
        let mut result = query.to_string();
        
        // Process ::text casts
        result = Self::process_double_colon_casts(&result, &constraints);
        
        // Process CAST(col AS TEXT)
        result = Self::process_cast_syntax(&result, &constraints);
        
        result
    }
    
    /// Load all numeric constraints from the database
    fn load_all_numeric_constraints(conn: &Connection) -> Result<HashMap<String, (i32, i32)>, rusqlite::Error> {
        let mut constraints = HashMap::new();
        
        // Check if the constraints table exists
        let exists: bool = conn.query_row(
            "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='__pgsqlite_numeric_constraints'",
            [],
            |row| row.get(0)
        ).unwrap_or(false);
        
        if !exists {
            return Ok(constraints);
        }
        
        // Load all constraints
        let mut stmt = conn.prepare(
            "SELECT table_name, column_name, precision, scale FROM __pgsqlite_numeric_constraints"
        )?;
        
        let constraint_iter = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,  // table_name
                row.get::<_, String>(1)?,  // column_name
                row.get::<_, i32>(2)?,     // precision
                row.get::<_, i32>(3)?      // scale
            ))
        })?;
        
        for constraint in constraint_iter {
            let (table_name, column_name, precision, scale) = constraint?;
            // Store with both table.column and just column formats
            constraints.insert(format!("{table_name}.{column_name}"), (precision, scale));
            constraints.insert(column_name.clone(), (precision, scale));
        }
        
        Ok(constraints)
    }
    
    /// Process ::text casts
    fn process_double_colon_casts(query: &str, constraints: &HashMap<String, (i32, i32)>) -> String {
        static CAST_REGEX: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(\b\w+(?:\.\w+)?)\s*::\s*text\b").unwrap()
        });
        
        let mut result = query.to_string();
        let mut offset = 0;
        
        for cap in CAST_REGEX.captures_iter(query) {
            let full_match = cap.get(0).unwrap();
            let column_ref = cap.get(1).unwrap().as_str();
            
            // Check if this column has numeric constraints
            if let Some((precision, scale)) = constraints.get(column_ref) {
                let replacement = format!("numeric_format({column_ref}, {precision}, {scale})");
                let start = full_match.start() + offset;
                let end = full_match.end() + offset;
                result.replace_range(start..end, &replacement);
                offset += replacement.len() - full_match.len();
            }
        }
        
        result
    }
    
    /// Process CAST(col AS TEXT) syntax
    fn process_cast_syntax(query: &str, constraints: &HashMap<String, (i32, i32)>) -> String {
        static CAST_REGEX: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(?i)CAST\s*\(\s*(\w+(?:\.\w+)?)\s+AS\s+TEXT\s*\)").unwrap()
        });
        
        let mut result = query.to_string();
        let mut offset = 0;
        
        for cap in CAST_REGEX.captures_iter(query) {
            let full_match = cap.get(0).unwrap();
            let column_ref = cap.get(1).unwrap().as_str();
            
            // Check if this column has numeric constraints
            if let Some((precision, scale)) = constraints.get(column_ref) {
                let replacement = format!("numeric_format({column_ref}, {precision}, {scale})");
                let start = full_match.start() + offset;
                let end = full_match.end() + offset;
                result.replace_range(start..end, &replacement);
                offset += replacement.len() - full_match.len();
            }
        }
        
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    
    #[test]
    fn test_numeric_format_translation() {
        let conn = Connection::open_in_memory().unwrap();
        
        // Create test schema
        conn.execute_batch(r#"
            CREATE TABLE __pgsqlite_numeric_constraints (
                table_name TEXT,
                column_name TEXT,
                precision INTEGER,
                scale INTEGER
            );
            
            INSERT INTO __pgsqlite_numeric_constraints VALUES 
                ('prices', 'amount', 10, 2),
                ('prices', 'tax', 5, 3);
        "#).unwrap();
        
        // Test ::text cast
        let query = "SELECT amount::text, tax::text FROM prices";
        let translated = NumericFormatTranslator::translate_query(query, &conn);
        assert_eq!(translated, "SELECT numeric_format(amount, 10, 2), numeric_format(tax, 5, 3) FROM prices");
        
        // Test CAST syntax
        let query = "SELECT CAST(amount AS TEXT) FROM prices";
        let translated = NumericFormatTranslator::translate_query(query, &conn);
        assert_eq!(translated, "SELECT numeric_format(amount, 10, 2) FROM prices");
        
        // Test with table prefix
        let query = "SELECT prices.amount::text FROM prices";
        let translated = NumericFormatTranslator::translate_query(query, &conn);
        assert_eq!(translated, "SELECT numeric_format(prices.amount, 10, 2) FROM prices");
        
        // Test non-numeric columns remain unchanged
        let query = "SELECT name::text FROM prices";
        let translated = NumericFormatTranslator::translate_query(query, &conn);
        assert_eq!(translated, "SELECT name::text FROM prices");
    }
}