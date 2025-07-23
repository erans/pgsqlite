use crate::translator::metadata::TranslationMetadata;
use parking_lot::Mutex;
use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;
use once_cell::sync::Lazy;

static UPDATE_VALUES_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ims)UPDATE\s+(\w+)(?:\s+AS\s+(\w+))?\s+SET\s+(.+?)\s+FROM\s+\(VALUES\s+(.+?)\)\s+AS\s+(\w+)\s*\(\s*([^)]+)\s*\)\s+WHERE\s+(.+)").unwrap()
});

static VALUES_ROW_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\(([^)]+)\)").unwrap()
});

/// Translates PostgreSQL UPDATE ... FROM (VALUES ...) syntax to SQLite-compatible format
pub struct BatchUpdateTranslator {
    #[allow(dead_code)]
    decimal_tables_cache: Arc<Mutex<HashMap<String, bool>>>,
}

#[derive(Debug)]
struct UpdateInfo {
    table: String,
    table_alias: Option<String>,
    set_clause: String,
    values_data: Vec<Vec<String>>,
    values_alias: String,
    values_columns: Vec<String>,
    where_clause: String,
}

impl BatchUpdateTranslator {
    pub fn new(decimal_tables_cache: Arc<Mutex<HashMap<String, bool>>>) -> Self {
        BatchUpdateTranslator {
            decimal_tables_cache,
        }
    }

    /// Check if the query contains UPDATE ... FROM (VALUES ...) pattern
    pub fn contains_batch_update(query: &str) -> bool {
        UPDATE_VALUES_REGEX.is_match(query)
    }

    /// Translate UPDATE ... FROM (VALUES ...) to SQLite-compatible format
    pub fn translate(&self, query: &str, _params: &[Vec<u8>]) -> String {
        if !Self::contains_batch_update(query) {
            return query.to_string();
        }

        match self.parse_update_values(query) {
            Ok(info) => self.generate_case_statement(&info),
            Err(_) => {
                // If parsing fails, return original query
                query.to_string()
            }
        }
    }

    /// Translate with metadata for integration with the query pipeline
    pub fn translate_with_metadata(&self, query: &str, params: &[Vec<u8>]) -> (String, TranslationMetadata) {
        if !Self::contains_batch_update(query) {
            return (query.to_string(), TranslationMetadata::default());
        }

        let translated = self.translate(query, params);
        let metadata = TranslationMetadata::default();
        
        (translated, metadata)
    }

    fn parse_update_values(&self, query: &str) -> Result<UpdateInfo, &'static str> {
        let captures = UPDATE_VALUES_REGEX.captures(query)
            .ok_or("Failed to match UPDATE VALUES pattern")?;

        let table = captures.get(1).unwrap().as_str().to_string();
        let table_alias = captures.get(2).map(|m| m.as_str().to_string());
        let set_clause = captures.get(3).unwrap().as_str().to_string();
        let values_section = captures.get(4).unwrap().as_str();
        let values_alias = captures.get(5).unwrap().as_str().to_string();
        let values_columns_str = captures.get(6).unwrap().as_str();
        let where_clause = captures.get(7).unwrap().as_str().to_string();

        // Parse VALUES columns
        let values_columns: Vec<String> = values_columns_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        // Parse VALUES data rows
        let mut values_data = Vec::new();
        for row_match in VALUES_ROW_REGEX.find_iter(values_section) {
            let row_content = row_match.as_str();
            // Remove outer parentheses
            let row_content = &row_content[1..row_content.len()-1];
            
            // Split by comma, handling quoted strings
            let values: Vec<String> = self.parse_row_values(row_content);
            values_data.push(values);
        }

        Ok(UpdateInfo {
            table,
            table_alias,
            set_clause,
            values_data,
            values_alias,
            values_columns,
            where_clause,
        })
    }

    fn parse_row_values(&self, row_content: &str) -> Vec<String> {
        let mut values = Vec::new();
        let mut current_value = String::new();
        let mut in_quotes = false;
        let mut quote_char = None;
        let mut i = 0;
        let chars: Vec<char> = row_content.chars().collect();

        while i < chars.len() {
            let ch = chars[i];
            
            if !in_quotes && (ch == '\'' || ch == '"') {
                in_quotes = true;
                quote_char = Some(ch);
                current_value.push(ch);
            } else if in_quotes && Some(ch) == quote_char {
                // Check for escaped quote
                if i + 1 < chars.len() && chars[i + 1] == ch {
                    current_value.push(ch);
                    current_value.push(ch);
                    i += 1; // Skip next character
                } else {
                    in_quotes = false;
                    quote_char = None;
                    current_value.push(ch);
                }
            } else if !in_quotes && ch == ',' {
                values.push(current_value.trim().to_string());
                current_value.clear();
            } else {
                current_value.push(ch);
            }
            
            i += 1;
        }
        
        if !current_value.trim().is_empty() {
            values.push(current_value.trim().to_string());
        }
        
        values
    }

    fn generate_case_statement(&self, info: &UpdateInfo) -> String {
        if info.values_data.is_empty() {
            return format!("UPDATE {} SET {}", info.table, info.set_clause);
        }

        // Extract the key column from WHERE clause (simplified)
        let key_column = self.extract_key_column(&info.where_clause, &info.table_alias);
        let key_column_index = info.values_columns.iter()
            .position(|col| col == &key_column)
            .unwrap_or(0);

        // Build CASE statements for each SET column
        let set_parts: Vec<String> = self.parse_set_clause(&info.set_clause, &info.values_alias)
            .into_iter()
            .map(|(column, values_column)| {
                let values_column_index = info.values_columns.iter()
                    .position(|col| col == &values_column)
                    .unwrap_or(1);

                let mut case_stmt = format!("{column} = CASE {key_column}");
                
                for row in &info.values_data {
                    if key_column_index < row.len() && values_column_index < row.len() {
                        case_stmt.push_str(&format!(
                            " WHEN {} THEN {}",
                            row[key_column_index],
                            row[values_column_index]
                        ));
                    }
                }
                
                case_stmt.push_str(" END");
                case_stmt
            })
            .collect();

        // Build WHERE IN clause
        let key_values: Vec<String> = info.values_data.iter()
            .filter_map(|row| row.get(key_column_index).cloned())
            .collect();

        format!(
            "UPDATE {} SET {} WHERE {} IN ({})",
            info.table,
            set_parts.join(", "),
            key_column,
            key_values.join(", ")
        )
    }

    fn extract_key_column(&self, where_clause: &str, table_alias: &Option<String>) -> String {
        // Simplified extraction - look for pattern like "t.id = v.id" or "table.id = values.id"
        if let Some(alias) = table_alias {
            // Remove alias prefix if present
            where_clause.replace(&format!("{alias}."), "")
                .split('=')
                .next()
                .unwrap_or("id")
                .trim()
                .to_string()
        } else {
            // Extract column name from WHERE clause
            where_clause.split('=')
                .next()
                .unwrap_or("id")
                .trim()
                .to_string()
        }
    }

    fn parse_set_clause(&self, set_clause: &str, values_alias: &str) -> Vec<(String, String)> {
        // Parse "col1 = v.val1, col2 = v.val2" into [(col1, val1), (col2, val2)]
        set_clause.split(',')
            .filter_map(|assignment| {
                let parts: Vec<&str> = assignment.split('=').collect();
                if parts.len() == 2 {
                    let column = parts[0].trim().to_string();
                    let value = parts[1].trim();
                    
                    // Extract values column name (remove alias prefix)
                    let values_column = value.replace(&format!("{values_alias}."), "");
                    
                    Some((column, values_column))
                } else {
                    None
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_translator() -> BatchUpdateTranslator {
        let cache = Arc::new(Mutex::new(HashMap::new()));
        BatchUpdateTranslator::new(cache)
    }

    #[test]
    fn test_contains_batch_update() {
        // Test with no space between alias and parentheses
        assert!(BatchUpdateTranslator::contains_batch_update(
            "UPDATE users AS u SET name = v.new_name FROM (VALUES (1, 'John')) AS v(id, new_name) WHERE u.id = v.id"
        ));
        
        // Test with space between alias and parentheses (integration test format)
        assert!(BatchUpdateTranslator::contains_batch_update(
            "UPDATE users AS u SET name = v.new_name FROM (VALUES (1, 'John')) AS v (id, new_name) WHERE u.id = v.id"
        ));
        
        assert!(BatchUpdateTranslator::contains_batch_update(
            "UPDATE products SET price = v.new_price FROM (VALUES (1, 99.99), (2, 149.99)) AS v(id, new_price) WHERE products.id = v.id"
        ));
        
        assert!(!BatchUpdateTranslator::contains_batch_update(
            "UPDATE users SET name = 'John' WHERE id = 1"
        ));
        
        assert!(!BatchUpdateTranslator::contains_batch_update(
            "SELECT * FROM users"
        ));
    }

    #[test]
    fn test_translate_simple_case() {
        let translator = create_translator();
        
        // Test that non-batch updates are passed through unchanged
        let query = "UPDATE users SET name = 'John' WHERE id = 1";
        assert_eq!(translator.translate(query, &[]), query);
    }

    #[test]
    fn test_translate_batch_update() {
        let translator = create_translator();
        
        let query = "UPDATE users AS u SET name = v.new_name FROM (VALUES (1, 'John'), (2, 'Jane')) AS v(id, new_name) WHERE u.id = v.id";
        let result = translator.translate(query, &[]);
        
        // Should contain CASE statement and WHERE IN
        assert!(result.contains("CASE"));
        assert!(result.contains("WHEN"));
        assert!(result.contains("THEN"));
        assert!(result.contains("WHERE id IN"));
    }

    #[test]
    fn test_parse_row_values() {
        let translator = create_translator();
        
        // Test simple values
        let values = translator.parse_row_values("1, 'John', 100");
        assert_eq!(values, vec!["1", "'John'", "100"]);
        
        // Test quoted values with commas
        let values = translator.parse_row_values("1, 'John, Jr.', 100");
        assert_eq!(values, vec!["1", "'John, Jr.'", "100"]);
        
        // Test escaped quotes
        let values = translator.parse_row_values(r#"1, 'John''s name', 100"#);
        assert_eq!(values, vec!["1", "'John''s name'", "100"]);
    }

    #[test]
    fn test_parse_update_values() {
        let translator = create_translator();
        
        let query = "UPDATE users AS u SET name = v.new_name, age = v.new_age FROM (VALUES (1, 'John', 25), (2, 'Jane', 30)) AS v(id, new_name, new_age) WHERE u.id = v.id";
        
        let info = translator.parse_update_values(query).unwrap();
        assert_eq!(info.table, "users");
        assert_eq!(info.table_alias, Some("u".to_string()));
        assert_eq!(info.values_alias, "v");
        assert_eq!(info.values_columns, vec!["id", "new_name", "new_age"]);
        assert_eq!(info.values_data.len(), 2);
        assert_eq!(info.values_data[0], vec!["1", "'John'", "25"]);
        assert_eq!(info.values_data[1], vec!["2", "'Jane'", "30"]);
    }

    #[test]
    fn test_generate_case_statement() {
        let translator = create_translator();
        
        let query = "UPDATE users AS u SET name = v.new_name FROM (VALUES (1, 'John'), (2, 'Jane')) AS v(id, new_name) WHERE u.id = v.id";
        let result = translator.translate(query, &[]);
        
        // Should generate proper CASE statement
        let expected_parts = vec![
            "UPDATE users SET",
            "name = CASE id",
            "WHEN 1 THEN 'John'",
            "WHEN 2 THEN 'Jane'",
            "END",
            "WHERE id IN (1, 2)"
        ];
        
        for part in expected_parts {
            assert!(result.contains(part), "Result should contain '{part}', but got: {result}");
        }
    }

    #[test]
    fn test_multiple_columns_update() {
        let translator = create_translator();
        
        let query = "UPDATE products AS p SET name = v.new_name, price = v.new_price FROM (VALUES (1, 'Widget', 99.99), (2, 'Gadget', 149.99)) AS v(id, new_name, new_price) WHERE p.id = v.id";
        let result = translator.translate(query, &[]);
        
        // Should have CASE statements for both columns
        assert!(result.contains("name = CASE"));
        assert!(result.contains("price = CASE"));
        assert!(result.contains("WHERE id IN (1, 2)"));
    }
}