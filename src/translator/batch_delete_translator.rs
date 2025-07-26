use crate::translator::metadata::TranslationMetadata;
use parking_lot::Mutex;
use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;
use once_cell::sync::Lazy;

static DELETE_USING_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?ims)DELETE\s+FROM\s+(\w+)(?:\s+AS\s+(\w+))?\s+USING\s+\(VALUES\s+(.+?)\)\s+AS\s+(\w+)\s*\(\s*([^)]+)\s*\)\s+WHERE\s+(.+)").unwrap()
});

static VALUES_ROW_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\(([^)]+)\)").unwrap()
});

/// Translates PostgreSQL DELETE ... USING (VALUES ...) syntax to SQLite-compatible format
pub struct BatchDeleteTranslator {
    #[allow(dead_code)]
    decimal_tables_cache: Arc<Mutex<HashMap<String, bool>>>,
}

#[derive(Debug)]
struct DeleteInfo {
    table: String,
    table_alias: Option<String>,
    values_data: Vec<Vec<String>>,
    #[allow(dead_code)]
    values_alias: String,
    values_columns: Vec<String>,
    where_clause: String,
}

impl BatchDeleteTranslator {
    pub fn new(decimal_tables_cache: Arc<Mutex<HashMap<String, bool>>>) -> Self {
        BatchDeleteTranslator {
            decimal_tables_cache,
        }
    }

    /// Check if the query contains DELETE ... USING (VALUES ...) pattern
    pub fn contains_batch_delete(query: &str) -> bool {
        DELETE_USING_REGEX.is_match(query)
    }

    /// Translate DELETE ... USING (VALUES ...) to SQLite-compatible format
    pub fn translate(&self, query: &str, _params: &[Vec<u8>]) -> String {
        if !Self::contains_batch_delete(query) {
            return query.to_string();
        }

        match self.parse_delete_using(query) {
            Ok(info) => self.generate_where_in_statement(&info),
            Err(_) => {
                // If parsing fails, return original query
                query.to_string()
            }
        }
    }

    /// Translate with metadata for integration with the query pipeline
    pub fn translate_with_metadata(&self, query: &str, params: &[Vec<u8>]) -> (String, TranslationMetadata) {
        if !Self::contains_batch_delete(query) {
            return (query.to_string(), TranslationMetadata::default());
        }

        let translated = self.translate(query, params);
        let metadata = TranslationMetadata::default();
        
        (translated, metadata)
    }

    fn parse_delete_using(&self, query: &str) -> Result<DeleteInfo, &'static str> {
        let captures = DELETE_USING_REGEX.captures(query)
            .ok_or("Failed to match DELETE USING pattern")?;

        let table = captures.get(1).unwrap().as_str().to_string();
        let table_alias = captures.get(2).map(|m| m.as_str().to_string());
        let values_section = captures.get(3).unwrap().as_str();
        let values_alias = captures.get(4).unwrap().as_str().to_string();
        let values_columns_str = captures.get(5).unwrap().as_str();
        let where_clause = captures.get(6).unwrap().as_str().to_string();

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

        Ok(DeleteInfo {
            table,
            table_alias,
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

    fn generate_where_in_statement(&self, info: &DeleteInfo) -> String {
        if info.values_data.is_empty() {
            return format!("DELETE FROM {}", info.table);
        }

        // Extract the key column from WHERE clause (simplified)
        let key_column = self.extract_key_column(&info.where_clause, &info.table_alias);
        let key_column_index = info.values_columns.iter()
            .position(|col| col == &key_column)
            .unwrap_or(0);

        // For single column (most common case), use simple WHERE IN
        if info.values_columns.len() == 1 {
            let key_values: Vec<String> = info.values_data.iter()
                .filter_map(|row| row.get(key_column_index).cloned())
                .collect();

            return format!(
                "DELETE FROM {} WHERE {} IN ({})",
                info.table,
                key_column,
                key_values.join(", ")
            );
        }

        // For multiple columns, use EXISTS with subquery
        self.generate_exists_statement(info, &key_column)
    }

    fn generate_exists_statement(&self, info: &DeleteInfo, _key_column: &str) -> String {
        // Build UNION ALL subquery for multi-column conditions
        let mut union_parts = Vec::new();
        
        for (i, row) in info.values_data.iter().enumerate() {
            let mut select_parts = Vec::new();
            
            for (j, column) in info.values_columns.iter().enumerate() {
                if let Some(value) = row.get(j) {
                    select_parts.push(format!("{value} as {column}"));
                }
            }
            
            if i == 0 {
                union_parts.push(format!("SELECT {}", select_parts.join(", ")));
            } else {
                union_parts.push(format!("UNION ALL SELECT {}", select_parts.join(", ")));
            }
        }

        // Build WHERE conditions for EXISTS
        let where_conditions: Vec<String> = info.values_columns.iter()
            .map(|col| format!("{}.{} = conditions.{}", info.table, col, col))
            .collect();

        format!(
            "DELETE FROM {} WHERE EXISTS (SELECT 1 FROM ({}) AS conditions WHERE {})",
            info.table,
            union_parts.join(" "),
            where_conditions.join(" AND ")
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_translator() -> BatchDeleteTranslator {
        let cache = Arc::new(Mutex::new(HashMap::new()));
        BatchDeleteTranslator::new(cache)
    }

    #[test]
    fn test_contains_batch_delete() {
        // Test with no space between alias and parentheses
        assert!(BatchDeleteTranslator::contains_batch_delete(
            "DELETE FROM users AS u USING (VALUES (1), (2)) AS v(id) WHERE u.id = v.id"
        ));
        
        // Test with space between alias and parentheses
        assert!(BatchDeleteTranslator::contains_batch_delete(
            "DELETE FROM users AS u USING (VALUES (1), (2)) AS v (id) WHERE u.id = v.id"
        ));
        
        assert!(BatchDeleteTranslator::contains_batch_delete(
            "DELETE FROM products USING (VALUES (1), (2), (3)) AS v(id) WHERE products.id = v.id"
        ));
        
        assert!(!BatchDeleteTranslator::contains_batch_delete(
            "DELETE FROM users WHERE id = 1"
        ));
        
        assert!(!BatchDeleteTranslator::contains_batch_delete(
            "SELECT * FROM users"
        ));
    }

    #[test]
    fn test_translate_simple_case() {
        let translator = create_translator();
        
        // Test that non-batch deletes are passed through unchanged
        let query = "DELETE FROM users WHERE id = 1";
        assert_eq!(translator.translate(query, &[]), query);
    }

    #[test]
    fn test_translate_batch_delete_single_column() {
        let translator = create_translator();
        
        let query = "DELETE FROM users AS u USING (VALUES (1), (2), (3)) AS v(id) WHERE u.id = v.id";
        let result = translator.translate(query, &[]);
        
        // Should contain WHERE IN statement
        assert!(result.contains("DELETE FROM users"));
        assert!(result.contains("WHERE id IN"));
        assert!(result.contains("1, 2, 3"));
    }

    #[test]
    fn test_translate_batch_delete_multi_column() {
        let translator = create_translator();
        
        let query = "DELETE FROM users AS u USING (VALUES (1, 'active'), (2, 'inactive')) AS v(id, status) WHERE u.id = v.id AND u.status = v.status";
        let result = translator.translate(query, &[]);
        
        // Should contain EXISTS statement for multi-column
        assert!(result.contains("DELETE FROM users"));
        assert!(result.contains("WHERE EXISTS"));
        assert!(result.contains("SELECT 1"));
        assert!(result.contains("UNION ALL"));
    }

    #[test]
    fn test_parse_row_values() {
        let translator = create_translator();
        
        // Test simple values
        let values = translator.parse_row_values("1, 2, 3");
        assert_eq!(values, vec!["1", "2", "3"]);
        
        // Test quoted values with commas
        let values = translator.parse_row_values("1, 'active, pending', 'test'");
        assert_eq!(values, vec!["1", "'active, pending'", "'test'"]);
        
        // Test escaped quotes
        let values = translator.parse_row_values(r#"1, 'John''s account', 'test'"#);
        assert_eq!(values, vec!["1", "'John''s account'", "'test'"]);
    }

    #[test]
    fn test_parse_delete_using() {
        let translator = create_translator();
        
        let query = "DELETE FROM users AS u USING (VALUES (1), (2), (3)) AS v(id) WHERE u.id = v.id";
        
        let info = translator.parse_delete_using(query).unwrap();
        assert_eq!(info.table, "users");
        assert_eq!(info.table_alias, Some("u".to_string()));
        assert_eq!(info.values_alias, "v");
        assert_eq!(info.values_columns, vec!["id"]);
        assert_eq!(info.values_data.len(), 3);
        assert_eq!(info.values_data[0], vec!["1"]);
        assert_eq!(info.values_data[1], vec!["2"]);
        assert_eq!(info.values_data[2], vec!["3"]);
    }

    #[test]
    fn test_generate_where_in_statement() {
        let translator = create_translator();
        
        let query = "DELETE FROM users AS u USING (VALUES (1), (2), (3)) AS v(id) WHERE u.id = v.id";
        let result = translator.translate(query, &[]);
        
        // Should generate proper WHERE IN statement
        let expected_parts = vec![
            "DELETE FROM users",
            "WHERE id IN (1, 2, 3)"
        ];
        
        for part in expected_parts {
            assert!(result.contains(part), "Result should contain '{part}', but got: {result}");
        }
    }

    #[test]
    fn test_no_table_alias() {
        let translator = create_translator();
        
        let query = "DELETE FROM users USING (VALUES (1), (2)) AS v(id) WHERE users.id = v.id";
        let result = translator.translate(query, &[]);
        
        assert!(result.contains("DELETE FROM users"));
        assert!(result.contains("WHERE users.id IN (1, 2)"));
    }
}