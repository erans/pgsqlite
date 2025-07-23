use lazy_static::lazy_static;
use regex::Regex;
use rusqlite::Connection;

lazy_static! {
    // Match FTS operators: @@ for match, @> and <@ for contains
    static ref FTS_MATCH_REGEX: Regex = Regex::new(
        r"(?i)\b(\w+(?:\.\w+)?)\s*@@\s*(.+?)(?:\s+AND|\s+OR|\s+WHERE|\s+GROUP|\s+ORDER|\s+LIMIT|;|$)"
    ).unwrap();
    
    // Match to_tsvector function calls
    static ref TO_TSVECTOR_REGEX: Regex = Regex::new(
        r"(?i)\bto_tsvector\s*\(\s*(?:'([^']+)')?\s*,?\s*([^)]+)\)"
    ).unwrap();
    
    // Match to_tsquery and related functions
    static ref TO_TSQUERY_REGEX: Regex = Regex::new(
        r"(?i)\b(to_tsquery|plainto_tsquery|phraseto_tsquery|websearch_to_tsquery)\s*\(\s*(?:'([^']+)')?\s*,?\s*([^)]+)\)"
    ).unwrap();
    
    // Match CREATE TABLE with tsvector columns
    static ref CREATE_TABLE_TSVECTOR_REGEX: Regex = Regex::new(
        r"(?is)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s*\((.*)\)"
    ).unwrap();
    
    // Match column definitions with tsvector type
    static ref TSVECTOR_COLUMN_REGEX: Regex = Regex::new(
        r"(?i)(\w+)\s+tsvector"
    ).unwrap();
}

pub struct FtsTranslator;

impl Default for FtsTranslator {
    fn default() -> Self {
        Self::new()
    }
}

impl FtsTranslator {
    pub fn new() -> Self {
        Self
    }
    
    
    /// Check if query contains FTS operations
    pub fn contains_fts_operations(query: &str) -> bool {
        // Fast early-exit checks using simple string operations
        // Most queries won't contain FTS operations, so we can avoid expensive regex matching
        
        // Check for FTS-specific keywords first (cheap string operations)
        let query_lower = query.to_lowercase();
        
        // Quick exit for obvious non-FTS queries
        if !query_lower.contains("@@") && 
           !query_lower.contains("to_tsvector") && 
           !query_lower.contains("to_tsquery") && 
           !query_lower.contains("plainto_tsquery") && 
           !query_lower.contains("phraseto_tsquery") && 
           !query_lower.contains("websearch_to_tsquery") && 
           !query_lower.contains("tsvector") {
            return false;
        }
        
        // Only run expensive regex operations if basic string checks suggest FTS content
        FTS_MATCH_REGEX.is_match(query) ||
        TO_TSVECTOR_REGEX.is_match(query) ||
        TO_TSQUERY_REGEX.is_match(query) ||
        (query.to_uppercase().contains("CREATE TABLE") && query_lower.contains("tsvector"))
    }
    
    /// Translate CREATE TABLE statements with tsvector columns
    pub fn translate_create_table(&self, query: &str, _conn: Option<&Connection>) -> anyhow::Result<Vec<String>> {
        if let Some(caps) = CREATE_TABLE_TSVECTOR_REGEX.captures(query) {
            let table_name = caps.get(1).unwrap().as_str();
            let columns_str = caps.get(2).unwrap().as_str();
            
            let mut translated_queries = Vec::new();
            let mut fts_columns = Vec::new();
            
            // Find all tsvector columns
            for col_match in TSVECTOR_COLUMN_REGEX.captures_iter(columns_str) {
                let column_name = col_match.get(1).unwrap().as_str();
                fts_columns.push(column_name.to_string());
            }
            
            // Create the main table with tsvector columns as TEXT
            let modified_query = query.replace("tsvector", "TEXT");
            translated_queries.push(modified_query);
            
            // Create FTS5 shadow tables for each tsvector column
            for column_name in &fts_columns {
                let fts_table_name = format!("__pgsqlite_fts_{table_name}_{column_name}");
                
                let fts_create = format!(
                    "CREATE VIRTUAL TABLE {fts_table_name} USING fts5(
                        content,
                        weights UNINDEXED,
                        lexemes UNINDEXED,
                        tokenize = 'porter unicode61'
                    )"
                );
                translated_queries.push(fts_create);
                
                // Insert metadata
                let metadata_insert = format!(
                    "INSERT INTO __pgsqlite_fts_metadata 
                     (table_name, column_name, fts_table_name, config_name, tokenizer)
                     VALUES ('{table_name}', '{column_name}', '{fts_table_name}', 'english', 'porter unicode61')"
                );
                translated_queries.push(metadata_insert);
                
                // Update schema table
                let schema_update = format!(
                    "UPDATE __pgsqlite_schema 
                     SET fts_table_name = '{fts_table_name}', fts_config = 'english'
                     WHERE table_name = '{table_name}' AND column_name = '{column_name}'"
                );
                translated_queries.push(schema_update);
            }
            
            Ok(translated_queries)
        } else {
            Ok(vec![query.to_string()])
        }
    }
    
    /// Translate INSERT statements with to_tsvector() calls
    pub fn translate_insert(&self, query: &str, conn: Option<&Connection>) -> anyhow::Result<Vec<String>> {
        if !TO_TSVECTOR_REGEX.is_match(query) {
            return Ok(vec![query.to_string()]);
        }
        
        let mut translated_queries = Vec::new();
        let mut modified_query = query.to_string();
        let mut fts_inserts = Vec::new();
        
        // Parse INSERT statement to extract table name and columns
        let insert_regex = regex::Regex::new(
            r"(?is)INSERT\s+INTO\s+(\w+)\s*(?:\(([^)]+)\))?\s*VALUES\s*\((.+)\)"
        )?;
        
        if let Some(caps) = insert_regex.captures(query) {
            let table_name = caps.get(1).unwrap().as_str();
            let columns_str = caps.get(2).map(|m| m.as_str()).unwrap_or("");
            let values_str = caps.get(3).unwrap().as_str();
            
            // Split columns and values
            let columns: Vec<String> = if columns_str.is_empty() {
                // Get columns from schema if not specified
                if let Some(conn) = conn {
                    self.get_table_columns(conn, table_name)?
                } else {
                    vec![]
                }
            } else {
                columns_str.split(',').map(|s| s.trim().to_string()).collect()
            };
            
            // Find to_tsvector calls in values and replace them
            let mut value_parts = Vec::new();
            let mut current_value = String::new();
            let mut paren_depth = 0;
            let mut in_string = false;
            let mut escape_next = false;
            
            for ch in values_str.chars() {
                if escape_next {
                    current_value.push(ch);
                    escape_next = false;
                    continue;
                }
                
                match ch {
                    '\\' if in_string => {
                        escape_next = true;
                        current_value.push(ch);
                    }
                    '\'' => {
                        in_string = !in_string;
                        current_value.push(ch);
                    }
                    '(' if !in_string => {
                        paren_depth += 1;
                        current_value.push(ch);
                    }
                    ')' if !in_string => {
                        paren_depth -= 1;
                        current_value.push(ch);
                    }
                    ',' if !in_string && paren_depth == 0 => {
                        value_parts.push(current_value.trim().to_string());
                        current_value.clear();
                    }
                    _ => {
                        current_value.push(ch);
                    }
                }
            }
            
            if !current_value.trim().is_empty() {
                value_parts.push(current_value.trim().to_string());
            }
            
            // Process each value and create FTS inserts for to_tsvector calls
            for (i, value) in value_parts.iter().enumerate() {
                if let Some(tsvector_match) = TO_TSVECTOR_REGEX.captures(value) {
                    let config = tsvector_match.get(1).map(|m| m.as_str()).unwrap_or("english");
                    let text_content = tsvector_match.get(2).unwrap().as_str();
                    
                    if i < columns.len() {
                        let column_name = &columns[i];
                        
                        // Create FTS insert for this tsvector column
                        let fts_table_name = format!("__pgsqlite_fts_{table_name}_{column_name}");
                        
                        // Replace to_tsvector with JSON metadata
                        let json_metadata = format!(
                            "'{{\"fts_ref\": \"{fts_table_name}\", \"config\": \"{config}\"}}'"
                        );
                        
                        modified_query = modified_query.replace(&tsvector_match[0], &json_metadata);
                        
                        // Create FTS table insert
                        let fts_insert = format!(
                            "INSERT INTO {fts_table_name} (rowid, content, weights, lexemes) VALUES (
                                (SELECT MAX(rowid) FROM {table_name} WHERE {column_name} = {json_metadata}),
                                {text_content},
                                '',
                                json_object()
                            )"
                        );
                        
                        fts_inserts.push(fts_insert);
                    }
                }
            }
        }
        
        // Add main table insert
        translated_queries.push(modified_query);
        
        // Add FTS table inserts
        translated_queries.extend(fts_inserts);
        
        Ok(translated_queries)
    }
    
    /// Get column names for a table from the database schema
    fn get_table_columns(&self, conn: &Connection, table_name: &str) -> anyhow::Result<Vec<String>> {
        let mut stmt = conn.prepare("PRAGMA table_info(?1)")?;
        let column_names = stmt.query_map([table_name], |row| {
            row.get::<_, String>(1) // Column name is at index 1
        })?
        .collect::<Result<Vec<_>, rusqlite::Error>>()?;
        
        Ok(column_names)
    }
    
    /// Translate SELECT statements with FTS operators
    pub fn translate_select(&self, query: &str, conn: Option<&Connection>) -> anyhow::Result<String> {
        let mut translated = query.to_string();
        
        // Translate @@ operator to FTS5 MATCH
        if let Some(caps) = FTS_MATCH_REGEX.captures(query) {
            let column_ref = caps.get(1).unwrap().as_str();
            let query_expr = caps.get(2).unwrap().as_str();
            
            // Parse column reference (could be table.column or just column)
            let (table_name, column_name) = if column_ref.contains('.') {
                let parts: Vec<&str> = column_ref.split('.').collect();
                let alias_or_table = parts[0];
                
                // Check if this is an alias by looking at the FROM clause
                let actual_table = self.resolve_table_alias(query, alias_or_table)
                    .unwrap_or_else(|| alias_or_table.to_string());
                    
                (Some(actual_table), parts[1])
            } else {
                // Try to extract table name from the FROM clause
                let inferred_table = self.extract_table_name_from_query(query);
                (inferred_table, column_ref)
            };
            
            // Look up FTS table name from metadata
            let fts_table_name = if let Some(conn) = conn {
                if let Some(ref table) = table_name {
                    self.get_fts_table_name(conn, table, column_name)?
                } else {
                    // Try to infer table name from query context or use fallback
                    format!("__pgsqlite_fts_table_{column_name}")
                }
            } else {
                format!("__pgsqlite_fts_{}_{}", table_name.as_deref().unwrap_or("table"), column_name)
            };
            
            // Process the query expression to handle to_tsquery calls
            let processed_query_expr = self.translate_tsquery_functions(query_expr)?;
            
            // Use a custom SQLite function approach to avoid MATCH syntax issues
            // Replace the @@ operator with a custom function call that's parser-friendly
            let original_match = format!("{column_ref} @@ {query_expr}");
            
            // Create FTS condition using a custom pgsqlite_fts_match function
            // Use the original column reference (could be alias) for rowid access
            let rowid_ref = if column_ref.contains('.') {
                let parts: Vec<&str> = column_ref.split('.').collect();
                format!("{}.rowid", parts[0])
            } else {
                "rowid".to_string()
            };
            
            let fts_condition = format!(
                "pgsqlite_fts_match('{fts_table_name}', {rowid_ref}, {processed_query_expr})"
            );
            
            // Replace the FTS operator with the FTS5 subquery
            translated = translated.replace(&original_match, &fts_condition);
        }
        
        // Translate any remaining to_tsquery calls
        translated = self.translate_tsquery_functions(&translated)?;
        
        Ok(translated)
    }
    
    /// Get FTS table name from metadata
    fn get_fts_table_name(&self, conn: &Connection, table_name: &str, column_name: &str) -> anyhow::Result<String> {
        let mut stmt = conn.prepare(
            "SELECT fts_table_name FROM __pgsqlite_fts_metadata WHERE table_name = ?1 AND column_name = ?2"
        )?;
        
        match stmt.query_row([table_name, column_name], |row| {
            row.get::<_, String>(0)
        }) {
            Ok(fts_table_name) => Ok(fts_table_name),
            Err(_) => {
                // Fallback to default naming pattern
                Ok(format!("__pgsqlite_fts_{table_name}_{column_name}"))
            }
        }
    }
    
    /// Translate tsquery function calls to FTS5 syntax
    fn translate_tsquery_functions(&self, query: &str) -> anyhow::Result<String> {
        let mut result = query.to_string();
        
        for caps in TO_TSQUERY_REGEX.captures_iter(query) {
            let function_name = caps.get(1).unwrap().as_str();
            let _config = caps.get(2).map(|m| m.as_str());
            let query_text = caps.get(3).unwrap().as_str();
            
            // Convert PostgreSQL query syntax to FTS5
            let fts5_query = match function_name {
                "to_tsquery" => self.convert_tsquery_to_fts5(query_text)?,
                "plainto_tsquery" => self.convert_plain_to_fts5(query_text)?,
                "phraseto_tsquery" => self.convert_phrase_to_fts5(query_text)?,
                "websearch_to_tsquery" => self.convert_websearch_to_fts5(query_text)?,
                _ => query_text.to_string(),
            };
            
            // Replace the function call with the FTS5 query
            let full_match = caps.get(0).unwrap().as_str();
            result = result.replace(full_match, &format!("'{fts5_query}'"));
        }
        
        Ok(result)
    }
    
    /// Convert PostgreSQL tsquery syntax to FTS5 MATCH syntax
    fn convert_tsquery_to_fts5(&self, query: &str) -> anyhow::Result<String> {
        // Remove quotes if present
        let query = query.trim_matches('\'').trim_matches('"');
        
        // Convert operators with proper spacing
        let result = query
            .replace(" & ", " AND ")
            .replace("&", " AND ")
            .replace(" | ", " OR ")
            .replace("|", " OR ")
            .replace("!", "NOT ")
            .replace(":*", "*");  // Prefix matching
            
        Ok(result)
    }
    
    /// Convert plain text to FTS5 query (all terms with AND)
    fn convert_plain_to_fts5(&self, query: &str) -> anyhow::Result<String> {
        let query = query.trim_matches('\'').trim_matches('"');
        let terms: Vec<&str> = query.split_whitespace().collect();
        Ok(terms.join(" AND "))
    }
    
    /// Convert phrase query to FTS5 (exact phrase match)
    fn convert_phrase_to_fts5(&self, query: &str) -> anyhow::Result<String> {
        let query = query.trim_matches('\'').trim_matches('"');
        Ok(format!("\"{query}\""))
    }
    
    /// Convert web search syntax to FTS5
    fn convert_websearch_to_fts5(&self, query: &str) -> anyhow::Result<String> {
        let query = query.trim_matches('\'').trim_matches('"');
        // Simple implementation - could be enhanced
        Ok(query.to_string())
    }
    
    /// Extract table name from FROM clause in SELECT query
    fn extract_table_name_from_query(&self, query: &str) -> Option<String> {
        // Handle both "FROM table" and "FROM table alias" patterns
        // Always capture the first word after FROM, which is the actual table name
        let from_regex = regex::Regex::new(r"(?i)\bFROM\s+(\w+)").ok()?;
        from_regex.captures(query)
            .and_then(|caps| caps.get(1))
            .map(|m| m.as_str().to_string())
    }
    
    /// Resolve table alias to actual table name
    fn resolve_table_alias(&self, query: &str, alias: &str) -> Option<String> {
        // Look for "FROM table_name alias" pattern
        let alias_regex = regex::RegexBuilder::new(&format!(r"(?i)\bFROM\s+(\w+)\s+{}\b", regex::escape(alias)))
            .build().ok()?;
        alias_regex.captures(query)
            .and_then(|caps| caps.get(1))
            .map(|m| m.as_str().to_string())
    }
    
    /// Main entry point for translating queries
    pub fn translate(&self, query: &str, conn: Option<&Connection>) -> anyhow::Result<Vec<String>> {
        let query_upper = query.to_uppercase();
        
        if query_upper.starts_with("CREATE TABLE") {
            self.translate_create_table(query, conn)
        } else if query_upper.starts_with("INSERT") {
            self.translate_insert(query, conn)
        } else if query_upper.starts_with("SELECT") {
            Ok(vec![self.translate_select(query, conn)?])
        } else if query_upper.starts_with("DELETE") || query_upper.starts_with("UPDATE") {
            // DELETE and UPDATE queries can also contain FTS operators in WHERE clause
            Ok(vec![self.translate_select(query, conn)?])
        } else {
            Ok(vec![query.to_string()])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_contains_fts_operations() {
        assert!(FtsTranslator::contains_fts_operations("SELECT * FROM docs WHERE content @@ to_tsquery('search')"));
        assert!(FtsTranslator::contains_fts_operations("INSERT INTO docs (content) VALUES (to_tsvector('hello world'))"));
        assert!(FtsTranslator::contains_fts_operations("CREATE TABLE docs (id INT, content tsvector)"));
        assert!(!FtsTranslator::contains_fts_operations("SELECT * FROM docs WHERE id = 1"));
    }
    
    #[test]
    fn test_convert_tsquery_to_fts5() {
        let translator = FtsTranslator::new();
        
        assert_eq!(
            translator.convert_tsquery_to_fts5("'cat & dog'").unwrap(),
            "cat AND dog"
        );
        assert_eq!(
            translator.convert_tsquery_to_fts5("'cat | dog'").unwrap(),
            "cat OR dog"
        );
        assert_eq!(
            translator.convert_tsquery_to_fts5("'!cat'").unwrap(),
            "NOT cat"
        );
        assert_eq!(
            translator.convert_tsquery_to_fts5("'cat:*'").unwrap(),
            "cat*"
        );
    }
    
    #[test]
    fn test_convert_plain_to_fts5() {
        let translator = FtsTranslator::new();
        
        assert_eq!(
            translator.convert_plain_to_fts5("'quick brown fox'").unwrap(),
            "quick AND brown AND fox"
        );
    }
    
    #[test]
    fn test_convert_phrase_to_fts5() {
        let translator = FtsTranslator::new();
        
        assert_eq!(
            translator.convert_phrase_to_fts5("'quick brown fox'").unwrap(),
            "\"quick brown fox\""
        );
    }
    
    #[test]
    fn test_resolve_table_alias() {
        let translator = FtsTranslator::new();
        
        let query = "SELECT d.id, d.title FROM documents d WHERE d.search_vector @@ to_tsquery('english', 'fox')";
        let resolved = translator.resolve_table_alias(query, "d");
        assert_eq!(resolved, Some("documents".to_string()));
        
        // Test case without alias
        let query2 = "SELECT id, title FROM documents WHERE search_vector @@ to_tsquery('english', 'fox')";
        let resolved2 = translator.resolve_table_alias(query2, "documents");
        assert_eq!(resolved2, None); // Should not find alias pattern
    }
    
    #[test]
    fn test_select_with_alias_translation() {
        let translator = FtsTranslator::new();
        
        let query = "SELECT d.id, d.title FROM documents d WHERE d.search_vector @@ to_tsquery('english', 'fox')";
        let result = translator.translate(query, None).unwrap();
        assert_eq!(result.len(), 1);
        
        let translated = &result[0];
        // Should use the actual table name 'documents', not the alias 'd'
        assert!(translated.contains("__pgsqlite_fts_documents_search_vector"));
        assert!(!translated.contains("__pgsqlite_fts_d_search_vector"));
        assert!(translated.contains("pgsqlite_fts_match"));
        assert!(translated.contains("d.rowid")); // Should use the alias for rowid reference
    }
    
    #[test]
    fn test_performance_optimization_early_exit() {
        // Test that non-FTS queries exit early without expensive regex operations
        let non_fts_queries = vec![
            "SELECT * FROM users WHERE id = 1",
            "INSERT INTO products (name, price) VALUES ('item', 10.99)",
            "UPDATE orders SET status = 'shipped' WHERE id = 42",
            "DELETE FROM temp_data WHERE created < '2024-01-01'",
            "SELECT COUNT(*) FROM inventory",
            "CREATE TABLE simple (id INT, name TEXT)",
        ];
        
        for query in non_fts_queries {
            // This should return false very quickly due to early-exit optimization
            assert!(!FtsTranslator::contains_fts_operations(query));
        }
        
        // Test that FTS queries are correctly detected
        let fts_queries = vec![
            "SELECT * FROM docs WHERE content @@ to_tsquery('search')",
            "INSERT INTO docs (content) VALUES (to_tsvector('hello world'))",
            "CREATE TABLE docs (id INT, content tsvector)",
            "SELECT * FROM articles WHERE title_vector @@ plainto_tsquery('news')",
        ];
        
        for query in fts_queries {
            assert!(FtsTranslator::contains_fts_operations(query));
        }
    }
}