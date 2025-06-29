use regex::Regex;
use std::collections::HashMap;
use crate::metadata::TypeMapping;
use crate::types::TypeMapper;

pub struct CreateTableTranslator;

impl CreateTableTranslator {
    /// Translate PostgreSQL CREATE TABLE statement to SQLite
    pub fn translate(pg_sql: &str) -> Result<(String, HashMap<String, TypeMapping>), String> {
        let mut type_mapping = HashMap::new();
        
        // Basic regex to match CREATE TABLE - use DOTALL flag to match newlines
        let create_regex = Regex::new(r"(?is)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s*\((.*)\)").unwrap();
        
        if let Some(captures) = create_regex.captures(pg_sql) {
            let table_name = captures.get(1).unwrap().as_str();
            let columns_str = captures.get(2).unwrap().as_str();
            
            // Parse columns
            let sqlite_columns = Self::parse_and_translate_columns(columns_str, table_name, &mut type_mapping)?;
            
            // Reconstruct CREATE TABLE
            let sqlite_sql = format!("CREATE TABLE {} ({})", table_name, sqlite_columns);
            
            Ok((sqlite_sql, type_mapping))
        } else {
            // Not a CREATE TABLE statement, return as-is
            Ok((pg_sql.to_string(), type_mapping))
        }
    }
    
    fn parse_and_translate_columns(
        columns_str: &str,
        table_name: &str,
        type_mapping: &mut HashMap<String, TypeMapping>
    ) -> Result<String, String> {
        let mut sqlite_columns = Vec::new();
        let mut paren_depth = 0;
        let mut current_column = String::new();
        
        for ch in columns_str.chars() {
            match ch {
                '(' => {
                    paren_depth += 1;
                    current_column.push(ch);
                }
                ')' => {
                    paren_depth -= 1;
                    current_column.push(ch);
                }
                ',' if paren_depth == 0 => {
                    // End of column definition
                    let translated = Self::translate_column_definition(
                        current_column.trim(),
                        table_name,
                        type_mapping
                    )?;
                    sqlite_columns.push(translated);
                    current_column.clear();
                }
                _ => {
                    current_column.push(ch);
                }
            }
        }
        
        // Don't forget the last column
        if !current_column.trim().is_empty() {
            let translated = Self::translate_column_definition(
                current_column.trim(),
                table_name,
                type_mapping
            )?;
            sqlite_columns.push(translated);
        }
        
        Ok(sqlite_columns.join(", "))
    }
    
    fn translate_column_definition(
        column_def: &str,
        table_name: &str,
        type_mapping: &mut HashMap<String, TypeMapping>
    ) -> Result<String, String> {
        // Handle constraints (PRIMARY KEY, FOREIGN KEY, etc.)
        if column_def.to_uppercase().starts_with("PRIMARY KEY") 
            || column_def.to_uppercase().starts_with("FOREIGN KEY")
            || column_def.to_uppercase().starts_with("UNIQUE")
            || column_def.to_uppercase().starts_with("CHECK")
            || column_def.to_uppercase().starts_with("CONSTRAINT") {
            return Ok(column_def.to_string());
        }
        
        // Parse column name and type
        let parts: Vec<&str> = column_def.split_whitespace().collect();
        if parts.is_empty() {
            return Ok(column_def.to_string());
        }
        
        let column_name = parts[0];
        if parts.len() < 2 {
            return Ok(column_def.to_string());
        }
        
        // Extract the PostgreSQL type (handle multi-word types and parametric types)
        let mut pg_type = parts[1].to_uppercase();
        let mut type_end_idx = 2;
        
        // Handle multi-word types like "TIMESTAMP WITH TIME ZONE", "DOUBLE PRECISION", etc.
        if parts.len() > 2 {
            // Check for known multi-word type patterns
            let potential_multiword = format!("{} {}", pg_type, parts[2].to_uppercase());
            if Self::is_multiword_type_start(&potential_multiword) {
                let mut combined = pg_type.clone();
                for (i, part) in parts[2..].iter().enumerate() {
                    combined.push(' ');
                    combined.push_str(&part.to_uppercase());
                    type_end_idx = 2 + i + 1;
                    
                    // Check if we've completed a known multi-word type
                    if Self::is_complete_multiword_type(&combined) {
                        break;
                    }
                    
                    // Stop if we hit a constraint keyword
                    if Self::is_constraint_keyword(part) {
                        // Remove the last part we added since it's not part of the type
                        combined = combined.rsplit_once(' ').map(|(s, _)| s.to_string()).unwrap_or(combined);
                        type_end_idx -= 1;
                        break;
                    }
                }
                pg_type = combined;
            }
        }
        
        // Handle types with parameters like VARCHAR(255) or NUMERIC(10,2)
        if parts.len() > type_end_idx && parts[type_end_idx].starts_with('(') {
            let mut combined = pg_type.clone();
            for (i, part) in parts[type_end_idx..].iter().enumerate() {
                combined.push(' ');
                combined.push_str(part);
                if part.contains(')') {
                    type_end_idx = type_end_idx + i + 1;
                    break;
                }
            }
            pg_type = combined;
        }
        
        // Translate to SQLite type using TypeMapper
        let type_mapper = TypeMapper::new();
        let sqlite_type = type_mapper.pg_to_sqlite_for_create_table(&pg_type);
        
        // Normalize the PostgreSQL type name (convert SQLite-style names to PostgreSQL equivalents)
        let normalized_pg_type = Self::normalize_pg_type_name(&pg_type);
        
        // Store both PostgreSQL and SQLite types
        let mapping_key = format!("{}.{}", table_name, column_name);
        type_mapping.insert(mapping_key, TypeMapping {
            pg_type: normalized_pg_type,
            sqlite_type: sqlite_type.clone(),
        });
        
        // Reconstruct the column definition with SQLite type
        let mut result = format!("{} {}", column_name, sqlite_type);
        
        // Add any remaining parts (constraints, defaults, etc.)
        let mut skip_next = false;
        for (i, part) in parts[type_end_idx..].iter().enumerate() {
            if skip_next {
                skip_next = false;
                continue;
            }
            
            // Special handling for SERIAL - skip PRIMARY KEY as it's included in the type translation
            if pg_type.to_uppercase() == "SERIAL" || pg_type.to_uppercase() == "BIGSERIAL" {
                if part.to_uppercase() == "PRIMARY" {
                    // Skip "PRIMARY" and check if next is "KEY"
                    if let Some(next_part) = parts.get(type_end_idx + i + 1) {
                        if next_part.to_uppercase() == "KEY" {
                            skip_next = true;
                        }
                    }
                    continue;
                }
            }
            
            result.push(' ');
            result.push_str(part);
        }
        
        Ok(result)
    }
    
    fn is_multiword_type_start(type_str: &str) -> bool {
        let start_patterns = [
            "TIMESTAMP WITH", "TIMESTAMP WITHOUT", "TIME WITH", "TIME WITHOUT",
            "DOUBLE PRECISION", "CHARACTER VARYING", "BIT VARYING"
        ];
        start_patterns.iter().any(|pattern| type_str.starts_with(pattern))
    }
    
    fn is_complete_multiword_type(type_str: &str) -> bool {
        let complete_types = [
            "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITHOUT TIME ZONE",
            "TIME WITH TIME ZONE", "TIME WITHOUT TIME ZONE",
            "DOUBLE PRECISION", "CHARACTER VARYING", "BIT VARYING"
        ];
        complete_types.iter().any(|complete| type_str == *complete)
    }
    
    fn is_constraint_keyword(word: &str) -> bool {
        let keywords = [
            "PRIMARY", "FOREIGN", "UNIQUE", "CHECK", "NOT", "NULL", "DEFAULT",
            "REFERENCES", "CONSTRAINT", "KEY"
        ];
        keywords.iter().any(|keyword| word.to_uppercase() == *keyword)
    }
    
    /// Normalize SQLite-style type names to their PostgreSQL equivalents
    fn normalize_pg_type_name(type_name: &str) -> String {
        match type_name.to_uppercase().as_str() {
            "BLOB" => "BYTEA".to_string(),
            _ => type_name.to_string(),
        }
    }
}