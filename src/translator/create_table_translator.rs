use regex::Regex;
use std::collections::HashMap;
use crate::metadata::{TypeMapping, EnumMetadata};
use crate::types::TypeMapper;
use rusqlite::Connection;
use std::cell::RefCell;

#[derive(Debug)]
pub struct CreateTableResult {
    pub sql: String,
    pub type_mappings: HashMap<String, TypeMapping>,
    pub enum_columns: Vec<(String, String)>, // (column_name, enum_type)
}

thread_local! {
    static ENUM_COLUMNS: RefCell<Vec<(String, String)>> = RefCell::new(Vec::new());
}

pub struct CreateTableTranslator;

#[allow(unused_variables)]
impl CreateTableTranslator {
    /// Translate PostgreSQL CREATE TABLE statement to SQLite
    pub fn translate(pg_sql: &str) -> Result<(String, HashMap<String, TypeMapping>), String> {
        Self::translate_with_connection(pg_sql, None)
    }
    
    /// Translate PostgreSQL CREATE TABLE statement to SQLite with connection for ENUM support
    pub fn translate_with_connection(
        pg_sql: &str, 
        conn: Option<&Connection>
    ) -> Result<(String, HashMap<String, TypeMapping>), String> {
        let result = Self::translate_with_connection_full(pg_sql, conn)?;
        Ok((result.sql, result.type_mappings))
    }
    
    /// Translate PostgreSQL CREATE TABLE statement to SQLite with full result including ENUM columns
    pub fn translate_with_connection_full(
        pg_sql: &str, 
        conn: Option<&Connection>
    ) -> Result<CreateTableResult, String> {
        let mut type_mapping = HashMap::new();
        let mut check_constraints = Vec::new();
        
        // Clear enum columns tracker
        ENUM_COLUMNS.with(|ec| ec.borrow_mut().clear());
        
        // Basic regex to match CREATE TABLE - use DOTALL flag to match newlines
        let create_regex = Regex::new(r"(?is)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s*\((.*)\)").unwrap();
        
        if let Some(captures) = create_regex.captures(pg_sql) {
            let table_name = captures.get(1).unwrap().as_str();
            let columns_str = captures.get(2).unwrap().as_str();
            
            // Parse columns
            let sqlite_columns = Self::parse_and_translate_columns(
                columns_str, 
                table_name, 
                &mut type_mapping,
                &mut check_constraints,
                conn
            )?;
            
            // Add CHECK constraints for ENUMs
            let mut final_columns = sqlite_columns;
            for constraint in check_constraints {
                final_columns.push_str(", ");
                final_columns.push_str(&constraint);
            }
            
            // Reconstruct CREATE TABLE
            let sqlite_sql = format!("CREATE TABLE {} ({})", table_name, final_columns);
            
            // Collect enum columns
            let enum_columns = ENUM_COLUMNS.with(|ec| ec.borrow().clone());
            
            Ok(CreateTableResult {
                sql: sqlite_sql,
                type_mappings: type_mapping,
                enum_columns,
            })
        } else {
            // Not a CREATE TABLE statement, return as-is
            Ok(CreateTableResult {
                sql: pg_sql.to_string(),
                type_mappings: type_mapping,
                enum_columns: Vec::new(),
            })
        }
    }
    
    fn parse_and_translate_columns(
        columns_str: &str,
        table_name: &str,
        type_mapping: &mut HashMap<String, TypeMapping>,
        check_constraints: &mut Vec<String>,
        conn: Option<&Connection>
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
                        type_mapping,
                        check_constraints,
                        conn
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
                type_mapping,
                check_constraints,
                conn
            )?;
            sqlite_columns.push(translated);
        }
        
        Ok(sqlite_columns.join(", "))
    }
    
    fn translate_column_definition(
        column_def: &str,
        table_name: &str,
        type_mapping: &mut HashMap<String, TypeMapping>,
        check_constraints: &mut Vec<String>,
        conn: Option<&Connection>
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
        
        // Check if this is an ENUM type
        let (sqlite_type, normalized_pg_type) = if let Some(conn) = conn {
            // Check if the type is an ENUM
            match EnumMetadata::get_enum_type(conn, &pg_type.to_lowercase()) {
                Ok(Some(_enum_type)) => {
                    // It's an ENUM type - store as TEXT
                    // Note: We don't add CHECK constraints here anymore.
                    // Instead, we'll create triggers after the table is created.
                    let sqlite_type = "TEXT".to_string();
                    
                    // Store enum column info for later trigger creation
                    ENUM_COLUMNS.with(|ec| {
                        ec.borrow_mut().push((column_name.to_string(), pg_type.to_lowercase().to_string()));
                    });
                    
                    (sqlite_type, pg_type.to_lowercase())
                }
                _ => {
                    // Not an ENUM, use regular type mapping
                    let type_mapper = TypeMapper::new();
                    let sqlite_type = type_mapper.pg_to_sqlite_for_create_table(&pg_type);
                    let normalized_pg_type = Self::normalize_pg_type_name(&pg_type);
                    (sqlite_type, normalized_pg_type)
                }
            }
        } else {
            // No connection available, use regular type mapping
            let type_mapper = TypeMapper::new();
            let sqlite_type = type_mapper.pg_to_sqlite_for_create_table(&pg_type);
            let normalized_pg_type = Self::normalize_pg_type_name(&pg_type);
            (sqlite_type, normalized_pg_type)
        };
        
        // Extract type modifier (length constraint) if present
        let type_modifier = Self::extract_type_modifier(&pg_type);
        
        // Store both PostgreSQL and SQLite types with modifier
        let mapping_key = format!("{}.{}", table_name, column_name);
        type_mapping.insert(mapping_key, TypeMapping {
            pg_type: normalized_pg_type,
            sqlite_type: sqlite_type.clone(),
            type_modifier,
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
    
    /// Extract type modifier from type definition (e.g., VARCHAR(255) -> Some(255))
    fn extract_type_modifier(type_name: &str) -> Option<i32> {
        // Look for pattern like TYPE(n) or TYPE(n,m)
        if let Some(start) = type_name.find('(') {
            if let Some(end) = type_name.find(')') {
                let params = &type_name[start + 1..end];
                // For now, we only care about the first parameter (length)
                if let Some(first_param) = params.split(',').next() {
                    if let Ok(length) = first_param.trim().parse::<i32>() {
                        return Some(length);
                    }
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_extract_type_modifier() {
        // Basic cases
        assert_eq!(CreateTableTranslator::extract_type_modifier("VARCHAR(255)"), Some(255));
        assert_eq!(CreateTableTranslator::extract_type_modifier("CHAR(10)"), Some(10));
        assert_eq!(CreateTableTranslator::extract_type_modifier("CHARACTER VARYING(100)"), Some(100));
        
        // With spaces
        assert_eq!(CreateTableTranslator::extract_type_modifier("VARCHAR ( 50 )"), Some(50));
        
        // Without modifier
        assert_eq!(CreateTableTranslator::extract_type_modifier("VARCHAR"), None);
        assert_eq!(CreateTableTranslator::extract_type_modifier("TEXT"), None);
        
        // Edge cases
        assert_eq!(CreateTableTranslator::extract_type_modifier("VARCHAR(0)"), Some(0));
        assert_eq!(CreateTableTranslator::extract_type_modifier("VARCHAR(1000000)"), Some(1000000));
        
        // Invalid cases
        assert_eq!(CreateTableTranslator::extract_type_modifier("VARCHAR()"), None);
        assert_eq!(CreateTableTranslator::extract_type_modifier("VARCHAR(abc)"), None);
        
        // NUMERIC with precision and scale - only first param
        assert_eq!(CreateTableTranslator::extract_type_modifier("NUMERIC(10,2)"), Some(10));
    }
    
    #[test]
    fn test_translate_varchar_constraints() {
        let sql = "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50),
            email VARCHAR(255),
            code CHAR(10)
        )";
        
        let (_translated, mappings) = CreateTableTranslator::translate(sql).unwrap();
        
        // Check that types were mapped correctly
        assert!(mappings.contains_key("users.name"));
        assert!(mappings.contains_key("users.email"));
        assert!(mappings.contains_key("users.code"));
        
        // Check type modifiers
        assert_eq!(mappings["users.name"].type_modifier, Some(50));
        assert_eq!(mappings["users.email"].type_modifier, Some(255));
        assert_eq!(mappings["users.code"].type_modifier, Some(10));
        
        // Check pg_type is preserved
        assert_eq!(mappings["users.name"].pg_type, "VARCHAR(50)");
        assert_eq!(mappings["users.code"].pg_type, "CHAR(10)");
    }
    
    #[test]
    fn test_translate_without_constraints() {
        let sql = "CREATE TABLE test (
            id INTEGER PRIMARY KEY,
            description TEXT,
            data VARCHAR
        )";
        
        let (_, mappings) = CreateTableTranslator::translate(sql).unwrap();
        
        // VARCHAR without length should have no modifier
        assert_eq!(mappings["test.data"].type_modifier, None);
        assert_eq!(mappings["test.data"].pg_type, "VARCHAR");
    }
    
    #[test]
    fn test_mixed_case_types() {
        let sql = "CREATE TABLE test (
            col1 VarChar(10),
            col2 CHARACTER varying(20),
            col3 Character(5)
        )";
        
        let (_, mappings) = CreateTableTranslator::translate(sql).unwrap();
        
        assert_eq!(mappings["test.col1"].type_modifier, Some(10));
        assert_eq!(mappings["test.col2"].type_modifier, Some(20));
        assert_eq!(mappings["test.col3"].type_modifier, Some(5));
    }
}