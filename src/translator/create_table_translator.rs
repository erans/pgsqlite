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
    pub array_columns: Vec<(String, String, i32)>, // (column_name, element_type, dimensions)
}

thread_local! {
    static ENUM_COLUMNS: RefCell<Vec<(String, String)>> = const { RefCell::new(Vec::new()) };
    static ARRAY_COLUMNS: RefCell<Vec<(String, String, i32)>> = const { RefCell::new(Vec::new()) };
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
        
        // Clear enum and array columns trackers
        ENUM_COLUMNS.with(|ec| ec.borrow_mut().clear());
        ARRAY_COLUMNS.with(|ac| ac.borrow_mut().clear());
        
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
            let sqlite_sql = format!("CREATE TABLE {table_name} ({final_columns})");
            
            // Collect enum and array columns
            let enum_columns = ENUM_COLUMNS.with(|ec| ec.borrow().clone());
            let array_columns = ARRAY_COLUMNS.with(|ac| ac.borrow().clone());
            
            Ok(CreateTableResult {
                sql: sqlite_sql,
                type_mappings: type_mapping,
                enum_columns,
                array_columns,
            })
        } else {
            // Not a CREATE TABLE statement, return as-is
            Ok(CreateTableResult {
                sql: pg_sql.to_string(),
                type_mappings: type_mapping,
                enum_columns: Vec::new(),
                array_columns: Vec::new(),
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
        let mut column_definitions = Vec::new();
        let mut serial_columns = std::collections::HashSet::new();
        
        // First pass: collect all column definitions
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
                    column_definitions.push(current_column.trim().to_string());
                    current_column.clear();
                }
                _ => {
                    current_column.push(ch);
                }
            }
        }
        
        // Don't forget the last column
        if !current_column.trim().is_empty() {
            column_definitions.push(current_column.trim().to_string());
        }
        
        // Identify SERIAL columns
        for column_def in &column_definitions {
            if let Some(column_name) = Self::extract_serial_column_name(column_def) {
                serial_columns.insert(column_name);
            }
        }
        
        // Second pass: translate columns, filtering out redundant PRIMARY KEY constraints
        for column_def in column_definitions {
            if Self::is_redundant_primary_key(&column_def, &serial_columns) {
                // Skip this PRIMARY KEY constraint as it's already handled by SERIAL
                continue;
            }
            
            let translated = Self::translate_column_definition(
                &column_def,
                table_name,
                type_mapping,
                check_constraints,
                conn
            )?;
            sqlite_columns.push(translated);
        }
        
        Ok(sqlite_columns.join(", "))
    }
    
    /// Extract column name if this is a SERIAL column definition
    fn extract_serial_column_name(column_def: &str) -> Option<String> {
        let parts: Vec<&str> = column_def.split_whitespace().collect();
        if parts.len() >= 2 {
            let pg_type = parts[1].to_uppercase();
            if pg_type == "SERIAL" || pg_type == "BIGSERIAL" {
                return Some(parts[0].to_string());
            }
        }
        None
    }
    
    /// Check if this is a PRIMARY KEY constraint that references a SERIAL column
    fn is_redundant_primary_key(column_def: &str, serial_columns: &std::collections::HashSet<String>) -> bool {
        let upper_def = column_def.to_uppercase();
        if upper_def.trim().starts_with("PRIMARY KEY") {
            // Parse PRIMARY KEY (column_name) format
            if let Some(start) = column_def.find('(') {
                if let Some(end) = column_def.find(')') {
                    let column_list = &column_def[start + 1..end];
                    let column_name = column_list.trim();
                    // Check if this references a SERIAL column (case-insensitive)
                    return serial_columns.iter().any(|serial_col| serial_col.eq_ignore_ascii_case(column_name));
                }
            }
        }
        false
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
        // Check if type already contains '(' (from splitting "NUMERIC(10," + "2)")
        if pg_type.contains('(') && !pg_type.contains(')') {
            // Need to continue collecting parts until we find the closing ')'
            let mut combined = pg_type.clone();
            for (i, part) in parts[type_end_idx..].iter().enumerate() {
                combined.push_str(part);  // Don't add space for comma-separated parameters
                if part.contains(')') {
                    type_end_idx = type_end_idx + i + 1;
                    break;
                }
            }
            pg_type = combined;
        } else if parts.len() > type_end_idx && parts[type_end_idx].starts_with('(') {
            // Handle case where type and parameters are separate: "NUMERIC" + "(255)"
            let mut combined = pg_type.clone();
            for (i, part) in parts[type_end_idx..].iter().enumerate() {
                combined.push_str(part);  // Don't add space
                if part.contains(')') {
                    type_end_idx = type_end_idx + i + 1;
                    break;
                }
            }
            pg_type = combined;
        }
        
        // Check for array types - handle [] notation
        let (is_array, element_type, dimensions) = Self::parse_array_type(&pg_type, &parts, type_end_idx);
        if is_array {
            // Adjust type_end_idx to skip array brackets
            for (i, part) in parts[type_end_idx..].iter().enumerate() {
                if part.contains('[') || part.contains(']') {
                    type_end_idx = type_end_idx + i + 1;
                } else if i > 0 && !parts[type_end_idx + i - 1].contains(']') {
                    // We've moved past the array brackets
                    break;
                }
            }
        }
        
        // Check if this is an array type first
        let (sqlite_type, normalized_pg_type) = if is_array {
            // Array types are stored as JSON TEXT
            let sqlite_type = "TEXT".to_string();
            
            // Store array column info for later metadata insertion
            ARRAY_COLUMNS.with(|ac| {
                ac.borrow_mut().push((
                    column_name.to_string(), 
                    element_type.to_lowercase(), 
                    dimensions
                ));
            });
            
            // Add CHECK constraint for JSON validation
            // Note: json_valid() doesn't accept NULL, so we check for NULL first
            let constraint_name = format!("chk_{}_{}_{}", table_name, column_name, "json");
            check_constraints.push(format!(
                "CONSTRAINT {constraint_name} CHECK ({column_name} IS NULL OR json_valid({column_name}))"
            ));
            
            (sqlite_type, pg_type.clone())
        } else if let Some(conn) = conn {
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
        let mapping_key = format!("{table_name}.{column_name}");
        type_mapping.insert(mapping_key, TypeMapping {
            pg_type: normalized_pg_type,
            sqlite_type: sqlite_type.clone(),
            type_modifier,
        });
        
        // Reconstruct the column definition with SQLite type
        let mut result = format!("{column_name} {sqlite_type}");
        
        // Add any remaining parts (constraints, defaults, etc.)
        let mut remaining_parts = Vec::new();
        let mut skip_next = false;
        for (i, part) in parts[type_end_idx..].iter().enumerate() {
            if skip_next {
                skip_next = false;
                continue;
            }
            
            // Special handling for SERIAL - skip PRIMARY KEY as it's included in the type translation
            if (pg_type.to_uppercase() == "SERIAL" || pg_type.to_uppercase() == "BIGSERIAL")
                && part.to_uppercase() == "PRIMARY" {
                    // Skip "PRIMARY" and check if next is "KEY"
                    if let Some(next_part) = parts.get(type_end_idx + i + 1) {
                        if next_part.to_uppercase() == "KEY" {
                            skip_next = true;
                        }
                    }
                    continue;
                }
            
            remaining_parts.push(*part);
        }
        
        // Join remaining parts and apply datetime translation if needed
        if !remaining_parts.is_empty() {
            let remaining_clause = remaining_parts.join(" ");
            
            // Apply datetime translation for DEFAULT clauses
            let translated_clause = if remaining_clause.to_uppercase().contains("DEFAULT") {
                use crate::translator::DateTimeTranslator;
                // Create a fake CREATE TABLE context so datetime translator uses SQLite's datetime('now')
                let fake_create_table_query = format!("CREATE TABLE temp ({column_name} {remaining_clause})");
                let translated_fake = DateTimeTranslator::translate_query(&fake_create_table_query);
                // Extract just the DEFAULT part from the translated result
                let temp_col_prefix = format!("CREATE TABLE temp ({column_name} ");
                if let Some(pos) = translated_fake.find(&temp_col_prefix) {
                    let start_pos = pos + temp_col_prefix.len();
                    let end_pos = translated_fake.rfind(')').unwrap_or(translated_fake.len());
                    translated_fake[start_pos..end_pos].to_string()
                } else {
                    remaining_clause
                }
            } else {
                remaining_clause
            };
            
            result.push(' ');
            result.push_str(&translated_clause);
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
        complete_types.contains(&type_str)
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
    
    /// Parse array type notation and return (is_array, element_type, dimensions)
    fn parse_array_type(pg_type: &str, parts: &[&str], type_start_idx: usize) -> (bool, String, i32) {
        // Check if the type ends with [] or has [] in subsequent parts
        let mut is_array = false;
        let mut element_type = pg_type.to_string();
        let mut dimensions = 0;
        
        // Check if the type itself contains []
        if pg_type.contains('[') {
            is_array = true;
            // Extract base type and count dimensions
            let base_end = pg_type.find('[').unwrap();
            element_type = pg_type[..base_end].to_string();
            dimensions = pg_type[base_end..].matches('[').count() as i32;
        } else if parts.len() > type_start_idx {
            // Check if [] appears in subsequent parts
            for part in &parts[type_start_idx..] {
                if part.starts_with('[') || *part == "[]" {
                    is_array = true;
                    dimensions += part.matches('[').count() as i32;
                    if !part.contains(']') {
                        // Multi-part array notation like [ ]
                        continue;
                    }
                    break;
                } else if dimensions > 0 && part.contains(']') {
                    // Found closing bracket
                    break;
                } else if dimensions == 0 {
                    // No array notation found yet
                    break;
                }
            }
        }
        
        // Normalize element type for known PostgreSQL array type names
        if element_type.ends_with("[]") {
            element_type = element_type[..element_type.len()-2].to_string();
        }
        
        // Ensure we have at least 1 dimension for arrays
        if is_array && dimensions == 0 {
            dimensions = 1;
        }
        
        (is_array, element_type, dimensions)
    }
    
    /// Extract type modifier from type definition
    /// For VARCHAR/CHAR: extracts length as modifier (e.g., VARCHAR(255) -> Some(255))
    /// For NUMERIC/DECIMAL: encodes precision and scale (e.g., NUMERIC(10,2) -> Some(655366))
    fn extract_type_modifier(type_name: &str) -> Option<i32> {
        // Look for pattern like TYPE(n) or TYPE(n,m)
        if let Some(start) = type_name.find('(') {
            if let Some(end) = type_name.find(')') {
                let params = &type_name[start + 1..end];
                let type_base = type_name[..start].trim().to_uppercase();
                
                // Handle NUMERIC/DECIMAL with precision and scale
                if type_base == "NUMERIC" || type_base == "DECIMAL" {
                    let parts: Vec<&str> = params.split(',').collect();
                    if let Ok(precision) = parts[0].trim().parse::<i32>() {
                        let scale = if parts.len() > 1 {
                            parts[1].trim().parse::<i32>().unwrap_or(0)
                        } else {
                            0
                        };
                        // Encode as PostgreSQL does: ((precision << 16) | scale) + VARHDRSZ
                        // VARHDRSZ = 4
                        return Some(((precision << 16) | (scale & 0xFFFF)) + 4);
                    }
                } else {
                    // For other types (VARCHAR, CHAR), just return the first parameter
                    if let Some(first_param) = params.split(',').next() {
                        if let Ok(length) = first_param.trim().parse::<i32>() {
                            return Some(length);
                        }
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
        
        // NUMERIC with precision and scale - encoded as PostgreSQL format
        // ((10 << 16) | 2) + 4 = 655366
        assert_eq!(CreateTableTranslator::extract_type_modifier("NUMERIC(10,2)"), Some(655366));
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
    
    #[test]
    fn test_parse_array_type() {
        // Test simple array types
        let (is_array, element, dims) = CreateTableTranslator::parse_array_type("INTEGER[]", &[], 0);
        assert!(is_array);
        assert_eq!(element, "INTEGER");
        assert_eq!(dims, 1);
        
        // Test multi-dimensional arrays
        let (is_array, element, dims) = CreateTableTranslator::parse_array_type("TEXT[][]", &[], 0);
        assert!(is_array);
        assert_eq!(element, "TEXT");
        assert_eq!(dims, 2);
        
        // Test array in separate parts
        let parts = vec!["column", "INTEGER", "[]"];
        let (is_array, element, dims) = CreateTableTranslator::parse_array_type("INTEGER", &parts, 2);
        assert!(is_array);
        assert_eq!(element, "INTEGER");
        assert_eq!(dims, 1);
        
        // Test non-array types
        let (is_array, _, _) = CreateTableTranslator::parse_array_type("VARCHAR(50)", &[], 0);
        assert!(!is_array);
    }
    
    #[test]
    fn test_translate_array_columns() {
        let sql = "CREATE TABLE array_test (
            id INTEGER PRIMARY KEY,
            int_array INTEGER[],
            text_array TEXT[],
            matrix REAL[][]
        )";
        
        let result = CreateTableTranslator::translate_with_connection_full(sql, None).unwrap();
        
        // Check that array columns were detected
        assert_eq!(result.array_columns.len(), 3);
        
        // Check array column metadata
        assert!(result.array_columns.iter().any(|(name, elem, dims)| {
            name == "int_array" && elem == "integer" && *dims == 1
        }));
        assert!(result.array_columns.iter().any(|(name, elem, dims)| {
            name == "text_array" && elem == "text" && *dims == 1
        }));
        assert!(result.array_columns.iter().any(|(name, elem, dims)| {
            name == "matrix" && elem == "real" && *dims == 2
        }));
        
        // Check that columns are mapped to TEXT
        assert_eq!(result.type_mappings["array_test.int_array"].sqlite_type, "TEXT");
        assert_eq!(result.type_mappings["array_test.text_array"].sqlite_type, "TEXT");
        assert_eq!(result.type_mappings["array_test.matrix"].sqlite_type, "TEXT");
        
        // Check that JSON validation constraints were added
        assert!(result.sql.contains("json_valid"));
    }
    
    #[test]
    fn test_translate_default_now() {
        let sql = "CREATE TABLE orders (
            id SERIAL PRIMARY KEY,
            customer_id INTEGER,
            order_date TIMESTAMP DEFAULT NOW(),
            total_amount DECIMAL(12,2),
            status VARCHAR(50)
        )";
        
        let result = CreateTableTranslator::translate_with_connection_full(sql, None).unwrap();
        
        println!("Translated SQL: {}", result.sql);
        
        // Check that NOW() was translated to datetime('now')
        assert!(result.sql.contains("DEFAULT datetime('now')"), 
                "Expected 'DEFAULT datetime('now')' but got: {}", result.sql);
        assert!(!result.sql.contains("DEFAULT now()"), 
                "Found 'DEFAULT now()' which should have been translated: {}", result.sql);
    }
}