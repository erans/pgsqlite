use rusqlite::Connection;
use anyhow::Result;
use tracing::{debug, info};

/// Populate PostgreSQL catalog tables with constraint information for a newly created table
pub fn populate_constraints_for_table(conn: &Connection, table_name: &str) -> Result<()> {
    info!("Populating constraints for table: {}", table_name);
    
    // Get the CREATE TABLE statement from SQLite
    let create_sql = get_create_table_sql(conn, table_name)?;
    debug!("CREATE TABLE SQL: {}", create_sql);
    
    // Generate table OID (consistent with pg_class view)
    let table_oid = generate_table_oid(table_name);
    
    // Parse and populate constraints
    populate_table_constraints(conn, table_name, &create_sql, &table_oid)?;
    
    // Parse and populate column defaults
    populate_column_defaults(conn, table_name, &create_sql, &table_oid)?;
    
    // Populate indexes (including those created by UNIQUE constraints)
    populate_table_indexes(conn, table_name, &table_oid)?;
    
    info!("Successfully populated constraints for table: {}", table_name);
    Ok(())
}

/// Get the CREATE TABLE statement for a table from sqlite_master
fn get_create_table_sql(conn: &Connection, table_name: &str) -> Result<String> {
    let mut stmt = conn.prepare("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1")?;
    let sql: String = stmt.query_row([table_name], |row| row.get(0))?;
    Ok(sql)
}

/// Generate table OID using the same algorithm as the pg_class view
fn generate_table_oid(name: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    (((hasher.finish() & 0x7FFFFFFF) % 1000000 + 16384) as i32).to_string()
}

/// Populate pg_constraint table with constraint information
fn populate_table_constraints(conn: &Connection, table_name: &str, create_sql: &str, table_oid: &str) -> Result<()> {
    let constraints = parse_table_constraints(table_name, create_sql);
    
    for constraint in constraints {
        conn.execute(
            "INSERT OR IGNORE INTO pg_constraint (
                oid, conname, contype, conrelid, conkey, consrc
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            [
                &constraint.oid,
                &constraint.name,
                &constraint.contype,
                table_oid,
                &constraint.columns.join(","),
                &constraint.definition,
            ]
        )?;
        
        debug!("Inserted constraint: {} (type: {}) for table: {}", 
               constraint.name, constraint.contype, table_name);
    }
    
    Ok(())
}

/// Populate pg_attrdef table with column default information
fn populate_column_defaults(conn: &Connection, table_name: &str, create_sql: &str, table_oid: &str) -> Result<()> {
    let defaults = parse_column_defaults(table_name, create_sql);
    
    for default in defaults {
        conn.execute(
            "INSERT OR IGNORE INTO pg_attrdef (
                oid, adrelid, adnum, adsrc
            ) VALUES (?1, ?2, ?3, ?4)",
            [
                &default.oid,
                table_oid,
                &default.column_num.to_string(),
                &default.default_expr,
            ]
        )?;
        
        debug!("Inserted default: column {} = '{}' for table: {}", 
               default.column_num, default.default_expr, table_name);
    }
    
    Ok(())
}

/// Populate pg_index table with index information
fn populate_table_indexes(conn: &Connection, table_name: &str, table_oid: &str) -> Result<()> {
    // Get indexes for this table from sqlite_master
    let mut stmt = conn.prepare("
        SELECT name, sql FROM sqlite_master 
        WHERE type = 'index' AND tbl_name = ?1 AND sql IS NOT NULL
    ")?;
    
    let indexes = stmt.query_map([table_name], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    
    for index_result in indexes {
        let (index_name, index_sql) = index_result?;
        let index_oid = generate_table_oid(&index_name);
        
        // Parse index information
        let is_unique = index_sql.to_uppercase().contains("UNIQUE");
        let is_primary = index_name.contains("primary") || index_name.contains("pkey");
        
        conn.execute(
            "INSERT OR IGNORE INTO pg_index (
                indexrelid, indrelid, indnatts, indnkeyatts, 
                indisunique, indisprimary
            ) VALUES (?1, ?2, 1, 1, ?3, ?4)",
            [
                &index_oid,
                table_oid,
                &(is_unique as i32).to_string(),
                &(is_primary as i32).to_string(),
            ]
        )?;
        
        debug!("Inserted index: {} (unique: {}, primary: {}) for table: {}", 
               index_name, is_unique, is_primary, table_name);
    }
    
    Ok(())
}

/// Information about a constraint
#[derive(Debug)]
struct ConstraintInfo {
    oid: String,
    name: String,
    contype: String,
    columns: Vec<String>,
    definition: String,
}

/// Information about a column default
#[derive(Debug)]
struct DefaultInfo {
    oid: String,
    column_num: i16,
    default_expr: String,
}

/// Parse table constraints from CREATE TABLE statement
fn parse_table_constraints(table_name: &str, create_sql: &str) -> Vec<ConstraintInfo> {
    use regex::Regex;
    
    let mut constraints = Vec::new();
    
    // Parse PRIMARY KEY constraints
    // Look for both inline PRIMARY KEY and table-level PRIMARY KEY
    if let Ok(pk_regex) = Regex::new(r"(?i)\b(\w+)\s+[^,\)]*\bPRIMARY\s+KEY\b") {
        for cap in pk_regex.captures_iter(create_sql) {
            if let Some(column_name) = cap.get(1) {
                constraints.push(ConstraintInfo {
                    oid: generate_table_oid(&format!("{}_pkey", table_name)),
                    name: format!("{}_pkey", table_name),
                    contype: "p".to_string(),
                    columns: vec![column_name.as_str().to_string()],
                    definition: "PRIMARY KEY".to_string(),
                });
            }
        }
    }
    
    // Parse table-level PRIMARY KEY constraints
    if let Ok(table_pk_regex) = Regex::new(r"(?i)PRIMARY\s+KEY\s*\(\s*([^)]+)\s*\)") {
        for cap in table_pk_regex.captures_iter(create_sql) {
            if let Some(columns_str) = cap.get(1) {
                let columns: Vec<String> = columns_str.as_str()
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect();
                constraints.push(ConstraintInfo {
                    oid: generate_table_oid(&format!("{}_pkey", table_name)),
                    name: format!("{}_pkey", table_name),
                    contype: "p".to_string(),
                    columns,
                    definition: "PRIMARY KEY".to_string(),
                });
            }
        }
    }
    
    // Parse UNIQUE constraints
    if let Ok(unique_regex) = Regex::new(r"(?i)\b(\w+)\s+[^,\)]*\bUNIQUE\b") {
        for cap in unique_regex.captures_iter(create_sql) {
            if let Some(column_name) = cap.get(1) {
                constraints.push(ConstraintInfo {
                    oid: generate_table_oid(&format!("{}_{}_key", table_name, column_name.as_str())),
                    name: format!("{}_{}_key", table_name, column_name.as_str()),
                    contype: "u".to_string(),
                    columns: vec![column_name.as_str().to_string()],
                    definition: "UNIQUE".to_string(),
                });
            }
        }
    }
    
    // Parse table-level UNIQUE constraints
    if let Ok(table_unique_regex) = Regex::new(r"(?i)UNIQUE\s*\(\s*([^)]+)\s*\)") {
        for cap in table_unique_regex.captures_iter(create_sql) {
            if let Some(columns_str) = cap.get(1) {
                let columns: Vec<String> = columns_str.as_str()
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect();
                let constraint_name = format!("{}_{}_key", table_name, columns.join("_"));
                constraints.push(ConstraintInfo {
                    oid: generate_table_oid(&constraint_name),
                    name: constraint_name,
                    contype: "u".to_string(),
                    columns,
                    definition: "UNIQUE".to_string(),
                });
            }
        }
    }
    
    // Parse CHECK constraints
    if let Ok(check_regex) = Regex::new(r"(?i)CHECK\s*\(\s*([^)]+)\s*\)") {
        for (i, cap) in check_regex.captures_iter(create_sql).enumerate() {
            if let Some(check_expr) = cap.get(1) {
                let constraint_name = format!("{}_check{}", table_name, i + 1);
                constraints.push(ConstraintInfo {
                    oid: generate_table_oid(&constraint_name),
                    name: constraint_name,
                    contype: "c".to_string(),
                    columns: vec![], // CHECK constraints don't have specific columns
                    definition: format!("CHECK ({})", check_expr.as_str()),
                });
            }
        }
    }
    
    // Parse NOT NULL constraints (treated as check constraints in PostgreSQL)
    if let Ok(not_null_regex) = Regex::new(r"(?i)\b(\w+)\s+[^,\)]*\bNOT\s+NULL\b") {
        for cap in not_null_regex.captures_iter(create_sql) {
            if let Some(column_name) = cap.get(1) {
                let constraint_name = format!("{}_{}_not_null", table_name, column_name.as_str());
                constraints.push(ConstraintInfo {
                    oid: generate_table_oid(&constraint_name),
                    name: constraint_name,
                    contype: "c".to_string(),
                    columns: vec![column_name.as_str().to_string()],
                    definition: format!("{} IS NOT NULL", column_name.as_str()),
                });
            }
        }
    }
    
    constraints
}

/// Parse column defaults from CREATE TABLE statement
fn parse_column_defaults(table_name: &str, create_sql: &str) -> Vec<DefaultInfo> {
    use regex::Regex;
    
    let mut defaults = Vec::new();
    
    // Parse DEFAULT clauses - look for column definitions with DEFAULT
    if let Ok(default_regex) = Regex::new(r"(?i)\b(\w+)\s+[^,\)]*\bDEFAULT\s+([^,\)]+)") {
        for cap in default_regex.captures_iter(create_sql) {
            if let (Some(column_name), Some(default_value)) = (cap.get(1), cap.get(2)) {
                // Get column number by counting columns before this one
                let column_num = get_column_number(create_sql, column_name.as_str()).unwrap_or(1);
                
                defaults.push(DefaultInfo {
                    oid: generate_table_oid(&format!("{}_{}_default", table_name, column_name.as_str())),
                    column_num,
                    default_expr: default_value.as_str().trim().to_string(),
                });
            }
        }
    }
    
    defaults
}

/// Get the column number (1-based) for a given column name in a CREATE TABLE statement
fn get_column_number(create_sql: &str, target_column: &str) -> Option<i16> {
    use regex::Regex;
    
    // Extract the column definitions from CREATE TABLE
    if let Ok(table_regex) = Regex::new(r"(?i)CREATE\s+TABLE\s+[^(]+\(\s*(.+)\s*\)") {
        if let Some(cap) = table_regex.captures(create_sql) {
            if let Some(columns_part) = cap.get(1) {
                // Split by comma and look for our target column
                let columns_str = columns_part.as_str();
                let mut column_count = 0i16;
                
                // Simple column parsing - split by commas but be careful of nested parentheses
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
                            column_count += 1;
                            if current_column.trim().starts_with(target_column) {
                                return Some(column_count);
                            }
                            current_column.clear();
                        }
                        _ => {
                            current_column.push(ch);
                        }
                    }
                }
                
                // Check the last column
                if !current_column.trim().is_empty() {
                    column_count += 1;
                    if current_column.trim().starts_with(target_column) {
                        return Some(column_count);
                    }
                }
            }
        }
    }
    
    None
}