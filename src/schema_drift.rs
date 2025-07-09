use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SchemaDriftError {
    #[error("Schema drift detected: {0}")]
    DriftDetected(String),
    #[error("Database error: {0}")]
    DatabaseError(#[from] rusqlite::Error),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnInfo {
    pub name: String,
    pub pg_type: String,
    pub sqlite_type: String,
    pub nullable: bool,
    pub default_value: Option<String>,
}

#[derive(Debug)]
pub struct TableDrift {
    pub table_name: String,
    pub missing_in_sqlite: Vec<ColumnInfo>,
    pub missing_in_metadata: Vec<ColumnInfo>,
    pub type_mismatches: Vec<TypeMismatch>,
}

#[derive(Debug)]
pub struct TypeMismatch {
    pub column_name: String,
    pub metadata_pg_type: String,
    pub metadata_sqlite_type: String,
    pub actual_sqlite_type: String,
}

#[derive(Debug)]
pub struct SchemaDrift {
    pub table_drifts: Vec<TableDrift>,
}

impl SchemaDrift {
    pub fn is_empty(&self) -> bool {
        self.table_drifts.is_empty()
    }

    pub fn format_report(&self) -> String {
        let mut report = String::new();
        
        for drift in &self.table_drifts {
            report.push_str(&format!("\nTable '{}' has schema drift:\n", drift.table_name));
            
            if !drift.missing_in_sqlite.is_empty() {
                report.push_str("  Columns in metadata but missing from SQLite:\n");
                for col in &drift.missing_in_sqlite {
                    report.push_str(&format!("    - {} ({})\n", col.name, col.pg_type));
                }
            }
            
            if !drift.missing_in_metadata.is_empty() {
                report.push_str("  Columns in SQLite but missing from metadata:\n");
                for col in &drift.missing_in_metadata {
                    report.push_str(&format!("    - {} ({})\n", col.name, col.sqlite_type));
                }
            }
            
            if !drift.type_mismatches.is_empty() {
                report.push_str("  Type mismatches:\n");
                for mismatch in &drift.type_mismatches {
                    report.push_str(&format!(
                        "    - {} expected SQLite type '{}' but found '{}'\n",
                        mismatch.column_name,
                        mismatch.metadata_sqlite_type,
                        mismatch.actual_sqlite_type
                    ));
                }
            }
        }
        
        report
    }
}

pub struct SchemaDriftDetector;

impl SchemaDriftDetector {
    pub fn detect_drift(conn: &Connection) -> Result<SchemaDrift, SchemaDriftError> {
        let mut table_drifts = Vec::new();
        
        // Get all tables tracked in __pgsqlite_schema
        let tracked_tables = Self::get_tracked_tables(conn)?;
        
        for table_name in tracked_tables {
            if let Some(drift) = Self::check_table_drift(conn, &table_name)? {
                table_drifts.push(drift);
            }
        }
        
        Ok(SchemaDrift { table_drifts })
    }
    
    fn get_tracked_tables(conn: &Connection) -> Result<Vec<String>, rusqlite::Error> {
        let mut stmt = conn.prepare(
            "SELECT DISTINCT table_name FROM __pgsqlite_schema ORDER BY table_name"
        )?;
        
        let tables = stmt.query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        
        Ok(tables)
    }
    
    fn check_table_drift(conn: &Connection, table_name: &str) -> Result<Option<TableDrift>, SchemaDriftError> {
        // Get metadata columns
        let metadata_columns = Self::get_metadata_columns(conn, table_name)?;
        
        // Get actual SQLite columns
        let sqlite_columns = Self::get_sqlite_columns(conn, table_name)?;
        
        // Build sets for comparison
        let metadata_names: HashSet<String> = metadata_columns.keys().cloned().collect();
        let sqlite_names: HashSet<String> = sqlite_columns.keys().cloned().collect();
        
        // Find missing columns
        let missing_in_sqlite: Vec<ColumnInfo> = metadata_names
            .difference(&sqlite_names)
            .map(|name| metadata_columns[name].clone())
            .collect();
        
        let missing_in_metadata: Vec<ColumnInfo> = sqlite_names
            .difference(&metadata_names)
            .map(|name| sqlite_columns[name].clone())
            .collect();
        
        // Check type mismatches
        let mut type_mismatches = Vec::new();
        for name in metadata_names.intersection(&sqlite_names) {
            let metadata_col = &metadata_columns[name];
            let sqlite_col = &sqlite_columns[name];
            
            // Compare SQLite types (normalize for comparison)
            let metadata_type_normalized = Self::normalize_sqlite_type(&metadata_col.sqlite_type);
            let actual_type_normalized = Self::normalize_sqlite_type(&sqlite_col.sqlite_type);
            
            if metadata_type_normalized != actual_type_normalized {
                type_mismatches.push(TypeMismatch {
                    column_name: name.clone(),
                    metadata_pg_type: metadata_col.pg_type.clone(),
                    metadata_sqlite_type: metadata_col.sqlite_type.clone(),
                    actual_sqlite_type: sqlite_col.sqlite_type.clone(),
                });
            }
        }
        
        // Only return drift if there are actual differences
        if missing_in_sqlite.is_empty() && missing_in_metadata.is_empty() && type_mismatches.is_empty() {
            Ok(None)
        } else {
            Ok(Some(TableDrift {
                table_name: table_name.to_string(),
                missing_in_sqlite,
                missing_in_metadata,
                type_mismatches,
            }))
        }
    }
    
    fn get_metadata_columns(conn: &Connection, table_name: &str) -> Result<HashMap<String, ColumnInfo>, rusqlite::Error> {
        let mut stmt = conn.prepare(
            "SELECT column_name, pg_type, sqlite_type 
             FROM __pgsqlite_schema 
             WHERE table_name = ?1"
        )?;
        
        let mut columns = HashMap::new();
        let rows = stmt.query_map([table_name], |row| {
            Ok(ColumnInfo {
                name: row.get(0)?,
                pg_type: row.get(1)?,
                sqlite_type: row.get(2)?,
                nullable: true, // We don't track this in metadata yet
                default_value: None, // We don't track this in metadata yet
            })
        })?;
        
        for row in rows {
            let col = row?;
            columns.insert(col.name.clone(), col);
        }
        
        Ok(columns)
    }
    
    fn get_sqlite_columns(conn: &Connection, table_name: &str) -> Result<HashMap<String, ColumnInfo>, rusqlite::Error> {
        let query = format!("PRAGMA table_info({})", table_name);
        let mut stmt = conn.prepare(&query)?;
        
        let mut columns = HashMap::new();
        let rows = stmt.query_map([], |row| {
            Ok(ColumnInfo {
                name: row.get(1)?,
                pg_type: String::new(), // Not available from PRAGMA
                sqlite_type: row.get(2)?,
                nullable: row.get::<_, i32>(3)? == 0,
                default_value: row.get(4)?,
            })
        })?;
        
        for row in rows {
            let col = row?;
            columns.insert(col.name.clone(), col);
        }
        
        Ok(columns)
    }
    
    fn normalize_sqlite_type(type_str: &str) -> String {
        let upper = type_str.to_uppercase();
        
        // Remove any size/precision specifications
        let base_type = if let Some(paren_pos) = upper.find('(') {
            upper[..paren_pos].trim().to_string()
        } else {
            upper.trim().to_string()
        };
        
        // Normalize common variations
        match base_type.as_str() {
            "INT" | "INT4" => "INTEGER".to_string(),
            "INT8" | "BIGINT" => "INTEGER".to_string(),
            "INT2" | "SMALLINT" => "INTEGER".to_string(),
            "FLOAT" | "DOUBLE" | "DOUBLE PRECISION" => "REAL".to_string(),
            "VARCHAR" | "CHARACTER VARYING" => "TEXT".to_string(),
            "CHAR" | "CHARACTER" => "TEXT".to_string(),
            "BOOL" | "BOOLEAN" => "INTEGER".to_string(), // SQLite stores as 0/1
            "BYTEA" => "BLOB".to_string(),
            "NUMERIC" | "DECIMAL" => "DECIMAL".to_string(),
            _ => base_type,
        }
    }
}