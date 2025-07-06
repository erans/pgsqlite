use rusqlite::{Connection, Result};
use std::collections::HashMap;

pub mod enum_metadata;
pub mod enum_triggers;
pub use enum_metadata::{EnumMetadata, EnumType, EnumValue};
pub use enum_triggers::EnumTriggers;

/// Represents a type mapping between PostgreSQL and SQLite
#[derive(Debug, Clone)]
pub struct TypeMapping {
    pub pg_type: String,
    pub sqlite_type: String,
}

pub struct TypeMetadata;

impl TypeMetadata {
    /// Initialize the metadata table
    pub fn init(conn: &Connection) -> Result<()> {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS __pgsqlite_schema (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                pg_type TEXT NOT NULL,
                sqlite_type TEXT NOT NULL,
                PRIMARY KEY (table_name, column_name)
            )",
            [],
        )?;
        
        // Initialize ENUM metadata tables
        EnumMetadata::init(conn)?;
        
        // Initialize ENUM usage tracking table
        EnumTriggers::init_enum_usage_table(conn).ok();
        
        Ok(())
    }
    
    /// Store type mappings for a table
    pub fn store_type_mappings(
        conn: &mut Connection,
        table_name: &str,
        mappings: &HashMap<String, TypeMapping>
    ) -> Result<()> {
        let tx = conn.transaction()?;
        
        for (full_column, type_mapping) in mappings {
            // Split table.column format
            let parts: Vec<&str> = full_column.split('.').collect();
            if parts.len() == 2 && parts[0] == table_name {
                tx.execute(
                    "INSERT OR REPLACE INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) 
                     VALUES (?1, ?2, ?3, ?4)",
                    [table_name, parts[1], &type_mapping.pg_type, &type_mapping.sqlite_type],
                )?;
            }
        }
        
        tx.commit()?;
        Ok(())
    }
    
    /// Get PostgreSQL type for a column
    pub fn get_pg_type(
        conn: &Connection,
        table_name: &str,
        column_name: &str
    ) -> Result<Option<String>> {
        let mut stmt = conn.prepare(
            "SELECT pg_type FROM __pgsqlite_schema 
             WHERE table_name = ?1 AND column_name = ?2"
        )?;
        
        let mut rows = stmt.query_map([table_name, column_name], |row| {
            row.get::<_, String>(0)
        })?;
        
        if let Some(row) = rows.next() {
            Ok(Some(row?))
        } else {
            Ok(None)
        }
    }
    
    /// Get all type mappings for a table
    pub fn get_table_types(
        conn: &Connection,
        table_name: &str
    ) -> Result<HashMap<String, String>> {
        let mut stmt = conn.prepare(
            "SELECT column_name, pg_type FROM __pgsqlite_schema 
             WHERE table_name = ?1"
        )?;
        
        let rows = stmt.query_map([table_name], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        
        let mut mappings = HashMap::new();
        for row in rows {
            let (col, typ) = row?;
            mappings.insert(col, typ);
        }
        
        Ok(mappings)
    }
}