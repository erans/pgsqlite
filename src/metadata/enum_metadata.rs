use rusqlite::{Connection, Result, params, OptionalExtension};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Offset for generated ENUM type OIDs to avoid conflicts with built-in types
const ENUM_TYPE_OID_OFFSET: i32 = 10000;
/// Offset for generated ENUM value OIDs
const ENUM_VALUE_OID_OFFSET: i32 = 20000;

/// Represents an ENUM type definition
#[derive(Debug, Clone)]
pub struct EnumType {
    pub type_oid: i32,
    pub type_name: String,
    pub namespace_oid: i32,
}

/// Represents an ENUM value within a type
#[derive(Debug, Clone)]
pub struct EnumValue {
    pub value_oid: i32,
    pub type_oid: i32,
    pub label: String,
    pub sort_order: f64,
}

pub struct EnumMetadata;

impl EnumMetadata {
    /// Initialize the ENUM metadata tables
    pub fn init(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "-- Track ENUM type definitions
            CREATE TABLE IF NOT EXISTS __pgsqlite_enum_types (
                type_oid INTEGER PRIMARY KEY,
                type_name TEXT NOT NULL UNIQUE,
                namespace_oid INTEGER DEFAULT 2200, -- public schema
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Track ENUM values with ordering
            CREATE TABLE IF NOT EXISTS __pgsqlite_enum_values (
                value_oid INTEGER PRIMARY KEY,
                type_oid INTEGER NOT NULL,
                label TEXT NOT NULL,
                sort_order REAL NOT NULL,
                FOREIGN KEY (type_oid) REFERENCES __pgsqlite_enum_types(type_oid),
                UNIQUE (type_oid, label)
            );
            
            -- Index for efficient lookups
            CREATE INDEX IF NOT EXISTS idx_enum_values_type ON __pgsqlite_enum_values(type_oid);
            CREATE INDEX IF NOT EXISTS idx_enum_values_label ON __pgsqlite_enum_values(type_oid, label);"
        )?;
        Ok(())
    }
    
    /// Generate a stable OID for an ENUM type based on its name
    pub fn generate_type_oid(type_name: &str) -> i32 {
        let mut hasher = DefaultHasher::new();
        type_name.hash(&mut hasher);
        let hash = hasher.finish() as i32;
        ENUM_TYPE_OID_OFFSET + (hash.abs() % 1000000)
    }
    
    /// Generate a stable OID for an ENUM value based on type OID and label
    pub fn generate_value_oid(type_oid: i32, label: &str) -> i32 {
        let mut hasher = DefaultHasher::new();
        type_oid.hash(&mut hasher);
        label.hash(&mut hasher);
        let hash = hasher.finish() as i32;
        ENUM_VALUE_OID_OFFSET + (hash.abs() % 1000000)
    }
    
    /// Create a new ENUM type with its values
    pub fn create_enum_type(
        conn: &mut Connection,
        type_name: &str,
        values: &[&str],
        namespace_oid: Option<i32>,
    ) -> Result<i32> {
        // Ensure metadata tables exist
        Self::init(conn)?;
        
        let tx = conn.transaction()?;
        
        // Generate type OID
        let type_oid = Self::generate_type_oid(type_name);
        let ns_oid = namespace_oid.unwrap_or(2200); // default to public schema
        
        // Insert type definition
        tx.execute(
            "INSERT INTO __pgsqlite_enum_types (type_oid, type_name, namespace_oid) 
             VALUES (?1, ?2, ?3)",
            params![type_oid, type_name, ns_oid],
        )?;
        
        // Insert values with sort order
        for (i, label) in values.iter().enumerate() {
            let value_oid = Self::generate_value_oid(type_oid, label);
            let sort_order = (i + 1) as f64;
            
            tx.execute(
                "INSERT INTO __pgsqlite_enum_values (value_oid, type_oid, label, sort_order) 
                 VALUES (?1, ?2, ?3, ?4)",
                params![value_oid, type_oid, label, sort_order],
            )?;
        }
        
        tx.commit()?;
        Ok(type_oid)
    }
    
    /// Add a new value to an existing ENUM type
    pub fn add_enum_value(
        conn: &mut Connection,
        type_name: &str,
        new_value: &str,
        before_value: Option<&str>,
        after_value: Option<&str>,
    ) -> Result<()> {
        let tx = conn.transaction()?;
        
        // Get type OID
        let type_oid: i32 = tx.query_row(
            "SELECT type_oid FROM __pgsqlite_enum_types WHERE type_name = ?1",
            [type_name],
            |row| row.get(0),
        )?;
        
        // Calculate sort order
        let sort_order = if let Some(before) = before_value {
            // Insert before specified value
            let before_order: f64 = tx.query_row(
                "SELECT sort_order FROM __pgsqlite_enum_values 
                 WHERE type_oid = ?1 AND label = ?2",
                params![type_oid, before],
                |row| row.get(0),
            )?;
            
            // Get previous value's sort order (if exists)
            let prev_order: Option<f64> = tx.query_row(
                "SELECT MAX(sort_order) FROM __pgsqlite_enum_values 
                 WHERE type_oid = ?1 AND sort_order < ?2",
                params![type_oid, before_order],
                |row| row.get(0),
            ).ok();
            
            // Place halfway between previous and before value
            match prev_order {
                Some(prev) => (prev + before_order) / 2.0,
                None => before_order / 2.0,
            }
        } else if let Some(after) = after_value {
            // Insert after specified value
            let after_order: f64 = tx.query_row(
                "SELECT sort_order FROM __pgsqlite_enum_values 
                 WHERE type_oid = ?1 AND label = ?2",
                params![type_oid, after],
                |row| row.get(0),
            )?;
            
            // Get next value's sort order (if exists)
            let next_order: Option<f64> = tx.query_row(
                "SELECT MIN(sort_order) FROM __pgsqlite_enum_values 
                 WHERE type_oid = ?1 AND sort_order > ?2",
                params![type_oid, after_order],
                |row| row.get(0),
            ).ok();
            
            // Place halfway between after value and next
            match next_order {
                Some(next) => (after_order + next) / 2.0,
                None => after_order + 1.0,
            }
        } else {
            // Append to end
            let max_order: Option<f64> = tx.query_row(
                "SELECT MAX(sort_order) FROM __pgsqlite_enum_values WHERE type_oid = ?1",
                [type_oid],
                |row| row.get(0),
            ).ok();
            
            max_order.unwrap_or(0.0) + 1.0
        };
        
        // Insert new value
        let value_oid = Self::generate_value_oid(type_oid, new_value);
        tx.execute(
            "INSERT INTO __pgsqlite_enum_values (value_oid, type_oid, label, sort_order) 
             VALUES (?1, ?2, ?3, ?4)",
            params![value_oid, type_oid, new_value, sort_order],
        )?;
        
        tx.commit()?;
        Ok(())
    }
    
    /// Get ENUM type information by name
    pub fn get_enum_type(conn: &Connection, type_name: &str) -> Result<Option<EnumType>> {
        // Check if tables exist first
        let table_exists: bool = conn.query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='__pgsqlite_enum_types' LIMIT 1",
            [],
            |_| Ok(true)
        ).unwrap_or(false);
        
        if !table_exists {
            return Ok(None);
        }
        
        conn.query_row(
            "SELECT type_oid, type_name, namespace_oid 
             FROM __pgsqlite_enum_types WHERE type_name = ?1",
            [type_name],
            |row| {
                Ok(EnumType {
                    type_oid: row.get(0)?,
                    type_name: row.get(1)?,
                    namespace_oid: row.get(2)?,
                })
            },
        ).optional()
    }
    
    /// Get ENUM type information by OID
    pub fn get_enum_type_by_oid(conn: &Connection, type_oid: i32) -> Result<Option<EnumType>> {
        conn.query_row(
            "SELECT type_oid, type_name, namespace_oid 
             FROM __pgsqlite_enum_types WHERE type_oid = ?1",
            [type_oid],
            |row| {
                Ok(EnumType {
                    type_oid: row.get(0)?,
                    type_name: row.get(1)?,
                    namespace_oid: row.get(2)?,
                })
            },
        ).optional()
    }
    
    /// Get all values for an ENUM type
    pub fn get_enum_values(conn: &Connection, type_oid: i32) -> Result<Vec<EnumValue>> {
        let mut stmt = conn.prepare(
            "SELECT value_oid, type_oid, label, sort_order 
             FROM __pgsqlite_enum_values 
             WHERE type_oid = ?1 
             ORDER BY sort_order"
        )?;
        
        let values = stmt.query_map([type_oid], |row| {
            Ok(EnumValue {
                value_oid: row.get(0)?,
                type_oid: row.get(1)?,
                label: row.get(2)?,
                sort_order: row.get(3)?,
            })
        })?;
        
        values.collect()
    }
    
    /// Get ENUM value by label
    pub fn get_enum_value(conn: &Connection, type_oid: i32, label: &str) -> Result<Option<EnumValue>> {
        conn.query_row(
            "SELECT value_oid, type_oid, label, sort_order 
             FROM __pgsqlite_enum_values 
             WHERE type_oid = ?1 AND label = ?2",
            params![type_oid, label],
            |row| {
                Ok(EnumValue {
                    value_oid: row.get(0)?,
                    type_oid: row.get(1)?,
                    label: row.get(2)?,
                    sort_order: row.get(3)?,
                })
            },
        ).optional()
    }
    
    /// Get ENUM value by OID
    pub fn get_enum_value_by_oid(conn: &Connection, value_oid: i32) -> Result<Option<EnumValue>> {
        conn.query_row(
            "SELECT value_oid, type_oid, label, sort_order 
             FROM __pgsqlite_enum_values 
             WHERE value_oid = ?1",
            [value_oid],
            |row| {
                Ok(EnumValue {
                    value_oid: row.get(0)?,
                    type_oid: row.get(1)?,
                    label: row.get(2)?,
                    sort_order: row.get(3)?,
                })
            },
        ).optional()
    }
    
    /// Validate if a value is valid for an ENUM type
    pub fn is_valid_enum_value(conn: &Connection, type_oid: i32, label: &str) -> Result<bool> {
        let count: i32 = conn.query_row(
            "SELECT COUNT(*) FROM __pgsqlite_enum_values 
             WHERE type_oid = ?1 AND label = ?2",
            params![type_oid, label],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }
    
    /// Drop an ENUM type and all its values
    pub fn drop_enum_type(conn: &mut Connection, type_name: &str) -> Result<()> {
        let tx = conn.transaction()?;
        
        // Get type OID
        let type_oid: i32 = tx.query_row(
            "SELECT type_oid FROM __pgsqlite_enum_types WHERE type_name = ?1",
            [type_name],
            |row| row.get(0),
        )?;
        
        // Delete values first (foreign key constraint)
        tx.execute(
            "DELETE FROM __pgsqlite_enum_values WHERE type_oid = ?1",
            [type_oid],
        )?;
        
        // Delete type
        tx.execute(
            "DELETE FROM __pgsqlite_enum_types WHERE type_oid = ?1",
            [type_oid],
        )?;
        
        tx.commit()?;
        Ok(())
    }
    
    /// Get all ENUM types
    pub fn get_all_enum_types(conn: &Connection) -> Result<Vec<EnumType>> {
        // Check if tables exist first
        let table_exists: bool = conn.query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='__pgsqlite_enum_types' LIMIT 1",
            [],
            |_| Ok(true)
        ).unwrap_or(false);
        
        if !table_exists {
            return Ok(Vec::new());
        }
        
        let mut stmt = conn.prepare(
            "SELECT type_oid, type_name, namespace_oid 
             FROM __pgsqlite_enum_types 
             ORDER BY type_name"
        )?;
        
        let types = stmt.query_map([], |row| {
            Ok(EnumType {
                type_oid: row.get(0)?,
                type_name: row.get(1)?,
                namespace_oid: row.get(2)?,
            })
        })?;
        
        types.collect()
    }
    
    /// Check if a type name is an ENUM
    pub fn is_enum_type(conn: &Connection, type_name: &str) -> Result<bool> {
        let count: i32 = conn.query_row(
            "SELECT COUNT(*) FROM __pgsqlite_enum_types WHERE type_name = ?1",
            [type_name],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_enum_metadata_creation() {
        let conn = Connection::open_in_memory().unwrap();
        EnumMetadata::init(&conn).unwrap();
        
        // Verify tables exist
        let count: i32 = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='__pgsqlite_enum_types'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(count, 1);
    }
    
    #[test]
    fn test_create_enum_type() {
        let mut conn = Connection::open_in_memory().unwrap();
        EnumMetadata::init(&conn).unwrap();
        
        let type_oid = EnumMetadata::create_enum_type(
            &mut conn,
            "mood",
            &["happy", "sad", "angry"],
            None,
        ).unwrap();
        
        assert!(type_oid >= ENUM_TYPE_OID_OFFSET);
        
        // Verify type exists
        let enum_type = EnumMetadata::get_enum_type(&conn, "mood").unwrap().unwrap();
        assert_eq!(enum_type.type_name, "mood");
        assert_eq!(enum_type.type_oid, type_oid);
        
        // Verify values
        let values = EnumMetadata::get_enum_values(&conn, type_oid).unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0].label, "happy");
        assert_eq!(values[1].label, "sad");
        assert_eq!(values[2].label, "angry");
    }
}