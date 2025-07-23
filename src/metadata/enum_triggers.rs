use rusqlite::{Connection, params};
use crate::PgSqliteError;

/// Manages ENUM validation triggers and usage tracking
pub struct EnumTriggers;

impl EnumTriggers {
    /// Initialize the enum usage tracking table
    pub fn init_enum_usage_table(conn: &Connection) -> Result<(), PgSqliteError> {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS __pgsqlite_enum_usage (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                enum_type TEXT NOT NULL,
                PRIMARY KEY (table_name, column_name),
                FOREIGN KEY (enum_type) REFERENCES __pgsqlite_enum_types(type_name) ON DELETE CASCADE
            )",
            [],
        ).map_err(|e| PgSqliteError::Protocol(format!("Failed to create enum usage table: {e}")))?;
        
        Ok(())
    }
    
    /// Record that a table column uses an ENUM type
    pub fn record_enum_usage(
        conn: &Connection,
        table_name: &str,
        column_name: &str,
        enum_type: &str,
    ) -> Result<(), PgSqliteError> {
        conn.execute(
            "INSERT OR REPLACE INTO __pgsqlite_enum_usage (table_name, column_name, enum_type) 
             VALUES (?1, ?2, ?3)",
            params![table_name, column_name, enum_type],
        ).map_err(|e| PgSqliteError::Protocol(format!("Failed to record enum usage: {e}")))?;
        
        Ok(())
    }
    
    /// Create validation triggers for an ENUM column
    pub fn create_enum_validation_triggers(
        conn: &Connection,
        table_name: &str,
        column_name: &str,
        enum_type: &str,
    ) -> Result<(), PgSqliteError> {
        // Create INSERT trigger
        let insert_trigger_name = format!("__pgsqlite_{table_name}_{column_name}_{enum_type}_insert_check");
        
        let insert_trigger_sql = format!(
            r#"CREATE TRIGGER IF NOT EXISTS "{insert_trigger_name}"
            BEFORE INSERT ON "{table_name}"
            FOR EACH ROW
            WHEN NEW."{column_name}" IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM __pgsqlite_enum_values ev
                JOIN __pgsqlite_enum_types et ON ev.type_oid = et.type_oid
                WHERE et.type_name = '{enum_type}' AND ev.label = NEW."{column_name}"
            )
            BEGIN
                SELECT RAISE(ABORT, 'invalid input value for enum {enum_type}: "' || NEW."{column_name}" || '"');
            END"#
        );
        
        conn.execute(&insert_trigger_sql, [])
            .map_err(|e| PgSqliteError::Protocol(format!("Failed to create INSERT trigger: {e}")))?;
        
        // Create UPDATE trigger
        let update_trigger_name = format!("__pgsqlite_{table_name}_{column_name}_{enum_type}_update_check");
        
        let update_trigger_sql = format!(
            r#"CREATE TRIGGER IF NOT EXISTS "{update_trigger_name}"
            BEFORE UPDATE OF "{column_name}" ON "{table_name}"
            FOR EACH ROW
            WHEN NEW."{column_name}" IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM __pgsqlite_enum_values ev
                JOIN __pgsqlite_enum_types et ON ev.type_oid = et.type_oid
                WHERE et.type_name = '{enum_type}' AND ev.label = NEW."{column_name}"
            )
            BEGIN
                SELECT RAISE(ABORT, 'invalid input value for enum {enum_type}: "' || NEW."{column_name}" || '"');
            END"#
        );
        
        conn.execute(&update_trigger_sql, [])
            .map_err(|e| PgSqliteError::Protocol(format!("Failed to create UPDATE trigger: {e}")))?;
        
        Ok(())
    }
    
    /// Drop validation triggers for an ENUM column
    pub fn drop_enum_validation_triggers(
        conn: &Connection,
        table_name: &str,
        column_name: &str,
        enum_type: &str,
    ) -> Result<(), PgSqliteError> {
        let insert_trigger_name = format!("__pgsqlite_{table_name}_{column_name}_{enum_type}_insert_check");
        let update_trigger_name = format!("__pgsqlite_{table_name}_{column_name}_{enum_type}_update_check");
        
        conn.execute(&format!("DROP TRIGGER IF EXISTS \"{insert_trigger_name}\""), [])?;
        conn.execute(&format!("DROP TRIGGER IF EXISTS \"{update_trigger_name}\""), [])?;
        
        Ok(())
    }
    
    /// Get all tables and columns using a specific ENUM type
    pub fn get_tables_using_enum(
        conn: &Connection,
        enum_type: &str,
    ) -> Result<Vec<(String, String)>, PgSqliteError> {
        let mut stmt = conn.prepare(
            "SELECT table_name, column_name FROM __pgsqlite_enum_usage WHERE enum_type = ?1"
        ).map_err(|e| PgSqliteError::Protocol(format!("Failed to prepare enum usage query: {e}")))?;
        
        let tables = stmt.query_map(params![enum_type], |row| {
            Ok((row.get(0)?, row.get(1)?))
        }).map_err(|e| PgSqliteError::Protocol(format!("Failed to query enum usage: {e}")))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| PgSqliteError::Protocol(format!("Failed to collect enum usage: {e}")))?;
        
        Ok(tables)
    }
    
    /// Clean up enum usage when a table is dropped
    pub fn clean_enum_usage_for_table(
        conn: &Connection,
        table_name: &str,
    ) -> Result<(), PgSqliteError> {
        conn.execute(
            "DELETE FROM __pgsqlite_enum_usage WHERE table_name = ?1",
            params![table_name],
        ).map_err(|e| PgSqliteError::Protocol(format!("Failed to clean enum usage: {e}")))?;
        
        Ok(())
    }
}