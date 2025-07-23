use rusqlite::{Connection, Result};
use tracing::info;

/// Manages numeric constraint validation triggers for tables with NUMERIC/DECIMAL columns
pub struct NumericTriggers;

impl NumericTriggers {
    /// Create validation triggers for a numeric column
    pub fn create_numeric_validation_triggers(
        conn: &Connection,
        table_name: &str,
        column_name: &str,
        precision: i32,
        scale: i32,
    ) -> Result<()> {
        // Create INSERT trigger with proper validation
        let insert_trigger_name = format!("__pgsqlite_numeric_insert_{table_name}_{column_name}");
        
        // For NUMERIC(p,s):
        // - p is total number of significant digits
        // - s is number of digits after decimal point
        // - Maximum integer part is 10^(p-s) - 1
        // - We need to check both scale and total precision
        
        let insert_trigger_sql = format!(
            r#"
            CREATE TRIGGER IF NOT EXISTS {trigger_name}
            BEFORE INSERT ON {table}
            FOR EACH ROW
            WHEN NEW.{column} IS NOT NULL AND NEW.{column} != ''
            BEGIN
                SELECT CASE
                    -- First ensure the value is numeric
                    WHEN (CASE 
                        WHEN typeof(NEW.{column}) IN ('integer', 'real') THEN 0
                        WHEN typeof(NEW.{column}) = 'text' AND 
                             LENGTH(TRIM(NEW.{column})) > 0 AND
                             NEW.{column} GLOB '*[0-9]*' AND
                             REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(NEW.{column}, '0', ''), '1', ''), '2', ''), '3', ''), '4', '') 
                             || REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(NEW.{column}, '5', ''), '6', ''), '7', ''), '8', ''), '9', '')
                             || REPLACE(REPLACE(REPLACE(NEW.{column}, '.', ''), '-', ''), '+', '') = ''
                        THEN 0
                        ELSE 1
                    END) = 1 THEN
                        RAISE(ABORT, 'numeric field overflow')
                    -- Check the number of decimal places doesn't exceed scale
                    WHEN (
                        CASE 
                            WHEN INSTR(CAST(NEW.{column} AS TEXT), '.') = 0 THEN 0
                            ELSE LENGTH(RTRIM(SUBSTR(CAST(NEW.{column} AS TEXT), INSTR(CAST(NEW.{column} AS TEXT), '.') + 1), '0'))
                        END
                    ) > {scale} THEN
                        RAISE(ABORT, 'numeric field overflow')
                    -- Check that the absolute value doesn't exceed the maximum
                    -- For NUMERIC(p,s), max value is (10^p - 1) / (10^s)
                    -- But it's easier to check if value * 10^s < 10^p
                    WHEN ABS(CAST(NEW.{column} AS REAL)) >= {max_value} THEN
                        RAISE(ABORT, 'numeric field overflow')
                END;
            END;
            "#,
            trigger_name = insert_trigger_name,
            table = table_name,
            column = column_name,
            scale = scale,
            // For NUMERIC(p,s), the maximum value is 10^(p-s) - 10^(-s)
            // But for simplicity and to avoid edge cases, we use 10^(p-s)
            max_value = 10_f64.powi(precision - scale)
        );
        
        conn.execute(&insert_trigger_sql, [])?;
        info!("Created numeric INSERT validation trigger for {}.{}", table_name, column_name);
        
        // Create UPDATE trigger with same logic
        let update_trigger_name = format!("__pgsqlite_numeric_update_{table_name}_{column_name}");
        let update_trigger_sql = format!(
            r#"
            CREATE TRIGGER IF NOT EXISTS {trigger_name}
            BEFORE UPDATE OF {column} ON {table}
            FOR EACH ROW
            WHEN NEW.{column} IS NOT NULL AND NEW.{column} != ''
            BEGIN
                SELECT CASE
                    -- First ensure the value is numeric
                    WHEN (CASE 
                        WHEN typeof(NEW.{column}) IN ('integer', 'real') THEN 0
                        WHEN typeof(NEW.{column}) = 'text' AND 
                             LENGTH(TRIM(NEW.{column})) > 0 AND
                             NEW.{column} GLOB '*[0-9]*' AND
                             REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(NEW.{column}, '0', ''), '1', ''), '2', ''), '3', ''), '4', '') 
                             || REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(NEW.{column}, '5', ''), '6', ''), '7', ''), '8', ''), '9', '')
                             || REPLACE(REPLACE(REPLACE(NEW.{column}, '.', ''), '-', ''), '+', '') = ''
                        THEN 0
                        ELSE 1
                    END) = 1 THEN
                        RAISE(ABORT, 'numeric field overflow')
                    -- Check the number of decimal places doesn't exceed scale
                    WHEN (
                        CASE 
                            WHEN INSTR(CAST(NEW.{column} AS TEXT), '.') = 0 THEN 0
                            ELSE LENGTH(RTRIM(SUBSTR(CAST(NEW.{column} AS TEXT), INSTR(CAST(NEW.{column} AS TEXT), '.') + 1), '0'))
                        END
                    ) > {scale} THEN
                        RAISE(ABORT, 'numeric field overflow')
                    -- Check that the absolute value doesn't exceed the maximum
                    -- For NUMERIC(p,s), max value is (10^p - 1) / (10^s)
                    -- But it's easier to check if value * 10^s < 10^p
                    WHEN ABS(CAST(NEW.{column} AS REAL)) >= {max_value} THEN
                        RAISE(ABORT, 'numeric field overflow')
                END;
            END;
            "#,
            trigger_name = update_trigger_name,
            table = table_name,
            column = column_name,
            scale = scale,
            max_value = 10_f64.powi(precision - scale)
        );
        
        conn.execute(&update_trigger_sql, [])?;
        info!("Created numeric UPDATE validation trigger for {}.{}", table_name, column_name);
        
        Ok(())
    }
    
    /// Drop validation triggers for a numeric column
    pub fn drop_numeric_validation_triggers(
        conn: &Connection,
        table_name: &str,
        column_name: &str,
    ) -> Result<()> {
        let insert_trigger_name = format!("__pgsqlite_numeric_insert_{table_name}_{column_name}");
        let update_trigger_name = format!("__pgsqlite_numeric_update_{table_name}_{column_name}");
        
        conn.execute(&format!("DROP TRIGGER IF EXISTS {insert_trigger_name}"), [])?;
        conn.execute(&format!("DROP TRIGGER IF EXISTS {update_trigger_name}"), [])?;
        
        info!("Dropped numeric validation triggers for {}.{}", table_name, column_name);
        Ok(())
    }
    
    /// Check if a table has numeric validation triggers
    pub fn has_numeric_triggers(
        conn: &Connection,
        table_name: &str,
        column_name: &str,
    ) -> Result<bool> {
        let trigger_name = format!("__pgsqlite_numeric_insert_{table_name}_{column_name}");
        let count: i32 = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name = ?1",
            [&trigger_name],
            |row| row.get(0),
        )?;
        
        Ok(count > 0)
    }
}