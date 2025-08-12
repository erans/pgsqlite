use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Type alias for numeric constraint cache
type NumericConstraintCache = Arc<RwLock<HashMap<String, HashMap<String, (i32, i32)>>>>;

/// Cache for numeric constraints to avoid repeated database queries
static NUMERIC_CONSTRAINT_CACHE: once_cell::sync::Lazy<NumericConstraintCache> = 
    once_cell::sync::Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

/// Format a numeric value according to its scale constraint
pub fn format_numeric_with_scale(value: f64, table_name: &str, column_name: &str, conn: &Connection) -> String {
    // Try to get constraints from cache
    {
        let cache = NUMERIC_CONSTRAINT_CACHE.read().unwrap();
        if let Some(table_constraints) = cache.get(table_name)
            && let Some((_precision, scale)) = table_constraints.get(column_name) {
                return format!("{:.prec$}", value, prec = *scale as usize);
            }
    }
    
    // Not in cache, try to load from database
    if let Ok(has_table) = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='__pgsqlite_numeric_constraints'",
        [],
        |row| row.get::<_, i32>(0)
    )
        && has_table > 0 {
            // Load constraints for this table
            if let Ok(mut stmt) = conn.prepare(
                "SELECT column_name, precision, scale FROM __pgsqlite_numeric_constraints WHERE table_name = ?1"
            )
                && let Ok(constraints) = stmt.query_map([table_name], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i32>(1)?,
                        row.get::<_, i32>(2)?
                    ))
                }).and_then(|mapped| mapped.collect::<Result<Vec<_>, _>>()) {
                    let mut table_constraints = HashMap::new();
                    for (col_name, precision, scale) in constraints {
                        table_constraints.insert(col_name, (precision, scale));
                    }
                    
                    // Update cache
                    let mut cache = NUMERIC_CONSTRAINT_CACHE.write().unwrap();
                    cache.insert(table_name.to_string(), table_constraints.clone());
                    
                    // Try again with cached value
                    if let Some((_precision, scale)) = table_constraints.get(column_name) {
                        return format!("{:.prec$}", value, prec = *scale as usize);
                    }
                }
        }
    
    // No constraints found, use default formatting
    value.to_string()
}

/// Clear the numeric constraint cache for a specific table
pub fn invalidate_numeric_cache(table_name: &str) {
    let mut cache = NUMERIC_CONSTRAINT_CACHE.write().unwrap();
    cache.remove(table_name);
}

/// Clear the entire numeric constraint cache
pub fn clear_numeric_cache() {
    let mut cache = NUMERIC_CONSTRAINT_CACHE.write().unwrap();
    cache.clear();
}