// Module for PostgreSQL function implementations
pub mod uuid_functions;
pub mod json_functions;
pub mod decimal_functions;

use rusqlite::{Connection, Result};

/// Register all custom PostgreSQL-compatible functions
pub fn register_all_functions(conn: &Connection) -> Result<()> {
    uuid_functions::register_uuid_functions(conn)?;
    json_functions::register_json_functions(conn)?;
    decimal_functions::register_decimal_functions(conn)?;
    // Future: Add date/time functions, etc.
    Ok(())
}