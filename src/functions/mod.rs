// Module for PostgreSQL function implementations
pub mod uuid_functions;
pub mod json_functions;
pub mod decimal_functions;
pub mod datetime_functions;
pub mod regex_functions;
pub mod catalog_functions;
pub mod hash_functions;
pub mod array_functions;
pub mod unnest_vtab;
pub mod string_functions;
pub mod math_functions;

use rusqlite::{Connection, Result};

/// Register all custom PostgreSQL-compatible functions
pub fn register_all_functions(conn: &Connection) -> Result<()> {
    uuid_functions::register_uuid_functions(conn)?;
    json_functions::register_json_functions(conn)?;
    decimal_functions::register_decimal_functions(conn)?;
    datetime_functions::register_datetime_functions(conn)?;
    regex_functions::register_regex_functions(conn)?;
    catalog_functions::register_catalog_functions(conn)?;
    hash_functions::register_hash_functions(conn)?;
    array_functions::register_array_functions(conn)?;
    unnest_vtab::register_unnest_vtab(conn)?;
    string_functions::register_string_functions(conn)?;
    math_functions::register_math_functions(conn)?;
    Ok(())
}