use rusqlite::Connection;
use crate::types::PgType;

/// Maps between PostgreSQL and SQLite types using actual schema information
pub struct SchemaTypeMapper;

impl SchemaTypeMapper {
    /// Get PostgreSQL type OID from SQLite schema
    pub fn get_type_from_schema(
        conn: &Connection,
        table_name: &str,
        column_name: &str
    ) -> Option<i32> {
        // First check if we have stored PostgreSQL type metadata
        if let Ok(Some(pg_type)) = crate::metadata::TypeMetadata::get_pg_type(conn, table_name, column_name) {
            return Some(Self::pg_type_string_to_oid(&pg_type));
        }
        
        // Fall back to SQLite schema
        if let Ok(sqlite_type) = Self::get_sqlite_column_type(conn, table_name, column_name) {
            return Some(Self::sqlite_type_to_pg_oid(&sqlite_type));
        }
        
        None
    }
    
    /// Get SQLite column type from PRAGMA table_info
    fn get_sqlite_column_type(
        conn: &Connection,
        table_name: &str,
        column_name: &str
    ) -> Result<String, rusqlite::Error> {
        let query = format!("PRAGMA table_info({})", table_name);
        let mut stmt = conn.prepare(&query)?;
        
        let mut rows = stmt.query_map([], |row| {
            let col_name: String = row.get(1)?;
            let col_type: String = row.get(2)?;
            Ok((col_name, col_type))
        })?;
        
        while let Some(Ok((col_name, col_type))) = rows.next() {
            if col_name == column_name {
                return Ok(col_type);
            }
        }
        
        Err(rusqlite::Error::QueryReturnedNoRows)
    }
    
    /// Map SQLite type declaration to PostgreSQL OID
    pub fn sqlite_type_to_pg_oid(sqlite_type: &str) -> i32 {
        let type_upper = sqlite_type.to_uppercase();
        
        match type_upper.as_str() {
            "INTEGER" => PgType::Int4.to_oid(), // int4
            "REAL" => PgType::Float8.to_oid(), // float8
            "TEXT" => PgType::Text.to_oid(), // text
            "BLOB" => PgType::Bytea.to_oid(), // bytea
            _ => PgType::Text.to_oid(), // default to text
        }
    }
    
    /// Map PostgreSQL type string to OID
    pub fn pg_type_string_to_oid(pg_type: &str) -> i32 {
        let upper_type = pg_type.to_uppercase();
        
        // Handle parametric types by removing parameters
        let base_type = if let Some(paren_pos) = upper_type.find('(') {
            &upper_type[..paren_pos].trim()
        } else {
            upper_type.as_str()
        };
        
        match base_type {
            // Integer types
            "SMALLINT" | "INT2" => PgType::Int2.to_oid(),
            "INTEGER" | "INT" | "INT4" => PgType::Int4.to_oid(),
            "BIGINT" | "INT8" => PgType::Int8.to_oid(),
            "SERIAL" => PgType::Int4.to_oid(), // Serial is int4 with sequence
            "BIGSERIAL" => PgType::Int8.to_oid(), // Bigserial is int8 with sequence
            
            // Floating point
            "REAL" | "FLOAT4" => PgType::Float4.to_oid(),
            "DOUBLE PRECISION" | "FLOAT8" | "FLOAT" => PgType::Float8.to_oid(),
            "NUMERIC" | "DECIMAL" => PgType::Numeric.to_oid(),
            
            // Text types
            "VARCHAR" | "CHARACTER VARYING" => PgType::Varchar.to_oid(),
            "CHAR" | "CHARACTER" => PgType::Char.to_oid(),
            "TEXT" => PgType::Text.to_oid(),
            
            // Binary
            "BYTEA" => PgType::Bytea.to_oid(),
            
            // Boolean
            "BOOLEAN" | "BOOL" => PgType::Bool.to_oid(),
            
            // Date/Time
            "DATE" => PgType::Date.to_oid(),
            "TIME" | "TIME WITHOUT TIME ZONE" => PgType::Time.to_oid(),
            "TIME WITH TIME ZONE" => 1266, // TIMETZ not in PgType enum yet
            "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => PgType::Timestamp.to_oid(),
            "TIMESTAMP WITH TIME ZONE" => PgType::Timestamptz.to_oid(),
            
            // JSON
            "JSON" => PgType::Json.to_oid(),
            "JSONB" => PgType::Jsonb.to_oid(),
            
            // UUID
            "UUID" => PgType::Uuid.to_oid(),
            
            // Money
            "MONEY" => PgType::Money.to_oid(),
            
            // Range types
            "INT4RANGE" => PgType::Int4range.to_oid(),
            "INT8RANGE" => PgType::Int8range.to_oid(),
            "NUMRANGE" => PgType::Numrange.to_oid(),
            
            // Network types
            "CIDR" => PgType::Cidr.to_oid(),
            "INET" => PgType::Inet.to_oid(),
            "MACADDR" => PgType::Macaddr.to_oid(),
            "MACADDR8" => PgType::Macaddr8.to_oid(),
            
            // Bit strings
            "BIT VARYING" | "VARBIT" => PgType::Varbit.to_oid(),
            "BIT" => PgType::Bit.to_oid(),
            
            // Default
            _ => PgType::Text.to_oid(), // text
        }
    }
    
    /// Convert PostgreSQL OID to type name
    pub fn pg_oid_to_type_name(oid: i32) -> &'static str {
        // Try to use PgType first
        if let Some(pg_type) = PgType::from_oid(oid) {
            return pg_type.name();
        }
        
        // Handle types not in PgType enum
        match oid {
            18 => "char", // single char type
            19 => "name", 
            26 => "oid",
            114 => "json", // JSON (not JSONB)
            1042 => "bpchar", // blank-padded char
            1186 => "interval",
            1266 => "timetz",
            _ => "text",
        }
    }
    
    /// Infer type from value when no schema is available
    pub fn infer_type_from_value(value: Option<&[u8]>) -> i32 {
        match value {
            None => PgType::Text.to_oid(), // NULL defaults to text
            Some(bytes) => {
                if let Ok(s) = std::str::from_utf8(bytes) {
                    Self::infer_type_from_string(s)
                } else {
                    PgType::Bytea.to_oid() // Binary data is bytea
                }
            }
        }
    }
    
    /// Infer type from string value
    fn infer_type_from_string(s: &str) -> i32 {
        // Check for boolean - only treat as bool if it's exactly these strings
        if s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("false") || 
           s.eq_ignore_ascii_case("t") || s.eq_ignore_ascii_case("f") {
            return PgType::Bool.to_oid(); // bool
        }
        
        // Check if it looks like a bit string (only 0s and 1s)
        // Must be at least 2 characters to avoid confusion with small integers
        if s.len() >= 2 && s.chars().all(|c| c == '0' || c == '1') {
            // Additional heuristic: if it's 8, 16, 32, or 64 bits, it's likely a bit string
            // Or if it has leading zeros (uncommon for regular integers)
            if s.len() == 8 || s.len() == 16 || s.len() == 32 || s.len() == 64 || s.starts_with('0') {
                return PgType::Bit.to_oid(); // bit
            }
        }
        
        // Try parsing as integer
        if let Ok(i) = s.parse::<i64>() {
            // Use int4 for values that fit, int8 for larger
            if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                return PgType::Int4.to_oid(); // int4
            } else {
                return PgType::Int8.to_oid(); // int8
            }
        }
        
        if let Ok(_) = s.parse::<f64>() {
            return PgType::Float8.to_oid(); // float8
        }
        
        // Check for date/time formats
        if s.len() == 10 && s.chars().filter(|&c| c == '-').count() == 2 {
            return PgType::Date.to_oid(); // Possibly a date
        }
        
        // Check for UUID format
        if s.len() == 36 && s.chars().filter(|&c| c == '-').count() == 4 {
            return PgType::Uuid.to_oid(); // Possibly a UUID
        }
        
        // Check for JSON
        if (s.starts_with('{') && s.ends_with('}')) || (s.starts_with('[') && s.ends_with(']')) {
            return PgType::Json.to_oid(); // json
        }
        
        PgType::Text.to_oid() // Default to text
    }
    
    /// Get type OID for aggregate functions
    pub fn get_aggregate_return_type(
        function_name: &str,
        conn: Option<&Connection>,
        table_name: Option<&str>
    ) -> Option<i32> {
        let upper = function_name.to_uppercase();
        
        // COUNT always returns bigint
        if upper == "COUNT(*)" || upper.starts_with("COUNT(") {
            return Some(PgType::Int8.to_oid()); // bigint
        }
        
        // JSON functions that return integers
        if upper.starts_with("JSON_ARRAY_LENGTH(") {
            return Some(PgType::Int4.to_oid()); // int4
        }
        
        // JSON functions that return text
        if upper.starts_with("JSON_GROUP_ARRAY(") || upper.starts_with("JSON_ARRAY(") || 
           upper.starts_with("JSON_OBJECT(") || upper.starts_with("JSON_EXTRACT(") {
            return Some(PgType::Text.to_oid()); // text
        }
        
        // For other aggregates, we need to know the column type
        if let Some(column_name) = crate::types::QueryContextAnalyzer::extract_column_from_aggregation(function_name) {
            // Try to get the column type from schema
            if let (Some(conn), Some(table)) = (conn, table_name) {
                if let Some(base_type) = Self::get_type_from_schema(conn, table, &column_name) {
                    // Map aggregate result based on base type
                    if upper.starts_with("SUM(") || upper.starts_with("AVG(") {
                        // SUM and AVG return numeric for numeric types
                        match base_type {
                            t if t == PgType::Int2.to_oid() || t == PgType::Int4.to_oid() || t == PgType::Int8.to_oid() ||
                                 t == PgType::Float4.to_oid() || t == PgType::Float8.to_oid() || t == PgType::Numeric.to_oid() => {
                                return Some(PgType::Numeric.to_oid()); // numeric
                            }
                            _ => return Some(base_type), // Keep original type
                        }
                    } else if upper.starts_with("MAX(") || upper.starts_with("MIN(") {
                        // MAX/MIN return the same type as the column
                        return Some(base_type);
                    }
                }
            }
        }
        
        // Default return types when we can't determine from schema
        if upper.starts_with("SUM(") || upper.starts_with("AVG(") {
            Some(PgType::Numeric.to_oid()) // numeric
        } else {
            None
        }
    }
}