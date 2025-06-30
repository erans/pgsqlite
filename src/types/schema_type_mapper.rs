use rusqlite::Connection;

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
            "INTEGER" => 23, // int4
            "REAL" => 701, // float8
            "TEXT" => 25, // text
            "BLOB" => 17, // bytea
            _ => 25, // default to text
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
            "SMALLINT" | "INT2" => 21,
            "INTEGER" | "INT" | "INT4" => 23,
            "BIGINT" | "INT8" => 20,
            "SERIAL" => 23, // Serial is int4 with sequence
            "BIGSERIAL" => 20, // Bigserial is int8 with sequence
            
            // Floating point
            "REAL" | "FLOAT4" => 700,
            "DOUBLE PRECISION" | "FLOAT8" | "FLOAT" => 701,
            "NUMERIC" | "DECIMAL" => 1700,
            
            // Text types
            "VARCHAR" | "CHARACTER VARYING" => 1043,
            "CHAR" | "CHARACTER" => 1042,
            "TEXT" => 25,
            
            // Binary
            "BYTEA" => 17,
            
            // Boolean
            "BOOLEAN" | "BOOL" => 16,
            
            // Date/Time
            "DATE" => 1082,
            "TIME" | "TIME WITHOUT TIME ZONE" => 1083,
            "TIME WITH TIME ZONE" => 1266,
            "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => 1114,
            "TIMESTAMP WITH TIME ZONE" => 1184,
            
            // JSON
            "JSON" => 114,
            "JSONB" => 3802,
            
            // UUID
            "UUID" => 2950,
            
            // Money
            "MONEY" => 790,
            
            // Range types
            "INT4RANGE" => 3904,
            "INT8RANGE" => 3926,
            "NUMRANGE" => 3906,
            
            // Network types
            "CIDR" => 650,
            "INET" => 869,
            "MACADDR" => 829,
            "MACADDR8" => 774,
            
            // Bit strings
            "BIT VARYING" | "VARBIT" => 1562,
            "BIT" => 1560,
            
            // Default
            _ => 25, // text
        }
    }
    
    /// Convert PostgreSQL OID to type name
    pub fn pg_oid_to_type_name(oid: i32) -> &'static str {
        match oid {
            16 => "bool",
            17 => "bytea",
            20 => "int8",
            21 => "int2",
            23 => "int4",
            25 => "text",
            700 => "float4",
            701 => "float8",
            790 => "money",
            829 => "macaddr",
            869 => "inet",
            774 => "macaddr8",
            650 => "cidr",
            1043 => "varchar",
            1082 => "date",
            1083 => "time",
            1114 => "timestamp",
            1184 => "timestamptz",
            1266 => "timetz",
            1560 => "bit",
            1562 => "varbit",
            1700 => "numeric",
            2950 => "uuid",
            3802 => "jsonb",
            3904 => "int4range",
            3906 => "numrange",
            3926 => "int8range",
            _ => "text",
        }
    }
    
    /// Infer type from value when no schema is available
    pub fn infer_type_from_value(value: Option<&[u8]>) -> i32 {
        match value {
            None => 25, // NULL defaults to text
            Some(bytes) => {
                if let Ok(s) = std::str::from_utf8(bytes) {
                    Self::infer_type_from_string(s)
                } else {
                    17 // Binary data is bytea
                }
            }
        }
    }
    
    /// Infer type from string value
    fn infer_type_from_string(s: &str) -> i32 {
        // Check for boolean - only treat as bool if it's exactly these strings
        if s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("false") || 
           s.eq_ignore_ascii_case("t") || s.eq_ignore_ascii_case("f") {
            return 16; // bool
        }
        
        // Check if it looks like a bit string (only 0s and 1s)
        // Must be at least 2 characters to avoid confusion with small integers
        if s.len() >= 2 && s.chars().all(|c| c == '0' || c == '1') {
            // Additional heuristic: if it's 8, 16, 32, or 64 bits, it's likely a bit string
            // Or if it has leading zeros (uncommon for regular integers)
            if s.len() == 8 || s.len() == 16 || s.len() == 32 || s.len() == 64 || s.starts_with('0') {
                return 1560; // bit
            }
        }
        
        // Try parsing as integer
        if let Ok(i) = s.parse::<i64>() {
            // Use int4 for values that fit, int8 for larger
            if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                return 23; // int4
            } else {
                return 20; // int8
            }
        }
        
        if let Ok(_) = s.parse::<f64>() {
            return 701; // float8
        }
        
        // Check for date/time formats
        if s.len() == 10 && s.chars().filter(|&c| c == '-').count() == 2 {
            return 1082; // Possibly a date
        }
        
        // Check for UUID format
        if s.len() == 36 && s.chars().filter(|&c| c == '-').count() == 4 {
            return 2950; // Possibly a UUID
        }
        
        // Check for JSON
        if (s.starts_with('{') && s.ends_with('}')) || (s.starts_with('[') && s.ends_with(']')) {
            return 114; // json
        }
        
        25 // Default to text
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
            return Some(20); // bigint
        }
        
        // JSON functions that return integers
        if upper.starts_with("JSON_ARRAY_LENGTH(") {
            return Some(23); // int4
        }
        
        // JSON functions that return text
        if upper.starts_with("JSON_GROUP_ARRAY(") || upper.starts_with("JSON_ARRAY(") || 
           upper.starts_with("JSON_OBJECT(") || upper.starts_with("JSON_EXTRACT(") {
            return Some(25); // text
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
                            21 | 23 | 20 | 700 | 701 | 1700 => return Some(1700), // numeric
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
            Some(1700) // numeric
        } else {
            None
        }
    }
}