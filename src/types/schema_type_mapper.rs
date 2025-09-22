use rusqlite::Connection;
use crate::types::PgType;
use crate::metadata::EnumMetadata;
use regex;

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
        let query = format!("PRAGMA table_info({table_name})");
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
        // Fast path for common exact matches
        match sqlite_type {
            "INTEGER" | "integer" => return PgType::Int4.to_oid(), // int4
            "REAL" | "real" => return PgType::Float8.to_oid(), // float8
            "TEXT" | "text" => return PgType::Text.to_oid(), // text
            "BLOB" | "blob" => return PgType::Bytea.to_oid(), // bytea
            _ => {}
        }
        
        // Fall back to case-insensitive comparison
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
        // Handle array types first (before basic types)
        if let Some(base_type) = pg_type.strip_suffix("[]") {
            // Remove any parameters like (50) from VARCHAR(50)
            let base_without_params = if let Some(paren_pos) = base_type.find('(') {
                &base_type[..paren_pos]
            } else {
                base_type
            };
            return match base_without_params.to_lowercase().as_str() {
                "boolean" | "bool" => 1000,       // BOOLEAN[]
                "integer" | "int4" | "int" => 1007, // INTEGER[]
                "bigint" | "int8" => 1016,        // BIGINT[]
                "smallint" | "int2" => 1005,      // SMALLINT[]
                "text" => 1009,                   // TEXT[]
                "varchar" => 1015,                // VARCHAR[]
                "real" | "float4" => 1021,        // REAL[]
                "double precision" | "float8" => 1022, // FLOAT8[]
                "numeric" | "decimal" => 1231,    // NUMERIC[]
                "char" => 1002,                   // CHAR[]
                "bytea" => 1001,                  // BYTEA[]
                "date" => 1182,                   // DATE[]
                "time" => 1183,                   // TIME[]
                "timestamp" => 1115,              // TIMESTAMP[]
                "timestamptz" => 1185,            // TIMESTAMPTZ[]
                "timetz" => 1270,                 // TIMETZ[]
                "interval" => 1187,               // INTERVAL[]
                "uuid" => 2951,                   // UUID[]
                _ => PgType::Text.to_oid(),       // Unknown array type, default to TEXT
            };
        }

        // Fast path for common exact matches (case-sensitive)
        match pg_type {
            "text" | "TEXT" => return PgType::Text.to_oid(),
            "integer" | "INTEGER" | "int4" | "INT4" | "int" | "INT" => return PgType::Int4.to_oid(),
            "bigint" | "BIGINT" | "int8" | "INT8" => return PgType::Int8.to_oid(),
            "smallint" | "SMALLINT" | "int2" | "INT2" => return PgType::Int2.to_oid(),
            "boolean" | "BOOLEAN" | "bool" | "BOOL" => return PgType::Bool.to_oid(),
            "real" | "REAL" | "float4" | "FLOAT4" => return PgType::Float4.to_oid(),
            "double precision" | "DOUBLE PRECISION" | "float8" | "FLOAT8" => return PgType::Float8.to_oid(),
            "numeric" | "NUMERIC" | "decimal" | "DECIMAL" => return PgType::Numeric.to_oid(),
            "varchar" | "VARCHAR" => return PgType::Varchar.to_oid(),
            "char" | "CHAR" => return PgType::Char.to_oid(),
            "bytea" | "BYTEA" => return PgType::Bytea.to_oid(),
            "date" | "DATE" => return PgType::Date.to_oid(),
            "time" | "TIME" => return PgType::Time.to_oid(),
            "timestamp" | "TIMESTAMP" => return PgType::Timestamp.to_oid(),
            "timestamptz" | "TIMESTAMPTZ" => return PgType::Timestamptz.to_oid(),
            "timetz" | "TIMETZ" => return PgType::Timetz.to_oid(),
            "interval" | "INTERVAL" => return PgType::Interval.to_oid(),
            "uuid" | "UUID" => return PgType::Uuid.to_oid(),
            "json" | "JSON" => return PgType::Json.to_oid(),
            "jsonb" | "JSONB" => return PgType::Jsonb.to_oid(),
            _ => {}
        }
        
        let upper_type = pg_type.to_uppercase();
        
        // Handle parametric types by removing parameters
        let base_type = if let Some(paren_pos) = upper_type.find('(') {
            upper_type[..paren_pos].trim()
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
            "TIME WITH TIME ZONE" | "TIMETZ" => PgType::Timetz.to_oid(),
            "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => PgType::Timestamp.to_oid(),
            "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => PgType::Timestamptz.to_oid(),
            "INTERVAL" => PgType::Interval.to_oid(),
            
            // JSON
            "JSON" => PgType::Json.to_oid(),
            "JSONB" => PgType::Jsonb.to_oid(),
            
            // UUID
            "UUID" => PgType::Uuid.to_oid(),
            
            // Money
            "MONEY" => PgType::Money.to_oid(),
            
            // Array types
            "TEXT[]" | "_TEXT" => PgType::TextArray.to_oid(),
            "INT[]" | "INT4[]" | "INTEGER[]" | "_INT4" => PgType::Int4Array.to_oid(),
            "BIGINT[]" | "INT8[]" | "_INT8" => PgType::Int8Array.to_oid(),
            
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
            
            // Default - might be an ENUM type, return a special marker
            _ => {
                // For unknown types, we'll return TEXT but the caller should check
                // if it's actually an ENUM type
                PgType::Text.to_oid()
            }
        }
    }
    
    /// Get PostgreSQL type OID, checking for ENUM types
    pub fn pg_type_string_to_oid_with_enum_check(pg_type: &str, conn: &Connection) -> i32 {
        // First try standard types
        let oid = Self::pg_type_string_to_oid(pg_type);
        
        // If we got TEXT OID, check if it's actually an ENUM
        if oid == PgType::Text.to_oid() {
            // Check if this is an ENUM type
            if let Ok(Some(enum_type)) = EnumMetadata::get_enum_type(conn, pg_type) {
                return enum_type.type_oid;
            }
        }
        
        oid
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
        
        if s.parse::<f64>().is_ok() {
            return PgType::Float8.to_oid(); // float8
        }
        
        // Don't infer date type from string values - SQLite's CURRENT_DATE returns text
        // and inferring DATE type causes deserialization errors in tokio-postgres
        // if s.len() == 10 && s.chars().filter(|&c| c == '-').count() == 2 {
        //     return PgType::Date.to_oid(); // Possibly a date
        // }
        
        // Check for UUID format
        if s.len() == 36 && s.chars().filter(|&c| c == '-').count() == 4 {
            return PgType::Uuid.to_oid(); // Possibly a UUID
        }
        
        // Check for JSON objects vs arrays
        if s.starts_with('{') && s.ends_with('}') {
            return PgType::Json.to_oid(); // json object
        }
        
        // Check for JSON arrays - keep as JSON/TEXT for now
        // We don't have proper binary array encoding yet
        if s.starts_with('[') && s.ends_with(']') {
            return PgType::Text.to_oid(); // text (JSON array)
        }
        
        PgType::Text.to_oid() // Default to text
    }
    
    /// Get type OID for aggregate functions
    pub fn get_aggregate_return_type(
        function_name: &str,
        conn: Option<&Connection>,
        table_name: Option<&str>,
    ) -> Option<i32> {
        Self::get_aggregate_return_type_with_query(function_name, conn, table_name, None)
    }
    
    /// Get type OID for aggregate functions with optional query context
    pub fn get_aggregate_return_type_with_query(
        function_name: &str,
        conn: Option<&Connection>,
        table_name: Option<&str>,
        query: Option<&str>
    ) -> Option<i32> {
        let upper = function_name.to_uppercase();

        // Handle aliased columns - if it's just a simple name, skip function detection
        // This prevents false positives for columns named "year_col", "hour_trunc", etc.
        if !function_name.contains('(') && !function_name.contains(' ') {
            // If we have the query, try to find what function produces this alias
            if let Some(q) = query {
                // Look for patterns like "sum(...) AS function_name" or "avg(...) AS function_name"
                // This handles both simple aggregates and aggregate expressions
                let pattern = format!(r"(?i)([\w_]+)\s*\([^)]+\)\s+(?:AS\s+)?{}\b", regex::escape(function_name));
                if let Ok(re) = regex::Regex::new(&pattern)
                    && let Some(captures) = re.captures(q) {
                        let actual_function = captures[1].to_uppercase();
                        // Check if this is an aggregate function
                        if matches!(actual_function.as_str(), "SUM" | "AVG" | "MAX" | "MIN" | "COUNT" | 
                                   "ARRAY_AGG" | "JSON_AGG" | "JSONB_AGG" | "STRING_AGG" |
                                   "JSON_OBJECT_AGG" | "JSONB_OBJECT_AGG") {
                            // For SUM/AVG on arithmetic expressions, always return NUMERIC
                            if actual_function == "SUM" || actual_function == "AVG" {
                                return Some(PgType::Numeric.to_oid());
                            }
                            // Recursively call with the actual function name
                            return Self::get_aggregate_return_type_with_query(&format!("{actual_function}()"), conn, table_name, None);
                        }
                    }
                
                // Also check for array concatenation operator pattern: column || array AS alias
                // NOTE: For now, we return TEXT instead of TextArray because:
                // 1. The data is stored as JSON strings in SQLite
                // 2. Clients expect to get strings, not PostgreSQL arrays
                // 3. Binary array encoding is not yet implemented
                let concat_pattern = format!(r"\w+\s*\|\|\s*[^\s]+\s+(?:AS\s+)?{}\b", regex::escape(function_name));
                if let Ok(re) = regex::Regex::new(&concat_pattern)
                    && re.is_match(q) {
                        // This is an array concatenation operation - return as TEXT
                        return Some(PgType::Text.to_oid());
                    }
            }
            return None;
        }
        
        // COUNT always returns bigint
        if upper == "COUNT(*)" || upper.starts_with("COUNT(") {
            return Some(PgType::Int8.to_oid()); // bigint
        }
        
        // Decimal arithmetic functions that return numeric
        if upper.starts_with("DECIMAL_ADD(") || upper.starts_with("DECIMAL_SUB(") || 
           upper.starts_with("DECIMAL_MUL(") || upper.starts_with("DECIMAL_DIV(") ||
           upper.starts_with("DECIMAL_FROM_TEXT(") {
            return Some(PgType::Numeric.to_oid()); // numeric
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
        
        // JSON aggregate functions that return text (for compatibility)
        if upper.starts_with("JSON_AGG(") || upper.starts_with("JSON_OBJECT_AGG(") ||
           upper.starts_with("JSONB_AGG(") || upper.starts_with("JSONB_OBJECT_AGG(") ||
           upper.starts_with("ROW_TO_JSON(") {
            return Some(PgType::Text.to_oid()); // text
        }
        
        // CURRENT_DATE returns text in YYYY-MM-DD format (SQLite built-in)
        if upper == "CURRENT_DATE" {
            return Some(PgType::Text.to_oid()); // text
        }
        
        // Timestamp functions that return INTEGER microseconds (stored as timestamp type)
        if upper == "NOW()" || upper == "CURRENT_TIMESTAMP" || upper == "CURRENT_TIMESTAMP()" {
            return Some(PgType::Timestamptz.to_oid()); // timestamptz (formatted timestamp string)
        }
        
        // Other datetime functions
        if upper.starts_with("DATE_TRUNC(") || upper.starts_with("TO_TIMESTAMP(") {
            return Some(PgType::Timestamp.to_oid()); // timestamp
        }
        
        if upper.starts_with("MAKE_DATE(") {
            return Some(PgType::Date.to_oid()); // date (INTEGER days since epoch)
        }
        
        if upper == "EPOCH()" {
            return Some(PgType::Timestamp.to_oid()); // epoch as timestamp
        }
        
        if upper.starts_with("AGE(") {
            return Some(PgType::Interval.to_oid()); // interval (INTEGER microseconds)
        }
        
        // Time functions that return INTEGER microseconds (stored as time type)
        if upper == "CURRENT_TIME" || upper == "CURRENT_TIME()" || upper.starts_with("MAKE_TIME(") {
            return Some(PgType::Time.to_oid()); // time (INTEGER microseconds since midnight)
        }
        
        // EXTRACT returns float8
        if upper.starts_with("EXTRACT(") {
            return Some(PgType::Float8.to_oid()); // float8
        }
        
        // For other aggregates, we need to know the column type
        if let Some(column_name) = crate::types::QueryContextAnalyzer::extract_column_from_aggregation(function_name) {
            // Try to get the column type from schema
            if let (Some(conn), Some(table)) = (conn, table_name)
                && let Some(base_type) = Self::get_type_from_schema(conn, table, &column_name) {
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
            
            // If we couldn't look up the type from schema (conn or table is None),
            // try to infer it from the query context for MAX/MIN on likely DECIMAL columns
            if let Some(fixed_type) = crate::types::aggregate_type_fixer::fix_aggregate_type_for_decimal(function_name, query) {
                return Some(fixed_type);
            }
        }
        
        // Last resort: Check if the query context suggests this is an aggregate on a decimal column
        // This handles cases where the column has an alias (e.g., "max_1") 
        if let Some(fixed_type) = crate::types::aggregate_type_fixer::fix_aggregate_type_for_decimal(function_name, query) {
            return Some(fixed_type);
        }
        
        // Array functions
        // NOTE: Return TEXT instead of array types because data is stored as JSON strings
        if upper.starts_with("ARRAY_AGG(") {
            return Some(PgType::Text.to_oid()); // text (JSON array)
        }
        
        if upper.starts_with("ARRAY_LENGTH(") || upper.starts_with("ARRAY_UPPER(") || 
           upper.starts_with("ARRAY_LOWER(") || upper.starts_with("ARRAY_NDIMS(") {
            return Some(PgType::Int4.to_oid()); // int4
        }
        
        if upper.starts_with("ARRAY_APPEND(") || upper.starts_with("ARRAY_PREPEND(") || 
           upper.starts_with("ARRAY_CAT(") || upper.starts_with("ARRAY_REMOVE(") || 
           upper.starts_with("ARRAY_REPLACE(") || upper.starts_with("ARRAY_SLICE(") ||
           upper.starts_with("STRING_TO_ARRAY(") {
            return Some(PgType::Text.to_oid()); // text (JSON array)
        }
        
        if upper.starts_with("ARRAY_POSITION(") {
            return Some(PgType::Int4.to_oid()); // int4
        }
        
        if upper.starts_with("ARRAY_POSITIONS(") {
            return Some(PgType::Text.to_oid()); // text (JSON array)
        }
        
        if upper.starts_with("ARRAY_TO_STRING(") || upper.starts_with("UNNEST(") {
            return Some(PgType::Text.to_oid()); // text
        }
        
        if upper.starts_with("ARRAY_CONTAINS(") || upper.starts_with("ARRAY_CONTAINED(") || 
           upper.starts_with("ARRAY_OVERLAP(") {
            return Some(PgType::Bool.to_oid()); // bool
        }
        
        // Default return types when we can't determine from schema
        if upper.starts_with("SUM(") || upper.starts_with("AVG(") {
            Some(PgType::Numeric.to_oid()) // numeric
        } else {
            None
        }
    }
}