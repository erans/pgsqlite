use std::collections::HashMap;

/// PostgreSQL type OIDs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PgType {
    Bool = 16,
    Int2 = 21,
    Int4 = 23,
    Int8 = 20,
    Float4 = 700,
    Float8 = 701,
    Text = 25,
    Varchar = 1043,
    Char = 1042,
    Uuid = 2950,
    Json = 114,
    Jsonb = 3802,
    Date = 1082,
    Time = 1083,
    Timestamp = 1114,
    Timestamptz = 1184,
    Timetz = 1266,
    Interval = 1186,
    Numeric = 1700,
    Bytea = 17,
    Money = 790,
    Int4range = 3904,
    Int8range = 3926,
    Numrange = 3906,
    Cidr = 650,
    Inet = 869,
    Macaddr = 829,
    Macaddr8 = 774,
    Bit = 1560,
    Varbit = 1562,
    Unknown = 705,
}

impl PgType {
    pub fn from_oid(oid: i32) -> Option<Self> {
        match oid {
            16 => Some(PgType::Bool),
            21 => Some(PgType::Int2),
            23 => Some(PgType::Int4),
            20 => Some(PgType::Int8),
            700 => Some(PgType::Float4),
            701 => Some(PgType::Float8),
            25 => Some(PgType::Text),
            1043 => Some(PgType::Varchar),
            1042 => Some(PgType::Char),
            2950 => Some(PgType::Uuid),
            114 => Some(PgType::Json),
            3802 => Some(PgType::Jsonb),
            1082 => Some(PgType::Date),
            1083 => Some(PgType::Time),
            1114 => Some(PgType::Timestamp),
            1184 => Some(PgType::Timestamptz),
            1266 => Some(PgType::Timetz),
            1186 => Some(PgType::Interval),
            1700 => Some(PgType::Numeric),
            17 => Some(PgType::Bytea),
            790 => Some(PgType::Money),
            3904 => Some(PgType::Int4range),
            3926 => Some(PgType::Int8range),
            3906 => Some(PgType::Numrange),
            650 => Some(PgType::Cidr),
            869 => Some(PgType::Inet),
            829 => Some(PgType::Macaddr),
            774 => Some(PgType::Macaddr8),
            1560 => Some(PgType::Bit),
            1562 => Some(PgType::Varbit),
            705 => Some(PgType::Unknown),
            _ => None,
        }
    }

    pub fn to_oid(&self) -> i32 {
        *self as i32
    }

    pub fn name(&self) -> &'static str {
        match self {
            PgType::Bool => "bool",
            PgType::Int2 => "int2",
            PgType::Int4 => "int4",
            PgType::Int8 => "int8",
            PgType::Float4 => "float4",
            PgType::Float8 => "float8",
            PgType::Text => "text",
            PgType::Varchar => "varchar",
            PgType::Char => "char",
            PgType::Uuid => "uuid",
            PgType::Json => "json",
            PgType::Jsonb => "jsonb",
            PgType::Date => "date",
            PgType::Time => "time",
            PgType::Timestamp => "timestamp",
            PgType::Timestamptz => "timestamptz",
            PgType::Timetz => "timetz",
            PgType::Interval => "interval",
            PgType::Numeric => "numeric",
            PgType::Bytea => "bytea",
            PgType::Money => "money",
            PgType::Int4range => "int4range",
            PgType::Int8range => "int8range",
            PgType::Numrange => "numrange",
            PgType::Cidr => "cidr",
            PgType::Inet => "inet",
            PgType::Macaddr => "macaddr",
            PgType::Macaddr8 => "macaddr8",
            PgType::Bit => "bit",
            PgType::Varbit => "varbit",
            PgType::Unknown => "unknown",
        }
    }
}

/// Maps between PostgreSQL types and SQLite types
pub struct TypeMapper {
    pg_to_sqlite: HashMap<String, String>,
    sqlite_to_pg: HashMap<String, PgType>,
}

impl TypeMapper {
    pub fn new() -> Self {
        let mut mapper = TypeMapper {
            pg_to_sqlite: HashMap::new(),
            sqlite_to_pg: HashMap::new(),
        };

        // PostgreSQL to SQLite mappings
        mapper.pg_to_sqlite.insert("bool".to_string(), "INTEGER".to_string());
        mapper.pg_to_sqlite.insert("boolean".to_string(), "INTEGER".to_string());
        mapper.pg_to_sqlite.insert("int2".to_string(), "INTEGER".to_string());
        mapper.pg_to_sqlite.insert("smallint".to_string(), "INTEGER".to_string());
        mapper.pg_to_sqlite.insert("int4".to_string(), "INTEGER".to_string());
        mapper.pg_to_sqlite.insert("integer".to_string(), "INTEGER".to_string());
        mapper.pg_to_sqlite.insert("int8".to_string(), "INTEGER".to_string());
        mapper.pg_to_sqlite.insert("bigint".to_string(), "INTEGER".to_string());
        mapper.pg_to_sqlite.insert("float4".to_string(), "DECIMAL".to_string());
        mapper.pg_to_sqlite.insert("real".to_string(), "DECIMAL".to_string());
        mapper.pg_to_sqlite.insert("float8".to_string(), "DECIMAL".to_string());
        mapper.pg_to_sqlite.insert("double precision".to_string(), "DECIMAL".to_string());
        mapper.pg_to_sqlite.insert("text".to_string(), "TEXT".to_string());
        mapper.pg_to_sqlite.insert("varchar".to_string(), "TEXT".to_string());
        mapper.pg_to_sqlite.insert("char".to_string(), "TEXT".to_string());
        mapper.pg_to_sqlite.insert("uuid".to_string(), "TEXT".to_string());
        mapper.pg_to_sqlite.insert("json".to_string(), "TEXT".to_string());
        mapper.pg_to_sqlite.insert("jsonb".to_string(), "TEXT".to_string());
        mapper.pg_to_sqlite.insert("date".to_string(), "INTEGER".to_string());
        mapper.pg_to_sqlite.insert("time".to_string(), "INTEGER".to_string());
        mapper.pg_to_sqlite.insert("timestamp".to_string(), "INTEGER".to_string());
        mapper.pg_to_sqlite.insert("timestamptz".to_string(), "INTEGER".to_string());
        mapper.pg_to_sqlite.insert("numeric".to_string(), "DECIMAL".to_string());
        mapper.pg_to_sqlite.insert("decimal".to_string(), "DECIMAL".to_string());
        mapper.pg_to_sqlite.insert("bytea".to_string(), "BLOB".to_string());
        
        // Additional mappings from PRD
        mapper.pg_to_sqlite.insert("serial".to_string(), "INTEGER".to_string());
        mapper.pg_to_sqlite.insert("bigserial".to_string(), "INTEGER".to_string());
        mapper.pg_to_sqlite.insert("character varying".to_string(), "TEXT".to_string());
        mapper.pg_to_sqlite.insert("character".to_string(), "TEXT".to_string());
        mapper.pg_to_sqlite.insert("timestamp with time zone".to_string(), "INTEGER".to_string());
        mapper.pg_to_sqlite.insert("timestamp without time zone".to_string(), "INTEGER".to_string());
        mapper.pg_to_sqlite.insert("time with time zone".to_string(), "INTEGER".to_string());
        mapper.pg_to_sqlite.insert("time without time zone".to_string(), "INTEGER".to_string());
        mapper.pg_to_sqlite.insert("timetz".to_string(), "INTEGER".to_string());
        mapper.pg_to_sqlite.insert("interval".to_string(), "INTEGER".to_string());
        
        // New type mappings
        mapper.pg_to_sqlite.insert("money".to_string(), "TEXT".to_string());
        mapper.pg_to_sqlite.insert("int4range".to_string(), "TEXT".to_string());
        mapper.pg_to_sqlite.insert("int8range".to_string(), "TEXT".to_string());
        mapper.pg_to_sqlite.insert("numrange".to_string(), "TEXT".to_string());
        mapper.pg_to_sqlite.insert("cidr".to_string(), "TEXT".to_string());
        mapper.pg_to_sqlite.insert("inet".to_string(), "TEXT".to_string());
        mapper.pg_to_sqlite.insert("macaddr".to_string(), "TEXT".to_string());
        mapper.pg_to_sqlite.insert("macaddr8".to_string(), "TEXT".to_string());
        mapper.pg_to_sqlite.insert("bit".to_string(), "TEXT".to_string());
        mapper.pg_to_sqlite.insert("bit varying".to_string(), "TEXT".to_string());
        mapper.pg_to_sqlite.insert("varbit".to_string(), "TEXT".to_string());

        // SQLite to PostgreSQL mappings (for result sets) 
        // Note: These should match SchemaTypeMapper::sqlite_type_to_pg_oid for consistency
        mapper.sqlite_to_pg.insert("INTEGER".to_string(), PgType::Int4); // OID 23, matches SchemaTypeMapper
        mapper.sqlite_to_pg.insert("REAL".to_string(), PgType::Float8);   // OID 701, matches SchemaTypeMapper  
        mapper.sqlite_to_pg.insert("TEXT".to_string(), PgType::Text);     // OID 25, matches SchemaTypeMapper
        mapper.sqlite_to_pg.insert("BLOB".to_string(), PgType::Bytea);    // OID 17, matches SchemaTypeMapper
        mapper.sqlite_to_pg.insert("DECIMAL".to_string(), PgType::Numeric); // OID 1700 for custom DECIMAL type

        mapper
    }

    /// Convert PostgreSQL type name to SQLite type
    pub fn pg_to_sqlite(&self, pg_type: &str) -> &str {
        // Fast path for common types (case-sensitive match first)
        match pg_type {
            "text" | "TEXT" => return "TEXT",
            "integer" | "INTEGER" | "int4" | "INT4" => return "INTEGER",
            "bigint" | "BIGINT" | "int8" | "INT8" => return "INTEGER",
            "boolean" | "BOOLEAN" | "bool" | "BOOL" => return "INTEGER",
            "real" | "REAL" | "float4" | "FLOAT4" => return "DECIMAL",
            "double precision" | "DOUBLE PRECISION" | "float8" | "FLOAT8" => return "DECIMAL",
            "varchar" | "VARCHAR" => return "TEXT",
            "numeric" | "NUMERIC" | "decimal" | "DECIMAL" => return "DECIMAL",
            _ => {}
        }
        
        // Fall back to hashmap lookup with lowercasing
        self.pg_to_sqlite
            .get(pg_type.to_lowercase().as_str())
            .map(|s| s.as_str())
            .unwrap_or("TEXT") // Default to TEXT for unknown types
    }

    /// Convert PostgreSQL type name to SQLite type for CREATE TABLE statements
    /// Handles special cases like SERIAL types that need AUTOINCREMENT
    pub fn pg_to_sqlite_for_create_table(&self, pg_type: &str) -> String {
        let normalized_type = self.normalize_parametric_type(pg_type);
        
        // Handle SERIAL types specially - they need AUTOINCREMENT
        match normalized_type.to_uppercase().as_str() {
            "SERIAL" => "INTEGER PRIMARY KEY AUTOINCREMENT".to_string(),
            "BIGSERIAL" => "INTEGER PRIMARY KEY AUTOINCREMENT".to_string(),
            _ => {
                // Check for parametric types first
                if let Some(base_type) = self.extract_base_type(&normalized_type) {
                    self.pg_to_sqlite(&base_type).to_string()
                } else {
                    self.pg_to_sqlite(&normalized_type).to_string()
                }
            }
        }
    }
    
    /// Normalize parametric types by removing extra spaces
    fn normalize_parametric_type(&self, pg_type: &str) -> String {
        // Handle cases like "CHARACTER VARYING (255)" -> "CHARACTER VARYING(255)"
        // Avoid to_uppercase if possible by checking common patterns
        let trimmed = pg_type.trim();
        
        // Fast check for already normalized
        if !trimmed.contains(" (") {
            return trimmed.to_string();
        }
        
        let normalized = trimmed.to_uppercase();
        // Remove spaces before parentheses
        normalized.replace(" (", "(")
    }
    
    /// Extract base type from parametric types like VARCHAR(255) -> VARCHAR
    fn extract_base_type(&self, pg_type: &str) -> Option<String> {
        // Check common patterns without uppercasing first
        if pg_type.starts_with("varchar") || pg_type.starts_with("VARCHAR") {
            return Some("varchar".to_string());
        }
        if pg_type.starts_with("character varying") || pg_type.starts_with("CHARACTER VARYING") {
            return Some("character varying".to_string());
        }
        if pg_type.starts_with("char") || pg_type.starts_with("CHAR") {
            return Some("char".to_string());
        }
        if pg_type.starts_with("numeric") || pg_type.starts_with("NUMERIC") {
            return Some("numeric".to_string());
        }
        if pg_type.starts_with("decimal") || pg_type.starts_with("DECIMAL") {
            return Some("decimal".to_string());
        }
        if pg_type.starts_with("bit") || pg_type.starts_with("BIT") {
            return Some("bit".to_string());
        }
        
        let normalized = pg_type.to_uppercase();
        
        // Handle parametric types
        if normalized.contains('(') {
            if let Some(base) = normalized.split('(').next() {
                let base_trimmed = base.trim();
                // Check if this is a known parametric type
                match base_trimmed {
                    "VARCHAR" | "CHARACTER VARYING" | "CHAR" | "CHARACTER" |
                    "NUMERIC" | "DECIMAL" | "BIT" => {
                        return Some(base_trimmed.to_lowercase());
                    }
                    _ => {}
                }
            }
        }
        
        // Handle multi-word types
        if normalized.starts_with("DOUBLE PRECISION") {
            return Some("double precision".to_string());
        }
        if normalized.starts_with("TIME WITH TIME ZONE") {
            return Some("time with time zone".to_string());
        }
        if normalized.starts_with("TIME WITHOUT TIME ZONE") {
            return Some("time without time zone".to_string());
        }
        if normalized.starts_with("TIMESTAMP WITH TIME ZONE") {
            return Some("timestamp with time zone".to_string());
        }
        if normalized.starts_with("TIMESTAMP WITHOUT TIME ZONE") {
            return Some("timestamp without time zone".to_string());
        }
        if normalized.starts_with("CHARACTER VARYING") {
            return Some("character varying".to_string());
        }
        if normalized.starts_with("BIT VARYING") {
            return Some("bit varying".to_string());
        }
        
        None
    }

    /// Infer PostgreSQL type from SQLite type name
    pub fn sqlite_to_pg(&self, sqlite_type: &str) -> PgType {
        // Fast path for common types
        match sqlite_type {
            "INTEGER" | "integer" => return PgType::Int4,
            "REAL" | "real" => return PgType::Float8,
            "TEXT" | "text" => return PgType::Text,
            "BLOB" | "blob" => return PgType::Bytea,
            "DECIMAL" | "decimal" => return PgType::Numeric,
            _ => {}
        }
        
        self.sqlite_to_pg
            .get(sqlite_type.to_uppercase().as_str())
            .copied()
            .unwrap_or(PgType::Text) // Default to text for unknown types
    }

    /// Infer PostgreSQL type from a value
    pub fn infer_pg_type_from_value(value: &str) -> PgType {
        // Try to parse as various types
        if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
            return PgType::Bool;
        }
        
        // Check if it looks like a bit string (only 0s and 1s, and more than 3 characters to avoid confusion with small integers)
        if value.chars().all(|c| c == '0' || c == '1') && !value.is_empty() && value.len() > 3 {
            return PgType::Bit;
        }
        
        if value.parse::<i64>().is_ok() {
            return PgType::Int8;
        }
        
        if value.parse::<f64>().is_ok() {
            return PgType::Float8;
        }
        
        // Check if it looks like a UUID (8-4-4-4-12 pattern)
        if value.len() == 36 && value.chars().filter(|&c| c == '-').count() == 4 {
            let parts: Vec<&str> = value.split('-').collect();
            if parts.len() == 5 
                && parts[0].len() == 8 
                && parts[1].len() == 4 
                && parts[2].len() == 4 
                && parts[3].len() == 4 
                && parts[4].len() == 12 
                && parts.iter().all(|p| p.chars().all(|c| c.is_ascii_hexdigit())) {
                return PgType::Uuid;
            }
        }
        
        // Check if it looks like JSON
        if (value.starts_with('{') && value.ends_with('}')) 
            || (value.starts_with('[') && value.ends_with(']')) {
            return PgType::Json;
        }
        
        // Check if it looks like MONEY (starts with currency symbol)
        if value.starts_with('$') || value.starts_with('€') || value.starts_with('£') {
            return PgType::Money;
        }
        
        // Check if it looks like a network address (CIDR/INET)
        if value.contains('/') && value.split('/').count() == 2 {
            if let Some(ip_part) = value.split('/').next() {
                if Self::is_ip_address(ip_part) {
                    return PgType::Cidr;
                }
            }
        }
        if Self::is_ip_address(value) {
            return PgType::Inet;
        }
        
        // Check if it looks like a MAC address
        if Self::is_mac_address(value) {
            if value.len() == 23 { // XX:XX:XX:XX:XX:XX:XX:XX format
                return PgType::Macaddr8;
            } else if value.len() == 17 { // XX:XX:XX:XX:XX:XX format
                return PgType::Macaddr;
            }
        }
        
        // Check if it looks like a range (contains brackets and comma)
        if (value.starts_with('[') || value.starts_with('(')) 
            && (value.ends_with(']') || value.ends_with(')')) 
            && value.contains(',') {
            return PgType::Int4range; // Default range type
        }
        
        // Default to text
        PgType::Text
    }
    
    fn is_ip_address(s: &str) -> bool {
        // Simple IPv4 check
        if s.split('.').count() == 4 {
            return s.split('.').all(|part| {
                part.parse::<u8>().is_ok()
            });
        }
        // Simple IPv6 check - but exclude MAC addresses (6 or 8 colon-separated hex pairs)
        if s.contains(':') && s.len() > 2 {
            let colon_parts: Vec<&str> = s.split(':').collect();
            // MAC addresses have exactly 6 or 8 parts, each 2 characters
            if (colon_parts.len() == 6 || colon_parts.len() == 8) 
                && colon_parts.iter().all(|p| p.len() == 2 && p.chars().all(|c| c.is_ascii_hexdigit())) {
                return false; // This is a MAC address, not an IP
            }
            // Check if it's a valid IPv6-like format
            return s.chars().all(|c| c.is_ascii_hexdigit() || c == ':');
        }
        false
    }
    
    fn is_mac_address(s: &str) -> bool {
        // Check for colon-separated hex pairs
        if s.contains(':') {
            let parts: Vec<&str> = s.split(':').collect();
            if parts.len() == 6 || parts.len() == 8 {
                return parts.iter().all(|part| {
                    part.len() == 2 && part.chars().all(|c| c.is_ascii_hexdigit())
                });
            }
        }
        // Check for hyphen-separated hex pairs
        if s.contains('-') {
            let parts: Vec<&str> = s.split('-').collect();
            if parts.len() == 6 || parts.len() == 8 {
                return parts.iter().all(|part| {
                    part.len() == 2 && part.chars().all(|c| c.is_ascii_hexdigit())
                });
            }
        }
        false
    }
}

impl Default for TypeMapper {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_to_sqlite_for_create_table_serial() {
        let mapper = TypeMapper::new();
        
        // Test SERIAL types get AUTOINCREMENT
        assert_eq!(mapper.pg_to_sqlite_for_create_table("SERIAL"), "INTEGER PRIMARY KEY AUTOINCREMENT");
        assert_eq!(mapper.pg_to_sqlite_for_create_table("serial"), "INTEGER PRIMARY KEY AUTOINCREMENT");
        assert_eq!(mapper.pg_to_sqlite_for_create_table("BIGSERIAL"), "INTEGER PRIMARY KEY AUTOINCREMENT");
        assert_eq!(mapper.pg_to_sqlite_for_create_table("bigserial"), "INTEGER PRIMARY KEY AUTOINCREMENT");
    }
    
    #[test]
    fn test_pg_to_sqlite_for_create_table_parametric() {
        let mapper = TypeMapper::new();
        
        // Test parametric types
        assert_eq!(mapper.pg_to_sqlite_for_create_table("VARCHAR(255)"), "TEXT");
        assert_eq!(mapper.pg_to_sqlite_for_create_table("CHAR(10)"), "TEXT");
        assert_eq!(mapper.pg_to_sqlite_for_create_table("NUMERIC(10,2)"), "DECIMAL");
        assert_eq!(mapper.pg_to_sqlite_for_create_table("BIT(8)"), "TEXT");
        assert_eq!(mapper.pg_to_sqlite_for_create_table("CHARACTER VARYING(100)"), "TEXT");
    }
    
    #[test]
    fn test_pg_to_sqlite_for_create_table_multiword() {
        let mapper = TypeMapper::new();
        
        // Test multi-word types
        assert_eq!(mapper.pg_to_sqlite_for_create_table("DOUBLE PRECISION"), "DECIMAL");
        assert_eq!(mapper.pg_to_sqlite_for_create_table("TIME WITH TIME ZONE"), "INTEGER");
        assert_eq!(mapper.pg_to_sqlite_for_create_table("TIMESTAMP WITHOUT TIME ZONE"), "INTEGER");
        assert_eq!(mapper.pg_to_sqlite_for_create_table("BIT VARYING"), "TEXT");
    }
    
    #[test]
    fn test_extract_base_type() {
        let mapper = TypeMapper::new();
        
        // Test parametric type extraction
        assert_eq!(mapper.extract_base_type("VARCHAR(255)"), Some("varchar".to_string()));
        assert_eq!(mapper.extract_base_type("NUMERIC(10,2)"), Some("numeric".to_string()));
        assert_eq!(mapper.extract_base_type("BIT(8)"), Some("bit".to_string()));
        
        // Test multi-word types
        assert_eq!(mapper.extract_base_type("DOUBLE PRECISION"), Some("double precision".to_string()));
        assert_eq!(mapper.extract_base_type("CHARACTER VARYING(100)"), Some("character varying".to_string()));
        
        // Test non-parametric types
        assert_eq!(mapper.extract_base_type("INTEGER"), None);
        assert_eq!(mapper.extract_base_type("TEXT"), None);
    }
    
    #[test]
    fn test_normalize_parametric_type() {
        let mapper = TypeMapper::new();
        
        // Test normalization
        assert_eq!(mapper.normalize_parametric_type("VARCHAR (255)"), "VARCHAR(255)");
        assert_eq!(mapper.normalize_parametric_type("CHARACTER VARYING (100)"), "CHARACTER VARYING(100)");
        assert_eq!(mapper.normalize_parametric_type("  NUMERIC ( 10 , 2 )  "), "NUMERIC( 10 , 2 )");
    }
}