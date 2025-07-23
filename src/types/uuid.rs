use crate::PgSqliteError;

/// UUID utilities for PostgreSQL compatibility
pub struct UuidHandler;

impl UuidHandler {
    /// Validate UUID format
    pub fn validate_uuid(value: &str) -> bool {
        if value.len() != 36 {
            return false;
        }
        
        let parts: Vec<&str> = value.split('-').collect();
        if parts.len() != 5 {
            return false;
        }
        
        parts[0].len() == 8 
            && parts[1].len() == 4 
            && parts[2].len() == 4 
            && parts[3].len() == 4 
            && parts[4].len() == 12 
            && parts.iter().all(|p| p.chars().all(|c| c.is_ascii_hexdigit()))
    }
    
    /// Normalize UUID to lowercase
    pub fn normalize_uuid(value: &str) -> String {
        value.to_lowercase()
    }
    
    /// Convert UUID string to bytes (for binary protocol)
    pub fn uuid_to_bytes(value: &str) -> Result<Vec<u8>, PgSqliteError> {
        if !Self::validate_uuid(value) {
            return Err(PgSqliteError::TypeConversion(format!("Invalid UUID format: {value}")));
        }
        
        let normalized = value.replace('-', "");
        hex::decode(normalized)
            .map_err(|e| PgSqliteError::TypeConversion(format!("Failed to decode UUID: {e}")))
    }
    
    /// Convert bytes to UUID string
    pub fn bytes_to_uuid(bytes: &[u8]) -> Result<String, PgSqliteError> {
        if bytes.len() != 16 {
            return Err(PgSqliteError::TypeConversion(format!("Invalid UUID byte length: {}", bytes.len())));
        }
        
        let hex = hex::encode(bytes);
        Ok(format!("{}-{}-{}-{}-{}",
            &hex[0..8],
            &hex[8..12],
            &hex[12..16],
            &hex[16..20],
            &hex[20..32]
        ))
    }
}

/// SQLite function for UUID generation (v4)
pub fn generate_uuid_v4() -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    let mut bytes = [0u8; 16];
    rng.fill(&mut bytes);
    
    // Set version (4) and variant bits
    bytes[6] = (bytes[6] & 0x0f) | 0x40; // Version 4
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // Variant 10
    
    format!("{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6], bytes[7],
        bytes[8], bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_validate_uuid() {
        assert!(UuidHandler::validate_uuid("550e8400-e29b-41d4-a716-446655440000"));
        assert!(UuidHandler::validate_uuid("6ba7b810-9dad-11d1-80b4-00c04fd430c8"));
        assert!(UuidHandler::validate_uuid("00000000-0000-0000-0000-000000000000"));
        
        assert!(!UuidHandler::validate_uuid("550e8400-e29b-41d4-a716-44665544000")); // Too short
        assert!(!UuidHandler::validate_uuid("550e8400-e29b-41d4-a716-4466554400000")); // Too long
        assert!(!UuidHandler::validate_uuid("550e8400e29b41d4a716446655440000")); // No dashes
        assert!(!UuidHandler::validate_uuid("550e8400-e29b-41d4-a716-44665544000g")); // Invalid char
    }
    
    #[test]
    fn test_uuid_conversions() {
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        
        // Test to bytes
        let bytes = UuidHandler::uuid_to_bytes(uuid_str).unwrap();
        assert_eq!(bytes.len(), 16);
        
        // Test back to string
        let uuid_back = UuidHandler::bytes_to_uuid(&bytes).unwrap();
        assert_eq!(uuid_back, uuid_str);
    }
    
    #[test]
    fn test_generate_uuid_v4() {
        let uuid1 = generate_uuid_v4();
        let uuid2 = generate_uuid_v4();
        
        // Should be valid UUIDs
        assert!(UuidHandler::validate_uuid(&uuid1));
        assert!(UuidHandler::validate_uuid(&uuid2));
        
        // Should be different
        assert_ne!(uuid1, uuid2);
        
        // Should have version 4 marker
        assert_eq!(&uuid1[14..15], "4");
        assert_eq!(&uuid2[14..15], "4");
    }
}