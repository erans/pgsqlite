use std::io;
use rusqlite::types::Value as SqliteValue;
use crate::protocol::{MappedValue, MappedValueFactory, MemoryMappedConfig};
use crate::types::PgType;
// use crate::types::value_converter::ValueConverter; // Reserved for future enhanced type conversion
use tracing::debug;

/// Configuration for value handling strategies
#[derive(Debug, Clone)]
pub struct ValueHandlerConfig {
    /// Memory mapping configuration
    pub mmap_config: MemoryMappedConfig,
    /// Enable memory mapping optimization
    pub enable_mmap: bool,
    /// Threshold for considering a value "large" (in bytes)
    pub large_value_threshold: usize,
}

impl Default for ValueHandlerConfig {
    fn default() -> Self {
        Self {
            mmap_config: MemoryMappedConfig::from_env(),
            enable_mmap: std::env::var("PGSQLITE_ENABLE_MMAP").unwrap_or_default() == "1",
            large_value_threshold: 32 * 1024, // 32KB
        }
    }
}

/// Handles conversion of SQLite values to optimized representations for transmission
pub struct ValueHandler {
    config: ValueHandlerConfig,
    mmap_factory: MappedValueFactory,
}

impl ValueHandler {
    /// Create a new value handler with default configuration
    pub fn new() -> Self {
        let config = ValueHandlerConfig::default();
        let mmap_factory = MappedValueFactory::with_config(config.mmap_config.clone());
        
        Self {
            config,
            mmap_factory,
        }
    }
    
    /// Create a value handler with custom configuration
    pub fn with_config(config: ValueHandlerConfig) -> Self {
        let mmap_factory = MappedValueFactory::with_config(config.mmap_config.clone());
        
        Self {
            config,
            mmap_factory,
        }
    }
    
    /// Convert a SQLite value to a PostgreSQL-encoded value with memory mapping optimization
    pub fn convert_value(
        &self,
        value: &SqliteValue,
        pg_type_oid: i32,
        binary_format: bool,
    ) -> io::Result<Option<MappedValue>> {
        match value {
            SqliteValue::Null => Ok(None),
            
            SqliteValue::Blob(blob_data) => {
                self.handle_blob_value(blob_data, pg_type_oid, binary_format)
            }
            
            SqliteValue::Text(text_data) => {
                self.handle_text_value(text_data, pg_type_oid, binary_format)
            }
            
            SqliteValue::Integer(int_val) => {
                self.handle_integer_value(*int_val, pg_type_oid, binary_format)
            }
            
            SqliteValue::Real(real_val) => {
                self.handle_real_value(*real_val, pg_type_oid, binary_format)
            }
        }
    }
    
    /// Handle BLOB values with potential memory mapping
    fn handle_blob_value(
        &self,
        blob_data: &[u8],
        pg_type_oid: i32,
        binary_format: bool,
    ) -> io::Result<Option<MappedValue>> {
        if blob_data.is_empty() {
            return Ok(Some(MappedValue::Memory(Vec::new())));
        }
        
        // Check if this should use memory mapping
        if self.config.enable_mmap && blob_data.len() >= self.config.large_value_threshold {
            debug!("Using memory mapping for large BLOB value: {} bytes", blob_data.len());
            
            // Convert to PostgreSQL format if needed
            let pg_data = if binary_format {
                // Binary format - use as-is for BYTEA
                blob_data.to_vec()
            } else {
                // Text format - hex encode for BYTEA
                if pg_type_oid == 17 { // BYTEA
                    format!("\\x{}", hex::encode(blob_data)).into_bytes()
                } else {
                    blob_data.to_vec()
                }
            };
            
            Ok(Some(self.mmap_factory.create_from_blob(&pg_data)?))
        } else {
            // Use regular memory storage
            let pg_data = self.convert_blob_to_pg_format(blob_data, pg_type_oid, binary_format);
            Ok(Some(MappedValue::Memory(pg_data)))
        }
    }
    
    /// Handle text values with potential memory mapping for large strings
    fn handle_text_value(
        &self,
        text_data: &str,
        _pg_type_oid: i32,
        binary_format: bool,
    ) -> io::Result<Option<MappedValue>> {
        if text_data.is_empty() {
            return Ok(Some(MappedValue::Memory(Vec::new())));
        }
        
        // Check if this should use memory mapping
        if self.config.enable_mmap && text_data.len() >= self.config.large_value_threshold {
            debug!("Using memory mapping for large text value: {} bytes", text_data.len());
            Ok(Some(self.mmap_factory.create_from_text(text_data)?))
        } else {
            // Use regular memory storage
            let pg_data = if binary_format {
                text_data.as_bytes().to_vec()
            } else {
                text_data.as_bytes().to_vec()
            };
            Ok(Some(MappedValue::Memory(pg_data)))
        }
    }
    
    /// Handle integer values
    fn handle_integer_value(
        &self,
        int_val: i64,
        pg_type_oid: i32,
        binary_format: bool,
    ) -> io::Result<Option<MappedValue>> {
        let pg_data = if binary_format {
            self.convert_integer_binary(int_val, pg_type_oid)
        } else {
            int_val.to_string().into_bytes()
        };
        
        Ok(Some(MappedValue::Memory(pg_data)))
    }
    
    /// Handle floating-point values
    fn handle_real_value(
        &self,
        real_val: f64,
        pg_type_oid: i32,
        binary_format: bool,
    ) -> io::Result<Option<MappedValue>> {
        let pg_data = if binary_format {
            self.convert_real_binary(real_val, pg_type_oid)
        } else {
            real_val.to_string().into_bytes()
        };
        
        Ok(Some(MappedValue::Memory(pg_data)))
    }
    
    /// Convert BLOB to PostgreSQL format
    fn convert_blob_to_pg_format(&self, blob_data: &[u8], pg_type_oid: i32, binary_format: bool) -> Vec<u8> {
        if binary_format {
            // Binary format - use as-is for BYTEA
            blob_data.to_vec()
        } else {
            // Text format
            if pg_type_oid == 17 { // BYTEA
                format!("\\x{}", hex::encode(blob_data)).into_bytes()
            } else {
                blob_data.to_vec()
            }
        }
    }
    
    /// Convert integer to binary PostgreSQL format
    fn convert_integer_binary(&self, value: i64, pg_type_oid: i32) -> Vec<u8> {
        use crate::protocol::BinaryEncoder;
        
        match pg_type_oid {
            t if t == PgType::Int2.to_oid() => BinaryEncoder::encode_int2(value as i16), // INT2
            t if t == PgType::Int4.to_oid() => BinaryEncoder::encode_int4(value as i32), // INT4
            t if t == PgType::Int8.to_oid() => BinaryEncoder::encode_int8(value),        // INT8
            t if t == PgType::Bool.to_oid() => BinaryEncoder::encode_bool(value != 0),   // BOOL
            _ => value.to_string().into_bytes(),           // Fallback to text
        }
    }
    
    /// Convert real to binary PostgreSQL format
    fn convert_real_binary(&self, value: f64, pg_type_oid: i32) -> Vec<u8> {
        use crate::protocol::BinaryEncoder;
        
        match pg_type_oid {
            t if t == PgType::Float4.to_oid() => BinaryEncoder::encode_float4(value as f32), // FLOAT4
            t if t == PgType::Float8.to_oid() => BinaryEncoder::encode_float8(value),        // FLOAT8
            _ => value.to_string().into_bytes(),              // Fallback to text
        }
    }
    
    /// Convert a row of SQLite values to mapped values
    pub fn convert_row(
        &self,
        values: &[SqliteValue],
        type_oids: &[i32],
        binary_format: bool,
    ) -> io::Result<Vec<Option<MappedValue>>> {
        if values.len() != type_oids.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Values and type OIDs length mismatch"
            ));
        }
        
        let mut result = Vec::with_capacity(values.len());
        
        for (value, &type_oid) in values.iter().zip(type_oids.iter()) {
            let mapped_value = self.convert_value(value, type_oid, binary_format)?;
            result.push(mapped_value);
        }
        
        Ok(result)
    }
    
    /// Get statistics about memory usage
    pub fn get_memory_stats(&self) -> ValueHandlerStats {
        ValueHandlerStats {
            mmap_threshold: self.config.large_value_threshold,
            mmap_enabled: self.config.enable_mmap,
            mmap_min_size: self.config.mmap_config.min_size_for_mmap,
        }
    }
}

/// Statistics about value handler performance
#[derive(Debug, Clone)]
pub struct ValueHandlerStats {
    pub mmap_threshold: usize,
    pub mmap_enabled: bool,
    pub mmap_min_size: usize,
}

impl Default for ValueHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // use crate::protocol::MemoryMappedConfig; // Used by test configuration
    
    #[test]
    fn test_value_handler_creation() {
        let handler = ValueHandler::new();
        let stats = handler.get_memory_stats();
        
        assert_eq!(stats.mmap_threshold, 32 * 1024);
        assert!(!stats.mmap_enabled); // Default is disabled
    }
    
    #[test]
    fn test_small_blob_handling() {
        let handler = ValueHandler::new();
        let small_blob = vec![1, 2, 3, 4, 5];
        let sqlite_value = SqliteValue::Blob(small_blob.clone());
        
        let result = handler.convert_value(&sqlite_value, 17, false).unwrap(); // BYTEA
        
        match result {
            Some(MappedValue::Memory(data)) => {
                // Should be hex-encoded for text format
                let expected = format!("\\x{}", hex::encode(&small_blob));
                assert_eq!(data, expected.as_bytes());
            }
            _ => panic!("Expected memory storage for small blob"),
        }
    }
    
    #[test]
    fn test_large_blob_with_mmap_disabled() {
        let handler = ValueHandler::new(); // mmap disabled by default
        let large_blob = vec![42u8; 64 * 1024]; // 64KB
        let sqlite_value = SqliteValue::Blob(large_blob.clone());
        
        let result = handler.convert_value(&sqlite_value, 17, false).unwrap(); // BYTEA
        
        // Should still use memory even for large values when mmap is disabled
        match result {
            Some(MappedValue::Memory(_)) => {}
            _ => panic!("Expected memory storage when mmap is disabled"),
        }
    }
    
    #[test]
    fn test_text_value_handling() {
        let handler = ValueHandler::new();
        let text_value = SqliteValue::Text("hello world".to_string());
        
        let result = handler.convert_value(&text_value, 25, false).unwrap(); // TEXT
        
        match result {
            Some(MappedValue::Memory(data)) => {
                assert_eq!(data, b"hello world");
            }
            _ => panic!("Expected memory storage for text"),
        }
    }
    
    #[test]
    fn test_integer_value_handling() {
        let handler = ValueHandler::new();
        let int_value = SqliteValue::Integer(42);
        
        let result = handler.convert_value(&int_value, 23, false).unwrap(); // INT4
        
        match result {
            Some(MappedValue::Memory(data)) => {
                assert_eq!(data, b"42");
            }
            _ => panic!("Expected memory storage for integer"),
        }
    }
    
    #[test]
    fn test_null_value_handling() {
        let handler = ValueHandler::new();
        let null_value = SqliteValue::Null;
        
        let result = handler.convert_value(&null_value, 25, false).unwrap();
        assert!(result.is_none());
    }
    
    #[test]
    fn test_row_conversion() {
        let handler = ValueHandler::new();
        let values = vec![
            SqliteValue::Integer(42),
            SqliteValue::Text("hello".to_string()),
            SqliteValue::Null,
        ];
        let type_oids = vec![23, 25, 25]; // INT4, TEXT, TEXT
        
        let result = handler.convert_row(&values, &type_oids, false).unwrap();
        
        assert_eq!(result.len(), 3);
        assert!(result[0].is_some());
        assert!(result[1].is_some());
        assert!(result[2].is_none());
    }
    
    #[test]
    fn test_mmap_enabled_config() {
        let mut config = ValueHandlerConfig::default();
        config.enable_mmap = true;
        config.large_value_threshold = 100; // Very small threshold for testing
        config.mmap_config.min_size_for_mmap = 50;
        
        let handler = ValueHandler::with_config(config);
        let large_text = "x".repeat(150); // Larger than threshold
        let sqlite_value = SqliteValue::Text(large_text.clone());
        
        let result = handler.convert_value(&sqlite_value, 25, false).unwrap(); // TEXT
        
        // Should use memory mapping for large text when enabled
        match result {
            Some(mapped_value) => {
                assert_eq!(mapped_value.as_slice(), large_text.as_bytes());
            }
            _ => panic!("Expected mapped value for large text"),
        }
    }
}