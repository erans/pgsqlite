use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use crate::protocol::binary::BinaryEncoder;
use crate::config::CONFIG;
use super::QueryFingerprint;
use itoa;

/// Pre-computed execution metadata for a query
#[derive(Clone, Debug)]
pub struct ExecutionMetadata {
    /// Column names in result order
    pub columns: Vec<String>,
    /// Pre-computed boolean column flags for fast conversion
    pub boolean_columns: Vec<bool>,
    /// Pre-computed type conversion functions (indexes into lookup table)
    pub type_converters: Vec<u8>,
    /// Column type OIDs for binary encoding
    pub type_oids: Vec<i32>,
    /// Result format codes (0=text, 1=binary) per column
    pub result_formats: Vec<i16>,
    /// Whether this query can use fast path execution
    pub fast_path_eligible: bool,
    /// Prepared statement SQL (may be rewritten)
    pub prepared_sql: String,
    /// Expected parameter count
    pub param_count: usize,
}

/// Optimized execution cache that stores complete execution context
pub struct ExecutionCache {
    cache: Arc<RwLock<HashMap<u64, CacheEntry>>>,
    ttl: Duration,
}

struct CacheEntry {
    metadata: ExecutionMetadata,
    cached_at: Instant,
    hit_count: u64,
}

impl ExecutionCache {
    pub fn new(ttl_seconds: u64) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            ttl: Duration::from_secs(ttl_seconds),
        }
    }

    /// Get cached execution metadata for a query
    pub fn get(&self, query_key: &str) -> Option<ExecutionMetadata> {
        // Use fingerprint with literals for queries without parameters
        // to avoid cache collisions between queries like "SELECT 42" and "SELECT 9999"
        let fingerprint = if query_key.contains('#') {
            // Has parameter types, use regular fingerprint
            QueryFingerprint::generate(query_key)
        } else {
            // No parameters, preserve literals to avoid collisions
            QueryFingerprint::generate_with_literals(query_key)
        };
        let mut cache = self.cache.write().unwrap();
        
        if let Some(entry) = cache.get_mut(&fingerprint) {
            if entry.cached_at.elapsed() < self.ttl {
                entry.hit_count += 1;
                return Some(entry.metadata.clone());
            } else {
                // Entry expired, remove it
                cache.remove(&fingerprint);
            }
        }
        
        None
    }

    /// Cache execution metadata for a query
    pub fn insert(&self, query_key: String, metadata: ExecutionMetadata) {
        // Use fingerprint with literals for queries without parameters
        // to avoid cache collisions between queries like "SELECT 42" and "SELECT 9999"
        let fingerprint = if query_key.contains('#') {
            // Has parameter types, use regular fingerprint
            QueryFingerprint::generate(&query_key)
        } else {
            // No parameters, preserve literals to avoid collisions
            QueryFingerprint::generate_with_literals(&query_key)
        };
        let mut cache = self.cache.write().unwrap();
        
        cache.insert(fingerprint, CacheEntry {
            metadata,
            cached_at: Instant::now(),
            hit_count: 0,
        });
    }

    /// Generate a cache key that includes parameter types for proper differentiation
    pub fn generate_key(query: &str, param_types: &[String]) -> String {
        if param_types.is_empty() {
            query.to_string()
        } else {
            format!("{}#{}", query, param_types.join(","))
        }
    }

    /// Clear the cache
    pub fn clear(&self) {
        self.cache.write().unwrap().clear();
    }

    /// Get cache statistics
    pub fn get_stats(&self) -> CacheStats {
        let cache = self.cache.read().unwrap();
        let total_entries = cache.len();
        let total_hits: u64 = cache.values().map(|entry| entry.hit_count).sum();
        
        CacheStats {
            total_entries,
            total_hits,
        }
    }
}

pub struct CacheStats {
    pub total_entries: usize,
    pub total_hits: u64,
}

/// Global execution cache instance
static GLOBAL_EXECUTION_CACHE: std::sync::LazyLock<ExecutionCache> = 
    std::sync::LazyLock::new(|| ExecutionCache::new(CONFIG.execution_cache_ttl));

/// Get the global execution cache
pub fn global_execution_cache() -> &'static ExecutionCache {
    &GLOBAL_EXECUTION_CACHE
}

/// Type converter function type
pub type TypeConverter = fn(&rusqlite::types::Value) -> Result<Vec<u8>, rusqlite::Error>;

/// Binary type converter function type
pub type BinaryTypeConverter = fn(&rusqlite::types::Value, i32) -> Result<Vec<u8>, rusqlite::Error>;

/// Pre-computed type converter lookup table
pub struct TypeConverterTable {
    converters: Vec<TypeConverter>,
    binary_converter: BinaryTypeConverter,
}

impl TypeConverterTable {
    pub fn new() -> Self {
        Self {
            binary_converter: |value, type_oid| {
                match BinaryEncoder::encode_value(value, type_oid, true) {
                    Some(bytes) => Ok(bytes),
                    None => {
                        // Fall back to text encoding
                        match value {
                            rusqlite::types::Value::Text(s) => Ok(s.as_bytes().to_vec()),
                            rusqlite::types::Value::Integer(i) => {
                                let mut buf = itoa::Buffer::new();
                                Ok(buf.format(*i).as_bytes().to_vec())
                            },
                            rusqlite::types::Value::Real(r) => Ok(r.to_string().as_bytes().to_vec()),
                            rusqlite::types::Value::Null => Ok(Vec::new()),
                            rusqlite::types::Value::Blob(b) => Ok(b.clone()),
                        }
                    }
                }
            },
            converters: vec![
                // 0: Text/String converter
                |value| match value {
                    rusqlite::types::Value::Text(s) => Ok(s.as_bytes().to_vec()),
                    rusqlite::types::Value::Integer(i) => {
                        let mut buf = itoa::Buffer::new();
                        Ok(buf.format(*i).as_bytes().to_vec())
                    },
                    rusqlite::types::Value::Real(r) => Ok(r.to_string().as_bytes().to_vec()),
                    rusqlite::types::Value::Null => Ok(Vec::new()),
                    _ => Ok("".as_bytes().to_vec()),
                },
                // 1: Integer converter  
                |value| match value {
                    rusqlite::types::Value::Integer(i) => {
                        let mut buf = itoa::Buffer::new();
                        Ok(buf.format(*i).as_bytes().to_vec())
                    },
                    rusqlite::types::Value::Text(s) => Ok(s.as_bytes().to_vec()),
                    rusqlite::types::Value::Null => Ok(Vec::new()),
                    _ => Ok("0".as_bytes().to_vec()),
                },
                // 2: Boolean converter (optimized for 0/1 -> f/t)
                |value| match value {
                    rusqlite::types::Value::Integer(i) => Ok(if *i == 0 { b"f".to_vec() } else { b"t".to_vec() }),
                    rusqlite::types::Value::Text(s) => {
                        let lower = s.to_lowercase();
                        Ok(if lower == "false" || lower == "f" || lower == "0" { 
                            b"f".to_vec() 
                        } else { 
                            b"t".to_vec() 
                        })
                    },
                    rusqlite::types::Value::Null => Ok(Vec::new()),
                    _ => Ok(b"f".to_vec()),
                },
                // 3: Float converter
                |value| match value {
                    rusqlite::types::Value::Real(r) => Ok(r.to_string().as_bytes().to_vec()),
                    rusqlite::types::Value::Integer(i) => Ok((*i as f64).to_string().as_bytes().to_vec()),
                    rusqlite::types::Value::Text(s) => Ok(s.as_bytes().to_vec()),
                    rusqlite::types::Value::Null => Ok(Vec::new()),
                    _ => Ok("0.0".as_bytes().to_vec()),
                },
                // 4: Blob converter
                |value| match value {
                    rusqlite::types::Value::Blob(b) => Ok(b.clone()),
                    rusqlite::types::Value::Text(s) => Ok(s.as_bytes().to_vec()),
                    rusqlite::types::Value::Null => Ok(Vec::new()),
                    _ => Ok(Vec::new()),
                },
                // 5: Null converter
                |_value| Ok(Vec::new()),
            ],
        }
    }

    pub fn convert(&self, converter_idx: u8, value: &rusqlite::types::Value) -> Result<Vec<u8>, rusqlite::Error> {
        if let Some(converter) = self.converters.get(converter_idx as usize) {
            converter(value)
        } else {
            // Fallback to text conversion
            match value {
                rusqlite::types::Value::Text(s) => Ok(s.as_bytes().to_vec()),
                rusqlite::types::Value::Integer(i) => {
                    let mut buf = itoa::Buffer::new();
                    Ok(buf.format(*i).as_bytes().to_vec())
                },
                rusqlite::types::Value::Real(r) => Ok(r.to_string().as_bytes().to_vec()),
                rusqlite::types::Value::Null => Ok(Vec::new()),
                _ => Ok("".as_bytes().to_vec()),
            }
        }
    }

    pub fn convert_binary(&self, value: &rusqlite::types::Value, type_oid: i32) -> Result<Vec<u8>, rusqlite::Error> {
        (self.binary_converter)(value, type_oid)
    }
}

/// Global type converter table
static GLOBAL_TYPE_CONVERTER_TABLE: std::sync::LazyLock<TypeConverterTable> = 
    std::sync::LazyLock::new(|| TypeConverterTable::new());

/// Get the global type converter table
pub fn global_type_converter_table() -> &'static TypeConverterTable {
    &GLOBAL_TYPE_CONVERTER_TABLE
}