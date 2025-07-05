use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use rusqlite::Connection;
use crate::metadata::{EnumType, EnumValue, EnumMetadata};

/// Cache entry for ENUM metadata
#[derive(Clone)]
struct CacheEntry<T> {
    data: T,
    timestamp: Instant,
}

/// Thread-safe cache for ENUM metadata
pub struct EnumCache {
    /// Map from type name to EnumType
    types_by_name: Arc<RwLock<HashMap<String, CacheEntry<EnumType>>>>,
    /// Map from type OID to EnumType
    types_by_oid: Arc<RwLock<HashMap<i32, CacheEntry<EnumType>>>>,
    /// Map from type OID to its values
    values_by_type: Arc<RwLock<HashMap<i32, CacheEntry<Vec<EnumValue>>>>>,
    /// Cache TTL
    ttl: Duration,
}

impl EnumCache {
    /// Create a new ENUM cache with specified TTL
    pub fn new(ttl_seconds: u64) -> Self {
        Self {
            types_by_name: Arc::new(RwLock::new(HashMap::new())),
            types_by_oid: Arc::new(RwLock::new(HashMap::new())),
            values_by_type: Arc::new(RwLock::new(HashMap::new())),
            ttl: Duration::from_secs(ttl_seconds),
        }
    }
    
    /// Clear all cached data
    pub fn clear(&self) {
        self.types_by_name.write().unwrap().clear();
        self.types_by_oid.write().unwrap().clear();
        self.values_by_type.write().unwrap().clear();
    }
    
    /// Get ENUM type by name (with caching)
    pub fn get_enum_type(&self, conn: &Connection, type_name: &str) -> rusqlite::Result<Option<EnumType>> {
        // Check cache first
        {
            let cache = self.types_by_name.read().unwrap();
            if let Some(entry) = cache.get(type_name) {
                if entry.timestamp.elapsed() < self.ttl {
                    return Ok(Some(entry.data.clone()));
                }
            }
        }
        
        // Load from database
        let enum_type = EnumMetadata::get_enum_type(conn, type_name)?;
        
        // Cache result if found
        if let Some(ref et) = enum_type {
            let mut name_cache = self.types_by_name.write().unwrap();
            let mut oid_cache = self.types_by_oid.write().unwrap();
            
            let entry = CacheEntry {
                data: et.clone(),
                timestamp: Instant::now(),
            };
            
            name_cache.insert(type_name.to_string(), entry.clone());
            oid_cache.insert(et.type_oid, entry);
        }
        
        Ok(enum_type)
    }
    
    /// Get ENUM type by OID (with caching)
    pub fn get_enum_type_by_oid(&self, conn: &Connection, type_oid: i32) -> rusqlite::Result<Option<EnumType>> {
        // Check cache first
        {
            let cache = self.types_by_oid.read().unwrap();
            if let Some(entry) = cache.get(&type_oid) {
                if entry.timestamp.elapsed() < self.ttl {
                    return Ok(Some(entry.data.clone()));
                }
            }
        }
        
        // Load from database
        let enum_type = EnumMetadata::get_enum_type_by_oid(conn, type_oid)?;
        
        // Cache result if found
        if let Some(ref et) = enum_type {
            let mut name_cache = self.types_by_name.write().unwrap();
            let mut oid_cache = self.types_by_oid.write().unwrap();
            
            let entry = CacheEntry {
                data: et.clone(),
                timestamp: Instant::now(),
            };
            
            name_cache.insert(et.type_name.clone(), entry.clone());
            oid_cache.insert(type_oid, entry);
        }
        
        Ok(enum_type)
    }
    
    /// Get ENUM values for a type (with caching)
    pub fn get_enum_values(&self, conn: &Connection, type_oid: i32) -> rusqlite::Result<Vec<EnumValue>> {
        // Check cache first
        {
            let cache = self.values_by_type.read().unwrap();
            if let Some(entry) = cache.get(&type_oid) {
                if entry.timestamp.elapsed() < self.ttl {
                    return Ok(entry.data.clone());
                }
            }
        }
        
        // Load from database
        let values = EnumMetadata::get_enum_values(conn, type_oid)?;
        
        // Cache result
        {
            let mut cache = self.values_by_type.write().unwrap();
            cache.insert(type_oid, CacheEntry {
                data: values.clone(),
                timestamp: Instant::now(),
            });
        }
        
        Ok(values)
    }
    
    /// Validate if a value is valid for an ENUM type (uses cache)
    pub fn is_valid_enum_value(&self, conn: &Connection, type_oid: i32, label: &str) -> rusqlite::Result<bool> {
        let values = self.get_enum_values(conn, type_oid)?;
        Ok(values.iter().any(|v| v.label == label))
    }
    
    /// Invalidate cache entries for a specific type
    pub fn invalidate_type(&self, type_oid: i32) {
        self.types_by_oid.write().unwrap().remove(&type_oid);
        self.values_by_type.write().unwrap().remove(&type_oid);
        
        // Also remove from name cache if we can find it
        let mut name_cache = self.types_by_name.write().unwrap();
        name_cache.retain(|_, entry| entry.data.type_oid != type_oid);
    }
}

lazy_static::lazy_static! {
    /// Global ENUM cache instance
    static ref GLOBAL_ENUM_CACHE: EnumCache = EnumCache::new(600); // 10 minute TTL
}

/// Get the global ENUM cache
pub fn global_enum_cache() -> &'static EnumCache {
    &GLOBAL_ENUM_CACHE
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_enum_cache() {
        let cache = EnumCache::new(60);
        let conn = Connection::open_in_memory().unwrap();
        
        // Initialize metadata tables
        EnumMetadata::init(&conn).unwrap();
        
        // Cache should be empty initially
        assert!(cache.get_enum_type(&conn, "test_enum").unwrap().is_none());
    }
}