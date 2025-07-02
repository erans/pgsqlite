use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use once_cell::sync::Lazy;

/// Cache for parameter type information to avoid repeated analysis
pub struct ParameterTypeCache {
    cache: RwLock<super::LruCache<String, CachedParameterInfo>>,
}

#[derive(Clone, Debug)]
pub struct CachedParameterInfo {
    pub param_types: Vec<i32>,
    pub original_types: Vec<i32>, // Original PostgreSQL types before mapping to TEXT
    pub table_name: Option<String>,
    pub column_names: Vec<String>,
    pub created_at: Instant,
}

impl CachedParameterInfo {
    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.created_at.elapsed() > ttl
    }
}

/// Global parameter type cache instance
pub static GLOBAL_PARAMETER_CACHE: Lazy<ParameterTypeCache> = Lazy::new(|| {
    let cache_size = std::env::var("PGSQLITE_PARAM_CACHE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500);
    
    let ttl_minutes = std::env::var("PGSQLITE_PARAM_CACHE_TTL_MINUTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    
    ParameterTypeCache::new(cache_size, Duration::from_secs(ttl_minutes * 60))
});

impl ParameterTypeCache {
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            cache: RwLock::new(super::LruCache::new(capacity, ttl)),
        }
    }
    
    /// Get cached parameter info for a query
    pub fn get(&self, query: &str) -> Option<CachedParameterInfo> {
        let cache = self.cache.read().ok()?;
        cache.get(&query.to_string())
    }
    
    /// Cache parameter info for a query
    pub fn insert(&self, query: String, info: CachedParameterInfo) {
        if let Ok(cache) = self.cache.write() {
            cache.insert(query, info);
        }
    }
    
    /// Clear the cache
    pub fn clear(&self) {
        if let Ok(cache) = self.cache.write() {
            cache.clear();
        }
    }
    
    /// Get cache statistics
    pub fn stats(&self) -> ParameterCacheStats {
        ParameterCacheStats {
            entries: 0, // TODO: Add len() method to LruCache
            capacity: 0,
        }
    }
}

#[derive(Debug)]
pub struct ParameterCacheStats {
    pub entries: usize,
    pub capacity: usize,
}

/// Cache for parameter value conversions to avoid repeated parsing
pub struct ParameterValueCache {
    cache: RwLock<HashMap<ParameterValueKey, rusqlite::types::Value>>,
    max_size: usize,
}

#[derive(Hash, Eq, PartialEq)]
struct ParameterValueKey {
    bytes: Vec<u8>,
    param_type: i32,
    format: i16,
}

impl ParameterValueCache {
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            max_size,
        }
    }
    
    pub fn get_or_convert<F>(
        &self, 
        bytes: &[u8], 
        param_type: i32, 
        format: i16,
        convert_fn: F
    ) -> Result<rusqlite::types::Value, crate::PgSqliteError>
    where
        F: FnOnce() -> Result<rusqlite::types::Value, crate::PgSqliteError>
    {
        let key = ParameterValueKey {
            bytes: bytes.to_vec(),
            param_type,
            format,
        };
        
        // Try to get from cache
        if let Ok(cache) = self.cache.read() {
            if let Some(value) = cache.get(&key) {
                return Ok(value.clone());
            }
        }
        
        // Convert and cache
        let value = convert_fn()?;
        
        if let Ok(mut cache) = self.cache.write() {
            // Simple size limit - could be improved with LRU
            if cache.len() < self.max_size {
                cache.insert(key, value.clone());
            }
        }
        
        Ok(value)
    }
}

/// Global parameter value cache
pub static GLOBAL_PARAM_VALUE_CACHE: Lazy<ParameterValueCache> = Lazy::new(|| {
    ParameterValueCache::new(1000)
});