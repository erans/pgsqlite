use std::time::{Duration, Instant};
use lru::LruCache;
use parking_lot::RwLock;
use crate::session::state::PreparedStatement;
use tracing::{debug, info};

/// A globally shared cache for prepared statements to avoid re-parsing identical queries
pub struct PreparedStatementCache {
    cache: RwLock<LruCache<String, CachedStatement>>,
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
    evictions: std::sync::atomic::AtomicU64,
    ttl: Duration,
}

#[derive(Clone)]
struct CachedStatement {
    statement: PreparedStatement,
    created_at: Instant,
    last_accessed: Instant,
    access_count: u64,
}

impl PreparedStatementCache {
    pub fn new(capacity: usize, ttl_seconds: u64) -> Self {
        Self {
            cache: RwLock::new(LruCache::new(capacity.try_into().unwrap())),
            hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
            evictions: std::sync::atomic::AtomicU64::new(0),
            ttl: Duration::from_secs(ttl_seconds),
        }
    }

    /// Generate a cache key from query and parameter types
    fn generate_key(query: &str, param_types: &[i32]) -> String {
        if param_types.is_empty() {
            query.to_string()
        } else {
            format!("{}::{:?}", query, param_types)
        }
    }

    /// Get a prepared statement from the cache
    pub fn get(&self, query: &str, param_types: &[i32]) -> Option<PreparedStatement> {
        let key = Self::generate_key(query, param_types);
        let mut cache = self.cache.write();
        
        if let Some(cached) = cache.get_mut(&key) {
            // Check if TTL has expired
            if cached.created_at.elapsed() > self.ttl {
                cache.pop(&key);
                self.evictions.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                debug!("Prepared statement cache TTL expired for query: {}", query);
                return None;
            }

            // Update access time and count
            cached.last_accessed = Instant::now();
            cached.access_count += 1;
            
            self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            debug!("Prepared statement cache hit for query: {} (access count: {})", 
                   query, cached.access_count);
            
            Some(cached.statement.clone())
        } else {
            self.misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            None
        }
    }

    /// Insert a prepared statement into the cache
    pub fn insert(&self, query: &str, param_types: &[i32], statement: PreparedStatement) {
        let key = Self::generate_key(query, param_types);
        let mut cache = self.cache.write();
        
        let cached = CachedStatement {
            statement,
            created_at: Instant::now(),
            last_accessed: Instant::now(),
            access_count: 1,
        };
        
        if cache.put(key.clone(), cached).is_some() {
            self.evictions.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        
        debug!("Cached prepared statement for query: {}", query);
    }

    /// Invalidate cache entries for queries that might be affected by DDL operations
    pub fn invalidate_for_table(&self, table_name: &str) {
        let mut cache = self.cache.write();
        let table_lower = table_name.to_lowercase();
        
        // Collect keys to remove
        let keys_to_remove: Vec<String> = cache.iter()
            .filter(|(key, _)| {
                // Simple heuristic: if the key contains the table name, invalidate it
                key.to_lowercase().contains(&table_lower)
            })
            .map(|(key, _)| key.clone())
            .collect();
        
        let count = keys_to_remove.len();
        for key in keys_to_remove {
            cache.pop(&key);
            self.evictions.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        
        if count > 0 {
            info!("Invalidated {} prepared statements for table: {}", 
                  count, table_name);
        }
    }

    /// Clear the entire cache
    pub fn clear(&self) {
        let mut cache = self.cache.write();
        let count = cache.len();
        cache.clear();
        self.evictions.fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
        info!("Cleared prepared statement cache ({} entries)", count);
    }

    /// Get cache statistics
    pub fn stats(&self) -> PreparedStatementCacheStats {
        let cache = self.cache.read();
        PreparedStatementCacheStats {
            size: cache.len(),
            hits: self.hits.load(std::sync::atomic::Ordering::Relaxed),
            misses: self.misses.load(std::sync::atomic::Ordering::Relaxed),
            evictions: self.evictions.load(std::sync::atomic::Ordering::Relaxed),
            hit_rate: {
                let hits = self.hits.load(std::sync::atomic::Ordering::Relaxed);
                let total = hits + self.misses.load(std::sync::atomic::Ordering::Relaxed);
                if total > 0 {
                    (hits as f64 / total as f64) * 100.0
                } else {
                    0.0
                }
            },
        }
    }
}

pub struct PreparedStatementCacheStats {
    pub size: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub hit_rate: f64,
}

// Make PreparedStatement cloneable for caching
impl Clone for PreparedStatement {
    fn clone(&self) -> Self {
        PreparedStatement {
            query: self.query.clone(),
            translated_query: self.translated_query.clone(),
            param_types: self.param_types.clone(),
            param_formats: self.param_formats.clone(),
            field_descriptions: self.field_descriptions.clone(),
            translation_metadata: self.translation_metadata.clone(),
        }
    }
}