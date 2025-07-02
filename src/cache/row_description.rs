use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use crate::protocol::FieldDescription;
use tracing::{debug, info};

/// Cache key for RowDescription entries
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct RowDescriptionKey {
    /// Normalized query text (lowercase, whitespace normalized)
    pub query: String,
    /// Table name if available
    pub table_name: Option<String>,
    /// Column names in order
    pub columns: Vec<String>,
}

/// Cached RowDescription data
#[derive(Debug, Clone)]
pub struct CachedRowDescription {
    /// Pre-built field descriptions
    pub fields: Vec<FieldDescription>,
    /// When this entry was created
    pub created_at: Instant,
    /// Number of times this cache entry was used
    pub hit_count: u64,
}

/// RowDescription cache with LRU eviction and TTL
pub struct RowDescriptionCache {
    cache: Arc<RwLock<HashMap<RowDescriptionKey, CachedRowDescription>>>,
    capacity: usize,
    ttl: Duration,
    stats: Arc<RwLock<RowDescriptionCacheStats>>,
}

#[derive(Debug, Default)]
pub struct RowDescriptionCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub entries: usize,
}

impl RowDescriptionCache {
    /// Create a new RowDescription cache
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::with_capacity(capacity))),
            capacity,
            ttl,
            stats: Arc::new(RwLock::new(RowDescriptionCacheStats::default())),
        }
    }

    /// Generate a cache key from query information
    pub fn create_key(query: &str, table_name: Option<&str>, columns: &[String]) -> RowDescriptionKey {
        // Normalize query for better cache hit rate
        let normalized_query = Self::normalize_query(query);
        
        RowDescriptionKey {
            query: normalized_query,
            table_name: table_name.map(|s| s.to_string()),
            columns: columns.to_vec(),
        }
    }

    /// Normalize query text for caching
    fn normalize_query(query: &str) -> String {
        // Convert to lowercase and normalize whitespace
        query.to_lowercase()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Get cached RowDescription if available
    pub fn get(&self, key: &RowDescriptionKey) -> Option<Vec<FieldDescription>> {
        let mut cache = self.cache.write().unwrap();
        let mut stats = self.stats.write().unwrap();
        
        if let Some(entry) = cache.get_mut(key) {
            // Check TTL
            if entry.created_at.elapsed() < self.ttl {
                entry.hit_count += 1;
                stats.hits += 1;
                debug!("RowDescription cache hit for query: {}", &key.query[..50.min(key.query.len())]);
                return Some(entry.fields.clone());
            } else {
                // Entry expired
                cache.remove(key);
                stats.evictions += 1;
                stats.entries = cache.len();
            }
        }
        
        stats.misses += 1;
        None
    }

    /// Insert a new RowDescription into the cache
    pub fn insert(&self, key: RowDescriptionKey, fields: Vec<FieldDescription>) {
        let mut cache = self.cache.write().unwrap();
        let mut stats = self.stats.write().unwrap();
        
        // Check capacity and evict if necessary
        if cache.len() >= self.capacity && !cache.contains_key(&key) {
            // Find and remove the least recently used entry
            if let Some((lru_key, _)) = cache.iter()
                .min_by_key(|(_, entry)| (entry.hit_count, entry.created_at)) {
                let lru_key = lru_key.clone();
                cache.remove(&lru_key);
                stats.evictions += 1;
                debug!("Evicted LRU RowDescription cache entry");
            }
        }
        
        let entry = CachedRowDescription {
            fields,
            created_at: Instant::now(),
            hit_count: 0,
        };
        
        cache.insert(key.clone(), entry);
        stats.entries = cache.len();
        debug!("Cached RowDescription for query: {}", &key.query[..50.min(key.query.len())]);
    }

    /// Clear all cache entries
    pub fn clear(&self) {
        let mut cache = self.cache.write().unwrap();
        let mut stats = self.stats.write().unwrap();
        
        cache.clear();
        stats.entries = 0;
        info!("Cleared RowDescription cache");
    }

    /// Get cache statistics
    pub fn stats(&self) -> RowDescriptionCacheStats {
        self.stats.read().unwrap().clone()
    }

    /// Get cache hit rate
    pub fn hit_rate(&self) -> f64 {
        let stats = self.stats.read().unwrap();
        let total = stats.hits + stats.misses;
        if total == 0 {
            0.0
        } else {
            (stats.hits as f64 / total as f64) * 100.0
        }
    }
}

impl Clone for RowDescriptionCacheStats {
    fn clone(&self) -> Self {
        Self {
            hits: self.hits,
            misses: self.misses,
            evictions: self.evictions,
            entries: self.entries,
        }
    }
}

use once_cell::sync::Lazy;

/// Global RowDescription cache instance
pub static GLOBAL_ROW_DESCRIPTION_CACHE: Lazy<RowDescriptionCache> = Lazy::new(|| {
    // Default: 1000 entries, 10 minute TTL
    let capacity = std::env::var("PGSQLITE_ROW_DESC_CACHE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    
    let ttl_minutes = std::env::var("PGSQLITE_ROW_DESC_CACHE_TTL_MINUTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    
    info!("Initializing RowDescription cache with capacity {} and TTL {} minutes", capacity, ttl_minutes);
    RowDescriptionCache::new(capacity, Duration::from_secs(ttl_minutes * 60))
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PgType;

    #[test]
    fn test_row_description_cache_basic() {
        let cache = RowDescriptionCache::new(10, Duration::from_secs(60));
        
        let key = RowDescriptionKey {
            query: "select * from users".to_string(),
            table_name: Some("users".to_string()),
            columns: vec!["id".to_string(), "name".to_string()],
        };
        
        let fields = vec![
            FieldDescription {
                name: "id".to_string(),
                table_oid: 0,
                column_id: 1,
                type_oid: PgType::Int4.to_oid(),
                type_size: 4,
                type_modifier: -1,
                format: 0,
            },
            FieldDescription {
                name: "name".to_string(),
                table_oid: 0,
                column_id: 2,
                type_oid: PgType::Text.to_oid(),
                type_size: -1,
                type_modifier: -1,
                format: 0,
            },
        ];
        
        // Insert and retrieve
        cache.insert(key.clone(), fields.clone());
        let cached = cache.get(&key).unwrap();
        assert_eq!(cached.len(), 2);
        assert_eq!(cached[0].name, "id");
        assert_eq!(cached[1].name, "name");
        
        // Check stats
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.entries, 1);
    }
    
    #[test]
    fn test_query_normalization() {
        let key1 = RowDescriptionCache::create_key(
            "SELECT   *  FROM   users   WHERE id = 1",
            Some("users"),
            &["id".to_string(), "name".to_string()]
        );
        
        let key2 = RowDescriptionCache::create_key(
            "select * from users where id = 1",
            Some("users"),
            &["id".to_string(), "name".to_string()]
        );
        
        assert_eq!(key1, key2);
    }
}