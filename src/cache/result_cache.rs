use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use crate::config::CONFIG;

/// Cached result set for a query
#[derive(Clone, Debug)]
pub struct CachedResultSet {
    /// Column names
    pub columns: Vec<String>,
    /// Cached rows - stored as raw bytes to avoid re-encoding
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    /// Number of rows affected (for DML)
    pub rows_affected: u64,
    /// When this result was cached
    pub cached_at: Instant,
    /// Number of times this result has been served
    pub hit_count: u64,
    /// Query execution time in microseconds
    pub execution_time_us: u64,
}

/// Key for result cache - includes query and parameters
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResultCacheKey {
    /// Normalized query text
    pub query: String,
    /// Parameter values (as strings for simplicity)
    pub params: Vec<String>,
}

impl ResultCacheKey {
    pub fn new(query: &str, params: &[Option<Vec<u8>>]) -> Self {
        // Normalize query for better cache hits
        let normalized_query = query.trim().to_lowercase();
        
        // Convert parameters to strings for hashing
        let param_strings: Vec<String> = params.iter()
            .map(|p| match p {
                None => "NULL".to_string(),
                Some(bytes) => {
                    // Try to convert to string, otherwise use hex representation
                    String::from_utf8(bytes.clone())
                        .unwrap_or_else(|_| hex::encode(bytes))
                }
            })
            .collect();
        
        Self {
            query: normalized_query,
            params: param_strings,
        }
    }
    
    /// Generate a hash key for quick lookups
    pub fn hash_key(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

/// Result set cache for caching complete query results
pub struct ResultSetCache {
    /// Cache storage - using hash key for faster lookups
    cache: Arc<RwLock<HashMap<u64, CacheEntry>>>,
    /// Maximum number of cached results
    max_entries: usize,
    /// Maximum size of result set to cache (in rows)
    max_result_rows: usize,
    /// Time-to-live for cached results
    ttl: Duration,
    /// Cache statistics
    stats: Arc<RwLock<CacheStats>>,
}

struct CacheEntry {
    key: ResultCacheKey,
    result: CachedResultSet,
}

#[derive(Default, Debug, Clone)]
pub struct CacheStats {
    pub total_queries: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub evictions: u64,
    pub total_bytes_saved: u64,
}

impl ResultSetCache {
    pub fn new(max_entries: usize, max_result_rows: usize, ttl_seconds: u64) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::with_capacity(max_entries))),
            max_entries,
            max_result_rows,
            ttl: Duration::from_secs(ttl_seconds),
            stats: Arc::new(RwLock::new(CacheStats::default())),
        }
    }
    
    /// Try to get a cached result
    pub fn get(&self, key: &ResultCacheKey) -> Option<CachedResultSet> {
        let hash_key = key.hash_key();
        let mut cache = self.cache.write().unwrap();
        let mut stats = self.stats.write().unwrap();
        
        stats.total_queries += 1;
        
        if let Some(entry) = cache.get_mut(&hash_key) {
            // Verify the key matches (hash collision check)
            if &entry.key != key {
                stats.cache_misses += 1;
                return None;
            }
            
            // Check if entry is still valid
            if entry.result.cached_at.elapsed() > self.ttl {
                cache.remove(&hash_key);
                stats.cache_misses += 1;
                stats.evictions += 1;
                return None;
            }
            
            // Update hit count
            entry.result.hit_count += 1;
            stats.cache_hits += 1;
            
            // Estimate bytes saved (rough calculation)
            let bytes_saved = estimate_result_size(&entry.result);
            stats.total_bytes_saved += bytes_saved;
            
            Some(entry.result.clone())
        } else {
            stats.cache_misses += 1;
            None
        }
    }
    
    /// Cache a result set
    pub fn insert(
        &self, 
        key: ResultCacheKey,
        columns: Vec<String>,
        rows: Vec<Vec<Option<Vec<u8>>>>,
        rows_affected: u64,
        execution_time_us: u64,
    ) -> bool {
        // Don't cache if result is too large
        if rows.len() > self.max_result_rows {
            return false;
        }
        
        let hash_key = key.hash_key();
        let mut cache = self.cache.write().unwrap();
        
        // Evict entries if cache is full
        if cache.len() >= self.max_entries {
            // Simple eviction: remove oldest entry
            if let Some((&oldest_key, _)) = cache.iter()
                .min_by_key(|(_, entry)| entry.result.cached_at) {
                cache.remove(&oldest_key);
                self.stats.write().unwrap().evictions += 1;
            }
        }
        
        let result = CachedResultSet {
            columns,
            rows,
            rows_affected,
            cached_at: Instant::now(),
            hit_count: 0,
            execution_time_us,
        };
        
        cache.insert(hash_key, CacheEntry { key, result });
        true
    }
    
    /// Clear the cache
    pub fn clear(&self) {
        self.cache.write().unwrap().clear();
    }
    
    /// Get cache statistics
    pub fn get_stats(&self) -> CacheStats {
        self.stats.read().unwrap().clone()
    }
    
    /// Get cache hit rate
    pub fn hit_rate(&self) -> f64 {
        let stats = self.stats.read().unwrap();
        if stats.total_queries == 0 {
            0.0
        } else {
            (stats.cache_hits as f64) / (stats.total_queries as f64) * 100.0
        }
    }
    
    /// Check if a query result should be cached based on heuristics
    pub fn should_cache(query: &str, execution_time_us: u64, row_count: usize) -> bool {
        // Don't cache DDL statements
        let query_upper = query.trim().to_uppercase();
        if query_upper.starts_with("CREATE") || 
           query_upper.starts_with("DROP") || 
           query_upper.starts_with("ALTER") ||
           query_upper.starts_with("INSERT") ||
           query_upper.starts_with("UPDATE") ||
           query_upper.starts_with("DELETE") {
            return false;
        }
        
        // Cache if query takes more than 1ms or returns many rows
        execution_time_us > 1000 || row_count > 10
    }
}

/// Estimate the size of a cached result in bytes
fn estimate_result_size(result: &CachedResultSet) -> u64 {
    let mut size = 0u64;
    
    // Column names
    for col in &result.columns {
        size += col.len() as u64;
    }
    
    // Row data
    for row in &result.rows {
        for cell in row {
            if let Some(data) = cell {
                size += data.len() as u64;
            }
        }
    }
    
    size
}

/// Global result cache instance
static GLOBAL_RESULT_CACHE: std::sync::LazyLock<ResultSetCache> = 
    std::sync::LazyLock::new(|| ResultSetCache::new(CONFIG.result_cache_size, 10000, CONFIG.result_cache_ttl));

/// Get the global result cache
pub fn global_result_cache() -> &'static ResultSetCache {
    &GLOBAL_RESULT_CACHE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_result_cache_key() {
        let params1 = vec![Some(b"test".to_vec()), None];
        let params2 = vec![Some(b"test".to_vec()), None];
        let params3 = vec![Some(b"other".to_vec()), None];
        
        let key1 = ResultCacheKey::new("SELECT * FROM users WHERE id = $1", &params1);
        let key2 = ResultCacheKey::new("SELECT * FROM users WHERE id = $1", &params2);
        let key3 = ResultCacheKey::new("SELECT * FROM users WHERE id = $1", &params3);
        
        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
        assert_eq!(key1.hash_key(), key2.hash_key());
        assert_ne!(key1.hash_key(), key3.hash_key());
    }

    #[test]
    fn test_result_cache_basic() {
        let cache = ResultSetCache::new(10, 100, 60);
        
        let key = ResultCacheKey::new("SELECT * FROM test", &[]);
        let columns = vec!["id".to_string(), "name".to_string()];
        let rows = vec![
            vec![Some(b"1".to_vec()), Some(b"Alice".to_vec())],
            vec![Some(b"2".to_vec()), Some(b"Bob".to_vec())],
        ];
        
        // Insert into cache
        assert!(cache.insert(key.clone(), columns.clone(), rows.clone(), 0, 1000));
        
        // Get from cache
        let cached = cache.get(&key).unwrap();
        assert_eq!(cached.columns, columns);
        assert_eq!(cached.rows, rows);
        assert_eq!(cached.hit_count, 1);
        
        // Check stats
        let stats = cache.get_stats();
        assert_eq!(stats.total_queries, 1);
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.cache_misses, 0);
    }

    #[test]
    fn test_should_cache_heuristics() {
        // Should cache SELECT queries
        assert!(ResultSetCache::should_cache("SELECT * FROM users", 2000, 5));
        assert!(ResultSetCache::should_cache("SELECT * FROM users", 500, 20));
        
        // Should not cache DDL or DML
        assert!(!ResultSetCache::should_cache("CREATE TABLE test (id INT)", 100, 0));
        assert!(!ResultSetCache::should_cache("INSERT INTO test VALUES (1)", 100, 1));
        assert!(!ResultSetCache::should_cache("UPDATE test SET x = 1", 100, 5));
        assert!(!ResultSetCache::should_cache("DELETE FROM test", 100, 10));
        
        // Should not cache fast queries with few results
        assert!(!ResultSetCache::should_cache("SELECT 1", 100, 1));
    }
}