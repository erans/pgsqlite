use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use sqlparser::ast::Statement;

/// Represents a cached parsed query
#[derive(Clone)]
pub struct CachedQuery {
    pub statement: Statement,
    pub param_types: Vec<i32>,
    pub is_decimal_query: bool,
    pub table_names: Vec<String>,
}

/// Cache for parsed queries to avoid re-parsing
pub struct QueryCache {
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
    capacity: usize,
    ttl: Duration,
}

struct CacheEntry {
    query: CachedQuery,
    cached_at: Instant,
    access_count: u64,
}

impl QueryCache {
    pub fn new(capacity: usize, ttl_seconds: u64) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::with_capacity(capacity))),
            capacity,
            ttl: Duration::from_secs(ttl_seconds),
        }
    }

    /// Get a cached query
    pub fn get(&self, query_text: &str) -> Option<CachedQuery> {
        let mut cache = self.cache.write().unwrap();
        
        if let Some(entry) = cache.get_mut(query_text) {
            if entry.cached_at.elapsed() < self.ttl {
                entry.access_count += 1;
                return Some(entry.query.clone());
            } else {
                cache.remove(query_text);
            }
        }
        
        None
    }

    /// Cache a parsed query
    pub fn insert(&self, query_text: String, query: CachedQuery) {
        let mut cache = self.cache.write().unwrap();
        
        // Simple eviction: remove least accessed entry if at capacity
        if cache.len() >= self.capacity && !cache.contains_key(&query_text) {
            if let Some((key_to_remove, _)) = cache.iter()
                .min_by_key(|(_, entry)| entry.access_count) {
                let key_to_remove = key_to_remove.clone();
                cache.remove(&key_to_remove);
            }
        }
        
        cache.insert(query_text, CacheEntry {
            query,
            cached_at: Instant::now(),
            access_count: 1,
        });
    }

    /// Invalidate cache entries for a specific table
    pub fn invalidate_table(&self, table_name: &str) {
        let mut cache = self.cache.write().unwrap();
        let table_lower = table_name.to_lowercase();
        
        cache.retain(|_, entry| {
            !entry.query.table_names.iter()
                .any(|t| t.to_lowercase() == table_lower)
        });
    }

    /// Clear entire cache
    pub fn clear(&self) {
        self.cache.write().unwrap().clear();
    }

    /// Get cache statistics
    pub fn stats(&self) -> (usize, u64) {
        let cache = self.cache.read().unwrap();
        let total_accesses: u64 = cache.values()
            .map(|entry| entry.access_count)
            .sum();
        (cache.len(), total_accesses)
    }
}