use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use sqlparser::ast::Statement;
use crate::types::type_mapper::PgType;
use super::QueryFingerprint;

/// Represents a cached parsed query with full analysis results
#[derive(Clone)]
pub struct CachedQuery {
    pub statement: Statement,
    pub param_types: Vec<i32>,
    pub is_decimal_query: bool,
    pub table_names: Vec<String>,
    pub column_types: Vec<(String, PgType)>,
    pub has_decimal_columns: bool,
    pub rewritten_query: Option<String>,
    pub normalized_query: String,
}

/// Cache for parsed queries to avoid re-parsing
pub struct QueryCache {
    cache: Arc<RwLock<HashMap<u64, CacheEntry>>>,
    capacity: usize,
    ttl: Duration,
    metrics: Arc<RwLock<CacheMetrics>>,
}

struct CacheEntry {
    query: CachedQuery,
    last_accessed: Instant,
    access_count: u64,
    hit_count: u64,
}

/// Cache metrics for monitoring performance
#[derive(Default, Clone)]
pub struct CacheMetrics {
    pub total_queries: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub evictions: u64,
    pub invalidations: u64,
}

impl QueryCache {
    pub fn new(capacity: usize, ttl_seconds: u64) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::with_capacity(capacity))),
            capacity,
            ttl: Duration::from_secs(ttl_seconds),
            metrics: Arc::new(RwLock::new(CacheMetrics::default())),
        }
    }

    /// Get a cached query
    pub fn get(&self, query_text: &str) -> Option<CachedQuery> {
        let fingerprint = QueryFingerprint::generate(query_text);
        let mut cache = self.cache.write().unwrap();
        let mut metrics = self.metrics.write().unwrap();
        
        metrics.total_queries += 1;
        
        if let Some(entry) = cache.get_mut(&fingerprint) {
            if entry.last_accessed.elapsed() < self.ttl {
                entry.access_count += 1;
                entry.hit_count += 1;
                entry.last_accessed = Instant::now();
                metrics.cache_hits += 1;
                return Some(entry.query.clone());
            } else {
                cache.remove(&fingerprint);
                metrics.evictions += 1;
            }
        }
        
        metrics.cache_misses += 1;
        None
    }

    /// Cache a parsed query
    pub fn insert(&self, query_text: String, query: CachedQuery) {
        let fingerprint = QueryFingerprint::generate(&query_text);
        let mut cache = self.cache.write().unwrap();
        let mut metrics = self.metrics.write().unwrap();
        
        // LRU eviction: remove least recently used entry if at capacity
        if cache.len() >= self.capacity && !cache.contains_key(&fingerprint) {
            if let Some((key_to_remove, _)) = cache.iter()
                .min_by_key(|(_, entry)| entry.last_accessed) {
                let key_to_remove = *key_to_remove;
                cache.remove(&key_to_remove);
                metrics.evictions += 1;
            }
        }
        
        cache.insert(fingerprint, CacheEntry {
            query,
            last_accessed: Instant::now(),
            access_count: 1,
            hit_count: 0,
        });
    }

    /// Invalidate cache entries for a specific table
    pub fn invalidate_table(&self, table_name: &str) {
        let mut cache = self.cache.write().unwrap();
        let mut metrics = self.metrics.write().unwrap();
        let table_lower = table_name.to_lowercase();
        
        let before_size = cache.len();
        cache.retain(|_, entry| {
            !entry.query.table_names.iter()
                .any(|t| t.to_lowercase() == table_lower)
        });
        let removed = before_size - cache.len();
        metrics.invalidations += removed as u64;
    }

    /// Clear entire cache
    pub fn clear(&self) {
        let mut cache = self.cache.write().unwrap();
        let mut metrics = self.metrics.write().unwrap();
        let removed = cache.len();
        cache.clear();
        metrics.invalidations += removed as u64;
    }

    /// Get cache statistics
    pub fn stats(&self) -> (usize, u64) {
        let cache = self.cache.read().unwrap();
        let total_accesses: u64 = cache.values()
            .map(|entry| entry.access_count)
            .sum();
        (cache.len(), total_accesses)
    }

    /// Get cache metrics
    pub fn get_metrics(&self) -> CacheMetrics {
        self.metrics.read().unwrap().clone()
    }

    /// Normalize query for cache key
    /// Removes extra whitespace, lowercases keywords, but preserves string literals
    pub fn normalize_query(query: &str) -> String {
        let mut normalized = String::with_capacity(query.len());
        let mut in_string = false;
        let mut string_delimiter = '\0';
        let chars = query.chars().peekable();
        
        for ch in chars {
            match ch {
                '\'' | '"' if !in_string => {
                    in_string = true;
                    string_delimiter = ch;
                    normalized.push(ch);
                }
                ch if ch == string_delimiter && in_string => {
                    in_string = false;
                    string_delimiter = '\0';
                    normalized.push(ch);
                }
                ' ' | '\t' | '\n' | '\r' if !in_string => {
                    // Collapse whitespace
                    if !normalized.ends_with(' ') && !normalized.is_empty() {
                        normalized.push(' ');
                    }
                }
                ch if !in_string => {
                    // Lowercase keywords outside strings
                    normalized.push(ch.to_ascii_lowercase());
                }
                ch => normalized.push(ch),
            }
        }
        
        normalized.trim().to_string()
    }
}