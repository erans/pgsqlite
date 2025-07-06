use std::sync::RwLock;
use once_cell::sync::Lazy;
use std::time::Duration;

/// Global translation cache for cast translations
static GLOBAL_TRANSLATION_CACHE: Lazy<TranslationCache> = Lazy::new(|| {
    let cache_size = std::env::var("PGSQLITE_TRANSLATION_CACHE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    
    let ttl = std::env::var("PGSQLITE_TRANSLATION_CACHE_TTL_MINUTES")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(60);
    
    TranslationCache::new(cache_size, Duration::from_secs(ttl * 60))
});

/// Get the global translation cache instance
pub fn global_translation_cache() -> &'static TranslationCache {
    &GLOBAL_TRANSLATION_CACHE
}

/// Translation cache for cast syntax translations
pub struct TranslationCache {
    cache: RwLock<super::LruCache<String, String>>,
}

impl TranslationCache {
    /// Create a new translation cache
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            cache: RwLock::new(super::LruCache::new(capacity, ttl)),
        }
    }
    
    /// Get a translated query from cache
    pub fn get(&self, query: &str) -> Option<String> {
        let cache = self.cache.read().unwrap();
        cache.get(&query.to_string())
    }
    
    /// Insert a translation into the cache
    pub fn insert(&self, original: String, translated: String) {
        let cache = self.cache.read().unwrap();
        cache.insert(original, translated);
    }
    
    /// Clear the cache
    pub fn clear(&self) {
        let cache = self.cache.read().unwrap();
        cache.clear();
    }
    
    /// Get cache statistics
    pub fn stats(&self) -> TranslationCacheStats {
        let cache = self.cache.read().unwrap();
        let size = cache.cache.read().unwrap().len();
        TranslationCacheStats {
            size,
            capacity: cache.capacity,
        }
    }
}

/// Statistics for the translation cache
#[derive(Debug, Clone)]
pub struct TranslationCacheStats {
    pub size: usize,
    pub capacity: usize,
}