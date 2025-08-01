use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

pub mod schema;
pub mod query;
pub mod status;
pub mod statement_pool;
pub mod enhanced_statement_pool;
pub mod execution;
pub mod result_cache;
pub mod row_description;
pub mod parameter_cache;
pub mod enum_cache;
pub mod translation_cache;
pub mod query_fingerprint;
pub mod lazy_schema_loader;
pub mod wire_protocol_cache;
pub mod prepared_statement_cache;

pub use schema::SchemaCache;
pub use query::{QueryCache, CachedQuery, CacheMetrics};
pub use status::{CacheStatus, get_cache_status, format_cache_status_as_table, log_cache_status};
pub use statement_pool::{StatementPool, StatementMetadata, StatementPoolStats};
pub use enhanced_statement_pool::{EnhancedStatementPool, StatementMetadata as EnhancedStatementMetadata, PoolStats};
pub use execution::{ExecutionCache, ExecutionMetadata, global_execution_cache, global_type_converter_table};
pub use result_cache::{ResultSetCache, ResultCacheKey, CachedResultSet, global_result_cache};
pub use row_description::{RowDescriptionCache, RowDescriptionKey, CachedRowDescription, GLOBAL_ROW_DESCRIPTION_CACHE};
pub use parameter_cache::{ParameterTypeCache, CachedParameterInfo, GLOBAL_PARAMETER_CACHE, GLOBAL_PARAM_VALUE_CACHE};
pub use enum_cache::{EnumCache, global_enum_cache};
pub use translation_cache::{TranslationCache, global_translation_cache};
pub use query_fingerprint::QueryFingerprint;
pub use lazy_schema_loader::LazySchemaLoader;
pub use wire_protocol_cache::{WireProtocolCache, CachedWireResponse, WIRE_PROTOCOL_CACHE, is_cacheable_for_wire_protocol, encode_data_row};
pub use prepared_statement_cache::{PreparedStatementCache, PreparedStatementCacheStats};

/// Simple LRU cache with TTL support
pub struct LruCache<K, V> {
    cache: Arc<RwLock<HashMap<K, CacheEntry<V>>>>,
    capacity: usize,
    ttl: Duration,
}

struct CacheEntry<V> {
    value: V,
    last_accessed: Instant,
}

impl<K: Eq + std::hash::Hash + Clone, V: Clone> LruCache<K, V> {
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::with_capacity(capacity))),
            capacity,
            ttl,
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let mut cache = self.cache.write().unwrap();
        
        if let Some(entry) = cache.get_mut(key) {
            if entry.last_accessed.elapsed() < self.ttl {
                entry.last_accessed = Instant::now();
                return Some(entry.value.clone());
            } else {
                cache.remove(key);
            }
        }
        
        None
    }

    pub fn insert(&self, key: K, value: V) {
        let mut cache = self.cache.write().unwrap();
        
        // Simple eviction: remove oldest entry if at capacity
        if cache.len() >= self.capacity && !cache.contains_key(&key) {
            if let Some((oldest_key, _)) = cache.iter()
                .min_by_key(|(_, entry)| entry.last_accessed) {
                let oldest_key = oldest_key.clone();
                cache.remove(&oldest_key);
            }
        }
        
        cache.insert(key, CacheEntry {
            value,
            last_accessed: Instant::now(),
        });
    }

    pub fn invalidate(&self, key: &K) {
        self.cache.write().unwrap().remove(key);
    }

    pub fn clear(&self) {
        self.cache.write().unwrap().clear();
    }
}