use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use tracing::{debug, info};
use crate::protocol::{MemoryPressure, global_memory_monitor};

/// Configuration for TTL-based cache
#[derive(Debug, Clone)]
pub struct TtlCacheConfig {
    /// Default TTL for cache entries
    pub default_ttl: Duration,
    /// Maximum number of entries before forced eviction
    pub max_entries: usize,
    /// Cleanup interval for expired entries
    pub cleanup_interval: Duration,
    /// Enable memory pressure-based eviction
    pub memory_pressure_enabled: bool,
    /// Memory pressure threshold for aggressive eviction (in MB)
    pub memory_pressure_threshold: usize,
    /// Percentage of entries to evict under memory pressure
    pub pressure_eviction_percentage: f32,
    /// Enable access-based LRU eviction
    pub enable_lru: bool,
}

impl Default for TtlCacheConfig {
    fn default() -> Self {
        Self {
            default_ttl: Duration::from_secs(300), // 5 minutes
            max_entries: 10000,
            cleanup_interval: Duration::from_secs(60), // 1 minute
            memory_pressure_enabled: true,
            memory_pressure_threshold: 64, // 64MB
            pressure_eviction_percentage: 0.25, // 25%
            enable_lru: true,
        }
    }
}

impl TtlCacheConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(val) = std::env::var("PGSQLITE_CACHE_DEFAULT_TTL_SECS")
            && let Ok(ttl_secs) = val.parse::<u64>() {
                config.default_ttl = Duration::from_secs(ttl_secs);
            }

        if let Ok(val) = std::env::var("PGSQLITE_CACHE_MAX_ENTRIES")
            && let Ok(max_entries) = val.parse::<usize>() {
                config.max_entries = max_entries;
            }

        if let Ok(val) = std::env::var("PGSQLITE_CACHE_CLEANUP_INTERVAL_SECS")
            && let Ok(interval_secs) = val.parse::<u64>() {
                config.cleanup_interval = Duration::from_secs(interval_secs);
            }

        if let Ok(val) = std::env::var("PGSQLITE_CACHE_MEMORY_PRESSURE_ENABLED") {
            config.memory_pressure_enabled = val == "1" || val.to_lowercase() == "true";
        }

        if let Ok(val) = std::env::var("PGSQLITE_CACHE_MEMORY_PRESSURE_THRESHOLD_MB")
            && let Ok(threshold) = val.parse::<usize>() {
                config.memory_pressure_threshold = threshold;
            }

        if let Ok(val) = std::env::var("PGSQLITE_CACHE_PRESSURE_EVICTION_PERCENTAGE")
            && let Ok(percentage) = val.parse::<f32>() {
                config.pressure_eviction_percentage = percentage.clamp(0.0, 1.0);
            }

        if let Ok(val) = std::env::var("PGSQLITE_CACHE_ENABLE_LRU") {
            config.enable_lru = val == "1" || val.to_lowercase() == "true";
        }

        config
    }
}

/// Cache entry with TTL and access tracking
#[derive(Debug, Clone)]
struct CacheEntry<V> {
    value: V,
    inserted_at: Instant,
    expires_at: Instant,
    last_accessed: Instant,
    access_count: u64,
    size_bytes: usize,
}

impl<V> CacheEntry<V> {
    fn new(value: V, ttl: Duration, size_bytes: usize) -> Self {
        let now = Instant::now();
        Self {
            value,
            inserted_at: now,
            expires_at: now + ttl,
            last_accessed: now,
            access_count: 1,
            size_bytes,
        }
    }

    fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }

    fn access(&mut self) {
        self.last_accessed = Instant::now();
        self.access_count += 1;
    }

    fn age(&self) -> Duration {
        Instant::now().duration_since(self.inserted_at)
    }

    fn time_since_last_access(&self) -> Duration {
        Instant::now().duration_since(self.last_accessed)
    }
}

/// TTL-based cache with memory pressure handling
pub struct TtlCache<K, V> {
    config: TtlCacheConfig,
    entries: Arc<RwLock<HashMap<K, CacheEntry<V>>>>,
    stats: Arc<RwLock<CacheStats>>,
    last_cleanup: Arc<RwLock<Instant>>,
}

/// Cache statistics
#[derive(Debug, Default, Clone)]
pub struct CacheStats {
    pub total_entries: usize,
    pub total_size_bytes: usize,
    pub hits: u64,
    pub misses: u64,
    pub insertions: u64,
    pub evictions: u64,
    pub expired_evictions: u64,
    pub pressure_evictions: u64,
    pub lru_evictions: u64,
    pub cleanup_runs: u64,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        if self.hits + self.misses == 0 {
            0.0
        } else {
            self.hits as f64 / (self.hits + self.misses) as f64
        }
    }

    pub fn average_entry_size(&self) -> f64 {
        if self.total_entries == 0 {
            0.0
        } else {
            self.total_size_bytes as f64 / self.total_entries as f64
        }
    }
}

impl<K, V> TtlCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Create a new TTL cache with default configuration
    pub fn new() -> Self {
        Self::with_config(TtlCacheConfig::default())
    }

    /// Create a new TTL cache with custom configuration
    pub fn with_config(config: TtlCacheConfig) -> Self {
        Self {
            config,
            entries: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(CacheStats::default())),
            last_cleanup: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Get a value from the cache
    pub fn get(&self, key: &K) -> Option<V> {
        self.maybe_cleanup();

        let mut entries = self.entries.write();
        let mut stats = self.stats.write();

        if let Some(entry) = entries.get_mut(key) {
            if entry.is_expired() {
                // Remove expired entry
                let size_bytes = entry.size_bytes;
                entries.remove(key);
                stats.total_entries = entries.len();
                stats.total_size_bytes = stats.total_size_bytes.saturating_sub(size_bytes);
                stats.misses += 1;
                stats.expired_evictions += 1;

                // Record memory deallocation for expired entry
                global_memory_monitor().record_query_deallocation(size_bytes as u64);

                None
            } else {
                // Update access tracking
                if self.config.enable_lru {
                    entry.access();
                }
                stats.hits += 1;
                Some(entry.value.clone())
            }
        } else {
            stats.misses += 1;
            None
        }
    }

    /// Insert a value into the cache with default TTL
    pub fn insert(&self, key: K, value: V) {
        self.insert_with_ttl(key, value, self.config.default_ttl);
    }

    /// Insert a value into the cache with custom TTL
    pub fn insert_with_ttl(&self, key: K, value: V, ttl: Duration) {
        let size_bytes = self.estimate_size(&value);
        self.insert_with_ttl_and_size(key, value, ttl, size_bytes);
    }

    /// Insert a value with explicit size tracking
    pub fn insert_with_ttl_and_size(&self, key: K, value: V, ttl: Duration, size_bytes: usize) {
        self.maybe_cleanup();
        self.maybe_handle_memory_pressure();

        let entry = CacheEntry::new(value, ttl, size_bytes);

        let mut entries = self.entries.write();
        let mut stats = self.stats.write();

        // Check if we need to evict based on max entries
        if entries.len() >= self.config.max_entries {
            self.evict_lru_entry(&mut entries, &mut stats);
        }

        // Insert or update entry
        if let Some(old_entry) = entries.insert(key, entry) {
            // Record deallocation for old entry before replacement
            global_memory_monitor().record_query_deallocation(old_entry.size_bytes as u64);

            // Update size tracking for replacement
            stats.total_size_bytes = stats.total_size_bytes
                .saturating_sub(old_entry.size_bytes)
                .saturating_add(size_bytes);
        } else {
            // New entry
            stats.total_entries = entries.len();
            stats.total_size_bytes += size_bytes;
            stats.insertions += 1;
        }

        // Register memory usage with global monitor
        global_memory_monitor().record_query_allocation(size_bytes as u64);
    }

    /// Remove a specific key from the cache
    pub fn remove(&self, key: &K) -> Option<V> {
        let mut entries = self.entries.write();
        let mut stats = self.stats.write();

        if let Some(entry) = entries.remove(key) {
            stats.total_entries = entries.len();
            stats.total_size_bytes = stats.total_size_bytes.saturating_sub(entry.size_bytes);
            stats.evictions += 1;

            // Unregister memory usage
            global_memory_monitor().record_query_deallocation(entry.size_bytes as u64);

            Some(entry.value)
        } else {
            None
        }
    }

    /// Clear all entries from the cache
    pub fn clear(&self) {
        let mut entries = self.entries.write();
        let mut stats = self.stats.write();

        let total_size = stats.total_size_bytes;
        let entries_count = entries.len();

        entries.clear();
        stats.total_entries = 0;
        stats.total_size_bytes = 0;
        stats.evictions += entries_count as u64;

        // Unregister memory usage
        if total_size > 0 {
            global_memory_monitor().record_query_deallocation(total_size as u64);
        }
    }

    /// Get current cache statistics
    pub fn stats(&self) -> CacheStats {
        self.stats.read().clone()
    }

    /// Force cleanup of expired entries
    pub fn cleanup(&self) {
        let mut entries = self.entries.write();
        let mut stats = self.stats.write();

        let mut last_cleanup = self.last_cleanup.write();
        *last_cleanup = Instant::now();

        let initial_count = entries.len();
        let mut removed_size = 0;

        entries.retain(|_key, entry| {
            if entry.is_expired() {
                removed_size += entry.size_bytes;
                false
            } else {
                true
            }
        });

        let removed_count = initial_count - entries.len();
        if removed_count > 0 {
            stats.total_entries = entries.len();
            stats.total_size_bytes = stats.total_size_bytes.saturating_sub(removed_size);
            stats.expired_evictions += removed_count as u64;
            stats.cleanup_runs += 1;

            debug!("Cache cleanup: removed {} expired entries, freed {} bytes",
                   removed_count, removed_size);

            // Unregister memory usage
            if removed_size > 0 {
                global_memory_monitor().record_query_deallocation(removed_size as u64);
            }
        }

        drop(stats);
        drop(entries);

        // Validate memory tracking consistency after cleanup
        if !self.validate_memory_tracking() {
            debug!("Memory tracking was inconsistent after cleanup, fixed automatically");
        }
    }

    /// Check if cleanup should be performed
    fn maybe_cleanup(&self) {
        let last_cleanup = *self.last_cleanup.read();
        if last_cleanup.elapsed() >= self.config.cleanup_interval {
            self.cleanup();
        }

        // Additional memory leak prevention: force cleanup if cache grows too large
        let current_size = self.stats.read().total_size_bytes;
        let size_threshold = self.config.memory_pressure_threshold * 1024 * 1024; // Convert MB to bytes

        if current_size > size_threshold && self.config.memory_pressure_enabled {
            debug!("Cache size ({} bytes) exceeds threshold ({} bytes), forcing cleanup",
                   current_size, size_threshold);
            self.cleanup();

            // If still too large after cleanup, perform emergency eviction
            let size_after_cleanup = self.stats.read().total_size_bytes;
            if size_after_cleanup > size_threshold {
                self.emergency_memory_eviction();
            }
        }
    }

    /// Handle memory pressure by evicting entries
    fn maybe_handle_memory_pressure(&self) {
        if !self.config.memory_pressure_enabled {
            return;
        }

        let memory_stats = global_memory_monitor().get_stats();
        let current_memory_mb = memory_stats.total_bytes() as f64 / (1024.0 * 1024.0);

        if current_memory_mb > self.config.memory_pressure_threshold as f64 {
            match memory_stats.pressure_level {
                MemoryPressure::High | MemoryPressure::Critical => {
                    self.handle_memory_pressure_eviction();
                }
                _ => {}
            }
        }
    }

    /// Perform memory pressure-based eviction
    fn handle_memory_pressure_eviction(&self) {
        let mut entries = self.entries.write();
        let mut stats = self.stats.write();

        if entries.is_empty() {
            return;
        }

        let target_evictions = (entries.len() as f32 * self.config.pressure_eviction_percentage) as usize;
        if target_evictions == 0 {
            return;
        }

        info!("Memory pressure detected, evicting {} cache entries", target_evictions);

        // Collect entries sorted by eviction priority (oldest first, least accessed)
        let mut entry_priorities: Vec<(K, f64)> = entries
            .iter()
            .map(|(key, entry)| {
                let age_score = entry.age().as_secs() as f64;
                let access_score = 1.0 / (entry.access_count as f64 + 1.0);
                let staleness_score = entry.time_since_last_access().as_secs() as f64;

                // Higher score = higher eviction priority
                let priority = age_score + (access_score * 100.0) + staleness_score;
                (key.clone(), priority)
            })
            .collect();

        // Sort by priority (highest first)
        entry_priorities.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let mut evicted_count = 0;
        let mut evicted_size = 0;

        for (key, _priority) in entry_priorities.into_iter().take(target_evictions) {
            if let Some(entry) = entries.remove(&key) {
                evicted_size += entry.size_bytes;
                evicted_count += 1;
            }
        }

        if evicted_count > 0 {
            stats.total_entries = entries.len();
            stats.total_size_bytes = stats.total_size_bytes.saturating_sub(evicted_size);
            stats.pressure_evictions += evicted_count as u64;
            stats.evictions += evicted_count as u64;

            info!("Memory pressure eviction: removed {} entries, freed {} bytes",
                  evicted_count, evicted_size);

            // Unregister memory usage
            if evicted_size > 0 {
                global_memory_monitor().record_query_deallocation(evicted_size as u64);
            }
        }
    }

    /// Emergency memory eviction when cache grows too large
    fn emergency_memory_eviction(&self) {
        let mut entries = self.entries.write();
        let mut stats = self.stats.write();

        if entries.is_empty() {
            return;
        }

        // More aggressive eviction: remove 50% of entries
        let target_evictions = entries.len() / 2;
        if target_evictions == 0 {
            return;
        }

        info!("Emergency memory eviction: cache size too large, evicting {} entries", target_evictions);

        // Collect all entries with simple priority (oldest first)
        let mut entry_keys: Vec<(K, Instant)> = entries
            .iter()
            .map(|(key, entry)| (key.clone(), entry.inserted_at))
            .collect();

        // Sort by insertion time (oldest first)
        entry_keys.sort_by_key(|(_, inserted_at)| *inserted_at);

        let mut evicted_count = 0;
        let mut evicted_size = 0;

        for (key, _) in entry_keys.into_iter().take(target_evictions) {
            if let Some(entry) = entries.remove(&key) {
                evicted_size += entry.size_bytes;
                evicted_count += 1;
            }
        }

        if evicted_count > 0 {
            stats.total_entries = entries.len();
            stats.total_size_bytes = stats.total_size_bytes.saturating_sub(evicted_size);
            stats.evictions += evicted_count as u64;

            info!("Emergency eviction completed: removed {} entries, freed {} bytes",
                  evicted_count, evicted_size);

            // Unregister memory usage
            if evicted_size > 0 {
                global_memory_monitor().record_query_deallocation(evicted_size as u64);
            }
        }
    }

    /// Evict least recently used entry
    fn evict_lru_entry(
        &self,
        entries: &mut HashMap<K, CacheEntry<V>>,
        stats: &mut CacheStats,
    ) {
        if entries.is_empty() {
            return;
        }

        // Find LRU entry
        let lru_key = if self.config.enable_lru {
            entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_accessed)
                .map(|(key, _)| key.clone())
        } else {
            // Fall back to oldest entry
            entries
                .iter()
                .min_by_key(|(_, entry)| entry.inserted_at)
                .map(|(key, _)| key.clone())
        };

        if let Some(key) = lru_key {
            if let Some(entry) = entries.remove(&key) {
                stats.total_entries = entries.len();
                stats.total_size_bytes = stats.total_size_bytes.saturating_sub(entry.size_bytes);
                stats.lru_evictions += 1;
                stats.evictions += 1;

                debug!("LRU eviction: removed entry, freed {} bytes", entry.size_bytes);

                // Unregister memory usage
                global_memory_monitor().record_query_deallocation(entry.size_bytes as u64);
            }
        }
    }

    /// Estimate the size of a value in bytes
    fn estimate_size(&self, _value: &V) -> usize {
        // This is a simple estimation. In a real implementation,
        // you might want to implement a trait for size estimation
        // or use more sophisticated memory profiling
        std::mem::size_of::<V>() + 64 // Base overhead estimate
    }

    /// Get all keys currently in the cache (for debugging)
    pub fn keys(&self) -> Vec<K> {
        self.entries.read().keys().cloned().collect()
    }

    /// Get the number of entries in the cache
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Get entries that will expire within the given duration
    pub fn entries_expiring_within(&self, duration: Duration) -> Vec<K> {
        let threshold = Instant::now() + duration;
        self.entries
            .read()
            .iter()
            .filter_map(|(key, entry)| {
                if entry.expires_at <= threshold {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Validate and fix memory tracking inconsistencies
    pub fn validate_memory_tracking(&self) -> bool {
        let entries = self.entries.read();
        let stats = self.stats.read();

        // Calculate actual size by summing all entries
        let actual_size: usize = entries.values().map(|entry| entry.size_bytes).sum();
        let actual_count = entries.len();

        // Check for inconsistencies
        let size_mismatch = actual_size != stats.total_size_bytes;
        let count_mismatch = actual_count != stats.total_entries;

        if size_mismatch || count_mismatch {
            debug!("Memory tracking inconsistency detected - Size: actual={}, tracked={}, Count: actual={}, tracked={}",
                   actual_size, stats.total_size_bytes, actual_count, stats.total_entries);

            drop(stats);
            drop(entries);

            // Fix inconsistencies
            let mut stats = self.stats.write();
            stats.total_size_bytes = actual_size;
            stats.total_entries = actual_count;

            return false; // Found inconsistencies
        }

        true // No inconsistencies found
    }

    /// Extend TTL for a specific key
    pub fn extend_ttl(&self, key: &K, additional_ttl: Duration) -> bool {
        let mut entries = self.entries.write();
        if let Some(entry) = entries.get_mut(key) {
            entry.expires_at += additional_ttl;
            true
        } else {
            false
        }
    }
}

impl<K, V> Default for TtlCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Factory for creating configured TTL caches
pub struct TtlCacheFactory {
    config: TtlCacheConfig,
}

impl TtlCacheFactory {
    pub fn new() -> Self {
        Self {
            config: TtlCacheConfig::from_env(),
        }
    }

    pub fn with_config(config: TtlCacheConfig) -> Self {
        Self { config }
    }

    pub fn create_cache<K, V>(&self) -> TtlCache<K, V>
    where
        K: Eq + Hash + Clone,
        V: Clone,
    {
        TtlCache::with_config(self.config.clone())
    }

    pub fn create_string_cache(&self) -> TtlCache<String, String> {
        self.create_cache()
    }

    pub fn create_query_cache(&self) -> TtlCache<String, Vec<u8>> {
        self.create_cache()
    }
}

impl Default for TtlCacheFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_basic_cache_operations() {
        let cache = TtlCache::new();

        // Insert and retrieve
        cache.insert("key1".to_string(), "value1".to_string());
        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));
        assert_eq!(cache.get(&"nonexistent".to_string()), None);

        // Check stats
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.insertions, 1);
    }

    #[test]
    fn test_ttl_expiration() {
        let cache = TtlCache::new();

        // Insert with short TTL
        cache.insert_with_ttl("key1".to_string(), "value1".to_string(), Duration::from_millis(50));

        // Should be available immediately
        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));

        // Wait for expiration
        thread::sleep(Duration::from_millis(100));

        // Should be expired
        assert_eq!(cache.get(&"key1".to_string()), None);

        let stats = cache.stats();
        assert_eq!(stats.expired_evictions, 1);
    }

    #[test]
    fn test_max_entries_eviction() {
        let config = TtlCacheConfig {
            max_entries: 3,
            enable_lru: true,
            ..Default::default()
        };
        let cache = TtlCache::with_config(config);

        // Fill cache to capacity
        cache.insert("key1".to_string(), "value1".to_string());
        cache.insert("key2".to_string(), "value2".to_string());
        cache.insert("key3".to_string(), "value3".to_string());

        assert_eq!(cache.len(), 3);

        // Access key1 to make it more recently used
        cache.get(&"key1".to_string());

        // Insert another key, should evict least recently used
        cache.insert("key4".to_string(), "value4".to_string());

        assert_eq!(cache.len(), 3);
        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string())); // Should still exist
        assert_eq!(cache.get(&"key4".to_string()), Some("value4".to_string())); // Should exist

        let stats = cache.stats();
        assert_eq!(stats.lru_evictions, 1);
    }

    #[test]
    fn test_cleanup() {
        let cache = TtlCache::new();

        // Insert entries with different TTLs
        cache.insert_with_ttl("key1".to_string(), "value1".to_string(), Duration::from_millis(50));
        cache.insert_with_ttl("key2".to_string(), "value2".to_string(), Duration::from_secs(60));

        assert_eq!(cache.len(), 2);

        // Wait for first entry to expire
        thread::sleep(Duration::from_millis(100));

        // Force cleanup
        cache.cleanup();

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(&"key2".to_string()), Some("value2".to_string()));

        let stats = cache.stats();
        assert_eq!(stats.cleanup_runs, 1);
    }

    #[test]
    fn test_cache_clear() {
        let cache = TtlCache::new();

        cache.insert("key1".to_string(), "value1".to_string());
        cache.insert("key2".to_string(), "value2".to_string());

        assert_eq!(cache.len(), 2);

        cache.clear();

        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_ttl_extension() {
        let cache = TtlCache::new();

        cache.insert_with_ttl("key1".to_string(), "value1".to_string(), Duration::from_millis(100));

        // Extend TTL
        assert!(cache.extend_ttl(&"key1".to_string(), Duration::from_millis(100)));
        assert!(!cache.extend_ttl(&"nonexistent".to_string(), Duration::from_millis(100)));

        // Wait for original TTL
        thread::sleep(Duration::from_millis(150));

        // Should still be available due to extension
        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));
    }

    #[test]
    fn test_entries_expiring_within() {
        let cache = TtlCache::new();

        cache.insert_with_ttl("key1".to_string(), "value1".to_string(), Duration::from_millis(50));
        cache.insert_with_ttl("key2".to_string(), "value2".to_string(), Duration::from_secs(60));

        let expiring = cache.entries_expiring_within(Duration::from_millis(100));
        assert_eq!(expiring.len(), 1);
        assert!(expiring.contains(&"key1".to_string()));
    }

    #[test]
    fn test_hit_rate_calculation() {
        let cache = TtlCache::new();

        cache.insert("key1".to_string(), "value1".to_string());

        // 2 hits, 1 miss
        cache.get(&"key1".to_string());
        cache.get(&"key1".to_string());
        cache.get(&"nonexistent".to_string());

        let stats = cache.stats();
        assert!((stats.hit_rate() - 0.6666666666666666).abs() < 0.0001);
    }

    #[test]
    fn test_cache_factory() {
        let factory = TtlCacheFactory::new();
        let cache: TtlCache<String, String> = factory.create_cache();

        cache.insert("test".to_string(), "value".to_string());
        assert_eq!(cache.get(&"test".to_string()), Some("value".to_string()));
    }

    #[test]
    fn test_config_from_env() {
        unsafe {
            std::env::set_var("PGSQLITE_CACHE_DEFAULT_TTL_SECS", "600");
            std::env::set_var("PGSQLITE_CACHE_MAX_ENTRIES", "5000");
            std::env::set_var("PGSQLITE_CACHE_MEMORY_PRESSURE_ENABLED", "false");
            std::env::set_var("PGSQLITE_CACHE_ENABLE_LRU", "false");
        }

        let config = TtlCacheConfig::from_env();
        assert_eq!(config.default_ttl, Duration::from_secs(600));
        assert_eq!(config.max_entries, 5000);
        assert!(!config.memory_pressure_enabled);
        assert!(!config.enable_lru);

        unsafe {
            std::env::remove_var("PGSQLITE_CACHE_DEFAULT_TTL_SECS");
            std::env::remove_var("PGSQLITE_CACHE_MAX_ENTRIES");
            std::env::remove_var("PGSQLITE_CACHE_MEMORY_PRESSURE_ENABLED");
            std::env::remove_var("PGSQLITE_CACHE_ENABLE_LRU");
        }
    }

    #[test]
    fn test_memory_leak_prevention() {
        let cache = TtlCache::new();

        // Insert entries with explicit size
        cache.insert_with_ttl_and_size("key1".to_string(), "value1".to_string(), Duration::from_secs(1), 100);
        cache.insert_with_ttl_and_size("key2".to_string(), "value2".to_string(), Duration::from_secs(1), 200);

        let initial_stats = cache.stats();
        assert_eq!(initial_stats.total_size_bytes, 300);
        assert_eq!(initial_stats.total_entries, 2);

        // Replace entry (should deallocate old one)
        cache.insert_with_ttl_and_size("key1".to_string(), "new_value1".to_string(), Duration::from_secs(1), 150);

        let stats_after_replace = cache.stats();
        assert_eq!(stats_after_replace.total_size_bytes, 350); // 200 + 150
        assert_eq!(stats_after_replace.total_entries, 2);

        // Wait for expiration
        thread::sleep(Duration::from_millis(1100));

        // Access expired entry (should trigger deallocation)
        cache.get(&"key1".to_string());

        let stats_after_expired_access = cache.stats();
        assert_eq!(stats_after_expired_access.expired_evictions, 1);

        // Validate memory tracking consistency
        assert!(cache.validate_memory_tracking());
    }

    #[test]
    fn test_emergency_memory_eviction() {
        let config = TtlCacheConfig {
            memory_pressure_threshold: 1, // 1MB threshold (very low for testing)
            memory_pressure_enabled: true,
            ..Default::default()
        };
        let cache = TtlCache::with_config(config);

        // Insert entries that exceed the memory threshold
        for i in 0..10 {
            cache.insert_with_ttl_and_size(
                format!("key{}", i),
                format!("value{}", i),
                Duration::from_secs(60),
                200_000 // 200KB each
            );
        }

        // This should trigger emergency eviction during maybe_cleanup
        cache.insert("trigger".to_string(), "value".to_string());

        // Validate that memory tracking is consistent after emergency eviction
        assert!(cache.validate_memory_tracking());
    }
}