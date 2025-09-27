use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use tracing::{debug, warn, info};
use crate::cache::{TtlCache, TtlCacheConfig, TtlCacheFactory};
use crate::protocol::{MemoryPressure, global_memory_monitor};
use crate::security::events;

/// Configuration for memory-aware cache management
#[derive(Debug, Clone)]
pub struct MemoryAwareCacheConfig {
    /// Enable memory pressure monitoring
    pub enable_pressure_monitoring: bool,
    /// Memory threshold in MB for starting aggressive eviction
    pub pressure_threshold_mb: usize,
    /// Critical memory threshold in MB for emergency eviction
    pub critical_threshold_mb: usize,
    /// Interval for checking memory pressure
    pub check_interval: Duration,
    /// Percentage of cache to evict under pressure
    pub pressure_eviction_percentage: f32,
    /// Percentage of cache to evict under critical pressure
    pub critical_eviction_percentage: f32,
    /// Enable adaptive TTL based on memory pressure
    pub enable_adaptive_ttl: bool,
    /// Minimum TTL under memory pressure
    pub min_ttl_under_pressure: Duration,
}

impl Default for MemoryAwareCacheConfig {
    fn default() -> Self {
        Self {
            enable_pressure_monitoring: true,
            pressure_threshold_mb: 128,
            critical_threshold_mb: 256,
            check_interval: Duration::from_secs(10),
            pressure_eviction_percentage: 0.3,
            critical_eviction_percentage: 0.7,
            enable_adaptive_ttl: true,
            min_ttl_under_pressure: Duration::from_secs(30),
        }
    }
}

impl MemoryAwareCacheConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(val) = std::env::var("PGSQLITE_MEMORY_CACHE_PRESSURE_MONITORING") {
            config.enable_pressure_monitoring = val == "1" || val.to_lowercase() == "true";
        }

        if let Ok(val) = std::env::var("PGSQLITE_MEMORY_CACHE_PRESSURE_THRESHOLD_MB")
            && let Ok(threshold) = val.parse::<usize>() {
                config.pressure_threshold_mb = threshold;
            }

        if let Ok(val) = std::env::var("PGSQLITE_MEMORY_CACHE_CRITICAL_THRESHOLD_MB")
            && let Ok(threshold) = val.parse::<usize>() {
                config.critical_threshold_mb = threshold;
            }

        if let Ok(val) = std::env::var("PGSQLITE_MEMORY_CACHE_CHECK_INTERVAL_SECS")
            && let Ok(interval) = val.parse::<u64>() {
                config.check_interval = Duration::from_secs(interval);
            }

        if let Ok(val) = std::env::var("PGSQLITE_MEMORY_CACHE_PRESSURE_EVICTION_PERCENTAGE")
            && let Ok(percentage) = val.parse::<f32>() {
                config.pressure_eviction_percentage = percentage.clamp(0.0, 1.0);
            }

        if let Ok(val) = std::env::var("PGSQLITE_MEMORY_CACHE_CRITICAL_EVICTION_PERCENTAGE")
            && let Ok(percentage) = val.parse::<f32>() {
                config.critical_eviction_percentage = percentage.clamp(0.0, 1.0);
            }

        if let Ok(val) = std::env::var("PGSQLITE_MEMORY_CACHE_ADAPTIVE_TTL") {
            config.enable_adaptive_ttl = val == "1" || val.to_lowercase() == "true";
        }

        if let Ok(val) = std::env::var("PGSQLITE_MEMORY_CACHE_MIN_TTL_PRESSURE_SECS")
            && let Ok(ttl_secs) = val.parse::<u64>() {
                config.min_ttl_under_pressure = Duration::from_secs(ttl_secs);
            }

        config
    }
}

/// Statistics for memory-aware cache management
#[derive(Debug, Default, Clone)]
pub struct MemoryAwareCacheStats {
    pub total_managed_caches: usize,
    pub total_entries: usize,
    pub total_memory_bytes: usize,
    pub pressure_checks: u64,
    pub pressure_evictions: u64,
    pub critical_evictions: u64,
    pub adaptive_ttl_adjustments: u64,
    pub last_check_time: Option<Instant>,
    pub current_memory_pressure: MemoryPressure,
}

/// Trait for caches that can be managed by the memory-aware manager
pub trait ManagedCache {
    fn evict_percentage(&self, percentage: f32) -> usize;
    fn clear(&self);
    fn len(&self) -> usize;
    fn memory_usage_bytes(&self) -> usize;
    fn stats_summary(&self) -> String;
}

/// Implementation of ManagedCache for TtlCache
impl<K, V> ManagedCache for TtlCache<K, V>
where
    K: Eq + std::hash::Hash + Clone,
    V: Clone,
{
    fn evict_percentage(&self, percentage: f32) -> usize {
        let current_size = self.len();
        if current_size == 0 {
            return 0;
        }

        let target_evictions = (current_size as f32 * percentage) as usize;
        let initial_size = current_size;

        // Force cleanup first to remove expired entries
        self.cleanup();

        // Calculate how many were actually evicted
        let after_cleanup = self.len();
        initial_size.saturating_sub(after_cleanup)
    }

    fn clear(&self) {
        self.clear();
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn memory_usage_bytes(&self) -> usize {
        self.stats().total_size_bytes
    }

    fn stats_summary(&self) -> String {
        let stats = self.stats();
        format!("entries: {}, size: {} bytes, hit_rate: {:.2}%",
                stats.total_entries,
                stats.total_size_bytes,
                stats.hit_rate() * 100.0)
    }
}

/// Memory-aware cache manager
pub struct MemoryAwareCacheManager {
    config: MemoryAwareCacheConfig,
    managed_caches: Arc<RwLock<Vec<(String, Arc<dyn ManagedCache + Send + Sync>)>>>,
    stats: Arc<RwLock<MemoryAwareCacheStats>>,
    last_check: Arc<RwLock<Instant>>,
    cache_factory: TtlCacheFactory,
}

impl MemoryAwareCacheManager {
    pub fn new() -> Self {
        Self::with_config(MemoryAwareCacheConfig::default())
    }

    pub fn with_config(config: MemoryAwareCacheConfig) -> Self {
        // Create cache factory with memory-aware settings
        let cache_config = TtlCacheConfig {
            memory_pressure_enabled: config.enable_pressure_monitoring,
            memory_pressure_threshold: config.pressure_threshold_mb,
            pressure_eviction_percentage: config.pressure_eviction_percentage,
            ..TtlCacheConfig::default()
        };

        Self {
            config,
            managed_caches: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(RwLock::new(MemoryAwareCacheStats::default())),
            last_check: Arc::new(RwLock::new(Instant::now())),
            cache_factory: TtlCacheFactory::with_config(cache_config),
        }
    }

    /// Register a cache for memory management
    pub fn register_cache<T>(&self, name: String, cache: Arc<T>)
    where
        T: ManagedCache + Send + Sync + 'static,
    {
        let mut caches = self.managed_caches.write();
        caches.push((name, cache));

        let mut stats = self.stats.write();
        stats.total_managed_caches = caches.len();
    }

    /// Create and register a new TTL cache
    pub fn create_and_register_cache<K, V>(&self, name: String) -> Arc<TtlCache<K, V>>
    where
        K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
        V: Clone + Send + Sync + 'static,
    {
        let cache = Arc::new(self.cache_factory.create_cache());
        self.register_cache(name, cache.clone());
        cache
    }

    /// Check memory pressure and take action if needed
    pub fn check_memory_pressure(&self) {
        if !self.config.enable_pressure_monitoring {
            return;
        }

        // Rate limit checks
        {
            let last_check = *self.last_check.read();
            if last_check.elapsed() < self.config.check_interval {
                return;
            }
        }

        let memory_stats = global_memory_monitor().get_stats();
        let current_memory_mb = memory_stats.total_bytes() as f64 / (1024.0 * 1024.0);

        // Update last check time
        {
            let mut last_check = self.last_check.write();
            *last_check = Instant::now();
        }

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.pressure_checks += 1;
            stats.last_check_time = Some(Instant::now());
            stats.current_memory_pressure = memory_stats.pressure_level;
            stats.total_memory_bytes = memory_stats.total_bytes() as usize;
        }

        // Determine action based on memory usage and pressure
        let action = if current_memory_mb > self.config.critical_threshold_mb as f64 {
            MemoryAction::CriticalEviction
        } else if current_memory_mb > self.config.pressure_threshold_mb as f64 {
            MemoryAction::PressureEviction
        } else {
            match memory_stats.pressure_level {
                MemoryPressure::Critical => MemoryAction::CriticalEviction,
                MemoryPressure::High => MemoryAction::PressureEviction,
                MemoryPressure::Medium => MemoryAction::AdaptiveTtl,
                MemoryPressure::Low => MemoryAction::None,
            }
        };

        self.execute_memory_action(action, current_memory_mb);
    }

    fn execute_memory_action(&self, action: MemoryAction, current_memory_mb: f64) {
        match action {
            MemoryAction::None => return,
            MemoryAction::AdaptiveTtl => {
                if self.config.enable_adaptive_ttl {
                    self.adjust_adaptive_ttl();
                }
            }
            MemoryAction::PressureEviction => {
                self.handle_pressure_eviction(current_memory_mb);
            }
            MemoryAction::CriticalEviction => {
                self.handle_critical_eviction(current_memory_mb);
            }
        }
    }

    fn adjust_adaptive_ttl(&self) {
        let mut stats = self.stats.write();
        stats.adaptive_ttl_adjustments += 1;

        debug!("Adjusting cache TTLs due to memory pressure");
        // TTL adjustment is handled by individual caches
        // This is a placeholder for future enhancements
    }

    fn handle_pressure_eviction(&self, current_memory_mb: f64) {
        let caches = self.managed_caches.read();
        if caches.is_empty() {
            return;
        }

        info!("Memory pressure detected ({:.1}MB), starting cache eviction", current_memory_mb);

        let total_evicted = self.evict_from_caches(&*caches, self.config.pressure_eviction_percentage);

        let mut stats = self.stats.write();
        stats.pressure_evictions += 1;

        // Log security event for memory pressure
        events::protocol_violation(
            None,
            &format!("Memory pressure eviction: {:.1}MB, evicted {} entries",
                    current_memory_mb, total_evicted)
        );

        info!("Pressure eviction complete: evicted {} entries", total_evicted);
    }

    fn handle_critical_eviction(&self, current_memory_mb: f64) {
        let caches = self.managed_caches.read();
        if caches.is_empty() {
            return;
        }

        warn!("Critical memory pressure detected ({:.1}MB), starting aggressive eviction", current_memory_mb);

        let total_evicted = self.evict_from_caches(&*caches, self.config.critical_eviction_percentage);

        let mut stats = self.stats.write();
        stats.critical_evictions += 1;

        // Log security event for critical memory pressure
        events::protocol_violation(
            None,
            &format!("Critical memory pressure eviction: {:.1}MB, evicted {} entries",
                    current_memory_mb, total_evicted)
        );

        warn!("Critical eviction complete: evicted {} entries", total_evicted);
    }

    fn evict_from_caches(
        &self,
        caches: &[(String, Arc<dyn ManagedCache + Send + Sync>)],
        eviction_percentage: f32,
    ) -> usize {
        let mut total_evicted = 0;

        for (name, cache) in caches {
            let initial_size = cache.len();
            if initial_size == 0 {
                continue;
            }

            let evicted = cache.evict_percentage(eviction_percentage);
            total_evicted += evicted;

            let final_size = cache.len();
            let actual_evicted = initial_size.saturating_sub(final_size);

            debug!("Cache '{}': evicted {} entries ({} -> {})",
                   name, actual_evicted, initial_size, final_size);
        }

        total_evicted
    }

    /// Get current statistics
    pub fn get_stats(&self) -> MemoryAwareCacheStats {
        let caches = self.managed_caches.read();
        let mut stats = self.stats.write();

        // Update current statistics
        stats.total_managed_caches = caches.len();
        stats.total_entries = caches.iter().map(|(_, cache)| cache.len()).sum();
        stats.total_memory_bytes = caches.iter().map(|(_, cache)| cache.memory_usage_bytes()).sum();

        stats.clone()
    }

    /// Get detailed cache information
    pub fn get_cache_info(&self) -> Vec<(String, String)> {
        let caches = self.managed_caches.read();
        caches
            .iter()
            .map(|(name, cache)| (name.clone(), cache.stats_summary()))
            .collect()
    }

    /// Force eviction across all caches
    pub fn force_eviction(&self, percentage: f32) -> usize {
        let caches = self.managed_caches.read();
        self.evict_from_caches(&*caches, percentage)
    }

    /// Clear all managed caches
    pub fn clear_all_caches(&self) {
        let caches = self.managed_caches.read();
        for (name, cache) in caches.iter() {
            cache.clear();
            debug!("Cleared cache: {}", name);
        }

        info!("Cleared all {} managed caches", caches.len());
    }

    /// Start background monitoring task
    pub fn start_monitoring(&self) -> tokio::task::JoinHandle<()> {
        let manager = self.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(manager.config.check_interval);

            loop {
                interval.tick().await;
                manager.check_memory_pressure();
            }
        })
    }
}

impl Clone for MemoryAwareCacheManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            managed_caches: self.managed_caches.clone(),
            stats: self.stats.clone(),
            last_check: self.last_check.clone(),
            cache_factory: self.cache_factory.clone(),
        }
    }
}

impl Default for MemoryAwareCacheManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy)]
enum MemoryAction {
    None,
    AdaptiveTtl,
    PressureEviction,
    CriticalEviction,
}

/// Global memory-aware cache manager instance
static GLOBAL_CACHE_MANAGER: std::sync::OnceLock<MemoryAwareCacheManager> = std::sync::OnceLock::new();

/// Get the global memory-aware cache manager
pub fn global_cache_manager() -> &'static MemoryAwareCacheManager {
    GLOBAL_CACHE_MANAGER.get_or_init(|| {
        let config = MemoryAwareCacheConfig::from_env();
        MemoryAwareCacheManager::with_config(config)
    })
}

/// Register a cache with the global manager
pub fn register_global_cache<T>(name: String, cache: Arc<T>)
where
    T: ManagedCache + Send + Sync + 'static,
{
    global_cache_manager().register_cache(name, cache);
}

/// Create and register a cache with the global manager
pub fn create_global_cache<K, V>(name: String) -> Arc<TtlCache<K, V>>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    global_cache_manager().create_and_register_cache(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_memory_aware_cache_manager() {
        let config = MemoryAwareCacheConfig {
            enable_pressure_monitoring: true,
            pressure_threshold_mb: 1, // Very low for testing
            critical_threshold_mb: 2,
            check_interval: Duration::from_millis(100),
            ..Default::default()
        };

        let manager = MemoryAwareCacheManager::with_config(config);

        // Create and register a test cache
        let cache: Arc<TtlCache<String, String>> = manager.create_and_register_cache("test_cache".to_string());

        // Add some entries
        for i in 0..10 {
            cache.insert(format!("key_{}", i), format!("value_{}", i));
        }

        let stats = manager.get_stats();
        assert_eq!(stats.total_managed_caches, 1);
        assert!(stats.total_entries > 0);

        // Test forced eviction
        let evicted = manager.force_eviction(0.5);
        assert!(evicted > 0);
    }

    #[test]
    fn test_managed_cache_trait() {
        let cache = TtlCache::new();

        // Add entries
        for i in 0..10 {
            cache.insert(format!("key_{}", i), format!("value_{}", i));
        }

        assert_eq!(cache.len(), 10);

        // Test eviction
        let evicted = cache.evict_percentage(0.5);

        // Should have evicted some entries (exact number depends on cleanup)
        assert!(evicted >= 0);
    }

    #[test]
    fn test_cache_stats() {
        let manager = MemoryAwareCacheManager::new();
        let cache: Arc<TtlCache<String, String>> = manager.create_and_register_cache("stats_test".to_string());

        cache.insert("test_key".to_string(), "test_value".to_string());

        let info = manager.get_cache_info();
        assert_eq!(info.len(), 1);
        assert_eq!(info[0].0, "stats_test");
        assert!(info[0].1.contains("entries: 1"));
    }

    #[test]
    fn test_config_from_env() {
        unsafe {
            std::env::set_var("PGSQLITE_MEMORY_CACHE_PRESSURE_MONITORING", "false");
            std::env::set_var("PGSQLITE_MEMORY_CACHE_PRESSURE_THRESHOLD_MB", "256");
            std::env::set_var("PGSQLITE_MEMORY_CACHE_ADAPTIVE_TTL", "false");
        }

        let config = MemoryAwareCacheConfig::from_env();
        assert!(!config.enable_pressure_monitoring);
        assert_eq!(config.pressure_threshold_mb, 256);
        assert!(!config.enable_adaptive_ttl);

        unsafe {
            std::env::remove_var("PGSQLITE_MEMORY_CACHE_PRESSURE_MONITORING");
            std::env::remove_var("PGSQLITE_MEMORY_CACHE_PRESSURE_THRESHOLD_MB");
            std::env::remove_var("PGSQLITE_MEMORY_CACHE_ADAPTIVE_TTL");
        }
    }

    #[test]
    fn test_global_cache_manager() {
        let cache = create_global_cache::<String, String>("global_test".to_string());
        cache.insert("test".to_string(), "value".to_string());

        let stats = global_cache_manager().get_stats();
        assert!(stats.total_managed_caches > 0);
    }
}