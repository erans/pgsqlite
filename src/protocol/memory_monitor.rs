use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::{Duration, Instant};
use parking_lot::Mutex;
use tracing::{debug, warn, info};

/// Configuration for memory pressure monitoring
#[derive(Debug, Clone)]
pub struct MemoryMonitorConfig {
    /// Memory threshold in bytes before triggering cleanup (default: 64MB)
    pub memory_threshold: usize,
    /// High memory threshold for aggressive cleanup (default: 128MB)
    pub high_memory_threshold: usize,
    /// Interval for memory usage checks
    pub check_interval: Duration,
    /// Enable automatic memory pressure response
    pub enable_auto_cleanup: bool,
    /// Enable detailed memory monitoring
    pub enable_detailed_monitoring: bool,
}

impl Default for MemoryMonitorConfig {
    fn default() -> Self {
        Self {
            memory_threshold: 64 * 1024 * 1024, // 64MB
            high_memory_threshold: 128 * 1024 * 1024, // 128MB
            check_interval: Duration::from_secs(10),
            enable_auto_cleanup: std::env::var("PGSQLITE_AUTO_CLEANUP").unwrap_or_default() == "1",
            enable_detailed_monitoring: std::env::var("PGSQLITE_MEMORY_MONITORING").unwrap_or_default() == "1",
        }
    }
}

impl MemoryMonitorConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();
        
        if let Ok(val) = std::env::var("PGSQLITE_MEMORY_THRESHOLD") {
            if let Ok(threshold) = val.parse::<usize>() {
                config.memory_threshold = threshold;
            }
        }
        
        if let Ok(val) = std::env::var("PGSQLITE_HIGH_MEMORY_THRESHOLD") {
            if let Ok(threshold) = val.parse::<usize>() {
                config.high_memory_threshold = threshold;
            }
        }
        
        if let Ok(val) = std::env::var("PGSQLITE_MEMORY_CHECK_INTERVAL") {
            if let Ok(secs) = val.parse::<u64>() {
                config.check_interval = Duration::from_secs(secs);
            }
        }
        
        config
    }
}

/// Memory pressure levels
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPressure {
    Low,     // Normal operation
    Medium,  // Approaching threshold
    High,    // Above threshold, cleanup recommended
    Critical, // Well above threshold, aggressive cleanup needed
}

/// Memory usage statistics
#[derive(Debug, Clone)]
pub struct MemoryStats {
    /// Total bytes allocated by buffer pools
    pub buffer_pool_bytes: u64,
    /// Total bytes allocated for message construction
    pub message_bytes: u64,
    /// Total bytes allocated for query processing
    pub query_bytes: u64,
    /// Peak memory usage
    pub peak_memory_bytes: u64,
    /// Current memory pressure level
    pub pressure_level: MemoryPressure,
    /// Number of cleanup events triggered
    pub cleanup_events: u64,
    /// Last cleanup timestamp
    pub last_cleanup: Option<Instant>,
    /// Memory allocations per second (estimated)
    pub allocation_rate: f64,
}

impl Default for MemoryStats {
    fn default() -> Self {
        Self {
            buffer_pool_bytes: 0,
            message_bytes: 0,
            query_bytes: 0,
            peak_memory_bytes: 0,
            pressure_level: MemoryPressure::Low,
            cleanup_events: 0,
            last_cleanup: None,
            allocation_rate: 0.0,
        }
    }
}

impl MemoryStats {
    /// Get total memory usage
    pub fn total_bytes(&self) -> u64 {
        self.buffer_pool_bytes + self.message_bytes + self.query_bytes
    }
    
    /// Calculate memory efficiency (reuse rate)
    pub fn efficiency_percentage(&self) -> f64 {
        if self.peak_memory_bytes == 0 {
            100.0
        } else {
            (1.0 - (self.total_bytes() as f64 / self.peak_memory_bytes as f64)) * 100.0
        }
    }
}

/// Thread-safe memory monitor for tracking and managing memory pressure
pub struct MemoryMonitor {
    config: MemoryMonitorConfig,
    stats: Arc<Mutex<MemoryStats>>,
    
    // Atomic counters for high-performance tracking
    buffer_pool_bytes: Arc<AtomicU64>,
    message_bytes: Arc<AtomicU64>,
    query_bytes: Arc<AtomicU64>,
    cleanup_events: Arc<AtomicU64>,
    
    // State tracking
    monitoring_active: Arc<AtomicBool>,
    last_check: Arc<Mutex<Instant>>,
    
    // Cleanup callbacks
    cleanup_callbacks: Arc<Mutex<Vec<Box<dyn Fn() + Send + Sync>>>>,
}

impl MemoryMonitor {
    /// Create a new memory monitor with default configuration
    pub fn new() -> Self {
        Self::with_config(MemoryMonitorConfig::default())
    }
    
    /// Create a new memory monitor with custom configuration
    pub fn with_config(config: MemoryMonitorConfig) -> Self {
        Self {
            config,
            stats: Arc::new(Mutex::new(MemoryStats::default())),
            buffer_pool_bytes: Arc::new(AtomicU64::new(0)),
            message_bytes: Arc::new(AtomicU64::new(0)),
            query_bytes: Arc::new(AtomicU64::new(0)),
            cleanup_events: Arc::new(AtomicU64::new(0)),
            monitoring_active: Arc::new(AtomicBool::new(true)),
            last_check: Arc::new(Mutex::new(Instant::now())),
            cleanup_callbacks: Arc::new(Mutex::new(Vec::new())),
        }
    }
    
    /// Record buffer pool memory allocation
    pub fn record_buffer_allocation(&self, bytes: u64) {
        self.buffer_pool_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.check_memory_pressure();
    }
    
    /// Record buffer pool memory deallocation
    pub fn record_buffer_deallocation(&self, bytes: u64) {
        self.buffer_pool_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }
    
    /// Record message construction memory usage
    pub fn record_message_allocation(&self, bytes: u64) {
        self.message_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.check_memory_pressure();
    }
    
    /// Record message memory deallocation
    pub fn record_message_deallocation(&self, bytes: u64) {
        self.message_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }
    
    /// Record query processing memory usage
    pub fn record_query_allocation(&self, bytes: u64) {
        self.query_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.check_memory_pressure();
    }
    
    /// Record query memory deallocation
    pub fn record_query_deallocation(&self, bytes: u64) {
        self.query_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }
    
    /// Check current memory pressure and take action if needed
    pub fn check_memory_pressure(&self) {
        if !self.monitoring_active.load(Ordering::Relaxed) {
            return;
        }
        
        let current_memory = self.get_current_memory_usage();
        let pressure = self.calculate_pressure_level(current_memory);
        
        // Update stats
        {
            let mut stats = self.stats.lock();
            stats.buffer_pool_bytes = self.buffer_pool_bytes.load(Ordering::Relaxed);
            stats.message_bytes = self.message_bytes.load(Ordering::Relaxed);
            stats.query_bytes = self.query_bytes.load(Ordering::Relaxed);
            stats.pressure_level = pressure;
            
            if current_memory > stats.peak_memory_bytes {
                stats.peak_memory_bytes = current_memory;
            }
            
            stats.cleanup_events = self.cleanup_events.load(Ordering::Relaxed);
        }
        
        // Take action based on pressure level
        if pressure != MemoryPressure::Low && self.config.enable_auto_cleanup {
            self.trigger_cleanup(pressure);
        }
        
        // Log memory status if detailed monitoring is enabled
        if self.config.enable_detailed_monitoring {
            self.log_memory_status(current_memory, pressure);
        }
    }
    
    /// Get current total memory usage
    fn get_current_memory_usage(&self) -> u64 {
        self.buffer_pool_bytes.load(Ordering::Relaxed) +
        self.message_bytes.load(Ordering::Relaxed) +
        self.query_bytes.load(Ordering::Relaxed)
    }
    
    /// Calculate memory pressure level based on usage
    fn calculate_pressure_level(&self, current_memory: u64) -> MemoryPressure {
        let _current_mb = current_memory as f64 / (1024.0 * 1024.0);
        let _threshold_mb = self.config.memory_threshold as f64 / (1024.0 * 1024.0);
        let _high_threshold_mb = self.config.high_memory_threshold as f64 / (1024.0 * 1024.0);
        
        if current_memory >= self.config.high_memory_threshold as u64 * 2 {
            MemoryPressure::Critical
        } else if current_memory >= self.config.high_memory_threshold as u64 {
            MemoryPressure::High
        } else if current_memory >= self.config.memory_threshold as u64 {
            MemoryPressure::Medium
        } else {
            MemoryPressure::Low
        }
    }
    
    /// Trigger cleanup based on memory pressure
    fn trigger_cleanup(&self, pressure: MemoryPressure) {
        let mut last_check = self.last_check.lock();
        
        // Rate limit cleanup events
        if last_check.elapsed() < self.config.check_interval {
            return;
        }
        
        *last_check = Instant::now();
        
        // Increment cleanup counter
        self.cleanup_events.fetch_add(1, Ordering::Relaxed);
        
        // Update stats
        {
            let mut stats = self.stats.lock();
            stats.last_cleanup = Some(Instant::now());
        }
        
        match pressure {
            MemoryPressure::Medium => {
                debug!("Medium memory pressure detected, triggering gentle cleanup");
                self.execute_cleanup_callbacks(false);
            }
            MemoryPressure::High => {
                info!("High memory pressure detected, triggering cleanup");
                self.execute_cleanup_callbacks(false);
            }
            MemoryPressure::Critical => {
                warn!("Critical memory pressure detected, triggering aggressive cleanup");
                self.execute_cleanup_callbacks(true);
            }
            MemoryPressure::Low => {}
        }
    }
    
    /// Execute registered cleanup callbacks
    fn execute_cleanup_callbacks(&self, aggressive: bool) {
        let callbacks = self.cleanup_callbacks.lock();
        
        for callback in callbacks.iter() {
            callback();
        }
        
        if aggressive {
            // Additional aggressive cleanup can be implemented here
            // For example, forcing garbage collection or clearing more caches
        }
    }
    
    /// Log current memory status
    fn log_memory_status(&self, current_memory: u64, pressure: MemoryPressure) {
        let current_mb = current_memory as f64 / (1024.0 * 1024.0);
        let threshold_mb = self.config.memory_threshold as f64 / (1024.0 * 1024.0);
        
        match pressure {
            MemoryPressure::Low => {
                debug!("Memory usage: {:.1}MB (threshold: {:.1}MB)", current_mb, threshold_mb);
            }
            MemoryPressure::Medium => {
                info!("Memory usage: {:.1}MB - approaching threshold ({:.1}MB)", current_mb, threshold_mb);
            }
            MemoryPressure::High => {
                warn!("Memory usage: {:.1}MB - above threshold ({:.1}MB)", current_mb, threshold_mb);
            }
            MemoryPressure::Critical => {
                warn!("Memory usage: {:.1}MB - CRITICAL level (threshold: {:.1}MB)", current_mb, threshold_mb);
            }
        }
    }
    
    /// Register a cleanup callback function
    pub fn register_cleanup_callback<F>(&self, callback: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        let mut callbacks = self.cleanup_callbacks.lock();
        callbacks.push(Box::new(callback));
    }
    
    /// Get current memory statistics
    pub fn get_stats(&self) -> MemoryStats {
        let mut stats = self.stats.lock();
        
        // Update stats from atomic counters
        stats.buffer_pool_bytes = self.buffer_pool_bytes.load(Ordering::Relaxed);
        stats.message_bytes = self.message_bytes.load(Ordering::Relaxed);
        stats.query_bytes = self.query_bytes.load(Ordering::Relaxed);
        stats.cleanup_events = self.cleanup_events.load(Ordering::Relaxed);
        
        // Update peak memory
        let current_total = stats.total_bytes();
        if current_total > stats.peak_memory_bytes {
            stats.peak_memory_bytes = current_total;
        }
        
        // Update pressure level based on current memory usage
        stats.pressure_level = self.calculate_pressure_level(current_total);
        
        stats.clone()
    }
    
    /// Force a memory cleanup
    pub fn force_cleanup(&self) {
        // Bypass rate limiting for forced cleanup
        self.cleanup_events.fetch_add(1, Ordering::Relaxed);
        
        // Update stats
        {
            let mut stats = self.stats.lock();
            stats.last_cleanup = Some(Instant::now());
        }
        
        // Execute callbacks directly
        self.execute_cleanup_callbacks(false);
    }
    
    /// Enable or disable monitoring
    pub fn set_monitoring_active(&self, active: bool) {
        self.monitoring_active.store(active, Ordering::Relaxed);
    }
    
    /// Check if monitoring is active
    pub fn is_monitoring_active(&self) -> bool {
        self.monitoring_active.load(Ordering::Relaxed)
    }
    
    /// Reset all statistics
    pub fn reset_stats(&self) {
        self.buffer_pool_bytes.store(0, Ordering::Relaxed);
        self.message_bytes.store(0, Ordering::Relaxed);
        self.query_bytes.store(0, Ordering::Relaxed);
        self.cleanup_events.store(0, Ordering::Relaxed);
        
        let mut stats = self.stats.lock();
        *stats = MemoryStats::default();
    }
}

impl Default for MemoryMonitor {
    fn default() -> Self {
        Self::new()
    }
}

/// Global memory monitor instance
static GLOBAL_MEMORY_MONITOR: std::sync::OnceLock<MemoryMonitor> = std::sync::OnceLock::new();

/// Get the global memory monitor instance
pub fn global_memory_monitor() -> &'static MemoryMonitor {
    GLOBAL_MEMORY_MONITOR.get_or_init(|| {
        let config = MemoryMonitorConfig::from_env();
        MemoryMonitor::with_config(config)
    })
}

/// Record buffer allocation in the global monitor
pub fn record_buffer_allocation(bytes: u64) {
    global_memory_monitor().record_buffer_allocation(bytes);
}

/// Record buffer deallocation in the global monitor
pub fn record_buffer_deallocation(bytes: u64) {
    global_memory_monitor().record_buffer_deallocation(bytes);
}

/// Record message allocation in the global monitor
pub fn record_message_allocation(bytes: u64) {
    global_memory_monitor().record_message_allocation(bytes);
}

/// Record message deallocation in the global monitor
pub fn record_message_deallocation(bytes: u64) {
    global_memory_monitor().record_message_deallocation(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    
    #[test]
    fn test_memory_monitor_creation() {
        let monitor = MemoryMonitor::new();
        let stats = monitor.get_stats();
        
        assert_eq!(stats.total_bytes(), 0);
        assert_eq!(stats.pressure_level, MemoryPressure::Low);
    }
    
    #[test]
    fn test_memory_allocation_tracking() {
        let monitor = MemoryMonitor::new();
        
        monitor.record_buffer_allocation(1024);
        monitor.record_message_allocation(2048);
        monitor.record_query_allocation(512);
        
        let stats = monitor.get_stats();
        assert_eq!(stats.total_bytes(), 3584);
        assert_eq!(stats.buffer_pool_bytes, 1024);
        assert_eq!(stats.message_bytes, 2048);
        assert_eq!(stats.query_bytes, 512);
    }
    
    #[test]
    fn test_memory_deallocation() {
        let monitor = MemoryMonitor::new();
        monitor.reset_stats(); // Reset to avoid interference from other tests
        
        monitor.record_buffer_allocation(2048);
        monitor.record_buffer_deallocation(1024);
        
        let stats = monitor.get_stats();
        assert_eq!(stats.buffer_pool_bytes, 1024);
    }
    
    #[test]
    fn test_pressure_level_calculation() {
        let config = MemoryMonitorConfig {
            memory_threshold: 1000,
            high_memory_threshold: 2000,
            ..Default::default()
        };
        let monitor = MemoryMonitor::with_config(config);
        
        // Test low pressure
        monitor.record_buffer_allocation(500);
        monitor.check_memory_pressure();
        assert_eq!(monitor.get_stats().pressure_level, MemoryPressure::Low);
        
        // Test medium pressure
        monitor.record_buffer_allocation(600); // Total: 1100
        monitor.check_memory_pressure();
        assert_eq!(monitor.get_stats().pressure_level, MemoryPressure::Medium);
        
        // Test high pressure
        monitor.record_buffer_allocation(1000); // Total: 2100
        monitor.check_memory_pressure();
        assert_eq!(monitor.get_stats().pressure_level, MemoryPressure::High);
        
        // Test critical pressure
        monitor.record_buffer_allocation(2000); // Total: 4100
        monitor.check_memory_pressure();
        assert_eq!(monitor.get_stats().pressure_level, MemoryPressure::Critical);
    }
    
    #[test]
    fn test_cleanup_callbacks() {
        let monitor = MemoryMonitor::new();
        let cleanup_called = Arc::new(AtomicBool::new(false));
        let cleanup_called_clone = Arc::clone(&cleanup_called);
        
        monitor.register_cleanup_callback(move || {
            cleanup_called_clone.store(true, Ordering::Relaxed);
        });
        
        monitor.force_cleanup();
        
        // Give some time for the callback to execute
        std::thread::sleep(Duration::from_millis(10));
        
        assert!(cleanup_called.load(Ordering::Relaxed));
    }
    
    #[test]
    fn test_monitoring_enable_disable() {
        let monitor = MemoryMonitor::new();
        
        assert!(monitor.is_monitoring_active());
        
        monitor.set_monitoring_active(false);
        assert!(!monitor.is_monitoring_active());
        
        monitor.set_monitoring_active(true);
        assert!(monitor.is_monitoring_active());
    }
    
    #[test]
    fn test_stats_reset() {
        let monitor = MemoryMonitor::new();
        
        monitor.record_buffer_allocation(1024);
        monitor.record_message_allocation(2048);
        
        let stats_before = monitor.get_stats();
        assert!(stats_before.total_bytes() > 0);
        
        monitor.reset_stats();
        
        let stats_after = monitor.get_stats();
        assert_eq!(stats_after.total_bytes(), 0);
    }
    
    #[test]
    fn test_global_memory_monitor() {
        record_buffer_allocation(1024);
        record_message_allocation(2048);
        
        let stats = global_memory_monitor().get_stats();
        assert!(stats.total_bytes() >= 3072); // May include previous test data
    }
    
    #[test]
    fn test_config_from_env() {
        // Set environment variables
        unsafe {
            std::env::set_var("PGSQLITE_MEMORY_THRESHOLD", "1048576");
            std::env::set_var("PGSQLITE_AUTO_CLEANUP", "1");
        }
        
        let config = MemoryMonitorConfig::from_env();
        assert_eq!(config.memory_threshold, 1048576);
        assert!(config.enable_auto_cleanup);
        
        // Clean up
        unsafe {
            std::env::remove_var("PGSQLITE_MEMORY_THRESHOLD");
            std::env::remove_var("PGSQLITE_AUTO_CLEANUP");
        }
    }
}