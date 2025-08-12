use bytes::BytesMut;
use std::collections::VecDeque;
use std::sync::Arc;
use parking_lot::Mutex;
use std::time::{Duration, Instant};
use tracing::{debug, info};
#[cfg(not(test))]
use crate::protocol::memory_monitor::global_memory_monitor;

/// Configuration for buffer pool behavior
#[derive(Debug, Clone)]
pub struct BufferPoolConfig {
    /// Maximum number of buffers to keep in the pool
    pub max_pool_size: usize,
    /// Initial capacity for new buffers (in bytes)
    pub initial_buffer_capacity: usize,
    /// Maximum capacity a buffer can grow to before being discarded
    pub max_buffer_capacity: usize,
    /// Time after which unused buffers are cleaned up
    pub cleanup_interval: Duration,
    /// Enable buffer pool monitoring and statistics
    pub enable_monitoring: bool,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            max_pool_size: 50,
            initial_buffer_capacity: 4096, // 4KB
            max_buffer_capacity: 64 * 1024, // 64KB
            cleanup_interval: Duration::from_secs(30),
            enable_monitoring: std::env::var("PGSQLITE_BUFFER_MONITORING").unwrap_or_default() == "1",
        }
    }
}

impl BufferPoolConfig {
    /// Create configuration from environment variables
    pub fn from_env() -> Self {
        let mut config = Self::default();
        
        if let Ok(val) = std::env::var("PGSQLITE_BUFFER_POOL_SIZE")
            && let Ok(size) = val.parse::<usize>() {
                config.max_pool_size = size;
            }
        
        if let Ok(val) = std::env::var("PGSQLITE_BUFFER_INITIAL_CAPACITY")
            && let Ok(capacity) = val.parse::<usize>() {
                config.initial_buffer_capacity = capacity;
            }
        
        if let Ok(val) = std::env::var("PGSQLITE_BUFFER_MAX_CAPACITY")
            && let Ok(capacity) = val.parse::<usize>() {
                config.max_buffer_capacity = capacity;
            }
        
        config
    }
}

/// Statistics for buffer pool performance monitoring
#[derive(Debug, Clone, Default)]
pub struct BufferPoolStats {
    /// Total number of buffers allocated
    pub buffers_allocated: u64,
    /// Total number of buffers reused from pool
    pub buffers_reused: u64,
    /// Total number of buffers returned to pool
    pub buffers_returned: u64,
    /// Total number of buffers discarded (too large)
    pub buffers_discarded: u64,
    /// Current number of buffers in pool
    pub current_pool_size: usize,
    /// Peak number of buffers in pool
    pub peak_pool_size: usize,
    /// Total bytes allocated across all buffers
    pub total_bytes_allocated: u64,
    /// Total bytes reused from pool
    pub total_bytes_reused: u64,
    /// Last cleanup time
    pub last_cleanup: Option<Instant>,
}

impl BufferPoolStats {
    /// Calculate the reuse rate as a percentage
    pub fn reuse_rate(&self) -> f64 {
        if self.buffers_allocated + self.buffers_reused == 0 {
            0.0
        } else {
            (self.buffers_reused as f64 / (self.buffers_allocated + self.buffers_reused) as f64) * 100.0
        }
    }
    
    /// Calculate average buffer size
    pub fn average_buffer_size(&self) -> f64 {
        if self.buffers_allocated == 0 {
            0.0
        } else {
            self.total_bytes_allocated as f64 / self.buffers_allocated as f64
        }
    }
}

/// A pooled buffer with metadata
struct PooledBuffer {
    buffer: BytesMut,
    last_used: Instant,
    allocation_count: u32,
}

impl PooledBuffer {
    fn reset(&mut self) {
        self.buffer.clear();
        self.last_used = Instant::now();
        self.allocation_count += 1;
    }
    
    fn should_discard(&self, max_capacity: usize, cleanup_age: Duration) -> bool {
        // Discard if buffer has grown too large or is too old
        self.buffer.capacity() > max_capacity || 
        self.last_used.elapsed() > cleanup_age
    }
}

/// Thread-safe buffer pool for reusing BytesMut instances
pub struct BufferPool {
    inner: Arc<Mutex<BufferPoolInner>>,
    config: BufferPoolConfig,
}

struct BufferPoolInner {
    buffers: VecDeque<PooledBuffer>,
    stats: BufferPoolStats,
    last_cleanup: Instant,
}

impl BufferPool {
    /// Create a new buffer pool with default configuration
    pub fn new() -> Self {
        Self::with_config(BufferPoolConfig::default())
    }
    
    /// Create a new buffer pool with custom configuration
    pub fn with_config(config: BufferPoolConfig) -> Self {
        let inner = BufferPoolInner {
            buffers: VecDeque::new(),
            stats: BufferPoolStats::default(),
            last_cleanup: Instant::now(),
        };
        
        Self {
            inner: Arc::new(Mutex::new(inner)),
            config,
        }
    }
    
    /// Get a buffer from the pool or allocate a new one
    pub fn get_buffer(&self) -> PooledBytesMut {
        let mut inner = self.inner.lock();
        
        // Try to reuse a buffer from the pool
        if let Some(mut pooled_buffer) = inner.buffers.pop_front() {
            pooled_buffer.reset();
            
            inner.stats.buffers_reused += 1;
            inner.stats.total_bytes_reused += pooled_buffer.buffer.capacity() as u64;
            
            if self.config.enable_monitoring {
                debug!("Reused buffer from pool: capacity={}B, pool_size={}", 
                       pooled_buffer.buffer.capacity(), inner.buffers.len());
            }
            
            PooledBytesMut::new(pooled_buffer.buffer, Arc::clone(&self.inner), self.config.clone())
        } else {
            // Allocate a new buffer
            let buffer = BytesMut::with_capacity(self.config.initial_buffer_capacity);
            
            inner.stats.buffers_allocated += 1;
            inner.stats.total_bytes_allocated += buffer.capacity() as u64;
            
            // Record allocation in global memory monitor (skip in tests)
            #[cfg(not(test))]
            global_memory_monitor().record_buffer_allocation(buffer.capacity() as u64);
            
            if self.config.enable_monitoring {
                debug!("Allocated new buffer: capacity={}B", buffer.capacity());
            }
            
            PooledBytesMut::new(buffer, Arc::clone(&self.inner), self.config.clone())
        }
    }
    
    /// Return a buffer to the pool for reuse
    fn return_buffer(&self, buffer: BytesMut) {
        let mut inner = self.inner.lock();
        
        // Check if we should discard this buffer
        if buffer.capacity() > self.config.max_buffer_capacity {
            inner.stats.buffers_discarded += 1;
            
            // Record deallocation in global memory monitor (skip in tests)
            #[cfg(not(test))]
            global_memory_monitor().record_buffer_deallocation(buffer.capacity() as u64);
            
            if self.config.enable_monitoring {
                debug!("Discarded oversized buffer: capacity={}B > max={}B", 
                       buffer.capacity(), self.config.max_buffer_capacity);
            }
            return;
        }
        
        // Check if pool is full
        if inner.buffers.len() >= self.config.max_pool_size {
            inner.stats.buffers_discarded += 1;
            
            // Record deallocation in global memory monitor (skip in tests)
            #[cfg(not(test))]
            global_memory_monitor().record_buffer_deallocation(buffer.capacity() as u64);
            
            if self.config.enable_monitoring {
                debug!("Discarded buffer: pool full (size={})", inner.buffers.len());
            }
            return;
        }
        
        // Return buffer to pool
        let pooled_buffer = PooledBuffer {
            buffer,
            last_used: Instant::now(),
            allocation_count: 1,
        };
        
        let buffer_capacity = pooled_buffer.buffer.capacity();
        
        inner.buffers.push_back(pooled_buffer);
        inner.stats.buffers_returned += 1;
        inner.stats.current_pool_size = inner.buffers.len();
        
        if inner.stats.current_pool_size > inner.stats.peak_pool_size {
            inner.stats.peak_pool_size = inner.stats.current_pool_size;
        }
        
        if self.config.enable_monitoring {
            debug!("Returned buffer to pool: capacity={}B, pool_size={}", 
                   buffer_capacity, inner.buffers.len());
        }
        
        // Perform cleanup if needed
        if inner.last_cleanup.elapsed() > self.config.cleanup_interval {
            self.cleanup_old_buffers(&mut inner);
        }
    }
    
    /// Clean up old or oversized buffers from the pool
    fn cleanup_old_buffers(&self, inner: &mut BufferPoolInner) {
        let old_size = inner.buffers.len();
        let cleanup_age = self.config.cleanup_interval * 2; // Keep buffers for 2x cleanup interval
        
        inner.buffers.retain(|pooled_buffer| {
            !pooled_buffer.should_discard(self.config.max_buffer_capacity, cleanup_age)
        });
        
        let cleaned_count = old_size - inner.buffers.len();
        if cleaned_count > 0 {
            inner.stats.buffers_discarded += cleaned_count as u64;
            inner.stats.current_pool_size = inner.buffers.len();
            
            if self.config.enable_monitoring {
                info!("Cleaned up {} old buffers from pool, new size: {}", 
                      cleaned_count, inner.buffers.len());
            }
        }
        
        inner.last_cleanup = Instant::now();
        inner.stats.last_cleanup = Some(inner.last_cleanup);
    }
    
    /// Get current buffer pool statistics
    pub fn get_stats(&self) -> BufferPoolStats {
        let inner = self.inner.lock();
        let mut stats = inner.stats.clone();
        stats.current_pool_size = inner.buffers.len();
        stats
    }
    
    /// Force cleanup of old buffers
    pub fn cleanup(&self) {
        let mut inner = self.inner.lock();
        self.cleanup_old_buffers(&mut inner);
    }
    
    /// Clear all buffers from the pool
    pub fn clear(&self) {
        let mut inner = self.inner.lock();
        let cleared_count = inner.buffers.len();
        inner.buffers.clear();
        inner.stats.buffers_discarded += cleared_count as u64;
        inner.stats.current_pool_size = 0;
        
        if self.config.enable_monitoring {
            info!("Cleared all {} buffers from pool", cleared_count);
        }
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new()
    }
}

/// A BytesMut wrapper that automatically returns to the pool when dropped
pub struct PooledBytesMut {
    buffer: Option<BytesMut>,
    pool: Arc<Mutex<BufferPoolInner>>,
    config: BufferPoolConfig,
}

impl PooledBytesMut {
    fn new(buffer: BytesMut, pool: Arc<Mutex<BufferPoolInner>>, config: BufferPoolConfig) -> Self {
        Self {
            buffer: Some(buffer),
            pool,
            config,
        }
    }
    
    /// Get a mutable reference to the underlying buffer
    pub fn buffer_mut(&mut self) -> &mut BytesMut {
        self.buffer.as_mut().expect("Buffer should be available")
    }
    
    /// Get an immutable reference to the underlying buffer
    pub fn buffer(&self) -> &BytesMut {
        self.buffer.as_ref().expect("Buffer should be available")
    }
    
    /// Take ownership of the buffer, preventing return to pool
    pub fn take(mut self) -> BytesMut {
        self.buffer.take().expect("Buffer should be available")
    }
    
    /// Get the length of the buffer
    pub fn len(&self) -> usize {
        self.buffer().len()
    }
    
    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.buffer().is_empty()
    }
    
    /// Get the capacity of the buffer
    pub fn capacity(&self) -> usize {
        self.buffer().capacity()
    }
    
    /// Clear the buffer content
    pub fn clear(&mut self) {
        self.buffer_mut().clear();
    }
}

impl Drop for PooledBytesMut {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            // Return buffer to pool through the BufferPool method
            // We need to reconstruct the BufferPool temporarily to call return_buffer
            let pool = BufferPool {
                inner: Arc::clone(&self.pool),
                config: self.config.clone(),
            };
            pool.return_buffer(buffer);
        }
    }
}

/// Global buffer pool instance for shared use across the application
static GLOBAL_BUFFER_POOL: std::sync::OnceLock<BufferPool> = std::sync::OnceLock::new();

/// Get the global buffer pool instance
pub fn global_buffer_pool() -> &'static BufferPool {
    GLOBAL_BUFFER_POOL.get_or_init(|| {
        let config = BufferPoolConfig::from_env();
        BufferPool::with_config(config)
    })
}

/// Get a buffer from the global pool
pub fn get_pooled_buffer() -> PooledBytesMut {
    global_buffer_pool().get_buffer()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;
    
    #[test]
    fn test_buffer_pool_creation() {
        let pool = BufferPool::new();
        let stats = pool.get_stats();
        
        assert_eq!(stats.buffers_allocated, 0);
        assert_eq!(stats.buffers_reused, 0);
        assert_eq!(stats.current_pool_size, 0);
    }
    
    #[test]
    fn test_buffer_allocation_and_reuse() {
        let pool = BufferPool::new();
        
        // Allocate a buffer
        {
            let mut buffer = pool.get_buffer();
            buffer.buffer_mut().extend_from_slice(b"test data");
            assert_eq!(buffer.len(), 9);
        } // Buffer should be returned to pool here
        
        let stats = pool.get_stats();
        assert_eq!(stats.buffers_allocated, 1);
        assert_eq!(stats.buffers_returned, 1);
        assert_eq!(stats.current_pool_size, 1);
        
        // Reuse the buffer
        {
            let mut buffer = pool.get_buffer();
            assert_eq!(buffer.len(), 0); // Should be cleared
            buffer.buffer_mut().extend_from_slice(b"new data");
        }
        
        let stats = pool.get_stats();
        assert_eq!(stats.buffers_allocated, 1);
        assert_eq!(stats.buffers_reused, 1);
        assert_eq!(stats.reuse_rate(), 50.0); // 1 reused out of 2 total gets
    }
    
    #[test]
    fn test_buffer_pool_size_limit() {
        let config = BufferPoolConfig {
            max_pool_size: 2,
            ..Default::default()
        };
        let pool = BufferPool::with_config(config);
        
        // Allocate more buffers than pool size
        {
            let _b1 = pool.get_buffer();
            let _b2 = pool.get_buffer();
            let _b3 = pool.get_buffer();
        } // All buffers returned
        
        let stats = pool.get_stats();
        assert_eq!(stats.current_pool_size, 2); // Only 2 should be kept
        assert_eq!(stats.buffers_discarded, 1); // 1 should be discarded
    }
    
    #[test]
    fn test_buffer_capacity_limit() {
        let config = BufferPoolConfig {
            max_buffer_capacity: 1024,
            initial_buffer_capacity: 512,
            ..Default::default()
        };
        let pool = BufferPool::with_config(config);
        
        // Allocate and grow a buffer beyond limit
        {
            let mut buffer = pool.get_buffer();
            buffer.buffer_mut().resize(2048, 0); // Grow beyond limit
        }
        
        let stats = pool.get_stats();
        assert_eq!(stats.buffers_discarded, 1); // Should be discarded
        assert_eq!(stats.current_pool_size, 0);
    }
    
    #[test]
    fn test_buffer_cleanup() {
        let config = BufferPoolConfig {
            cleanup_interval: Duration::from_millis(50),
            ..Default::default()
        };
        let pool = BufferPool::with_config(config);
        
        // Add some buffers
        {
            let _b1 = pool.get_buffer();
            let _b2 = pool.get_buffer();
        }
        
        // Wait for cleanup interval
        thread::sleep(Duration::from_millis(100));
        
        // Force cleanup
        pool.cleanup();
        
        let stats = pool.get_stats();
        assert!(stats.last_cleanup.is_some());
    }
    
    #[test]
    fn test_pooled_bytes_mut_operations() {
        let pool = BufferPool::new();
        let mut buffer = pool.get_buffer();
        
        assert!(buffer.is_empty());
        assert!(buffer.capacity() > 0);
        
        buffer.buffer_mut().extend_from_slice(b"hello");
        assert_eq!(buffer.len(), 5);
        assert!(!buffer.is_empty());
        
        buffer.clear();
        assert!(buffer.is_empty());
    }
    
    #[test]
    fn test_global_buffer_pool() {
        let buffer1 = get_pooled_buffer();
        let buffer2 = get_pooled_buffer();
        
        // Both should come from the same global pool
        assert!(buffer1.capacity() > 0);
        assert!(buffer2.capacity() > 0);
        
        drop(buffer1);
        drop(buffer2);
        
        let stats = global_buffer_pool().get_stats();
        assert!(stats.buffers_allocated >= 2);
    }
    
    #[test]
    fn test_config_from_env() {
        // Set environment variables
        unsafe {
            std::env::set_var("PGSQLITE_BUFFER_POOL_SIZE", "100");
            std::env::set_var("PGSQLITE_BUFFER_INITIAL_CAPACITY", "8192");
        }
        
        let config = BufferPoolConfig::from_env();
        assert_eq!(config.max_pool_size, 100);
        assert_eq!(config.initial_buffer_capacity, 8192);
        
        // Clean up
        unsafe {
            std::env::remove_var("PGSQLITE_BUFFER_POOL_SIZE");
            std::env::remove_var("PGSQLITE_BUFFER_INITIAL_CAPACITY");
        }
    }
}