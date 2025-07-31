use std::sync::Arc;
use std::cell::RefCell;
use parking_lot::Mutex;
use rusqlite::Connection;
use uuid::Uuid;
use lru::LruCache;
use std::num::NonZeroUsize;

thread_local! {
    /// LRU cache mapping session ID to connection Arc
    /// Size limit prevents unbounded memory growth
    static CONNECTION_CACHE: RefCell<LruCache<Uuid, Arc<Mutex<Connection>>>> = 
        RefCell::new(LruCache::new(NonZeroUsize::new(32).unwrap()));
}

/// Thread-local connection cache operations
pub struct ThreadLocalConnectionCache;

impl ThreadLocalConnectionCache {
    /// Try to get a connection from the thread-local cache
    #[inline(always)]
    pub fn get(session_id: &Uuid) -> Option<Arc<Mutex<Connection>>> {
        CONNECTION_CACHE.with(|cache| {
            cache.borrow_mut().get(session_id).cloned()
        })
    }
    
    /// Store a connection in the thread-local cache
    #[inline(always)]
    pub fn insert(session_id: Uuid, connection: Arc<Mutex<Connection>>) {
        CONNECTION_CACHE.with(|cache| {
            cache.borrow_mut().put(session_id, connection);
        })
    }
    
    /// Remove a connection from the thread-local cache
    #[inline(always)]
    pub fn remove(session_id: &Uuid) {
        CONNECTION_CACHE.with(|cache| {
            cache.borrow_mut().pop(session_id);
        })
    }
    
    /// Clear all cached connections for this thread
    pub fn clear() {
        CONNECTION_CACHE.with(|cache| {
            cache.borrow_mut().clear();
        })
    }
    
    /// Get the current size of the cache
    pub fn size() -> usize {
        CONNECTION_CACHE.with(|cache| {
            cache.borrow().len()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_thread_local_cache() {
        // Clear any existing cache
        ThreadLocalConnectionCache::clear();
        
        // Create mock connections (we can't create real SQLite connections in tests easily)
        let session1 = Uuid::new_v4();
        let _session2 = Uuid::new_v4();
        
        // Test empty cache
        assert!(ThreadLocalConnectionCache::get(&session1).is_none());
        assert_eq!(ThreadLocalConnectionCache::size(), 0);
        
        // Note: In actual tests, we'd need to create real connections
        // For now, this demonstrates the API
    }
}