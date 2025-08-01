use std::sync::Arc;
use std::cell::RefCell;
use parking_lot::Mutex;
use rusqlite::Connection;
use uuid::Uuid;
use lru::LruCache;
use std::num::NonZeroUsize;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::thread::ThreadId;

thread_local! {
    /// LRU cache mapping session ID to connection Arc
    /// Size limit prevents unbounded memory growth
    static CONNECTION_CACHE: RefCell<LruCache<Uuid, Arc<Mutex<Connection>>>> = 
        RefCell::new(LruCache::new(NonZeroUsize::new(32).unwrap()));
}

/// Thread-to-session affinity map for fastest lookups
static THREAD_AFFINITY: Lazy<DashMap<ThreadId, Uuid>> = Lazy::new(DashMap::new);

/// Pre-warmed connection pool for cross-thread sharing
static CONNECTION_POOL: Lazy<DashMap<Uuid, Arc<Mutex<Connection>>>> = Lazy::new(DashMap::new);

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
    
    /// Get connection with thread affinity for optimal performance
    #[inline(always)]
    pub fn get_with_affinity(session_id: &Uuid) -> Option<Arc<Mutex<Connection>>> {
        let thread_id = std::thread::current().id();
        
        // Fast path: check if this thread has affinity with the session
        if let Some(affinity_session) = THREAD_AFFINITY.get(&thread_id) {
            if *affinity_session == *session_id {
                // Thread affinity match - use thread-local cache directly
                return CONNECTION_CACHE.with(|cache| {
                    cache.borrow_mut().get(session_id).cloned()
                });
            }
        }
        
        // Try normal cache lookup
        CONNECTION_CACHE.with(|cache| {
            let mut cache_ref = cache.borrow_mut();
            if let Some(conn) = cache_ref.get(session_id).cloned() {
                // Establish affinity for future requests on this thread
                THREAD_AFFINITY.insert(thread_id, *session_id);
                Some(conn)
            } else {
                // Check shared pool as last resort
                if let Some(conn) = CONNECTION_POOL.get(session_id).map(|c| c.clone()) {
                    // Cache it locally and establish affinity
                    cache_ref.put(*session_id, conn.clone());
                    THREAD_AFFINITY.insert(thread_id, *session_id);
                    Some(conn)
                } else {
                    None
                }
            }
        })
    }
    
    /// Store a connection in the thread-local cache
    #[inline(always)]
    pub fn insert(session_id: Uuid, connection: Arc<Mutex<Connection>>) {
        CONNECTION_CACHE.with(|cache| {
            cache.borrow_mut().put(session_id, connection);
        })
    }
    
    /// Pre-warm connection cache with thread affinity
    pub fn pre_warm(session_id: Uuid, connection: Arc<Mutex<Connection>>) {
        let thread_id = std::thread::current().id();
        
        // Insert into current thread's cache
        CONNECTION_CACHE.with(|cache| {
            cache.borrow_mut().put(session_id, connection.clone());
        });
        
        // Establish thread affinity
        THREAD_AFFINITY.insert(thread_id, session_id);
        
        // Also add to shared pool for other threads
        CONNECTION_POOL.insert(session_id, connection);
    }
    
    /// Remove a connection from the thread-local cache
    #[inline(always)]
    pub fn remove(session_id: &Uuid) {
        let thread_id = std::thread::current().id();
        
        // Remove from thread-local cache
        CONNECTION_CACHE.with(|cache| {
            cache.borrow_mut().pop(session_id);
        });
        
        // Clear thread affinity if it matches
        if let Some(affinity_session) = THREAD_AFFINITY.get(&thread_id) {
            if *affinity_session == *session_id {
                THREAD_AFFINITY.remove(&thread_id);
            }
        }
        
        // Remove from shared pool
        CONNECTION_POOL.remove(session_id);
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