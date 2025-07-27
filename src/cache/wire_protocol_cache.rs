use std::sync::Arc;
use parking_lot::RwLock;
use lru::LruCache;
use std::num::NonZeroUsize;
use crate::protocol::FieldDescription;

/// Cached wire protocol response
#[derive(Clone)]
pub struct CachedWireResponse {
    /// Row description message (field metadata)
    pub row_description: Vec<FieldDescription>,
    /// Pre-encoded data rows in wire format
    pub encoded_rows: Vec<Vec<u8>>,
    /// Number of rows
    pub row_count: usize,
}

/// Wire protocol response cache
pub struct WireProtocolCache {
    cache: RwLock<LruCache<String, Arc<CachedWireResponse>>>,
}

impl WireProtocolCache {
    /// Create a new wire protocol cache with the specified capacity
    pub fn new(capacity: usize) -> Self {
        let capacity = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(100).unwrap());
        Self {
            cache: RwLock::new(LruCache::new(capacity)),
        }
    }
    
    /// Get a cached response
    pub fn get(&self, query: &str) -> Option<Arc<CachedWireResponse>> {
        let mut cache = self.cache.write();
        cache.get(query).cloned()
    }
    
    /// Store a response in the cache
    pub fn put(&self, query: String, response: CachedWireResponse) {
        let mut cache = self.cache.write();
        cache.put(query, Arc::new(response));
    }
    
    /// Clear the cache
    pub fn clear(&self) {
        let mut cache = self.cache.write();
        cache.clear();
    }
    
    /// Get cache statistics
    pub fn stats(&self) -> (usize, usize) {
        let cache = self.cache.read();
        (cache.len(), cache.cap().get())
    }
}

/// Global wire protocol cache instance
use once_cell::sync::Lazy;
pub static WIRE_PROTOCOL_CACHE: Lazy<WireProtocolCache> = Lazy::new(|| {
    WireProtocolCache::new(1000)
});

/// Check if a query is suitable for wire protocol caching
pub fn is_cacheable_for_wire_protocol(query: &str) -> bool {
    // Simple heuristics for now - cache simple SELECT queries
    let query_lower = query.to_lowercase();
    
    // Must be a SELECT query
    if !query_lower.starts_with("select") {
        return false;
    }
    
    // Skip queries with parameters (for now)
    if query.contains('$') {
        return false;
    }
    
    // Skip queries that might return different results
    if query_lower.contains("random()") || 
       query_lower.contains("now()") ||
       query_lower.contains("current_") ||
       query_lower.contains("gen_random_uuid") ||
       query_lower.contains("uuid_generate_v4") {
        return false;
    }
    
    true
}

/// Encode a data row for wire protocol
pub fn encode_data_row(row: &[Option<Vec<u8>>]) -> Vec<u8> {
    use bytes::{BytesMut, BufMut};
    
    let mut buf = BytesMut::new();
    
    // Message type 'D' for DataRow
    buf.put_u8(b'D');
    
    // Placeholder for message length (we'll fill this in later)
    let len_pos = buf.len();
    buf.put_i32(0);
    
    // Number of columns
    buf.put_i16(row.len() as i16);
    
    // Column data
    for cell in row {
        if let Some(data) = cell {
            // Length of data
            buf.put_i32(data.len() as i32);
            // Actual data
            buf.extend_from_slice(data);
        } else {
            // NULL value
            buf.put_i32(-1);
        }
    }
    
    // Fill in the message length (excluding the message type byte)
    let msg_len = (buf.len() - len_pos - 4) as i32 + 4;
    buf[len_pos..len_pos + 4].copy_from_slice(&msg_len.to_be_bytes());
    
    buf.to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_wire_protocol_cache() {
        let cache = WireProtocolCache::new(10);
        
        let response = CachedWireResponse {
            row_description: vec![],
            encoded_rows: vec![],
            row_count: 0,
        };
        
        cache.put("SELECT 1".to_string(), response.clone());
        assert!(cache.get("SELECT 1").is_some());
        assert!(cache.get("SELECT 2").is_none());
    }
    
    #[test]
    fn test_is_cacheable() {
        assert!(is_cacheable_for_wire_protocol("SELECT * FROM users"));
        assert!(is_cacheable_for_wire_protocol("select id, name from products"));
        assert!(!is_cacheable_for_wire_protocol("INSERT INTO users VALUES (1)"));
        assert!(!is_cacheable_for_wire_protocol("SELECT * FROM users WHERE id = $1"));
        assert!(!is_cacheable_for_wire_protocol("SELECT NOW()"));
    }
    
    #[test]
    fn test_encode_data_row() {
        let row = vec![
            Some(b"hello".to_vec()),
            None,
            Some(b"world".to_vec()),
        ];
        
        let encoded = encode_data_row(&row);
        
        // Check message type
        assert_eq!(encoded[0], b'D');
        
        // Check number of columns (at offset 5)
        let num_cols = i16::from_be_bytes([encoded[5], encoded[6]]);
        assert_eq!(num_cols, 3);
    }
}