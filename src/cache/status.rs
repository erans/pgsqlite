use crate::session::GLOBAL_QUERY_CACHE;
use crate::session::db_handler::DbRows;

/// Cache status information
#[derive(Debug, Clone)]
pub struct CacheStatus {
    pub total_queries: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub hit_rate: f64,
    pub evictions: u64,
    pub cache_size: usize,
    pub cache_capacity: usize,
}

/// Get current cache status
pub fn get_cache_status() -> CacheStatus {
    let metrics = GLOBAL_QUERY_CACHE.get_metrics();
    let (cache_size, _) = GLOBAL_QUERY_CACHE.stats();
    
    let hit_rate = if metrics.total_queries > 0 {
        (metrics.cache_hits as f64 / metrics.total_queries as f64) * 100.0
    } else {
        0.0
    };
    
    CacheStatus {
        total_queries: metrics.total_queries,
        cache_hits: metrics.cache_hits,
        cache_misses: metrics.cache_misses,
        hit_rate,
        evictions: metrics.evictions,
        cache_size,
        cache_capacity: 1000, // From GLOBAL_QUERY_CACHE initialization
    }
}

/// Format cache status as a PostgreSQL result set
pub fn format_cache_status_as_table() -> (Vec<String>, DbRows) {
    let status = get_cache_status();
    
    let columns = vec![
        "metric".to_string(),
        "value".to_string(),
    ];
    
    let rows = vec![
        vec![
            Some(b"total_queries".to_vec()),
            Some(status.total_queries.to_string().into_bytes()),
        ],
        vec![
            Some(b"cache_hits".to_vec()),
            Some(status.cache_hits.to_string().into_bytes()),
        ],
        vec![
            Some(b"cache_misses".to_vec()),
            Some(status.cache_misses.to_string().into_bytes()),
        ],
        vec![
            Some(b"hit_rate_percent".to_vec()),
            Some(format!("{:.1}", status.hit_rate).into_bytes()),
        ],
        vec![
            Some(b"evictions".to_vec()),
            Some(status.evictions.to_string().into_bytes()),
        ],
        vec![
            Some(b"cache_size".to_vec()),
            Some(status.cache_size.to_string().into_bytes()),
        ],
        vec![
            Some(b"cache_capacity".to_vec()),
            Some(status.cache_capacity.to_string().into_bytes()),
        ],
    ];
    
    (columns, rows)
}

/// Log cache status to tracing
pub fn log_cache_status() {
    let status = get_cache_status();
    
    tracing::info!(
        "Query Cache Status - Total: {}, Hits: {} ({:.1}%), Misses: {}, Evictions: {}, Size: {}/{}",
        status.total_queries,
        status.cache_hits,
        status.hit_rate,
        status.cache_misses,
        status.evictions,
        status.cache_size,
        status.cache_capacity
    );
}

/// Get top cached queries by access count
pub fn get_top_cached_queries(_limit: usize) -> Vec<(String, u64)> {
    // This would require modifying the cache to expose internal data
    // For now, return empty - this is a placeholder for future enhancement
    Vec::new()
}