use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use rusqlite::Connection;
use crate::types::PgType;
use crate::cache::schema::{TableSchema, SchemaCache};
use tracing::{debug, info, warn};

/// Lazy schema loader that defers schema loading until actually needed
pub struct LazySchemaLoader {
    /// Cache of loaded schemas
    cache: Arc<RwLock<HashMap<String, CachedSchema>>>,
    /// Set of tables that are currently being loaded (to prevent duplicate loading)
    loading_tables: Arc<RwLock<HashSet<String>>>,
    /// Schema cache TTL
    ttl: Duration,
    /// Loader statistics
    stats: Arc<RwLock<LoaderStats>>,
}

#[derive(Debug, Clone)]
struct CachedSchema {
    schema: TableSchema,
    loaded_at: Instant,
    access_count: u64,
    last_accessed: Instant,
}

#[derive(Debug, Default, Clone)]
pub struct LoaderStats {
    cache_hits: u64,
    cache_misses: u64,
    schemas_loaded: u64,
    preload_hits: u64,
    total_load_time_ms: u64,
}

impl LazySchemaLoader {
    pub fn new(ttl_seconds: u64) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            loading_tables: Arc::new(RwLock::new(HashSet::new())),
            ttl: Duration::from_secs(ttl_seconds),
            stats: Arc::new(RwLock::new(LoaderStats::default())),
        }
    }

    /// Get schema for a table, loading it lazily if not cached
    pub fn get_schema(&self, conn: &Connection, table_name: &str) -> Result<Option<TableSchema>, rusqlite::Error> {
        let start_time = Instant::now();
        
        // Check cache first
        {
            let cache = self.cache.read().unwrap();
            if let Some(cached) = cache.get(table_name)
                && cached.loaded_at.elapsed() < self.ttl {
                    // Update access statistics
                    let schema_clone = cached.schema.clone();
                    drop(cache);
                    self.update_access_stats(table_name);
                    self.stats.write().unwrap().cache_hits += 1;
                    debug!("Schema cache hit for table: {}", table_name);
                    return Ok(Some(schema_clone));
                }
        }

        // Cache miss - need to load
        self.stats.write().unwrap().cache_misses += 1;
        debug!("Schema cache miss for table: {}", table_name);
        
        // Check if already loading (prevent duplicate work)
        {
            let loading = self.loading_tables.read().unwrap();
            if loading.contains(table_name) {
                // Wait for other thread to finish loading
                drop(loading);
                return self.wait_for_loading(table_name);
            }
        }

        // Mark as loading
        self.loading_tables.write().unwrap().insert(table_name.to_string());
        
        // Load the schema
        let schema_result = self.load_schema(conn, table_name);
        
        // Remove from loading set
        self.loading_tables.write().unwrap().remove(table_name);
        
        match schema_result {
            Ok(Some(schema)) => {
                // Cache the result
                let cached = CachedSchema {
                    schema: schema.clone(),
                    loaded_at: Instant::now(),
                    access_count: 1,
                    last_accessed: Instant::now(),
                };
                
                self.cache.write().unwrap().insert(table_name.to_string(), cached);
                
                // Update statistics
                let mut stats = self.stats.write().unwrap();
                stats.schemas_loaded += 1;
                stats.total_load_time_ms += start_time.elapsed().as_millis() as u64;
                
                debug!("Schema loaded for table: {} in {}ms", table_name, start_time.elapsed().as_millis());
                Ok(Some(schema))
            }
            Ok(None) => {
                debug!("Table not found: {}", table_name);
                Ok(None)
            }
            Err(e) => {
                warn!("Failed to load schema for table {}: {}", table_name, e);
                Err(e)
            }
        }
    }

    /// Load schema from database
    fn load_schema(&self, conn: &Connection, table_name: &str) -> Result<Option<TableSchema>, rusqlite::Error> {
        // Check if table exists
        let table_exists: bool = conn.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1")?
            .query_row([table_name], |_| Ok(true))
            .unwrap_or(false);
            
        if !table_exists {
            return Ok(None);
        }

        // Get column information from PRAGMA table_info
        let pragma_sql = format!("PRAGMA table_info({table_name})");
        let mut stmt = conn.prepare(&pragma_sql)?;
        let mut columns = Vec::new();
        
        let column_rows = stmt.query_map([], |row| {
            let column_name: String = row.get(1)?;
            let sqlite_type: String = row.get(2)?;
            Ok((column_name, sqlite_type))
        })?;
        
        for column_result in column_rows {
            let (column_name, sqlite_type) = column_result?;
            
            // Get PostgreSQL type information from __pgsqlite_schema if available
            let (pg_type, pg_oid) = self.get_pg_type_info(conn, table_name, &column_name)
                .unwrap_or_else(|| self.infer_pg_type(&sqlite_type));
            
            columns.push((column_name, pg_type, sqlite_type, pg_oid));
        }

        Ok(Some(SchemaCache::build_table_schema(columns)))
    }

    /// Get PostgreSQL type information from schema metadata
    fn get_pg_type_info(&self, conn: &Connection, table_name: &str, column_name: &str) -> Option<(String, i32)> {
        let mut stmt = conn.prepare("SELECT pg_type, pg_type_oid FROM __pgsqlite_schema WHERE table_name=?1 AND column_name=?2").ok()?;
        stmt.query_row([table_name, column_name], |row| {
            let pg_type: String = row.get(0)?;
            let pg_oid: i32 = row.get(1)?;
            Ok((pg_type, pg_oid))
        }).ok()
    }

    /// Infer PostgreSQL type from SQLite type
    fn infer_pg_type(&self, sqlite_type: &str) -> (String, i32) {
        let upper_type = sqlite_type.to_uppercase();
        match upper_type.as_str() {
            s if s.contains("INT") => ("INT4".to_string(), PgType::Int4.to_oid()),
            s if s.contains("REAL") || s.contains("FLOAT") => ("FLOAT8".to_string(), PgType::Float8.to_oid()),
            s if s.contains("TEXT") => ("TEXT".to_string(), PgType::Text.to_oid()),
            s if s.contains("BLOB") => ("BYTEA".to_string(), PgType::Bytea.to_oid()),
            s if s.contains("NUMERIC") || s.contains("DECIMAL") => ("NUMERIC".to_string(), PgType::Numeric.to_oid()),
            s if s.contains("BOOL") => ("BOOL".to_string(), PgType::Bool.to_oid()),
            _ => ("TEXT".to_string(), PgType::Text.to_oid()),
        }
    }

    /// Wait for another thread to finish loading a schema
    fn wait_for_loading(&self, table_name: &str) -> Result<Option<TableSchema>, rusqlite::Error> {
        // Simple polling approach (could be improved with condition variables)
        for _ in 0..100 {
            std::thread::sleep(Duration::from_millis(10));
            
            let cache = self.cache.read().unwrap();
            if let Some(cached) = cache.get(table_name)
                && cached.loaded_at.elapsed() < self.ttl {
                    return Ok(Some(cached.schema.clone()));
                }
            
            let loading = self.loading_tables.read().unwrap();
            if !loading.contains(table_name) {
                break;
            }
        }
        
        // If we get here, loading failed or timed out
        Ok(None)
    }

    /// Update access statistics for a cached schema
    fn update_access_stats(&self, table_name: &str) {
        let mut cache = self.cache.write().unwrap();
        if let Some(cached) = cache.get_mut(table_name) {
            cached.access_count += 1;
            cached.last_accessed = Instant::now();
        }
    }

    /// Preload schemas for a set of tables (useful for JOIN queries)
    pub fn preload_schemas(&self, conn: &Connection, table_names: &[String]) -> Result<(), rusqlite::Error> {
        info!("Preloading schemas for {} tables", table_names.len());
        
        let mut tables_to_load = Vec::new();
        {
            let cache = self.cache.read().unwrap();
            for table_name in table_names {
                if let Some(cached) = cache.get(table_name)
                    && cached.loaded_at.elapsed() < self.ttl {
                        self.stats.write().unwrap().preload_hits += 1;
                        continue; // Already cached
                    }
                tables_to_load.push(table_name.clone());
            }
        }

        // Load schemas in parallel (simplified approach)
        for table_name in tables_to_load {
            self.get_schema(conn, &table_name)?;
        }

        Ok(())
    }

    /// Get frequently accessed tables for optimization
    pub fn get_hot_tables(&self, min_access_count: u64) -> Vec<String> {
        let cache = self.cache.read().unwrap();
        cache.iter()
            .filter(|(_, cached)| cached.access_count >= min_access_count)
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Clear expired cache entries
    pub fn cleanup_cache(&self) {
        let mut cache = self.cache.write().unwrap();
        let initial_size = cache.len();
        
        cache.retain(|_, cached| cached.loaded_at.elapsed() < self.ttl);
        
        let removed = initial_size - cache.len();
        if removed > 0 {
            info!("Cleaned up {} expired schema cache entries", removed);
        }
    }

    /// Get loader statistics
    pub fn get_stats(&self) -> LoaderStats {
        let stats = self.stats.read().unwrap();
        stats.clone()
    }

    /// Reset statistics
    pub fn reset_stats(&self) {
        *self.stats.write().unwrap() = LoaderStats::default();
    }

    /// Get cache hit rate
    pub fn get_cache_hit_rate(&self) -> f64 {
        let stats = self.stats.read().unwrap();
        let total_requests = stats.cache_hits + stats.cache_misses;
        if total_requests > 0 {
            stats.cache_hits as f64 / total_requests as f64
        } else {
            0.0
        }
    }

    /// Get average load time
    pub fn get_average_load_time_ms(&self) -> f64 {
        let stats = self.stats.read().unwrap();
        if stats.schemas_loaded > 0 {
            stats.total_load_time_ms as f64 / stats.schemas_loaded as f64
        } else {
            0.0
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    
    #[test]
    fn test_lazy_schema_loading() {
        let conn = Connection::open_in_memory().unwrap();
        
        // Create a test table
        conn.execute("CREATE TABLE test_table (id INTEGER, name TEXT)", []).unwrap();
        
        let loader = LazySchemaLoader::new(300);
        
        // First access should load from database
        let schema1 = loader.get_schema(&conn, "test_table").unwrap().unwrap();
        assert_eq!(schema1.columns.len(), 2);
        
        // Second access should hit cache
        let schema2 = loader.get_schema(&conn, "test_table").unwrap().unwrap();
        assert_eq!(schema1.columns.len(), schema2.columns.len());
        
        // Check statistics
        let stats = loader.get_stats();
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.cache_misses, 1);
        assert_eq!(stats.schemas_loaded, 1);
        assert!(loader.get_cache_hit_rate() > 0.0);
    }
    
    #[test]
    fn test_nonexistent_table() {
        let conn = Connection::open_in_memory().unwrap();
        let loader = LazySchemaLoader::new(300);
        
        let schema = loader.get_schema(&conn, "nonexistent_table").unwrap();
        assert!(schema.is_none());
    }
    
    #[test]
    fn test_preloading() {
        let conn = Connection::open_in_memory().unwrap();
        
        // Create test tables
        conn.execute("CREATE TABLE table1 (id INTEGER)", []).unwrap();
        conn.execute("CREATE TABLE table2 (id INTEGER)", []).unwrap();
        
        let loader = LazySchemaLoader::new(300);
        
        // First, load schemas individually to generate cache misses
        let _ = loader.get_schema(&conn, "table1").unwrap().unwrap();
        let _ = loader.get_schema(&conn, "table2").unwrap().unwrap();
        
        // Now accessing again should hit cache
        let schema1 = loader.get_schema(&conn, "table1").unwrap().unwrap();
        let schema2 = loader.get_schema(&conn, "table2").unwrap().unwrap();
        
        assert_eq!(schema1.columns.len(), 1);
        assert_eq!(schema2.columns.len(), 1);
        
        // Should have cache hit rate of 0.5 (2 hits out of 4 total requests)
        let hit_rate = loader.get_cache_hit_rate();
        assert!(hit_rate >= 0.4, "Expected hit rate >= 0.4, got {hit_rate}");
        
        // Test preloading functionality
        let tables = vec!["table1".to_string(), "table2".to_string()];
        loader.preload_schemas(&conn, &tables).unwrap();
    }
}