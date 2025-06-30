use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Represents column information for a table
#[derive(Clone, Debug)]
pub struct ColumnInfo {
    pub name: String,
    pub pg_type: String,
    pub pg_oid: i32,
    pub sqlite_type: String,
}

/// Represents complete schema information for a table
#[derive(Clone, Debug)]
pub struct TableSchema {
    pub columns: Vec<ColumnInfo>,
    pub column_map: HashMap<String, ColumnInfo>,
}

/// Cache for table schema information to avoid repeated PRAGMA queries
pub struct SchemaCache {
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
    ttl: Duration,
}

struct CacheEntry {
    schema: TableSchema,
    cached_at: Instant,
}

impl SchemaCache {
    pub fn new(ttl_seconds: u64) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            ttl: Duration::from_secs(ttl_seconds),
        }
    }

    /// Get cached schema for a table
    pub fn get(&self, table_name: &str) -> Option<TableSchema> {
        let cache = self.cache.read().unwrap();
        
        if let Some(entry) = cache.get(table_name) {
            if entry.cached_at.elapsed() < self.ttl {
                return Some(entry.schema.clone());
            }
        }
        
        None
    }

    /// Cache schema for a table
    pub fn insert(&self, table_name: String, schema: TableSchema) {
        let mut cache = self.cache.write().unwrap();
        
        cache.insert(table_name, CacheEntry {
            schema,
            cached_at: Instant::now(),
        });
    }

    /// Invalidate cache for a specific table (e.g., after ALTER TABLE)
    pub fn invalidate(&self, table_name: &str) {
        self.cache.write().unwrap().remove(table_name);
    }

    /// Clear entire cache (e.g., after CREATE TABLE or DROP TABLE)
    pub fn clear(&self) {
        self.cache.write().unwrap().clear();
    }

    /// Build TableSchema from raw column data
    pub fn build_table_schema(columns: Vec<(String, String, String, i32)>) -> TableSchema {
        let mut column_infos = Vec::new();
        let mut column_map = HashMap::new();
        
        for (name, pg_type, sqlite_type, pg_oid) in columns {
            let info = ColumnInfo {
                name: name.clone(),
                pg_type,
                pg_oid,
                sqlite_type,
            };
            
            column_map.insert(name.to_lowercase(), info.clone());
            column_infos.push(info);
        }
        
        TableSchema {
            columns: column_infos,
            column_map,
        }
    }
}