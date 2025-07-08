use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use rusqlite::Connection;
use crate::types::PgType;

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
    decimal_tables: Arc<RwLock<HashSet<String>>>,
    all_tables_loaded: Arc<RwLock<bool>>,
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
            decimal_tables: Arc::new(RwLock::new(HashSet::new())),
            all_tables_loaded: Arc::new(RwLock::new(false)),
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
        self.decimal_tables.write().unwrap().clear();
        *self.all_tables_loaded.write().unwrap() = false;
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

    /// Check if a table has decimal columns (optimized with bloom filter)
    pub fn has_decimal_columns(&self, table_name: &str) -> bool {
        self.decimal_tables.read().unwrap().contains(table_name)
    }

    /// Preload all table schemas from the database
    pub fn preload_all_schemas(&self, conn: &Connection) -> Result<(), rusqlite::Error> {
        // Check if already loaded
        if *self.all_tables_loaded.read().unwrap() {
            return Ok(());
        }

        // Get all table names
        let mut table_names = Vec::new();
        let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name != '__pgsqlite_schema'")?;
        let rows = stmt.query_map([], |row| {
            let name: String = row.get(0)?;
            Ok(name)
        })?;

        for row in rows {
            table_names.push(row?);
        }

        // Bulk load all schema information
        let mut all_schemas = HashMap::new();
        let mut decimal_tables_set = HashSet::new();

        for table_name in table_names {
            if let Ok(schema) = self.load_table_schema_direct(conn, &table_name) {
                // Check for decimal columns
                let has_decimal = schema.columns.iter().any(|col| {
                    col.pg_type == "numeric" || col.pg_oid == PgType::Numeric.to_oid()
                });
                
                if has_decimal {
                    decimal_tables_set.insert(table_name.clone());
                }

                all_schemas.insert(table_name, CacheEntry {
                    schema,
                    cached_at: Instant::now(),
                });
            }
        }

        // Update cache atomically
        {
            let mut cache = self.cache.write().unwrap();
            cache.extend(all_schemas);
        }
        {
            let mut decimal_tables = self.decimal_tables.write().unwrap();
            decimal_tables.extend(decimal_tables_set);
        }
        *self.all_tables_loaded.write().unwrap() = true;

        Ok(())
    }

    /// Load a single table schema directly from database (bypassing cache)
    fn load_table_schema_direct(&self, conn: &Connection, table_name: &str) -> Result<TableSchema, rusqlite::Error> {
        let mut column_data = Vec::new();
        
        // First get all columns from SQLite schema
        let pragma_query = format!("PRAGMA table_info({})", table_name);
        let mut stmt = conn.prepare(&pragma_query)?;
        let rows = stmt.query_map([], |row| {
            let name: String = row.get(1)?;
            let sqlite_type: String = row.get(2)?;
            Ok((name, sqlite_type))
        })?;
        
        let mut sqlite_columns = Vec::new();
        for row in rows {
            sqlite_columns.push(row?);
        }

        // Bulk query for all PostgreSQL types for this table
        let mut pg_metadata = HashMap::new();
        if let Ok(mut stmt) = conn.prepare("SELECT column_name, pg_type FROM __pgsqlite_schema WHERE table_name = ?1") {
            if let Ok(rows) = stmt.query_map([table_name], |row| {
                let col_name: String = row.get(0)?;
                let pg_type: String = row.get(1)?;
                Ok((col_name, pg_type))
            }) {
                for row in rows.flatten() {
                    pg_metadata.insert(row.0, row.1);
                }
            }
        }

        // Build column data
        for (col_name, sqlite_type) in sqlite_columns {
            let (pg_type, pg_oid) = if let Some(pg_type_str) = pg_metadata.get(&col_name) {
                let oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(pg_type_str);
                (pg_type_str.clone(), oid)
            } else {
                // Fallback to type mapping
                let type_mapper = crate::types::TypeMapper::new();
                let pg_type = type_mapper.sqlite_to_pg(&sqlite_type);
                let oid = pg_type.to_oid();
                let pg_type_str = match pg_type {
                    crate::types::PgType::Text => "text",
                    crate::types::PgType::Int8 => "int8",
                    crate::types::PgType::Int4 => "int4", 
                    crate::types::PgType::Int2 => "int2",
                    crate::types::PgType::Float8 => "float8",
                    crate::types::PgType::Float4 => "float4",
                    crate::types::PgType::Bool => "boolean",
                    crate::types::PgType::Bytea => "bytea",
                    crate::types::PgType::Date => "date",
                    crate::types::PgType::Timestamp => "timestamp",
                    crate::types::PgType::Timestamptz => "timestamptz",
                    crate::types::PgType::Uuid => "uuid",
                    crate::types::PgType::Numeric => "numeric",
                    crate::types::PgType::Json => "json",
                    crate::types::PgType::Jsonb => "jsonb",
                    crate::types::PgType::Money => "money",
                    crate::types::PgType::Int4range => "int4range",
                    crate::types::PgType::Int8range => "int8range",
                    crate::types::PgType::Numrange => "numrange",
                    crate::types::PgType::Cidr => "cidr",
                    crate::types::PgType::Inet => "inet",
                    crate::types::PgType::Macaddr => "macaddr",
                    crate::types::PgType::Macaddr8 => "macaddr8",
                    crate::types::PgType::Bit => "bit",
                    crate::types::PgType::Varbit => "varbit",
                    crate::types::PgType::Varchar => "varchar",
                    crate::types::PgType::Char => "char",
                    crate::types::PgType::Time => "time",
                    crate::types::PgType::Timetz => "timetz",
                    crate::types::PgType::Interval => "interval",
                    crate::types::PgType::Unknown => "unknown",
                };
                (pg_type_str.to_string(), oid)
            };
            
            column_data.push((col_name, pg_type, sqlite_type, pg_oid));
        }
        
        Ok(Self::build_table_schema(column_data))
    }

    /// Get table schema with automatic preloading on first access
    pub fn get_or_load(&self, conn: &Connection, table_name: &str) -> Result<TableSchema, rusqlite::Error> {
        // Try cache first
        if let Some(schema) = self.get(table_name) {
            return Ok(schema);
        }

        // If all tables haven't been loaded yet, do bulk preload
        if !*self.all_tables_loaded.read().unwrap() {
            self.preload_all_schemas(conn)?;
            
            // Try cache again after preload
            if let Some(schema) = self.get(table_name) {
                return Ok(schema);
            }
        }

        // If still not found, load this specific table
        let schema = self.load_table_schema_direct(conn, table_name)?;
        self.insert(table_name.to_string(), schema.clone());
        
        // Check for decimal columns and update bloom filter
        let has_decimal = schema.columns.iter().any(|col| {
            col.pg_type == "numeric" || col.pg_oid == PgType::Numeric.to_oid()
        });
        if has_decimal {
            self.decimal_tables.write().unwrap().insert(table_name.to_string());
        }
        
        Ok(schema)
    }
}