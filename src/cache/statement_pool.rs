use std::collections::HashMap;
use std::sync::Mutex;
use rusqlite::{Connection, Statement, Params};
use once_cell::sync::Lazy;
use crate::config::CONFIG;

/// A pool of prepared SQLite statements for reuse
/// This avoids the overhead of preparing the same statement multiple times
pub struct StatementPool {
    statements: Mutex<HashMap<String, CachedStatement>>,
    max_size: usize,
}

/// Metadata about a cached prepared statement
#[derive(Debug, Clone)]
pub struct StatementMetadata {
    pub column_names: Vec<String>,
    pub column_types: Vec<Option<String>>, // PostgreSQL type names if known
    pub parameter_count: usize,
    pub is_select: bool,
}

/// A cached prepared statement with its metadata
struct CachedStatement {
    // We can't store the Statement directly because it borrows from Connection
    // Instead we store the metadata and recreate the statement when needed
    metadata: StatementMetadata,
    last_used: std::time::Instant,
}

/// Global statement pool instance
static GLOBAL_STATEMENT_POOL: Lazy<StatementPool> = Lazy::new(|| {
    StatementPool::new(CONFIG.statement_pool_size)
});

impl StatementPool {
    pub fn new(max_size: usize) -> Self {
        Self {
            statements: Mutex::new(HashMap::new()),
            max_size,
        }
    }

    /// Get the global statement pool instance
    pub fn global() -> &'static StatementPool {
        &GLOBAL_STATEMENT_POOL
    }
    
    /// Generate a normalized fingerprint for batch INSERT queries
    /// This allows caching the same prepared statement for different batch sizes
    pub fn batch_insert_fingerprint(query: &str) -> Option<String> {
        let upper_query = query.to_uppercase();
        
        // Check if it's a batch INSERT (with or without spaces)
        if !upper_query.contains("INSERT") || (!query.contains("),(") && !query.contains("), (")) {
            return None;
        }
        
        // Extract the pattern: INSERT INTO table (cols) VALUES
        if let Some(_values_pos) = upper_query.find("VALUES") {
            // Find the position in the original query (case-sensitive)
            let original_values_pos = query.to_uppercase().find("VALUES").unwrap();
            let prefix = &query[..original_values_pos + 6].trim(); // Include "VALUES"
            // Replace the actual values with a placeholder
            Some(format!("{prefix} (?)"))
        } else {
            None
        }
    }

    /// Prepare a statement and cache its metadata for future use
    pub fn prepare_and_cache<'conn>(
        &self,
        conn: &'conn Connection,
        query: &str,
    ) -> Result<(Statement<'conn>, StatementMetadata), rusqlite::Error> {
        // For batch INSERTs, use a normalized fingerprint for caching
        let cache_key = if let Some(fingerprint) = Self::batch_insert_fingerprint(query) {
            fingerprint
        } else {
            query.to_string()
        };
        
        // Check if we have cached metadata for this query
        if let Some(metadata) = self.get_metadata(&cache_key) {
            // We have metadata, prepare the statement with that info
            let stmt = conn.prepare(query)?;
            return Ok((stmt, metadata));
        }

        // First time seeing this query, prepare it and extract metadata
        let stmt = conn.prepare(query)?;
        let metadata = self.extract_metadata(&stmt, query)?;

        // Cache the metadata
        self.cache_metadata(cache_key, metadata.clone());

        Ok((stmt, metadata))
    }

    /// Execute a cached statement with parameters
    pub fn execute_cached<P: Params>(
        &self,
        conn: &Connection,
        query: &str,
        params: P,
    ) -> Result<usize, rusqlite::Error> {
        let (mut stmt, _metadata) = self.prepare_and_cache(conn, query)?;
        stmt.execute(params)
    }

    /// Query with a cached statement
    pub fn query_cached<P: Params>(
        &self,
        conn: &Connection,
        query: &str,
        params: P,
    ) -> Result<(Vec<String>, Vec<Vec<Option<Vec<u8>>>>), rusqlite::Error> {
        let (mut stmt, metadata) = self.prepare_and_cache(conn, query)?;
        
        // Execute query and collect results
        let rows = stmt.query_map(params, |row| {
            let mut row_data = Vec::new();
            for i in 0..metadata.column_names.len() {
                match row.get_ref(i)? {
                    rusqlite::types::ValueRef::Null => row_data.push(None),
                    rusqlite::types::ValueRef::Integer(int_val) => {
                        // Check if this should be a boolean conversion
                        let is_boolean = metadata.column_types.get(i)
                            .and_then(|opt| opt.as_ref())
                            .map(|pg_type| {
                                let type_lower = pg_type.to_lowercase();
                                type_lower == "boolean" || type_lower == "bool"
                            })
                            .unwrap_or(false);
                        
                        if is_boolean {
                            let bool_str = if int_val == 0 { "f" } else { "t" };
                            row_data.push(Some(bool_str.as_bytes().to_vec()));
                        } else {
                            row_data.push(Some(int_val.to_string().into_bytes()));
                        }
                    },
                    rusqlite::types::ValueRef::Real(f) => {
                        row_data.push(Some(f.to_string().into_bytes()));
                    },
                    rusqlite::types::ValueRef::Text(s) => {
                        row_data.push(Some(s.to_vec()));
                    },
                    rusqlite::types::ValueRef::Blob(b) => {
                        row_data.push(Some(b.to_vec()));
                    },
                }
            }
            Ok(row_data)
        })?;

        let mut result_rows = Vec::new();
        for row in rows {
            result_rows.push(row?);
        }

        Ok((metadata.column_names.clone(), result_rows))
    }

    /// Get cached metadata for a query
    fn get_metadata(&self, query: &str) -> Option<StatementMetadata> {
        let statements = self.statements.lock().ok()?;
        statements.get(query).map(|cached| {
            // Update last used time (we can't modify through the immutable reference)
            cached.metadata.clone()
        })
    }

    /// Cache metadata for a query
    fn cache_metadata(&self, query: String, metadata: StatementMetadata) {
        if let Ok(mut statements) = self.statements.lock() {
            // Evict old entries if we're at capacity
            if statements.len() >= self.max_size {
                self.evict_oldest(&mut statements);
            }

            statements.insert(query.clone(), CachedStatement {
                metadata,
                last_used: std::time::Instant::now(),
            });
        }
    }

    /// Extract metadata from a prepared statement
    fn extract_metadata(&self, stmt: &Statement, query: &str) -> Result<StatementMetadata, rusqlite::Error> {
        let column_count = stmt.column_count();
        let mut column_names = Vec::new();
        let mut column_types = Vec::new();

        for i in 0..column_count {
            column_names.push(stmt.column_name(i)?.to_string());
            // We can't easily get PostgreSQL types here, so we'll leave them as None
            // They can be filled in later by the caller if needed
            column_types.push(None);
        }

        let parameter_count = stmt.parameter_count();
        let is_select = query.trim().to_uppercase().starts_with("SELECT");

        Ok(StatementMetadata {
            column_names,
            column_types,
            parameter_count,
            is_select,
        })
    }

    /// Evict the oldest entry from the cache
    fn evict_oldest(&self, statements: &mut HashMap<String, CachedStatement>) {
        if let Some((oldest_key, _)) = statements
            .iter()
            .min_by_key(|(_, cached)| cached.last_used)
            .map(|(k, v)| (k.clone(), v.last_used))
        {
            statements.remove(&oldest_key);
        }
    }

    /// Update the last used time for a cached statement
    pub fn touch(&self, query: &str) {
        if let Ok(mut statements) = self.statements.lock() {
            if let Some(cached) = statements.get_mut(query) {
                cached.last_used = std::time::Instant::now();
            }
        }
    }

    /// Clear the statement pool (useful for DDL operations)
    pub fn clear(&self) {
        if let Ok(mut statements) = self.statements.lock() {
            statements.clear();
        }
    }

    /// Get statistics about the statement pool
    pub fn stats(&self) -> StatementPoolStats {
        if let Ok(statements) = self.statements.lock() {
            StatementPoolStats {
                cached_statements: statements.len(),
                max_capacity: self.max_size,
            }
        } else {
            StatementPoolStats {
                cached_statements: 0,
                max_capacity: self.max_size,
            }
        }
    }
}

#[derive(Debug)]
pub struct StatementPoolStats {
    pub cached_statements: usize,
    pub max_capacity: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn test_statement_pool_basic() {
        let pool = StatementPool::new(10);
        let conn = Connection::open_in_memory().unwrap();
        
        // Create a test table
        conn.execute("CREATE TABLE test (id INTEGER, name TEXT)", []).unwrap();
        conn.execute("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')", []).unwrap();

        // Test caching behavior
        let query = "SELECT id, name FROM test WHERE id = ?";
        
        // First execution should cache metadata
        let (_stmt1, metadata1) = pool.prepare_and_cache(&conn, query).unwrap();
        assert_eq!(metadata1.column_names, vec!["id", "name"]);
        assert_eq!(metadata1.parameter_count, 1);
        assert!(metadata1.is_select);

        // Second execution should reuse cached metadata
        let (_stmt2, metadata2) = pool.prepare_and_cache(&conn, query).unwrap();
        assert_eq!(metadata1.column_names, metadata2.column_names);
        assert_eq!(metadata1.parameter_count, metadata2.parameter_count);
    }

    #[test]
    fn test_statement_pool_execute() {
        let pool = StatementPool::new(10);
        let conn = Connection::open_in_memory().unwrap();
        
        conn.execute("CREATE TABLE test (id INTEGER, name TEXT)", []).unwrap();
        
        // Test execute through pool
        let rows_affected = pool.execute_cached(&conn, 
            "INSERT INTO test (id, name) VALUES (?, ?)", 
            [&1i32 as &dyn rusqlite::ToSql, &"Alice"]).unwrap();
        assert_eq!(rows_affected, 1);

        // Test query through pool
        let (columns, rows) = pool.query_cached(&conn,
            "SELECT id, name FROM test WHERE id = ?",
            [&1i32 as &dyn rusqlite::ToSql]).unwrap();
        assert_eq!(columns, vec!["id", "name"]);
        assert_eq!(rows.len(), 1);
    }
    
    #[test]
    fn test_batch_insert_fingerprint() {
        // Simple batch INSERT
        let query = "INSERT INTO users (id, name) VALUES (1, 'test'), (2, 'test2')";
        let fingerprint = StatementPool::batch_insert_fingerprint(query);
        assert_eq!(fingerprint, Some("INSERT INTO users (id, name) VALUES (?)".to_string()));
        
        // Larger batch should have same fingerprint
        let query2 = "INSERT INTO users (id, name) VALUES (1, 'test'), (2, 'test2'), (3, 'test3')";
        let fingerprint2 = StatementPool::batch_insert_fingerprint(query2);
        assert_eq!(fingerprint, fingerprint2);
        
        // Single row INSERT should not be fingerprinted
        let single = "INSERT INTO users (id, name) VALUES (1, 'test')";
        assert!(StatementPool::batch_insert_fingerprint(single).is_none());
        
        // Non-INSERT should not be fingerprinted
        let select = "SELECT * FROM users WHERE id IN (1, 2, 3)";
        assert!(StatementPool::batch_insert_fingerprint(select).is_none());
    }
}