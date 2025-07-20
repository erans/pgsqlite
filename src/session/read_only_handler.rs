use crate::session::pool::SqlitePool;
use crate::session::db_handler::DbResponse;
use crate::config::Config;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ReadOnlyError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("Pool error: {0}")]
    Pool(String),
    #[error("Write operation not allowed on read-only handler")]
    WriteNotAllowed,
}

/// Read-only database handler with connection pooling
/// Optimized for concurrent SELECT operations
pub struct ReadOnlyDbHandler {
    pool: SqlitePool,
    config: Arc<Config>,
}

impl ReadOnlyDbHandler {
    /// Create a new read-only handler with connection pool
    pub fn new(db_path: &str, config: Arc<Config>) -> Result<Self, ReadOnlyError> {
        let pool = SqlitePool::new(db_path)?;
        
        Ok(ReadOnlyDbHandler {
            pool,
            config,
        })
    }

    /// Create with custom pool size
    pub fn new_with_pool_size(
        db_path: &str, 
        config: Arc<Config>, 
        pool_size: usize
    ) -> Result<Self, ReadOnlyError> {
        let pool = SqlitePool::new_with_size(db_path, pool_size)?;
        
        Ok(ReadOnlyDbHandler {
            pool,
            config,
        })
    }

    /// Execute a SELECT query using a pooled connection
    pub async fn query(&self, sql: &str) -> Result<DbResponse, ReadOnlyError> {
        // Ensure this is a read-only operation
        if !is_read_only_query(sql) {
            return Err(ReadOnlyError::WriteNotAllowed);
        }

        let conn = self.pool.acquire().await?;
        
        // Execute query using rusqlite
        let mut stmt = conn.prepare(sql)?;
        let column_names: Vec<String> = stmt.column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        let rows = stmt.query_map([], |row| {
            let mut values = Vec::new();
            for i in 0..column_names.len() {
                // Convert SQLite values to bytes for DbResponse compatibility
                let value = match row.get::<_, rusqlite::types::Value>(i)? {
                    rusqlite::types::Value::Null => None,
                    rusqlite::types::Value::Integer(i) => Some(i.to_string().into_bytes()),
                    rusqlite::types::Value::Real(f) => Some(f.to_string().into_bytes()),
                    rusqlite::types::Value::Text(s) => Some(s.into_bytes()),
                    rusqlite::types::Value::Blob(b) => Some(b),
                };
                values.push(value);
            }
            Ok(values)
        })?;

        let mut result_rows = Vec::new();
        for row_result in rows {
            result_rows.push(row_result?);
        }

        let rows_affected = result_rows.len();
        Ok(DbResponse {
            columns: column_names,
            rows: result_rows,
            rows_affected,
        })
    }

    /// Execute a prepared statement with parameters
    pub async fn query_with_params(
        &self, 
        sql: &str, 
        params: &[&dyn rusqlite::ToSql]
    ) -> Result<DbResponse, ReadOnlyError> {
        // Ensure this is a read-only operation
        if !is_read_only_query(sql) {
            return Err(ReadOnlyError::WriteNotAllowed);
        }

        let conn = self.pool.acquire().await?;
        
        let mut stmt = conn.prepare(sql)?;
        let column_names: Vec<String> = stmt.column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        let rows = stmt.query_map(params, |row| {
            let mut values = Vec::new();
            for i in 0..column_names.len() {
                let value = match row.get::<_, rusqlite::types::Value>(i)? {
                    rusqlite::types::Value::Null => None,
                    rusqlite::types::Value::Integer(i) => Some(i.to_string().into_bytes()),
                    rusqlite::types::Value::Real(f) => Some(f.to_string().into_bytes()),
                    rusqlite::types::Value::Text(s) => Some(s.into_bytes()),
                    rusqlite::types::Value::Blob(b) => Some(b),
                };
                values.push(value);
            }
            Ok(values)
        })?;

        let mut result_rows = Vec::new();
        for row_result in rows {
            result_rows.push(row_result?);
        }

        let rows_affected = result_rows.len();
        Ok(DbResponse {
            columns: column_names,
            rows: result_rows,
            rows_affected,
        })
    }

    /// Get pool statistics for monitoring
    pub fn pool_stats(&self) -> PoolStats {
        // Implementation would depend on pool metrics
        PoolStats {
            active_connections: 0, // Would need pool.active_count()
            idle_connections: 0,   // Would need pool.idle_count()
            total_connections: 0,  // Would need pool.total_count()
        }
    }

    /// Test connection health
    pub async fn health_check(&self) -> Result<(), ReadOnlyError> {
        let conn = self.pool.acquire().await?;
        conn.execute("SELECT 1", [])?;
        Ok(())
    }
}

/// Check if a SQL query is read-only
fn is_read_only_query(sql: &str) -> bool {
    let sql_upper = sql.trim().to_uppercase();
    
    // Allow only SELECT statements and certain pragmas
    sql_upper.starts_with("SELECT") 
        || sql_upper.starts_with("WITH") // CTEs
        || sql_upper.starts_with("EXPLAIN")
        || sql_upper.starts_with("PRAGMA TABLE_INFO")
        || sql_upper.starts_with("PRAGMA INDEX_LIST")
        || sql_upper.starts_with("PRAGMA FOREIGN_KEY_LIST")
        // Add other read-only pragmas as needed
}

#[derive(Debug, Clone)]
pub struct PoolStats {
    pub active_connections: usize,
    pub idle_connections: usize,
    pub total_connections: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_read_only_query() {
        assert!(is_read_only_query("SELECT * FROM users"));
        assert!(is_read_only_query("  select id from table  "));
        assert!(is_read_only_query("WITH cte AS (SELECT 1) SELECT * FROM cte"));
        assert!(is_read_only_query("EXPLAIN SELECT * FROM table"));
        
        assert!(!is_read_only_query("INSERT INTO users VALUES (1)"));
        assert!(!is_read_only_query("UPDATE users SET name = 'test'"));
        assert!(!is_read_only_query("DELETE FROM users"));
        assert!(!is_read_only_query("CREATE TABLE test (id INTEGER)"));
        assert!(!is_read_only_query("DROP TABLE test"));
    }

    #[tokio::test]
    async fn test_read_only_handler_creation() {
        let config = Arc::new(Config::load());
        let handler = ReadOnlyDbHandler::new(":memory:", config);
        assert!(handler.is_ok());
    }

    #[tokio::test]
    async fn test_write_query_rejection() {
        let config = Arc::new(Config::load());
        let handler = ReadOnlyDbHandler::new(":memory:", config).unwrap();
        
        let result = handler.query("INSERT INTO test VALUES (1)").await;
        assert!(matches!(result, Err(ReadOnlyError::WriteNotAllowed)));
    }
}