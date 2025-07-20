use crate::session::{DbHandler, ReadOnlyDbHandler, DbResponse, ReadOnlyError};
use crate::session::state::SessionState;
use crate::config::Config;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info};

#[derive(Error, Debug)]
pub enum RouterError {
    #[error("Read-only handler error: {0}")]
    ReadOnly(#[from] ReadOnlyError),
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("Other error: {0}")]
    Other(String),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum QueryRoute {
    /// Use read-only handler with connection pool
    ReadOnly,
    /// Use main write handler
    Write,
    /// Use write handler due to active transaction
    WriteTransaction,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Alter,
    Begin,
    Commit,
    Rollback,
    Pragma,
    Explain,
    Unknown,
}

/// Query router that determines whether to use read-only pool or write connection
pub struct QueryRouter {
    write_handler: Arc<DbHandler>,
    read_handler: Arc<ReadOnlyDbHandler>,
    config: Arc<Config>,
    /// Track if connection pooling is enabled
    pooling_enabled: bool,
}

impl QueryRouter {
    /// Create a new query router
    pub fn new(
        write_handler: Arc<DbHandler>,
        read_handler: Arc<ReadOnlyDbHandler>,
        config: Arc<Config>,
    ) -> Self {
        Self {
            write_handler,
            read_handler,
            config,
            pooling_enabled: true, // TODO: Make this configurable
        }
    }

    /// Execute a query using the appropriate handler
    pub async fn execute_query(
        &self,
        sql: &str,
        session_state: &SessionState,
    ) -> Result<DbResponse, RouterError> {
        let route = self.determine_route(sql, session_state);
        debug!("Query route: {:?} for SQL: {}", route, sql.chars().take(100).collect::<String>());

        match route {
            QueryRoute::ReadOnly => {
                info!("Executing query via read-only pool");
                let result = self.read_handler.query(sql).await?;
                Ok(result)
            }
            QueryRoute::Write | QueryRoute::WriteTransaction => {
                info!("Executing query via write handler");
                let result = self.write_handler.query(sql).await?;
                Ok(result)
            }
        }
    }

    /// Execute a parameterized query
    pub async fn execute_query_with_params(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
        session_state: &SessionState,
    ) -> Result<DbResponse, RouterError> {
        let route = self.determine_route(sql, session_state);
        debug!("Parameterized query route: {:?}", route);

        match route {
            QueryRoute::ReadOnly => {
                let result = self.read_handler.query_with_params(sql, params).await?;
                Ok(result)
            }
            QueryRoute::Write | QueryRoute::WriteTransaction => {
                // For now, use the write handler for parameterized queries
                // TODO: Implement parameterized queries in write handler
                let result = self.write_handler.query(sql).await?;
                Ok(result)
            }
        }
    }

    /// Determine which route to use for a query
    pub fn determine_route(&self, sql: &str, session_state: &SessionState) -> QueryRoute {
        // If pooling is disabled, always use write handler
        if !self.pooling_enabled {
            return QueryRoute::Write;
        }

        // If we're in a transaction, always use write handler for consistency
        if session_state.in_transaction() {
            return QueryRoute::WriteTransaction;
        }

        let query_type = self.classify_query(sql);
        
        match query_type {
            QueryType::Select | QueryType::Explain => {
                // Use read-only pool for SELECT and EXPLAIN queries
                QueryRoute::ReadOnly
            }
            QueryType::Pragma => {
                // Most pragma queries are read-only, but some modify state
                if self.is_read_only_pragma(sql) {
                    QueryRoute::ReadOnly
                } else {
                    QueryRoute::Write
                }
            }
            _ => {
                // All other operations use write handler
                QueryRoute::Write
            }
        }
    }

    /// Classify the type of SQL query
    pub fn classify_query(&self, sql: &str) -> QueryType {
        let sql_trimmed = sql.trim().to_uppercase();
        
        if sql_trimmed.starts_with("SELECT") || sql_trimmed.starts_with("WITH") {
            QueryType::Select
        } else if sql_trimmed.starts_with("INSERT") {
            QueryType::Insert
        } else if sql_trimmed.starts_with("UPDATE") {
            QueryType::Update
        } else if sql_trimmed.starts_with("DELETE") {
            QueryType::Delete
        } else if sql_trimmed.starts_with("CREATE") {
            QueryType::Create
        } else if sql_trimmed.starts_with("DROP") {
            QueryType::Drop
        } else if sql_trimmed.starts_with("ALTER") {
            QueryType::Alter
        } else if sql_trimmed.starts_with("BEGIN") || sql_trimmed.starts_with("START") {
            QueryType::Begin
        } else if sql_trimmed.starts_with("COMMIT") || sql_trimmed.starts_with("END") {
            QueryType::Commit
        } else if sql_trimmed.starts_with("ROLLBACK") {
            QueryType::Rollback
        } else if sql_trimmed.starts_with("PRAGMA") {
            QueryType::Pragma
        } else if sql_trimmed.starts_with("EXPLAIN") {
            QueryType::Explain
        } else {
            QueryType::Unknown
        }
    }

    /// Check if a PRAGMA statement is read-only
    fn is_read_only_pragma(&self, sql: &str) -> bool {
        let sql_upper = sql.to_uppercase();
        
        // Read-only pragma statements (queries that read values, not set them)
        sql_upper.contains("PRAGMA TABLE_INFO") ||
        sql_upper.contains("PRAGMA INDEX_LIST") ||
        sql_upper.contains("PRAGMA FOREIGN_KEY_LIST") ||
        sql_upper.contains("PRAGMA DATABASE_LIST") ||
        sql_upper.contains("PRAGMA COMPILE_OPTIONS") ||
        sql_upper.contains("PRAGMA INTEGRITY_CHECK") ||
        sql_upper.contains("PRAGMA QUICK_CHECK") ||
        // Only read-only if they don't contain assignment operators
        (sql_upper.contains("PRAGMA USER_VERSION") && !sql_upper.contains("=")) ||
        (sql_upper.contains("PRAGMA APPLICATION_ID") && !sql_upper.contains("=")) ||
        (sql_upper.contains("PRAGMA SCHEMA_VERSION") && !sql_upper.contains("=")) ||
        (sql_upper.contains("PRAGMA FREELIST_COUNT") && !sql_upper.contains("=")) ||
        (sql_upper.contains("PRAGMA PAGE_COUNT") && !sql_upper.contains("=")) ||
        (sql_upper.contains("PRAGMA PAGE_SIZE") && !sql_upper.contains("=")) ||
        (sql_upper.contains("PRAGMA CACHE_SIZE") && !sql_upper.contains("=")) ||
        (sql_upper.contains("PRAGMA JOURNAL_MODE") && !sql_upper.contains("=")) ||
        (sql_upper.contains("PRAGMA SYNCHRONOUS") && !sql_upper.contains("="))
    }

    /// Get routing statistics for monitoring
    pub fn get_stats(&self) -> RouterStats {
        RouterStats {
            pooling_enabled: self.pooling_enabled,
            read_pool_stats: self.read_handler.pool_stats(),
        }
    }

    /// Enable or disable connection pooling
    pub fn set_pooling_enabled(&mut self, enabled: bool) {
        self.pooling_enabled = enabled;
        info!("Connection pooling {}", if enabled { "enabled" } else { "disabled" });
    }
}

#[derive(Debug, Clone)]
pub struct RouterStats {
    pub pooling_enabled: bool,
    pub read_pool_stats: crate::session::PoolStats,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_classification() {
        let config = Arc::new(Config::load());
        let write_handler = Arc::new(DbHandler::new(":memory:").unwrap());
        let read_handler = Arc::new(ReadOnlyDbHandler::new(":memory:", config.clone()).unwrap());
        let router = QueryRouter::new(write_handler, read_handler, config);

        assert_eq!(router.classify_query("SELECT * FROM users"), QueryType::Select);
        assert_eq!(router.classify_query("  select id from table  "), QueryType::Select);
        assert_eq!(router.classify_query("WITH cte AS (SELECT 1) SELECT * FROM cte"), QueryType::Select);
        assert_eq!(router.classify_query("INSERT INTO users VALUES (1)"), QueryType::Insert);
        assert_eq!(router.classify_query("UPDATE users SET name = 'test'"), QueryType::Update);
        assert_eq!(router.classify_query("DELETE FROM users"), QueryType::Delete);
        assert_eq!(router.classify_query("CREATE TABLE test (id INTEGER)"), QueryType::Create);
        assert_eq!(router.classify_query("DROP TABLE test"), QueryType::Drop);
        assert_eq!(router.classify_query("BEGIN TRANSACTION"), QueryType::Begin);
        assert_eq!(router.classify_query("COMMIT"), QueryType::Commit);
        assert_eq!(router.classify_query("ROLLBACK"), QueryType::Rollback);
        assert_eq!(router.classify_query("PRAGMA table_info(users)"), QueryType::Pragma);
        assert_eq!(router.classify_query("EXPLAIN SELECT * FROM users"), QueryType::Explain);
    }

    #[test]
    fn test_read_only_pragma_detection() {
        let config = Arc::new(Config::load());
        let write_handler = Arc::new(DbHandler::new(":memory:").unwrap());
        let read_handler = Arc::new(ReadOnlyDbHandler::new(":memory:", config.clone()).unwrap());
        let router = QueryRouter::new(write_handler, read_handler, config);

        assert!(router.is_read_only_pragma("PRAGMA table_info(users)"));
        assert!(router.is_read_only_pragma("PRAGMA INDEX_LIST(users)"));
        assert!(router.is_read_only_pragma("PRAGMA foreign_key_list(users)"));
        assert!(router.is_read_only_pragma("PRAGMA database_list"));
        
        // These should be considered write operations (they don't match our read-only patterns)
        assert!(!router.is_read_only_pragma("PRAGMA journal_mode = WAL"));
        assert!(!router.is_read_only_pragma("PRAGMA synchronous = NORMAL"));
    }

    #[test]
    fn test_route_determination() {
        let config = Arc::new(Config::load());
        let write_handler = Arc::new(DbHandler::new(":memory:").unwrap());
        let read_handler = Arc::new(ReadOnlyDbHandler::new(":memory:", config.clone()).unwrap());
        let router = QueryRouter::new(write_handler, read_handler, config);
        
        let session_state = SessionState::new_test();

        // SELECT queries should use read-only pool
        assert_eq!(
            router.determine_route("SELECT * FROM users", &session_state),
            QueryRoute::ReadOnly
        );

        // Write operations should use write handler
        assert_eq!(
            router.determine_route("INSERT INTO users VALUES (1)", &session_state),
            QueryRoute::Write
        );

        // Read-only pragmas should use read-only pool
        assert_eq!(
            router.determine_route("PRAGMA table_info(users)", &session_state),
            QueryRoute::ReadOnly
        );
    }
}