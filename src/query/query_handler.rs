use crate::session::{DbHandler, DbResponse, QueryRouter, SessionState};
use crate::PgSqliteError;
use std::sync::Arc;
use async_trait::async_trait;

/// Trait for handling database queries - can be implemented by DbHandler or QueryRouter
#[async_trait]
pub trait QueryHandler: Send + Sync {
    /// Execute a SELECT query
    async fn query(&self, sql: &str) -> Result<DbResponse, PgSqliteError>;
    
    /// Execute a DML/DDL query
    async fn execute(&self, sql: &str) -> Result<DbResponse, PgSqliteError>;
    
    /// Get schema type information
    async fn get_schema_type(&self, table: &str, column: &str) -> Result<Option<String>, PgSqliteError>;
    
    /// Check if using connection pooling
    fn is_pooling_enabled(&self) -> bool {
        false
    }
}

/// Wrapper enum to hold either a DbHandler or QueryRouter
pub enum QueryHandlerImpl {
    Direct(Arc<DbHandler>),
    Routed(Arc<QueryRouter>),
}

#[async_trait]
impl QueryHandler for QueryHandlerImpl {
    async fn query(&self, sql: &str) -> Result<DbResponse, PgSqliteError> {
        match self {
            QueryHandlerImpl::Direct(db) => {
                db.query(sql).await
            },
            QueryHandlerImpl::Routed(router) => {
                // For routed queries, we need the session state to determine transaction status
                // For now, we'll create a temporary session state - this needs to be refactored
                let session = SessionState::new("temp".to_string(), "temp".to_string());
                router.execute_query(sql, &session).await
                    .map_err(|e| PgSqliteError::Protocol(e.to_string()))
            }
        }
    }
    
    async fn execute(&self, sql: &str) -> Result<DbResponse, PgSqliteError> {
        match self {
            QueryHandlerImpl::Direct(db) => {
                db.execute(sql).await
            },
            QueryHandlerImpl::Routed(router) => {
                // For routed queries, we need the session state to determine transaction status
                let session = SessionState::new("temp".to_string(), "temp".to_string());
                router.execute_query(sql, &session).await
                    .map_err(|e| PgSqliteError::Protocol(e.to_string()))
            }
        }
    }
    
    async fn get_schema_type(&self, table: &str, column: &str) -> Result<Option<String>, PgSqliteError> {
        match self {
            QueryHandlerImpl::Direct(db) => {
                db.get_schema_type(table, column).await
            },
            QueryHandlerImpl::Routed(_) => {
                // For now, schema queries always go to the write handler
                // This could be optimized to use read-only connections
                Ok(None)
            }
        }
    }
    
    fn is_pooling_enabled(&self) -> bool {
        matches!(self, QueryHandlerImpl::Routed(_))
    }
}

/// Implementation for Arc<DbHandler> to maintain backward compatibility
#[async_trait]
impl QueryHandler for Arc<DbHandler> {
    async fn query(&self, sql: &str) -> Result<DbResponse, PgSqliteError> {
        DbHandler::query(self, sql).await
            .map_err(|e| e.into())
    }
    
    async fn execute(&self, sql: &str) -> Result<DbResponse, PgSqliteError> {
        DbHandler::execute(self, sql).await
            .map_err(|e| e.into())
    }
    
    async fn get_schema_type(&self, table: &str, column: &str) -> Result<Option<String>, PgSqliteError> {
        DbHandler::get_schema_type(self, table, column).await
            .map_err(|e| e.into())
    }
}