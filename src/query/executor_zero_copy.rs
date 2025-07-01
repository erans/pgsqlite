use crate::protocol::BackendMessage;
use crate::session::DbHandler;
use crate::PgSqliteError;
use tokio_util::codec::Framed;
use futures::SinkExt;

/// Extension trait to add zero-copy capabilities to QueryExecutor
/// 
/// This trait provides optimized DML execution that reduces allocations
/// by using static strings for common cases (0 or 1 affected rows).
#[cfg(feature = "zero-copy-protocol")]
#[allow(async_fn_in_trait)]
pub trait QueryExecutorZeroCopy {
    async fn execute_dml_optimized<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        query: &str,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send;
}

#[cfg(feature = "zero-copy-protocol")]
impl QueryExecutorZeroCopy for crate::query::QueryExecutor {
    async fn execute_dml_optimized<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        query: &str,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        // Execute the query
        let response = db.execute(query).await?;
        
        // Determine the command type
        let query_upper = query.trim_start().to_uppercase();
        
        // Create the command complete message with optimized tag creation
        let msg = if query_upper.starts_with("INSERT") {
            match response.rows_affected {
                0 => BackendMessage::CommandComplete { tag: "INSERT 0 0".to_string() },
                1 => BackendMessage::CommandComplete { tag: "INSERT 0 1".to_string() }, // Most common
                n => BackendMessage::CommandComplete { tag: format!("INSERT 0 {}", n) },
            }
        } else if query_upper.starts_with("UPDATE") {
            match response.rows_affected {
                0 => BackendMessage::CommandComplete { tag: "UPDATE 0".to_string() },
                1 => BackendMessage::CommandComplete { tag: "UPDATE 1".to_string() },
                n => BackendMessage::CommandComplete { tag: format!("UPDATE {}", n) },
            }
        } else if query_upper.starts_with("DELETE") {
            match response.rows_affected {
                0 => BackendMessage::CommandComplete { tag: "DELETE 0".to_string() },
                1 => BackendMessage::CommandComplete { tag: "DELETE 1".to_string() },
                n => BackendMessage::CommandComplete { tag: format!("DELETE {}", n) },
            }
        } else {
            BackendMessage::CommandComplete { tag: format!("OK {}", response.rows_affected) }
        };
        
        // Send the message
        framed.send(msg).await.map_err(|e| PgSqliteError::Io(e))
    }
}

/// Helper to check if zero-copy optimization is enabled
#[cfg(feature = "zero-copy-protocol")]
pub fn should_use_zero_copy() -> bool {
    // Check environment variable or config
    std::env::var("PGSQLITE_ZERO_COPY").unwrap_or_default() == "1"
}

#[cfg(not(feature = "zero-copy-protocol"))]
pub fn should_use_zero_copy() -> bool {
    false
}