use crate::protocol::{BackendMessage, FieldDescription};
use crate::session::DbHandler;
use crate::PgSqliteError;
use tokio_util::codec::Framed;
use futures::SinkExt;

/// Extension trait to add batched DataRow sending for SELECT queries
/// 
/// This trait provides optimized SELECT execution that batches multiple
/// DataRow messages before flushing to reduce syscall overhead.
#[cfg(feature = "zero-copy-protocol")]
#[allow(async_fn_in_trait)]
pub trait QueryExecutorBatch {
    async fn execute_select_batched<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        query: &str,
        batch_size: usize,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send;
}

#[cfg(feature = "zero-copy-protocol")]
impl QueryExecutorBatch for crate::query::QueryExecutor {
    async fn execute_select_batched<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        query: &str,
        batch_size: usize,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        // Execute the query
        let response = db.query(query).await?;
        
        // Build field descriptions (simplified for demo)
        let fields: Vec<FieldDescription> = response.columns.iter()
            .enumerate()
            .map(|(i, name)| {
                FieldDescription {
                    name: name.clone(),
                    table_oid: 0,
                    column_id: (i + 1) as i16,
                    type_oid: 25, // TEXT for simplicity
                    type_size: -1,
                    type_modifier: -1,
                    format: 0,
                }
            })
            .collect();
        
        // Send RowDescription
        framed.send(BackendMessage::RowDescription(fields)).await
            .map_err(|e| PgSqliteError::Io(e))?;
        
        // Send data rows in batches
        let mut sent_count = 0;
        for (i, row) in response.rows.iter().enumerate() {
            framed.send(BackendMessage::DataRow(row.clone())).await
                .map_err(|e| PgSqliteError::Io(e))?;
            sent_count += 1;
            
            // Flush every batch_size rows
            if (i + 1) % batch_size == 0 {
                framed.flush().await
                    .map_err(|e| PgSqliteError::Io(e))?;
            }
        }
        
        // Final flush if needed
        if sent_count % batch_size != 0 {
            framed.flush().await
                .map_err(|e| PgSqliteError::Io(e))?;
        }
        
        // Send CommandComplete
        let tag = format!("SELECT {}", response.rows.len());
        framed.send(BackendMessage::CommandComplete { tag }).await
            .map_err(|e| PgSqliteError::Io(e))?;
        
        Ok(())
    }
}

/// Configuration for batched operations
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Number of rows to batch before flushing
    pub row_batch_size: usize,
    /// Whether batching is enabled
    pub enabled: bool,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            row_batch_size: 100,
            enabled: true,
        }
    }
}

impl BatchConfig {
    /// Create config from environment variables
    pub fn from_env() -> Self {
        let enabled = std::env::var("PGSQLITE_BATCH_ENABLED")
            .unwrap_or_default() != "0";
        
        let row_batch_size = std::env::var("PGSQLITE_BATCH_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        
        Self {
            row_batch_size,
            enabled,
        }
    }
}