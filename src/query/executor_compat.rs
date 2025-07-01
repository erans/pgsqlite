use crate::protocol::ProtocolWriter;
use crate::session::DbHandler;
use crate::query::executor_v2::QueryExecutorV2;
use crate::PgSqliteError;
use tokio_util::codec::Framed;

/// Compatibility layer to use QueryExecutorV2 with existing Framed connections
pub struct QueryExecutorCompat;

impl QueryExecutorCompat {
    /// Execute a query using a Framed connection (wraps it with FramedWriter)
    pub async fn execute_query<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        query: &str,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        // Create a temporary FramedWriter wrapper
        let mut writer = FramedWriterAdapter::new(framed);
        
        // Use QueryExecutorV2 with the writer
        QueryExecutorV2::execute_query(&mut writer, db, query).await
    }
}

/// Adapter that allows using a mutable reference to Framed as a ProtocolWriter
struct FramedWriterAdapter<'a, T> {
    framed: &'a mut Framed<T, crate::protocol::PostgresCodec>,
}

impl<'a, T> FramedWriterAdapter<'a, T> 
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    fn new(framed: &'a mut Framed<T, crate::protocol::PostgresCodec>) -> Self {
        Self { framed }
    }
}

#[async_trait::async_trait]
impl<'a, T> ProtocolWriter for FramedWriterAdapter<'a, T>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    async fn send_auth_ok(&mut self) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        use crate::protocol::{BackendMessage, AuthenticationMessage};
        self.framed.send(BackendMessage::Authentication(AuthenticationMessage::Ok)).await
            .map_err(|e| PgSqliteError::Io(e))
    }
    
    async fn send_parameter_status(&mut self, param: &str, value: &str) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        use crate::protocol::BackendMessage;
        self.framed.send(BackendMessage::ParameterStatus {
            name: param.to_string(),
            value: value.to_string(),
        }).await
            .map_err(|e| PgSqliteError::Io(e))
    }
    
    async fn send_backend_key_data(&mut self, process_id: i32, secret_key: i32) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        use crate::protocol::BackendMessage;
        self.framed.send(BackendMessage::BackendKeyData { process_id, secret_key }).await
            .map_err(|e| PgSqliteError::Io(e))
    }
    
    async fn send_ready_for_query(&mut self, status: crate::protocol::TransactionStatus) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        use crate::protocol::BackendMessage;
        self.framed.send(BackendMessage::ReadyForQuery { status }).await
            .map_err(|e| PgSqliteError::Io(e))
    }
    
    async fn send_row_description(&mut self, fields: &[crate::protocol::FieldDescription]) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        use crate::protocol::BackendMessage;
        self.framed.send(BackendMessage::RowDescription(fields.to_vec())).await
            .map_err(|e| PgSqliteError::Io(e))
    }
    
    async fn send_data_row(&mut self, values: &[Option<Vec<u8>>]) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        use crate::protocol::BackendMessage;
        self.framed.send(BackendMessage::DataRow(values.to_vec())).await
            .map_err(|e| PgSqliteError::Io(e))
    }
    
    async fn send_data_row_raw(&mut self, values: &[Option<&[u8]>]) -> Result<(), PgSqliteError> {
        let values: Vec<Option<Vec<u8>>> = values.iter()
            .map(|opt| opt.map(|v| v.to_vec()))
            .collect();
        self.send_data_row(&values).await
    }
    
    async fn send_command_complete(&mut self, tag: &str) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        use crate::protocol::BackendMessage;
        self.framed.send(BackendMessage::CommandComplete { tag: tag.to_string() }).await
            .map_err(|e| PgSqliteError::Io(e))
    }
    
    async fn send_error(&mut self, error: &PgSqliteError) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        use crate::protocol::{BackendMessage, ErrorResponse};
        let err_response = ErrorResponse::new(
            "ERROR".to_string(),
            error.pg_error_code().to_string(),
            error.to_string()
        );
        self.framed.send(BackendMessage::ErrorResponse(err_response)).await
            .map_err(|e| PgSqliteError::Io(e))
    }
    
    async fn send_parse_complete(&mut self) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        use crate::protocol::BackendMessage;
        self.framed.send(BackendMessage::ParseComplete).await
            .map_err(|e| PgSqliteError::Io(e))
    }
    
    async fn send_bind_complete(&mut self) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        use crate::protocol::BackendMessage;
        self.framed.send(BackendMessage::BindComplete).await
            .map_err(|e| PgSqliteError::Io(e))
    }
    
    async fn send_close_complete(&mut self) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        use crate::protocol::BackendMessage;
        self.framed.send(BackendMessage::CloseComplete).await
            .map_err(|e| PgSqliteError::Io(e))
    }
    
    async fn send_no_data(&mut self) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        use crate::protocol::BackendMessage;
        self.framed.send(BackendMessage::NoData).await
            .map_err(|e| PgSqliteError::Io(e))
    }
    
    async fn send_portal_suspended(&mut self) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        use crate::protocol::BackendMessage;
        self.framed.send(BackendMessage::PortalSuspended).await
            .map_err(|e| PgSqliteError::Io(e))
    }
    
    async fn flush(&mut self) -> Result<(), PgSqliteError> {
        use futures::SinkExt;
        self.framed.flush().await
            .map_err(|e| PgSqliteError::Io(e))
    }
}

/// Migration helper - gradually replace QueryExecutor calls with this
pub async fn execute_query_with_writer<W: ProtocolWriter>(
    writer: &mut W,
    db: &DbHandler,
    query: &str,
) -> Result<(), PgSqliteError> {
    QueryExecutorV2::execute_query(writer, db, query).await
}