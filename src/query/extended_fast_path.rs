use crate::protocol::BackendMessage;
use crate::session::{DbHandler, SessionState};
use crate::types::DecimalHandler;
use crate::cache::GLOBAL_PARAM_VALUE_CACHE;
use crate::PgSqliteError;
use tokio_util::codec::Framed;
use futures::SinkExt;
use tracing::info;
use std::sync::Arc;

/// Optimized parameter binding that avoids string substitution
pub struct ExtendedFastPath;

impl ExtendedFastPath {
    /// Execute a parameterized query using prepared statements directly
    pub async fn execute_with_params<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        session: &Arc<SessionState>,
        portal_name: &str,
        query: &str,
        bound_values: &[Option<Vec<u8>>],
        param_formats: &[i16],
        param_types: &[i32],
        query_type: QueryType,
    ) -> Result<bool, PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // Convert parameters to rusqlite values with caching
        let rusqlite_params = match Self::convert_parameters_cached(bound_values, param_formats, param_types) {
            Ok(params) => params,
            Err(_) => return Ok(false), // Fall back to normal path
        };
        
        // Execute based on query type
        match query_type {
            QueryType::Select => {
                Self::execute_select_with_params(framed, db, session, portal_name, query, rusqlite_params).await?;
                Ok(true)
            }
            QueryType::Insert | QueryType::Update | QueryType::Delete => {
                Self::execute_dml_with_params(framed, db, query, rusqlite_params, query_type).await?;
                Ok(true)
            }
            _ => Ok(false), // Fall back for other query types
        }
    }
    
    /// Convert parameters using cache to avoid repeated conversions
    fn convert_parameters_cached(
        bound_values: &[Option<Vec<u8>>],
        param_formats: &[i16],
        param_types: &[i32],
    ) -> Result<Vec<rusqlite::types::Value>, PgSqliteError> {
        let mut params = Vec::with_capacity(bound_values.len());
        
        for (i, value) in bound_values.iter().enumerate() {
            match value {
                None => params.push(rusqlite::types::Value::Null),
                Some(bytes) => {
                    let format = param_formats.get(i).copied().unwrap_or(0);
                    let param_type = param_types.get(i).copied().unwrap_or(25); // Default to TEXT
                    
                    // Use cache for parameter value conversion
                    let converted = GLOBAL_PARAM_VALUE_CACHE.get_or_convert(
                        bytes,
                        param_type,
                        format,
                        || Self::convert_parameter_value(bytes, format, param_type)
                    )?;
                    
                    params.push(converted);
                }
            }
        }
        
        Ok(params)
    }
    
    /// Convert a single parameter value
    fn convert_parameter_value(
        bytes: &[u8],
        format: i16,
        param_type: i32,
    ) -> Result<rusqlite::types::Value, PgSqliteError> {
        if format == 0 {
            // Text format
            let text = std::str::from_utf8(bytes)
                .map_err(|_| PgSqliteError::Protocol("Invalid UTF-8 in parameter".to_string()))?;
            
            match param_type {
                16 => {
                    // BOOL
                    let val = match text {
                        "t" | "true" | "TRUE" | "1" => 1,
                        _ => 0,
                    };
                    Ok(rusqlite::types::Value::Integer(val))
                }
                20 | 23 | 21 => {
                    // INT8, INT4, INT2
                    text.parse::<i64>()
                        .map(rusqlite::types::Value::Integer)
                        .map_err(|_| PgSqliteError::Protocol(format!("Invalid integer: {}", text)))
                }
                700 | 701 => {
                    // FLOAT4, FLOAT8
                    text.parse::<f64>()
                        .map(rusqlite::types::Value::Real)
                        .map_err(|_| PgSqliteError::Protocol(format!("Invalid float: {}", text)))
                }
                1700 => {
                    // NUMERIC - validate and store as text
                    match DecimalHandler::validate_numeric_string(text) {
                        Ok(_) => Ok(rusqlite::types::Value::Text(text.to_string())),
                        Err(e) => Err(PgSqliteError::Protocol(format!("Invalid NUMERIC: {}", e))),
                    }
                }
                790 => {
                    // MONEY - store as text
                    Ok(rusqlite::types::Value::Text(text.to_string()))
                }
                _ => {
                    // Default to TEXT
                    Ok(rusqlite::types::Value::Text(text.to_string()))
                }
            }
        } else {
            // Binary format
            match param_type {
                23 => {
                    // INT4
                    if bytes.len() == 4 {
                        let val = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i64;
                        Ok(rusqlite::types::Value::Integer(val))
                    } else {
                        Err(PgSqliteError::Protocol("Invalid INT4 binary format".to_string()))
                    }
                }
                20 => {
                    // INT8
                    if bytes.len() == 8 {
                        let val = i64::from_be_bytes([
                            bytes[0], bytes[1], bytes[2], bytes[3],
                            bytes[4], bytes[5], bytes[6], bytes[7]
                        ]);
                        Ok(rusqlite::types::Value::Integer(val))
                    } else {
                        Err(PgSqliteError::Protocol("Invalid INT8 binary format".to_string()))
                    }
                }
                700 => {
                    // FLOAT4
                    if bytes.len() == 4 {
                        let bits = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                        let val = f32::from_bits(bits) as f64;
                        Ok(rusqlite::types::Value::Real(val))
                    } else {
                        Err(PgSqliteError::Protocol("Invalid FLOAT4 binary format".to_string()))
                    }
                }
                701 => {
                    // FLOAT8
                    if bytes.len() == 8 {
                        let bits = u64::from_be_bytes([
                            bytes[0], bytes[1], bytes[2], bytes[3],
                            bytes[4], bytes[5], bytes[6], bytes[7]
                        ]);
                        let val = f64::from_bits(bits);
                        Ok(rusqlite::types::Value::Real(val))
                    } else {
                        Err(PgSqliteError::Protocol("Invalid FLOAT8 binary format".to_string()))
                    }
                }
                1700 => {
                    // NUMERIC
                    match DecimalHandler::decode_numeric(bytes) {
                        Ok(decimal) => Ok(rusqlite::types::Value::Text(decimal.to_string())),
                        Err(e) => Err(PgSqliteError::Protocol(format!("Invalid binary NUMERIC: {}", e))),
                    }
                }
                _ => {
                    // Store as BLOB for unsupported binary types
                    Ok(rusqlite::types::Value::Blob(bytes.to_vec()))
                }
            }
        }
    }
    
    async fn execute_select_with_params<T>(
        _framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        _session: &Arc<SessionState>,
        _portal_name: &str,
        query: &str,
        params: Vec<rusqlite::types::Value>,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // Use DbHandler's fast path method which has access to the connection
        let response = match db.try_execute_fast_path_with_params(query, &params).await {
            Ok(Some(resp)) => resp,
            Ok(None) => return Err(PgSqliteError::Protocol("Fast path failed".to_string())),
            Err(e) => return Err(PgSqliteError::Sqlite(e)),
        };
        
        // TODO: Implement proper response handling
        info!("Fast path SELECT executed, {} rows returned", response.rows.len());
        
        Ok(())
    }
    
    async fn execute_dml_with_params<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        query: &str,
        params: Vec<rusqlite::types::Value>,
        query_type: QueryType,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // Use DbHandler's fast path method
        let response = match db.try_execute_fast_path_with_params(query, &params).await {
            Ok(Some(resp)) => resp,
            Ok(None) => return Err(PgSqliteError::Protocol("Fast path failed".to_string())),
            Err(e) => return Err(PgSqliteError::Sqlite(e)),
        };
        
        // Send appropriate CommandComplete
        let tag = match query_type {
            QueryType::Insert => format!("INSERT 0 {}", response.rows_affected),
            QueryType::Update => format!("UPDATE {}", response.rows_affected),
            QueryType::Delete => format!("DELETE {}", response.rows_affected),
            _ => format!("OK {}", response.rows_affected),
        };
        
        framed.send(BackendMessage::CommandComplete { tag }).await
            .map_err(|e| PgSqliteError::Io(e))?;
        
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
    Other,
}

impl QueryType {
    pub fn from_query(query: &str) -> Self {
        let query_upper = query.trim().to_uppercase();
        if query_upper.starts_with("SELECT") {
            QueryType::Select
        } else if query_upper.starts_with("INSERT") {
            QueryType::Insert
        } else if query_upper.starts_with("UPDATE") {
            QueryType::Update
        } else if query_upper.starts_with("DELETE") {
            QueryType::Delete
        } else {
            QueryType::Other
        }
    }
}