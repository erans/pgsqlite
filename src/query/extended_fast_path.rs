use crate::protocol::BackendMessage;
use crate::session::{DbHandler, SessionState};
use crate::types::{DecimalHandler, PgType};
use crate::cache::GLOBAL_PARAM_VALUE_CACHE;
use crate::PgSqliteError;
use tokio_util::codec::Framed;
use futures::SinkExt;
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
        result_formats: &[i16],
        param_types: &[i32],
        original_types: &[i32],
        query_type: QueryType,
    ) -> Result<bool, PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // Convert parameters to rusqlite values with caching, using original types for proper conversion
        let rusqlite_params = match Self::convert_parameters_cached(bound_values, param_formats, param_types, original_types) {
            Ok(params) => params,
            Err(_) => {
                // Parameter conversion failed, fall back to normal path
                return Ok(false); // Fall back to normal path
            }
        };
        
        // Execute based on query type
        match query_type {
            QueryType::Select => {
                // Fast check for binary result formats - optimize for common case
                // Most queries use text format (empty or [0])
                // Check first element as most queries have uniform format
                if !result_formats.is_empty() && result_formats[0] == 1 {
                    // Fall back to normal path for binary results
                    // TODO: Implement proper binary encoding for result formats
                    return Ok(false);
                }
                match Self::execute_select_with_params(framed, db, session, portal_name, query, rusqlite_params, result_formats).await {
                    Ok(()) => Ok(true),
                    Err(_) => {
                        // Fast path SELECT failed, fall back
                        Ok(false) // Fall back to normal path
                    }
                }
            }
            QueryType::Insert | QueryType::Update | QueryType::Delete => {
                match Self::execute_dml_with_params(framed, db, query, rusqlite_params, query_type).await {
                    Ok(()) => Ok(true),
                    Err(_) => {
                        // Fast path DML failed, fall back
                        Ok(false) // Fall back to normal path
                    }
                }
            }
            _ => Ok(false), // Fall back for other query types
        }
    }
    
    /// Convert parameters using cache to avoid repeated conversions
    fn convert_parameters_cached(
        bound_values: &[Option<Vec<u8>>],
        param_formats: &[i16],
        param_types: &[i32],
        original_types: &[i32],
    ) -> Result<Vec<rusqlite::types::Value>, PgSqliteError> {
        let mut params = Vec::with_capacity(bound_values.len());
        
        for (i, value) in bound_values.iter().enumerate() {
            match value {
                None => params.push(rusqlite::types::Value::Null),
                Some(bytes) => {
                    let format = param_formats.get(i).copied().unwrap_or(0);
                    let param_type = param_types.get(i).copied().unwrap_or(PgType::Text.to_oid()); // Default to TEXT
                    let original_type = original_types.get(i).copied().unwrap_or(param_type);
                    
                    // Use cache for parameter value conversion, using original type for conversion
                    let converted = GLOBAL_PARAM_VALUE_CACHE.get_or_convert(
                        bytes,
                        original_type,
                        format,
                        || Self::convert_parameter_value(bytes, format, original_type)
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
                t if t == PgType::Bool.to_oid() => {
                    // BOOL
                    let val = match text {
                        "t" | "true" | "TRUE" | "1" => 1,
                        _ => 0,
                    };
                    Ok(rusqlite::types::Value::Integer(val))
                }
                t if t == PgType::Int8.to_oid() || t == PgType::Int4.to_oid() || t == PgType::Int2.to_oid() => {
                    // INT8, INT4, INT2
                    text.parse::<i64>()
                        .map(rusqlite::types::Value::Integer)
                        .map_err(|_| PgSqliteError::Protocol(format!("Invalid integer: {}", text)))
                }
                t if t == PgType::Float4.to_oid() || t == PgType::Float8.to_oid() => {
                    // FLOAT4, FLOAT8
                    text.parse::<f64>()
                        .map(rusqlite::types::Value::Real)
                        .map_err(|_| PgSqliteError::Protocol(format!("Invalid float: {}", text)))
                }
                t if t == PgType::Numeric.to_oid() => {
                    // NUMERIC - validate and store as text
                    match DecimalHandler::validate_numeric_string(text) {
                        Ok(_) => Ok(rusqlite::types::Value::Text(text.to_string())),
                        Err(e) => Err(PgSqliteError::Protocol(format!("Invalid NUMERIC: {}", e))),
                    }
                }
                t if t == PgType::Money.to_oid() || t == PgType::Macaddr.to_oid() || t == PgType::Macaddr8.to_oid() ||
                     t == PgType::Inet.to_oid() || t == PgType::Cidr.to_oid() || t == PgType::Int4range.to_oid() ||
                     t == PgType::Int8range.to_oid() || t == PgType::Numrange.to_oid() || t == PgType::Bit.to_oid() ||
                     t == PgType::Varbit.to_oid() => {
                    // Special types that are mapped to TEXT
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
                t if t == PgType::Int4.to_oid() => {
                    // INT4
                    if bytes.len() == 4 {
                        let val = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i64;
                        Ok(rusqlite::types::Value::Integer(val))
                    } else {
                        Err(PgSqliteError::Protocol("Invalid INT4 binary format".to_string()))
                    }
                }
                t if t == PgType::Int8.to_oid() => {
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
                t if t == PgType::Float4.to_oid() => {
                    // FLOAT4
                    if bytes.len() == 4 {
                        let bits = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                        let val = f32::from_bits(bits) as f64;
                        Ok(rusqlite::types::Value::Real(val))
                    } else {
                        Err(PgSqliteError::Protocol("Invalid FLOAT4 binary format".to_string()))
                    }
                }
                t if t == PgType::Float8.to_oid() => {
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
                t if t == PgType::Numeric.to_oid() => {
                    // NUMERIC
                    match DecimalHandler::decode_numeric(bytes) {
                        Ok(decimal) => Ok(rusqlite::types::Value::Text(decimal.to_string())),
                        Err(e) => Err(PgSqliteError::Protocol(format!("Invalid binary NUMERIC: {}", e))),
                    }
                }
                t if t == PgType::Money.to_oid() => {
                    // MONEY - tokio-postgres sends text even when format is marked as binary
                    // Try to parse as text first
                    if let Ok(text) = std::str::from_utf8(bytes) {
                        Ok(rusqlite::types::Value::Text(text.to_string()))
                    } else if bytes.len() == 8 {
                        // Fallback to binary format (int64 representing cents * 100)
                        let microdollars = i64::from_be_bytes([
                            bytes[0], bytes[1], bytes[2], bytes[3],
                            bytes[4], bytes[5], bytes[6], bytes[7]
                        ]);
                        // Convert microdollars to dollar string format
                        let dollars = microdollars as f64 / 100.0;
                        let text = format!("${:.2}", dollars);
                        Ok(rusqlite::types::Value::Text(text))
                    } else {
                        Err(PgSqliteError::Protocol(format!("Invalid MONEY format, {} bytes", bytes.len())))
                    }
                }
                t if t == PgType::Macaddr.to_oid() || t == PgType::Macaddr8.to_oid() || t == PgType::Inet.to_oid() ||
                     t == PgType::Cidr.to_oid() || t == PgType::Int4range.to_oid() || t == PgType::Int8range.to_oid() ||
                     t == PgType::Numrange.to_oid() || t == PgType::Bit.to_oid() || t == PgType::Varbit.to_oid() => {
                    // Other special types - for now, error out so we can implement them properly
                    Err(PgSqliteError::Protocol(format!("Binary format not implemented for type {}", param_type)))
                }
                _ => {
                    // Store as BLOB for unsupported binary types
                    Ok(rusqlite::types::Value::Blob(bytes.to_vec()))
                }
            }
        }
    }
    
    async fn execute_select_with_params<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        _session: &Arc<SessionState>,
        _portal_name: &str,
        query: &str,
        params: Vec<rusqlite::types::Value>,
        result_formats: &[i16],
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
        
        // TODO: Handle result_formats for binary encoding
        // For now, we only support text format (handled by falling back earlier)
        let _ = result_formats; // Suppress unused warning
        
        // Send data rows
        for row in response.rows {
            framed.send(BackendMessage::DataRow(row)).await
                .map_err(|e| PgSqliteError::Io(e))?;
        }
        
        // Send CommandComplete
        let tag = format!("SELECT {}", response.rows_affected);
        framed.send(BackendMessage::CommandComplete { tag }).await
            .map_err(|e| PgSqliteError::Io(e))?;
        
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
        let query_trimmed = query.trim();
        // Use case-insensitive comparison to avoid expensive to_uppercase()
        let first_chars = query_trimmed.as_bytes();
        if first_chars.len() >= 6 {
            match &first_chars[0..6] {
                b"SELECT" | b"select" | b"Select" => return QueryType::Select,
                b"INSERT" | b"insert" | b"Insert" => return QueryType::Insert,
                b"UPDATE" | b"update" | b"Update" => return QueryType::Update,
                b"DELETE" | b"delete" | b"Delete" => return QueryType::Delete,
                _ => {}
            }
        }
        // Check mixed case or shorter queries
        let query_start = &query_trimmed[..query_trimmed.len().min(6)];
        if query_start.eq_ignore_ascii_case("SELECT") {
            QueryType::Select
        } else if query_start.eq_ignore_ascii_case("INSERT") {
            QueryType::Insert
        } else if query_start.eq_ignore_ascii_case("UPDATE") {
            QueryType::Update
        } else if query_start.eq_ignore_ascii_case("DELETE") {
            QueryType::Delete
        } else {
            QueryType::Other
        }
    }
}