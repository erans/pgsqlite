use crate::protocol::BackendMessage;
use crate::session::{DbHandler, SessionState};
use crate::types::{DecimalHandler, PgType};
use crate::cache::GLOBAL_PARAM_VALUE_CACHE;
use crate::PgSqliteError;
use tokio_util::codec::Framed;
use futures::SinkExt;
use std::sync::Arc;
use tracing::debug;

/// Optimized parameter binding that avoids string substitution
pub struct ExtendedFastPath;

impl ExtendedFastPath {
    /// Execute a parameterized query using prepared statements directly
    pub async fn execute_with_params<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &Arc<DbHandler>,
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
        eprintln!("🔍 DEBUG: ExtendedFastPath::execute_with_params called for query: {}", query);
        eprintln!("🔍 DEBUG:   bound_values.len(): {}, param_types: {:?}, original_types: {:?}", bound_values.len(), param_types, original_types);
        // Convert parameters to rusqlite values with caching, using original types for proper conversion
        let rusqlite_params = match Self::convert_parameters_cached(query, bound_values, param_formats, param_types, original_types) {
            Ok(params) => {
                eprintln!("🔍 DEBUG: Fast path parameter conversion succeeded, {} params", params.len());
                params
            },
            Err(e) => {
                eprintln!("🔍 DEBUG: Fast path parameter conversion FAILED: {}, falling back to normal path", e);
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
                    Ok(()) => {
                        eprintln!("🔍 DEBUG: Fast path SELECT succeeded");
                        Ok(true)
                    },
                    Err(e) => {
                        if e.to_string().contains("FastPathFallback") {
                            eprintln!("🔍 DEBUG: Fast path SELECT requesting fallback");
                            Ok(false) // Fall back to normal path
                        } else {
                            eprintln!("🔍 DEBUG: Fast path SELECT failed: {}, falling back", e);
                            Ok(false) // Fall back to normal path
                        }
                    }
                }
            }
            QueryType::Insert | QueryType::Update | QueryType::Delete => {
                match Self::execute_dml_with_params(framed, db, session, query, rusqlite_params, query_type).await {
                    Ok(()) => {
                        eprintln!("🔍 DEBUG: Fast path DML succeeded");
                        Ok(true)
                    },
                    Err(e) => {
                        if e.to_string().contains("FastPathFallback") {
                            eprintln!("🔍 DEBUG: Fast path DML requesting fallback");
                            Ok(false) // Fall back to normal path
                        } else {
                            eprintln!("🔍 DEBUG: Fast path DML failed: {}, falling back", e);
                            Ok(false) // Fall back to normal path
                        }
                    }
                }
            }
            _ => Ok(false), // Fall back for other query types
        }
    }
    
    /// Infer parameter types from CAST expressions and function calls in the query
    fn infer_types_from_query(query: &str, param_count: usize) -> Vec<i32> {
        let mut inferred_types = vec![0; param_count];
        
        debug!("Type inference: analyzing query '{}' for {} parameters", query, param_count);
        
        // Look for CAST($N AS TYPE) patterns
        for i in 1..=param_count {
            if let Some(cast_start) = query.find(&format!("CAST(${} AS ", i)) {
                let type_start = cast_start + format!("CAST(${} AS ", i).len();
                if let Some(type_end) = query[type_start..].find(')') {
                    let type_name = &query[type_start..type_start + type_end];
                    let type_oid = match type_name.to_uppercase().as_str() {
                        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => PgType::Timestamp.to_oid(),
                        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => PgType::Timestamptz.to_oid(),
                        "DATE" => PgType::Date.to_oid(),
                        "TIME" | "TIME WITHOUT TIME ZONE" => PgType::Time.to_oid(),
                        "TIMETZ" | "TIME WITH TIME ZONE" => PgType::Timetz.to_oid(),
                        "INTERVAL" => PgType::Interval.to_oid(),
                        "VARCHAR" | "TEXT" => PgType::Text.to_oid(),
                        "INTEGER" | "INT4" => PgType::Int4.to_oid(),
                        "BIGINT" | "INT8" => PgType::Int8.to_oid(),
                        "SMALLINT" | "INT2" => PgType::Int2.to_oid(),
                        "NUMERIC" | "DECIMAL" => PgType::Numeric.to_oid(),
                        "BOOLEAN" => PgType::Bool.to_oid(),
                        _ => 0, // Unknown type
                    };
                    if type_oid != 0 {
                        inferred_types[i - 1] = type_oid;
                        debug!("Inferred parameter ${} type as {} (OID {}) from CAST", i, type_name, type_oid);
                    }
                }
            }
            
            // Look for pgsqlite datetime function patterns
            let datetime_functions = [
                ("pg_timestamp_from_text($", PgType::Timestamp.to_oid()),
                ("pg_timestamptz_from_text($", PgType::Timestamptz.to_oid()),
                ("pg_date_from_text($", PgType::Date.to_oid()),
                ("pg_time_from_text($", PgType::Time.to_oid()),
                ("pg_timetz_from_text($", PgType::Timetz.to_oid()),
                ("pg_interval_from_text($", PgType::Interval.to_oid()),
            ];
            
            for (func_pattern, type_oid) in datetime_functions {
                let pattern = format!("{}{}", func_pattern, i);
                debug!("Checking for pattern '{}' in query", pattern);
                if query.contains(&pattern) {
                    inferred_types[i - 1] = type_oid;
                    debug!("Inferred parameter ${} type as datetime (OID {}) from function {}", i, type_oid, func_pattern);
                }
            }
        }
        
        debug!("Type inference result: {:?}", inferred_types);
        inferred_types
    }
    
    /// Convert parameters using cache to avoid repeated conversions
    fn convert_parameters_cached(
        query: &str,
        bound_values: &[Option<Vec<u8>>],
        param_formats: &[i16],
        _param_types: &[i32],
        original_types: &[i32],
    ) -> Result<Vec<rusqlite::types::Value>, PgSqliteError> {
        // First, try to infer types from CAST expressions in the query
        let inferred_types = Self::infer_types_from_query(query, bound_values.len());
        
        // Use inferred types where available, fall back to original types
        let effective_types: Vec<i32> = (0..bound_values.len())
            .map(|i| {
                if inferred_types[i] != 0 {
                    inferred_types[i]
                } else {
                    original_types.get(i).copied().unwrap_or(0)
                }
            })
            .collect();
        let mut params = Vec::with_capacity(bound_values.len());
        
        for (i, value) in bound_values.iter().enumerate() {
            match value {
                None => params.push(rusqlite::types::Value::Null),
                Some(bytes) => {
                    let format = param_formats.get(i).copied().unwrap_or(0);
                    let effective_type = effective_types[i];
                    
                    // Use cache for parameter value conversion, using effective type (includes CAST inference)
                    let converted = GLOBAL_PARAM_VALUE_CACHE.get_or_convert(
                        bytes,
                        effective_type,
                        format,
                        || Self::convert_parameter_value(bytes, format, effective_type)
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
        debug!("Converting parameter: {} bytes, format={} ({}), type_oid={}", 
               bytes.len(), format, if format == 0 { "text" } else { "binary" }, param_type);
        
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
                        .map_err(|_| PgSqliteError::Protocol(format!("Invalid integer: {text}")))
                }
                t if t == PgType::Float4.to_oid() || t == PgType::Float8.to_oid() => {
                    // FLOAT4, FLOAT8
                    text.parse::<f64>()
                        .map(rusqlite::types::Value::Real)
                        .map_err(|_| PgSqliteError::Protocol(format!("Invalid float: {text}")))
                }
                t if t == PgType::Numeric.to_oid() => {
                    // NUMERIC - validate and store as text
                    match DecimalHandler::validate_numeric_string(text) {
                        Ok(_) => Ok(rusqlite::types::Value::Text(text.to_string())),
                        Err(e) => Err(PgSqliteError::Protocol(format!("Invalid NUMERIC: {e}"))),
                    }
                }
                t if t == PgType::Date.to_oid() => {
                    // DATE - convert to days since epoch (INTEGER)
                    match crate::types::ValueConverter::convert_date_to_unix(text) {
                        Ok(days_str) => {
                            let days = days_str.parse::<i64>()
                                .map_err(|_| PgSqliteError::Protocol(format!("Invalid date days: {days_str}")))?;
                            Ok(rusqlite::types::Value::Integer(days))
                        }
                        Err(e) => Err(PgSqliteError::Protocol(format!("Invalid date: {e}")))
                    }
                }
                t if t == PgType::Time.to_oid() => {
                    // TIME - convert to microseconds since midnight (INTEGER)
                    match crate::types::ValueConverter::convert_time_to_seconds(text) {
                        Ok(micros_str) => {
                            let micros = micros_str.parse::<i64>()
                                .map_err(|_| PgSqliteError::Protocol(format!("Invalid time microseconds: {micros_str}")))?;
                            Ok(rusqlite::types::Value::Integer(micros))
                        }
                        Err(e) => Err(PgSqliteError::Protocol(format!("Invalid time: {e}")))
                    }
                }
                t if t == PgType::Timestamp.to_oid() => {
                    // TIMESTAMP - convert to microseconds since epoch (INTEGER)
                    match crate::types::ValueConverter::convert_timestamp_to_unix(text) {
                        Ok(micros_str) => {
                            let micros = micros_str.parse::<i64>()
                                .map_err(|_| PgSqliteError::Protocol(format!("Invalid timestamp microseconds: {micros_str}")))?;
                            Ok(rusqlite::types::Value::Integer(micros))
                        }
                        Err(e) => Err(PgSqliteError::Protocol(format!("Invalid timestamp: {e}")))
                    }
                }
                t if t == PgType::Timestamptz.to_oid() || t == PgType::Timetz.to_oid() || t == PgType::Interval.to_oid() => {
                    // Other datetime types - convert to INTEGER (microseconds)
                    // For now, store as text until we implement proper conversion
                    // TODO: Implement proper conversion for TIMESTAMPTZ, TIMETZ, INTERVAL
                    Ok(rusqlite::types::Value::Text(text.to_string()))
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
                t if t == PgType::Int2.to_oid() => {
                    // INT2 - accept 2, 4, or 8 byte integers with appropriate conversion
                    match bytes.len() {
                        2 => {
                            // Standard INT2
                            let val = i16::from_be_bytes([bytes[0], bytes[1]]) as i64;
                            Ok(rusqlite::types::Value::Integer(val))
                        }
                        4 => {
                            // INT4 -> INT2: check if it fits in INT2 range
                            let val = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                            if val >= i16::MIN as i32 && val <= i16::MAX as i32 {
                                Ok(rusqlite::types::Value::Integer(val as i64))
                            } else {
                                Err(PgSqliteError::Protocol(format!("INT4 value {} too large for INT2", val)))
                            }
                        }
                        8 => {
                            // INT8 -> INT2: check if it fits in INT2 range
                            let val = i64::from_be_bytes([
                                bytes[0], bytes[1], bytes[2], bytes[3],
                                bytes[4], bytes[5], bytes[6], bytes[7]
                            ]);
                            if val >= i16::MIN as i64 && val <= i16::MAX as i64 {
                                Ok(rusqlite::types::Value::Integer(val))
                            } else {
                                Err(PgSqliteError::Protocol(format!("INT8 value {} too large for INT2", val)))
                            }
                        }
                        _ => {
                            Err(PgSqliteError::Protocol(format!("Invalid INT2 binary format: {} bytes", bytes.len())))
                        }
                    }
                }
                t if t == PgType::Int4.to_oid() => {
                    // INT4 - accept 2, 4, or 8 byte integers with appropriate conversion
                    match bytes.len() {
                        2 => {
                            // INT2 -> INT4: sign extend from 16-bit to 32-bit
                            let val = i16::from_be_bytes([bytes[0], bytes[1]]) as i64;
                            debug!("Converting 2-byte binary to INT4: {}", val);
                            Ok(rusqlite::types::Value::Integer(val))
                        }
                        4 => {
                            // Standard INT4
                            let val = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i64;
                            Ok(rusqlite::types::Value::Integer(val))
                        }
                        8 => {
                            // INT8 -> INT4: check if it fits in INT4 range
                            let val = i64::from_be_bytes([
                                bytes[0], bytes[1], bytes[2], bytes[3],
                                bytes[4], bytes[5], bytes[6], bytes[7]
                            ]);
                            if val >= i32::MIN as i64 && val <= i32::MAX as i64 {
                                Ok(rusqlite::types::Value::Integer(val))
                            } else {
                                Err(PgSqliteError::Protocol(format!("INT8 value {} too large for INT4", val)))
                            }
                        }
                        _ => {
                            Err(PgSqliteError::Protocol(format!("Invalid INT4 binary format: {} bytes", bytes.len())))
                        }
                    }
                }
                t if t == PgType::Int8.to_oid() => {
                    // INT8 - accept 2, 4, or 8 byte integers with appropriate sign extension
                    match bytes.len() {
                        2 => {
                            // INT2 -> INT8: sign extend from 16-bit to 64-bit
                            let val = i16::from_be_bytes([bytes[0], bytes[1]]) as i64;
                            debug!("Converting 2-byte binary to INT8: {}", val);
                            Ok(rusqlite::types::Value::Integer(val))
                        }
                        4 => {
                            // INT4 -> INT8: sign extend from 32-bit to 64-bit
                            let val = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i64;
                            debug!("Converting 4-byte binary to INT8: {}", val);
                            Ok(rusqlite::types::Value::Integer(val))
                        }
                        8 => {
                            // Standard INT8
                            let val = i64::from_be_bytes([
                                bytes[0], bytes[1], bytes[2], bytes[3],
                                bytes[4], bytes[5], bytes[6], bytes[7]
                            ]);
                            Ok(rusqlite::types::Value::Integer(val))
                        }
                        _ => {
                            Err(PgSqliteError::Protocol(format!("Invalid INT8 binary format: {} bytes", bytes.len())))
                        }
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
                        Err(e) => Err(PgSqliteError::Protocol(format!("Invalid binary NUMERIC: {e}"))),
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
                        let text = format!("${dollars:.2}");
                        Ok(rusqlite::types::Value::Text(text))
                    } else {
                        Err(PgSqliteError::Protocol(format!("Invalid MONEY format, {} bytes", bytes.len())))
                    }
                }
                t if t == PgType::Text.to_oid() || t == PgType::Varchar.to_oid() => {
                    // TEXT/VARCHAR - binary format is just UTF-8 bytes
                    match std::str::from_utf8(bytes) {
                        Ok(text) => Ok(rusqlite::types::Value::Text(text.to_string())),
                        Err(_) => {
                            // Invalid UTF-8, store as blob
                            Ok(rusqlite::types::Value::Blob(bytes.to_vec()))
                        }
                    }
                }
                t if t == PgType::Date.to_oid() => {
                    // DATE - 4 bytes, days since 2000-01-01
                    if bytes.len() == 4 {
                        let days_since_2000 = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                        // Convert to days since 1970-01-01 (Unix epoch)
                        let days_since_1970 = days_since_2000 + 10957; // 10957 days between 1970-01-01 and 2000-01-01
                        Ok(rusqlite::types::Value::Integer(days_since_1970 as i64))
                    } else {
                        Err(PgSqliteError::Protocol("Invalid DATE binary format".to_string()))
                    }
                }
                t if t == PgType::Time.to_oid() || t == PgType::Timetz.to_oid() => {
                    // TIME - 8 bytes, microseconds since midnight
                    if bytes.len() == 8 {
                        let micros = i64::from_be_bytes([
                            bytes[0], bytes[1], bytes[2], bytes[3],
                            bytes[4], bytes[5], bytes[6], bytes[7]
                        ]);
                        Ok(rusqlite::types::Value::Integer(micros))
                    } else {
                        Err(PgSqliteError::Protocol("Invalid TIME binary format".to_string()))
                    }
                }
                t if t == PgType::Timestamp.to_oid() || t == PgType::Timestamptz.to_oid() => {
                    // TIMESTAMP - 8 bytes, microseconds since 2000-01-01
                    if bytes.len() == 8 {
                        let pg_micros = i64::from_be_bytes([
                            bytes[0], bytes[1], bytes[2], bytes[3],
                            bytes[4], bytes[5], bytes[6], bytes[7]
                        ]);
                        // Convert to microseconds since 1970-01-01 (Unix epoch)
                        const PG_EPOCH_OFFSET: i64 = 946684800 * 1_000_000; // microseconds between 1970-01-01 and 2000-01-01
                        let unix_micros = pg_micros + PG_EPOCH_OFFSET;
                        Ok(rusqlite::types::Value::Integer(unix_micros))
                    } else {
                        Err(PgSqliteError::Protocol("Invalid TIMESTAMP binary format".to_string()))
                    }
                }
                t if t == PgType::Interval.to_oid() => {
                    // INTERVAL - 16 bytes: 8 bytes microseconds + 4 bytes days + 4 bytes months
                    if bytes.len() == 16 {
                        let micros = i64::from_be_bytes([
                            bytes[0], bytes[1], bytes[2], bytes[3],
                            bytes[4], bytes[5], bytes[6], bytes[7]
                        ]);
                        let days = i32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
                        let _months = i32::from_be_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
                        
                        // Convert to total microseconds (simple intervals only)
                        let total_micros = micros + (days as i64 * 86400 * 1_000_000);
                        Ok(rusqlite::types::Value::Integer(total_micros))
                    } else {
                        Err(PgSqliteError::Protocol("Invalid INTERVAL binary format".to_string()))
                    }
                }
                t if t == PgType::Macaddr.to_oid() || t == PgType::Macaddr8.to_oid() || t == PgType::Inet.to_oid() ||
                     t == PgType::Cidr.to_oid() || t == PgType::Int4range.to_oid() || t == PgType::Int8range.to_oid() ||
                     t == PgType::Numrange.to_oid() || t == PgType::Bit.to_oid() || t == PgType::Varbit.to_oid() => {
                    // Other special types - for now, error out so we can implement them properly
                    Err(PgSqliteError::Protocol(format!("Binary format not implemented for type {param_type}")))
                }
                0 => {
                    // Type not specified (OID 0) - try to infer from binary format
                    if bytes.len() == 8 {
                        // 8 bytes could be INT8, FLOAT8, TIME, or TIMESTAMP
                        // For now, try to parse as timestamp (common in SQLAlchemy)
                        let pg_micros = i64::from_be_bytes([
                            bytes[0], bytes[1], bytes[2], bytes[3],
                            bytes[4], bytes[5], bytes[6], bytes[7]
                        ]);
                        // Check if this looks like a PostgreSQL timestamp (year 1900-2100)
                        const PG_EPOCH_OFFSET: i64 = 946684800 * 1_000_000;
                        let unix_micros = pg_micros + PG_EPOCH_OFFSET;
                        let seconds = unix_micros / 1_000_000;
                        
                        // If it's a reasonable timestamp (between year 1970 and 2100), treat as timestamp
                        if seconds >= 0 && seconds < 4102444800 { // 2100-01-01
                            debug!("Inferring 8-byte binary parameter as TIMESTAMP: {} microseconds since Unix epoch", unix_micros);
                            Ok(rusqlite::types::Value::Integer(unix_micros))
                        } else {
                            // Might be INT8 or something else, try parsing as INT8
                            debug!("Inferring 8-byte binary parameter as INT8: {}", pg_micros);
                            Ok(rusqlite::types::Value::Integer(pg_micros))
                        }
                    } else if bytes.len() == 4 {
                        // 4 bytes could be INT4 or FLOAT4, try INT4 first
                        let val = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i64;
                        debug!("Inferring 4-byte binary parameter as INT4: {}", val);
                        Ok(rusqlite::types::Value::Integer(val))
                    } else if bytes.len() == 2 {
                        // 2 bytes is likely INT2
                        let val = i16::from_be_bytes([bytes[0], bytes[1]]) as i64;
                        debug!("Inferring 2-byte binary parameter as INT2: {}", val);
                        Ok(rusqlite::types::Value::Integer(val))
                    } else {
                        // Unknown size, store as BLOB
                        debug!("Unknown binary parameter type (OID 0), {} bytes, storing as blob", bytes.len());
                        Ok(rusqlite::types::Value::Blob(bytes.to_vec()))
                    }
                }
                _ => {
                    // Store as BLOB for unsupported binary types
                    debug!("Unknown binary type OID {param_type}, storing as blob. Bytes: {} bytes", bytes.len());
                    Ok(rusqlite::types::Value::Blob(bytes.to_vec()))
                }
            }
        }
    }
    
    async fn execute_select_with_params<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &Arc<DbHandler>,
        session: &Arc<SessionState>,
        portal_name: &str,
        query: &str,
        params: Vec<rusqlite::types::Value>,
        result_formats: &[i16],
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // Use DbHandler's fast path method which has access to the connection
        let response = match db.try_execute_fast_path_with_params(query, &params, &session.id).await {
            Ok(Some(resp)) => {
                eprintln!("🔍 DEBUG: DbHandler fast path succeeded for SELECT");
                resp
            },
            Ok(None) => {
                eprintln!("🔍 DEBUG: DbHandler fast path returned None for SELECT (not implemented), falling back");
                return Err(PgSqliteError::Protocol("FastPathFallback".to_string()));
            },
            Err(e) => {
                eprintln!("🔍 DEBUG: DbHandler fast path errored for SELECT: {}", e);
                return Err(e);
            },
        };
        
        // Check if we need to send RowDescription
        let send_row_desc = {
            let portals = session.portals.read().await;
            let portal = portals.get(portal_name).unwrap();
            let statements = session.prepared_statements.read().await;
            let stmt = statements.get(&portal.statement_name).unwrap();
            stmt.field_descriptions.is_empty() && !response.columns.is_empty()
        };
        
        if send_row_desc {
            // Build field descriptions based on the response columns and inferred types
            let portal_inferred_types = {
                let portals = session.portals.read().await;
                let portal = portals.get(portal_name).unwrap();
                portal.inferred_param_types.clone()
            };
            
            let fields: Vec<crate::protocol::FieldDescription> = response.columns.iter()
                .enumerate()
                .map(|(i, col_name)| {
                    // For parameter columns, use inferred type
                    let type_oid = if col_name.starts_with('$') || col_name == "?column?" || col_name == "NULL" {
                        if let Some(ref inferred_types) = portal_inferred_types {
                            let param_idx = if col_name.starts_with('$') {
                                col_name[1..].parse::<usize>().ok().map(|n| n - 1).unwrap_or(i)
                            } else {
                                i
                            };
                            *inferred_types.get(param_idx).unwrap_or(&PgType::Text.to_oid())
                        } else {
                            PgType::Text.to_oid()
                        }
                    } else {
                        PgType::Text.to_oid() // Default for other columns
                    };
                    
                    crate::protocol::FieldDescription {
                        name: col_name.clone(),
                        table_oid: 0,
                        column_id: (i + 1) as i16,
                        type_oid,
                        type_size: -1,
                        type_modifier: -1,
                        format: if result_formats.is_empty() {
                            0
                        } else if result_formats.len() == 1 {
                            result_formats[0]
                        } else {
                            *result_formats.get(i).unwrap_or(&0)
                        },
                    }
                })
                .collect();
            
            framed.send(BackendMessage::RowDescription(fields)).await
                .map_err(PgSqliteError::Io)?;
        }
        
        // TODO: Handle result_formats for binary encoding
        // For now, we only support text format (handled by falling back earlier)
        let _ = result_formats; // Suppress unused warning
        
        // Send data rows
        for row in response.rows {
            framed.send(BackendMessage::DataRow(row)).await
                .map_err(PgSqliteError::Io)?;
        }
        
        // Send CommandComplete
        let tag = format!("SELECT {}", response.rows_affected);
        framed.send(BackendMessage::CommandComplete { tag }).await
            .map_err(PgSqliteError::Io)?;
        
        Ok(())
    }
    
    async fn execute_dml_with_params<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &Arc<DbHandler>,
        session: &Arc<SessionState>,
        query: &str,
        params: Vec<rusqlite::types::Value>,
        query_type: QueryType,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // Use DbHandler's fast path method
        let response = match db.try_execute_fast_path_with_params(query, &params, &session.id).await {
            Ok(Some(resp)) => {
                eprintln!("🔍 DEBUG: DbHandler fast path succeeded");
                resp
            },
            Ok(None) => {
                eprintln!("🔍 DEBUG: DbHandler fast path returned None (not implemented), falling back");
                return Err(PgSqliteError::Protocol("FastPathFallback".to_string()));
            },
            Err(e) => {
                eprintln!("🔍 DEBUG: DbHandler fast path errored: {}", e);
                return Err(e);
            },
        };
        
        // For queries with RETURNING clause, send RowDescription and DataRows
        if query.contains("RETURNING") && !response.columns.is_empty() {
            // Send RowDescription
            let fields: Vec<crate::protocol::FieldDescription> = response.columns.iter()
                .enumerate()
                .map(|(i, col_name)| {
                    crate::protocol::FieldDescription {
                        name: col_name.clone(),
                        table_oid: 0,
                        column_id: (i + 1) as i16,
                        type_oid: PgType::Text.to_oid(), // Default to text for RETURNING columns
                        type_size: -1,
                        type_modifier: -1,
                        format: 0, // Text format
                    }
                })
                .collect();
            
            framed.send(BackendMessage::RowDescription(fields)).await
                .map_err(PgSqliteError::Io)?;
            
            // Send data rows
            for row in response.rows {
                framed.send(BackendMessage::DataRow(row)).await
                    .map_err(PgSqliteError::Io)?;
            }
        }
        
        // Send appropriate CommandComplete
        let tag = match query_type {
            QueryType::Insert => format!("INSERT 0 {}", response.rows_affected),
            QueryType::Update => format!("UPDATE {}", response.rows_affected),
            QueryType::Delete => format!("DELETE {}", response.rows_affected),
            _ => format!("OK {}", response.rows_affected),
        };
        
        framed.send(BackendMessage::CommandComplete { tag }).await
            .map_err(PgSqliteError::Io)?;
        
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