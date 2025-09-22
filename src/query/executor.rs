use crate::protocol::{BackendMessage, FieldDescription};
use crate::session::{DbHandler, SessionState, QueryRouter};
use crate::translator::{JsonTranslator, ReturningTranslator, BatchUpdateTranslator, BatchDeleteTranslator, FtsTranslator};
use crate::types::PgType;
use crate::cache::{RowDescriptionKey, GLOBAL_ROW_DESCRIPTION_CACHE};
use crate::metadata::EnumTriggers;
use crate::PgSqliteError;
use crate::query::join_type_inference::build_column_to_table_mapping;
use tokio_util::codec::Framed;
use futures::SinkExt;
use tokio::io::AsyncWriteExt;
use tracing::{info, debug};
use std::sync::Arc;
use rusqlite::params;
use serde_json;
use std::collections::HashMap;
use parking_lot::RwLock;
use once_cell::sync::Lazy;
use uuid::Uuid;

/// Combined schema information for a table
#[derive(Clone)]
struct TableSchemaInfo {
    boolean_columns: std::collections::HashSet<String>,
    datetime_columns: std::collections::HashMap<String, String>,
    column_types: std::collections::HashMap<String, String>,
    enum_columns: std::collections::HashMap<String, String>, // column_name -> enum_type
}

/// Cache for table schema information to avoid repeated database queries
static TABLE_SCHEMA_CACHE: Lazy<RwLock<HashMap<String, TableSchemaInfo>>> = 
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Get all schema information for a table in one query
async fn get_table_schema_info(table_name: &str, db: &Arc<DbHandler>, session_id: &Uuid) -> TableSchemaInfo {
    // Check cache first
    {
        let cache = TABLE_SCHEMA_CACHE.read();
        if let Some(cached_info) = cache.get(table_name) {
            return cached_info.clone();
        }
    }
    
    // Cache miss - query the database once for all info
    let mut schema_info = TableSchemaInfo {
        boolean_columns: std::collections::HashSet::new(),
        datetime_columns: std::collections::HashMap::new(),
        column_types: std::collections::HashMap::new(),
        enum_columns: std::collections::HashMap::new(),
    };
    
    // Use session connection to query schema information
    if let Ok(()) = db.with_session_connection(session_id, |conn| {
        if let Ok(mut stmt) = conn.prepare("SELECT column_name, pg_type FROM __pgsqlite_schema WHERE table_name = ?1")
            && let Ok(rows) = stmt.query_map([table_name], |row| {
                let col_name: String = row.get(0)?;
                let pg_type: String = row.get(1)?;
                Ok((col_name, pg_type))
            }) {
                for row in rows.flatten() {
                    let (col_name, pg_type) = row;
                    
                    // Store all column types
                    schema_info.column_types.insert(col_name.clone(), pg_type.clone());
                    
                    // Check if boolean
                    if pg_type.eq_ignore_ascii_case("boolean") || pg_type.eq_ignore_ascii_case("bool") {
                        schema_info.boolean_columns.insert(col_name.clone());
                    }
                    
                    // Check if datetime
                    let pg_type_lower = pg_type.to_lowercase();
                    if pg_type_lower == "date" || pg_type_lower == "time" || pg_type_lower == "timetz" ||
                       pg_type_lower == "timestamp" || pg_type_lower == "timestamptz" ||
                       pg_type_lower == "time without time zone" || pg_type_lower == "time with time zone" ||
                       pg_type_lower == "timestamp without time zone" || pg_type_lower == "timestamp with time zone" {
                        schema_info.datetime_columns.insert(col_name.clone(), pg_type_lower.clone());
                    }
                    
                    // Check if enum - enum types are stored with their actual type name (e.g., "status", "priority")
                    // not as standard PostgreSQL types
                    if !matches!(pg_type_lower.as_str(), 
                        "integer" | "int" | "int4" | "int8" | "bigint" | "smallint" | "int2" |
                        "real" | "float4" | "double precision" | "float8" | 
                        "text" | "varchar" | "char" | "character varying" | "character" |
                        "boolean" | "bool" |
                        "date" | "time" | "timetz" | "timestamp" | "timestamptz" |
                        "time without time zone" | "time with time zone" |
                        "timestamp without time zone" | "timestamp with time zone" |
                        "numeric" | "decimal" | "uuid" | "json" | "jsonb" | "bytea" | "blob") {
                        // This is likely an enum type
                        schema_info.enum_columns.insert(col_name, pg_type);
                    }
                }
            }
        Ok::<(), rusqlite::Error>(())
    }).await {
        // Successfully populated schema info
    }
    
    // Cache the result
    {
        let mut cache = TABLE_SCHEMA_CACHE.write();
        cache.insert(table_name.to_string(), schema_info.clone());
    }
    
    schema_info
}


/// Create a command complete tag with optimized static strings for common cases
fn create_command_tag(operation: &str, rows_affected: usize) -> String {
    match (operation, rows_affected) {
        // Optimized static strings for most common cases (0/1 rows affected)
        ("INSERT", 0) => "INSERT 0 0".to_string(),
        ("INSERT", 1) => "INSERT 0 1".to_string(),
        ("UPDATE", 0) => "UPDATE 0".to_string(),
        ("UPDATE", 1) => "UPDATE 1".to_string(),
        ("DELETE", 0) => "DELETE 0".to_string(),
        ("DELETE", 1) => "DELETE 1".to_string(),
        // Format for all other cases
        ("INSERT", n) => format!("INSERT 0 {n}"),
        (op, n) => format!("{op} {n}"),
    }
}

pub struct QueryExecutor;

impl QueryExecutor {
    /// Get cached connection or fetch and cache it
    async fn get_or_cache_connection(
        session: &Arc<SessionState>,
        db: &Arc<DbHandler>
    ) -> Option<Arc<parking_lot::Mutex<rusqlite::Connection>>> {
        // First check if we have a cached connection
        if let Some(cached) = session.get_cached_connection() {
            // debug!("Using cached connection for session {}", session.id);
            return Some(cached);
        }
        
        // Try to get connection from manager and cache it
        // debug!("Connection not cached for session {}, fetching from manager", session.id);
        if let Some(conn_arc) = db.connection_manager().get_connection_arc(&session.id) {
            session.cache_connection(conn_arc.clone());
            // debug!("Cached connection for session {}", session.id);
            Some(conn_arc)
        } else {
            // debug!("No connection found for session {}", session.id);
            None
        }
    }
    pub async fn execute_query<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &Arc<DbHandler>,
        session: &Arc<SessionState>,
        query: &str,
        query_router: Option<&Arc<QueryRouter>>,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        println!("EXECUTOR: execute_query called with: '{}'", query);
        // Executing query
        
        // Strip SQL comments first to avoid parsing issues
        let cleaned_query = crate::query::strip_sql_comments(query);
        let query_to_execute = cleaned_query.trim();
        
        // Check if query is empty after comment stripping
        if query_to_execute.is_empty() {
            return Err(PgSqliteError::Protocol("Empty query".to_string()));
        }
        
        // Handle PostgreSQL DEALLOCATE commands (used for prepared statement cleanup)
        let query_upper = query_to_execute.to_uppercase();
        if query_upper.starts_with("DEALLOCATE") {
            debug!("DEALLOCATE command - treating as successful no-op (SQLite manages prepared statements automatically)");
            // Send CommandComplete for successful DEALLOCATE
            let msg = BackendMessage::CommandComplete { tag: "DEALLOCATE".to_string() };
            framed.send(msg).await.map_err(PgSqliteError::Io)?;
            return Ok(());
        }
        
        // debug!("Executing query: {}", query_to_execute);
        
        // Check for Python-style parameters and provide helpful error
        use crate::query::parameter_parser::ParameterParser;
        let python_params = ParameterParser::find_python_parameters(query_to_execute);
        if !python_params.is_empty() {
            let error_msg = format!(
                "Python-style parameters detected: {python_params:?}. pgsqlite requires parameter values to be substituted before execution. This usually means psycopg2 client-side substitution failed. Please ensure parameters are properly bound when executing the query."
            );
            debug!("⚠️  {}", error_msg);
            debug!("Query: {}", query_to_execute);
            return Err(PgSqliteError::Protocol(error_msg));
        }
        
        // Check if query contains multiple statements
        let trimmed = query_to_execute.trim();
        if trimmed.contains(';') {
            // Split by semicolon and execute each statement
            let statements: Vec<&str> = trimmed.split(';')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();
            
            // Handle empty query case (just semicolon) - SQLAlchemy uses ";" for ping
            if statements.is_empty() {
                debug!("Empty query (just semicolon) - treating as successful no-op");
                // Send CommandComplete for successful empty query
                let msg = BackendMessage::CommandComplete { tag: "SELECT 0".to_string() };
                framed.send(msg).await.map_err(PgSqliteError::Io)?;
                return Ok(());
            }
            
            if statements.len() > 1 {
                debug!("Query contains {} statements", statements.len());
                for (i, stmt) in statements.iter().enumerate() {
                    debug!("Executing statement {}: {}", i + 1, stmt);
                    Self::execute_single_statement(framed, db, session, stmt, query_router).await?;
                }
                return Ok(());
            }
        }
        
        // Single statement execution
        Self::execute_single_statement(framed, db, session, query_to_execute, query_router).await
    }
    
    async fn execute_single_statement<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &Arc<DbHandler>,
        session: &Arc<SessionState>,
        query: &str,
        query_router: Option<&Arc<QueryRouter>>,
    ) -> Result<(), PgSqliteError> 
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        use crate::protocol::TransactionStatus;
        
        // Check if we're in a failed transaction
        if session.get_transaction_status().await == TransactionStatus::InFailedTransaction {
            // Only ROLLBACK is allowed in a failed transaction
            use crate::query::{QueryTypeDetector, QueryType};
            if !matches!(QueryTypeDetector::detect_query_type(query), QueryType::Rollback) {
                return Err(PgSqliteError::Protocol(
                    "current transaction is aborted, commands ignored until end of transaction block".to_string()
                ));
            }
        }
        // Ultra-fast path: Skip all translation if query is simple enough
        let is_ultra_simple = crate::query::simple_query_detector::is_ultra_simple_query(query);
        // Checking if query is ultra-simple
        if is_ultra_simple {
            // Simple query routing without any processing
            match QueryTypeDetector::detect_query_type(query) {
                QueryType::Select => {
                    // Route query through query router if available and appropriate
                    let response = if let Some(router) = query_router {
                        router.execute_query(query, session).await.map_err(|e| PgSqliteError::Protocol(e.to_string()))?
                    } else {
                        let cached_conn = Self::get_or_cache_connection(session, db).await;
                        db.query_with_session_cached(query, &session.id, cached_conn.as_ref()).await?
                    };
                    
                    // Always check for type conversion to handle datetime columns
                    let needs_type_conversion = true;
                    
                    // Extract table name once and get all schema information in one query
                    let table_name = if needs_type_conversion {
                        extract_table_name_from_select(query)
                    } else {
                        None
                    };
                    
                    let (boolean_columns, mut datetime_columns, column_types, column_mappings, enum_columns) = if let Some(ref table) = table_name.filter(|_| needs_type_conversion) {
                        let schema_info = get_table_schema_info(table, db, &session.id).await;
                        let mappings = extract_column_mappings_from_query(query, table);
                        // debug!("Column mappings for table '{}': {:?}", table, mappings);
                        // debug!("Datetime columns for table '{}': {:?}", table, schema_info.datetime_columns);
                        (
                            schema_info.boolean_columns,
                            schema_info.datetime_columns,
                            schema_info.column_types,
                            mappings,
                            schema_info.enum_columns
                        )
                    } else {
                        (
                            std::collections::HashSet::new(),
                            std::collections::HashMap::new(),
                            std::collections::HashMap::new(),
                            std::collections::HashMap::new(),
                            std::collections::HashMap::new()
                        )
                    };
                    
                    // Check for scalar subqueries that return timestamps
                    // Pattern: (SELECT MAX/MIN(timestamp_col) FROM table) as alias
                    // Checking for scalar subqueries
                    for col_name in &response.columns {
                        // Check if this might be a scalar subquery result
                        if col_name.contains("max") || col_name.contains("min") || 
                           col_name.contains("MAX") || col_name.contains("MIN") {
                            // Column might be scalar subquery
                            
                            // Look for the subquery pattern in the original query
                            // Pattern: (SELECT MAX(col) FROM table)
                            let pattern = format!(r"(?i)\(\s*SELECT\s+(?:MAX|MIN)\s*\(\s*(\w+)\s*\)\s+FROM\s+(\w+)\s*\)\s+(?:AS\s+)?{}", regex::escape(col_name));
                            if let Ok(re) = regex::Regex::new(&pattern)
                                && let Some(captures) = re.captures(query)
                                    && let (Some(inner_col), Some(inner_table)) = (captures.get(1), captures.get(2)) {
                                        let inner_col_name = inner_col.as_str();
                                        let inner_table_name = inner_table.as_str();
                                        // Found scalar subquery
                                        
                                        // Check if the inner column is a timestamp
                                        if let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, inner_table_name, inner_col_name).await {
                                            // Inner column type found
                                            if pg_type.to_uppercase().contains("TIMESTAMP") || 
                                               pg_type.to_uppercase().contains("DATE") || 
                                               pg_type.to_uppercase().contains("TIME") {
                                                // Adding datetime column
                                                datetime_columns.insert(col_name.clone(), pg_type);
                                            }
                                        }
                                    }
                        }
                    }
                    
                    let fields: Vec<FieldDescription> = response.columns.iter()
                        .enumerate()
                        .map(|(i, name)| {
                            // We need to determine type OID before creating the closure
                            let type_oid = if let Some(pg_type) = column_types.get(name) {
                                // Try to get enum-aware type OID, fall back to basic type if fails
                                crate::types::SchemaTypeMapper::pg_type_string_to_oid(pg_type)
                            } else {
                                PgType::Text.to_oid() // Fallback to TEXT
                            };
                            
                            FieldDescription {
                                name: name.clone(),
                                table_oid: 0,
                                column_id: (i + 1) as i16,
                                type_oid,
                                type_size: -1,
                                type_modifier: -1,
                                format: 0,
                            }
                        })
                        .collect();
                    
                    framed.send(BackendMessage::RowDescription(fields)).await
                        .map_err(PgSqliteError::Io)?;
                    
                    // Pre-fetch enum mappings if needed
                    let enum_mappings: std::collections::HashMap<String, std::collections::HashMap<i32, String>> = 
                        if !enum_columns.is_empty() {
                            let mut mappings = std::collections::HashMap::new();
                            
                            // Use session connection to fetch enum values
                            let _ = db.with_session_connection(&session.id, |conn| {
                                for enum_type in enum_columns.values() {
                                    if !mappings.contains_key(enum_type)
                                        && let Ok(mut stmt) = conn.prepare(
                                            "SELECT sort_order, label FROM __pgsqlite_enum_values ev 
                                             JOIN __pgsqlite_enum_types et ON ev.type_oid = et.type_oid 
                                             WHERE et.type_name = ?1 
                                             ORDER BY ev.sort_order"
                                        )
                                            && let Ok(values) = stmt.query_map([enum_type], |row| {
                                                // sort_order is a REAL, but we need to map it to integers 0, 1, 2...
                                                let sort_order: f64 = row.get(0)?;
                                                let ordinal = (sort_order as i32) - 1; // Convert 1-based to 0-based
                                                let label: String = row.get(1)?;
                                                Ok((ordinal, label))
                                            }) {
                                                let enum_values: std::collections::HashMap<i32, String> = 
                                                    values.flatten().collect();
                                                mappings.insert(enum_type.clone(), enum_values);
                                            }
                                }
                                Ok::<(), rusqlite::Error>(())
                            }).await;
                            mappings
                        } else {
                            std::collections::HashMap::new()
                        };
                    
                    // Send data rows with boolean, datetime, and enum conversion
                    // Processing rows with datetime/boolean conversion
                    for row in response.rows {
                        // Fast path - if no special columns, send row as-is
                        // DISABLED: We need to check all columns for potential timestamp values
                        // if boolean_columns.is_empty() && datetime_columns.is_empty() && enum_columns.is_empty() {
                        //     framed.send(BackendMessage::DataRow(row)).await
                        //         .map_err(PgSqliteError::Io)?;
                        //     continue;
                        // }
                        
                        let converted_row: Vec<Option<Vec<u8>>> = row.into_iter()
                            .enumerate()
                            .map(|(col_idx, cell)| {
                                if let Some(data) = cell {
                                    // Convert based on column type
                                    if col_idx < response.columns.len() {
                                        let col_name = &response.columns[col_idx];
                                        
                                        // Debug enum cast
                                        if col_name == "casted_status" {
                                            eprintln!("DEBUG: Processing casted_status column");
                                            eprintln!("  Raw data: {data:?}");
                                            eprintln!("  As string: {:?}", std::str::from_utf8(&data));
                                        }
                                        
                                        // Check for boolean columns
                                        if boolean_columns.contains(col_name) {
                                            // Check if this looks like a boolean value
                                            match std::str::from_utf8(&data) {
                                                Ok(s) => match s.trim() {
                                                    "0" => Some(b"f".to_vec()),
                                                    "1" => Some(b"t".to_vec()),
                                                    _ => Some(data), // Keep original data if not 0/1
                                                },
                                                Err(_) => Some(data), // Keep original data if not valid UTF-8
                                            }
                                        }
                                        // Check for datetime columns
                                        // First try exact match, then check column mappings
                                        else if let Some(dt_type) = datetime_columns.get(col_name)
                                            .or_else(|| {
                                                // Check if this is an alias mapped to a real column
                                                // Checking datetime conversion
                                                if let Some(real_column) = column_mappings.get(col_name) {
                                                    // Found column mapping
                                                    datetime_columns.get(real_column)
                                                } else {
                                                    None
                                                }
                                            }) {
                                            match std::str::from_utf8(&data) {
                                                Ok(s) => {
                                                    // Try to parse as integer (days/microseconds)
                                                    if let Ok(int_val) = s.parse::<i64>() {
                                                        match dt_type.as_str() {
                                                            "date" => {
                                                                // Convert days since epoch to YYYY-MM-DD
                                                                use crate::types::datetime_utils::format_days_to_date_buf;
                                                                let mut buf = vec![0u8; 32];
                                                                let len = format_days_to_date_buf(int_val as i32, &mut buf);
                                                                buf.truncate(len);
                                                                Some(buf)
                                                            }
                                                            "time" | "timetz" | "time without time zone" | "time with time zone" => {
                                                                // Convert microseconds since midnight to HH:MM:SS.ffffff
                                                                use crate::types::datetime_utils::format_microseconds_to_time_buf;
                                                                let mut buf = vec![0u8; 32];
                                                                let len = format_microseconds_to_time_buf(int_val, &mut buf);
                                                                buf.truncate(len);
                                                                Some(buf)
                                                            }
                                                            "timestamp" | "timestamptz" | "timestamp without time zone" | "timestamp with time zone" => {
                                                                // Convert microseconds since epoch to YYYY-MM-DD HH:MM:SS.ffffff
                                                                use crate::types::datetime_utils::format_microseconds_to_timestamp_buf;
                                                                let mut buf = vec![0u8; 32];
                                                                let len = format_microseconds_to_timestamp_buf(int_val, &mut buf);
                                                                buf.truncate(len);
                                                                Some(buf)
                                                            }
                                                            _ => Some(data), // Keep original data for unknown datetime types
                                                        }
                                                    } else {
                                                        Some(data) // Keep original data if not an integer
                                                    }
                                                }
                                                Err(_) => Some(data), // Keep original data if not valid UTF-8
                                            }
                                        }
                                        // Check for enum columns
                                        else if let Some(enum_type) = enum_columns.get(col_name)
                                            .or_else(|| {
                                                // Check if this is an alias mapped to a real column
                                                if let Some(real_column) = column_mappings.get(col_name) {
                                                    enum_columns.get(real_column)
                                                } else {
                                                    None
                                                }
                                            }) {
                                            match std::str::from_utf8(&data) {
                                                Ok(s) => {
                                                    // Try to parse as integer (ordinal value)
                                                    if let Ok(ordinal) = s.parse::<i32>() {
                                                        // Look up enum value from pre-fetched mappings
                                                        if let Some(type_mappings) = enum_mappings.get(enum_type) {
                                                            if let Some(label) = type_mappings.get(&ordinal) {
                                                                Some(label.as_bytes().to_vec())
                                                            } else {
                                                                Some(data) // Keep original if ordinal not found
                                                            }
                                                        } else {
                                                            Some(data) // Keep original if type not found
                                                        }
                                                    } else {
                                                        Some(data) // Keep original if not an integer
                                                    }
                                                }
                                                Err(_) => Some(data), // Keep original data if not valid UTF-8
                                            }
                                        } else {
                                            // Check if this might be a timestamp in a TEXT column
                                            // This handles scalar subqueries that return timestamps
                                            if let Ok(s) = std::str::from_utf8(&data) {
                                                // Debug logging for scalar subquery columns
                                                if col_name.contains("max_created") || col_name.contains("MAX(") {
                                                    info!("Checking column '{}' with value '{}'", col_name, s);
                                                }
                                                if let Ok(micros) = s.parse::<i64>() {
                                                    // Check if this looks like microseconds since epoch
                                                    // Valid timestamp range: roughly 1970-2100 (0 to ~4.1 trillion microseconds)
                                                    // We check for values > 100 billion to avoid converting small integers
                                                    if micros > 100_000_000_000 && micros < 4_102_444_800_000_000 {
                                                        // This is likely a datetime value stored as INTEGER microseconds
                                                        use crate::types::datetime_utils::format_microseconds_to_timestamp_buf;
                                                        let mut buf = vec![0u8; 32];
                                                        let len = format_microseconds_to_timestamp_buf(micros, &mut buf);
                                                        buf.truncate(len);
                                                        info!("Converting TEXT column '{}' timestamp value {} to formatted", col_name, micros);
                                                        Some(buf)
                                                    } else {
                                                        Some(data) // Not a timestamp range
                                                    }
                                                } else {
                                                    Some(data) // Not an integer
                                                }
                                            } else {
                                                Some(data) // Not valid UTF-8
                                            }
                                        }
                                    } else {
                                        Some(data) // Keep original data if column index is out of bounds
                                    }
                                } else {
                                    None
                                }
                            })
                            .collect();
                        
                        framed.send(BackendMessage::DataRow(converted_row)).await
                            .map_err(PgSqliteError::Io)?;
                    }
                    
                    // Send command complete
                    let tag = create_command_tag("SELECT", response.rows_affected);
                    framed.send(BackendMessage::CommandComplete { tag }).await
                        .map_err(PgSqliteError::Io)?;
                    
                    return Ok(());
                }
                QueryType::Insert | QueryType::Update | QueryType::Delete => {
                    // For ultra-simple queries, bypass all validation and translation
                    debug!("Using ultra-fast path for DML query: {}", query);
                    return Self::execute_dml(framed, db, session, query, query_router).await;
                }
                _ => {} // Fall through to normal processing
            }
        }
        
        // Analyze query once to determine which translators are needed
        let translation_flags = crate::translator::QueryAnalyzer::analyze(query);
        debug!("Query analysis flags: {:?}", translation_flags);
        
        // Translate PostgreSQL cast syntax if present and collect metadata
        let mut translation_metadata = crate::translator::TranslationMetadata::new();
        let mut translated_query = if translation_flags.contains(crate::translator::TranslationFlags::CAST) {
            if crate::profiling::is_profiling_enabled() {
                crate::time_cast_translation!({
                    use crate::translator::CastTranslator;
                    let (translated, metadata) = db.with_session_connection(&session.id, |conn| {
                        Ok(CastTranslator::translate_with_metadata(query, Some(conn)))
                    }).await?;
                    translation_metadata.merge(metadata);
                    translated
                })
            } else {
                use crate::translator::CastTranslator;
                let (translated, metadata) = db.with_session_connection(&session.id, |conn| {
                    Ok(CastTranslator::translate_with_metadata(query, Some(conn)))
                }).await?;
                translation_metadata.merge(metadata);
                translated
            }
        } else {
            query.to_string()
        };
        
        // Translate NUMERIC to TEXT casts with proper formatting
        if translation_flags.contains(crate::translator::TranslationFlags::NUMERIC_FORMAT) {
            use crate::translator::NumericFormatTranslator;
            translated_query = db.with_session_connection(&session.id, |conn| {
                Ok(NumericFormatTranslator::translate_query(&translated_query, conn))
            }).await?
        }
        
        // Translate batch UPDATE operations if needed
        if translation_flags.contains(crate::translator::TranslationFlags::BATCH_UPDATE) {
            use std::collections::HashMap;
            use parking_lot::Mutex;
            let decimal_cache = Arc::new(Mutex::new(HashMap::new()));
            let batch_translator = BatchUpdateTranslator::new(decimal_cache);
            translated_query = batch_translator.translate(&translated_query, &[]);
            debug!("Query after batch UPDATE translation: {}", translated_query);
        }
        
        // Translate batch DELETE operations if needed
        if translation_flags.contains(crate::translator::TranslationFlags::BATCH_DELETE) {
            use std::collections::HashMap;
            use parking_lot::Mutex;
            let decimal_cache = Arc::new(Mutex::new(HashMap::new()));
            let batch_translator = BatchDeleteTranslator::new(decimal_cache);
            translated_query = batch_translator.translate(&translated_query, &[]);
            debug!("Query after batch DELETE translation: {}", translated_query);
        }
        
        // Translate FTS operations if needed
        if translation_flags.contains(crate::translator::TranslationFlags::FTS) {
            debug!("Query contains FTS operations: {}", translated_query);
            let fts_translator = FtsTranslator::new();
            
            // Get connection, do translation, and immediately drop it to avoid Send issues
            let fts_result = db.with_session_connection(&session.id, |conn| {
                let result = fts_translator.translate(&translated_query, Some(conn));
                Ok::<_, rusqlite::Error>(result)
            }).await;
            
            match fts_result {
                Ok(Ok(fts_queries)) => {
                    // For multiple queries (like CREATE TABLE with shadow tables), execute them all
                    if fts_queries.len() > 1 {
                        debug!("FTS translation produced {} queries", fts_queries.len());
                        
                        // Execute all but the last query first
                        for (i, fts_query) in fts_queries.iter().take(fts_queries.len() - 1).enumerate() {
                            debug!("Executing FTS query {}: {}", i + 1, fts_query);
                            let cached_conn = Self::get_or_cache_connection(session, db).await;
                            db.execute_with_session_cached(fts_query, &session.id, cached_conn.as_ref()).await?;
                        }
                        
                        // Use the last query as the main query
                        if let Some(main_query) = fts_queries.last() {
                            translated_query = main_query.clone();
                            debug!("Using final FTS query: {}", translated_query);
                        }
                    } else if fts_queries.len() == 1 {
                        translated_query = fts_queries[0].clone();
                        debug!("Query after FTS translation: {}", translated_query);
                    }
                }
                Ok(Err(e)) => {
                    debug!("FTS translation failed: {}", e);
                    return Err(PgSqliteError::Protocol(format!("FTS translation error: {e}")));
                }
                Err(e) => {
                    debug!("FTS connection failed: {}", e);
                    return Err(PgSqliteError::Protocol(format!("Failed to translate FTS: {e}")));
                }
            }
        }
        
        // Translate INSERT statements with datetime values if needed
        if translation_flags.contains(crate::translator::TranslationFlags::INSERT_DATETIME) {
            use crate::translator::InsertTranslator;
            debug!("Query needs INSERT datetime translation: {}", translated_query);
            match InsertTranslator::translate_query(&translated_query, db).await {
                Ok(translated) => {
                    debug!("Query after INSERT translation: {}", translated);
                    translated_query = translated;
                }
                Err(e) => {
                    debug!("INSERT translation failed: {}", e);
                    // Return the error to the user
                    return Err(PgSqliteError::Protocol(e));
                }
            }
        }
        
        // Translate PostgreSQL datetime functions if present and capture metadata
        // translation_metadata already initialized above with cast metadata
        if translation_flags.contains(crate::translator::TranslationFlags::DATETIME) {
            if crate::profiling::is_profiling_enabled() {
                crate::time_datetime_translation!({
                    use crate::translator::DateTimeTranslator;
                    debug!("Query needs datetime translation: {}", translated_query);
                    let (translated, metadata) = DateTimeTranslator::translate_with_metadata(&translated_query);
                    translated_query = translated;
                    translation_metadata.merge(metadata);
                    debug!("Query after datetime translation: {}", translated_query);
                });
            } else {
                use crate::translator::DateTimeTranslator;
                debug!("Query needs datetime translation: {}", translated_query);
                let (translated, metadata) = DateTimeTranslator::translate_with_metadata(&translated_query);
                translated_query = translated;
                translation_metadata.merge(metadata);
                debug!("Query after datetime translation: {}", translated_query);
            }
        }
        
        // Translate JSON operators if present
        if translation_flags.contains(crate::translator::TranslationFlags::JSON) {
            use crate::translator::JsonTranslator;
            debug!("Query needs JSON operator translation: {}", translated_query);
            match JsonTranslator::translate_json_operators(&translated_query) {
                Ok(translated) => {
                    debug!("Query after JSON operator translation: {}", translated);
                    translated_query = translated;
                }
                Err(e) => {
                    debug!("JSON operator translation failed: {}", e);
                    // Continue with original query - some operators might not be supported yet
                }
            }
            
            // Note: JSON path $ restoration will happen right before SQLite execution
            debug!("Query after JSON translation ($ placeholders preserved): {}", translated_query);
        }
        
        // Translate catalog functions (remove pg_catalog prefix)
        {
            use crate::translator::{CatalogFunctionTranslator, PgTableIsVisibleTranslator};
            translated_query = CatalogFunctionTranslator::translate(&translated_query);
            translated_query = PgTableIsVisibleTranslator::translate(&translated_query);
        }
        
        // Translate array operators with metadata
        if translation_flags.contains(crate::translator::TranslationFlags::ARRAY) {
            use crate::translator::ArrayTranslator;
            match ArrayTranslator::translate_with_metadata(&translated_query) {
            Ok((translated, metadata)) => {
                if translated != translated_query {
                    debug!("Query after array operator translation: {}", translated);
                    translated_query = translated;
                }
                debug!("Array translation metadata: {} hints", metadata.column_mappings.len());
                for (col, hint) in &metadata.column_mappings {
                    debug!("  Column '{}': type={:?}", col, hint.suggested_type);
                }
                translation_metadata.merge(metadata);
            }
            Err(e) => {
                debug!("Array operator translation failed: {}", e);
                // Continue with original query
            }
            }
        }
        
        // Translate array_agg functions with ORDER BY/DISTINCT support
        if translation_flags.contains(crate::translator::TranslationFlags::ARRAY_AGG) {
            use crate::translator::ArrayAggTranslator;
            match ArrayAggTranslator::translate_with_metadata(&translated_query) {
            Ok((translated, metadata)) => {
                if translated != translated_query {
                    debug!("Query after array_agg translation: {}", translated);
                    translated_query = translated;
                }
                debug!("Array_agg translation metadata: {} hints", metadata.column_mappings.len());
                translation_metadata.merge(metadata);
            }
            Err(e) => {
                debug!("Array_agg translation failed: {}", e);
                // Continue with original query
            }
            }
        }
        
        // Translate unnest() functions to json_each() equivalents
        if translation_flags.contains(crate::translator::TranslationFlags::UNNEST) {
            use crate::translator::UnnestTranslator;
            match UnnestTranslator::translate_with_metadata(&translated_query) {
            Ok((translated, metadata)) => {
                if translated != translated_query {
                    debug!("Query after unnest translation: {}", translated);
                    translated_query = translated;
                }
                debug!("Unnest translation metadata: {} hints", metadata.column_mappings.len());
                translation_metadata.merge(metadata);
            }
            Err(e) => {
                debug!("Unnest translation failed: {}", e);
                // Continue with original query
            }
            }
        }
        
        // Translate json_each()/jsonb_each() functions for PostgreSQL compatibility
        if translation_flags.contains(crate::translator::TranslationFlags::JSON_EACH) {
            use crate::translator::JsonEachTranslator;
            match JsonEachTranslator::translate_with_metadata(&translated_query) {
            Ok((translated, metadata)) => {
                if translated != translated_query {
                    debug!("Query after json_each translation: {}", translated);
                    translated_query = translated;
                }
                debug!("JsonEach translation metadata: {} hints", metadata.column_mappings.len());
                translation_metadata.merge(metadata);
            }
            Err(e) => {
                debug!("JsonEach translation failed: {}", e);
                // Continue with original query
            }
            }
        }
        
        // Translate row_to_json() functions for PostgreSQL compatibility
        if translation_flags.contains(crate::translator::TranslationFlags::ROW_TO_JSON) {
            use crate::translator::RowToJsonTranslator;
            let (translated, metadata) = RowToJsonTranslator::translate_row_to_json(&translated_query);
            if translated != translated_query {
            debug!("Query after row_to_json translation: {}", translated);
            translated_query = translated;
            }
            debug!("RowToJson translation metadata: {} hints", metadata.column_mappings.len());
            translation_metadata.merge(metadata);
        }
        
        // Analyze arithmetic expressions for type metadata
        if translation_flags.contains(crate::translator::TranslationFlags::ARITHMETIC) {
            debug!("Analyzing arithmetic expressions in query");
            let arithmetic_metadata = crate::translator::ArithmeticAnalyzer::analyze_query(&translated_query);
            debug!("ArithmeticAnalyzer found {} hints", arithmetic_metadata.column_mappings.len());
            translation_metadata.merge(arithmetic_metadata);
            debug!("Total translation metadata after merge: {} hints", translation_metadata.column_mappings.len());
        }
        
        let query_to_execute = translated_query.as_str();
        
        // Simple query routing using optimized detection
        use crate::query::{QueryTypeDetector, QueryType};
        
        let query_type = QueryTypeDetector::detect_query_type(query_to_execute);
        debug!("Query type detected: {:?} for query: {}", query_type, query_to_execute);
        match query_type {
            QueryType::Select => {
                // debug!("Detected SELECT, calling execute_select for query: {}", query_to_execute);
                debug!("Calling execute_select for query: {}", query_to_execute);
                Self::execute_select(framed, db, session, query_to_execute, &translation_metadata, query_router).await
            },
            QueryType::Insert | QueryType::Update | QueryType::Delete => {
                Self::execute_dml(framed, db, session, query_to_execute, query_router).await
            }
            QueryType::Create | QueryType::Drop | QueryType::Alter => {
                Self::execute_ddl(framed, db, session, query_to_execute, query_router).await
            }
            QueryType::Begin | QueryType::Commit | QueryType::Rollback => {
                Self::execute_transaction(framed, db, session, query_to_execute, query_router).await
            }
            _ => {
                // Check if it's a SET command
                if crate::query::SetHandler::is_set_command(query_to_execute) {
                    crate::query::SetHandler::handle_set_command(framed, session, query_to_execute).await
                } else if query_to_execute.trim().to_uppercase().starts_with("GRANT") {
                    // Handle GRANT commands
                    info!("GRANT command received - SQLite doesn't have user/privilege management, succeeding with no-op");
                    framed.send(BackendMessage::CommandComplete {
                        tag: "GRANT".to_string()
                    }).await
                        .map_err(PgSqliteError::Io)?;
                    Ok(())
                } else if query_to_execute.trim().to_uppercase().starts_with("REVOKE") {
                    // Handle REVOKE commands (often used with GRANT)
                    info!("REVOKE command received - SQLite doesn't have user/privilege management, succeeding with no-op");
                    framed.send(BackendMessage::CommandComplete {
                        tag: "REVOKE".to_string()
                    }).await
                        .map_err(PgSqliteError::Io)?;
                    Ok(())
                } else if query_to_execute.trim().to_uppercase().starts_with("FLUSH") {
                    // Handle FLUSH commands
                    info!("FLUSH command received - SQLite doesn't have caching layers like PostgreSQL, succeeding with no-op");
                    framed.send(BackendMessage::CommandComplete {
                        tag: "FLUSH".to_string()
                    }).await
                        .map_err(PgSqliteError::Io)?;
                    Ok(())
                } else {
                    // Try to execute as-is
                    Self::execute_generic(framed, db, session, query_to_execute, query_router).await
                }
            }
        }
    }
    
    async fn execute_select<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &Arc<DbHandler>,
        session: &Arc<SessionState>,
        query: &str,
        translation_metadata: &crate::translator::TranslationMetadata,
        query_router: Option<&Arc<QueryRouter>>,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        // debug!("execute_select (non-ultra-simple) called with query: {}", query);
        // SQLAlchemy manages transactions explicitly - don't start implicit transactions
        // debug!("=== EXECUTE_SELECT CALLED with query: {}", query);
        
        // Check wire protocol cache first for cacheable queries
        if crate::cache::is_cacheable_for_wire_protocol(query)
            && let Some(cached_response) = crate::cache::WIRE_PROTOCOL_CACHE.get(query) {
                debug!("Wire protocol cache hit for query: {}", query);
                
                // Send cached row description
                framed.send(BackendMessage::RowDescription(cached_response.row_description.clone())).await
                    .map_err(PgSqliteError::Io)?;
                
                // Send cached data rows (already encoded)
                for encoded_row in &cached_response.encoded_rows {
                    // Send pre-encoded data directly
                    framed.get_mut().write_all(encoded_row).await
                        .map_err(PgSqliteError::Io)?;
                }
                
                // Send command complete
                let tag = format!("SELECT {}", cached_response.row_count);
                framed.send(BackendMessage::CommandComplete { tag }).await
                    .map_err(PgSqliteError::Io)?;
                
                return Ok(());
            }
        
        // Check if this is a catalog query first
        println!("EXECUTOR: About to call catalog interceptor with query: '{}'", query);
        let response = if let Some(catalog_result) = crate::catalog::CatalogInterceptor::intercept_query(query, db.clone(), Some(session.clone())).await {
            info!("Query intercepted by catalog handler");
            println!("EXECUTOR: Got catalog result, about to unwrap");
            let unwrapped = catalog_result?;
            println!("EXECUTOR: Unwrapped catalog result, columns: {}, rows: {}", unwrapped.columns.len(), unwrapped.rows.len());
            unwrapped
        } else {
            // Route query through query router if available
            if let Some(router) = query_router {
                router.execute_query(query, session).await.map_err(|e| PgSqliteError::Protocol(e.to_string()))?
            } else {
                let cached_conn = Self::get_or_cache_connection(session, db).await;
                db.query_with_session_cached(query, &session.id, cached_conn.as_ref()).await?
            }
        };
        
        // Extract table name from query to look up schema
        let table_name = extract_table_name_from_select(query);
        // debug!("Non-ultra execute_select: table_name={:?}", table_name);
        // debug!("Table name extraction result: {:?} for query: {}", table_name, query);
        
        // Extract column mappings for aliased columns (e.g., "column AS alias")
        let column_mappings = if let Some(ref table) = table_name {
            // debug!("Non-ultra execute_select: column_mappings={:?}", mappings);
            extract_column_mappings_from_query(query, table)
        } else {
            std::collections::HashMap::new()
        };
        
        // For JOIN queries, extract all tables and build column mappings
        // Optimized: check for JOIN without converting entire query to uppercase
        let is_join_query = query.contains(" JOIN ") || query.contains(" join ") || 
                           query.contains(" Join ") || query.contains(" JoIn ");
        let column_to_table_map = if is_join_query {
            debug!("Type inference: Detected JOIN query, building column-to-table mappings");
            build_column_to_table_mapping(query)
        } else {
            std::collections::HashMap::new()
        };
        
        // Create cache key
        let cache_key = RowDescriptionKey {
            query: query.to_string(),
            table_name: table_name.clone(),
            columns: response.columns.clone(),
        };
        
        // Check cache first
        let fields = if let Some(cached_fields) = GLOBAL_ROW_DESCRIPTION_CACHE.get(&cache_key) {
            cached_fields
        } else {
            // Pre-fetch schema types for all columns if we have a table name
            let mut schema_types = std::collections::HashMap::new();
            let mut hint_source_types = std::collections::HashMap::new();
            
            // For JOIN queries, use column-to-table mapping
            if is_join_query && !column_to_table_map.is_empty() {
                debug!("Type inference: Using JOIN column mappings for {} columns", response.columns.len());
                
                for col_name in &response.columns {
                    // First check if we have a direct mapping from the query
                    if let Some(table) = column_to_table_map.get(col_name) {
                        // Try to find the actual column name (strip alias prefix if needed)
                        let actual_column = if col_name.starts_with(&format!("{table}_")) {
                            &col_name[table.len() + 1..]
                        } else {
                            col_name
                        };
                        
                        debug!("Type inference: JOIN query column '{}' mapped to table '{}', actual column '{}'", 
                              col_name, table, actual_column);
                        
                        if let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, table, actual_column).await {
                            debug!("Type inference: Found schema type for '{}.{}' (via JOIN mapping) -> {}", table, actual_column, pg_type);
                            schema_types.insert(col_name.clone(), pg_type);
                        } else {
                            debug!("Type inference: No schema type found for '{}.{}'", table, actual_column);
                        }
                    } else {
                        debug!("Type inference: No table mapping found for column '{}'", col_name);
                    }
                }
            }
            
            if let Some(ref table) = table_name {
                debug!("Type inference: Found table name '{}', looking up schema for {} columns", table, response.columns.len());
                
                // Extract column mappings from query if possible
                let column_mappings = extract_column_mappings_from_query(query, table);
                
                // Fetch types for actual columns
                for col_name in &response.columns {
                    // Try direct lookup first
                    if let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, table, col_name).await {
                        debug!("Type inference: Found schema type for '{}.{}' -> {}", table, col_name, pg_type);
                        schema_types.insert(col_name.clone(), pg_type);
                    } else if let Some(source_column) = column_mappings.get(col_name) {
                        // Try using the column mapping from SELECT clause
                        if let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, table, source_column).await {
                            debug!("Type inference: Found schema type for '{}.{}' (via SELECT mapping {}) -> {}", table, source_column, col_name, pg_type);
                            schema_types.insert(col_name.clone(), pg_type);
                            continue;
                        }
                    } else {
                        // Try stripping table name prefix from column alias
                        let potential_column = if col_name.starts_with(&format!("{table}_")) {
                            let after_table = &col_name[table.len() + 1..];
                            // Handle SQLAlchemy patterns like "products_name_1" -> "name"
                            if let Some(underscore_pos) = after_table.rfind('_') {
                                if after_table[underscore_pos + 1..].chars().all(|c| c.is_ascii_digit()) {
                                    // Strip numeric suffix: "name_1" -> "name"
                                    &after_table[..underscore_pos]
                                } else {
                                    after_table
                                }
                            } else {
                                after_table
                            }
                        } else {
                            col_name
                        };
                        
                        // Special handling for JOIN queries with table prefixes
                        // Check if this is a column from a different table (e.g., "order_items_unit_price")
                        if potential_column == col_name && col_name.contains('_') {
                            // Try to extract table and column from patterns like "order_items_unit_price"
                            let parts: Vec<&str> = col_name.split('_').collect();
                            if parts.len() >= 3 {
                                // Try common patterns: table_name_column_name
                                let potential_table_single = parts[0];
                                let potential_table_double = format!("{}_{}", parts[0], parts[1]);
                                let potential_col_single = parts[parts.len() - 1];
                                let potential_col_double = format!("{}_{}", parts[parts.len() - 2], parts[parts.len() - 1]);
                                
                                // Try different combinations
                                debug!("Type inference: Trying pattern matching for '{}' with parts: {:?}", col_name, parts);
                                for (try_table, try_col) in [
                                    (potential_table_double.as_str(), potential_col_double.as_str()),
                                    (potential_table_double.as_str(), potential_col_single),
                                    (potential_table_single, potential_col_double.as_str()),
                                ] {
                                    debug!("Type inference: Trying combination table='{}', col='{}'", try_table, try_col);
                                    if let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, try_table, try_col).await {
                                        debug!("Type inference: Found schema type for '{}.{}' (via pattern matching {}) -> {}", try_table, try_col, col_name, pg_type);
                                        schema_types.insert(col_name.clone(), pg_type);
                                        break;
                                    }
                                }
                            }
                        }
                        
                        if potential_column != col_name
                            && let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, table, potential_column).await {
                                debug!("Type inference: Found schema type for '{}.{}' (via alias {}) -> {}", table, potential_column, col_name, pg_type);
                                schema_types.insert(col_name.clone(), pg_type);
                                continue;
                            }
                        
                        debug!("Type inference: No schema type found for '{}.{}'", table, col_name);
                    }
                }
            } else {
                debug!("Type inference: No table name extracted from query, using fallback logic");
            }
                
            // Fetch types for source columns referenced in translation hints
            if let Some(ref table) = table_name {
                for col_name in &response.columns {
                    if let Some(hint) = translation_metadata.get_hint(col_name)
                        && let Some(ref source_col) = hint.source_column
                            && let Ok(Some(source_type)) = db.get_schema_type_with_session(&session.id, table, source_col).await {
                                hint_source_types.insert(col_name.clone(), source_type);
                            }
                }
            }
            
            // Build field descriptions with proper type inference
            let fields: Vec<FieldDescription> = response.columns.iter()
                .enumerate()
                .map(|(i, name)| {
                    // First priority: Check schema table for stored type mappings
                    let type_oid = if let Some(pg_type) = schema_types.get(name) {
                        // Use basic type OID mapping (enum checking would require async which isn't allowed in closure)
                        crate::types::SchemaTypeMapper::pg_type_string_to_oid(pg_type)
                    } else if let Some(aggregate_oid) = crate::types::SchemaTypeMapper::get_aggregate_return_type_with_query(name, None, None, Some(query)) {
                        // Second priority: Check for aggregate functions
                        aggregate_oid
                    } else if crate::types::aggregate_type_fixer::fix_aggregate_type_for_decimal(name, Some(query)).is_some() {
                        // Third priority: Check if this is an aliased aggregate on a decimal column
                        crate::types::PgType::Numeric.to_oid()
                    } else if let Some(hint) = translation_metadata.get_hint(name) {
                        // Third priority: Check translation metadata (datetime or arithmetic)
                        debug!("Found translation hint for column '{}': {:?}", name, hint);
                        debug!("  Expression type: {:?}", hint.expression_type);
                        debug!("  Source column: {:?}", hint.source_column);
                        
                        // Check if we pre-fetched the source type
                        if let Some(source_type) = hint_source_types.get(name) {
                            debug!("Found source column type for '{}' -> '{}': {}", name, hint.source_column.as_ref().unwrap_or(&"<none>".to_string()), source_type);
                            // For arithmetic on numeric columns, preserve the type
                            if hint.expression_type == Some(crate::translator::ExpressionType::ArithmeticOnFloat) {
                                if source_type.contains("NUMERIC") || source_type.contains("DECIMAL") {
                                    // For NUMERIC/DECIMAL types, arithmetic returns NUMERIC
                                    PgType::Numeric.to_oid()
                                } else if source_type.contains("REAL") || source_type.contains("FLOAT") || source_type.contains("DOUBLE") {
                                    // For floating point types, return FLOAT8
                                    PgType::Float8.to_oid()
                                } else if source_type.contains("INT") || source_type.contains("BIGINT") || source_type.contains("SMALLINT") {
                                    // For integer types in arithmetic with potential decimal results, return NUMERIC
                                    PgType::Numeric.to_oid()
                                } else {
                                    // Default to NUMERIC for unknown numeric types
                                    PgType::Numeric.to_oid()
                                }
                            } else {
                                // For other expression types, use the source column type
                                crate::types::SchemaTypeMapper::pg_type_string_to_oid(source_type)
                            }
                        } else if let Some(suggested_type) = &hint.suggested_type {
                            // Fall back to suggested type if source lookup fails
                            suggested_type.to_oid()
                        } else {
                            // Default to NUMERIC for arithmetic operations
                            PgType::Numeric.to_oid()
                        }
                    } else if Self::is_datetime_expression(query, name) {
                        // Fourth priority: Legacy datetime expression detection
                        debug!("Detected datetime expression for column '{}'", name);
                        PgType::Date.to_oid()
                    } else {
                        // Check if this looks like a user table (not system/catalog queries)
                        if let Some(ref table) = table_name {
                            // System/catalog tables are allowed to use type inference
                            let is_system_table = table.starts_with("pg_") || 
                                                 table.starts_with("information_schema") ||
                                                 table == "__pgsqlite_schema";
                            
                            if !is_system_table {
                                // For user tables, missing metadata should be logged at debug level
                                debug!("Column '{}' in table '{}' not found in __pgsqlite_schema. Using type inference.", name, table);
                            }
                        }
                        
                        // Default to text for simple queries without schema info
                        debug!("Column '{}' using default text type", name);
                        PgType::Text.to_oid()
                    };
                    
                    debug!("Column '{}' final type OID: {} ({})", name, type_oid, 
                        crate::types::SchemaTypeMapper::pg_oid_to_type_name(type_oid));
                    
                    FieldDescription {
                        name: name.clone(),
                        table_oid: 0,
                        column_id: (i + 1) as i16,
                        type_oid,
                        type_size: -1,
                        type_modifier: -1,
                        format: 0, // text format
                    }
                })
                .collect();
            
            // Cache the field descriptions
            GLOBAL_ROW_DESCRIPTION_CACHE.insert(cache_key, fields.clone());
            
            fields
        };
        
        // Send RowDescription
        framed.send(BackendMessage::RowDescription(fields.clone())).await
            .map_err(PgSqliteError::Io)?;
        
        
        // Build datetime column info for conversion
        let mut datetime_columns = std::collections::HashMap::new();
        let mut column_types_map = std::collections::HashMap::new();
        
        // Check for scalar subqueries that return timestamps (same logic as ultra-simple path)
        info!("Non-ultra path: Checking for scalar subqueries in columns: {:?}", response.columns);
        for col_name in &response.columns {
            // Check if this might be a scalar subquery result
            if col_name.contains("max") || col_name.contains("min") || 
               col_name.contains("MAX") || col_name.contains("MIN") {
                info!("Non-ultra path: Column '{}' might be a scalar subquery result", col_name);
                
                // Look for the subquery pattern in the original query
                // Pattern: (SELECT MAX(col) FROM table)
                let pattern = format!(r"(?i)\(\s*SELECT\s+(?:MAX|MIN)\s*\(\s*(\w+)\s*\)\s+FROM\s+(\w+)\s*\)\s+(?:AS\s+)?{}", regex::escape(col_name));
                if let Ok(re) = regex::Regex::new(&pattern)
                    && let Some(captures) = re.captures(query)
                        && let (Some(inner_col), Some(inner_table)) = (captures.get(1), captures.get(2)) {
                            let inner_col_name = inner_col.as_str();
                            let inner_table_name = inner_table.as_str();
                            info!("Non-ultra path: Found scalar subquery: MAX/MIN({}) FROM {}", inner_col_name, inner_table_name);
                            
                            // Check if the inner column is a timestamp
                            if let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, inner_table_name, inner_col_name).await {
                                info!("Non-ultra path: Inner column type: {}", pg_type);
                                if pg_type.to_uppercase().contains("TIMESTAMP") || 
                                   pg_type.to_uppercase().contains("DATE") || 
                                   pg_type.to_uppercase().contains("TIME") {
                                    info!("Non-ultra path: Adding '{}' as datetime column (type: {})", col_name, pg_type);
                                    datetime_columns.insert(col_name.clone(), pg_type);
                                }
                            }
                        }
                
                // Also check for direct MAX/MIN without subquery
                // Pattern: MAX(created_at) or MIN(created_at)
                let direct_pattern = r"(?i)(?:MAX|MIN)\s*\(\s*(\w+)\s*\)";
                if let Ok(re) = regex::Regex::new(direct_pattern)
                    && let Some(captures) = re.captures(col_name)
                        && let Some(inner_col) = captures.get(1) {
                            let inner_col_name = inner_col.as_str();
                            info!("Non-ultra path: Found direct aggregate: {}", col_name);
                            
                            // Try all tables in the query to find the column
                            if let Some(ref table) = table_name
                                && let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, table, inner_col_name).await {
                                    info!("Non-ultra path: Direct aggregate column type: {}", pg_type);
                                    if pg_type.to_uppercase().contains("TIMESTAMP") || 
                                       pg_type.to_uppercase().contains("DATE") || 
                                       pg_type.to_uppercase().contains("TIME") {
                                        info!("Non-ultra path: Adding '{}' as datetime column from direct aggregate (type: {})", col_name, pg_type);
                                        datetime_columns.insert(col_name.clone(), pg_type);
                                    }
                                }
                        }
            }
        }
        
        if let Some(ref table) = table_name {
            // First check aliased columns using column mappings
            for (col_idx, col_name) in response.columns.iter().enumerate() {
                // Check if this is an aliased column
                if let Some(source_column) = column_mappings.get(col_name) {
                    // Look up the source column type
                    if let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, table, source_column).await {
                        column_types_map.insert(col_idx, pg_type.clone());
                        
                        // Check if it's a datetime type
                        match pg_type.to_uppercase().as_str() {
                            "DATE" | "TIME" | "TIME WITHOUT TIME ZONE" | "TIME WITH TIME ZONE" | "TIMETZ" |
                            "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => {
                                datetime_columns.insert(col_name.clone(), pg_type);
                            }
                            _ => {}
                        }
                    }
                } else {
                    // Check if this is a wildcard pattern (table.*)
                    // If the query contains "table.*" and we have no explicit mappings, 
                    // treat each column as mapping to itself
                    let wildcard_pattern = format!("{table}.*");
                    if query.contains(&wildcard_pattern) && column_mappings.is_empty() {
                        // For wildcard queries, map each column to itself
                        // Use session connection to look up schema information
                        if let Ok(Some(pg_type)) = db.with_session_connection(&session.id, |conn| {
                            let mut stmt = conn.prepare(
                                "SELECT pg_type FROM __pgsqlite_schema WHERE table_name = ?1 AND column_name = ?2"
                            )?;
                            
                            use rusqlite::OptionalExtension;
                            let result = stmt.query_row([table, col_name], |row| {
                                row.get::<_, String>(0)
                            }).optional()?;
                            
                            Ok::<Option<String>, rusqlite::Error>(result)
                        }).await {
                            column_types_map.insert(col_idx, pg_type.clone());
                            
                            // Check if it's a datetime type
                            match pg_type.to_uppercase().as_str() {
                                "DATE" | "TIME" | "TIME WITHOUT TIME ZONE" | "TIME WITH TIME ZONE" | "TIMETZ" |
                                "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => {
                                    datetime_columns.insert(col_name.clone(), pg_type);
                                }
                                _ => {}
                            }
                        }
                    } else {
                        // Try direct lookup for non-aliased columns
                        if let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, table, col_name).await {
                            column_types_map.insert(col_idx, pg_type.clone());
                            
                            // Check if it's a datetime type
                            match pg_type.to_uppercase().as_str() {
                                "DATE" | "TIME" | "TIME WITHOUT TIME ZONE" | "TIME WITH TIME ZONE" | "TIMETZ" |
                                "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => {
                                    datetime_columns.insert(col_name.clone(), pg_type);
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
        
        
        // Convert array data before sending rows
        debug!("Converting array data for {} rows", response.rows.len());
        debug!("About to convert array data for {} rows", response.rows.len());
        let mut converted_rows = Self::convert_array_data_in_rows(response.rows, &fields)?;
        debug!("Completed array data conversion");
        
        // Convert datetime data if needed
        if !datetime_columns.is_empty() {
            // debug!("Converting datetime values for {} columns", datetime_columns.len());
            for row in &mut converted_rows {
                for (col_idx, col_name) in response.columns.iter().enumerate() {
                    if let Some(pg_type) = datetime_columns.get(col_name)
                        && let Some(Some(value_bytes)) = row.get_mut(col_idx) {
                            // Apply datetime conversion
                            match pg_type.to_uppercase().as_str() {
                                "DATE" => {
                                    if let Ok(value_str) = std::str::from_utf8(value_bytes)
                                        && let Ok(days) = value_str.parse::<i32>() {
                                            use crate::types::datetime_utils::format_days_to_date_buf;
                                            let mut buf = vec![0u8; 32];
                                            let len = format_days_to_date_buf(days, &mut buf);
                                            buf.truncate(len);
                                            *value_bytes = buf;
                                        }
                                }
                                "TIME" | "TIME WITHOUT TIME ZONE" | "TIME WITH TIME ZONE" | "TIMETZ" => {
                                    if let Ok(value_str) = std::str::from_utf8(value_bytes)
                                        && let Ok(micros) = value_str.parse::<i64>() {
                                            use crate::types::datetime_utils::format_microseconds_to_time_buf;
                                            let mut buf = vec![0u8; 32];
                                            let len = format_microseconds_to_time_buf(micros, &mut buf);
                                            buf.truncate(len);
                                            *value_bytes = buf;
                                        }
                                }
                                "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => {
                                    if let Ok(value_str) = std::str::from_utf8(value_bytes)
                                        && let Ok(micros) = value_str.parse::<i64>() {
                                            // debug!("Converting timestamp {} for column '{}'", micros, col_name);
                                            use crate::types::datetime_utils::format_microseconds_to_timestamp_buf;
                                            let mut buf = vec![0u8; 32];
                                            let len = format_microseconds_to_timestamp_buf(micros, &mut buf);
                                            buf.truncate(len);
                                            *value_bytes = buf;
                                        }
                                }
                                _ => {}
                            }
                        }
                }
            }
        }
        
        // Store row count before potential move
        let row_count = converted_rows.len();
        
        // Prepare wire protocol cache if this query is cacheable
        let mut encoded_rows = Vec::new();
        let should_cache = crate::cache::is_cacheable_for_wire_protocol(query) && row_count <= 1000; // Don't cache huge results
        
        // Optimized data row sending for better SELECT performance
        if converted_rows.len() > 5 {
            // Use batch sending for larger result sets
            if should_cache {
                // Encode rows for caching while sending
                for row in &converted_rows {
                    let encoded = crate::cache::encode_data_row(row);
                    encoded_rows.push(encoded.clone());
                    framed.get_mut().write_all(&encoded).await
                        .map_err(PgSqliteError::Io)?;
                }
            } else {
                Self::send_data_rows_batched(framed, converted_rows).await?;
            }
        } else {
            // Use individual sending for small result sets
            for row in &converted_rows {
                if should_cache {
                    let encoded = crate::cache::encode_data_row(row);
                    encoded_rows.push(encoded.clone());
                    framed.get_mut().write_all(&encoded).await
                        .map_err(PgSqliteError::Io)?;
                } else {
                    framed.send(BackendMessage::DataRow(row.clone())).await
                        .map_err(PgSqliteError::Io)?;
                }
            }
        }
        
        // Cache the response if appropriate
        if should_cache && !encoded_rows.is_empty() {
            let cached_response = crate::cache::CachedWireResponse {
                row_description: fields.clone(),
                encoded_rows,
                row_count,
            };
            crate::cache::WIRE_PROTOCOL_CACHE.put(query.to_string(), cached_response);
            debug!("Cached wire protocol response for query: {}", query);
        }
        
        // Send CommandComplete with optimized tag creation
        let tag = create_command_tag("SELECT", row_count);
        framed.send(BackendMessage::CommandComplete { tag }).await
            .map_err(PgSqliteError::Io)?;
        
        Ok(())
    }
    
    async fn execute_dml<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &Arc<DbHandler>,
        session: &Arc<SessionState>,
        query: &str,
        query_router: Option<&Arc<QueryRouter>>,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        // SQLAlchemy manages transactions explicitly - don't start implicit transactions
        // This was interfering with SQLAlchemy's unit-of-work dirty detection
        
        debug!("execute_dml called with query: {}", query);
        
        // Check for RETURNING clause
        if ReturningTranslator::has_returning_clause(query) {
            debug!("Query has RETURNING clause, using execute_dml_with_returning: {}", query);
            return Self::execute_dml_with_returning(framed, db, session, query, query_router).await;
        } else {
            debug!("Query does NOT have RETURNING clause: {}", query);
        }
        
        // Validate numeric constraints for INSERT/UPDATE before execution
        use crate::query::{QueryTypeDetector, QueryType};
        use crate::validator::NumericValidator;
        
        // Validate before executing - do all database work before any await
        let validation_error = match QueryTypeDetector::detect_query_type(query) {
            QueryType::Insert => {
                if let Some(table_name) = extract_table_name_from_insert(query) {
                    // Validate numeric constraints using session connection
                    match db.with_session_connection(&session.id, |conn| {
                        match NumericValidator::validate_insert(conn, query, &table_name) {
                            Ok(()) => Ok(()),
                            Err(crate::error::PgError::NumericValueOutOfRange { .. }) => {
                                Err(rusqlite::Error::SqliteFailure(
                                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                                    Some("NUMERIC_VALUE_OUT_OF_RANGE".to_string())
                                ))
                            },
                            Err(e) => Err(rusqlite::Error::SqliteFailure(
                                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                                Some(format!("Numeric validation failed: {e}"))
                            ))
                        }
                    }).await {
                        Ok(()) => None,
                        Err(PgSqliteError::Sqlite(rusqlite::Error::SqliteFailure(_, Some(msg)))) if msg == "NUMERIC_VALUE_OUT_OF_RANGE" => {
                            // Create a numeric value out of range error
                            Some(PgSqliteError::Validation(crate::error::PgError::NumericValueOutOfRange {
                                type_name: "numeric".to_string(),
                                column_name: String::new(),
                                value: String::new(),
                            }))
                        },
                        Err(e) => Some(e),
                    }
                } else {
                    None
                }
            }
            QueryType::Update => {
                if let Some(table_name) = extract_table_name_from_update(query) {
                    // Validate numeric constraints using session connection
                    match db.with_session_connection(&session.id, |conn| {
                        match NumericValidator::validate_update(conn, query, &table_name) {
                            Ok(()) => Ok(()),
                            Err(crate::error::PgError::NumericValueOutOfRange { .. }) => {
                                Err(rusqlite::Error::SqliteFailure(
                                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                                    Some("NUMERIC_VALUE_OUT_OF_RANGE".to_string())
                                ))
                            },
                            Err(e) => Err(rusqlite::Error::SqliteFailure(
                                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                                Some(format!("Numeric validation failed: {e}"))
                            ))
                        }
                    }).await {
                        Ok(()) => None,
                        Err(PgSqliteError::Sqlite(rusqlite::Error::SqliteFailure(_, Some(msg)))) if msg == "NUMERIC_VALUE_OUT_OF_RANGE" => {
                            // Create a numeric value out of range error
                            Some(PgSqliteError::Validation(crate::error::PgError::NumericValueOutOfRange {
                                type_name: "numeric".to_string(),
                                column_name: String::new(),
                                value: String::new(),
                            }))
                        },
                        Err(e) => Some(e),
                    }
                } else {
                    None
                }
            }
            _ => None, // No validation needed for DELETE or other DML
        };
        
        // If there was a validation error, send it and return
        if let Some(e) = validation_error {
            let error_response = match &e {
                PgSqliteError::Validation(pg_err) => {
                    // Convert PgError to ErrorResponse directly
                    pg_err.to_error_response()
                }
                _ => {
                    // Default error response for other errors
                    crate::protocol::ErrorResponse {
                        severity: "ERROR".to_string(),
                        code: "23514".to_string(), // check_violation
                        message: e.to_string(),
                        detail: None,
                        hint: None,
                        position: None,
                        internal_position: None,
                        internal_query: None,
                        where_: None,
                        schema: None,
                        table: None,
                        column: None,
                        datatype: None,
                        constraint: None,
                        file: None,
                        line: None,
                        routine: None,
                    }
                }
            };
            framed.send(BackendMessage::ErrorResponse(Box::new(error_response))).await
                .map_err(PgSqliteError::Io)?;
            return Ok(());
        }
        
        // Route query through query router if available
        let response = if let Some(router) = query_router {
            router.execute_query(query, session).await.map_err(|e| PgSqliteError::Protocol(e.to_string()))?
        } else {
            let cached_conn = Self::get_or_cache_connection(session, db).await;
            db.execute_with_session_cached(query, &session.id, cached_conn.as_ref()).await?
        };
        
        // Optimized tag creation with static strings for common cases and buffer pooling for larger counts
        let tag = match QueryTypeDetector::detect_query_type(query) {
            QueryType::Insert => create_command_tag("INSERT", response.rows_affected),
            QueryType::Update => create_command_tag("UPDATE", response.rows_affected),
            QueryType::Delete => create_command_tag("DELETE", response.rows_affected),
            _ => create_command_tag("OK", response.rows_affected),
        };
        
        framed.send(BackendMessage::CommandComplete { tag }).await
            .map_err(PgSqliteError::Io)?;
        
        Ok(())
    }
    
    async fn execute_dml_with_returning<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &Arc<DbHandler>,
        session: &Arc<SessionState>,
        query: &str,
        query_router: Option<&Arc<QueryRouter>>,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        use crate::query::{QueryTypeDetector, QueryType};
        
        // SQLite 3.35.0+ supports native RETURNING clause
        // Execute the query with RETURNING clause directly
        let returning_response = if let Some(router) = query_router {
            router.execute_query(query, session).await.map_err(|e| PgSqliteError::Protocol(e.to_string()))?
        } else {
            let cached_conn = Self::get_or_cache_connection(session, db).await;
            db.query_with_session_cached(query, &session.id, cached_conn.as_ref()).await?
        };
        
        // Extract table name from query for type lookup
        let table_name = match QueryTypeDetector::detect_query_type(query) {
            QueryType::Insert => extract_table_name_from_insert(query),
            QueryType::Update => extract_table_name_from_update(query),
            QueryType::Delete => extract_table_name_from_delete(query),
            _ => None,
        };
        
        // Build field descriptions with proper type information
        let mut fields: Vec<FieldDescription> = Vec::new();
        let mut column_types: Vec<Option<String>> = Vec::new();
        
        for (i, col_name) in returning_response.columns.iter().enumerate() {
            let mut type_oid = PgType::Text.to_oid(); // Default to text
            let mut pg_type = None;
            
            // Try to get type information from schema
            if let Some(ref table) = table_name
                && let Ok(Some(schema_type)) = db.get_schema_type_with_session(&session.id, table, col_name).await {
                    pg_type = Some(schema_type.clone());
                    type_oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(&schema_type);
                }
            
            fields.push(FieldDescription {
                name: col_name.clone(),
                table_oid: 0,
                column_id: (i + 1) as i16,
                type_oid,
                type_size: -1,
                type_modifier: -1,
                format: 0,
            });
            
            column_types.push(pg_type);
        }
        
        framed.send(BackendMessage::RowDescription(fields)).await
            .map_err(PgSqliteError::Io)?;
        
        // Send data rows with proper type conversion
        let mut row_count = 0;
        for row in returning_response.rows {
            // Convert row values based on column types
            let mut converted_row = Vec::new();
            
            for (col_idx, value_opt) in row.iter().enumerate() {
                if let Some(value_bytes) = value_opt {
                    if let Some(Some(pg_type)) = column_types.get(col_idx) {
                        // Apply type-specific formatting for datetime types
                        let formatted = match pg_type.to_uppercase().as_str() {
                            "DATE" => {
                                // Convert INTEGER days to YYYY-MM-DD format
                                if let Ok(value_str) = std::str::from_utf8(value_bytes) {
                                    if let Ok(days) = value_str.parse::<i32>() {
                                        use crate::types::datetime_utils::format_days_to_date_buf;
                                        let mut buf = vec![0u8; 32];
                                        let len = format_days_to_date_buf(days, &mut buf);
                                        buf.truncate(len);
                                        Some(buf)
                                    } else {
                                        Some(value_bytes.clone())
                                    }
                                } else {
                                    Some(value_bytes.clone())
                                }
                            }
                            "TIME" | "TIME WITHOUT TIME ZONE" | "TIME WITH TIME ZONE" | "TIMETZ" => {
                                // Convert INTEGER microseconds to HH:MM:SS.ffffff format
                                if let Ok(value_str) = std::str::from_utf8(value_bytes) {
                                    if let Ok(micros) = value_str.parse::<i64>() {
                                        use crate::types::datetime_utils::format_microseconds_to_time_buf;
                                        let mut buf = vec![0u8; 32];
                                        let len = format_microseconds_to_time_buf(micros, &mut buf);
                                        buf.truncate(len);
                                        Some(buf)
                                    } else {
                                        Some(value_bytes.clone())
                                    }
                                } else {
                                    Some(value_bytes.clone())
                                }
                            }
                            "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => {
                                // Convert INTEGER microseconds to YYYY-MM-DD HH:MM:SS.ffffff format
                                if let Ok(value_str) = std::str::from_utf8(value_bytes) {
                                    if let Ok(micros) = value_str.parse::<i64>() {
                                        use crate::types::datetime_utils::format_microseconds_to_timestamp_buf;
                                        let mut buf = vec![0u8; 32];
                                        let len = format_microseconds_to_timestamp_buf(micros, &mut buf);
                                        buf.truncate(len);
                                        Some(buf)
                                    } else {
                                        Some(value_bytes.clone())
                                    }
                                } else {
                                    Some(value_bytes.clone())
                                }
                            }
                            _ => Some(value_bytes.clone()),
                        };
                        converted_row.push(formatted);
                    } else {
                        converted_row.push(Some(value_bytes.clone()));
                    }
                } else {
                    converted_row.push(None);
                }
            }
            
            framed.send(BackendMessage::DataRow(converted_row)).await
                .map_err(PgSqliteError::Io)?;
            row_count += 1;
        }
        
        // Determine the command tag based on query type
        let query_type = QueryTypeDetector::detect_query_type(query);
        let tag = match query_type {
            QueryType::Insert => format!("INSERT 0 {row_count}"),
            QueryType::Update => format!("UPDATE {row_count}"),
            QueryType::Delete => format!("DELETE {row_count}"),
            _ => format!("OK {row_count}"),
        };
        
        framed.send(BackendMessage::CommandComplete { tag }).await
            .map_err(PgSqliteError::Io)?;
        
        Ok(())
    }
    
    async fn execute_ddl<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &Arc<DbHandler>,
        session: &Arc<SessionState>,
        query: &str,
        _query_router: Option<&Arc<QueryRouter>>,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        use crate::translator::CreateTableTranslator;
        use crate::query::{QueryTypeDetector, QueryType};
        use crate::ddl::EnumDdlHandler;
        use tracing::info;

        // Check if this is a CREATE DATABASE statement
        if matches!(QueryTypeDetector::detect_query_type(query), QueryType::Create) &&
           query.trim_start()[6..].trim_start().to_uppercase().starts_with("DATABASE") {
            info!("CREATE DATABASE command received - SQLite doesn't have database concept, succeeding with no-op");

            // Send command complete
            framed.send(BackendMessage::CommandComplete {
                tag: "CREATE DATABASE".to_string()
            }).await
                .map_err(PgSqliteError::Io)?;

            return Ok(());
        }

        // Check if this is a DROP DATABASE statement
        if matches!(QueryTypeDetector::detect_query_type(query), QueryType::Drop) &&
           query.trim_start()[4..].trim_start().to_uppercase().starts_with("DATABASE") {
            info!("DROP DATABASE command received - SQLite doesn't have database concept, succeeding with no-op");

            // Send command complete
            framed.send(BackendMessage::CommandComplete {
                tag: "DROP DATABASE".to_string()
            }).await
                .map_err(PgSqliteError::Io)?;

            return Ok(());
        }

        // Check if this is a CREATE USER/ROLE statement
        if matches!(QueryTypeDetector::detect_query_type(query), QueryType::Create) {
            let after_create = query.trim_start()[6..].trim_start().to_uppercase();
            if after_create.starts_with("USER") || after_create.starts_with("ROLE") {
                info!("CREATE USER/ROLE command received - SQLite doesn't have user management, succeeding with no-op");

                let tag = if after_create.starts_with("USER") { "CREATE USER" } else { "CREATE ROLE" };
                framed.send(BackendMessage::CommandComplete {
                    tag: tag.to_string()
                }).await
                    .map_err(PgSqliteError::Io)?;

                return Ok(());
            }
        }

        // Check if this is a DROP USER/ROLE statement
        if matches!(QueryTypeDetector::detect_query_type(query), QueryType::Drop) {
            let after_drop = query.trim_start()[4..].trim_start().to_uppercase();
            if after_drop.starts_with("USER") || after_drop.starts_with("ROLE") {
                info!("DROP USER/ROLE command received - SQLite doesn't have user management, succeeding with no-op");

                let tag = if after_drop.starts_with("USER") { "DROP USER" } else { "DROP ROLE" };
                framed.send(BackendMessage::CommandComplete {
                    tag: tag.to_string()
                }).await
                    .map_err(PgSqliteError::Io)?;

                return Ok(());
            }
        }

        // Check if this is an ENUM DDL statement
        if EnumDdlHandler::is_enum_ddl(query) {
            // Handle ENUM DDL with session connections
            db.with_session_connection_mut(&session.id, |conn| {
                EnumDdlHandler::handle_enum_ddl(conn, query)
                    .map_err(|e| rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                        Some(format!("ENUM DDL failed: {e}"))
                    ))
            }).await?;
            
            let command_tag = if query.trim().to_uppercase().starts_with("CREATE TYPE") {
                "CREATE TYPE"
            } else if query.trim().to_uppercase().starts_with("ALTER TYPE") {
                "ALTER TYPE"
            } else if query.trim().to_uppercase().starts_with("DROP TYPE") {
                "DROP TYPE"  
            } else {
                "OK"
            };
            
            // Send command complete
            framed.send(BackendMessage::CommandComplete { 
                tag: command_tag.to_string() 
            }).await
                .map_err(PgSqliteError::Io)?;
            
            return Ok(());
        }
        
        let (translated_query, type_mappings, enum_columns, array_columns) = if matches!(QueryTypeDetector::detect_query_type(query), QueryType::Create) && query.trim_start()[6..].trim_start().to_uppercase().starts_with("TABLE") {
            // Use CREATE TABLE translator with connection for ENUM support
            db.with_session_connection(&session.id, |conn| {
                let result = CreateTableTranslator::translate_with_connection_full(query, Some(conn))
                    .map_err(|e| rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                        Some(format!("CREATE TABLE translation failed: {e}"))
                    ))?;
                
                Ok((result.sql, result.type_mappings, result.enum_columns, result.array_columns))
            }).await?
        } else {
            // For other DDL, check for JSON/JSONB types
            let translated = if query.to_lowercase().contains("json") || query.to_lowercase().contains("jsonb") {
                JsonTranslator::translate_statement(query)?
            } else {
                query.to_string()
            };
            (translated, std::collections::HashMap::new(), Vec::new(), Vec::new())
        };
        
        // Check if this is a DROP TABLE command and extract table name
        let is_drop_table = matches!(QueryTypeDetector::detect_query_type(query), QueryType::Drop) 
            && query.trim_start()[4..].trim_start().to_uppercase().starts_with("TABLE");
        
        let table_name_to_clean = if is_drop_table {
            // Extract table name from DROP TABLE statement
            let drop_table_pattern = regex::Regex::new(r"(?i)DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)").unwrap();
            drop_table_pattern.captures(query)
                .and_then(|caps| caps.get(1))
                .map(|m| m.as_str().to_string())
        } else {
            None
        };
        
        // Execute the translated query
        let cached_conn = Self::get_or_cache_connection(session, db).await;
        db.execute_with_session_cached(&translated_query, &session.id, cached_conn.as_ref()).await?;
        
        // If this was a DROP TABLE, clean up enum usage records
        if let Some(table_name) = table_name_to_clean {
            db.with_session_connection_mut(&session.id, |conn| {
                use crate::metadata::EnumTriggers;
                EnumTriggers::clean_enum_usage_for_table(conn, &table_name)
                    .map_err(|e| rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                        Some(format!("Failed to clean enum usage for table {table_name}: {e}"))
                    ))
            }).await?;
            debug!("Cleaned up enum usage records for dropped table: {}", table_name);
        }
        
        // If we have type mappings, store them in the metadata table
        debug!("Type mappings count: {}", type_mappings.len());
        if !type_mappings.is_empty() {
            // Extract table name from the original query
            if let Some(table_name) = extract_table_name_from_create(query) {
                // Initialize the metadata table if it doesn't exist
                let init_query = "CREATE TABLE IF NOT EXISTS __pgsqlite_schema (
                    table_name TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    pg_type TEXT NOT NULL,
                    sqlite_type TEXT NOT NULL,
                    PRIMARY KEY (table_name, column_name)
                )";
                
                let cached_conn = Self::get_or_cache_connection(session, db).await;
                match db.execute_with_session_cached(init_query, &session.id, cached_conn.as_ref()).await {
                    Ok(_) => debug!("Successfully created/verified __pgsqlite_schema table"),
                    Err(e) => debug!("Failed to create __pgsqlite_schema table: {}", e),
                }
                
                // Store each type mapping
                for (full_column, type_mapping) in &type_mappings {
                    // Split table.column format
                    let parts: Vec<&str> = full_column.split('.').collect();
                    if parts.len() == 2 && parts[0] == table_name {
                        let insert_query = format!(
                            "INSERT OR REPLACE INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES ('{}', '{}', '{}', '{}')",
                            table_name, parts[1], type_mapping.pg_type, type_mapping.sqlite_type
                        );
                        
                        let cached_conn = Self::get_or_cache_connection(session, db).await;
                        match db.execute_with_session_cached(&insert_query, &session.id, cached_conn.as_ref()).await {
                            Ok(_) => debug!("Stored metadata: {}.{} -> {} ({})", table_name, parts[1], type_mapping.pg_type, type_mapping.sqlite_type),
                            Err(e) => debug!("Failed to store metadata for {}.{}: {}", table_name, parts[1], e),
                        }
                        
                        // Store string constraints if present
                        if let Some(modifier) = type_mapping.type_modifier {
                            // Extract base type without parameters
                            let base_type = if let Some(paren_pos) = type_mapping.pg_type.find('(') {
                                type_mapping.pg_type[..paren_pos].trim()
                            } else {
                                &type_mapping.pg_type
                            };
                            let pg_type_lower = base_type.to_lowercase();
                            debug!("Processing type mapping: {}.{} -> {} (base_type: {}, modifier: {})",
                                table_name, parts[1], type_mapping.pg_type, base_type, modifier);
                            
                            if pg_type_lower == "varchar" || pg_type_lower == "char" || 
                               pg_type_lower == "character varying" || pg_type_lower == "character" ||
                               pg_type_lower == "nvarchar" {
                                let is_char = pg_type_lower == "char" || pg_type_lower == "character";
                                let constraint_query = format!(
                                    "INSERT OR REPLACE INTO __pgsqlite_string_constraints (table_name, column_name, max_length, is_char_type) 
                                     VALUES ('{}', '{}', {}, {})",
                                    table_name, parts[1], modifier, if is_char { 1 } else { 0 }
                                );
                                
                                let cached_conn = Self::get_or_cache_connection(session, db).await;
                                match db.execute_with_session_cached(&constraint_query, &session.id, cached_conn.as_ref()).await {
                                    Ok(_) => debug!("Stored string constraint: {}.{} max_length={}", table_name, parts[1], modifier),
                                    Err(e) => debug!("Failed to store string constraint for {}.{}: {}", table_name, parts[1], e),
                                }
                            } else if pg_type_lower == "numeric" || pg_type_lower == "decimal" {
                                // Decode precision and scale from modifier
                                let tmp_typmod = modifier - 4; // Remove VARHDRSZ
                                let precision = (tmp_typmod >> 16) & 0xFFFF;
                                let scale = tmp_typmod & 0xFFFF;
                                
                                
                                let constraint_query = format!(
                                    "INSERT OR REPLACE INTO __pgsqlite_numeric_constraints (table_name, column_name, precision, scale) 
                                     VALUES ('{}', '{}', {}, {})",
                                    table_name, parts[1], precision, scale
                                );
                                
                                let cached_conn = Self::get_or_cache_connection(session, db).await;
                                match db.execute_with_session_cached(&constraint_query, &session.id, cached_conn.as_ref()).await {
                                    Ok(_) => {
                                        debug!("Stored numeric constraint: {}.{} precision={} scale={}", table_name, parts[1], precision, scale);
                                    }
                                    Err(e) => {
                                        debug!("Failed to store numeric constraint for {}.{}: {}", table_name, parts[1], e);
                                    }
                                }
                            }
                        }
                    }
                }
                
                debug!("Stored type mappings for table {} (simple query protocol)", table_name);
                
                // Create triggers for ENUM columns
                if !enum_columns.is_empty() {
                    db.with_session_connection(&session.id, |conn| {
                        for (column_name, enum_type) in &enum_columns {
                            // Record enum usage
                            EnumTriggers::record_enum_usage(conn, &table_name, column_name, enum_type)
                                .map_err(|e| rusqlite::Error::SqliteFailure(
                                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                                    Some(format!("Failed to record enum usage: {e}"))
                                ))?;
                            
                            // Create validation triggers
                            EnumTriggers::create_enum_validation_triggers(conn, &table_name, column_name, enum_type)
                                .map_err(|e| rusqlite::Error::SqliteFailure(
                                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                                    Some(format!("Failed to create enum triggers: {e}"))
                                ))?;
                            
                            debug!("Created ENUM validation triggers for {}.{} (type: {})", table_name, column_name, enum_type);
                        }
                        Ok(())
                    }).await?;
                }
                
                // Store array column metadata
                if !array_columns.is_empty() {
                    db.with_session_connection(&session.id, |conn| {
                        // Create array metadata table if it doesn't exist (should exist from migration v8)
                        conn.execute(
                            "CREATE TABLE IF NOT EXISTS __pgsqlite_array_types (
                                table_name TEXT NOT NULL,
                                column_name TEXT NOT NULL,
                                element_type TEXT NOT NULL,
                                dimensions INTEGER DEFAULT 1,
                                PRIMARY KEY (table_name, column_name)
                            )", 
                            []
                        ).map_err(|e| rusqlite::Error::SqliteFailure(
                            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                            Some(format!("Failed to create array metadata table: {e}"))
                        ))?;
                        
                        // Insert array column metadata
                        for (column_name, element_type, dimensions) in &array_columns {
                            conn.execute(
                                "INSERT OR REPLACE INTO __pgsqlite_array_types (table_name, column_name, element_type, dimensions) 
                                 VALUES (?1, ?2, ?3, ?4)",
                                params![table_name, column_name, element_type, dimensions]
                            ).map_err(|e| rusqlite::Error::SqliteFailure(
                            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                            Some(format!("Failed to store array metadata: {e}"))
                        ))?;
                            
                            debug!("Stored array column metadata for {}.{} (element_type: {}, dimensions: {})", 
                                  table_name, column_name, element_type, dimensions);
                        }
                        Ok(())
                    }).await?;
                }
                
                // Numeric validation is now handled at the application layer in execute_dml
                // No need for triggers anymore
                
                // Datetime conversion is now handled by InsertTranslator and value converters
                // No need for triggers anymore
                
            }
        }

        // Populate PostgreSQL catalog tables with constraint information for ALL CREATE TABLE statements
        if let Some(table_name) = extract_table_name_from_create(query) {
            db.with_session_connection(&session.id, |conn| {
                // Populate pg_constraint, pg_attrdef, and pg_index tables
                if let Err(e) = crate::catalog::constraint_populator::populate_constraints_for_table(conn, &table_name) {
                    // Log the error but don't fail the CREATE TABLE operation
                    debug!("Failed to populate constraints for table {}: {}", table_name, e);
                } else {
                    debug!("Successfully populated constraint catalog tables for table: {}", table_name);
                }
                Ok(())
            }).await?;
        }
        
        let tag = match QueryTypeDetector::detect_query_type(query) {
            QueryType::Create => {
                let after_create = query.trim_start()[6..].trim_start();
                if after_create.to_uppercase().starts_with("TABLE") {
                    "CREATE TABLE".to_string()
                } else if after_create.to_uppercase().starts_with("INDEX") {
                    "CREATE INDEX".to_string()
                } else if after_create.to_uppercase().starts_with("DATABASE") {
                    "CREATE DATABASE".to_string()
                } else if after_create.to_uppercase().starts_with("USER") {
                    "CREATE USER".to_string()
                } else if after_create.to_uppercase().starts_with("ROLE") {
                    "CREATE ROLE".to_string()
                } else {
                    "CREATE".to_string()
                }
            }
            QueryType::Drop => {
                let after_drop = query.trim_start()[4..].trim_start();
                if after_drop.to_uppercase().starts_with("TABLE") {
                    "DROP TABLE".to_string()
                } else if after_drop.to_uppercase().starts_with("DATABASE") {
                    "DROP DATABASE".to_string()
                } else if after_drop.to_uppercase().starts_with("USER") {
                    "DROP USER".to_string()
                } else if after_drop.to_uppercase().starts_with("ROLE") {
                    "DROP ROLE".to_string()
                } else {
                    "DROP".to_string()
                }
            }
            _ => "OK".to_string(),
        };
        
        framed.send(BackendMessage::CommandComplete { tag }).await
            .map_err(PgSqliteError::Io)?;
        
        Ok(())
    }
    
    async fn execute_transaction<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &Arc<DbHandler>,
        session: &Arc<SessionState>,
        query: &str,
        _query_router: Option<&Arc<QueryRouter>>,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        use crate::query::{QueryTypeDetector, QueryType};
        use crate::protocol::TransactionStatus;
        
        // Check if we're in a failed transaction
        let current_status = session.get_transaction_status().await;
        if current_status == TransactionStatus::InFailedTransaction {
            // Only ROLLBACK is allowed in a failed transaction
            if !matches!(QueryTypeDetector::detect_query_type(query), QueryType::Rollback) {
                return Err(PgSqliteError::Protocol(
                    "current transaction is aborted, commands ignored until end of transaction block".to_string()
                ));
            }
        }
        
        match QueryTypeDetector::detect_query_type(query) {
            QueryType::Begin => {
                // Check if we're already in a transaction
                if current_status == TransactionStatus::InTransaction {
                    // PostgreSQL behavior: warn but don't fail
                    tracing::warn!("BEGIN command received while already in transaction");
                    // Send a warning notice
                    use crate::protocol::messages::NoticeResponse;
                    framed.send(BackendMessage::NoticeResponse(NoticeResponse {
                        severity: "WARNING".to_string(),
                        code: "25001".to_string(), // active_sql_transaction
                        message: "there is already a transaction in progress".to_string(),
                        detail: None,
                        hint: None,
                        position: None,
                        where_: None,
                    })).await.map_err(PgSqliteError::Io)?;
                    // Still send CommandComplete, but don't actually execute BEGIN
                    framed.send(BackendMessage::CommandComplete { tag: "BEGIN".to_string() }).await
                        .map_err(PgSqliteError::Io)?;
                } else {
                    tracing::debug!("Executing BEGIN command");
                    db.begin_with_session(&session.id).await?;
                    tracing::debug!("BEGIN executed successfully");
                    // Update transaction status to InTransaction
                    *session.transaction_status.write().await = TransactionStatus::InTransaction;
                    tracing::debug!("Transaction status updated to InTransaction");
                    framed.send(BackendMessage::CommandComplete { tag: "BEGIN".to_string() }).await
                        .map_err(PgSqliteError::Io)?;
                }
            }
            QueryType::Commit => {
                // Can't commit a failed transaction
                if current_status == TransactionStatus::InFailedTransaction {
                    return Err(PgSqliteError::Protocol(
                        "current transaction is aborted, commands ignored until end of transaction block".to_string()
                    ));
                }
                tracing::debug!("Executing COMMIT command");
                db.commit_with_session(&session.id).await?;
                tracing::debug!("COMMIT executed successfully");
                
                // Update transaction status to Idle
                *session.transaction_status.write().await = TransactionStatus::Idle;
                tracing::debug!("Transaction status updated to Idle");
                framed.send(BackendMessage::CommandComplete { tag: "COMMIT".to_string() }).await
                    .map_err(PgSqliteError::Io)?;
            }
            QueryType::Rollback => {
                // Use the rollback method which handles the "no transaction active" case gracefully
                db.rollback_with_session(&session.id).await.map_err(|e| PgSqliteError::Protocol(e.to_string()))?;
                
                // Update transaction status to Idle (regardless of previous state)
                *session.transaction_status.write().await = TransactionStatus::Idle;
                framed.send(BackendMessage::CommandComplete { tag: "ROLLBACK".to_string() }).await
                    .map_err(PgSqliteError::Io)?;
            }
            _ => {}
        }
        
        Ok(())
    }
    
    async fn execute_generic<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &Arc<DbHandler>,
        session: &Arc<SessionState>,
        query: &str,
        query_router: Option<&Arc<QueryRouter>>,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        // Try to execute as a simple statement
        if let Some(router) = query_router {
            router.execute_query(query, session).await.map_err(|e| PgSqliteError::Protocol(e.to_string()))?;
        } else {
            let cached_conn = Self::get_or_cache_connection(session, db).await;
            db.execute_with_session_cached(query, &session.id, cached_conn.as_ref()).await?;
        }
        
        framed.send(BackendMessage::CommandComplete { tag: "OK".to_string() }).await
            .map_err(PgSqliteError::Io)?;
        
        Ok(())
    }
    
    /// Optimized batch sending of data rows with intelligent batching
    async fn send_data_rows_batched<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        rows: Vec<Vec<Option<Vec<u8>>>>,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        use futures::SinkExt;
        
        // Use intelligent batch sizing based on result set size
        let batch_size = if rows.len() <= 20 {
            // Small result sets: send individually to minimize latency
            1
        } else if rows.len() <= 100 {
            // Medium result sets: use small batches
            10
        } else {
            // Large result sets: use larger batches for throughput
            25
        };
        
        if batch_size == 1 {
            // Send individually for small result sets
            for row in rows {
                framed.send(BackendMessage::DataRow(row)).await
                    .map_err(PgSqliteError::Io)?;
            }
        } else {
            // Send in batches with periodic flushing
            let mut row_iter = rows.into_iter();
            loop {
                let mut batch_sent = false;
                for _ in 0..batch_size {
                    if let Some(row) = row_iter.next() {
                        framed.send(BackendMessage::DataRow(row)).await
                            .map_err(PgSqliteError::Io)?;
                        batch_sent = true;
                    } else {
                        break;
                    }
                }
                if !batch_sent {
                    break;
                }
                // Flush after each batch to ensure timely delivery
                framed.flush().await.map_err(PgSqliteError::Io)?;
            }
        }
        
        Ok(())
    }
    
    /// Check if this is a datetime expression that we translated
    fn is_datetime_expression(query: &str, column_name: &str) -> bool {
        // Check if the query contains our datetime translation patterns
        // Looking for patterns like: CAST((julianday(...) - 2440587.5) * 86400 AS REAL)
        // Also check if the column name matches common date function patterns
        let has_datetime_translation = query.contains("julianday") && query.contains("2440587.5") && query.contains("86400");
        let is_date_function = column_name.starts_with("date(") || 
                              column_name.starts_with("DATE(") ||
                              column_name.starts_with("time(") ||
                              column_name.starts_with("TIME(") ||
                              column_name.starts_with("datetime(") ||
                              column_name.starts_with("DATETIME(");
        
        has_datetime_translation || is_date_function
    }
    
    /// Convert array data in rows using type OIDs from field descriptions
    fn convert_array_data_in_rows(
        rows: Vec<Vec<Option<Vec<u8>>>>,
        fields: &[FieldDescription],
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, PgSqliteError> {
        // Extract type OIDs from field descriptions
        let type_oids: Vec<i32> = fields.iter().map(|f| f.type_oid).collect();
        debug!("Type OIDs for conversion: {:?}", type_oids);
        debug!("Boolean type OID: {}", PgType::Bool.to_oid());
        
        // Quick check: if no array, boolean, or datetime types, return rows as-is
        let bool_oid = PgType::Bool.to_oid();
        let date_oid = PgType::Date.to_oid();
        let time_oid = PgType::Time.to_oid();
        let timetz_oid = PgType::Timetz.to_oid();
        let timestamp_oid = PgType::Timestamp.to_oid();
        let timestamptz_oid = PgType::Timestamptz.to_oid();
        
        let needs_conversion = type_oids.iter().any(|&oid| {
            oid == bool_oid || 
            oid == date_oid ||
            oid == time_oid ||
            oid == timetz_oid ||
            oid == timestamp_oid ||
            oid == timestamptz_oid ||
            PgType::from_oid(oid).is_some_and(|t| t.is_array())
        });
        
        if !needs_conversion {
            return Ok(rows);
        }
        
        // Convert each row
        let mut converted_rows = Vec::with_capacity(rows.len());
        
        for row in rows {
            let mut converted_row = Vec::with_capacity(row.len());
            
            for (col_idx, cell) in row.into_iter().enumerate() {
                let converted_cell = if let Some(data) = cell {
                    let type_oid = type_oids.get(col_idx).copied().unwrap_or(25); // Default to TEXT
                    
                    // Check if this is an array type that needs conversion
                    if PgType::from_oid(type_oid).is_some_and(|t| t.is_array()) {
                        // Try to convert JSON array to PostgreSQL array format
                        match Self::convert_json_to_pg_array(&data) {
                            Ok(converted_data) => Some(converted_data),
                            Err(_) => Some(data), // Keep original data if conversion fails
                        }
                    } else if type_oid == PgType::Bool.to_oid() {
                        // Convert boolean values from integer 0/1 to PostgreSQL f/t format
                        // Optimized: work directly with bytes to avoid string conversion overhead
                        if data.len() == 1 && data[0] == b'0' {
                            Some(b"f".to_vec())
                        } else if data.len() == 1 && data[0] == b'1' {
                            Some(b"t".to_vec())
                        } else {
                            Some(data) // Keep original data if not 0/1
                        }
                    } else if type_oid == date_oid {
                        // Convert INTEGER days to YYYY-MM-DD format
                        if let Ok(s) = std::str::from_utf8(&data) {
                            if let Ok(days) = s.parse::<i32>() {
                                use crate::types::datetime_utils::format_days_to_date_buf;
                                let mut buf = vec![0u8; 32];
                                let len = format_days_to_date_buf(days, &mut buf);
                                buf.truncate(len);
                                Some(buf)
                            } else {
                                Some(data) // Keep original if not an integer
                            }
                        } else {
                            Some(data) // Keep original if not valid UTF-8
                        }
                    } else if type_oid == time_oid || type_oid == timetz_oid {
                        // Convert INTEGER microseconds to HH:MM:SS.ffffff format
                        if let Ok(s) = std::str::from_utf8(&data) {
                            if let Ok(micros) = s.parse::<i64>() {
                                use crate::types::datetime_utils::format_microseconds_to_time_buf;
                                let mut buf = vec![0u8; 32];
                                let len = format_microseconds_to_time_buf(micros, &mut buf);
                                buf.truncate(len);
                                Some(buf)
                            } else {
                                Some(data) // Keep original if not an integer
                            }
                        } else {
                            Some(data) // Keep original if not valid UTF-8
                        }
                    } else if type_oid == timestamp_oid || type_oid == timestamptz_oid {
                        // Convert INTEGER microseconds to YYYY-MM-DD HH:MM:SS.ffffff format
                        if let Ok(s) = std::str::from_utf8(&data) {
                            if let Ok(micros) = s.parse::<i64>() {
                                use crate::types::datetime_utils::format_microseconds_to_timestamp_buf;
                                let mut buf = vec![0u8; 32];
                                let len = format_microseconds_to_timestamp_buf(micros, &mut buf);
                                buf.truncate(len);
                                Some(buf)
                            } else {
                                Some(data) // Keep original if not an integer
                            }
                        } else {
                            Some(data) // Keep original if not valid UTF-8
                        }
                    } else {
                        Some(data)
                    }
                } else {
                    None
                };
                
                converted_row.push(converted_cell);
            }
            
            converted_rows.push(converted_row);
        }
        
        Ok(converted_rows)
    }
    
    /// Convert JSON array string to PostgreSQL array format
    pub fn convert_json_to_pg_array(json_data: &[u8]) -> Result<Vec<u8>, String> {
        // Convert bytes to string
        let s = std::str::from_utf8(json_data).map_err(|_| "Invalid UTF-8")?;
        
        // Try to parse as JSON array
        match serde_json::from_str::<serde_json::Value>(s) {
            Ok(json_val) => {
                if let serde_json::Value::Array(arr) = json_val {
                    // Convert to PostgreSQL array literal format
                    let pg_array = Self::json_array_to_pg_text(&arr);
                    Ok(pg_array.into_bytes())
                } else {
                    // Not an array, return as-is
                    Ok(json_data.to_vec())
                }
            }
            Err(_) => {
                // Not valid JSON, return as-is
                Ok(json_data.to_vec())
            }
        }
    }
    
    /// Convert JSON array elements to PostgreSQL text array format
    fn json_array_to_pg_text(arr: &[serde_json::Value]) -> String {
        let elements: Vec<String> = arr.iter().map(|elem| {
            match elem {
                serde_json::Value::Null => "NULL".to_string(),
                serde_json::Value::Bool(b) => b.to_string(),
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::String(s) => {
                    // Escape quotes and backslashes
                    let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
                    format!("\"{escaped}\"")
                }
                serde_json::Value::Array(_) => {
                    // Nested arrays - convert recursively
                    // For now, just stringify
                    elem.to_string()
                }
                serde_json::Value::Object(_) => {
                    // Objects - stringify
                    elem.to_string()
                }
            }
        }).collect();
        
        format!("{{{}}}", elements.join(","))
    }
}

fn extract_table_name_from_select(query: &str) -> Option<String> {
    // Look for FROM keyword using regex to handle various whitespace patterns
    use once_cell::sync::Lazy;
    use regex::Regex;
    
    // debug!("extract_table_name_from_select called with query: {}", query);
    
    static FROM_TABLE_REGEX: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)\bFROM\s+([^\s,;()]+)").unwrap()
    });
    
    if let Some(captures) = FROM_TABLE_REGEX.captures(query)
        && let Some(table_match) = captures.get(1) {
            let table_name = table_match.as_str().trim();
            
            // Remove quotes if present
            let table_name = table_name.trim_matches('"').trim_matches('\'');
            
            if !table_name.is_empty() {
                // debug!("extract_table_name_from_select: extracted table='{}'", table_name);
                debug!("extract_table_name_from_select: query='{}' -> table='{}'", query, table_name);
                return Some(table_name.to_string());
            }
        }
    
    // debug!("extract_table_name_from_select: failed to extract table name");
    debug!("extract_table_name_from_select: query='{}' -> None", query);
    None
}

/// Extract column mappings from SELECT query with AS aliases
fn extract_column_mappings_from_query(query: &str, table: &str) -> std::collections::HashMap<String, String> {
    use regex::Regex;
    use std::collections::HashMap;
    
    let mut mappings = HashMap::new();
    
    debug!("extract_column_mappings_from_query: query='{}', table='{}'", query, table);
    
    // First, try to match patterns like "table.column_name AS alias"
    let table_pattern = Regex::new(&format!(
        r"(?i)\b{}\.(\w+)\s+AS\s+(\w+)",
        regex::escape(table)
    ));
    
    if let Ok(re) = table_pattern {
        debug!("Table pattern regex created: {:?}", re.as_str());
        let matches_found = re.captures_iter(query).count();
        debug!("Table pattern matches found: {}", matches_found);
        
        for captures in re.captures_iter(query) {
            if let (Some(source_col), Some(alias)) = (captures.get(1), captures.get(2)) {
                let source_column = source_col.as_str().to_string();
                let alias_name = alias.as_str().to_string();
                
                debug!("Column mapping (with table prefix): {} -> {}.{}", alias_name, table, source_column);
                mappings.insert(alias_name, source_column);
            }
        }
    } else {
        debug!("Failed to create table pattern regex");
    }
    
    // Also match simple patterns like "column_name AS alias" (without table prefix)
    // This is common in queries like "SELECT id AS event_id, created_at AS event_created_at FROM events"
    // BUT we need to be careful not to match the table name in "table.column AS alias" patterns
    let simple_pattern = Regex::new(r"(?i)(?:^|,|\s)(\w+)\s+AS\s+(\w+)");
    
    if let Ok(re) = simple_pattern {
        for captures in re.captures_iter(query) {
            if let (Some(source_col), Some(alias)) = (captures.get(1), captures.get(2)) {
                let source_column = source_col.as_str().to_string();
                let alias_name = alias.as_str().to_string();
                
                // Only add if we haven't already found this alias with a table prefix
                // (table-prefixed mappings are more specific and should take precedence)
                if let std::collections::hash_map::Entry::Vacant(e) = mappings.entry(alias_name.clone()) {
                    // Check if this is actually a table name (if the character before it is a dot)
                    // We need to look at the full match to see if there's a dot before
                    let _full_match = captures.get(0).unwrap().as_str();
                    // Skip if this looks like it's part of a table.column pattern
                    // (i.e., the source_column is actually the table name)
                    if !query.contains(&format!("{source_column}.{alias_name}")) &&
                       !query.contains(&format!("{source_column}.")) {
                        debug!("Column mapping (simple alias): {} -> {}", alias_name, source_column);
                        e.insert(source_column);
                    }
                }
            }
        }
    }
    
    // Handle wildcard patterns like "table.*" 
    // For these, we need to map each actual column back to itself for datetime conversion
    let wildcard_pattern = Regex::new(&format!(r"(?i)\b{}\.\*", regex::escape(table)));
    if let Ok(re) = wildcard_pattern
        && re.is_match(query) {
            debug!("Detected wildcard pattern for table: {}", table);
            // For wildcard patterns, we'll let the caller handle the actual column mapping
            // by checking if the query contains "table.*" and then looking at actual column names
            // This is handled in the execute_select function
        }
    
    debug!("Final column mappings: {:?}", mappings);
    mappings
}

/// Extract table name from CREATE TABLE statement
pub fn extract_table_name_from_create(query: &str) -> Option<String> {
    // Look for CREATE TABLE pattern with case-insensitive search
    let create_table_pos = query.as_bytes().windows(12)
        .position(|window| window.eq_ignore_ascii_case(b"CREATE TABLE"))?;
    
    let after_create = &query[create_table_pos + 12..].trim();
    
    // Skip IF NOT EXISTS if present
    let after_create = if after_create.len() >= 13 && after_create[..13].eq_ignore_ascii_case("IF NOT EXISTS") {
        &after_create[13..].trim()
    } else {
        after_create
    };
    
    // Find the end of table name
    let table_end = after_create.find(|c: char| {
        c.is_whitespace() || c == '('
    }).unwrap_or(after_create.len());
    
    let table_name = after_create[..table_end].trim();
    
    // Remove quotes if present
    let table_name = table_name.trim_matches('"').trim_matches('\'');
    
    if !table_name.is_empty() {
        Some(table_name.to_string())
    } else {
        None
    }
}

/// Extract table name from INSERT statement
fn extract_table_name_from_insert(query: &str) -> Option<String> {
    // Look for INSERT INTO pattern with case-insensitive search
    let insert_pos = query.as_bytes().windows(11)
        .position(|window| window.eq_ignore_ascii_case(b"INSERT INTO"))?;
    
    let after_insert = &query[insert_pos + 11..].trim();
    
    // Find the end of table name
    let table_end = after_insert.find(|c: char| {
        c.is_whitespace() || c == '(' || c == ';'
    }).unwrap_or(after_insert.len());
    
    let table_name = after_insert[..table_end].trim();
    
    // Remove quotes if present
    let table_name = table_name.trim_matches('"').trim_matches('\'');
    
    if !table_name.is_empty() {
        Some(table_name.to_string())
    } else {
        None
    }
}

/// Extract table name from UPDATE statement
fn extract_table_name_from_update(query: &str) -> Option<String> {
    // Look for UPDATE pattern with case-insensitive search
    let update_pos = query.as_bytes().windows(6)
        .position(|window| window.eq_ignore_ascii_case(b"UPDATE"))?;
    
    let after_update = &query[update_pos + 6..].trim();
    
    // Find the end of table name (SET keyword)
    let table_end = after_update.find(|c: char| {
        c.is_whitespace() || c == ';'
    }).unwrap_or(after_update.len());
    
    let table_name = after_update[..table_end].trim();
    
    // Remove quotes if present
    let table_name = table_name.trim_matches('"').trim_matches('\'');
    
    if !table_name.is_empty() {
        Some(table_name.to_string())
    } else {
        None
    }
}

/// Extract table name from DELETE statement
fn extract_table_name_from_delete(query: &str) -> Option<String> {
    // Look for DELETE FROM pattern with case-insensitive search
    let delete_pos = query.as_bytes().windows(6)
        .position(|window| window.eq_ignore_ascii_case(b"DELETE"))?;
    
    let after_delete = &query[delete_pos + 6..].trim();
    
    // Skip optional FROM keyword
    let after_from = if after_delete.to_uppercase().starts_with("FROM") {
        &after_delete[4..].trim()
    } else {
        after_delete
    };
    
    // Find the end of table name (WHERE or end of query)
    let table_end = after_from.find(|c: char| {
        c.is_whitespace() || c == ';'
    }).unwrap_or(after_from.len());
    
    let table_name = after_from[..table_end].trim();
    
    // Remove quotes if present
    let table_name = table_name.trim_matches('"').trim_matches('\'');
    
    if !table_name.is_empty() {
        Some(table_name.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_json_to_pg_array_conversion() {
        let json_data = b"[\"a\", \"b\", \"c\"]";
        let result = QueryExecutor::convert_json_to_pg_array(json_data).unwrap();
        let pg_array = String::from_utf8(result).unwrap();
        assert_eq!(pg_array, r#"{"a","b","c"}"#);
    }
    
    #[test]
    fn test_json_to_pg_array_numbers() {
        let json_data = b"[1, 2, 3]";
        let result = QueryExecutor::convert_json_to_pg_array(json_data).unwrap();
        let pg_array = String::from_utf8(result).unwrap();
        assert_eq!(pg_array, "{1,2,3}");
    }
    
    #[test]
    fn test_non_array_json() {
        let json_data = b"\"not an array\"";
        let result = QueryExecutor::convert_json_to_pg_array(json_data).unwrap();
        assert_eq!(result, json_data);
    }
    
    #[test]
    fn test_array_type_detection() {
        use crate::protocol::FieldDescription;
        
        // Test that TextArray type OID 1009 is correctly detected as an array
        let text_array_type = PgType::TextArray.to_oid();
        assert_eq!(text_array_type, 1009);
        assert!(PgType::from_oid(text_array_type).is_some_and(|t| t.is_array()));
        
        // Test that regular text is not detected as an array
        let text_type = PgType::Text.to_oid();
        assert_eq!(text_type, 25);
        assert!(!PgType::from_oid(text_type).is_some_and(|t| t.is_array()));
        
        // Test conversion with array type
        let fields = vec![
            FieldDescription {
                name: "test_col".to_string(),
                table_oid: 0,
                column_id: 1,
                type_oid: 1009, // TextArray
                type_size: -1,
                type_modifier: -1,
                format: 0,
            }
        ];
        
        let rows = vec![vec![Some(b"[\"a\", \"b\", \"c\"]".to_vec())]];
        let converted = QueryExecutor::convert_array_data_in_rows(rows, &fields).unwrap();
        let result_data = &converted[0][0].as_ref().unwrap();
        let result_str = String::from_utf8_lossy(result_data);
        assert_eq!(result_str, r#"{"a","b","c"}"#);
    }
}