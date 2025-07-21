use crate::protocol::{BackendMessage, FieldDescription};
use crate::session::{DbHandler, SessionState, QueryRouter};
use crate::catalog::CatalogInterceptor;
use crate::translator::{JsonTranslator, ReturningTranslator};
use crate::types::PgType;
use crate::cache::{RowDescriptionKey, GLOBAL_ROW_DESCRIPTION_CACHE};
use crate::metadata::EnumTriggers;
use crate::PgSqliteError;
use tokio_util::codec::Framed;
use futures::SinkExt;
use tracing::{info, debug};
use std::sync::Arc;
use rusqlite::params;
use serde_json;
use std::collections::HashMap;
use parking_lot::RwLock;
use once_cell::sync::Lazy;

/// Cache for boolean column information to avoid repeated database queries
static BOOLEAN_COLUMNS_CACHE: Lazy<RwLock<HashMap<String, std::collections::HashSet<String>>>> = 
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Get boolean columns for a table, using cache for performance
fn get_boolean_columns(table_name: &str, db: &DbHandler) -> std::collections::HashSet<String> {
    // Check cache first
    {
        let cache = BOOLEAN_COLUMNS_CACHE.read();
        if let Some(cached_columns) = cache.get(table_name) {
            return cached_columns.clone();
        }
    }
    
    // Cache miss - query the database
    let mut boolean_columns = std::collections::HashSet::new();
    
    if let Ok(conn) = db.get_mut_connection() {
        if let Ok(mut stmt) = conn.prepare("SELECT column_name, pg_type FROM __pgsqlite_schema WHERE table_name = ?1") {
            if let Ok(rows) = stmt.query_map([table_name], |row| {
                let col_name: String = row.get(0)?;
                let pg_type: String = row.get(1)?;
                Ok((col_name, pg_type))
            }) {
                for row in rows.flatten() {
                    let (col_name, pg_type) = row;
                    if pg_type.eq_ignore_ascii_case("boolean") || pg_type.eq_ignore_ascii_case("bool") {
                        boolean_columns.insert(col_name);
                    }
                }
            }
        }
    }
    
    // Cache the result
    {
        let mut cache = BOOLEAN_COLUMNS_CACHE.write();
        cache.insert(table_name.to_string(), boolean_columns.clone());
    }
    
    boolean_columns
}

/// Cache for datetime column information to avoid repeated database queries
static DATETIME_COLUMNS_CACHE: Lazy<RwLock<HashMap<String, std::collections::HashMap<String, String>>>> = 
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Get datetime columns for a table, using cache for performance
/// Returns a HashMap mapping column names to their datetime types ("date", "time", "timestamp", etc.)
fn get_datetime_columns(table_name: &str, db: &DbHandler) -> std::collections::HashMap<String, String> {
    // Check cache first
    {
        let cache = DATETIME_COLUMNS_CACHE.read();
        if let Some(cached_columns) = cache.get(table_name) {
            return cached_columns.clone();
        }
    }
    
    // Cache miss - query the database
    let mut datetime_columns = std::collections::HashMap::new();
    
    if let Ok(conn) = db.get_mut_connection() {
        if let Ok(mut stmt) = conn.prepare("SELECT column_name, pg_type FROM __pgsqlite_schema WHERE table_name = ?1") {
            if let Ok(rows) = stmt.query_map([table_name], |row| {
                let col_name: String = row.get(0)?;
                let pg_type: String = row.get(1)?;
                Ok((col_name, pg_type))
            }) {
                for row in rows.flatten() {
                    let (col_name, pg_type) = row;
                    let pg_type_lower = pg_type.to_lowercase();
                    if pg_type_lower == "date" || pg_type_lower == "time" || pg_type_lower == "timetz" ||
                       pg_type_lower == "timestamp" || pg_type_lower == "timestamptz" ||
                       pg_type_lower == "time without time zone" || pg_type_lower == "time with time zone" ||
                       pg_type_lower == "timestamp without time zone" || pg_type_lower == "timestamp with time zone" {
                        datetime_columns.insert(col_name, pg_type_lower);
                    }
                }
            }
        }
    }
    
    // Cache the result
    {
        let mut cache = DATETIME_COLUMNS_CACHE.write();
        cache.insert(table_name.to_string(), datetime_columns.clone());
    }
    
    datetime_columns
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
        ("INSERT", n) => format!("INSERT 0 {}", n),
        (op, n) => format!("{} {}", op, n),
    }
}

pub struct QueryExecutor;

impl QueryExecutor {
    pub async fn execute_query<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        session: &Arc<SessionState>,
        query: &str,
        query_router: Option<&Arc<QueryRouter>>,
    ) -> Result<(), PgSqliteError> 
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        // Strip SQL comments first to avoid parsing issues
        let cleaned_query = crate::query::strip_sql_comments(query);
        let query_to_execute = cleaned_query.trim();
        
        // Check if query is empty after comment stripping
        if query_to_execute.is_empty() {
            return Err(PgSqliteError::Protocol("Empty query".to_string()));
        }
        
        info!("Executing query: {}", query_to_execute);
        
        // Check if query contains multiple statements
        let trimmed = query_to_execute.trim();
        if trimmed.contains(';') {
            // Split by semicolon and execute each statement
            let statements: Vec<&str> = trimmed.split(';')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();
            
            if statements.len() > 1 {
                info!("Query contains {} statements", statements.len());
                for (i, stmt) in statements.iter().enumerate() {
                    info!("Executing statement {}: {}", i + 1, stmt);
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
        db: &DbHandler,
        session: &Arc<SessionState>,
        query: &str,
        query_router: Option<&Arc<QueryRouter>>,
    ) -> Result<(), PgSqliteError> 
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        // Ultra-fast path: Skip all translation if query is simple enough
        if crate::query::simple_query_detector::is_ultra_simple_query(query) {
            debug!("Using ultra-fast path for query: {}", query);
            // Simple query routing without any processing
            match QueryTypeDetector::detect_query_type(query) {
                QueryType::Select => {
                    // Route query through query router if available and appropriate
                    let response = if let Some(router) = query_router {
                        router.execute_query(query, session).await.map_err(|e| PgSqliteError::Protocol(e.to_string()))?
                    } else {
                        db.query(query).await?
                    };
                    
                    // Get boolean and datetime columns for proper conversion (cached for performance)
                    let (boolean_columns, datetime_columns) = if let Some(table_name) = extract_table_name_from_select(query) {
                        (get_boolean_columns(&table_name, db), get_datetime_columns(&table_name, db))
                    } else {
                        (std::collections::HashSet::new(), std::collections::HashMap::new())
                    };
                    
                    // Send minimal row description with all TEXT types
                    let fields: Vec<FieldDescription> = response.columns.iter()
                        .enumerate()
                        .map(|(i, name)| FieldDescription {
                            name: name.clone(),
                            table_oid: 0,
                            column_id: (i + 1) as i16,
                            type_oid: PgType::Text.to_oid(), // Default to text for ultra-fast path
                            type_size: -1,
                            type_modifier: -1,
                            format: 0,
                        })
                        .collect();
                    
                    framed.send(BackendMessage::RowDescription(fields)).await
                        .map_err(|e| PgSqliteError::Io(e))?;
                    
                    // Send data rows with boolean and datetime conversion
                    for row in response.rows {
                        let converted_row: Vec<Option<Vec<u8>>> = row.into_iter()
                            .enumerate()
                            .map(|(col_idx, cell)| {
                                if let Some(data) = cell {
                                    // Convert based on column type
                                    if col_idx < response.columns.len() {
                                        let col_name = &response.columns[col_idx];
                                        
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
                                        else if let Some(dt_type) = datetime_columns.get(col_name) {
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
                                        } else {
                                            Some(data) // Keep original data for non-boolean/datetime columns
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
                            .map_err(|e| PgSqliteError::Io(e))?;
                    }
                    
                    // Send command complete
                    let tag = create_command_tag("SELECT", response.rows_affected);
                    framed.send(BackendMessage::CommandComplete { tag }).await
                        .map_err(|e| PgSqliteError::Io(e))?;
                    
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
        
        // Translate PostgreSQL cast syntax if present
        let mut translated_query = if crate::translator::CastTranslator::needs_translation(query) {
            if crate::profiling::is_profiling_enabled() {
                crate::time_cast_translation!({
                    use crate::translator::CastTranslator;
                    let conn = db.get_mut_connection()
                        .map_err(|e| PgSqliteError::Protocol(format!("Failed to get connection: {}", e)))?;
                    let translated = CastTranslator::translate_query(query, Some(&conn));
                    drop(conn); // Release the connection
                    translated
                })
            } else {
                use crate::translator::CastTranslator;
                let conn = db.get_mut_connection()
                    .map_err(|e| PgSqliteError::Protocol(format!("Failed to get connection: {}", e)))?;
                let translated = CastTranslator::translate_query(query, Some(&conn));
                drop(conn); // Release the connection
                translated
            }
        } else {
            query.to_string()
        };
        
        // Translate NUMERIC to TEXT casts with proper formatting
        if crate::translator::NumericFormatTranslator::needs_translation(&translated_query) {
            use crate::translator::NumericFormatTranslator;
            let conn = db.get_mut_connection()
                .map_err(|e| PgSqliteError::Protocol(format!("Failed to get connection: {}", e)))?;
            translated_query = NumericFormatTranslator::translate_query(&translated_query, &conn);
            drop(conn); // Release the connection
        }
        
        // Translate INSERT statements with datetime values if needed
        if crate::translator::InsertTranslator::needs_translation(&translated_query) {
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
        let mut translation_metadata = crate::translator::TranslationMetadata::new();
        if crate::translator::DateTimeTranslator::needs_translation(&translated_query) {
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
        if crate::translator::JsonTranslator::contains_json_operations(&translated_query) {
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
        
        // Translate array operators with metadata
        use crate::translator::ArrayTranslator;
        match ArrayTranslator::translate_with_metadata(&translated_query) {
            Ok((translated, metadata)) => {
                if translated != translated_query {
                    info!("Query after array operator translation: {}", translated);
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
        
        // Translate array_agg functions with ORDER BY/DISTINCT support
        use crate::translator::ArrayAggTranslator;
        match ArrayAggTranslator::translate_with_metadata(&translated_query) {
            Ok((translated, metadata)) => {
                if translated != translated_query {
                    info!("Query after array_agg translation: {}", translated);
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
        
        // Translate unnest() functions to json_each() equivalents
        use crate::translator::UnnestTranslator;
        match UnnestTranslator::translate_with_metadata(&translated_query) {
            Ok((translated, metadata)) => {
                if translated != translated_query {
                    info!("Query after unnest translation: {}", translated);
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
        
        // Translate json_each()/jsonb_each() functions for PostgreSQL compatibility
        use crate::translator::JsonEachTranslator;
        match JsonEachTranslator::translate_with_metadata(&translated_query) {
            Ok((translated, metadata)) => {
                if translated != translated_query {
                    info!("Query after json_each translation: {}", translated);
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
        
        // Translate row_to_json() functions for PostgreSQL compatibility
        use crate::translator::RowToJsonTranslator;
        let (translated, metadata) = RowToJsonTranslator::translate_row_to_json(&translated_query);
        if translated != translated_query {
            info!("Query after row_to_json translation: {}", translated);
            translated_query = translated;
        }
        debug!("RowToJson translation metadata: {} hints", metadata.column_mappings.len());
        translation_metadata.merge(metadata);
        
        // Analyze arithmetic expressions for type metadata
        if crate::translator::ArithmeticAnalyzer::needs_analysis(&translated_query) {
            let arithmetic_metadata = crate::translator::ArithmeticAnalyzer::analyze_query(&translated_query);
            translation_metadata.merge(arithmetic_metadata);
            debug!("Found {} type hints from translation", translation_metadata.column_mappings.len());
        }
        
        let query_to_execute = translated_query.as_str();
        
        // Simple query routing using optimized detection
        use crate::query::{QueryTypeDetector, QueryType};
        
        match QueryTypeDetector::detect_query_type(query_to_execute) {
            QueryType::Select => Self::execute_select(framed, db, session, query_to_execute, &translation_metadata, query_router).await,
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
                } else {
                    // Try to execute as-is
                    Self::execute_generic(framed, db, session, query_to_execute, query_router).await
                }
            }
        }
    }
    
    async fn execute_select<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        session: &Arc<SessionState>,
        query: &str,
        translation_metadata: &crate::translator::TranslationMetadata,
        query_router: Option<&Arc<QueryRouter>>,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        // Check if this is a catalog query first
        let response = if let Some(catalog_result) = CatalogInterceptor::intercept_query(query, Arc::new(db.clone())).await {
            catalog_result?
        } else {
            // Route query through query router if available
            if let Some(router) = query_router {
                router.execute_query(query, session).await.map_err(|e| PgSqliteError::Protocol(e.to_string()))?
            } else {
                db.query(query).await?
            }
        };
        
        // Extract table name from query to look up schema
        let table_name = extract_table_name_from_select(query);
        
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
            
            if let Some(ref table) = table_name {
                // Fetch types for actual columns
                for col_name in &response.columns {
                    if let Ok(Some(pg_type)) = db.get_schema_type(table, col_name).await {
                        schema_types.insert(col_name.clone(), pg_type);
                    }
                }
                
                // Fetch types for source columns referenced in translation hints
                for col_name in &response.columns {
                    if let Some(hint) = translation_metadata.get_hint(col_name) {
                        if let Some(ref source_col) = hint.source_column {
                            if let Ok(Some(source_type)) = db.get_schema_type(table, source_col).await {
                                hint_source_types.insert(col_name.clone(), source_type);
                            }
                        }
                    }
                }
            }
            
            // Build field descriptions with proper type inference
            let fields: Vec<FieldDescription> = response.columns.iter()
                .enumerate()
                .map(|(i, name)| {
                    // First priority: Check schema table for stored type mappings
                    let type_oid = if let Some(pg_type) = schema_types.get(name) {
                        // Need to check if this is an ENUM type
                        // Get a connection to check ENUM metadata
                        if let Ok(conn) = db.get_mut_connection() {
                            crate::types::SchemaTypeMapper::pg_type_string_to_oid_with_enum_check(pg_type, &conn)
                        } else {
                            crate::types::SchemaTypeMapper::pg_type_string_to_oid(pg_type)
                        }
                    } else if let Some(aggregate_oid) = crate::types::SchemaTypeMapper::get_aggregate_return_type_with_query(name, None, None, Some(query)) {
                        // Second priority: Check for aggregate functions
                        aggregate_oid
                    } else if let Some(hint) = translation_metadata.get_hint(name) {
                        // Third priority: Check translation metadata (datetime or arithmetic)
                        debug!("Found translation hint for column '{}': {:?}", name, hint.suggested_type);
                        
                        // Check if we pre-fetched the source type
                        if let Some(source_type) = hint_source_types.get(name) {
                            debug!("Found source column type for '{}' -> '{}': {}", name, hint.source_column.as_ref().unwrap_or(&"<none>".to_string()), source_type);
                            // For arithmetic on float columns, the result is float
                            if hint.expression_type == Some(crate::translator::ExpressionType::ArithmeticOnFloat) {
                                if source_type.contains("REAL") || source_type.contains("FLOAT") || source_type.contains("DOUBLE") {
                                    PgType::Float8.to_oid()
                                } else {
                                    // For other numeric types in arithmetic, still return float
                                    PgType::Float8.to_oid()
                                }
                            } else {
                                // For other expression types, use the source column type
                                crate::types::SchemaTypeMapper::pg_type_string_to_oid(source_type)
                            }
                        } else if let Some(suggested_type) = &hint.suggested_type {
                            // Fall back to suggested type if source lookup fails
                            suggested_type.to_oid()
                        } else {
                            PgType::Float8.to_oid() // Default for arithmetic
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
            .map_err(|e| PgSqliteError::Io(e))?;
        
        
        // Convert array data before sending rows
        debug!("Converting array data for {} rows", response.rows.len());
        info!("About to convert array data for {} rows", response.rows.len());
        let converted_rows = Self::convert_array_data_in_rows(response.rows, &fields)?;
        info!("Completed array data conversion");
        
        // Optimized data row sending for better SELECT performance
        if converted_rows.len() > 5 {
            // Use batch sending for larger result sets
            Self::send_data_rows_batched(framed, converted_rows).await?;
        } else {
            // Use individual sending for small result sets
            for row in converted_rows {
                framed.send(BackendMessage::DataRow(row)).await
                    .map_err(|e| PgSqliteError::Io(e))?;
            }
        }
        
        // Send CommandComplete with optimized tag creation
        let tag = create_command_tag("SELECT", response.rows_affected);
        framed.send(BackendMessage::CommandComplete { tag }).await
            .map_err(|e| PgSqliteError::Io(e))?;
        
        Ok(())
    }
    
    async fn execute_dml<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        session: &Arc<SessionState>,
        query: &str,
        query_router: Option<&Arc<QueryRouter>>,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        // Check for RETURNING clause
        if ReturningTranslator::has_returning_clause(query) {
            return Self::execute_dml_with_returning(framed, db, session, query, query_router).await;
        }
        
        // Validate numeric constraints for INSERT/UPDATE before execution
        use crate::query::{QueryTypeDetector, QueryType};
        use crate::validator::NumericValidator;
        
        // Validate before executing - do all database work before any await
        let validation_error = match QueryTypeDetector::detect_query_type(query) {
            QueryType::Insert => {
                if let Some(table_name) = extract_table_name_from_insert(query) {
                    // Get a connection to check constraints
                    let conn = db.get_mut_connection()
                        .map_err(|e| PgSqliteError::Protocol(format!("Failed to get connection: {}", e)))?;
                    
                    // Validate numeric constraints
                    let validation_result = NumericValidator::validate_insert(&conn, query, &table_name);
                    drop(conn); // Release connection before any await
                    
                    validation_result.err()
                } else {
                    None
                }
            }
            QueryType::Update => {
                if let Some(table_name) = extract_table_name_from_update(query) {
                    // Get a connection to check constraints
                    let conn = db.get_mut_connection()
                        .map_err(|e| PgSqliteError::Protocol(format!("Failed to get connection: {}", e)))?;
                    
                    // Validate numeric constraints
                    let validation_result = NumericValidator::validate_update(&conn, query, &table_name);
                    drop(conn); // Release connection before any await
                    
                    validation_result.err()
                } else {
                    None
                }
            }
            _ => None, // No validation needed for DELETE or other DML
        };
        
        // If there was a validation error, send it and return
        if let Some(e) = validation_error {
            let error_response = e.to_error_response();
            framed.send(BackendMessage::ErrorResponse(Box::new(error_response))).await
                .map_err(|e| PgSqliteError::Io(e))?;
            return Ok(());
        }
        
        // Route query through query router if available
        let response = if let Some(router) = query_router {
            router.execute_query(query, session).await.map_err(|e| PgSqliteError::Protocol(e.to_string()))?
        } else {
            db.execute(query).await?
        };
        
        // Optimized tag creation with static strings for common cases and buffer pooling for larger counts
        let tag = match QueryTypeDetector::detect_query_type(query) {
            QueryType::Insert => create_command_tag("INSERT", response.rows_affected),
            QueryType::Update => create_command_tag("UPDATE", response.rows_affected),
            QueryType::Delete => create_command_tag("DELETE", response.rows_affected),
            _ => create_command_tag("OK", response.rows_affected),
        };
        
        framed.send(BackendMessage::CommandComplete { tag }).await
            .map_err(|e| PgSqliteError::Io(e))?;
        
        Ok(())
    }
    
    async fn execute_dml_with_returning<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        session: &Arc<SessionState>,
        query: &str,
        query_router: Option<&Arc<QueryRouter>>,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        let (base_query, returning_clause) = ReturningTranslator::extract_returning_clause(query)
            .ok_or_else(|| PgSqliteError::Protocol("Failed to parse RETURNING clause".to_string()))?;
        
        use crate::query::{QueryTypeDetector, QueryType};
        
        if matches!(QueryTypeDetector::detect_query_type(&base_query), QueryType::Insert) {
            // For INSERT, execute the insert and then query by last_insert_rowid
            let table_name = ReturningTranslator::extract_table_from_insert(&base_query)
                .ok_or_else(|| PgSqliteError::Protocol("Failed to extract table name".to_string()))?;
            
            // Execute the INSERT
            let response = if let Some(router) = query_router {
                router.execute_query(&base_query, session).await.map_err(|e| PgSqliteError::Protocol(e.to_string()))?
            } else {
                db.execute(&base_query).await?
            };
            
            // Get the last inserted rowid and query for RETURNING data
            let returning_query = format!(
                "SELECT {} FROM {} WHERE rowid = last_insert_rowid()",
                returning_clause,
                table_name
            );
            
            let returning_response = if let Some(router) = query_router {
                router.execute_query(&returning_query, session).await.map_err(|e| PgSqliteError::Protocol(e.to_string()))?
            } else {
                db.query(&returning_query).await?
            };
            
            // Send row description
            let fields: Vec<FieldDescription> = returning_response.columns.iter()
                .enumerate()
                .map(|(i, name)| FieldDescription {
                    name: name.clone(),
                    table_oid: 0,
                    column_id: (i + 1) as i16,
                    type_oid: PgType::Text.to_oid(), // Default to text
                    type_size: -1,
                    type_modifier: -1,
                    format: 0,
                })
                .collect();
            
            framed.send(BackendMessage::RowDescription(fields)).await
                .map_err(|e| PgSqliteError::Io(e))?;
            
            // Send data rows
            for row in returning_response.rows {
                framed.send(BackendMessage::DataRow(row)).await
                    .map_err(|e| PgSqliteError::Io(e))?;
            }
            
            // Send command complete
            let tag = format!("INSERT 0 {}", response.rows_affected);
            framed.send(BackendMessage::CommandComplete { tag }).await
                .map_err(|e| PgSqliteError::Io(e))?;
        } else if matches!(QueryTypeDetector::detect_query_type(&base_query), QueryType::Update) {
            // For UPDATE, we need a different approach
            // SQLite doesn't support RETURNING natively, so we'll use a workaround
            let table_name = ReturningTranslator::extract_table_from_update(&base_query)
                .ok_or_else(|| PgSqliteError::Protocol("Failed to extract table name".to_string()))?;
            
            // First, get the rowids of rows that will be updated
            let where_clause = ReturningTranslator::extract_where_clause(&base_query);
            let rowid_query = format!(
                "SELECT rowid FROM {} {}",
                table_name,
                where_clause
            );
            let rowid_response = if let Some(router) = query_router {
                router.execute_query(&rowid_query, session).await.map_err(|e| PgSqliteError::Protocol(e.to_string()))?
            } else {
                db.query(&rowid_query).await?
            };
            let rowids: Vec<String> = rowid_response.rows.iter()
                .filter_map(|row| row[0].as_ref())
                .map(|bytes| String::from_utf8_lossy(bytes).to_string())
                .collect();
            
            // Execute the UPDATE
            let response = if let Some(router) = query_router {
                router.execute_query(&base_query, session).await.map_err(|e| PgSqliteError::Protocol(e.to_string()))?
            } else {
                db.execute(&base_query).await?
            };
            
            // Now query the updated rows
            if !rowids.is_empty() {
                let rowid_list = rowids.join(",");
                let returning_query = format!(
                    "SELECT {} FROM {} WHERE rowid IN ({})",
                    returning_clause,
                    table_name,
                    rowid_list
                );
                
                let returning_response = if let Some(router) = query_router {
                    router.execute_query(&returning_query, session).await.map_err(|e| PgSqliteError::Protocol(e.to_string()))?
                } else {
                    db.query(&returning_query).await?
                };
                
                // Send row description
                let fields: Vec<FieldDescription> = returning_response.columns.iter()
                    .enumerate()
                    .map(|(i, name)| FieldDescription {
                        name: name.clone(),
                        table_oid: 0,
                        column_id: (i + 1) as i16,
                        type_oid: PgType::Text.to_oid(),
                        type_size: -1,
                        type_modifier: -1,
                        format: 0,
                    })
                    .collect();
                
                framed.send(BackendMessage::RowDescription(fields)).await
                    .map_err(|e| PgSqliteError::Io(e))?;
                
                // Send data rows
                for row in returning_response.rows {
                    framed.send(BackendMessage::DataRow(row)).await
                        .map_err(|e| PgSqliteError::Io(e))?;
                }
            }
            
            // Send command complete
            let tag = format!("UPDATE {}", response.rows_affected);
            framed.send(BackendMessage::CommandComplete { tag }).await
                .map_err(|e| PgSqliteError::Io(e))?;
        } else if matches!(QueryTypeDetector::detect_query_type(&base_query), QueryType::Delete) {
            // For DELETE, capture rows before deletion
            let table_name = ReturningTranslator::extract_table_from_delete(&base_query)
                .ok_or_else(|| PgSqliteError::Protocol("Failed to extract table name".to_string()))?;
            
            let capture_query = ReturningTranslator::generate_capture_query(
                &base_query,
                &table_name,
                &returning_clause
            )?;
            
            // Capture the rows that will be affected
            let captured_rows = if let Some(router) = query_router {
                router.execute_query(&capture_query, session).await.map_err(|e| PgSqliteError::Protocol(e.to_string()))?
            } else {
                db.query(&capture_query).await?
            };
            
            // Execute the actual DELETE
            let response = if let Some(router) = query_router {
                router.execute_query(&base_query, session).await.map_err(|e| PgSqliteError::Protocol(e.to_string()))?
            } else {
                db.execute(&base_query).await?
            };
            
            // Send row description
            let fields: Vec<FieldDescription> = captured_rows.columns.iter()
                .skip(1) // Skip rowid column
                .enumerate()
                .map(|(i, name)| FieldDescription {
                    name: name.clone(),
                    table_oid: 0,
                    column_id: (i + 1) as i16,
                    type_oid: PgType::Text.to_oid(),
                    type_size: -1,
                    type_modifier: -1,
                    format: 0,
                })
                .collect();
            
            framed.send(BackendMessage::RowDescription(fields)).await
                .map_err(|e| PgSqliteError::Io(e))?;
            
            // Send captured rows (skip rowid column)
            for row in captured_rows.rows {
                let data_row: Vec<Option<Vec<u8>>> = row.into_iter()
                    .skip(1) // Skip rowid
                    .collect();
                framed.send(BackendMessage::DataRow(data_row)).await
                    .map_err(|e| PgSqliteError::Io(e))?;
            }
            
            // Send command complete
            let tag = format!("DELETE {}", response.rows_affected);
            framed.send(BackendMessage::CommandComplete { tag }).await
                .map_err(|e| PgSqliteError::Io(e))?;
        }
        
        Ok(())
    }
    
    async fn execute_ddl<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        _session: &Arc<SessionState>,
        query: &str,
        _query_router: Option<&Arc<QueryRouter>>,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        use crate::translator::CreateTableTranslator;
        use crate::query::{QueryTypeDetector, QueryType};
        use crate::ddl::EnumDdlHandler;
        
        // Check if this is an ENUM DDL statement
        if EnumDdlHandler::is_enum_ddl(query) {
            // Handle the ENUM DDL in a scope to ensure the mutex guard is dropped
            let command_tag = {
                let mut conn = db.get_mut_connection()
                    .map_err(|e| PgSqliteError::Protocol(format!("Failed to get connection: {}", e)))?;
                
                // Handle the ENUM DDL
                EnumDdlHandler::handle_enum_ddl(&mut conn, query)?;
                
                // Determine command tag
                if query.trim().to_uppercase().starts_with("CREATE TYPE") {
                    "CREATE TYPE"
                } else if query.trim().to_uppercase().starts_with("ALTER TYPE") {
                    "ALTER TYPE"
                } else if query.trim().to_uppercase().starts_with("DROP TYPE") {
                    "DROP TYPE"
                } else {
                    "OK"
                }
            }; // Mutex guard is dropped here
            
            // Send command complete
            framed.send(BackendMessage::CommandComplete { 
                tag: command_tag.to_string() 
            }).await
                .map_err(|e| PgSqliteError::Io(e))?;
            
            return Ok(());
        }
        
        let (translated_query, type_mappings, enum_columns, array_columns) = if matches!(QueryTypeDetector::detect_query_type(query), QueryType::Create) && query.trim_start()[6..].trim_start().to_uppercase().starts_with("TABLE") {
            // Use CREATE TABLE translator with connection for ENUM support
            let conn = db.get_mut_connection()
                .map_err(|e| PgSqliteError::Protocol(format!("Failed to get connection: {}", e)))?;
            
            let result = CreateTableTranslator::translate_with_connection_full(query, Some(&conn))
                .map_err(|e| PgSqliteError::Protocol(format!("CREATE TABLE translation failed: {}", e)))?;
            
            // Connection guard is dropped here
            (result.sql, result.type_mappings, result.enum_columns, result.array_columns)
        } else {
            // For other DDL, check for JSON/JSONB types
            let translated = if query.to_lowercase().contains("json") || query.to_lowercase().contains("jsonb") {
                JsonTranslator::translate_statement(query)?
            } else {
                query.to_string()
            };
            (translated, std::collections::HashMap::new(), Vec::new(), Vec::new())
        };
        
        // Execute the translated query
        db.execute(&translated_query).await?;
        
        // If we have type mappings, store them in the metadata table
        info!("Type mappings count: {}", type_mappings.len());
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
                
                match db.execute(init_query).await {
                    Ok(_) => info!("Successfully created/verified __pgsqlite_schema table"),
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
                        
                        match db.execute(&insert_query).await {
                            Ok(_) => info!("Stored metadata: {}.{} -> {} ({})", table_name, parts[1], type_mapping.pg_type, type_mapping.sqlite_type),
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
                            
                            if pg_type_lower == "varchar" || pg_type_lower == "char" || 
                               pg_type_lower == "character varying" || pg_type_lower == "character" ||
                               pg_type_lower == "nvarchar" {
                                let is_char = pg_type_lower == "char" || pg_type_lower == "character";
                                let constraint_query = format!(
                                    "INSERT OR REPLACE INTO __pgsqlite_string_constraints (table_name, column_name, max_length, is_char_type) 
                                     VALUES ('{}', '{}', {}, {})",
                                    table_name, parts[1], modifier, if is_char { 1 } else { 0 }
                                );
                                
                                match db.execute(&constraint_query).await {
                                    Ok(_) => info!("Stored string constraint: {}.{} max_length={}", table_name, parts[1], modifier),
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
                                
                                match db.execute(&constraint_query).await {
                                    Ok(_) => {
                                        info!("Stored numeric constraint: {}.{} precision={} scale={}", table_name, parts[1], precision, scale);
                                    }
                                    Err(e) => {
                                        debug!("Failed to store numeric constraint for {}.{}: {}", table_name, parts[1], e);
                                    }
                                }
                            }
                        }
                    }
                }
                
                info!("Stored type mappings for table {} (simple query protocol)", table_name);
                
                // Create triggers for ENUM columns
                if !enum_columns.is_empty() {
                    let conn = db.get_mut_connection()
                        .map_err(|e| PgSqliteError::Protocol(format!("Failed to get connection for triggers: {}", e)))?;
                    
                    for (column_name, enum_type) in &enum_columns {
                        // Record enum usage
                        EnumTriggers::record_enum_usage(&conn, &table_name, column_name, enum_type)
                            .map_err(|e| PgSqliteError::Protocol(format!("Failed to record enum usage: {}", e)))?;
                        
                        // Create validation triggers
                        EnumTriggers::create_enum_validation_triggers(&conn, &table_name, column_name, enum_type)
                            .map_err(|e| PgSqliteError::Protocol(format!("Failed to create enum triggers: {}", e)))?;
                        
                        info!("Created ENUM validation triggers for {}.{} (type: {})", table_name, column_name, enum_type);
                    }
                }
                
                // Store array column metadata
                if !array_columns.is_empty() {
                    let conn = db.get_mut_connection()
                        .map_err(|e| PgSqliteError::Protocol(format!("Failed to get connection for array metadata: {}", e)))?;
                    
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
                    ).map_err(|e| PgSqliteError::Protocol(format!("Failed to create array metadata table: {}", e)))?;
                    
                    // Insert array column metadata
                    for (column_name, element_type, dimensions) in &array_columns {
                        conn.execute(
                            "INSERT OR REPLACE INTO __pgsqlite_array_types (table_name, column_name, element_type, dimensions) 
                             VALUES (?1, ?2, ?3, ?4)",
                            params![table_name, column_name, element_type, dimensions]
                        ).map_err(|e| PgSqliteError::Protocol(format!("Failed to store array metadata: {}", e)))?;
                        
                        info!("Stored array column metadata for {}.{} (element_type: {}, dimensions: {})", 
                              table_name, column_name, element_type, dimensions);
                    }
                }
                
                // Numeric validation is now handled at the application layer in execute_dml
                // No need for triggers anymore
                
                // Datetime conversion is now handled by InsertTranslator and value converters
                // No need for triggers anymore
                
                // Populate PostgreSQL catalog tables with constraint information
                if let Some(table_name) = extract_table_name_from_create(query) {
                    let conn = db.get_mut_connection()
                        .map_err(|e| PgSqliteError::Protocol(format!("Failed to get connection for constraint population: {}", e)))?;
                    
                    // Populate pg_constraint, pg_attrdef, and pg_index tables
                    if let Err(e) = crate::catalog::constraint_populator::populate_constraints_for_table(&conn, &table_name) {
                        // Log the error but don't fail the CREATE TABLE operation
                        debug!("Failed to populate constraints for table {}: {}", table_name, e);
                    } else {
                        info!("Successfully populated constraint catalog tables for table: {}", table_name);
                    }
                }
            }
        }
        
        let tag = match QueryTypeDetector::detect_query_type(query) {
            QueryType::Create => {
                let after_create = query.trim_start()[6..].trim_start();
                if after_create.to_uppercase().starts_with("TABLE") {
                    "CREATE TABLE".to_string()
                } else if after_create.to_uppercase().starts_with("INDEX") {
                    "CREATE INDEX".to_string()
                } else {
                    "CREATE".to_string()
                }
            }
            QueryType::Drop => {
                let after_drop = query.trim_start()[4..].trim_start();
                if after_drop.to_uppercase().starts_with("TABLE") {
                    "DROP TABLE".to_string()
                } else {
                    "DROP".to_string()
                }
            }
            _ => "OK".to_string(),
        };
        
        framed.send(BackendMessage::CommandComplete { tag }).await
            .map_err(|e| PgSqliteError::Io(e))?;
        
        Ok(())
    }
    
    async fn execute_transaction<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        _session: &Arc<SessionState>,
        query: &str,
        _query_router: Option<&Arc<QueryRouter>>,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        use crate::query::{QueryTypeDetector, QueryType};
        match QueryTypeDetector::detect_query_type(query) {
            QueryType::Begin => {
                db.execute("BEGIN").await?;
                framed.send(BackendMessage::CommandComplete { tag: "BEGIN".to_string() }).await
                    .map_err(|e| PgSqliteError::Io(e))?;
            }
            QueryType::Commit => {
                db.execute("COMMIT").await?;
                framed.send(BackendMessage::CommandComplete { tag: "COMMIT".to_string() }).await
                    .map_err(|e| PgSqliteError::Io(e))?;
            }
            QueryType::Rollback => {
                db.execute("ROLLBACK").await?;
                framed.send(BackendMessage::CommandComplete { tag: "ROLLBACK".to_string() }).await
                    .map_err(|e| PgSqliteError::Io(e))?;
            }
            _ => {}
        }
        
        Ok(())
    }
    
    async fn execute_generic<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
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
            db.execute(query).await?;
        }
        
        framed.send(BackendMessage::CommandComplete { tag: "OK".to_string() }).await
            .map_err(|e| PgSqliteError::Io(e))?;
        
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
                    .map_err(|e| PgSqliteError::Io(e))?;
            }
        } else {
            // Send in batches with periodic flushing
            let mut row_iter = rows.into_iter();
            loop {
                let mut batch_sent = false;
                for _ in 0..batch_size {
                    if let Some(row) = row_iter.next() {
                        framed.send(BackendMessage::DataRow(row)).await
                            .map_err(|e| PgSqliteError::Io(e))?;
                        batch_sent = true;
                    } else {
                        break;
                    }
                }
                if !batch_sent {
                    break;
                }
                // Flush after each batch to ensure timely delivery
                framed.flush().await.map_err(|e| PgSqliteError::Io(e))?;
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
        info!("Type OIDs for conversion: {:?}", type_oids);
        info!("Boolean type OID: {}", PgType::Bool.to_oid());
        
        // Convert each row
        let mut converted_rows = Vec::with_capacity(rows.len());
        
        for row in rows {
            let mut converted_row = Vec::with_capacity(row.len());
            
            for (col_idx, cell) in row.into_iter().enumerate() {
                let converted_cell = if let Some(data) = cell {
                    let type_oid = type_oids.get(col_idx).copied().unwrap_or(25); // Default to TEXT
                    
                    // Check if this is an array type that needs conversion
                    if PgType::from_oid(type_oid).map_or(false, |t| t.is_array()) {
                        // Try to convert JSON array to PostgreSQL array format
                        match Self::convert_json_to_pg_array(&data) {
                            Ok(converted_data) => Some(converted_data),
                            Err(_) => Some(data), // Keep original data if conversion fails
                        }
                    } else if type_oid == PgType::Bool.to_oid() {
                        // Convert boolean values from integer 0/1 to PostgreSQL f/t format
                        info!("Converting boolean data for column {}: {:?}", col_idx, std::str::from_utf8(&data));
                        match std::str::from_utf8(&data) {
                            Ok(s) => match s.trim() {
                                "0" => {
                                    info!("Converted '0' to 'f'");
                                    Some(b"f".to_vec())
                                },
                                "1" => {
                                    info!("Converted '1' to 't'");
                                    Some(b"t".to_vec())
                                },
                                _ => Some(data), // Keep original data if not 0/1
                            },
                            Err(_) => Some(data), // Keep original data if not valid UTF-8
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
                    format!("\"{}\"", escaped)
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
        assert!(PgType::from_oid(text_array_type).map_or(false, |t| t.is_array()));
        
        // Test that regular text is not detected as an array
        let text_type = PgType::Text.to_oid();
        assert_eq!(text_type, 25);
        assert!(!PgType::from_oid(text_type).map_or(false, |t| t.is_array()));
        
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

fn extract_table_name_from_select(query: &str) -> Option<String> {
    // Look for FROM clause with case-insensitive search
    let from_pos = query.as_bytes().windows(6)
        .position(|window| window.eq_ignore_ascii_case(b" from "))?;
    
    let after_from = &query[from_pos + 6..].trim();
    
    // Find the end of table name (space, where, order by, etc.)
    let table_end = after_from.find(|c: char| {
        c.is_whitespace() || c == ',' || c == ';' || c == '('
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

/// Extract table name from CREATE TABLE statement
fn extract_table_name_from_create(query: &str) -> Option<String> {
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