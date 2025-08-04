use crate::protocol::{BackendMessage, FieldDescription, BinaryResultEncoder, BinaryEncoder};
use crate::session::{DbHandler, SessionState, PreparedStatement, Portal, GLOBAL_QUERY_CACHE};
use crate::catalog::CatalogInterceptor;
use crate::translator::{JsonTranslator, ReturningTranslator};
use crate::types::{DecimalHandler, PgType};
use crate::cache::{RowDescriptionKey, GLOBAL_ROW_DESCRIPTION_CACHE, GLOBAL_PARAMETER_CACHE, CachedParameterInfo, PreparedStatementCache};
use once_cell::sync::Lazy;
use crate::validator::NumericValidator;
use crate::query::ParameterParser;
use crate::PgSqliteError;
use tokio_util::codec::Framed;
use futures::SinkExt;
use tracing::{debug, info, warn, error};
use std::sync::Arc;
use std::str::FromStr;
use chrono::{NaiveDate, NaiveTime, NaiveDateTime, Timelike};

/// Efficient case-insensitive query type detection
#[inline]
fn query_starts_with_ignore_case(query: &str, prefix: &str) -> bool {
    let query_trimmed = query.trim();
    let query_bytes = query_trimmed.as_bytes();
    let prefix_bytes = prefix.as_bytes();
    
    if query_bytes.len() < prefix_bytes.len() {
        return false;
    }
    
    // Fast byte comparison for common cases
    match prefix {
        "INSERT" => matches!(&query_bytes[0..6], b"INSERT" | b"insert" | b"Insert"),
        "SELECT" => matches!(&query_bytes[0..6], b"SELECT" | b"select" | b"Select"),
        "UPDATE" => matches!(&query_bytes[0..6], b"UPDATE" | b"update" | b"Update"),
        "DELETE" => matches!(&query_bytes[0..6], b"DELETE" | b"delete" | b"Delete"),
        _ => query_trimmed[..prefix.len()].eq_ignore_ascii_case(prefix),
    }
}

/// Find position of a keyword in query text (case-insensitive)
#[inline]
fn find_keyword_position(query: &str, keyword: &str) -> Option<usize> {
    // For small keywords, do simple case-insensitive search
    let query_bytes = query.as_bytes();
    let keyword_bytes = keyword.as_bytes();
    
    if keyword_bytes.is_empty() || query_bytes.len() < keyword_bytes.len() {
        return None;
    }
    
    // Sliding window search
    for i in 0..=(query_bytes.len() - keyword_bytes.len()) {
        let window = &query_bytes[i..i + keyword_bytes.len()];
        if window.eq_ignore_ascii_case(keyword_bytes) {
            return Some(i);
        }
    }
    
    None
}

// Global prepared statement cache to avoid re-parsing identical queries
pub static GLOBAL_PREPARED_STATEMENT_CACHE: Lazy<Arc<PreparedStatementCache>> = Lazy::new(|| {
    Arc::new(PreparedStatementCache::new(1000, 300)) // 1000 statements, 5 minute TTL
});

pub struct ExtendedQueryHandler;

impl ExtendedQueryHandler {
    /// Get cached connection or fetch and cache it
    async fn get_or_cache_connection(
        session: &Arc<SessionState>,
        db: &Arc<DbHandler>
    ) -> Option<Arc<parking_lot::Mutex<rusqlite::Connection>>> {
        // First check if we have a cached connection
        if let Some(cached) = session.get_cached_connection() {
            return Some(cached);
        }
        
        // Try to get connection from manager and cache it
        if let Some(conn_arc) = db.connection_manager().get_connection_arc(&session.id) {
            session.cache_connection(conn_arc.clone());
            Some(conn_arc)
        } else {
            None
        }
    }
    pub async fn handle_parse<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &Arc<DbHandler>,
        session: &Arc<SessionState>,
        name: String,
        query: String,
        param_types: Vec<i32>,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // Fast path: Check if we already have this prepared statement
        // This avoids re-parsing the same query multiple times
        if !name.is_empty() {
            let statements = session.prepared_statements.read().await;
            if let Some(existing) = statements.get(&name) {
                // Check if it's the same query
                if existing.query == query && existing.param_types == param_types {
                    // Already parsed, just send ParseComplete
                    drop(statements);
                    framed.send(BackendMessage::ParseComplete).await
                        .map_err(PgSqliteError::Io)?;
                    return Ok(());
                }
            }
        } else {
            // For unnamed statements, check if we have cached info about this query
            // This is important for benchmarks that use parameterized queries
            if let Some(cached_info) = GLOBAL_PARAMETER_CACHE.get(&query) {
                // We already know about this query, create a fast prepared statement
                let stmt = PreparedStatement {
                    query: query.clone(),
                    translated_query: cached_info.translated_query.clone(), // Use cached translated query
                    param_types: cached_info.param_types.clone(),
                    client_param_types: cached_info.original_types.clone(), // Use cached original types
                    param_formats: vec![0; cached_info.param_types.len()],
                    field_descriptions: Vec::new(), // Will be populated during bind/execute
                    translation_metadata: None,
                };
                
                // Store as unnamed statement (replaces any existing unnamed statement)
                info!("Storing unnamed statement, replacing any existing one");
                session.prepared_statements.write().await.insert(String::new(), stmt);
                
                framed.send(BackendMessage::ParseComplete).await
                    .map_err(PgSqliteError::Io)?;
                return Ok(());
            }
        }
        
        // Strip SQL comments first to avoid parsing issues
        let mut cleaned_query = crate::query::strip_sql_comments(&query);
        
        // Check if query is empty after comment stripping
        if cleaned_query.trim().is_empty() {
            return Err(PgSqliteError::Protocol("Empty query".to_string()));
        }
        
        // debug!("Parsing statement '{}': {}", name, cleaned_query);
        // debug!("Provided param_types: {:?}", param_types);
        
        // Check for Python-style parameters and convert to PostgreSQL-style
        use crate::query::parameter_parser::ParameterParser;
        let python_params = ParameterParser::find_python_parameters(&cleaned_query);
        if !python_params.is_empty() {
        // debug!("Found Python-style parameters: {:?}", python_params);
            
            // Convert %(name)s parameters to $1, $2, $3, etc.
            let mut param_counter = 1;
            for param_name in &python_params {
                let placeholder = format!("%({param_name})s");
                let numbered_placeholder = format!("${param_counter}");
                cleaned_query = cleaned_query.replace(&placeholder, &numbered_placeholder);
                param_counter += 1;
            }
            
        // debug!("Converted query: {}", cleaned_query);
            
            // Store the parameter mapping in session for later use in bind
            let mut python_param_mapping = session.python_param_mapping.write().await;
            python_param_mapping.insert(name.clone(), python_params);
        }
        
        // For prepared statement cache, we need to handle explicit client types
        // Build the cache key with merged types (client explicit + analyzed for unknowns)
        let cache_key_types = if param_types.iter().any(|&t| t != 0) {
            // Has some explicit types - don't use cache yet, will merge after analysis
            vec![]
        } else {
            // All unknown or empty - can use as-is
            param_types.clone()
        };
        
        // Only check cache if we have no explicit types (all 0 or empty)
        if !cache_key_types.is_empty() || param_types.is_empty() {
            if let Some(cached_stmt) = GLOBAL_PREPARED_STATEMENT_CACHE.get(&cleaned_query, &cache_key_types) {
                // Use the cached prepared statement
                session.prepared_statements.write().await.insert(name.clone(), cached_stmt);
                
                framed.send(BackendMessage::ParseComplete).await
                    .map_err(PgSqliteError::Io)?;
                return Ok(());
            }
        }
        
        // Check if this is a SET command - handle it specially
        if crate::query::SetHandler::is_set_command(&cleaned_query) {
            // For SET commands, we need to create a special prepared statement
            // that will be handled during execution
            let stmt = PreparedStatement {
                query: cleaned_query.clone(),
                translated_query: None,
                param_types: vec![], // SET commands don't have parameters
                client_param_types: vec![], // SET commands don't have parameters
                param_formats: vec![],
                field_descriptions: if cleaned_query.trim().to_uppercase().starts_with("SHOW") {
                    // SHOW commands return one column
                    vec![FieldDescription {
                        name: "setting".to_string(),
                        table_oid: 0,
                        column_id: 1,
                        type_oid: PgType::Text.to_oid(),
                        type_size: -1,
                        type_modifier: -1,
                        format: 0,
                    }]
                } else {
                    vec![]
                },
                translation_metadata: None, // SET commands don't need translation metadata
            };
            
            // Cache SET/SHOW commands as well
            GLOBAL_PREPARED_STATEMENT_CACHE.insert(&cleaned_query, &vec![], stmt.clone());
            
            session.prepared_statements.write().await.insert(name.clone(), stmt);
            
            // Send ParseComplete
            framed.send(BackendMessage::ParseComplete).await
                .map_err(PgSqliteError::Io)?;
            
            return Ok(());
        }
        
        // Check if this is a simple parameter SELECT (e.g., SELECT $1, $2)
        let is_simple_param_select = query_starts_with_ignore_case(&query, "SELECT") && 
            !query.to_uppercase().contains("FROM") && 
            query.contains('$');
        
        // For INSERT and SELECT queries, we need to determine parameter types from the target table schema
        let mut actual_param_types = param_types.clone();
        // Save the original client param types before we potentially override them with schema types
        let original_client_param_types = param_types.clone();
        info!("Parse received param_types: {:?}", param_types);
        
        // Only analyze INSERT/UPDATE/SELECT queries if we have unknown parameter types (0)
        // If client provides explicit types (like INT2=21, FLOAT8=701), we should respect them
        let has_unknown_types = param_types.is_empty() || param_types.iter().any(|&t| t == 0);
        let needs_schema_analysis = cleaned_query.contains('$') && has_unknown_types &&
            (query_starts_with_ignore_case(&query, "INSERT") || 
             query_starts_with_ignore_case(&query, "UPDATE") ||
             query_starts_with_ignore_case(&query, "SELECT"));
        
        info!("has_unknown_types: {}, needs_schema_analysis: {}", has_unknown_types, needs_schema_analysis);
        
        if (param_types.is_empty() && cleaned_query.contains('$')) || needs_schema_analysis {
            // First check parameter cache
            if let Some(cached_info) = GLOBAL_PARAMETER_CACHE.get(&query) {
                actual_param_types = cached_info.param_types;
                info!("Using cached parameter types for query: {:?}", actual_param_types);
            } else {
                // Check if we have this query cached in query cache
                if let Some(cached) = GLOBAL_QUERY_CACHE.get(&query) {
                    actual_param_types = cached.param_types.clone();
                    info!("Using cached parameter types from query cache: {:?}", actual_param_types);
                    
                    // Also cache in parameter cache for faster access
                    GLOBAL_PARAMETER_CACHE.insert(query.clone(), CachedParameterInfo {
                        param_types: actual_param_types.clone(),
                        original_types: actual_param_types.clone(), // Use same types since we don't have original info here
                        table_name: cached.table_names.first().cloned(),
                        column_names: Vec::new(), // Will be populated later if needed
                        translated_query: None, // Will be set later if translation is needed
                        created_at: std::time::Instant::now(),
                    });
                } else {
                    // Need to analyze the query
                    let (analyzed_types, _original_types_opt, table_name, column_names) = if query_starts_with_ignore_case(&query, "INSERT") {
                        match Self::analyze_insert_params(&query, db, session).await {
                            Ok((types, orig_types)) => {
                                info!("Analyzed INSERT parameter types: {:?} (original: {:?})", types, orig_types);
                                
                                // Extract table and columns for caching
                                let (table, cols) = crate::types::QueryContextAnalyzer::get_insert_column_info(&query)
                                    .unwrap_or_else(|| (String::new(), Vec::new()));
                                
                                (types, Some(orig_types), Some(table), cols)
                            }
                            Err(_) => {
                                // If we can't determine types, default to text
                                let param_count = ParameterParser::count_parameters(&query);
                                let types = vec![PgType::Text.to_oid(); param_count];
                                (types.clone(), Some(types), None, Vec::new())
                            }
                        }
                    } else if query_starts_with_ignore_case(&query, "SELECT") {
                        let types = Self::analyze_select_params(&query, db, session).await.unwrap_or_else(|_| {
                            // If we can't determine types, default to text
                            let param_count = ParameterParser::count_parameters(&query);
                            vec![PgType::Text.to_oid(); param_count]
                        });
                        info!("Analyzed SELECT parameter types: {:?}", types);
                        
                        let table = extract_table_name_from_select(&query);
                        (types.clone(), Some(types), table, Vec::new())
                    } else {
                        // Other query types - just count parameters
                        let param_count = ParameterParser::count_parameters(&query);
                        let types = vec![PgType::Text.to_oid(); param_count];
                        (types.clone(), Some(types), None, Vec::new())
                    };
                    
                    // Merge analyzed types with client types - only override unknown (0) types
                    // If param_types is empty, use all analyzed types
                    actual_param_types = if param_types.is_empty() {
                        analyzed_types.clone()
                    } else {
                        param_types.iter().enumerate().map(|(i, &client_type)| {
                            if client_type == 0 {
                                // Unknown type - use analyzed type
                                analyzed_types.get(i).copied().unwrap_or(PgType::Text.to_oid())
                            } else {
                                // Client provided explicit type - respect it
                                client_type
                            }
                        }).collect()
                    };
                    
                    info!("Merged parameter types: client {:?} + analyzed {:?} = actual {:?}", 
                          param_types, analyzed_types, actual_param_types);
                    
                    // Cache the parameter info with the merged types (translated query will be added later)
                    GLOBAL_PARAMETER_CACHE.insert(query.clone(), CachedParameterInfo {
                        param_types: actual_param_types.clone(),
                        original_types: param_types.clone(), // Store the original client types
                        table_name,
                        column_names,
                        translated_query: None, // Will be set after translation
                        created_at: std::time::Instant::now(),
                    });
                    
                    // Also update query cache if it's a parseable query (keep JSON path placeholders for now)
                    if let Ok(parsed) = sqlparser::parser::Parser::parse_sql(
                        &sqlparser::dialect::PostgreSqlDialect {},
                        &cleaned_query
                    ) {
                        if let Some(statement) = parsed.first() {
                            let table_names = Self::extract_table_names_from_statement(statement);
                            GLOBAL_QUERY_CACHE.insert(cleaned_query.clone(), crate::cache::CachedQuery {
                                statement: statement.clone(),
                                param_types: actual_param_types.clone(),
                                is_decimal_query: false, // Will be determined later
                                table_names,
                                column_types: Vec::new(), // Will be filled when query is executed
                                has_decimal_columns: false,
                                rewritten_query: None,
                                normalized_query: crate::cache::QueryCache::normalize_query(&cleaned_query),
                            });
                        }
                    }
                }
            }
        }
        
        // Initialize translation_metadata early
        let mut translation_metadata = crate::translator::TranslationMetadata::new();
        
        // Pre-translate the query first so we can analyze the translated version
        #[cfg(feature = "unified_processor")]
        let mut translated_for_analysis = {
            // Use unified processor for translation - it handles ALL translations
            db.with_session_connection(&session.id, |conn| {
                crate::query::process_query(&cleaned_query, conn, db.get_schema_cache())
            }).await?
        };
        
        // Translate array operators FIRST (before CastTranslator)
        // This is important because ArrayTranslator needs to see ANY(ARRAY[...]) patterns
        // before CastTranslator modifies the parameter casts
        #[cfg(not(feature = "unified_processor"))]
        let mut translated_for_analysis = {
            use crate::translator::ArrayTranslator;
            info!("Translating array operators for query: {}", cleaned_query);
            match ArrayTranslator::translate_with_metadata(&cleaned_query) {
                Ok((translated, metadata)) => {
                    if translated != cleaned_query {
                        info!("Array translation changed query to: {}", translated);
                    }
                    info!("Array metadata has {} hints", metadata.column_mappings.len());
                    translation_metadata.merge(metadata);
                    translated
                }
                Err(_) => {
                    // Continue with original query
                    cleaned_query.clone()
                }
            }
        };
        
        // Now translate casts (after array translation)
        #[cfg(not(feature = "unified_processor"))]
        if crate::translator::CastTranslator::needs_translation(&translated_for_analysis) {
            info!("Parse: Query needs cast translation: {}", translated_for_analysis);
            let translated = db.with_session_connection(&session.id, |conn| {
                Ok(crate::translator::CastTranslator::translate_query(&translated_for_analysis, Some(conn)))
            }).await?;
            info!("Parse: Cast translated to: {}", translated);
            translated_for_analysis = translated;
        }
        
        // Translate NUMERIC to TEXT casts with proper formatting
        #[cfg(not(feature = "unified_processor"))] // Skip when using unified processor
        if crate::translator::NumericFormatTranslator::needs_translation(&translated_for_analysis) {
            translated_for_analysis = db.with_session_connection(&session.id, |conn| {
                Ok(crate::translator::NumericFormatTranslator::translate_query(&translated_for_analysis, conn))
            }).await?;
        }
        
        // Translate datetime functions if needed and capture metadata
        #[cfg(not(feature = "unified_processor"))] // Skip when using unified processor
        if crate::translator::DateTimeTranslator::needs_translation(&translated_for_analysis) {
            let (translated, metadata) = crate::translator::DateTimeTranslator::translate_with_metadata(&translated_for_analysis);
            translated_for_analysis = translated;
            translation_metadata.merge(metadata);
        }
        
        // Remove schema prefixes (e.g., pg_catalog.) from tables and functions
        #[cfg(not(feature = "unified_processor"))] // Skip when using unified processor
        {
            use crate::translator::SchemaPrefixTranslator;
            let translated = SchemaPrefixTranslator::translate_query(&translated_for_analysis);
            if translated != translated_for_analysis {
                info!("Schema prefix translation changed query to: {}", translated);
                translated_for_analysis = translated;
            }
        }
        
        // Translate json_each()/jsonb_each() functions for PostgreSQL compatibility
        #[cfg(not(feature = "unified_processor"))] // Skip when using unified processor
        {
            use crate::translator::JsonEachTranslator;
        match JsonEachTranslator::translate_with_metadata(&translated_for_analysis) {
            Ok((translated, metadata)) => {
                if translated != translated_for_analysis {
                    info!("JSON each translation changed query from: {}", translated_for_analysis);
                    info!("JSON each translation changed query to: {}", translated);
                    translated_for_analysis = translated;
                }
        // debug!("JSON each metadata hints: {:?}", metadata);
                translation_metadata.merge(metadata);
            }
            Err(_e) => {
        // debug!("JSON each translation failed: {:?}", e);
                // Continue with original query
            }
        }
        }
        
        // Translate row_to_json() functions for PostgreSQL compatibility
        #[cfg(not(feature = "unified_processor"))] // Skip when using unified processor
        {
            use crate::translator::RowToJsonTranslator;
        let (translated, metadata) = RowToJsonTranslator::translate_row_to_json(&translated_for_analysis);
        if translated != translated_for_analysis {
            info!("row_to_json translation changed query from: {}", translated_for_analysis);
            info!("row_to_json translation changed query to: {}", translated);
            translated_for_analysis = translated;
        }
        // debug!("row_to_json metadata hints: {:?}", metadata);
        translation_metadata.merge(metadata);
        }
        
        // Analyze arithmetic expressions for type metadata
        #[cfg(not(feature = "unified_processor"))] // Skip when using unified processor
        if crate::translator::ArithmeticAnalyzer::needs_analysis(&translated_for_analysis) {
            let arithmetic_metadata = crate::translator::ArithmeticAnalyzer::analyze_query(&translated_for_analysis);
            translation_metadata.merge(arithmetic_metadata);
            info!("Found {} arithmetic type hints", translation_metadata.column_mappings.len());
        }
        
        // For now, we'll just analyze the query to get field descriptions
        // In a real implementation, we'd parse the SQL and validate it
        info!("Analyzing query '{}' for field descriptions", translated_for_analysis);
        info!("Original query: {}", cleaned_query);
        info!("Is simple param select: {}", is_simple_param_select);
        let field_descriptions = if query_starts_with_ignore_case(&cleaned_query, "SELECT") || 
                                   ReturningTranslator::has_returning_clause(&cleaned_query) {
            // Don't try to get field descriptions if this is a catalog query
            // These queries are handled specially and don't need real field info
            if cleaned_query.contains("pg_catalog") || cleaned_query.contains("pg_type") {
                info!("Skipping field description for catalog query");
                Vec::new()
            } else {
                // Try to get field descriptions
                // For parameterized queries, substitute dummy values
                // Use the translated query for analysis
                let mut test_query = translated_for_analysis.to_string();
                let param_count = ParameterParser::count_parameters(&translated_for_analysis);
                
                if param_count > 0 {
                    // Replace parameters with dummy values using proper parser
                    let dummy_values = vec!["NULL".to_string(); param_count];
                    test_query = ParameterParser::substitute_parameters(&test_query, &dummy_values)
                        .unwrap_or(test_query); // Fall back to original if substitution fails
                }
                
                // First, analyze the original query for type casts in the SELECT clause
                let cast_info = Self::analyze_column_casts(&cleaned_query);
                info!("Detected column casts: {:?}", cast_info);
                
                // Remove PostgreSQL-style type casts before executing
                // Be careful not to match IPv6 addresses like ::1
                let cast_regex = regex::Regex::new(r"::[a-zA-Z]\w*").unwrap();
                test_query = cast_regex.replace_all(&test_query, "").to_string();
                
                // Add LIMIT 1 to avoid processing too much data (but not for INSERT/UPDATE/DELETE RETURNING)
                if query_starts_with_ignore_case(&test_query, "SELECT") {
                    test_query = format!("{test_query} LIMIT 1");
                }
                let cached_conn = Self::get_or_cache_connection(session, db).await;
                let test_response = db.query_with_session_cached(&test_query, &session.id, cached_conn.as_ref()).await;
                
                match test_response {
                    Ok(response) => {
                        info!("Test query returned {} columns: {:?}", response.columns.len(), response.columns);
                        // Extract table name from query to look up schema
                        let table_name = extract_table_name_from_select(&query);
                        info!("Extracted table name from query '{}': {:?}", query, table_name);
                        
                        // Pre-fetch schema types for all columns if we have a table name
                        let mut schema_types = std::collections::HashMap::new();
                        if let Some(ref table) = table_name {
                            info!("Looking up schema types for table '{}' with columns: {:?}", table, response.columns);
                            // For aliased columns, try to find the source column
                            for col_name in &response.columns {
                                // First try direct lookup
                                match db.get_schema_type_with_session(&session.id, table, col_name).await {
                                    Ok(Some(pg_type)) => {
                                        info!("Found schema type for {}.{}: {}", table, col_name, pg_type);
                                        schema_types.insert(col_name.clone(), pg_type);
                                    }
                                    Ok(None) => {
                                        info!("No schema type found for {}.{}", table, col_name);
                                    }
                                    Err(e) => {
                                        info!("Error looking up schema type for {}.{}: {}", table, col_name, e);
                                    }
                                }
                                
                                if schema_types.get(col_name).is_none() {
                                        // First check translation metadata
                                    if let Some(hint) = translation_metadata.get_hint(col_name) {
                                        // For datetime expressions, check if we have a source column and prefer its type
                                        if let Some(ref source_col) = hint.source_column {
                                            if let Ok(Some(source_type)) = db.get_schema_type_with_session(&session.id, table, source_col).await {
                                                info!("Found source column type for datetime expression '{}' -> '{}': {}", col_name, source_col, source_type);
                                                schema_types.insert(col_name.clone(), source_type);
                                            } else if let Some(suggested_type) = &hint.suggested_type {
                                                info!("Using suggested type for datetime expression '{}': {:?}", col_name, suggested_type);
                                                // Convert PgType to the string format used in schema
                                                let type_string = match suggested_type {
                                                    crate::types::PgType::Float8 => "DOUBLE PRECISION",
                                                    crate::types::PgType::Float4 => "REAL",
                                                    crate::types::PgType::Int4 => "INTEGER",
                                                    crate::types::PgType::Int8 => "BIGINT",
                                                    crate::types::PgType::Text => "TEXT",
                                                    crate::types::PgType::Date => "DATE",
                                                    crate::types::PgType::Time => "TIME",
                                                    crate::types::PgType::Timestamp => "TIMESTAMP",
                                                    crate::types::PgType::Timestamptz => "TIMESTAMPTZ",
                                                    crate::types::PgType::TextArray => "TEXT[]",
                                                    _ => "TEXT", // Default to TEXT for unknown types
                                                };
                                                schema_types.insert(col_name.clone(), type_string.to_string());
                                            }
                                        } else if let Some(suggested_type) = &hint.suggested_type {
                                            info!("Found type hint from translation for '{}': {:?}", col_name, suggested_type);
                                            // Convert PgType to the string format used in schema
                                            let type_string = match suggested_type {
                                                crate::types::PgType::Float8 => "DOUBLE PRECISION",
                                                crate::types::PgType::Float4 => "REAL",
                                                crate::types::PgType::Int4 => "INTEGER",
                                                crate::types::PgType::Int8 => "BIGINT",
                                                crate::types::PgType::Text => "TEXT",
                                                crate::types::PgType::Date => "DATE",
                                                crate::types::PgType::Time => "TIME",
                                                crate::types::PgType::Timestamp => "TIMESTAMP",
                                                crate::types::PgType::Timestamptz => "TIMESTAMPTZ",
                                                _ => "TEXT", // Default to TEXT for unknown types
                                            };
                                            schema_types.insert(col_name.clone(), type_string.to_string());
                                        }
                                    } else {
                                        // Try to find source column if this is an alias
                                        if let Some(source_col) = Self::extract_source_column_for_alias(&cleaned_query, col_name) {
                                            if let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, table, &source_col).await {
                                                info!("Found schema type for alias '{}' -> source column '{}': {}", col_name, source_col, pg_type);
                                                schema_types.insert(col_name.clone(), pg_type);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
                        // Try to infer types from the first row if available
                        let inferred_types = response.columns.iter()
                            .enumerate()
                            .map(|(i, col_name)| {
                                // First priority: Check if this column has an explicit cast
                                if let Some(cast_type) = cast_info.get(&i) {
                                    return Self::cast_type_to_oid(cast_type);
                                }
                                
                                // For parameter columns (NULL from SELECT $1), try to match with parameters
                                if col_name == "NULL" || col_name == "?column?" {
                                    // For queries like SELECT $1, $2, the columns correspond to parameters
                                    if is_simple_param_select {
                                        // Count which parameter this column represents
                                        // For SELECT $1, $2, column 0 = param 0, column 1 = param 1
                                        info!("Simple parameter SELECT detected, column {} likely corresponds to parameter {}", i, i + 1);
                                        
                                        // Check actual_param_types which includes inferred types
                                        if !actual_param_types.is_empty() && i < actual_param_types.len() {
                                            let param_type = actual_param_types[i];
                                            if param_type != 0 && param_type != PgType::Text.to_oid() {
                                                info!("Using actual param type {} for column {}", param_type, i);
                                                return param_type;
                                            }
                                        }
                                        
                                        // If we have param_types provided, use them
                                        if !param_types.is_empty() && i < param_types.len() {
                                            let param_type = param_types[i];
                                            if param_type != 0 {
                                                info!("Using provided param type {} for column {}", param_type, i);
                                                return param_type;
                                            }
                                        }
                                        
                                        // Default to TEXT for now - will be handled during execution
                                        info!("No specific param type for column {}, defaulting to TEXT", i);
                                        return PgType::Text.to_oid();
                                    }
                                    
                                    // For other queries with NULL columns, default to TEXT
                                    return PgType::Text.to_oid();
                                }
                                
                                // Second priority: Check translation metadata for type hints
                                if let Some(hint) = translation_metadata.get_hint(col_name) {
                                    if let Some(suggested_type) = &hint.suggested_type {
                                        info!("Using type hint from translation metadata for '{}': {:?}", col_name, suggested_type);
                                        return suggested_type.to_oid();
                                    }
                                } else {
                                    info!("No type hint found in translation metadata for '{}'", col_name);
                                }
                                
                                // Third priority: Check schema table for stored type mappings
                                if let Some(pg_type) = schema_types.get(col_name) {
                                    // Use basic type OID mapping (enum checking would require async which isn't allowed in closure)
                                    return crate::types::SchemaTypeMapper::pg_type_string_to_oid(pg_type);
                                }
                                
                                // Third priority: Check for aggregate functions
                                let col_lower = col_name.to_lowercase();
                                if let Some(oid) = crate::types::SchemaTypeMapper::get_aggregate_return_type_with_query(&col_lower, None, None, Some(&cleaned_query)) {
                                    info!("Column '{}' identified with type OID {} from aggregate detection", col_name, oid);
                                    return oid;
                                }
                                
                                // Check if this looks like a numeric result column based on the translated query
                                // For arithmetic operations that result in decimal functions, the cleaned_query
                                // might contain patterns like "decimal_mul(...) AS col_name"
                                if cleaned_query.contains("decimal_mul") || cleaned_query.contains("decimal_add") || 
                                   cleaned_query.contains("decimal_sub") || cleaned_query.contains("decimal_div") {
                                    // This query uses decimal arithmetic functions
                                    // Check if this column might be the result
                                    if col_name.contains("total") || col_name.contains("sum") || 
                                       col_name.contains("price") || col_name.contains("amount") ||
                                       col_name == "?column?" {
                                        info!("Column '{}' appears to be result of decimal arithmetic", col_name);
                                        return PgType::Numeric.to_oid();
                                    }
                                }
                                
                                // Fourth priority: For expressions, try to infer from SQLite's type affinity
                                // SQLite will tell us the actual type of the expression result
                                
                                // Last resort: Try to infer from value if we have data
                                if !response.rows.is_empty() {
                                    if let Some(value) = response.rows[0].get(i) {
                                        let value_str = value.as_ref().and_then(|v| std::str::from_utf8(v).ok()).unwrap_or("<non-utf8>");
                                        let inferred_type = crate::types::SchemaTypeMapper::infer_type_from_value(value.as_deref());
                                        info!("Column '{}': inferring type from value '{}' -> type OID {}", col_name, value_str, inferred_type);
                                        inferred_type
                                    } else {
                                        info!("Column '{}': NULL value, defaulting to text", col_name);
                                        PgType::Text.to_oid() // text for NULL
                                    }
                                } else {
                                    info!("Column '{}': no data rows, defaulting to text", col_name);
                                    PgType::Text.to_oid() // text default when no data
                                }
                            })
                            .collect::<Vec<_>>();
                        
                        let fields = response.columns.iter()
                            .enumerate()
                            .map(|(i, col_name)| FieldDescription {
                                name: col_name.clone(),
                                table_oid: 0,
                                column_id: (i + 1) as i16,
                                type_oid: *inferred_types.get(i).unwrap_or(&25),
                                type_size: -1,
                                type_modifier: -1,
                                format: 0,
                            })
                            .collect::<Vec<_>>();
                        info!("Parsed {} field descriptions from query with inferred types", fields.len());
                        fields
                    }
                    Err(e) => {
                        info!("Failed to get field descriptions: {} - will determine during execute", e);
                        Vec::new()
                    }
                }
            }
        } else {
            Vec::new()
        };
        
        // If param_types is still empty after analysis but query has parameters, infer basic types
        // This should only happen if analysis failed completely
        if actual_param_types.is_empty() && cleaned_query.contains('$') && !needs_schema_analysis {
            // Count parameters in the query
            let mut max_param = 0;
            for i in 1..=99 {
                if cleaned_query.contains(&format!("${i}")) {
                    max_param = i;
                } else if max_param > 0 {
                    break;
                }
            }
            
            info!("Query has {} parameters and analysis didn't run, defaulting all to text", max_param);
            // Default all to text - we'll handle type conversion during execution
            actual_param_types = vec![PgType::Text.to_oid(); max_param];
        }
        
        info!("Final param_types for statement: {:?}", actual_param_types);
        
        // Store the prepared statement
        // We already translated the query above for analysis, so just use that
        let translated_query = Some(translated_for_analysis);
        
        let stmt = PreparedStatement {
            query: cleaned_query.clone(),
            translated_query,
            param_types: actual_param_types.clone(),
            client_param_types: original_client_param_types, // Store the original client-sent types
            param_formats: vec![0; actual_param_types.len()], // Default to text format
            field_descriptions,
            translation_metadata: if translation_metadata.column_mappings.is_empty() {
                None
            } else {
                Some(translation_metadata)
            },
        };
        
        // Cache the prepared statement globally to avoid re-parsing
        GLOBAL_PREPARED_STATEMENT_CACHE.insert(&cleaned_query, &actual_param_types, stmt.clone());
        
        // Update the parameter cache with the translated query
        if let Some(mut cached_info) = GLOBAL_PARAMETER_CACHE.get(&cleaned_query) {
            cached_info.translated_query = stmt.translated_query.clone();
            GLOBAL_PARAMETER_CACHE.insert(cleaned_query.clone(), cached_info);
        }
        
        session.prepared_statements.write().await.insert(name.clone(), stmt);
        
        // Send ParseComplete
        framed.send(BackendMessage::ParseComplete).await
            .map_err(PgSqliteError::Io)?;
        
        Ok(())
    }
    
    /// Try to extract the source column for an alias in a simple SELECT
    /// e.g., "SELECT ts AT TIME ZONE 'UTC' as ts_utc" -> source column is "ts"
    fn extract_source_column_for_alias(query: &str, alias: &str) -> Option<String> {
        // This is a simple heuristic for the common case
        // Look for "SELECT <expr> as <alias>" pattern
        let query_upper = query.to_uppercase();
        let alias_upper = alias.to_uppercase();
        
        // Find "AS <alias>" in the query
        let as_pattern = format!(" AS {alias_upper}");
        if let Some(as_pos) = query_upper.find(&as_pattern) {
            // Work backwards to find the start of the expression
            let before_as = &query[..as_pos];
            
            // For simple cases like "SELECT column_name AS alias"
            // Find the last word before AS
            let words: Vec<&str> = before_as.split_whitespace().collect();
            if let Some(last_word) = words.last() {
                // Check if it's a simple identifier (no operators, functions, etc.)
                if last_word.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    return Some(last_word.to_string());
                }
            }
        }
        
        None
    }
    
    pub async fn handle_bind<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        session: &Arc<SessionState>,
        portal: String,
        statement: String,
        formats: Vec<i16>,
        values: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // Fast path for simple queries - skip debug logging and python parameter checking
        let is_simple_query = {
            let statements = session.prepared_statements.read().await;
            if let Some(stmt) = statements.get(&statement) {
                stmt.query.starts_with("SELECT") && !stmt.query.contains("%(")
            } else {
                false
            }
        };
        
        if !is_simple_query {
        // debug!("Binding portal '{}' to statement '{}' with {} values", portal, statement, values.len());
            
            // Check if this statement used Python-style parameters and reorder values if needed
            {
                let python_mappings = session.python_param_mapping.read().await;
                if let Some(param_names) = python_mappings.get(&statement) {
                    info!("Statement '{}' used Python parameters: {:?}", statement, param_names);
                    
                    // The values come in as a map (conceptually), but we received them as a Vec
                    // We need to reorder them to match the $1, $2, $3... order we created
                    // Since we already converted %(name__0)s -> $1, %(name__1)s -> $2, etc. in parse,
                    // the values should already be in the correct order
                    info!("Python parameter mapping found, values should already be in correct order");
                }
            }
        }
        
        // Get the prepared statement
        let statements = session.prepared_statements.read().await;
        let stmt = statements.get(&statement)
            .ok_or_else(|| PgSqliteError::Protocol(format!("Unknown statement: {statement}")))?;
            
        // debug!("Statement has param_types: {:?}", stmt.param_types);
        // debug!("Received param formats: {:?}", formats);
        
        // Check if we need to infer types (only when param types are empty or unknown)
        let needs_inference = stmt.param_types.is_empty() || 
            stmt.param_types.iter().all(|&t| t == 0);
        
        let mut inferred_types = None;
        
        if needs_inference && !values.is_empty() {
        // debug!("Need to infer parameter types from values");
        // debug!("Statement param_types: {:?}", stmt.param_types);
            let mut types = Vec::new();
            
            for (i, val) in values.iter().enumerate() {
                let format = formats.get(i).copied().unwrap_or(0);
                let inferred_type = if let Some(v) = val {
                    // For binary format, check the length to infer integer types
                    if format == 1 {
                        match v.len() {
                            4 => PgType::Int4.to_oid(), // 4 bytes = int32
                            8 => PgType::Int8.to_oid(), // 8 bytes = int64
                            _ => Self::infer_type_from_value(v, format)
                        }
                    } else {
                        Self::infer_type_from_value(v, format)
                    }
                } else {
                    PgType::Text.to_oid() // NULL can be any type, default to text
                };
                
                info!("  Param {}: inferred type OID {} from value (format={})", i + 1, inferred_type, format);
                types.push(inferred_type);
            }
            
            inferred_types = Some(types);
        }
        
        for (i, val) in values.iter().enumerate() {
            let expected_type = stmt.param_types.get(i).unwrap_or(&0);
            let format = formats.get(i).copied().unwrap_or(0);
            if let Some(v) = val {
                info!("  Param {}: {} bytes, expected type OID {}, format {} ({})", 
                      i + 1, v.len(), expected_type, format, 
                      if format == 1 { "binary" } else { "text" });
                // Log first few bytes as hex for debugging
                let hex_preview = v.iter().take(20).map(|b| format!("{b:02x}")).collect::<Vec<_>>().join(" ");
                info!("    First bytes (hex): {}", hex_preview);
                if format == 0 {
                    // Try to show as string if text format
                    if let Ok(s) = String::from_utf8(v.clone()) {
                        info!("    As string: {:?}", s);
                    }
                }
            } else {
                info!("  Param {}: NULL, expected type OID {}, format {} ({})", 
                      i + 1, expected_type, format,
                      if format == 1 { "binary" } else { "text" });
            }
        }
        
        // Create portal
        let portal_obj = Portal {
            statement_name: statement.clone(),
            query: stmt.query.clone(),
            translated_query: stmt.translated_query.clone(), // Use pre-translated query
            bound_values: values,
            param_formats: if formats.is_empty() {
                vec![0; stmt.param_types.len()] // Default to text format for all params
            } else if formats.len() == 1 {
                vec![formats[0]; stmt.param_types.len()] // Use same format for all params
            } else {
                formats
            },
            result_formats: if result_formats.is_empty() {
                vec![0] // Default to text format
            } else {
                result_formats
            },
            inferred_param_types: inferred_types,
            client_param_types: stmt.client_param_types.clone(), // Store client types for binary decoding
        };
        
        // Debug: Log result formats for binary protocol debugging
        info!("Bind portal '{}': result_formats = {:?}", portal, portal_obj.result_formats);
        
        drop(statements);
        
        // Use portal manager to create portal
        session.portal_manager.create_portal(portal.clone(), portal_obj.clone())?;
        
        // Also maintain backward compatibility with direct portal storage
        session.portals.write().await.insert(portal.clone(), portal_obj);
        
        // Send BindComplete
        framed.send(BackendMessage::BindComplete).await
            .map_err(PgSqliteError::Io)?;
        
        Ok(())
    }
    
    pub async fn handle_execute<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &Arc<DbHandler>,
        session: &Arc<SessionState>,
        portal: String,
        max_rows: i32,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        info!("handle_execute: Executing portal '{}' with max_rows: {}", portal, max_rows);
        
        // Create execution context to track state through all execution paths
        let execution_context = crate::query::ExecutionContext::new(portal.clone());
        
        // Get the portal
        let (query, translated_query, bound_values, param_formats, result_formats, statement_name, inferred_param_types, client_param_types) = {
            let portals = session.portals.read().await;
            let portal_obj = portals.get(&portal)
                .ok_or_else(|| PgSqliteError::Protocol(format!("Unknown portal: {portal}")))?;
            
            info!("Portal '{}' has statement_name='{}'", portal, portal_obj.statement_name);
            
            (portal_obj.query.clone(),
             portal_obj.translated_query.clone(),
             portal_obj.bound_values.clone(),
             portal_obj.param_formats.clone(),
             portal_obj.result_formats.clone(),
             portal_obj.statement_name.clone(),
             portal_obj.inferred_param_types.clone(),
             portal_obj.client_param_types.clone())
        };
        
        info!("handle_execute: query='{}', result_formats={:?}", query, result_formats);
        
        // Get parameter types from the prepared statement
        let param_types = if let Some(inferred) = inferred_param_types {
            // Use inferred types if available
            inferred
        } else {
            let statements = session.prepared_statements.read().await;
            let stmt = statements.get(&statement_name).unwrap();
            stmt.param_types.clone()
        };
        
        // Fast path for simple parameterized queries (SELECT, INSERT, UPDATE, DELETE)
        let is_simple_dml = query_starts_with_ignore_case(&query, "INSERT") ||
                           query_starts_with_ignore_case(&query, "UPDATE") ||
                           query_starts_with_ignore_case(&query, "DELETE");
        
        let meets_fast_path_conditions = (query_starts_with_ignore_case(&query, "SELECT") || is_simple_dml) && 
           !query.contains("JOIN") && 
           !query.contains("GROUP BY") && 
           !query.contains("HAVING") &&
           !query.contains("::") &&
           !query.contains("UNION") &&
           !query.contains("INTERSECT") &&
           !query.contains("EXCEPT") &&
           !bound_values.is_empty() &&  // Must have parameters
           query.contains('$');          // Must be parameterized
        info!("Fast path conditions met: {} (query type: {})", 
              meets_fast_path_conditions, 
              if query_starts_with_ignore_case(&query, "SELECT") { "SELECT" } 
              else if query_starts_with_ignore_case(&query, "INSERT") { "INSERT" }
              else if query_starts_with_ignore_case(&query, "UPDATE") { "UPDATE" }
              else if query_starts_with_ignore_case(&query, "DELETE") { "DELETE" }
              else { "OTHER" });
        
        if meets_fast_path_conditions {
            
        info!("Using fast path for simple parameterized query: {}", query);
            
            // Get cached connection first
            let _cached_conn = Self::get_or_cache_connection(session, db).await;
            
            // Use the original query if no translation needed
            let query_to_execute = if let Some(ref translated) = translated_query {
                translated
            } else {
                &query
            };
            
            // Convert parameters to rusqlite values based on format and type
            let mut converted_params = Vec::new();
            for (i, value) in bound_values.iter().enumerate() {
                match value {
                    Some(bytes) => {
                        let format = param_formats.get(i).unwrap_or(&0);
                        // Use client param type for binary format, schema type for text format
                        let param_type = if *format == 1 {
                            // For binary format, prefer client type if available, otherwise use analyzed type
                            client_param_types.get(i).copied()
                                .filter(|&t| t != 0) // Ignore unknown types
                                .or_else(|| param_types.get(i).copied())
                                .unwrap_or(25) // Default to TEXT
                        } else {
                            *param_types.get(i).unwrap_or(&25) // Use schema type for text parsing
                        };
                        
                        match Self::convert_parameter_to_value(bytes, *format, param_type) {
                            Ok(sql_value) => {
                                converted_params.push(sql_value)
                            },
                            Err(e) => {
                                warn!("Failed to convert parameter {}: {}", i, e);
                                // Fall back to text conversion
                                match std::str::from_utf8(bytes) {
                                    Ok(text) => {
                                        converted_params.push(rusqlite::types::Value::Text(text.to_string()))
                                    },
                                    Err(_) => converted_params.push(rusqlite::types::Value::Blob(bytes.clone())),
                                }
                            }
                        }
                    }
                    None => converted_params.push(rusqlite::types::Value::Null),
                }
            }
            
            // Use the new execute method that accepts rusqlite values
            info!("Fast path: About to call execute_with_rusqlite_params");
            match db.execute_with_rusqlite_params(query_to_execute, &converted_params, &session.id).await {
                Ok(response) => {
                    info!("Fast path: execute_with_rusqlite_params succeeded");
                    
                    // Check if this is a DML operation
                    let is_select = query_starts_with_ignore_case(&query, "SELECT");
                    let is_insert = query_starts_with_ignore_case(&query, "INSERT");
                    let is_update = query_starts_with_ignore_case(&query, "UPDATE");
                    let is_delete = query_starts_with_ignore_case(&query, "DELETE");
                    let has_returning = ReturningTranslator::has_returning_clause(&query);
                    
                    if !is_select && !has_returning {
                        // DML operation without RETURNING - send CommandComplete
                        let tag = if is_insert {
                            format!("INSERT 0 {}", response.rows_affected)
                        } else if is_update {
                            format!("UPDATE {}", response.rows_affected)
                        } else if is_delete {
                            format!("DELETE {}", response.rows_affected)
                        } else {
                            format!("OK")
                        };
                        
                        info!("Fast path: Sending CommandComplete for DML operation: {}", tag);
                        framed.send(BackendMessage::CommandComplete { tag }).await
                            .map_err(PgSqliteError::Io)?;
                        
                        return Ok(());
                    }
                    
                    // SELECT operation - continue with RowDescription and DataRows
                    let send_row_desc = {
                        let statements = session.prepared_statements.read().await;
                        info!("Fast path: statement_name='{}'", statement_name);
                        if let Some(stmt) = statements.get(&statement_name) {
                                // Only send RowDescription if statement has no field descriptions
                                // If it has field descriptions, that means Describe was already called
                                // and already sent RowDescription to the client
                                let needs_row_desc = stmt.field_descriptions.is_empty();
                                info!("Fast path: stmt.field_descriptions.is_empty()={}, send_row_desc={}", 
                                     stmt.field_descriptions.is_empty(), needs_row_desc);
                                needs_row_desc
                            } else {
                                info!("Fast path: No statement found, send_row_desc=true");
                                true
                            }
                        };
                        
                        if send_row_desc {
                            info!("Fast path: Sending RowDescription for binary format");
                            let format = if result_formats.is_empty() { 0 } else { result_formats[0] };
                            
                            // Infer field types from data
                            let fields: Vec<FieldDescription> = response.columns.iter()
                                .enumerate()
                                .map(|(i, name)| {
                                    // Try to infer type from data
                                    let type_oid = if !response.rows.is_empty() {
                                        if let Some(value) = response.rows[0].get(i) {
                                            if let Some(bytes) = value {
                                                if let Ok(s) = std::str::from_utf8(bytes) {
                                                    // Check aggregate functions
                                                    let col_lower = name.to_lowercase();
                                                    if let Some(oid) = crate::types::SchemaTypeMapper::get_aggregate_return_type(&col_lower, None, None) {
                                                        oid
                                                    } else if s == "t" || s == "f" || s == "true" || s == "false" {
                                                        PgType::Bool.to_oid()
                                                    } else if let Ok(_) = s.parse::<i64>() {
                                                        // Integer without decimal point
                                                        if !s.contains('.') {
                                                            PgType::Int4.to_oid()  // Use INT4 instead of INT8 for compatibility
                                                        } else {
                                                            // Should not happen - i64 parse would fail
                                                            PgType::Float8.to_oid()
                                                        }
                                                    } else if let Ok(_) = s.parse::<f64>() {
                                                        // It's a float - use FLOAT8 for compatibility
                                                        PgType::Float8.to_oid()
                                                    } else {
                                                        PgType::Text.to_oid()
                                                    }
                                                } else {
                                                    PgType::Bytea.to_oid()
                                                }
                                            } else {
                                                PgType::Text.to_oid()
                                            }
                                        } else {
                                            PgType::Text.to_oid()
                                        }
                                    } else {
                                        PgType::Text.to_oid()
                                    };
                                    
                                    FieldDescription {
                                        name: name.clone(),
                                        table_oid: 0,
                                        column_id: (i + 1) as i16,
                                        type_oid,
                                        type_size: -1,
                                        type_modifier: -1,
                                        format,
                                    }
                                })
                                .collect();
                            framed.send(BackendMessage::RowDescription(fields.clone())).await
                                .map_err(PgSqliteError::Io)?;
                            info!("Fast path: RowDescription sent successfully with {} fields", fields.len());
                        } else {
                            info!("Fast path: Not sending RowDescription (send_row_desc={}, already_sent={})", 
                                  send_row_desc, execution_context.is_row_description_sent());
                        }
                        
                        // Send data rows
                        let row_count = response.rows.len();
                        info!("Fast path: About to send {} data rows", row_count);
                        
                        // If binary format is requested, we need to encode the rows
                        if result_formats.iter().any(|&f| f == 1) {
                            info!("Fast path: Binary format requested, using optimized encoding");
                            
                            // Get field types from the statement's field descriptions if available
                            let field_types: Vec<i32> = {
                                let statements = session.prepared_statements.read().await;
                                if let Some(stmt) = statements.get(&statement_name) {
                                    if !stmt.field_descriptions.is_empty() {
                                        // Use the field types from the statement's field descriptions
                                        info!("Fast path: Using field types from statement field descriptions");
                                        stmt.field_descriptions.iter()
                                            .map(|field| field.type_oid)
                                            .collect()
                                    } else {
                                        // Fall back to inferring from data
                                        info!("Fast path: No field descriptions, inferring types from data");
                                        response.columns.iter()
                                            .enumerate()
                                            .map(|(i, name)| {
                                                // Try to infer type from data (same logic as RowDescription)
                                                if !response.rows.is_empty() {
                                                    if let Some(value) = response.rows[0].get(i) {
                                                        if let Some(bytes) = value {
                                                            if let Ok(s) = std::str::from_utf8(bytes) {
                                                                // Check aggregate functions
                                                                let col_lower = name.to_lowercase();
                                                                if let Some(oid) = crate::types::SchemaTypeMapper::get_aggregate_return_type(&col_lower, None, None) {
                                                                    oid
                                                                } else if s == "t" || s == "f" || s == "true" || s == "false" {
                                                                    PgType::Bool.to_oid()
                                                                } else if let Ok(_) = s.parse::<i64>() {
                                                                    // Integer without decimal point
                                                                    if !s.contains('.') {
                                                                        PgType::Int4.to_oid()  // Use INT4 instead of INT8 for compatibility
                                                                    } else {
                                                                        // Should not happen - i64 parse would fail
                                                                        PgType::Float8.to_oid()
                                                                    }
                                                                } else if let Ok(_) = s.parse::<f64>() {
                                                                    // It's a float - use FLOAT8 for compatibility
                                                                    PgType::Float8.to_oid()
                                                                } else {
                                                                    PgType::Text.to_oid()
                                                                }
                                                            } else {
                                                                PgType::Bytea.to_oid()
                                                            }
                                                        } else {
                                                            PgType::Text.to_oid()
                                                        }
                                                    } else {
                                                        PgType::Text.to_oid()
                                                    }
                                                } else {
                                                    PgType::Text.to_oid()
                                                }
                                            })
                                            .collect()
                                    }
                                } else {
                                    // No statement found, fall back to inferring
                                    info!("Fast path: No statement found, inferring types from data");
                                    response.columns.iter()
                                        .enumerate()
                                        .map(|(i, name)| {
                                            // Try to infer type from data (same logic as RowDescription)
                                            if !response.rows.is_empty() {
                                                if let Some(value) = response.rows[0].get(i) {
                                                    if let Some(bytes) = value {
                                                        if let Ok(s) = std::str::from_utf8(bytes) {
                                                            // Check aggregate functions
                                                            let col_lower = name.to_lowercase();
                                                            if let Some(oid) = crate::types::SchemaTypeMapper::get_aggregate_return_type(&col_lower, None, None) {
                                                                oid
                                                            } else if s == "t" || s == "f" || s == "true" || s == "false" {
                                                                PgType::Bool.to_oid()
                                                            } else if let Ok(_) = s.parse::<i64>() {
                                                                // Integer without decimal point
                                                                if !s.contains('.') {
                                                                    PgType::Int4.to_oid()  // Use INT4 instead of INT8 for compatibility
                                                                } else {
                                                                    // Should not happen - i64 parse would fail
                                                                    PgType::Float8.to_oid()
                                                                }
                                                            } else if let Ok(_) = s.parse::<f64>() {
                                                                // It's a float - use FLOAT8 for compatibility
                                                                PgType::Float8.to_oid()
                                                            } else {
                                                                PgType::Text.to_oid()
                                                            }
                                                        } else {
                                                            PgType::Bytea.to_oid()
                                                        }
                                                    } else {
                                                        PgType::Text.to_oid()
                                                    }
                                                } else {
                                                    PgType::Text.to_oid()
                                                }
                                            } else {
                                                PgType::Text.to_oid()
                                            }
                                        })
                                        .collect()
                                }
                            };
                            info!("Fast path: Field types: {:?}", field_types);
                            
                            // Use optimized batch encoder
                            let (encoded_rows, _encoder) = Self::encode_rows_optimized(&response.rows, &result_formats, &field_types)?;
                            
                            // Send all encoded rows
                            for encoded_row in encoded_rows {
                                framed.send(BackendMessage::DataRow(encoded_row)).await
                                    .map_err(PgSqliteError::Io)?;
                            }
                        } else {
                            // Text format - send as-is
                            for row in response.rows {
                                framed.send(BackendMessage::DataRow(row)).await
                                    .map_err(PgSqliteError::Io)?;
                            }
                        }
                        
                        framed.send(BackendMessage::CommandComplete { 
                            tag: format!("SELECT {}", row_count) 
                        }).await.map_err(PgSqliteError::Io)?;
                        
                        // Portal management for suspended queries
                        if max_rows > 0 && row_count >= max_rows as usize {
                            // Portal suspended - but we consumed all rows
                            framed.send(BackendMessage::PortalSuspended).await
                                .map_err(PgSqliteError::Io)?;
                        }
                        
                        return Ok(());
                }
                Err(e) => {
                    // Check if this is an execution error vs compatibility issue
                    if e.to_string().contains("SQLite error:") || 
                       e.to_string().contains("constraint") ||
                       e.to_string().contains("UNIQUE") {
                        // This is an execution error, not a compatibility issue
                        error!("Fast path execution failed with SQLite error: {}", e);
                        
                        // Convert SQLite errors to appropriate PostgreSQL error codes
                        let error_response = match &e {
                            PgSqliteError::Sqlite(sqlite_err) => {
                                crate::error::sqlite_error_to_pg(sqlite_err, query_to_execute)
                            }
                            PgSqliteError::Validation(pg_err) => {
                                pg_err.to_error_response()
                            }
                            _ => crate::protocol::ErrorResponse::new(
                                "ERROR".to_string(),
                                "42000".to_string(),
                                e.to_string(),
                            )
                        };
                        framed.send(BackendMessage::ErrorResponse(Box::new(error_response))).await?;
                        return Ok(());
                    }
                    // Only fall back for compatibility issues
                    info!("Fast path: execute_with_rusqlite_params failed: {}, falling back to substitution", e);
                }
            }
        }
        
        info!("After fast path block, continuing execution");
        
        // Try optimized extended fast path first for parameterized queries
        if !bound_values.is_empty() && query.contains('$') {
            info!("Checking extended fast path for parameterized query: '{}'", query);
            if query.contains("::") {
                info!("Query contains cast operator '::', may fall back to substitution path");
            }
            let query_type = super::extended_fast_path::QueryType::from_query(&query);
            
            // Early check: Skip fast path for SELECT with binary results
            if matches!(query_type, super::extended_fast_path::QueryType::Select) 
                && !result_formats.is_empty() 
                && result_formats[0] == 1 {
                info!("Extended fast path: Skipping for binary SELECT results");
                // Skip fast path entirely for binary SELECT results
            } else {
            
            // Use client-sent types for binary parameter decoding, but fall back to analyzed types if empty
            let original_types = if client_param_types.is_empty() || client_param_types.iter().all(|&t| t == 0) {
                param_types.clone()
            } else {
                client_param_types.clone()
            };
            
            // Use optimized path for SELECT, INSERT, UPDATE, DELETE
            match query_type {
                super::extended_fast_path::QueryType::Select |
                super::extended_fast_path::QueryType::Insert |
                super::extended_fast_path::QueryType::Update |
                super::extended_fast_path::QueryType::Delete => {
                    match super::extended_fast_path::ExtendedFastPath::execute_with_params(
                        framed,
                        db,
                        session,
                        &portal,
                        &query,
                        &bound_values,
                        &param_formats,
                        &result_formats,
                        &param_types,
                        &original_types,
                        query_type,
                        &execution_context,
                    ).await {
                        Ok(true) => return Ok(()), // Successfully executed via fast path
                        Ok(false) => {}, // Fall back to normal path
                        Err(e) => {
                            // Check if this is an execution error vs compatibility issue
                            if e.to_string().contains("SQLite error:") || 
                               e.to_string().contains("constraint") ||
                               e.to_string().contains("UNIQUE") {
                                // This is an execution error, not a compatibility issue
                                error!("Extended fast path execution failed with SQLite error: {}", e);
                                return Err(e);
                            }
                            warn!("Extended fast path failed with error: {}, falling back to normal path", e);
                            // Fall back to normal path on error
                        }
                    }
                }
                _ => {}, // Fall back to normal path for other query types
            }
            } // End of else block for binary result check
        }
        
        // Try existing fast path as second option
        if let Some(fast_query) = crate::query::can_use_fast_path_enhanced(&query) {
            // Only use fast path for queries that actually have parameters in the extended protocol
            if !bound_values.is_empty() && query.contains('$') {
                if let Ok(Some(result)) = Self::try_execute_fast_path_with_params(
                    framed, 
                    db, 
                    session, 
                    &portal, 
                    &query, 
                    &bound_values, 
                    &param_formats, 
                    &param_types,
                    &client_param_types,
                    &fast_query, 
                    max_rows,
                    &execution_context
                ).await {
                    return result;
                }
            }
        }

        // Use translated query if available, otherwise use original
        let query_to_use = if let Some(ref tq) = translated_query {
            info!("Using translated query: '{}' (original: '{}')", tq, query);
            tq
        } else {
            info!("No translated query available, using original: '{}'", query);
            &query
        };
        
        // Validate numeric constraints before parameter substitution
        // TEMPORARILY DISABLED to test binary parameter handling
        let validation_error = if false && query_starts_with_ignore_case(query_to_use, "INSERT") {
            if let Some(table_name) = Self::extract_table_name_from_insert(query_to_use) {
                // For parameterized queries, we need to check constraints with actual values
                // Build a substituted query just for validation
                let validation_query = Self::substitute_parameters(query_to_use, &bound_values, &param_formats, &param_types, &client_param_types)?;
                
                match db.with_session_connection(&session.id, |conn| {
                    match NumericValidator::validate_insert(conn, &validation_query, &table_name) {
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
        } else if query_starts_with_ignore_case(query_to_use, "UPDATE") {
            if let Some(table_name) = Self::extract_table_name_from_update(query_to_use) {
                // For parameterized queries, we need to check constraints with actual values
                // Build a substituted query just for validation
                let validation_query = Self::substitute_parameters(query_to_use, &bound_values, &param_formats, &param_types, &client_param_types)?;
                
                match db.with_session_connection(&session.id, |conn| {
                    match NumericValidator::validate_update(conn, &validation_query, &table_name) {
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
        } else {
            None
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
        
        // Check if we have binary parameters or should use parameterized execution
        let _has_binary_params = param_formats.iter().any(|&f| f == 1);
        // Check the original query for $ parameters, not the translated one
        let has_params = !bound_values.is_empty() && query.contains('$');
        
        // Debug logging
        if !bound_values.is_empty() {
            info!("Original query '{}' has {} bound values, contains '$': {}", query, bound_values.len(), query.contains('$'));
            info!("Query to use: '{}'", query_to_use);
        }
        
        // Use parameterized path for all queries with parameters (including RETURNING)
        // Now that we use native SQLite RETURNING, we can handle it in the parameterized path
        if has_params { 
            info!("USING PARAMETERIZED PATH for query: {} (translated: {})", query, query_to_use);
            // Convert parameters to rusqlite values
            let mut converted_params = Vec::new();
            for (i, value) in bound_values.iter().enumerate() {
                match value {
                    Some(bytes) => {
                        let format = param_formats.get(i).unwrap_or(&0);
                        // Use client param type for binary format, schema type for text format
                        let param_type = if *format == 1 {
                            // For binary format, prefer client type if available, otherwise use analyzed type
                            client_param_types.get(i).copied()
                                .filter(|&t| t != 0) // Ignore unknown types
                                .or_else(|| param_types.get(i).copied())
                                .unwrap_or(25) // Default to TEXT
                        } else {
                            *param_types.get(i).unwrap_or(&25) // Use schema type for text parsing
                        };
                        
                        info!("Parameter {}: {} bytes, format={}, type={}", i, bytes.len(), format, param_type);
                        
                        match Self::convert_parameter_to_value(bytes, *format, param_type) {
                            Ok(sql_value) => {
                                converted_params.push(sql_value);
                            }
                            Err(e) => {
                                warn!("Failed to convert parameter {}: {}", i, e);
                                // Fall back to text conversion
                                match std::str::from_utf8(bytes) {
                                    Ok(text) => {
                                        converted_params.push(rusqlite::types::Value::Text(text.to_string()))
                                    },
                                    Err(_) => converted_params.push(rusqlite::types::Value::Blob(bytes.clone())),
                                }
                            }
                        }
                    }
                    None => converted_params.push(rusqlite::types::Value::Null),
                }
            }
            
            // Apply JSON operator translation if needed
            // IMPORTANT: Use the translated query with parameter placeholders
            let mut parameterized_query = query_to_use.to_string();
            if JsonTranslator::contains_json_operations(&parameterized_query) {
                match JsonTranslator::translate_json_operators(&parameterized_query) {
                    Ok(translated) => {
                        parameterized_query = translated;
                    }
                    Err(_) => {
                        // Continue with original query
                    }
                }
            }
            
            // Execute based on query type using parameterized execution
            if query_starts_with_ignore_case(&parameterized_query, "SELECT") {
                // For SELECT, use the fast path we already set up
                match db.execute_with_rusqlite_params(&parameterized_query, &converted_params, &session.id).await {
                    Ok(response) => {
                        // Check if we need to send RowDescription
                        let _send_row_desc = {
                            let statements = session.prepared_statements.read().await;
                            info!("Non-fast path SELECT: checking statement_name='{}'", statement_name);
                            if let Some(stmt) = statements.get(&statement_name) {
                                // Only send RowDescription if statement has no field descriptions
                                // If it has field descriptions, that means Describe was already called
                                // and already sent RowDescription to the client
                                let needs_row_desc = stmt.field_descriptions.is_empty();
                                info!("Non-fast path SELECT: field_descriptions.is_empty()={}, send_row_desc={}", 
                                     stmt.field_descriptions.is_empty(), needs_row_desc);
                                needs_row_desc
                            } else {
                                info!("Non-fast path SELECT: No statement found, send_row_desc=true");
                                true
                            }
                        };
                        
                        // Always use send_select_response which will check ExecutionContext
                        Self::send_select_response(framed, response, max_rows, &result_formats, Some(&execution_context)).await?;
                        return Ok(());
                    }
                    Err(e) => {
                        // Check if this is an execution error vs compatibility issue
                        if e.to_string().contains("SQLite error:") || 
                           e.to_string().contains("constraint") ||
                           e.to_string().contains("UNIQUE") {
                            // This is an execution error, not a compatibility issue
                            error!("Parameterized SELECT execution failed with SQLite error: {}", e);
                            return Err(e);
                        }
                        warn!("Parameterized SELECT failed: {}, falling back to substitution", e);
                        // Fall through to substitution-based execution
                    }
                }
            } else if query_starts_with_ignore_case(&parameterized_query, "INSERT") 
                || query_starts_with_ignore_case(&parameterized_query, "UPDATE") 
                || query_starts_with_ignore_case(&parameterized_query, "DELETE") {
                
                // Check if it has RETURNING clause
                if ReturningTranslator::has_returning_clause(&parameterized_query) {
                    // For DML with RETURNING, use query to get results
                    info!("Executing DML with RETURNING using native SQLite support");
                    match db.execute_with_rusqlite_params(&parameterized_query, &converted_params, &session.id).await {
                        Ok(response) => {
                            info!("DML with RETURNING succeeded, got {} columns and {} rows", response.columns.len(), response.rows.len());
                            // Don't send RowDescription here - it was already sent during Describe
                            // Just send the data rows
                            Self::send_data_rows_only(framed, response, &result_formats).await?;
                            
                            // Send appropriate command complete tag
                            let tag = if query_starts_with_ignore_case(&parameterized_query, "INSERT") {
                                "INSERT 0 1".to_string() // TODO: Get actual row count
                            } else if query_starts_with_ignore_case(&parameterized_query, "UPDATE") {
                                "UPDATE 1".to_string()
                            } else {
                                "DELETE 1".to_string()
                            };
                            framed.send(BackendMessage::CommandComplete { tag }).await?;
                            return Ok(()); // Important: return here to avoid fallback
                        }
                        Err(e) => {
                            // Check if this is an execution error vs compatibility issue
                            if e.to_string().contains("SQLite error:") || 
                               e.to_string().contains("constraint") ||
                               e.to_string().contains("UNIQUE") {
                                // This is an execution error, not a compatibility issue
                                error!("DML with RETURNING execution failed with SQLite error: {}", e);
                                
                                // Convert SQLite errors to appropriate PostgreSQL error codes
                                let error_response = match &e {
                                    PgSqliteError::Sqlite(sqlite_err) => {
                                        crate::error::sqlite_error_to_pg(sqlite_err, &parameterized_query)
                                    }
                                    PgSqliteError::Validation(pg_err) => {
                                        pg_err.to_error_response()
                                    }
                                    _ => crate::protocol::ErrorResponse::new(
                                        "ERROR".to_string(),
                                        "42000".to_string(),
                                        e.to_string(),
                                    )
                                };
                                framed.send(BackendMessage::ErrorResponse(Box::new(error_response))).await?;
                                return Ok(());
                            }
                            info!("Failed to execute DML with RETURNING: {}", e);
                            warn!("Failed to execute DML with RETURNING: {}", e);
                            // Fall through to substitution for compatibility issues
                        }
                    }
                } else {
                    // For DML without RETURNING, use parameterized execution
                    match db.execute_with_rusqlite_params(&parameterized_query, &converted_params, &session.id).await {
                        Ok(response) => {
                            let tag = if query_starts_with_ignore_case(&parameterized_query, "INSERT") {
                                format!("INSERT 0 {}", response.rows_affected)
                            } else if query_starts_with_ignore_case(&parameterized_query, "UPDATE") {
                                format!("UPDATE {}", response.rows_affected)
                            } else {
                                format!("DELETE {}", response.rows_affected)
                            };
                            framed.send(BackendMessage::CommandComplete { tag }).await?;
                            return Ok(());
                        }
                        Err(e) => {
                            // Check if this is an execution error vs compatibility issue
                            if e.to_string().contains("SQLite error:") || 
                               e.to_string().contains("constraint") ||
                               e.to_string().contains("UNIQUE") {
                                // This is an execution error, not a compatibility issue
                                error!("Parameterized DML execution failed with SQLite error: {}", e);
                                
                                // Convert SQLite errors to appropriate PostgreSQL error codes
                                let error_response = match &e {
                                    PgSqliteError::Sqlite(sqlite_err) => {
                                        crate::error::sqlite_error_to_pg(sqlite_err, &parameterized_query)
                                    }
                                    PgSqliteError::Validation(pg_err) => {
                                        pg_err.to_error_response()
                                    }
                                    _ => crate::protocol::ErrorResponse::new(
                                        "ERROR".to_string(),
                                        "42000".to_string(),
                                        e.to_string(),
                                    )
                                };
                                framed.send(BackendMessage::ErrorResponse(Box::new(error_response))).await?;
                                return Ok(());
                            }
                            warn!("Parameterized DML failed: {}, falling back to substitution", e);
                            // Fall through to substitution-based execution
                        }
                    }
                }
            }
        }
        
        // Fall back to parameter substitution for non-parameterized queries or if parameterized execution failed
        if !bound_values.is_empty() {
            info!("FALLING BACK TO SUBSTITUTION for query: {}", query);
        }
        info!("Falling back to substitution path for query: '{}'", query_to_use);
        let mut final_query = Self::substitute_parameters(query_to_use, &bound_values, &param_formats, &param_types, &client_param_types)?;
        info!("After substitution, final query: '{}'", final_query);
        
        // Apply JSON operator translation if needed
        if JsonTranslator::contains_json_operations(&final_query) {
            match JsonTranslator::translate_json_operators(&final_query) {
                Ok(translated) => {
                    final_query = translated;
                }
                Err(_e) => {
                    // Continue with original query - some operators might not be supported yet
                }
            }
        }
        
        // Debug: Check if this is a catalog query
        if final_query.contains("pg_catalog") || final_query.contains("pg_type") {
            info!("Detected catalog query in extended protocol: {}", final_query);
        }
        
        // Execute based on query type
        if query_starts_with_ignore_case(&final_query, "SELECT") {
            info!("Calling execute_select for query: {}", final_query);
            info!("Statement name: '{}', checking if field_descriptions exist", statement_name);
            {
                let statements = session.prepared_statements.read().await;
                if let Some(stmt) = statements.get(&statement_name) {
                    info!("Statement has {} field_descriptions", stmt.field_descriptions.len());
                } else {
                    info!("Statement '{}' not found in prepared statements", statement_name);
                }
            }
            Self::execute_select(framed, db, session, &portal, &final_query, max_rows, &execution_context).await?;
        } else if query_starts_with_ignore_case(&final_query, "INSERT") 
            || query_starts_with_ignore_case(&final_query, "UPDATE") 
            || query_starts_with_ignore_case(&final_query, "DELETE") {
            Self::execute_dml(framed, db, &final_query, &portal, session).await?;
        } else if query_starts_with_ignore_case(&final_query, "CREATE") 
            || query_starts_with_ignore_case(&final_query, "DROP") 
            || query_starts_with_ignore_case(&final_query, "ALTER") {
            Self::execute_ddl(framed, db, session, &final_query).await?;
        } else if query_starts_with_ignore_case(&final_query, "BEGIN") 
            || query_starts_with_ignore_case(&final_query, "COMMIT") 
            || query_starts_with_ignore_case(&final_query, "ROLLBACK") {
            Self::execute_transaction(framed, db, session, &final_query).await?;
        } else if crate::query::SetHandler::is_set_command(&final_query) {
            // Check if we should skip row description
            let skip_row_desc = {
                let portals = session.portals.read().await;
                if let Some(portal) = portals.get(&portal) {
                    let statements = session.prepared_statements.read().await;
                    if let Some(stmt) = statements.get(&portal.statement_name) {
                        // Skip row description if statement already has field descriptions
                        !stmt.field_descriptions.is_empty()
                    } else {
                        false
                    }
                } else {
                    false
                }
            };
            
            crate::query::SetHandler::handle_set_command_extended(framed, session, &final_query, skip_row_desc).await?;
        } else {
            Self::execute_generic(framed, db, session, &final_query).await?;
        }
        
        Ok(())
    }
    
    pub async fn handle_describe<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        session: &Arc<SessionState>,
        typ: u8,
        name: String,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        info!("Describing {} '{}' (type byte: {:02x})", if typ == b'S' { "statement" } else { "portal" }, if name.is_empty() { "<unnamed>" } else { &name }, typ);
        
        if typ == b'S' {
            // Describe statement
            let statements = session.prepared_statements.read().await;
            let stmt = statements.get(&name)
                .ok_or_else(|| PgSqliteError::Protocol(format!("Unknown statement: {name}")))?;
            
            // Send ParameterDescription first
            framed.send(BackendMessage::ParameterDescription(stmt.param_types.clone())).await
                .map_err(PgSqliteError::Io)?;
            
            // Check if this is a catalog query that needs special handling
            let query = &stmt.query;
            let is_catalog_query = query.contains("pg_catalog") || query.contains("pg_type") || 
                                   query.contains("pg_namespace") || query.contains("pg_class") || 
                                   query.contains("pg_attribute");
            
            // Then send RowDescription or NoData
            if !stmt.field_descriptions.is_empty() {
                info!("Sending RowDescription with {} fields in Describe", stmt.field_descriptions.len());
                framed.send(BackendMessage::RowDescription(stmt.field_descriptions.clone())).await
                    .map_err(PgSqliteError::Io)?;
            } else if is_catalog_query && query_starts_with_ignore_case(query, "SELECT") {
                // For catalog SELECT queries, we need to provide field descriptions
                // even though we skipped them during Parse
                info!("Catalog query detected in Describe, generating field descriptions");
                
                // Parse the query to extract the selected columns (keep JSON path placeholders for now)
                let field_descriptions = if let Ok(parsed) = sqlparser::parser::Parser::parse_sql(
                    &sqlparser::dialect::PostgreSqlDialect {},
                    query
                ) {
                    if let Some(sqlparser::ast::Statement::Query(query_stmt)) = parsed.first() {
                        if let sqlparser::ast::SetExpr::Select(select) = &*query_stmt.body {
                            let mut fields = Vec::new();
                            
                            // Check if it's SELECT *
                            let is_select_star = select.projection.len() == 1 && 
                                matches!(&select.projection[0], sqlparser::ast::SelectItem::Wildcard(_));
                            
                            if is_select_star {
                                // For SELECT *, we need to determine which catalog table is being queried
                                // and return all its columns
                                if query.contains("pg_class") {
                                    // Return all pg_class columns (33 total in current PostgreSQL)
                                    const OID_TYPE: i32 = 26;
                                    const XID_TYPE: i32 = 28;
                                    const ACLITEM_ARRAY_TYPE: i32 = 1034;
                                    const TEXT_ARRAY_TYPE: i32 = 1009;
                                    const PG_NODE_TREE_TYPE: i32 = 194;
                                    
                                    let all_columns = vec![
                                        ("oid", OID_TYPE),
                                        ("relname", PgType::Text.to_oid()),
                                        ("relnamespace", OID_TYPE),
                                        ("reltype", OID_TYPE),
                                        ("reloftype", OID_TYPE),
                                        ("relowner", OID_TYPE),
                                        ("relam", OID_TYPE),
                                        ("relfilenode", OID_TYPE),
                                        ("reltablespace", OID_TYPE),
                                        ("relpages", PgType::Int4.to_oid()),
                                        ("reltuples", PgType::Float4.to_oid()),
                                        ("relallvisible", PgType::Int4.to_oid()),
                                        ("reltoastrelid", OID_TYPE),
                                        ("relhasindex", PgType::Bool.to_oid()),
                                        ("relisshared", PgType::Bool.to_oid()),
                                        ("relpersistence", PgType::Char.to_oid()),
                                        ("relkind", PgType::Char.to_oid()),
                                        ("relnatts", PgType::Int2.to_oid()),
                                        ("relchecks", PgType::Int2.to_oid()),
                                        ("relhasrules", PgType::Bool.to_oid()),
                                        ("relhastriggers", PgType::Bool.to_oid()),
                                        ("relhassubclass", PgType::Bool.to_oid()),
                                        ("relrowsecurity", PgType::Bool.to_oid()),
                                        ("relforcerowsecurity", PgType::Bool.to_oid()),
                                        ("relispopulated", PgType::Bool.to_oid()),
                                        ("relreplident", PgType::Char.to_oid()),
                                        ("relispartition", PgType::Bool.to_oid()),
                                        ("relrewrite", OID_TYPE),
                                        ("relfrozenxid", XID_TYPE),
                                        ("relminmxid", XID_TYPE),
                                        ("relacl", ACLITEM_ARRAY_TYPE),
                                        ("reloptions", TEXT_ARRAY_TYPE),
                                        ("relpartbound", PG_NODE_TREE_TYPE),
                                    ];
                                    
                                    for (i, (name, oid)) in all_columns.into_iter().enumerate() {
                                        fields.push(FieldDescription {
                                            name: name.to_string(),
                                            table_oid: 0,
                                            column_id: (i + 1) as i16,
                                            type_oid: oid,
                                            type_size: -1,
                                            type_modifier: -1,
                                            format: 0,
                                        });
                                    }
                                } else if query.contains("pg_attribute") {
                                    // Return all pg_attribute columns
                                    const OID_TYPE: i32 = 26;
                                    
                                    let all_columns = vec![
                                        ("attrelid", OID_TYPE),
                                        ("attname", PgType::Text.to_oid()),
                                        ("atttypid", OID_TYPE),
                                        ("attstattarget", PgType::Int4.to_oid()),
                                        ("attlen", PgType::Int2.to_oid()),
                                        ("attnum", PgType::Int2.to_oid()),
                                        ("attndims", PgType::Int4.to_oid()),
                                        ("attcacheoff", PgType::Int4.to_oid()),
                                        ("atttypmod", PgType::Int4.to_oid()),
                                        ("attbyval", PgType::Bool.to_oid()),
                                        ("attalign", PgType::Char.to_oid()),
                                        ("attstorage", PgType::Char.to_oid()),
                                        ("attcompression", PgType::Char.to_oid()),
                                        ("attnotnull", PgType::Bool.to_oid()),
                                        ("atthasdef", PgType::Bool.to_oid()),
                                        ("atthasmissing", PgType::Bool.to_oid()),
                                        ("attidentity", PgType::Char.to_oid()),
                                        ("attgenerated", PgType::Char.to_oid()),
                                        ("attisdropped", PgType::Bool.to_oid()),
                                        ("attislocal", PgType::Bool.to_oid()),
                                        ("attinhcount", PgType::Int4.to_oid()),
                                        ("attcollation", OID_TYPE),
                                        ("attacl", PgType::Text.to_oid()), // Simplified - actually aclitem[]
                                        ("attoptions", PgType::Text.to_oid()), // Simplified - actually text[]
                                        ("attfdwoptions", PgType::Text.to_oid()), // Simplified - actually text[]
                                        ("attmissingval", PgType::Text.to_oid()), // Simplified
                                    ];
                                    
                                    for (i, (name, oid)) in all_columns.into_iter().enumerate() {
                                        fields.push(FieldDescription {
                                            name: name.to_string(),
                                            table_oid: 0,
                                            column_id: (i + 1) as i16,
                                            type_oid: oid,
                                            type_size: -1,
                                            type_modifier: -1,
                                            format: 0,
                                        });
                                    }
                                }
                            } else {
                                // Parse the projection to get column names and types
                                for (i, proj) in select.projection.iter().enumerate() {
                                    let (col_name, type_oid) = match proj {
                                        sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                                            match expr {
                                                sqlparser::ast::Expr::Identifier(ident) => {
                                                    let name = ident.value.to_lowercase();
                                                    let type_oid = Self::get_catalog_column_type(&name, query);
                                                    (name, type_oid)
                                                }
                                                sqlparser::ast::Expr::CompoundIdentifier(parts) => {
                                                    let name = parts.last().map(|p| p.value.to_lowercase()).unwrap_or_else(|| "?column?".to_string());
                                                    let type_oid = Self::get_catalog_column_type(&name, query);
                                                    (name, type_oid)
                                                }
                                                _ => ("?column?".to_string(), PgType::Text.to_oid()),
                                            }
                                        }
                                        sqlparser::ast::SelectItem::ExprWithAlias { alias, expr } => {
                                            let type_oid = match expr {
                                                sqlparser::ast::Expr::Identifier(ident) => {
                                                    Self::get_catalog_column_type(&ident.value.to_lowercase(), query)
                                                }
                                                sqlparser::ast::Expr::CompoundIdentifier(parts) => {
                                                    let name = parts.last().map(|p| p.value.to_lowercase()).unwrap_or_else(|| "?column?".to_string());
                                                    Self::get_catalog_column_type(&name, query)
                                                }
                                                _ => PgType::Text.to_oid(),
                                            };
                                            (alias.value.clone(), type_oid)
                                        }
                                        _ => ("?column?".to_string(), PgType::Text.to_oid()),
                                    };
                                    
                                    fields.push(FieldDescription {
                                        name: col_name,
                                        table_oid: 0,
                                        column_id: (i + 1) as i16,
                                        type_oid,
                                        type_size: -1,
                                        type_modifier: -1,
                                        format: 0,
                                    });
                                }
                            }
                            
                            fields
                        } else {
                            Vec::new()
                        }
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                };
                
                if !field_descriptions.is_empty() {
                    info!("Sending RowDescription with {} catalog fields in Describe", field_descriptions.len());
                    
                    // Update the prepared statement with these field descriptions
                    // so they're available during Execute
                    drop(statements);
                    let mut statements_mut = session.prepared_statements.write().await;
                    if let Some(stmt_mut) = statements_mut.get_mut(&name) {
                        stmt_mut.field_descriptions = field_descriptions.clone();
                        info!("Updated statement '{}' with {} catalog field descriptions", name, field_descriptions.len());
                    }
                    drop(statements_mut);
                    
                    framed.send(BackendMessage::RowDescription(field_descriptions)).await
                        .map_err(PgSqliteError::Io)?;
                } else {
                    // Fallback to NoData if we couldn't parse the query
                    info!("Could not determine catalog fields, sending NoData in Describe");
                    framed.send(BackendMessage::NoData).await
                        .map_err(PgSqliteError::Io)?;
                }
            } else {
                info!("Sending NoData in Describe");
                framed.send(BackendMessage::NoData).await
                    .map_err(PgSqliteError::Io)?;
            }
        } else {
            // Describe portal
            let portals = session.portals.read().await;
            let portal = portals.get(&name)
                .ok_or_else(|| PgSqliteError::Protocol(format!("Unknown portal: {name}")))?;
            
            let statements = session.prepared_statements.read().await;
            let stmt = statements.get(&portal.statement_name)
                .ok_or_else(|| PgSqliteError::Protocol(format!("Unknown statement: {}", portal.statement_name)))?;
            
            if !stmt.field_descriptions.is_empty() {
                // If we have inferred parameter types, update field descriptions for parameter columns
                let mut fields = stmt.field_descriptions.clone();
                info!("Describe portal: original fields: {:?}", fields);
                // Always update field formats based on portal's result_formats, regardless of inferred types
                for (col_idx, field) in fields.iter_mut().enumerate() {
                    // Update format based on portal's result_formats for ALL fields
                    let format = if portal.result_formats.is_empty() {
                        0 // Default to text if no formats specified
                    } else if portal.result_formats.len() == 1 {
                        portal.result_formats[0] // Single format applies to all columns
                    } else if col_idx < portal.result_formats.len() {
                        portal.result_formats[col_idx] // Use column-specific format
                    } else {
                        0 // Default to text if not enough formats
                    };
                    field.format = format;
                    info!("Updated field '{}' format to {} (portal result_formats: {:?})", field.name, format, portal.result_formats);
                }
                
                if let Some(ref inferred_types) = portal.inferred_param_types {
                    info!("Describe portal: inferred types available: {:?}", inferred_types);
                    info!("Describe portal: field count: {}", fields.len());
                    
                    // For queries like SELECT $1, $2, $3, each parameter creates a column
                    // The columns might be named NULL, ?column?, or $1, $2, etc.
                    let mut param_column_count = 0;
                    
                    for (col_idx, field) in fields.iter_mut().enumerate() {
                        // Check if this is a parameter column and update type
                        if field.name == "NULL" || field.name == "?column?" || field.name.starts_with('$') {
                            // This is a parameter column, use the parameter index
                            let param_idx = if field.name.starts_with('$') {
                                // Extract parameter number from name like "$1"
                                field.name[1..].parse::<usize>().ok().map(|n| n - 1).unwrap_or(param_column_count)
                            } else {
                                // For NULL or ?column?, use sequential parameter index
                                param_column_count
                            };
                            
                            if let Some(&inferred_type) = inferred_types.get(param_idx) {
                                info!("Updating column '{}' at index {} (param {}) type from {} to {}", 
                                      field.name, col_idx, param_idx + 1, field.type_oid, inferred_type);
                                field.type_oid = inferred_type;
                            } else {
                                info!("No inferred type for column '{}' at index {} (param {})", 
                                      field.name, col_idx, param_idx + 1);
                            }
                            
                            param_column_count += 1;
                        }
                    }
                }
                info!("Describe portal: sending updated fields: {:?}", fields);
                framed.send(BackendMessage::RowDescription(fields)).await
                    .map_err(PgSqliteError::Io)?;
            } else {
                framed.send(BackendMessage::NoData).await
                    .map_err(PgSqliteError::Io)?;
            }
        }
        
        Ok(())
    }
    
    pub async fn handle_close<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        session: &Arc<SessionState>,
        typ: u8,
        name: String,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        info!("Closing {} '{}'", if typ == b'S' { "statement" } else { "portal" }, name);
        
        if typ == b'S' {
            // Close statement
            session.prepared_statements.write().await.remove(&name);
        } else {
            // Close portal
            session.portal_manager.close_portal(&name);
            session.portals.write().await.remove(&name);
        }
        
        // Send CloseComplete
        framed.send(BackendMessage::CloseComplete).await
            .map_err(PgSqliteError::Io)?;
        
        Ok(())
    }
    
    async fn try_execute_fast_path_with_params<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &Arc<DbHandler>,
        session: &Arc<SessionState>,
        portal: &str,
        query: &str,
        bound_values: &[Option<Vec<u8>>],
        param_formats: &[i16],
        param_types: &[i32],
        client_param_types: &[i32],
        fast_query: &crate::query::FastPathQuery,
        max_rows: i32,
        _execution_context: &crate::query::ExecutionContext,
    ) -> Result<Option<Result<(), PgSqliteError>>, PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // Convert parameters to rusqlite Values
        let mut rusqlite_params: Vec<rusqlite::types::Value> = Vec::new();
        for (i, value) in bound_values.iter().enumerate() {
            match value {
                Some(bytes) => {
                    let format = param_formats.get(i).unwrap_or(&0);
                    // Use client param type for binary format, schema type for text format
                    let param_type = if *format == 1 {
                        client_param_types.get(i).unwrap_or(&25) // Use client type for binary decoding
                    } else {
                        param_types.get(i).unwrap_or(&25) // Use schema type for text parsing
                    };
                    
                    match Self::convert_parameter_to_value(bytes, *format, *param_type) {
                        Ok(sql_value) => rusqlite_params.push(sql_value),
                        Err(_) => return Ok(None), // Fall back to normal path on conversion error
                    }
                }
                None => rusqlite_params.push(rusqlite::types::Value::Null),
            }
        }
        
        // Get result formats from portal
        let result_formats = {
            let portals = session.portals.read().await;
            let portal_obj = portals.get(portal).unwrap();
            portal_obj.result_formats.clone()
        };
        
        // Try fast path execution first
        if let Ok(Some(response)) = db.try_execute_fast_path_with_params(query, &rusqlite_params, &session.id).await {
            if response.columns.is_empty() {
                // DML operation - send command complete
                let tag = match fast_query.operation {
                    crate::query::FastPathOperation::Insert => format!("INSERT 0 {}", response.rows_affected),
                    crate::query::FastPathOperation::Update => format!("UPDATE {}", response.rows_affected),
                    crate::query::FastPathOperation::Delete => format!("DELETE {}", response.rows_affected),
                    _ => unreachable!(),
                };
                framed.send(BackendMessage::CommandComplete { tag }).await?;
            } else {
                // SELECT operation - check if we need to send RowDescription
                let send_row_desc = {
                    let portals = session.portals.read().await;
                    if let Some(portal_obj) = portals.get(portal) {
                        let statements = session.prepared_statements.read().await;
                        if let Some(stmt) = statements.get(&portal_obj.statement_name) {
                            stmt.field_descriptions.is_empty()
                        } else {
                            true
                        }
                    } else {
                        true
                    }
                };
                
                if send_row_desc {
                    // Send full response with RowDescription
                    Self::send_select_response(framed, response, max_rows, &result_formats, None).await?;
                } else {
                    // Just send data rows without RowDescription
                    info!("try_execute_fast_path_with_params: Not sending RowDescription (already sent during Describe)");
                    let row_count = response.rows.len();
                    Self::send_data_rows_only(framed, response, &result_formats).await?;
                    framed.send(BackendMessage::CommandComplete { 
                        tag: format!("SELECT {}", row_count) 
                    }).await?;
                }
            }
            return Ok(Some(Ok(())));
        }
        
        // Try statement pool execution for parameterized queries
        if let Ok(response) = Self::try_statement_pool_execution(db, session, query, &rusqlite_params, fast_query).await {
            if response.columns.is_empty() {
                // DML operation
                let tag = match fast_query.operation {
                    crate::query::FastPathOperation::Insert => format!("INSERT 0 {}", response.rows_affected),
                    crate::query::FastPathOperation::Update => format!("UPDATE {}", response.rows_affected),
                    crate::query::FastPathOperation::Delete => format!("DELETE {}", response.rows_affected),
                    _ => unreachable!(),
                };
                framed.send(BackendMessage::CommandComplete { tag }).await?;
            } else {
                // SELECT operation - check if we need to send RowDescription
                let send_row_desc = {
                    let portals = session.portals.read().await;
                    if let Some(portal_obj) = portals.get(portal) {
                        let statements = session.prepared_statements.read().await;
                        if let Some(stmt) = statements.get(&portal_obj.statement_name) {
                            stmt.field_descriptions.is_empty()
                        } else {
                            true
                        }
                    } else {
                        true
                    }
                };
                
                if send_row_desc {
                    // Send full response with RowDescription
                    Self::send_select_response(framed, response, max_rows, &result_formats, None).await?;
                } else {
                    // Just send data rows without RowDescription
                    info!("try_execute_fast_path_with_params (statement pool): Not sending RowDescription (already sent during Describe)");
                    let row_count = response.rows.len();
                    Self::send_data_rows_only(framed, response, &result_formats).await?;
                    framed.send(BackendMessage::CommandComplete { 
                        tag: format!("SELECT {}", row_count) 
                    }).await?;
                }
            }
            return Ok(Some(Ok(())));
        }
        
        Ok(None) // Fast path didn't work, fall back to normal execution
    }
    
    async fn try_statement_pool_execution(
        db: &Arc<DbHandler>,
        session: &Arc<SessionState>,
        query: &str,
        params: &[rusqlite::types::Value],
        fast_query: &crate::query::FastPathQuery,
    ) -> Result<crate::session::db_handler::DbResponse, PgSqliteError> {
        // Convert rusqlite values back to byte format for the statement pool methods
        let byte_params: Vec<Option<Vec<u8>>> = params.iter().map(|v| {
            match v {
                rusqlite::types::Value::Null => None,
                rusqlite::types::Value::Integer(i) => Some(i.to_string().into_bytes()),
                rusqlite::types::Value::Real(f) => Some(f.to_string().into_bytes()),
                rusqlite::types::Value::Text(s) => Some(s.clone().into_bytes()),
                rusqlite::types::Value::Blob(b) => Some(b.clone()),
            }
        }).collect();
        
        // Only try statement pool for queries without decimal columns
        // (decimal queries need rewriting which complicates caching)
        match fast_query.operation {
            crate::query::FastPathOperation::Select => {
                db.query_with_statement_pool_params(query, &byte_params, &session.id)
                    .await
                    .map_err(|e| PgSqliteError::Protocol(e.to_string()))
            }
            _ => {
                db.execute_with_statement_pool_params(query, &byte_params, &session.id)
                    .await
                    .map_err(|e| PgSqliteError::Protocol(e.to_string()))
            }
        }
    }
    
    fn convert_parameter_to_value(
        bytes: &[u8], 
        format: i16, 
        param_type: i32
    ) -> Result<rusqlite::types::Value, PgSqliteError> {
        // Convert based on format and type
        if format == 0 { // Text format
            let text = std::str::from_utf8(bytes)
                .map_err(|_| PgSqliteError::Protocol("Invalid UTF-8 in parameter".to_string()))?;
                
            // Convert based on PostgreSQL type OID
            match param_type {
                t if t == PgType::Bool.to_oid() => Ok(rusqlite::types::Value::Integer(if text == "t" || text == "true" { 1 } else { 0 })), // BOOL
                t if t == PgType::Int8.to_oid() => Ok(rusqlite::types::Value::Integer(text.parse::<i64>().map_err(|_| PgSqliteError::Protocol("Invalid int8".to_string()))?)), // INT8
                t if t == PgType::Int4.to_oid() => Ok(rusqlite::types::Value::Integer(text.parse::<i64>().map_err(|_| PgSqliteError::Protocol("Invalid int4".to_string()))?)), // INT4
                t if t == PgType::Int2.to_oid() => Ok(rusqlite::types::Value::Integer(text.parse::<i64>().map_err(|_| PgSqliteError::Protocol("Invalid int2".to_string()))?)), // INT2
                t if t == PgType::Float4.to_oid() => Ok(rusqlite::types::Value::Real(text.parse::<f64>().map_err(|_| PgSqliteError::Protocol("Invalid float4".to_string()))?)), // FLOAT4
                t if t == PgType::Float8.to_oid() => Ok(rusqlite::types::Value::Real(text.parse::<f64>().map_err(|_| PgSqliteError::Protocol("Invalid float8".to_string()))?)), // FLOAT8
                t if t == PgType::Date.to_oid() => {
                    // DATE - convert to days since epoch
                    match crate::types::ValueConverter::convert_date_to_unix(text) {
                        Ok(days_str) => Ok(rusqlite::types::Value::Integer(days_str.parse::<i64>().map_err(|_| PgSqliteError::Protocol("Invalid date days".to_string()))?)),
                        Err(e) => Err(PgSqliteError::Protocol(format!("Invalid date: {e}")))
                    }
                }
                t if t == PgType::Time.to_oid() => {
                    // TIME - convert to microseconds since midnight
                    match crate::types::ValueConverter::convert_time_to_seconds(text) {
                        Ok(micros_str) => Ok(rusqlite::types::Value::Integer(micros_str.parse::<i64>().map_err(|_| PgSqliteError::Protocol("Invalid time microseconds".to_string()))?)),
                        Err(e) => Err(PgSqliteError::Protocol(format!("Invalid time: {e}")))
                    }
                }
                t if t == PgType::Timestamp.to_oid() => {
                    // TIMESTAMP - convert to microseconds since epoch
                    match crate::types::ValueConverter::convert_timestamp_to_unix(text) {
                        Ok(micros_str) => Ok(rusqlite::types::Value::Integer(micros_str.parse::<i64>().map_err(|_| PgSqliteError::Protocol("Invalid timestamp microseconds".to_string()))?)),
                        Err(e) => Err(PgSqliteError::Protocol(format!("Invalid timestamp: {e}")))
                    }
                }
                _ => Ok(rusqlite::types::Value::Text(text.to_string())), // Default to TEXT
            }
        } else {
            // Binary format - decode based on type
            match param_type {
                t if t == PgType::Bool.to_oid() => {
                    // BOOL
                    if bytes.len() == 1 {
                        let val = bytes[0] != 0;
                        Ok(rusqlite::types::Value::Integer(if val { 1 } else { 0 }))
                    } else {
                        Err(PgSqliteError::Protocol("Invalid BOOL binary format".to_string()))
                    }
                }
                t if t == PgType::Int2.to_oid() => {
                    // INT2
                    if bytes.len() == 2 {
                        let val = i16::from_be_bytes([bytes[0], bytes[1]]) as i64;
                        Ok(rusqlite::types::Value::Integer(val))
                    } else {
                        Err(PgSqliteError::Protocol("Invalid INT2 binary format".to_string()))
                    }
                }
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
                    match crate::types::DecimalHandler::decode_numeric(bytes) {
                        Ok(decimal) => Ok(rusqlite::types::Value::Text(decimal.to_string())),
                        Err(e) => Err(PgSqliteError::Protocol(format!("Invalid binary NUMERIC: {e}"))),
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
                _ => {
                    // For unsupported binary types, store as blob
                    Ok(rusqlite::types::Value::Blob(bytes.to_vec()))
                }
            }
        }
    }
    
    async fn send_select_response<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        response: crate::session::db_handler::DbResponse,
        _max_rows: i32,
        result_formats: &[i16],
        execution_context: Option<&crate::query::ExecutionContext>,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // debug!("send_select_response called with {} columns: {:?}", response.columns.len(), response.columns);
        // Send RowDescription only if we should
        let mut field_descriptions = Vec::new();
        for (i, column_name) in response.columns.iter().enumerate() {
            let format = if result_formats.is_empty() {
                0 // Default to text if no formats specified
            } else if result_formats.len() == 1 {
                result_formats[0] // Single format applies to all columns
            } else if i < result_formats.len() {
                result_formats[i] // Use column-specific format
            } else {
                0 // Default to text if not enough formats
            };
            
            field_descriptions.push(FieldDescription {
                name: column_name.clone(),
                table_oid: 0,
                column_id: (i + 1) as i16,
                type_oid: 25, // TEXT for now - could be improved with type detection
                type_size: -1,
                type_modifier: -1,
                format,
            });
        }
        
        // Only send RowDescription if ExecutionContext allows it (or if no context provided)
        let should_send = match execution_context {
            Some(ctx) => ctx.should_send_row_description(),
            None => true, // If no context, default to sending (backward compatibility)
        };
        
        if should_send {
            info!("Sending RowDescription with {} fields", field_descriptions.len());
            framed.send(BackendMessage::RowDescription(field_descriptions.clone())).await?;
        } else {
            info!("Not sending RowDescription (already sent)");
        }
        
        // Send DataRows
        // Check if binary format is requested
        let needs_encoding = result_formats.iter().any(|&f| f == 1);
        
        if needs_encoding {
            // Binary format requested - use optimized batch encoding
            // Get field types from field descriptions we just sent
            let field_types: Vec<i32> = field_descriptions.iter()
                .map(|f| f.type_oid)
                .collect();
            
            // Use optimized batch encoder
            let (encoded_rows, _encoder) = Self::encode_rows_optimized(&response.rows, result_formats, &field_types)?;
            
            // Send all encoded rows
            for encoded_row in encoded_rows {
                framed.send(BackendMessage::DataRow(encoded_row)).await?;
            }
        } else {
            // Text format - apply datetime formatting but no binary encoding
            for row in response.rows {
                let mut values = Vec::new();
                for (i, cell) in row.iter().enumerate() {
                    if let Some(bytes) = cell {
                        let column_name = response.columns.get(i).map(|s| s.as_str()).unwrap_or("unknown");
                        let column_lower = column_name.to_lowercase();
            // debug!("Processing column '{}' (lowercase: '{}')", column_name, column_lower);
                        // Check if this is a datetime function result that needs formatting
                        if let Ok(s) = String::from_utf8(bytes.clone()) {
            // debug!("Column '{}' value as string: '{}'", column_name, s);
                            if let Ok(micros) = s.parse::<i64>() {
            // debug!("Column '{}' parsed as i64: {}", column_name, micros);
                                // Check if this looks like microseconds (large integer)
                                if micros > 1_000_000_000_000 && 
                                   (column_lower.contains("now") || 
                                    column_lower.contains("current_timestamp") ||
                                    column_lower == "now" ||
                                    column_lower == "current_timestamp") {
                                    // This is likely a datetime function result, format it
                                    use crate::types::datetime_utils::format_microseconds_to_timestamp;
                                    let formatted = format_microseconds_to_timestamp(micros);
            // debug!("Converting datetime function result {} to formatted timestamp: {}", micros, formatted);
                                    values.push(Some(formatted.into_bytes()));
                                } else {
            // debug!("Column '{}' not converted: micros={}, contains_now={}, contains_current_timestamp={}, eq_now={}, eq_current_timestamp={}", 
            //                                column_name, micros, column_lower.contains("now"), column_lower.contains("current_timestamp"), 
            //                                column_lower == "now", column_lower == "current_timestamp");
                                    values.push(cell.clone());
                                }
                            } else {
            // debug!("Column '{}' failed to parse as i64", column_name);
                                values.push(cell.clone());
                            }
                        } else {
            // debug!("Column '{}' failed to parse as UTF-8", column_name);
                            values.push(cell.clone());
                        }
                    } else {
                        values.push(cell.clone());
                    }
                }
                framed.send(BackendMessage::DataRow(values)).await?;
            }
        }
        
        // Send CommandComplete
        framed.send(BackendMessage::CommandComplete { tag: format!("SELECT {}", response.rows_affected) }).await?;
        
        Ok(())
    }
    
    fn substitute_parameters(query: &str, values: &[Option<Vec<u8>>], formats: &[i16], param_types: &[i32], client_param_types: &[i32]) -> Result<String, PgSqliteError> {
        // Convert parameter values to strings for substitution
        let mut string_values = Vec::new();
        
        for (i, value) in values.iter().enumerate() {
            let format = formats.get(i).copied().unwrap_or(0); // Default to text format
            // Use client param type for binary format, schema type for text format
            let param_type = if format == 1 {
                client_param_types.get(i).copied().unwrap_or(PgType::Text.to_oid())
            } else {
                param_types.get(i).copied().unwrap_or(PgType::Text.to_oid())
            };
            
            let replacement = match value {
                None => "NULL".to_string(),
                Some(bytes) => {
                    if format == 1 {
                        // Binary format - decode based on expected type
                        match param_type {
                            t if t == PgType::Int2.to_oid() => {
                                // int2
                                if bytes.len() == 2 {
                                    let value = i16::from_be_bytes([bytes[0], bytes[1]]);
                                    debug!("Decoded binary int16 parameter {}: {}", i + 1, value);
                                    value.to_string()
                                } else {
                                    format!("X'{}'", hex::encode(bytes))
                                }
                            }
                            t if t == PgType::Int4.to_oid() => {
                                // int4
                                if bytes.len() == 4 {
                                    let value = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                                    debug!("Decoded binary int32 parameter {}: {}", i + 1, value);
                                    value.to_string()
                                } else {
                                    format!("X'{}'", hex::encode(bytes))
                                }
                            }
                            t if t == PgType::Int8.to_oid() => {
                                // int8
                                if bytes.len() == 8 {
                                    let value = i64::from_be_bytes([
                                        bytes[0], bytes[1], bytes[2], bytes[3],
                                        bytes[4], bytes[5], bytes[6], bytes[7]
                                    ]);
                                    debug!("Decoded binary int64 parameter {}: {}", i + 1, value);
                                    value.to_string()
                                } else {
                                    format!("X'{}'", hex::encode(bytes))
                                }
                            }
                            t if t == PgType::Money.to_oid() => {
                                // money - binary format is int8 cents
                                if bytes.len() == 8 {
                                    let cents = i64::from_be_bytes([
                                        bytes[0], bytes[1], bytes[2], bytes[3],
                                        bytes[4], bytes[5], bytes[6], bytes[7]
                                    ]);
                                    let dollars = cents as f64 / 100.0;
                                    let formatted = format!("'${dollars:.2}'");
                                    debug!("Decoded binary money parameter {}: {} cents -> {}", i + 1, cents, formatted);
                                    formatted
                                } else {
                                    format!("X'{}'", hex::encode(bytes))
                                }
                            }
                            t if t == PgType::Numeric.to_oid() => {
                                // numeric - decode binary format
                                match DecimalHandler::decode_numeric(bytes) {
                                    Ok(decimal) => {
                                        let s = decimal.to_string();
                                        debug!("Decoded binary numeric parameter {}: {}", i + 1, s);
                                        format!("'{}'", s.replace('\'', "''"))
                                    }
                                    Err(e) => {
        // debug!("Failed to decode binary NUMERIC parameter: {}", e);
                                        return Err(PgSqliteError::InvalidParameter(format!("Invalid binary NUMERIC: {e}")));
                                    }
                                }
                            }
                            t if t == PgType::Text.to_oid() || t == PgType::Varchar.to_oid() => {
                                // TEXT/VARCHAR in binary format is just UTF-8 bytes
                                match String::from_utf8(bytes.clone()) {
                                    Ok(s) => {
                                        
                                        format!("'{}'", s.replace('\'', "''"))
                                    }
                                    Err(_) => {
                                        // Invalid UTF-8, treat as blob
                                        info!("Failed to decode as UTF-8, treating as blob. Hex: {}", hex::encode(bytes));
                                        format!("X'{}'", hex::encode(bytes))
                                    }
                                }
                            }
                            _ => {
                                // Other binary data - treat as blob
                                format!("X'{}'", hex::encode(bytes))
                            }
                        }
                    } else {
                        // Text format - interpret as UTF-8 string
                        match String::from_utf8(bytes.clone()) {
                            Ok(s) => {
                                // Check parameter type to determine handling
                                match param_type {
                                    t if t == PgType::Int4.to_oid() || t == PgType::Int8.to_oid() || t == PgType::Int2.to_oid() || 
                                         t == PgType::Float4.to_oid() || t == PgType::Float8.to_oid() => {
                                        // Integer and float types - use as-is if valid number
                                        if s.parse::<i64>().is_ok() || s.parse::<f64>().is_ok() {
                                            s
                                        } else {
                                            format!("'{}'", s.replace('\'', "''"))
                                        }
                                    }
                                    t if t == PgType::Money.to_oid() => {
                                        // MONEY type - always quote
                                        format!("'{}'", s.replace('\'', "''"))
                                    }
                                    t if t == PgType::Numeric.to_oid() => {
                                        // NUMERIC type - validate and quote
                                        match DecimalHandler::validate_numeric_string(&s) {
                                            Ok(_) => {
                                                // Valid numeric value - quote it for SQLite TEXT storage
                                                format!("'{}'", s.replace('\'', "''"))
                                            }
                                            Err(e) => {
        // debug!("Invalid NUMERIC parameter: {}", e);
                                                return Err(PgSqliteError::InvalidParameter(format!("Invalid NUMERIC value: {e}")));
                                            }
                                        }
                                    }
                                    t if t == PgType::Timestamp.to_oid() || t == PgType::Timestamptz.to_oid() => {
                                        // TIMESTAMP types - convert to Unix timestamp
                                        match crate::types::ValueConverter::convert_timestamp_to_unix(&s) {
                                            Ok(unix_timestamp) => unix_timestamp,
                                            Err(e) => {
        // debug!("Invalid TIMESTAMP parameter: {}", e);
                                                return Err(PgSqliteError::InvalidParameter(format!("Invalid TIMESTAMP value: {e}")));
                                            }
                                        }
                                    }
                                    t if t == PgType::Date.to_oid() => {
                                        // DATE type - convert to Unix timestamp
                                        match crate::types::ValueConverter::convert_date_to_unix(&s) {
                                            Ok(unix_timestamp) => unix_timestamp,
                                            Err(e) => {
        // debug!("Invalid DATE parameter: {}", e);
                                                return Err(PgSqliteError::InvalidParameter(format!("Invalid DATE value: {e}")));
                                            }
                                        }
                                    }
                                    t if t == PgType::Time.to_oid() || t == PgType::Timetz.to_oid() => {
                                        // TIME types - convert to seconds since midnight
                                        match crate::types::ValueConverter::convert_time_to_seconds(&s) {
                                            Ok(seconds) => seconds,
                                            Err(e) => {
        // debug!("Invalid TIME parameter: {}", e);
                                                return Err(PgSqliteError::InvalidParameter(format!("Invalid TIME value: {e}")));
                                            }
                                        }
                                    }
                                    _ => {
                                        // For other types, check if it's a plain number
                                        if s.parse::<i64>().is_ok() || s.parse::<f64>().is_ok() {
                                            s // Use as-is for numeric values
                                        } else {
                                            // Quote string values
                                            format!("'{}'", s.replace('\'', "''"))
                                        }
                                    }
                                }
                            }
                            Err(_) => {
                                // Invalid UTF-8 - treat as blob
                                format!("X'{}'", hex::encode(bytes))
                            }
                        }
                    }
                }
            };
            string_values.push(replacement);
        }
        
        // Use the proper parameter parser that respects string literals
        let result = ParameterParser::substitute_parameters(query, &string_values)
            .map_err(|e| PgSqliteError::InvalidParameter(format!("Parameter substitution error: {e}")))?;
        
        // Remove PostgreSQL-style casts (::type) as SQLite doesn't support them
        // Be careful not to match IPv6 addresses like ::1
        let cast_regex = regex::Regex::new(r"::[a-zA-Z]\w*").unwrap();
        let result = cast_regex.replace_all(&result, "").to_string();
        
        Ok(result)
    }
    
    // PostgreSQL epoch is 2000-01-01 00:00:00
    const _PG_EPOCH: i64 = 946684800; // Unix timestamp for 2000-01-01
    
    // Helper function to get the PostgreSQL type OID for a catalog column
    fn get_catalog_column_type(column_name: &str, query: &str) -> i32 {
        // OID type constant (not in PgType enum)
        const OID_TYPE: i32 = 26;
        const XID_TYPE: i32 = 28;
        const ACLITEM_ARRAY_TYPE: i32 = 1034;
        const TEXT_ARRAY_TYPE: i32 = 1009;
        const PG_NODE_TREE_TYPE: i32 = 194;
        
        // Determine which catalog table based on query
        if query.contains("pg_class") {
            match column_name {
                "oid" | "relnamespace" | "reltype" | "reloftype" | "relowner" | "relam" | "relfilenode" | 
                "reltablespace" | "reltoastrelid" | "relrewrite" => OID_TYPE,
                "relname" => PgType::Text.to_oid(),
                "relpages" | "relallvisible" => PgType::Int4.to_oid(),
                "reltuples" => PgType::Float4.to_oid(),
                "relhasindex" | "relisshared" | "relhasrules" | "relhastriggers" | 
                "relhassubclass" | "relrowsecurity" | "relforcerowsecurity" | 
                "relispopulated" | "relispartition" => PgType::Bool.to_oid(),
                "relpersistence" | "relkind" | "relreplident" => PgType::Char.to_oid(),
                "relnatts" | "relchecks" => PgType::Int2.to_oid(),
                "relfrozenxid" | "relminmxid" => XID_TYPE,
                "relacl" => ACLITEM_ARRAY_TYPE,
                "reloptions" => TEXT_ARRAY_TYPE,
                "relpartbound" => PG_NODE_TREE_TYPE,
                _ => PgType::Text.to_oid(),
            }
        } else if query.contains("pg_attribute") {
            match column_name {
                "attrelid" | "atttypid" | "attcollation" => OID_TYPE,
                "attname" | "attacl" | "attoptions" | "attfdwoptions" | "attmissingval" => PgType::Text.to_oid(),
                "attstattarget" | "attndims" | "attcacheoff" | "atttypmod" | "attinhcount" => PgType::Int4.to_oid(),
                "attlen" | "attnum" => PgType::Int2.to_oid(),
                "attbyval" | "attnotnull" | "atthasdef" | "atthasmissing" | "attisdropped" | "attislocal" => PgType::Bool.to_oid(),
                "attalign" | "attstorage" | "attcompression" | "attidentity" | "attgenerated" => PgType::Char.to_oid(),
                _ => PgType::Text.to_oid(),
            }
        } else if query.contains("pg_type") {
            match column_name {
                "oid" | "typnamespace" | "typowner" | "typrelid" | "typelem" | "typarray" | 
                "typinput" | "typoutput" | "typreceive" | "typsend" | "typmodin" | 
                "typmodout" | "typanalyze" | "typbasetype" | "typcollation" => OID_TYPE,
                "typname" | "typdefault" | "typacl" => PgType::Text.to_oid(),
                "typlen" => PgType::Int2.to_oid(),
                "typmod" | "typndims" => PgType::Int4.to_oid(),
                "typbyval" | "typisdefined" | "typnotnull" => PgType::Bool.to_oid(),
                "typtype" | "typcategory" | "typalign" | "typstorage" | "typdelim" => PgType::Char.to_oid(),
                _ => PgType::Text.to_oid(),
            }
        } else if query.contains("pg_namespace") {
            match column_name {
                "oid" | "nspowner" => OID_TYPE,
                "nspname" | "nspacl" => PgType::Text.to_oid(),
                _ => PgType::Text.to_oid(),
            }
        } else {
            // Default to text for unknown catalog tables
            PgType::Text.to_oid()
        }
    }
    
    // Convert date string to days since PostgreSQL epoch
    fn date_to_pg_days(date_str: &str) -> Option<i32> {
        if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
            let pg_epoch = NaiveDate::from_ymd_opt(2000, 1, 1)?;
            let days = (date - pg_epoch).num_days() as i32;
            Some(days)
        } else {
            None
        }
    }
    
    
    // Convert time string to microseconds since midnight
    fn time_to_microseconds(time_str: &str) -> Option<i64> {
        // Try different time formats
        let formats = ["%H:%M:%S%.f", "%H:%M:%S", "%H:%M"];
        for format in &formats {
            if let Ok(time) = NaiveTime::parse_from_str(time_str, format) {
                let micros = time.num_seconds_from_midnight() as i64 * 1_000_000 
                           + (time.nanosecond() as i64 / 1000);
                return Some(micros);
            }
        }
        None
    }
    
    // Convert timestamp string to microseconds since PostgreSQL epoch
    fn timestamp_to_pg_microseconds(timestamp_str: &str) -> Option<i64> {
        // Try different timestamp formats
        let formats = [
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S",
        ];
        
        for format in &formats {
            if let Ok(dt) = NaiveDateTime::parse_from_str(timestamp_str, format) {
                let pg_epoch = NaiveDate::from_ymd_opt(2000, 1, 1)?.and_hms_opt(0, 0, 0)?;
                let duration = dt - pg_epoch;
                let micros = duration.num_microseconds()?;
                return Some(micros);
            }
        }
        None
    }
    
    // Parse MAC address to bytes
    fn parse_macaddr(mac_str: &str) -> Option<Vec<u8>> {
        let cleaned = mac_str.replace([':', '-'], "");
        if cleaned.len() == 12 {
            let mut bytes = Vec::with_capacity(6);
            for i in 0..6 {
                let byte_str = &cleaned[i*2..i*2+2];
                if let Ok(byte) = u8::from_str_radix(byte_str, 16) {
                    bytes.push(byte);
                } else {
                    return None;
                }
            }
            Some(bytes)
        } else {
            None
        }
    }
    
    // Parse MAC address (8 bytes) to bytes
    fn parse_macaddr8(mac_str: &str) -> Option<Vec<u8>> {
        let cleaned = mac_str.replace([':', '-'], "");
        if cleaned.len() == 16 {
            let mut bytes = Vec::with_capacity(8);
            for i in 0..8 {
                let byte_str = &cleaned[i*2..i*2+2];
                if let Ok(byte) = u8::from_str_radix(byte_str, 16) {
                    bytes.push(byte);
                } else {
                    return None;
                }
            }
            Some(bytes)
        } else {
            None
        }
    }
    
    // Parse IPv4/IPv6 address for CIDR/INET types
    fn parse_inet(addr_str: &str) -> Option<Vec<u8>> {
        use std::net::IpAddr;
        
        // Split address and netmask if present
        let parts: Vec<&str> = addr_str.split('/').collect();
        let ip_str = parts[0];
        let bits = if parts.len() > 1 {
            parts[1].parse::<u8>().ok()?
        } else {
            // Default netmask
            match ip_str.parse::<IpAddr>().ok()? {
                IpAddr::V4(_) => 32,
                IpAddr::V6(_) => 128,
            }
        };
        
        // Parse IP address
        match ip_str.parse::<IpAddr>().ok()? {
            IpAddr::V4(addr) => {
                let mut result = Vec::with_capacity(8);
                result.push(2); // AF_INET
                result.push(bits); // bits
                result.push(0); // is_cidr (0 for INET, 1 for CIDR)
                result.push(4); // nb (number of bytes)
                result.extend_from_slice(&addr.octets());
                Some(result)
            }
            IpAddr::V6(addr) => {
                let mut result = Vec::with_capacity(20);
                result.push(3); // AF_INET6
                result.push(bits); // bits
                result.push(0); // is_cidr
                result.push(16); // nb
                result.extend_from_slice(&addr.octets());
                Some(result)
            }
        }
    }
    
    
    // Parse bit string
    fn parse_bit_string(bit_str: &str) -> Option<Vec<u8>> {
        // Remove B prefix if present (e.g., B'101010')
        let cleaned = bit_str.trim_start_matches("B'").trim_start_matches("b'").trim_end_matches('\'');
        
        // Count bits
        let bit_count = cleaned.len() as i32;
        
        // Convert to bytes
        let mut bytes = Vec::new();
        let mut current_byte = 0u8;
        let mut bit_pos = 0;
        
        for ch in cleaned.chars() {
            match ch {
                '0' => {
                    current_byte <<= 1;
                    bit_pos += 1;
                }
                '1' => {
                    current_byte = (current_byte << 1) | 1;
                    bit_pos += 1;
                }
                _ => return None, // Invalid character
            }
            
            if bit_pos == 8 {
                bytes.push(current_byte);
                current_byte = 0;
                bit_pos = 0;
            }
        }
        
        // Handle remaining bits
        if bit_pos > 0 {
            current_byte <<= 8 - bit_pos;
            bytes.push(current_byte);
        }
        
        // Prepend length
        let mut result = (bit_count as i32).to_be_bytes().to_vec();
        result.extend_from_slice(&bytes);
        
        Some(result)
    }
    
    // Range type flags
    const RANGE_EMPTY: u8 = 0x01;
    const RANGE_LB_INC: u8 = 0x02;
    const RANGE_UB_INC: u8 = 0x04;
    const RANGE_LB_INF: u8 = 0x08;
    const RANGE_UB_INF: u8 = 0x10;
    
    // Encode range types
    fn encode_range(range_str: &str, element_type: i32) -> Option<Vec<u8>> {
        // Parse range format: [lower,upper), (lower,upper], [lower,upper], (lower,upper), empty
        let trimmed = range_str.trim();
        
        // Handle empty range
        if trimmed.eq_ignore_ascii_case("empty") {
            return Some(vec![Self::RANGE_EMPTY]);
        }
        
        // Parse bounds and inclusivity
        if trimmed.len() < 3 {
            return None; // Too short to be valid
        }
        
        let lower_inc = trimmed.starts_with('[');
        let upper_inc = trimmed.ends_with(']');
        
        // Remove brackets/parentheses
        let inner = &trimmed[1..trimmed.len()-1];
        
        // Split by comma
        let parts: Vec<&str> = inner.split(',').collect();
        if parts.len() != 2 {
            return None; // Invalid format
        }
        
        let lower_str = parts[0].trim();
        let upper_str = parts[1].trim();
        
        // Calculate flags
        let mut flags = 0u8;
        if lower_inc {
            flags |= Self::RANGE_LB_INC;
        }
        if upper_inc {
            flags |= Self::RANGE_UB_INC;
        }
        
        // Check for infinity bounds
        let lower_inf = lower_str.is_empty() || lower_str == "-infinity";
        let upper_inf = upper_str.is_empty() || upper_str == "infinity";
        
        if lower_inf {
            flags |= Self::RANGE_LB_INF;
        }
        if upper_inf {
            flags |= Self::RANGE_UB_INF;
        }
        
        let mut result = vec![flags];
        
        // Encode lower bound if not infinite
        if !lower_inf {
            let lower_bytes = match element_type {
                t if t == PgType::Int4.to_oid() => {
                    // int4
                    if let Ok(val) = lower_str.parse::<i32>() {
                        val.to_be_bytes().to_vec()
                    } else {
                        return None;
                    }
                }
                t if t == PgType::Int8.to_oid() => {
                    // int8
                    if let Ok(val) = lower_str.parse::<i64>() {
                        val.to_be_bytes().to_vec()
                    } else {
                        return None;
                    }
                }
                t if t == PgType::Numeric.to_oid() => {
                    // numeric
                    match DecimalHandler::parse_decimal(lower_str) {
                        Ok(decimal) => DecimalHandler::encode_numeric(&decimal),
                        Err(_e) => return None,
                    }
                }
                _ => return None, // Unsupported element type
            };
            
            // Add length header and data
            result.extend_from_slice(&(lower_bytes.len() as i32).to_be_bytes());
            result.extend_from_slice(&lower_bytes);
        }
        
        // Encode upper bound if not infinite
        if !upper_inf {
            let upper_bytes = match element_type {
                t if t == PgType::Int4.to_oid() => {
                    // int4
                    if let Ok(val) = upper_str.parse::<i32>() {
                        val.to_be_bytes().to_vec()
                    } else {
                        return None;
                    }
                }
                t if t == PgType::Int8.to_oid() => {
                    // int8
                    if let Ok(val) = upper_str.parse::<i64>() {
                        val.to_be_bytes().to_vec()
                    } else {
                        return None;
                    }
                }
                t if t == PgType::Numeric.to_oid() => {
                    // numeric
                    match DecimalHandler::parse_decimal(upper_str) {
                        Ok(decimal) => DecimalHandler::encode_numeric(&decimal),
                        Err(_e) => return None,
                    }
                }
                _ => return None, // Unsupported element type
            };
            
            // Add length header and data
            result.extend_from_slice(&(upper_bytes.len() as i32).to_be_bytes());
            result.extend_from_slice(&upper_bytes);
        }
        
        Some(result)
    }
    
    fn encode_row(
        row: &[Option<Vec<u8>>],
        result_formats: &[i16],
        field_types: &[i32],
    ) -> Result<Vec<Option<Vec<u8>>>, PgSqliteError> {
        // debug!("encode_row called with {} fields, result_formats: {:?}, field_types: {:?}", row.len(), result_formats, field_types);
        
        // Log the first few values for debugging
        for (_i, value) in row.iter().take(3).enumerate() {
            if let Some(bytes) = value {
                if let Ok(_s) = std::str::from_utf8(bytes) {
        // debug!("  Field {}: '{}' (type OID {})", i, s, field_types.get(i).unwrap_or(&0));
                } else {
        // debug!("  Field {}: <binary data> (type OID {})", i, field_types.get(i).unwrap_or(&0));
                }
            } else {
        // debug!("  Field {}: NULL (type OID {})", i, field_types.get(i).unwrap_or(&0));
            }
        }
        
        let mut encoded_row = Vec::new();
        
        for (i, value) in row.iter().enumerate() {
            // If result_formats has only one element, it applies to all columns
            let format = if result_formats.len() == 1 {
                result_formats[0]
            } else {
                result_formats.get(i).copied().unwrap_or(0)
            };
            let type_oid = field_types.get(i).copied().unwrap_or(PgType::Text.to_oid());
            
            
            let encoded_value = match value {
                None => None,
                Some(bytes) => {
                    if format == 1 {
                        // Binary format requested
                        match type_oid {
                            t if t == PgType::Bool.to_oid() => {
                                // bool - convert text to binary
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    let val = match s.trim() {
                                        "1" | "t" | "true" | "TRUE" | "T" => true,
                                        "0" | "f" | "false" | "FALSE" | "F" => false,
                                        _ => {
                                            // Invalid boolean, keep as text
                                            encoded_row.push(Some(bytes.clone()));
                                            continue;
                                        }
                                    };
                                    Some(BinaryEncoder::encode_bool(val))
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::Int2.to_oid() => {
                                // int2 - convert text to binary
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Ok(val) = s.parse::<i16>() {
                                        Some(BinaryEncoder::encode_int2(val))
                                    } else {
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::Int4.to_oid() => {
                                // int4 - convert text to binary
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Ok(val) = s.trim().parse::<i32>() {
                                        Some(BinaryEncoder::encode_int4(val))
                                    } else {
                                        // Invalid integer, return as error
                                        info!("Invalid INT4 value: '{}'", s);
                                        Some(BinaryEncoder::encode_int4(0)) // Default to 0
                                    }
                                } else {
                                    // Invalid UTF-8 for integer
                                    info!("Invalid UTF-8 for INT4: {:?}", bytes);
                                    Some(BinaryEncoder::encode_int4(0)) // Default to 0
                                }
                            }
                            t if t == PgType::Int8.to_oid() => {
                                // int8 - convert text to binary
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Ok(val) = s.parse::<i64>() {
                                        Some(BinaryEncoder::encode_int8(val))
                                    } else {
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::Float4.to_oid() => {
                                // float4 - convert text to binary
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Ok(val) = s.trim().parse::<f32>() {
                                        Some(BinaryEncoder::encode_float4(val))
                                    } else {
                                        // Invalid float, return as NaN
                                        info!("Invalid FLOAT4 value: '{}'", s);
                                        Some(BinaryEncoder::encode_float4(0.0)) // Default to 0.0
                                    }
                                } else {
                                    // Invalid UTF-8 for float
                                    info!("Invalid UTF-8 for FLOAT4: {:?}", bytes);
                                    Some(BinaryEncoder::encode_float4(0.0)) // Default to 0.0
                                }
                            }
                            t if t == PgType::Float8.to_oid() => {
                                // float8 - convert text to binary
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Ok(val) = s.parse::<f64>() {
                                        Some(BinaryEncoder::encode_float8(val))
                                    } else {
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            // NOTE: Array type handling removed because:
                            // 1. Arrays are stored as JSON strings in SQLite
                            // 2. We return them as TEXT type to clients
                            // 3. Binary array encoding is not implemented
                            t if t == PgType::Uuid.to_oid() => {
                                // uuid - convert text to binary (16 bytes)
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Ok(uuid_bytes) = crate::types::uuid::UuidHandler::uuid_to_bytes(&s) {
                                        Some(uuid_bytes)
                                    } else {
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            // Date types
                            t if t == PgType::Date.to_oid() => {
                                // date - days since 2000-01-01 as int4
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    // Check if this is already an integer (days since 1970)
                                    if let Ok(days_since_1970) = s.parse::<i32>() {
                                        // Use BinaryEncoder which handles the conversion
                                        Some(BinaryEncoder::encode_date(days_since_1970 as f64))
                                    } else if let Some(days) = Self::date_to_pg_days(&s) {
                                        // Handle date strings like "2025-01-01" 
                                        Some(days.to_be_bytes().to_vec())
                                    } else {
                                        // If parsing fails, keep as text
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::Time.to_oid() => {
                                // time - microseconds since midnight as int8
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    // First check if this is already an integer (microseconds since midnight)
                                    if let Ok(micros) = s.parse::<i64>() {
                                        // Already in microseconds format
                                        Some(BinaryEncoder::encode_time(micros as f64))
                                    } else if let Some(micros) = Self::time_to_microseconds(&s) {
                                        Some(BinaryEncoder::encode_time(micros as f64))
                                    } else {
                                        // If parsing fails, keep as text
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::Timestamp.to_oid() || t == PgType::Timestamptz.to_oid() => {
                                // timestamp/timestamptz - microseconds since 2000-01-01 as int8
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    // First check if this is already an integer (microseconds since Unix epoch)
                                    if let Ok(unix_micros) = s.parse::<i64>() {
                                        // Convert from Unix epoch (1970-01-01) to PostgreSQL epoch (2000-01-01)
                                        // 946684800 seconds = 30 years between epochs
                                        Some(BinaryEncoder::encode_timestamp(unix_micros as f64))
                                    } else if let Some(micros) = Self::timestamp_to_pg_microseconds(&s) {
                                        Some(micros.to_be_bytes().to_vec())
                                    } else {
                                        // If parsing fails, keep as text
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            // Numeric type - use proper PostgreSQL binary encoding
                            t if t == PgType::Numeric.to_oid() => {
                                // Use our improved NUMERIC binary encoder
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Ok(decimal) = rust_decimal::Decimal::from_str(&s) {
                                        Some(BinaryEncoder::encode_numeric(&decimal))
                                    } else {
                                        // If parsing fails, keep as text
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            // Money type
                            t if t == PgType::Money.to_oid() => {
                                // money - int8 representing cents (amount * 100)
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    // Remove currency symbols and convert to cents
                                    let cleaned = s.trim_start_matches('$').replace(',', "");
                                    if let Ok(val) = cleaned.parse::<f64>() {
                                        let cents = (val * 100.0).round() as i64;
                                        Some(cents.to_be_bytes().to_vec())
                                    } else {
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            // Network types
                            t if t == PgType::Cidr.to_oid() || t == PgType::Inet.to_oid() => {
                                // cidr/inet - family(1) + bits(1) + is_cidr(1) + nb(1) + address bytes
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Some(inet_bytes) = Self::parse_inet(&s) {
                                        Some(inet_bytes)
                                    } else {
                                        // If parsing fails, keep as text
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::Macaddr.to_oid() => {
                                // macaddr - 6 bytes
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Some(mac_bytes) = Self::parse_macaddr(&s) {
                                        Some(mac_bytes)
                                    } else {
                                        // If parsing fails, keep as text
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::Macaddr8.to_oid() => {
                                // macaddr8 - 8 bytes
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Some(mac_bytes) = Self::parse_macaddr8(&s) {
                                        Some(mac_bytes)
                                    } else {
                                        // If parsing fails, keep as text
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            // Bit string types
                            t if t == PgType::Bit.to_oid() || t == PgType::Varbit.to_oid() => {
                                // bit/varbit - length(int4) + bit data
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Some(bit_bytes) = Self::parse_bit_string(&s) {
                                        Some(bit_bytes)
                                    } else {
                                        // If parsing fails, keep as text
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            // Range types
                            t if t == PgType::Int4range.to_oid() => {
                                // int4range
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Some(range_bytes) = Self::encode_range(&s, PgType::Int4.to_oid()) {
                                        Some(range_bytes)
                                    } else {
                                        // If parsing fails, keep as text
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::Int8range.to_oid() => {
                                // int8range
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Some(range_bytes) = Self::encode_range(&s, PgType::Int8.to_oid()) {
                                        Some(range_bytes)
                                    } else {
                                        // If parsing fails, keep as text
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::Numrange.to_oid() => {
                                // numrange
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Some(range_bytes) = Self::encode_range(&s, PgType::Numeric.to_oid()) {
                                        Some(range_bytes)
                                    } else {
                                        // If parsing fails, keep as text
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            // Text types - ensure valid UTF-8 in binary format
                            t if t == PgType::Text.to_oid() || t == PgType::Varchar.to_oid() || t == PgType::Char.to_oid() => {
                                // text/varchar/char - UTF-8 encoded text
                                // Validate UTF-8 encoding to prevent client decode errors
                                if let Ok(_) = std::str::from_utf8(bytes) {
                                    Some(bytes.clone())
                                } else {
                                    // Invalid UTF-8, convert to error message
                                    Some(b"<invalid UTF-8 data>".to_vec())
                                }
                            }
                            // JSON types
                            t if t == PgType::Json.to_oid() => {
                                // json - UTF-8 encoded JSON text
                                Some(bytes.clone())
                            }
                            t if t == PgType::Jsonb.to_oid() => {
                                // jsonb - version byte (1) + UTF-8 encoded JSON text
                                let mut result = vec![1u8]; // Version 1
                                result.extend_from_slice(bytes);
                                Some(result)
                            }
                            // Bytea - already binary
                            t if t == PgType::Bytea.to_oid() => {
                                // bytea - raw bytes
                                Some(bytes.clone())
                            }
                            // Small integers
                            t if t == PgType::Int2.to_oid() => {
                                // int2 - convert text to binary
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Ok(val) = s.parse::<i16>() {
                                        Some(BinaryEncoder::encode_int2(val))
                                    } else {
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            _ => {
                                // For unknown types, validate UTF-8 and keep as text
                                if let Ok(_) = std::str::from_utf8(bytes) {
                                    Some(bytes.clone())
                                } else {
                                    // Invalid UTF-8, convert to error message
                                    info!("Invalid UTF-8 for unknown type OID {}: {:?}", type_oid, bytes);
                                    Some(format!("<invalid UTF-8 data for type {}>", type_oid).into_bytes())
                                }
                            }
                        }
                    } else {
                        // Text format
                        match type_oid {
                            t if t == PgType::Bool.to_oid() => {
                                // bool - convert SQLite's 0/1 to PostgreSQL's f/t format
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    let pg_bool_str = match s.trim() {
                                        "0" => "f",
                                        "1" => "t",
                                        // Already in PostgreSQL format or other values
                                        "f" | "t" | "false" | "true" => &s,
                                        _ => &s, // Keep unknown values as-is
                                    };
                                    Some(pg_bool_str.as_bytes().to_vec())
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            // Timestamp types - convert from INTEGER microseconds to formatted string
                            t if t == PgType::Timestamp.to_oid() || t == PgType::Timestamptz.to_oid() => {
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    // Check if this is already an integer (microseconds since epoch)
                                    if let Ok(micros) = s.parse::<i64>() {
                                        // Convert microseconds to formatted timestamp
                                        use crate::types::datetime_utils::format_microseconds_to_timestamp;
                                        let formatted = format_microseconds_to_timestamp(micros);
                                        Some(formatted.into_bytes())
                                    } else {
                                        // Already formatted or invalid, keep as-is
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            // NOTE: Array type handling removed for text format too
                            // Arrays are returned as JSON strings with TEXT type
                            t if t == PgType::Text.to_oid() => {
                                // Check if this is a datetime function result (integer microseconds that should be formatted)
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Ok(micros) = s.parse::<i64>() {
                                        // Check if this looks like microseconds (large integer)
                                        if micros > 1_000_000_000_000 { // > year 2001 in microseconds
                                            // This is likely a datetime function result, format it
                                            use crate::types::datetime_utils::format_microseconds_to_timestamp;
                                            let formatted = format_microseconds_to_timestamp(micros);
        // debug!("Converting datetime function result {} to formatted timestamp: {}", micros, formatted);
                                            Some(formatted.into_bytes())
                                        } else {
                                            Some(bytes.clone())
                                        }
                                    } else {
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            _ => {
                                // For other types, keep as-is
                                Some(bytes.clone())
                            }
                        }
                    }
                }
            };
            
            encoded_row.push(encoded_value);
        }
        
        Ok(encoded_row)
    }
    
    /// Optimized batch encoding for multiple rows using zero-copy encoder
    fn encode_rows_optimized(
        rows: &[Vec<Option<Vec<u8>>>],
        result_formats: &[i16],
        field_types: &[i32],
    ) -> Result<(Vec<Vec<Option<Vec<u8>>>>, BinaryResultEncoder), PgSqliteError> {
        let num_rows = rows.len();
        let num_cols = if rows.is_empty() { 0 } else { rows[0].len() };
        
        // Create encoder with pre-allocated buffer
        let mut encoder = BinaryResultEncoder::new(num_rows, num_cols);
        let mut encoded_rows = Vec::with_capacity(num_rows);
        
        // Encode all rows
        for row in rows {
            let encoded_row = encoder.encode_row(row, result_formats, field_types)?;
            encoded_rows.push(encoded_row);
        }
        
        // Log statistics
        let (size, capacity, row_count) = encoder.stats();
        debug!("Binary encoding stats: {} bytes used, {} capacity, {} rows", size, capacity, row_count);
        
        Ok((encoded_rows, encoder))
    }
    
    async fn execute_select<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &Arc<DbHandler>,
        session: &Arc<SessionState>,
        portal_name: &str,
        query: &str,
        max_rows: i32,
        execution_context: &crate::query::ExecutionContext,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        debug!("execute_select called with query: {}", query);
        // Check if this is a catalog query first
        info!("execute_select: Checking if query is catalog query: {}", query);
        let response = if let Some(catalog_result) = CatalogInterceptor::intercept_query(query, db.clone(), Some(session.clone())).await {
            info!("execute_select: Query intercepted by catalog handler");
            let mut catalog_response = catalog_result?;
            
            // For catalog queries with binary result formats, we need to ensure the data
            // is in the correct format for binary encoding
            let portals = session.portals.read().await;
            let portal = portals.get(portal_name).unwrap();
            let has_binary_format = portal.result_formats.contains(&1);
            drop(portals);
            
            if has_binary_format && query.contains("pg_attribute") {
                info!("Converting catalog text data for binary encoding");
                // pg_attribute specific handling - ensure numeric columns are properly formatted
                for row in &mut catalog_response.rows {
                    // attnum is at index 5
                    if row.len() > 5 {
                        if let Some(Some(attnum_bytes)) = row.get_mut(5) {
                            if let Ok(attnum_str) = String::from_utf8(attnum_bytes.clone()) {
                                // Ensure it's just the numeric value without extra formatting
                                *attnum_bytes = attnum_str.trim().as_bytes().to_vec();
                            }
                        }
                    }
                }
            }
            
            catalog_response
        } else {
            info!("Query not intercepted, executing normally");
            let cached_conn = Self::get_or_cache_connection(session, db).await;
            db.query_with_session_cached(query, &session.id, cached_conn.as_ref()).await?
        };
        
        // Check if we need to send RowDescription
        // We send it if:
        // 1. The prepared statement had no field descriptions (wasn't Described or Describe sent NoData)
        // 2. OR if binary format is requested (psycopg3 binary cursors need RowDescription before DataRows)
        // BUT NOT for catalog queries - they should already have field descriptions from Describe
        let send_row_desc = {
            let portals = session.portals.read().await;
            let portal = portals.get(portal_name);
            info!("execute_select: Looking for portal '{}'", portal_name);
            
            if let Some(portal) = portal {
                info!("execute_select: Found portal with statement_name='{}'", portal.statement_name);
                let statements = session.prepared_statements.read().await;
                let stmt = statements.get(&portal.statement_name);
                
                if let Some(stmt) = stmt {
                    // Only send RowDescription if statement has no field descriptions
                    // If it has field descriptions, that means Describe was already called
                    // and already sent RowDescription to the client
                    let needs_row_desc = stmt.field_descriptions.is_empty() && !response.columns.is_empty();
                    info!("execute_select: stmt.field_descriptions.len()={}, response.columns.is_empty()={}, send_row_desc={}", 
                           stmt.field_descriptions.len(), response.columns.is_empty(), needs_row_desc);
                    needs_row_desc
                } else {
                    info!("execute_select: No statement found for name '{}', sending RowDescription", portal.statement_name);
                    true
                }
            } else {
                info!("execute_select: No portal found for name '{}', sending RowDescription", portal_name);
                true
            }
        };
        
        debug!("execute_select: send_row_desc = {}", send_row_desc);
        if send_row_desc {
            debug!("Execute: Will send RowDescription");
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
                // Update formats from portal
                let portals = session.portals.read().await;
                let portal = portals.get(portal_name).unwrap();
                let result_formats = &portal.result_formats;
                
                cached_fields.into_iter()
                    .enumerate()
                    .map(|(i, mut field)| {
                        field.format = if result_formats.is_empty() {
                            0 // Default to text if no formats specified
                        } else if result_formats.len() == 1 {
                            result_formats[0] // Single format applies to all columns
                        } else if i < result_formats.len() {
                            result_formats[i] // Use column-specific format
                        } else {
                            0 // Default to text if not enough formats
                        };
                        field
                    })
                    .collect()
            } else {
                // Pre-fetch schema types for all columns if we have a table name
                let mut schema_types = std::collections::HashMap::new();
                if let Some(ref table) = table_name {
                    for col_name in &response.columns {
                        // Try to look up the actual column name (without aliases)
                        let lookup_col = if col_name.contains('_') {
                            // For aggregate results like 'value_array', try the base column name
                            if let Some(base) = col_name.split('_').next() {
                                base.to_string()
                            } else {
                                col_name.clone()
                            }
                        } else {
                            col_name.clone()
                        };
                        
                        if let Ok(Some(pg_type)) = db.get_schema_type(table, &lookup_col).await {
                            schema_types.insert(col_name.clone(), pg_type);
                        }
                    }
                }
                
                // Get inferred types from portal if available
                let portal_inferred_types = {
                    let portals = session.portals.read().await;
                    let portal = portals.get(portal_name).unwrap();
                    portal.inferred_param_types.clone()
                };
                
                // Try to infer field types from data
                let field_types = response.columns.iter()
                    .enumerate()
                    .map(|(i, col_name)| {
                        // Special handling for parameter columns (e.g., $1, ?column?)
                        if col_name.starts_with('$') || col_name == "?column?" {
                            // This is a parameter column, get type from portal's inferred types
                            if let Some(ref inferred_types) = portal_inferred_types {
                                // Try to extract parameter number from column name
                                let param_idx = if col_name.starts_with('$') {
                                    col_name[1..].parse::<usize>().ok().map(|n| n - 1)
                                } else {
                                    Some(i) // Use column index for ?column?
                                };
                                
                                if let Some(idx) = param_idx {
                                    if let Some(&type_oid) = inferred_types.get(idx) {
                                        info!("Column '{}' is parameter with inferred type OID {}", col_name, type_oid);
                                        return type_oid;
                                    }
                                }
                            }
                        }
                        
                        // First priority: Check schema table for stored type mappings
                        if let Some(pg_type) = schema_types.get(col_name) {
                            // Use basic type OID mapping (enum checking would require async which isn't allowed in closure)
                            let oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(pg_type);
                            info!("Column '{}' found in schema as type '{}' (OID {})", col_name, pg_type, oid);
                            return oid;
                        }
                        
                        // Second priority: Check for aggregate functions
                        let col_lower = col_name.to_lowercase();
                        if let Some(oid) = crate::types::SchemaTypeMapper::get_aggregate_return_type(&col_lower, None, None) {
                            info!("Column '{}' is aggregate function with type OID {}", col_name, oid);
                            return oid;
                        }
                        
                        // Check if this looks like a user table (not system/catalog queries)
                        if let Some(ref table) = table_name {
                            // System/catalog tables are allowed to use type inference
                            let is_system_table = table.starts_with("pg_") || 
                                                 table.starts_with("information_schema") ||
                                                 table == "__pgsqlite_schema";
                            
                            if !is_system_table {
                                // For user tables, missing metadata is an error
        // debug!("Column '{}' in table '{}' not found in __pgsqlite_schema. Using type inference.", col_name, table);
        // debug!("Falling back to type inference, but this may cause type compatibility issues.");
                            }
                        }
                        
                        // Last resort: Try to get type from value (with warning for user tables)
                        let type_oid = if !response.rows.is_empty() {
                            if let Some(value) = response.rows[0].get(i) {
                                crate::types::SchemaTypeMapper::infer_type_from_value(value.as_deref())
                            } else {
                                25 // text for NULL
                            }
                        } else {
                            25 // text default when no data
                        };
                        
                        warn!("Column '{}' using inferred type OID {} (should have metadata)", col_name, type_oid);
                        type_oid
                    })
                    .collect::<Vec<_>>();
                
                let fields: Vec<FieldDescription> = {
                    let portals = session.portals.read().await;
                    let portal = portals.get(portal_name).unwrap();
                    let result_formats = &portal.result_formats;
                    
                    response.columns.iter()
                        .enumerate()
                        .map(|(i, col_name)| {
                            let format = if result_formats.is_empty() {
                                0 // Default to text if no formats specified
                            } else if result_formats.len() == 1 {
                                result_formats[0] // Single format applies to all columns
                            } else if i < result_formats.len() {
                                result_formats[i] // Use column-specific format
                            } else {
                                0 // Default to text if not enough formats
                            };
                            
                            FieldDescription {
                                name: col_name.clone(),
                                table_oid: 0,
                                column_id: (i + 1) as i16,
                                type_oid: *field_types.get(i).unwrap_or(&25),
                                type_size: -1,
                                type_modifier: -1,
                                format,
                            }
                        })
                        .collect()
                };
                
                // Cache the field descriptions (without format, as that's per-portal)
                let cache_fields = fields.iter().map(|f| FieldDescription {
                    name: f.name.clone(),
                    table_oid: f.table_oid,
                    column_id: f.column_id,
                    type_oid: f.type_oid,
                    type_size: f.type_size,
                    type_modifier: f.type_modifier,
                    format: 0, // Default format for cache
                }).collect::<Vec<_>>();
                GLOBAL_ROW_DESCRIPTION_CACHE.insert(cache_key, cache_fields);
                
                fields
            };
            
            info!("Sending RowDescription with {} fields during Execute with inferred types", fields.len());
            framed.send(BackendMessage::RowDescription(fields)).await
                .map_err(PgSqliteError::Io)?;
        } else {
            info!("execute_select: Not sending RowDescription (send_row_desc={}, already_sent={})", 
                  send_row_desc, execution_context.is_row_description_sent());
        }
        
        // Get result formats and field types from the portal and statement
        let (result_formats, field_types) = {
            let portals = session.portals.read().await;
            let portal = portals.get(portal_name).unwrap();
            let statements = session.prepared_statements.read().await;
            let stmt = statements.get(&portal.statement_name).unwrap();
            let field_types: Vec<i32> = if stmt.field_descriptions.is_empty() {
                // Try to infer types from data
                response.columns.iter()
                    .enumerate()
                    .map(|(i, col_name)| {
                        // Check for aggregate functions first
                        let col_lower = col_name.to_lowercase();
                        if let Some(oid) = crate::types::SchemaTypeMapper::get_aggregate_return_type(&col_lower, None, None) {
                            info!("Column '{}' is aggregate function with type OID {} (field_types)", col_name, oid);
                            return oid;
                        }
                        
                        // Check if this column name suggests numeric arithmetic result
                        // This handles cases like 'item_total', 'discounted_price', etc.
                        if col_name.contains("total") || col_name.contains("price") || col_name.contains("amount") || col_name.contains("sum") {
                            // Check if we have numeric data
                            if !response.rows.is_empty() {
                                if let Some(value) = response.rows[0].get(i) {
                                    if let Some(bytes) = value {
                                        if let Ok(s) = std::str::from_utf8(bytes) {
                                            // If it parses as a decimal number, treat as NUMERIC
                                            if s.contains('.') && s.parse::<f64>().is_ok() {
                                                info!("Column '{}' appears to be numeric based on name and value '{}'", col_name, s);
                                                return PgType::Numeric.to_oid();
                                            }
                                        }
                                    }
                                }
                            }
                            // Even without data, assume these columns are numeric
                            info!("Column '{}' assumed to be numeric based on name", col_name);
                            return PgType::Numeric.to_oid();
                        }
                        
                        // Try to get type from value
                        let type_oid = if !response.rows.is_empty() {
                            if let Some(value) = response.rows[0].get(i) {
                                crate::types::SchemaTypeMapper::infer_type_from_value(value.as_deref())
                            } else {
                                25 // text for NULL
                            }
                        } else {
                            25 // text default when no data
                        };
                        
                        info!("Column '{}' inferred as type OID {} (field_types)", col_name, type_oid);
                        type_oid
                    })
                    .collect::<Vec<_>>()
            } else {
                stmt.field_descriptions.iter().map(|fd| fd.type_oid).collect()
            };
            (portal.result_formats.clone(), field_types)
        };
        
        // Check if we're resuming from a previous Execute
        let has_portal_state = session.portal_manager.get_execution_state(portal_name).is_some();
        let (rows_to_send, sent_count, total_rows) = if let Some(state) = session.portal_manager.get_execution_state(portal_name) {
            if state.cached_result.is_some() {
                // Resume from cached results
                let cached = state.cached_result.as_ref().unwrap();
                let start_idx = state.row_offset;
                let available_rows = cached.rows.len() - start_idx;
                
                let take_count = if max_rows > 0 {
                    std::cmp::min(max_rows as usize, available_rows)
                } else {
                    available_rows
                };
                
                let rows: Vec<_> = cached.rows[start_idx..start_idx + take_count].to_vec();
                (rows, take_count, cached.rows.len())
            } else {
                // First execution - cache the results
                let all_rows = response.rows.clone();
                let total = all_rows.len();
                
                // Cache the result for future partial fetches
                let cached_result = crate::session::CachedQueryResult {
                    rows: all_rows.clone(),
                    field_descriptions: vec![], // Will be populated if needed
                    command_tag: format!("SELECT {total}"),
                };
                
                session.portal_manager.update_execution_state(
                    portal_name,
                    0,
                    false,
                    Some(cached_result),
                )?;
                
                // Take rows for this execution
                let rows_to_send = if max_rows > 0 {
                    response.rows.into_iter().take(max_rows as usize).collect()
                } else {
                    response.rows
                };
                let sent = rows_to_send.len();
                (rows_to_send, sent, total)
            }
        } else {
            // Portal not managed - use old behavior
            let total = response.rows.len();
            let rows_to_send = if max_rows > 0 {
                response.rows.into_iter().take(max_rows as usize).collect()
            } else {
                response.rows
            };
            let sent = rows_to_send.len();
            (rows_to_send, sent, total)
        };
        
        // Debug logging for catalog queries
        if query.contains("pg_catalog") || query.contains("pg_attribute") {
            info!("Catalog query data encoding:");
            info!("  Result formats: {:?}", result_formats);
            info!("  Field types: {:?}", field_types);
            if !rows_to_send.is_empty() {
                info!("  First row has {} columns", rows_to_send[0].len());
                for (i, col) in rows_to_send[0].iter().enumerate() {
                    if let Some(data) = col {
                        let preview = if data.len() <= 10 {
                            format!("{data:?}")
                        } else {
                            format!("{:?}... ({} bytes)", &data[..10], data.len())
                        };
                        info!("    Col {}: {}", i, preview);
                    } else {
                        info!("    Col {}: NULL", i);
                    }
                }
            }
        }
        
        // Check if we need binary encoding
        let needs_binary = result_formats.iter().any(|&f| f == 1);
        
        if needs_binary && !rows_to_send.is_empty() {
            // Use optimized batch encoding for binary format
            let (encoded_rows, _encoder) = Self::encode_rows_optimized(&rows_to_send, &result_formats, &field_types)?;
            for encoded_row in encoded_rows {
                framed.send(BackendMessage::DataRow(encoded_row)).await
                    .map_err(PgSqliteError::Io)?;
            }
        } else {
            // Text format or empty result - encode row by row
            for row in rows_to_send {
                let encoded_row = Self::encode_row(&row, &result_formats, &field_types)?;
                framed.send(BackendMessage::DataRow(encoded_row)).await
                    .map_err(PgSqliteError::Io)?;
            }
        }
        
        // Update portal execution state
        if let Some(state) = session.portal_manager.get_execution_state(portal_name) {
            let new_offset = state.row_offset + sent_count;
            let is_complete = new_offset >= total_rows;
            
            session.portal_manager.update_execution_state(
                portal_name,
                new_offset,
                is_complete,
                None, // Keep existing cached result
            )?;
        }
        
        // Send appropriate completion message
        if max_rows > 0 && sent_count == max_rows as usize && sent_count < total_rows {
            framed.send(BackendMessage::PortalSuspended).await
                .map_err(PgSqliteError::Io)?;
        } else {
            // Either we sent all remaining rows or max_rows was 0 (fetch all)
            let tag = format!("SELECT {}", if has_portal_state {
                // For resumed portals, report total rows fetched across all executions
                let state = session.portal_manager.get_execution_state(portal_name).unwrap();
                state.row_offset
            } else {
                sent_count
            });
            framed.send(BackendMessage::CommandComplete { tag }).await
                .map_err(PgSqliteError::Io)?;
        }
        
        Ok(())
    }
    
    async fn execute_dml<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &Arc<DbHandler>,
        query: &str,
        portal_name: &str,
        session: &Arc<SessionState>,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // Check for RETURNING clause
        if ReturningTranslator::has_returning_clause(query) {
        // debug!("Extended protocol: Query has RETURNING clause, using execute_dml_with_returning: {}", query);
            // Get result formats from portal
            let result_formats = {
                let portals = session.portals.read().await;
                let portal = portals.get(portal_name).unwrap();
                portal.result_formats.clone()
            };
            return Self::execute_dml_with_returning(framed, db, session, query, &result_formats).await;
        }
        
        // Validation is now done in handle_execute before parameter substitution
        
        // debug!("Extended protocol: Executing DML query without RETURNING: {}", query);
        let cached_conn = Self::get_or_cache_connection(session, db).await;
        let response = db.execute_with_session_cached(query, &session.id, cached_conn.as_ref()).await?;
        
        let tag = if query_starts_with_ignore_case(query, "INSERT") {
            format!("INSERT 0 {}", response.rows_affected)
        } else if query_starts_with_ignore_case(query, "UPDATE") {
            format!("UPDATE {}", response.rows_affected)
        } else if query_starts_with_ignore_case(query, "DELETE") {
            format!("DELETE {}", response.rows_affected)
        } else {
            format!("OK {}", response.rows_affected)
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
        result_formats: &[i16],
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // Use SQLite's native RETURNING support - just execute the query directly
        info!("Using native SQLite RETURNING for query: {}", query);
        
        // Execute the query with RETURNING as a single operation
        let cached_conn = Self::get_or_cache_connection(session, db).await;
        let returning_response = db.query_with_session_cached(query, &session.id, cached_conn.as_ref()).await?;
        
        // Extract the base operation type for the command tag
        let tag = if query_starts_with_ignore_case(query, "INSERT") {
            format!("INSERT 0 {}", returning_response.rows.len())
        } else if query_starts_with_ignore_case(query, "UPDATE") {
            format!("UPDATE {}", returning_response.rows.len())
        } else if query_starts_with_ignore_case(query, "DELETE") {
            format!("DELETE {}", returning_response.rows.len())
        } else {
            format!("OK {}", returning_response.rows.len())
        };
            
        // Send row description with proper format handling
        let fields: Vec<FieldDescription> = returning_response.columns.iter()
            .enumerate()
            .map(|(i, name)| {
                let format = if result_formats.is_empty() {
                    0 // Default to text if no formats specified
                } else if result_formats.len() == 1 {
                    result_formats[0] // Single format applies to all columns
                } else if i < result_formats.len() {
                    result_formats[i] // Use column-specific format
                } else {
                    0 // Default to text if not enough formats
                };
                
                FieldDescription {
                    name: name.clone(),
                    table_oid: 0,
                    column_id: (i + 1) as i16,
                    type_oid: 25, // Default to text - could be improved with type detection
                    type_size: -1,
                    type_modifier: -1,
                    format,
                }
            })
            .collect();
        
        framed.send(BackendMessage::RowDescription(fields.clone())).await
            .map_err(PgSqliteError::Io)?;
        
        // Send data rows with binary encoding if requested
        let needs_binary_encoding = result_formats.iter().any(|&f| f == 1);
        
        if needs_binary_encoding {
            // Binary format requested - use optimized batch encoding
            let field_types: Vec<i32> = fields.iter()
                .map(|f| f.type_oid)
                .collect();
            
            // Use optimized batch encoder for binary results
            let (encoded_rows, _encoder) = Self::encode_rows_optimized(&returning_response.rows, result_formats, &field_types)?;
            
            // Send all encoded rows
            for encoded_row in encoded_rows {
                framed.send(BackendMessage::DataRow(encoded_row)).await
                    .map_err(PgSqliteError::Io)?;
            }
        } else {
            // Text format - send rows directly
            for row in returning_response.rows {
                framed.send(BackendMessage::DataRow(row)).await
                    .map_err(PgSqliteError::Io)?;
            }
        }
        
        // Send command complete
        framed.send(BackendMessage::CommandComplete { tag }).await
            .map_err(PgSqliteError::Io)?;
        
        Ok(())
    }
    
    async fn execute_ddl<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &Arc<DbHandler>,
        session: &Arc<SessionState>,
        query: &str,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        use crate::ddl::EnumDdlHandler;
        
        // Check if this is an ENUM DDL statement first
        if EnumDdlHandler::is_enum_ddl(query) {
            // ENUM DDL needs special handling through direct SQL execution
            // Parse and execute the ENUM DDL as SQL statements
            let enum_error = PgSqliteError::Protocol(
                "ENUM DDL is not supported in the current per-session connection mode. \
                Please create ENUMs before establishing connections.".to_string()
            );
            return Err(enum_error);
        }
        
        // Handle CREATE TABLE translation
        let _translated_query = if query_starts_with_ignore_case(query, "CREATE TABLE") {
            // Use translator with connection for ENUM support
            let (sqlite_sql, type_mappings, enum_columns, array_columns) = db.with_session_connection(&session.id, |conn| {
                let result = crate::translator::CreateTableTranslator::translate_with_connection_full(query, Some(conn))
                    .map_err(|e| rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                        Some(format!("CREATE TABLE translation failed: {e}"))
                    ))?;
                
                Ok((result.sql, result.type_mappings, result.enum_columns, result.array_columns))
            }).await
            .map_err(|e| PgSqliteError::Protocol(format!("Failed to translate CREATE TABLE: {e}")))?;
            
            // Execute the translated CREATE TABLE
            let cached_conn = Self::get_or_cache_connection(session, db).await;
            db.execute_with_session_cached(&sqlite_sql, &session.id, cached_conn.as_ref()).await?;
            
            // Store the type mappings if we have any
        // debug!("Type mappings count: {}", type_mappings.len());
            info!("CREATE TABLE processing: Found {} type mappings", type_mappings.len());
            for (key, mapping) in &type_mappings {
                info!("Type mapping: {} -> {} (sqlite: {})", key, mapping.pg_type, mapping.sqlite_type);
            }
            
            if !type_mappings.is_empty() {
                // Extract table name from query
                if let Some(table_name) = extract_table_name_from_create(query) {
                    info!("CREATE TABLE: Extracted table name '{}' from query", table_name);
                    // Initialize the metadata table if it doesn't exist
                    let init_query = "CREATE TABLE IF NOT EXISTS __pgsqlite_schema (
                        table_name TEXT NOT NULL,
                        column_name TEXT NOT NULL,
                        pg_type TEXT NOT NULL,
                        sqlite_type TEXT NOT NULL,
                        PRIMARY KEY (table_name, column_name)
                    )";
                    let cached_conn = Self::get_or_cache_connection(session, db).await;
                    let _ = db.execute_with_session_cached(init_query, &session.id, cached_conn.as_ref()).await;
                    
                    // Store each type mapping and numeric constraints
                    for (full_column, type_mapping) in type_mappings {
                        // Split table.column format
                        let parts: Vec<&str> = full_column.split('.').collect();
                        if parts.len() == 2 && parts[0] == table_name {
                            let insert_query = format!(
                                "INSERT OR REPLACE INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES ('{}', '{}', '{}', '{}')",
                                table_name, parts[1], type_mapping.pg_type, type_mapping.sqlite_type
                            );
                            let cached_conn = Self::get_or_cache_connection(session, db).await;
                            let _ = db.execute_with_session_cached(&insert_query, &session.id, cached_conn.as_ref()).await;
                            
                            // Store numeric constraints if applicable
                            if let Some(modifier) = type_mapping.type_modifier {
                                // Extract base type without parameters
                                let base_type = if let Some(paren_pos) = type_mapping.pg_type.find('(') {
                                    type_mapping.pg_type[..paren_pos].trim()
                                } else {
                                    &type_mapping.pg_type
                                };
                                let pg_type_lower = base_type.to_lowercase();
                                
                                if pg_type_lower == "numeric" || pg_type_lower == "decimal" {
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
                                            info!("Stored numeric constraint: {}.{} precision={} scale={}", table_name, parts[1], precision, scale);
                                        }
                                        Err(_e) => {
        // debug!("Failed to store numeric constraint for {}.{}: {}", table_name, parts[1], e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
        // debug!("Stored type mappings for table {} (extended query protocol)", table_name);
                    
                    // Create triggers for ENUM columns
                    if !enum_columns.is_empty() {
                        db.with_session_connection(&session.id, |conn| {
                            for (column_name, enum_type) in &enum_columns {
                                // Record enum usage
                                crate::metadata::EnumTriggers::record_enum_usage(conn, &table_name, column_name, enum_type)
                                    .map_err(|e| rusqlite::Error::SqliteFailure(
                                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                                        Some(format!("Failed to record enum usage: {e}"))
                                    ))?;
                                
                                // Create validation triggers
                                crate::metadata::EnumTriggers::create_enum_validation_triggers(conn, &table_name, column_name, enum_type)
                                    .map_err(|e| rusqlite::Error::SqliteFailure(
                                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                                        Some(format!("Failed to create enum triggers: {e}"))
                                    ))?;
                                
                                info!("Created ENUM validation triggers for {}.{} (type: {})", table_name, column_name, enum_type);
                            }
                            Ok::<(), rusqlite::Error>(())
                        }).await
                        .map_err(|e| PgSqliteError::Protocol(format!("Failed to create ENUM triggers: {e}")))?;
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
                            )?;
                            
                            // Insert array column metadata
                            for (column_name, element_type, dimensions) in &array_columns {
                                conn.execute(
                                    "INSERT OR REPLACE INTO __pgsqlite_array_types (table_name, column_name, element_type, dimensions) 
                                     VALUES (?1, ?2, ?3, ?4)",
                                    rusqlite::params![table_name, column_name, element_type, dimensions]
                                )?;
                                
                                info!("Stored array column metadata for {}.{} (element_type: {}, dimensions: {})", 
                                      table_name, column_name, element_type, dimensions);
                            }
                            Ok::<(), rusqlite::Error>(())
                        }).await
                        .map_err(|e| PgSqliteError::Protocol(format!("Failed to store array metadata: {e}")))?;
                    }
                }
            }
            
            // Send CommandComplete and return
            framed.send(BackendMessage::CommandComplete { tag: "CREATE TABLE".to_string() }).await
                .map_err(PgSqliteError::Io)?;
            
            return Ok(());
        };
        
        // Handle other DDL with potential JSON translation
        let translated_query = if query.to_lowercase().contains("json") || query.to_lowercase().contains("jsonb") {
            JsonTranslator::translate_statement(query)?
        } else {
            query.to_string()
        };
        
        let cached_conn = Self::get_or_cache_connection(session, db).await;
        db.execute_with_session_cached(&translated_query, &session.id, cached_conn.as_ref()).await?;
        
        let tag = if query_starts_with_ignore_case(query, "CREATE TABLE") {
            "CREATE TABLE".to_string()
        } else if query_starts_with_ignore_case(query, "DROP TABLE") {
            "DROP TABLE".to_string()
        } else if query_starts_with_ignore_case(query, "CREATE INDEX") {
            "CREATE INDEX".to_string()
        } else {
            "OK".to_string()
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
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        if query_starts_with_ignore_case(query, "BEGIN") {
            db.begin_with_session(&session.id).await?;
            framed.send(BackendMessage::CommandComplete { tag: "BEGIN".to_string() }).await
                .map_err(PgSqliteError::Io)?;
        } else if query_starts_with_ignore_case(query, "COMMIT") {
            db.commit_with_session(&session.id).await?;
            framed.send(BackendMessage::CommandComplete { tag: "COMMIT".to_string() }).await
                .map_err(PgSqliteError::Io)?;
        } else if query_starts_with_ignore_case(query, "ROLLBACK") {
            db.rollback_with_session(&session.id).await?;
            framed.send(BackendMessage::CommandComplete { tag: "ROLLBACK".to_string() }).await
                .map_err(PgSqliteError::Io)?;
        }
        
        Ok(())
    }
    
    async fn execute_generic<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &Arc<DbHandler>,
        session: &Arc<SessionState>,
        query: &str,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let cached_conn = Self::get_or_cache_connection(session, db).await;
        db.execute_with_session_cached(query, &session.id, cached_conn.as_ref()).await?;
        
        framed.send(BackendMessage::CommandComplete { tag: "OK".to_string() }).await
            .map_err(PgSqliteError::Io)?;
        
        Ok(())
    }
    
    /// Analyze INSERT query to determine parameter types from schema
    async fn analyze_insert_params(query: &str, db: &Arc<DbHandler>, session: &Arc<SessionState>) -> Result<(Vec<i32>, Vec<i32>), PgSqliteError> {
        // Use QueryContextAnalyzer to extract table and column info
        let (table_name, columns) = crate::types::QueryContextAnalyzer::get_insert_column_info(query)
            .ok_or_else(|| PgSqliteError::Protocol("Failed to parse INSERT query".to_string()))?;
        
        info!("Analyzing INSERT for table '{}' with columns: {:?}", table_name, columns);
        
        // Get table schema using session-aware method to see session-specific tables
        let table_schema = db.with_session_connection(&session.id, |conn| {
            db.get_schema_cache().get_or_load(conn, &table_name)
        }).await.map_err(|e| PgSqliteError::Protocol(format!("Failed to get table schema: {e}")))?;
        
        // If no explicit columns, use all columns from the table
        let columns = if columns.is_empty() {
            table_schema.columns.iter()
                .map(|col| col.name.clone())
                .collect()
        } else {
            columns
        };
        
        // Look up types for each column using cached schema
        let mut param_types = Vec::new();
        let mut original_types = Vec::new();
        for column in &columns {
            if let Some(col_info) = table_schema.column_map.get(&column.to_lowercase()) {
                original_types.push(col_info.pg_oid);
                
                // For certain PostgreSQL types that tokio-postgres doesn't support in binary format,
                // use TEXT as the parameter type to allow string representation
                let param_oid = match col_info.pg_oid {
                    t if t == PgType::Macaddr8.to_oid() => PgType::Text.to_oid(), // MACADDR8 -> TEXT
                    t if t == PgType::Macaddr.to_oid() => PgType::Text.to_oid(), // MACADDR -> TEXT  
                    t if t == PgType::Inet.to_oid() => PgType::Text.to_oid(), // INET -> TEXT
                    t if t == PgType::Cidr.to_oid() => PgType::Text.to_oid(), // CIDR -> TEXT
                    t if t == PgType::Money.to_oid() => PgType::Text.to_oid(), // MONEY -> TEXT
                    t if t == PgType::Int4range.to_oid() => PgType::Text.to_oid(), // INT4RANGE -> TEXT
                    t if t == PgType::Int8range.to_oid() => PgType::Text.to_oid(), // INT8RANGE -> TEXT
                    t if t == PgType::Numrange.to_oid() => PgType::Text.to_oid(), // NUMRANGE -> TEXT
                    t if t == PgType::Bit.to_oid() => PgType::Text.to_oid(), // BIT -> TEXT
                    t if t == PgType::Varbit.to_oid() => PgType::Text.to_oid(), // VARBIT -> TEXT
                    _ => col_info.pg_oid, // Use original OID for supported types
                };
                
                param_types.push(param_oid);
                if param_oid != col_info.pg_oid {
                    info!("Mapped parameter type for {}.{}: {} (OID {}) -> TEXT (OID 25) for binary protocol compatibility", 
                          table_name, column, col_info.pg_type, col_info.pg_oid);
                } else {
                    info!("Found cached type for {}.{}: {} (OID {})", 
                          table_name, column, col_info.pg_type, col_info.pg_oid);
                }
            } else {
                // Default to text if column not found
                param_types.push(PgType::Text.to_oid());
                original_types.push(PgType::Text.to_oid());
                info!("Column {}.{} not found in schema, defaulting to text", table_name, column);
            }
        }
        
        Ok((param_types, original_types))
    }
    
    /// Convert PostgreSQL type name to OID
    fn pg_type_name_to_oid(type_name: &str) -> i32 {
        match type_name.to_lowercase().as_str() {
            "bool" | "boolean" => PgType::Bool.to_oid(),
            "bytea" => PgType::Bytea.to_oid(),
            "char" => PgType::Char.to_oid(),
            "name" => 19, // Name type not in PgType enum yet
            "int8" | "bigint" => PgType::Int8.to_oid(),
            "int2" | "smallint" => PgType::Int2.to_oid(),
            "int4" | "integer" | "int" => PgType::Int4.to_oid(),
            "text" => PgType::Text.to_oid(),
            "oid" => 26, // OID type not in PgType enum yet
            "float4" | "real" => PgType::Float4.to_oid(),
            "float8" | "double" | "double precision" => PgType::Float8.to_oid(),
            "varchar" | "character varying" => PgType::Varchar.to_oid(),
            "date" => PgType::Date.to_oid(),
            "time" => PgType::Time.to_oid(),
            "timestamp" => PgType::Timestamp.to_oid(),
            "timestamptz" | "timestamp with time zone" => PgType::Timestamptz.to_oid(),
            "interval" => 1186, // Interval type not in PgType enum yet
            "numeric" | "decimal" => PgType::Numeric.to_oid(),
            "uuid" => PgType::Uuid.to_oid(),
            "json" => PgType::Json.to_oid(),
            "jsonb" => PgType::Jsonb.to_oid(),
            "money" => PgType::Money.to_oid(),
            "int4range" => PgType::Int4range.to_oid(),
            "int8range" => PgType::Int8range.to_oid(),
            "numrange" => PgType::Numrange.to_oid(),
            "cidr" => PgType::Cidr.to_oid(),
            "inet" => PgType::Inet.to_oid(),
            "macaddr" => PgType::Macaddr.to_oid(),
            "macaddr8" => PgType::Macaddr8.to_oid(),
            "bit" => PgType::Bit.to_oid(),
            "varbit" | "bit varying" => PgType::Varbit.to_oid(),
            _ => {
                info!("Unknown PostgreSQL type '{}', defaulting to text", type_name);
                PgType::Text.to_oid() // Default to text
            }
        }
    }

    /// Analyze SELECT query to determine parameter types from WHERE clause
    async fn analyze_select_params(query: &str, db: &Arc<DbHandler>, _session: &Arc<SessionState>) -> Result<Vec<i32>, PgSqliteError> {
        // First, check for explicit parameter casts like $1::int4
        let mut param_types = Vec::new();
        
        // Count parameters and try to determine their types
        for i in 1..=99 {
            let param = format!("${i}");
            if !query.contains(&param) {
                break;
            }
            
            // Check for explicit cast first (e.g., $1::int4)
            let cast_pattern = format!(r"\${i}::\s*(\w+)");
            let cast_regex = regex::Regex::new(&cast_pattern).unwrap();
            let mut found_type = false;
            
            if let Some(captures) = cast_regex.captures(query) {
                if let Some(type_match) = captures.get(1) {
                    let cast_type = type_match.as_str();
                    let oid = Self::pg_type_name_to_oid(cast_type);
                    param_types.push(oid);
                    info!("Found explicit cast for parameter {}: {} (OID {})", i, cast_type, oid);
                    found_type = true;
                }
            }
            
            if found_type {
                continue;
            }
            
            // If no explicit cast, try to infer from column comparisons
            // Extract table name from SELECT query (only if needed)
            let table_name = if let Some(name) = extract_table_name_from_select(query) {
                name
            } else {
                // No table found, default to text
                param_types.push(25);
                info!("Could not extract table name for parameter {}, defaulting to text", i);
                continue;
            };
            
            info!("Analyzing SELECT params for table: {}", table_name);
            let query_lower = query.to_lowercase();
            
            // Try to find which column this parameter is compared against
            // Look for patterns like "column = $n" or "column < $n" etc.
            
            // Look for the parameter in the query and find the column it's compared to
            // Use simpler string matching instead of complex regex
            let param_escaped = regex::escape(&param);
            let patterns = vec![
                format!(r"(\w+)\s*=\s*{}", param_escaped),
                format!(r"(\w+)\s*<\s*{}", param_escaped),
                format!(r"(\w+)\s*>\s*{}", param_escaped),
                format!(r"(\w+)\s*<=\s*{}", param_escaped),
                format!(r"(\w+)\s*>=\s*{}", param_escaped),
                format!(r"(\w+)\s*!=\s*{}", param_escaped),
                format!(r"(\w+)\s*<>\s*{}", param_escaped),
            ];
            
            for pattern in &patterns {
                let regex = regex::Regex::new(pattern).unwrap();
                if let Some(captures) = regex.captures(&query_lower) {
                    if let Some(column_match) = captures.get(1) {
                        let column = column_match.as_str();
                        
                        // Look up the type for this column
                        if let Ok(Some(pg_type)) = db.get_schema_type(&table_name, column).await {
                            let oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(&pg_type);
                            param_types.push(oid);
                            info!("Found type for parameter {} from column {}: {} (OID {})", 
                                  i, column, pg_type, oid);
                            found_type = true;
                            break;
                        } else {
                            // Try SQLite schema
                            let schema_query = format!("PRAGMA table_info({table_name})");
                            if let Ok(response) = db.query(&schema_query).await {
                                for row in &response.rows {
                                    if let (Some(Some(name_bytes)), Some(Some(type_bytes))) = (row.get(1), row.get(2)) {
                                        if let (Ok(col_name), Ok(sqlite_type)) = (
                                            String::from_utf8(name_bytes.clone()),
                                            String::from_utf8(type_bytes.clone())
                                        ) {
                                            if col_name.to_lowercase() == column {
                                                let pg_type = crate::types::SchemaTypeMapper::sqlite_type_to_pg_oid(&sqlite_type);
                                                param_types.push(pg_type);
                                                info!("Mapped SQLite type for parameter {} from column {}: {} -> PG OID {}", 
                                                      i, column, sqlite_type, pg_type);
                                                found_type = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
                        if found_type {
                            break;
                        }
                    }
                }
            }
            
            if !found_type {
                // Default to text if we can't determine the type
                param_types.push(25);
                info!("Could not determine type for parameter {}, defaulting to text", i);
            }
        }
        
        Ok(param_types)
    }
    
    /// Analyze a SELECT query to find explicit type casts on columns
    /// Returns a map of column index to cast type
    fn analyze_column_casts(query: &str) -> std::collections::HashMap<usize, String> {
        let mut cast_map = std::collections::HashMap::new();
        
        // Find the SELECT clause - use case-insensitive search
        let select_pos = if let Some(pos) = find_keyword_position(query, "SELECT") {
            pos
        } else {
            return cast_map; // No SELECT found
        };
        
        let after_select = &query[select_pos + 6..];
        
        // Find the FROM clause to know where SELECT list ends
        let from_pos = find_keyword_position(after_select, " FROM ")
            .unwrap_or(after_select.len());
        
        let select_list = &after_select[..from_pos];
        
        // Split by commas (simple parsing - doesn't handle nested functions perfectly)
        let mut column_idx = 0;
        let mut current_expr = String::new();
        let mut paren_depth = 0;
        
        for ch in select_list.chars() {
            match ch {
                '(' => {
                    paren_depth += 1;
                    current_expr.push(ch);
                }
                ')' => {
                    paren_depth -= 1;
                    current_expr.push(ch);
                }
                ',' if paren_depth == 0 => {
                    // Found a column separator
                    if let Some(cast_type) = Self::extract_cast_from_expression(&current_expr) {
                        cast_map.insert(column_idx, cast_type);
                    }
                    column_idx += 1;
                    current_expr.clear();
                }
                _ => {
                    current_expr.push(ch);
                }
            }
        }
        
        // Don't forget the last expression
        if !current_expr.trim().is_empty() {
            if let Some(cast_type) = Self::extract_cast_from_expression(&current_expr) {
                cast_map.insert(column_idx, cast_type);
            }
        }
        
        cast_map
    }
    
    /// Extract cast type from an expression like "column::text"
    fn extract_cast_from_expression(expr: &str) -> Option<String> {
        if let Some(cast_pos) = expr.find("::") {
            let cast_type = &expr[cast_pos + 2..];
            // Extract just the type name (before any whitespace or AS alias)
            let type_end = cast_type.find(|c: char| c.is_whitespace() || c == ')')
                .unwrap_or(cast_type.len());
            let type_name = cast_type[..type_end].trim().to_lowercase();
            
            if !type_name.is_empty() {
                Some(type_name)
            } else {
                None
            }
        } else {
            None
        }
    }
    
    /// Convert a PostgreSQL cast type name to its OID
    fn cast_type_to_oid(cast_type: &str) -> i32 {
        match cast_type {
            "text" => PgType::Text.to_oid(),
            "int4" | "int" | "integer" => PgType::Int4.to_oid(),
            "int8" | "bigint" => PgType::Int8.to_oid(),
            "int2" | "smallint" => PgType::Int2.to_oid(),
            "float4" | "real" => PgType::Float4.to_oid(),
            "float8" | "double precision" => PgType::Float8.to_oid(),
            "bool" | "boolean" => PgType::Bool.to_oid(),
            "bytea" => PgType::Bytea.to_oid(),
            "char" => PgType::Char.to_oid(),
            "varchar" => PgType::Varchar.to_oid(),
            "date" => PgType::Date.to_oid(),
            "time" => PgType::Time.to_oid(),
            "timestamp" => PgType::Timestamp.to_oid(),
            "timestamptz" => PgType::Timestamptz.to_oid(),
            "numeric" | "decimal" => PgType::Numeric.to_oid(),
            "json" => PgType::Json.to_oid(),
            "jsonb" => PgType::Jsonb.to_oid(),
            "uuid" => PgType::Uuid.to_oid(),
            "money" => PgType::Money.to_oid(),
            "int4range" => PgType::Int4range.to_oid(),
            "int8range" => PgType::Int8range.to_oid(),
            "numrange" => PgType::Numrange.to_oid(),
            "cidr" => PgType::Cidr.to_oid(),
            "inet" => PgType::Inet.to_oid(),
            "macaddr" => PgType::Macaddr.to_oid(),
            "macaddr8" => PgType::Macaddr8.to_oid(),
            "bit" => PgType::Bit.to_oid(),
            "varbit" | "bit varying" => PgType::Varbit.to_oid(),
            _ => PgType::Text.to_oid(), // Default to text for unknown types
        }
    }
    
    /// Infer parameter type from the actual value
    fn infer_type_from_value(value: &[u8], format: i16) -> i32 {
        if format == 1 {
            // Binary format - harder to infer, default to text
            // In a real implementation, we could try to decode common binary formats
            PgType::Text.to_oid()
        } else {
            // Text format - try to parse the value
            if let Ok(s) = String::from_utf8(value.to_vec()) {
                let trimmed = s.trim();
                
                // Check for boolean values
                if trimmed == "t" || trimmed == "f" || 
                   trimmed == "true" || trimmed == "false" || 
                   trimmed == "1" || trimmed == "0" {
                    return PgType::Bool.to_oid();
                }
                
                // Check for integer
                if trimmed.parse::<i32>().is_ok() {
                    return PgType::Int4.to_oid();
                }
                
                // Check for bigint
                if trimmed.parse::<i64>().is_ok() {
                    return PgType::Int8.to_oid();
                }
                
                // Check for float
                if trimmed.parse::<f64>().is_ok() {
                    return PgType::Float8.to_oid();
                }
                
                // Check for common date/time patterns
                if trimmed.len() == 10 && trimmed.chars().filter(|&c| c == '-').count() == 2 {
                    // Looks like a date (YYYY-MM-DD)
                    return PgType::Date.to_oid();
                }
                
                if trimmed.contains(':') && (trimmed.contains('-') || trimmed.contains('/')) {
                    // Looks like a timestamp
                    return PgType::Timestamp.to_oid();
                }
                
                // Default to text for everything else
                PgType::Text.to_oid()
            } else {
                // Not valid UTF-8, treat as bytea
                PgType::Bytea.to_oid()
            }
        }
    }
    
    /// Extract table names from a parsed SQL statement
    fn extract_table_names_from_statement(statement: &sqlparser::ast::Statement) -> Vec<String> {
        use sqlparser::ast::TableFactor;
        
        let mut tables = Vec::new();
        
        match statement {
            sqlparser::ast::Statement::Insert(insert) => {
                tables.push(insert.table.to_string());
            }
            sqlparser::ast::Statement::Query(query) => {
                super::extended_helpers::extract_tables_from_query(query, &mut tables);
            }
            sqlparser::ast::Statement::Update { table, .. } => {
                if let TableFactor::Table { name, .. } = &table.relation {
                    tables.push(name.to_string());
                }
            }
            sqlparser::ast::Statement::Delete(delete) => {
                // For DELETE, just get the main table from the FROM clause
                match &delete.from {
                    sqlparser::ast::FromTable::WithFromKeyword(table_list) => {
                        for table in table_list {
                            if let TableFactor::Table { name, .. } = &table.relation {
                                tables.push(name.to_string());
                            }
                        }
                    }
                    sqlparser::ast::FromTable::WithoutKeyword(names) => {
                        for name in names {
                            tables.push(name.to_string());
                        }
                    }
                }
            }
            _ => {}
        }
        
        tables
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
    
    async fn send_data_rows_only<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        response: crate::session::db_handler::DbResponse,
        result_formats: &[i16],
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        info!("send_data_rows_only: Sending {} data rows without RowDescription", response.rows.len());
        
        // Send DataRows only (no RowDescription)
        for row in &response.rows {
            let mut values = Vec::new();
            for (i, value) in row.iter().enumerate() {
                let format = if result_formats.is_empty() {
                    0 // Default to text
                } else if result_formats.len() == 1 {
                    result_formats[0]
                } else if i < result_formats.len() {
                    result_formats[i]
                } else {
                    0
                };
                
                if format == 0 {
                    // Text format
                    values.push(value.clone());
                } else {
                    // Binary format - for now just pass through
                    values.push(value.clone());
                }
            }
            framed.send(BackendMessage::DataRow(values)).await?;
        }
        
        Ok(())
    }
}


/// Extract table name from SELECT query
fn extract_table_name_from_select(query: &str) -> Option<String> {
    info!("extract_table_name_from_select: Analyzing query: '{}'", query);
    // Look for FROM clause using case-insensitive search
    if let Some(from_pos) = find_keyword_position(query, " from ") {
        info!("extract_table_name_from_select: Found FROM at position {}", from_pos);
        let after_from = &query[from_pos + 6..].trim();
        
        // Find the end of table name (space, where, order by, etc.)
        let table_end = after_from.find(|c: char| {
            c.is_whitespace() || c == ',' || c == ';' || c == '('
        }).unwrap_or(after_from.len());
        
        let table_name = after_from[..table_end].trim();
        
        // Remove quotes if present
        let table_name = table_name.trim_matches('"').trim_matches('\'');
        
        if !table_name.is_empty() {
            info!("extract_table_name_from_select: Extracted table name: '{}'", table_name);
            Some(table_name.to_string())
        } else {
            info!("extract_table_name_from_select: Empty table name");
            None
        }
    } else {
        info!("extract_table_name_from_select: No FROM clause found");
        None
    }
}

/// Extract table name from CREATE TABLE statement
fn extract_table_name_from_create(query: &str) -> Option<String> {
    info!("extract_table_name_from_create: Analyzing CREATE query: '{}'", query);
    // Look for CREATE TABLE pattern
    if let Some(table_pos) = find_keyword_position(query, "CREATE TABLE") {
        info!("extract_table_name_from_create: Found CREATE TABLE at position {}", table_pos);
        let after_create = &query[table_pos + 12..].trim();
        
        // Skip IF NOT EXISTS if present
        let after_create = if query_starts_with_ignore_case(after_create, "IF NOT EXISTS") {
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
            info!("extract_table_name_from_create: Extracted table name: '{}'", table_name);
            Some(table_name.to_string())
        } else {
            info!("extract_table_name_from_create: Empty table name");
            None
        }
    } else {
        info!("extract_table_name_from_create: No CREATE TABLE found");
        None
    }
}