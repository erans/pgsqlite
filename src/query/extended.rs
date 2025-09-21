use crate::protocol::{BackendMessage, FieldDescription};
use crate::session::{DbHandler, SessionState, PreparedStatement, Portal, GLOBAL_QUERY_CACHE};
use crate::catalog::CatalogInterceptor;
use crate::translator::{JsonTranslator, ReturningTranslator, CastTranslator};
use crate::types::{DecimalHandler, PgType};
use std::str::FromStr;
use crate::cache::{RowDescriptionKey, GLOBAL_ROW_DESCRIPTION_CACHE, GLOBAL_PARAMETER_CACHE, CachedParameterInfo};
use crate::validator::NumericValidator;
use crate::query::ParameterParser;
use crate::PgSqliteError;
use tokio_util::codec::Framed;
use futures::SinkExt;
use tracing::{info, warn, debug};
use std::sync::Arc;
use byteorder::{BigEndian, ByteOrder};
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
        info!("PARSE: Starting parse for statement '{}', query: {}", name, query);
        // Fast path: Check if we already have this prepared statement
        // This avoids re-parsing the same query multiple times
        if !name.is_empty() {
            let statements = session.prepared_statements.read().await;
            if let Some(existing) = statements.get(&name) {
                // Check if it's the same query
                if existing.query == query && existing.param_types == param_types {
                    // Already parsed, just send ParseComplete
                    info!("PARSE: Using cached statement '{}' with {} field_descriptions", name, existing.field_descriptions.len());
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
                // Translate the query for cached statements too
                // In per-session mode, we can't get a connection during parse,
                // so we'll translate without connection (which handles most cases)
                let translated_query = if CastTranslator::needs_translation(&query) {
                    Some(CastTranslator::translate_query(&query, None))
                } else {
                    None
                };
                
                // We already know about this query, create a fast prepared statement
                let stmt = PreparedStatement {
                    query: query.clone(),
                    translated_query,
                    param_types: cached_info.param_types.clone(),
                    param_formats: vec![0; cached_info.param_types.len()],
                    field_descriptions: Vec::new(), // Will be populated during bind/execute
                    translation_metadata: None,
                };
                
                // Store as unnamed statement
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
        
        // Removed verbose debug logging for parsing
        
        // Extract cast type information BEFORE any query translation
        let mut extracted_param_types = vec![0i32; ParameterParser::count_parameters(&cleaned_query)];
        
        // Check for Python-style parameters and convert to PostgreSQL-style
        use crate::query::parameter_parser::ParameterParser;
        let python_params = ParameterParser::find_python_parameters(&cleaned_query);
        if !python_params.is_empty() {
            // Python-style parameters found
            
            // First, extract type information from Python-style parameter casts
            for (index, param_name) in python_params.iter().enumerate() {
                let param_pattern = format!("%({param_name})s::");
                if let Some(cast_start) = cleaned_query.find(&param_pattern) {
                    let type_start = cast_start + param_pattern.len();
                    // Find the end of the type (space, comma, or parenthesis)
                    let mut type_end = type_start;
                    while type_end < cleaned_query.len() {
                        let ch = cleaned_query.chars().nth(type_end).unwrap();
                        if ch.is_whitespace() || ch == ',' || ch == ')' || ch == ';' {
                            break;
                        }
                        type_end += 1;
                    }
                    
                    if type_end > type_start {
                        let type_name = &cleaned_query[type_start..type_end];
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
                            extracted_param_types[index] = type_oid;
                            // Extracted parameter type
                        }
                    }
                }
            }
            
            // Convert %(name)s parameters to $1, $2, $3, etc.
            let mut param_counter = 1;
            for param_name in &python_params {
                let placeholder = format!("%({param_name})s");
                let numbered_placeholder = format!("${param_counter}");
                cleaned_query = cleaned_query.replace(&placeholder, &numbered_placeholder);
                param_counter += 1;
            }
            
            // Query converted and types extracted
            
            // Store the parameter mapping in session for later use in bind
            let mut python_param_mapping = session.python_param_mapping.write().await;
            python_param_mapping.insert(name.clone(), python_params);
        } else {
            // Also extract types from PostgreSQL-style parameter casts ($1::TYPE)
            for i in 1..=extracted_param_types.len() {
                let cast_pattern = format!("${i}::");
                if let Some(cast_start) = cleaned_query.find(&cast_pattern) {
                    let type_start = cast_start + cast_pattern.len();
                    let mut type_end = type_start;
                    while type_end < cleaned_query.len() {
                        let ch = cleaned_query.chars().nth(type_end).unwrap();
                        if ch.is_whitespace() || ch == ',' || ch == ')' || ch == ';' {
                            break;
                        }
                        type_end += 1;
                    }
                    
                    if type_end > type_start {
                        let type_name = &cleaned_query[type_start..type_end];
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
                            extracted_param_types[i - 1] = type_oid;
                            // Extracted parameter type
                        }
                    }
                }
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
        
        // Use extracted parameter types if we found any
        if extracted_param_types.iter().any(|&t| t != 0) {
            // Using extracted parameter types
            actual_param_types = extracted_param_types.clone();
            
            // Also cache the extracted parameter info for fast path access
            GLOBAL_PARAMETER_CACHE.insert(query.clone(), CachedParameterInfo {
                param_types: extracted_param_types.clone(),
                original_types: extracted_param_types.clone(), // Same as param_types since we extracted them directly
                table_name: None, // Will be populated later if needed
                column_names: Vec::new(), // Will be populated later if needed
                created_at: std::time::Instant::now(),
            });
            // Cached parameter types
        } else if param_types.is_empty() && cleaned_query.contains('$') {
            // First check parameter cache
            if let Some(cached_info) = GLOBAL_PARAMETER_CACHE.get(&query) {
                actual_param_types = cached_info.param_types;
                debug!("Using cached parameter types for query: {:?}", actual_param_types);
            } else {
                // Check if we have this query cached in query cache
                if let Some(cached) = GLOBAL_QUERY_CACHE.get(&query) {
                    actual_param_types = cached.param_types.clone();
                    debug!("Using cached parameter types from query cache: {:?}", actual_param_types);
                    
                    // Also cache in parameter cache for faster access
                    GLOBAL_PARAMETER_CACHE.insert(query.clone(), CachedParameterInfo {
                        param_types: actual_param_types.clone(),
                        original_types: actual_param_types.clone(), // Use same types since we don't have original info here
                        table_name: cached.table_names.first().cloned(),
                        column_names: Vec::new(), // Will be populated later if needed
                        created_at: std::time::Instant::now(),
                    });
                } else {
                    // Need to analyze the query
                    let (analyzed_types, original_types_opt, table_name, column_names) = if query_starts_with_ignore_case(&query, "INSERT") {
                        match Self::analyze_insert_params(&query, db).await {
                            Ok((types, orig_types)) => {
                                debug!("Analyzed INSERT parameter types: {:?} (original: {:?})", types, orig_types);
                                
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
                        debug!("Analyzed SELECT parameter types: {:?}", types);
                        
                        let table = extract_table_name_from_select(&query);
                        (types.clone(), Some(types), table, Vec::new())
                    } else {
                        // Other query types - just count parameters
                        let param_count = ParameterParser::count_parameters(&query);
                        let types = vec![PgType::Text.to_oid(); param_count];
                        (types.clone(), Some(types), None, Vec::new())
                    };
                    
                    actual_param_types = analyzed_types.clone();
                    
                    // Cache the parameter info
                    GLOBAL_PARAMETER_CACHE.insert(query.clone(), CachedParameterInfo {
                        param_types: analyzed_types,
                        original_types: original_types_opt.unwrap_or_else(|| actual_param_types.clone()),
                        table_name,
                        column_names,
                        created_at: std::time::Instant::now(),
                    });
                    
                    // Also update query cache if it's a parseable query (keep JSON path placeholders for now)
                    if let Ok(parsed) = sqlparser::parser::Parser::parse_sql(
                        &sqlparser::dialect::PostgreSqlDialect {},
                        &cleaned_query
                    )
                        && let Some(statement) = parsed.first() {
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
        
        // Pre-translate the query first so we can analyze the translated version
        #[cfg(feature = "unified_processor")]
        #[allow(unused_mut)]
        let mut translated_for_analysis = {
            // Use unified processor for translation - it handles ALL translations
            db.with_session_connection(&session.id, |conn| {
                crate::query::process_query(&cleaned_query, conn, db.get_schema_cache())
            }).await?
        };
        
        #[cfg(not(feature = "unified_processor"))]
        let mut translated_for_analysis = if crate::translator::CastTranslator::needs_translation(&cleaned_query) {
            db.with_session_connection(&session.id, |conn| {
                Ok(crate::translator::CastTranslator::translate_query(&cleaned_query, Some(conn)))
            }).await?
        } else {
            cleaned_query.clone()
        };
        
        // Translate NUMERIC to TEXT casts with proper formatting
        #[cfg(not(feature = "unified_processor"))] // Skip when using unified processor
        if crate::translator::NumericFormatTranslator::needs_translation(&translated_for_analysis) {
            translated_for_analysis = db.with_session_connection(&session.id, |conn| {
                Ok(crate::translator::NumericFormatTranslator::translate_query(&translated_for_analysis, conn))
            }).await?;
        }
        
        // Translate datetime functions if needed and capture metadata
        #[allow(unused_mut)]
        let mut translation_metadata = crate::translator::TranslationMetadata::new();
        #[cfg(not(feature = "unified_processor"))] // Skip when using unified processor
        if crate::translator::DateTimeTranslator::needs_translation(&translated_for_analysis) {
            let (translated, metadata) = crate::translator::DateTimeTranslator::translate_with_metadata(&translated_for_analysis);
            translated_for_analysis = translated;
            translation_metadata.merge(metadata);
        }
        
        // Translate catalog functions (remove pg_catalog prefix)
        #[cfg(not(feature = "unified_processor"))] // Skip when using unified processor
        {
            use crate::translator::{CatalogFunctionTranslator, PgTableIsVisibleTranslator};
            translated_for_analysis = CatalogFunctionTranslator::translate(&translated_for_analysis);
            translated_for_analysis = PgTableIsVisibleTranslator::translate(&translated_for_analysis);
        }
        
        // Translate array operators with metadata
        #[cfg(not(feature = "unified_processor"))] // Skip when using unified processor
        {
            use crate::translator::ArrayTranslator;
            // Translating array operators
            match ArrayTranslator::translate_with_metadata(&translated_for_analysis) {
            Ok((translated, metadata)) => {
                if translated != translated_for_analysis {
                    // Array translation applied
                    translated_for_analysis = translated;
                }
                // Array metadata processed
                translation_metadata.merge(metadata);
            }
                Err(_) => {
                    // Continue with original query
                }
            }
        }
        
        // Translate json_each()/jsonb_each() functions for PostgreSQL compatibility
        #[cfg(not(feature = "unified_processor"))] // Skip when using unified processor
        {
            use crate::translator::JsonEachTranslator;
        match JsonEachTranslator::translate_with_metadata(&translated_for_analysis) {
            Ok((translated, metadata)) => {
                if translated != translated_for_analysis {
                    // JSON each translation applied
                    translated_for_analysis = translated;
                }
                // JSON each metadata processed
                translation_metadata.merge(metadata);
            }
            Err(_) => {
                // JSON each translation failed
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
            // row_to_json translation applied
            // Translation complete
            translated_for_analysis = translated;
        }
        // row_to_json metadata processed
        translation_metadata.merge(metadata);
        }
        
        // Note: System function processing (like to_regtype) is handled during Execute phase
        // after parameter substitution, not during Parse phase
        
        // Analyze arithmetic expressions for type metadata
        #[cfg(not(feature = "unified_processor"))] // Skip when using unified processor
        if crate::translator::ArithmeticAnalyzer::needs_analysis(&translated_for_analysis) {
            let arithmetic_metadata = crate::translator::ArithmeticAnalyzer::analyze_query(&translated_for_analysis);
            translation_metadata.merge(arithmetic_metadata);
            debug!("Found {} arithmetic type hints", translation_metadata.column_mappings.len());
        }
        
        // For now, we'll just analyze the query to get field descriptions
        // In a real implementation, we'd parse the SQL and validate it
        info!("PARSE: Analyzing query '{}' for field descriptions", translated_for_analysis);
        info!("PARSE: Original query: {}", cleaned_query);
        info!("PARSE: Is simple param select: {}", is_simple_param_select);
        let field_descriptions = if query_starts_with_ignore_case(&cleaned_query, "SELECT") {
            // Don't try to get field descriptions if this is a catalog query
            // These queries are handled specially and don't need real field info
            if cleaned_query.contains("pg_catalog") || cleaned_query.contains("pg_type") ||
               cleaned_query.contains("pg_class") || cleaned_query.contains("pg_attribute") ||
               cleaned_query.contains("pg_namespace") || cleaned_query.contains("pg_enum") ||
               cleaned_query.contains("pg_constraint") || cleaned_query.contains("pg_index") ||
               cleaned_query.contains("pg_depend") {
                info!("PARSE: Skipping field description for catalog query: {}", cleaned_query);
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
                
                // Add LIMIT 1 to avoid processing too much data, but only if there's no existing LIMIT
                if !test_query.to_uppercase().contains(" LIMIT ") {
                    test_query = format!("{test_query} LIMIT 1");
                }
                let cached_conn = Self::get_or_cache_connection(session, db).await;
                let test_response = db.query_with_session_cached(&test_query, &session.id, cached_conn.as_ref()).await;
                
                match test_response {
                    Ok(response) => {
                        info!("Test query returned {} columns: {:?}", response.columns.len(), response.columns);
                        // Extract table name from query to look up schema
                        let table_name = extract_table_name_from_select(&query);
                        
                        // Pre-fetch schema types for all columns if we have a table name
                        let mut schema_types = std::collections::HashMap::new();
                        if let Some(ref table) = table_name {
                            // For aliased columns, try to find the source column
                            for col_name in &response.columns {
                                // First try direct lookup
                                if let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, table, col_name).await {
                                    schema_types.insert(col_name.clone(), pg_type);
                                } else {
                                    // Parse the query to find the source column for this alias
                                    // Look for pattern like "table.column AS alias" in the SELECT clause
                                    let pattern = format!(r"(?i)(\w+)\.(\w+)\s+AS\s+{}", regex::escape(col_name));
                                    // Checking alias pattern
                                    if let Ok(re) = regex::Regex::new(&pattern)
                                        && let Some(captures) = re.captures(&query)
                                            && let Some(src_table) = captures.get(1)
                                                && let Some(src_col) = captures.get(2) {
                                                    let src_table_name = src_table.as_str();
                                                    let src_col_name = src_col.as_str();
                                                    // Only use if it's the same table we identified
                                                    if src_table_name == table
                                                        && let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, table, src_col_name).await {
                                                            info!("Found type for aliased column '{}' from query pattern '{}.{}' in table '{}': {}", col_name, src_table_name, src_col_name, table, pg_type);
                                                            schema_types.insert(col_name.clone(), pg_type);
                                                            continue;
                                                        }
                                                }
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
                                        // Try to find source table and column if this is an alias
                                        info!("Attempting to extract source for alias: '{}' from query: {}", col_name, cleaned_query);
                                        if let Some((source_table, source_col)) = Self::extract_source_table_column_for_alias(&cleaned_query, col_name) {
                                            info!("Successfully resolved alias '{}' -> table '{}', column '{}'", col_name, source_table, source_col);
                                            if let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, &source_table, &source_col).await {
                                                info!("Found schema type for alias '{}' -> source column '{}.{}': {}", col_name, source_table, source_col, pg_type);
                                                schema_types.insert(col_name.clone(), pg_type);
                                            } else {
                                                info!("No schema type found for '{}.{}'", source_table, source_col);
                                            }
                                        } else {
                                            info!("Could not extract source table/column for alias '{}' from query", col_name);
                                        }
                                    }
                                }
                            }
                        }
                        
                        // Try to infer types from the first row if available
                        // We need to handle this asynchronously for schema lookup
                        let mut inferred_types = Vec::new();
                        
                        for (i, col_name) in response.columns.iter().enumerate() {
                            let inferred_type = {
                                // First priority: Check if this column has an explicit cast
                                if let Some(cast_type) = cast_info.get(&i) {
                                    Self::cast_type_to_oid(cast_type)
                                }
                                // For parameter columns (NULL from SELECT $1), try to match with parameters
                                else if col_name == "NULL" || col_name == "?column?" {
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
                                                param_type
                                            } else if !param_types.is_empty() && i < param_types.len() {
                                                let param_type = param_types[i];
                                                if param_type != 0 {
                                                    info!("Using provided param type {} for column {}", param_type, i);
                                                    param_type
                                                } else {
                                                    info!("No specific param type for column {}, defaulting to TEXT", i);
                                                    PgType::Text.to_oid()
                                                }
                                            } else {
                                                info!("No specific param type for column {}, defaulting to TEXT", i);
                                                PgType::Text.to_oid()
                                            }
                                        } else if !param_types.is_empty() && i < param_types.len() {
                                            let param_type = param_types[i];
                                            if param_type != 0 {
                                                info!("Using provided param type {} for column {}", param_type, i);
                                                param_type
                                            } else {
                                                info!("No specific param type for column {}, defaulting to TEXT", i);
                                                PgType::Text.to_oid()
                                            }
                                        } else {
                                            info!("No specific param type for column {}, defaulting to TEXT", i);
                                            PgType::Text.to_oid()
                                        }
                                    } else {
                                        // For other queries with NULL columns, default to TEXT
                                        PgType::Text.to_oid()
                                    }
                                }
                                // Second priority: Check translation metadata for type hints
                                else if let Some(hint) = translation_metadata.get_hint(col_name) {
                                    // FIRST: Check for arithmetic expressions on float columns
                                    if hint.expression_type == Some(crate::translator::ExpressionType::ArithmeticOnFloat) {
                                        debug!("Arithmetic expression '{}' detected with ArithmeticOnFloat hint, returning FLOAT8", col_name);
                                        // For arithmetic on REAL/FLOAT columns, always return FLOAT8
                                        PgType::Float8.to_oid()
                                    } else if let Some(suggested_type) = &hint.suggested_type {
                                        debug!("Using type hint from translation metadata for '{}': {:?}", col_name, suggested_type);
                                        suggested_type.to_oid()
                                    } else if hint.is_expression {
                                        // For other arithmetic expressions without suggested type, infer from source column
                                        if let Some(source_col) = &hint.source_column {
                                            // Extract table.column if present, or use the table from context
                                            let (source_table, source_column) = if source_col.contains('.') {
                                                let parts: Vec<&str> = source_col.split('.').collect();
                                                (parts[0], parts[1])
                                            } else if let Some(ref table) = table_name {
                                                (table.as_str(), source_col.as_str())
                                            } else {
                                                info!("Arithmetic expression '{}' has no table context and source column '{}' has no table prefix", col_name, source_col);
                                                ("", source_col.as_str()) // Will likely fail, but try anyway
                                            };
                                            
                                            if let Ok(Some(source_type_str)) = db.get_schema_type_with_session(&session.id, source_table, source_column).await {
                                                // Convert type string to OID for comparison
                                                let source_type_oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(&source_type_str);
                                                
                                                // Determine arithmetic result type based on source column type
                                                let arithmetic_result_type = match source_type_oid {
                                                    1700 => PgType::Numeric, // NUMERIC arithmetic = NUMERIC
                                                    23 | 21 | 20 => PgType::Numeric, // INTEGER arithmetic often = NUMERIC for safety
                                                    700 | 701 => PgType::Float8, // FLOAT arithmetic = FLOAT
                                                    _ => PgType::Float8, // Default to FLOAT8 for unknown types
                                                };
                                                info!("Resolved arithmetic expression '{}' from source '{}' (type {} -> OID {}) -> {:?}", 
                                                      col_name, source_col, source_type_str, source_type_oid, arithmetic_result_type);
                                                arithmetic_result_type.to_oid()
                                            } else {
                                                info!("Could not resolve source column type for arithmetic expression '{}'", col_name);
                                                0 // Will be handled below
                                            }
                                        } else {
                                            info!("Arithmetic expression '{}' has no source column", col_name);
                                            0 // Will be handled below  
                                        }
                                    } else {
                                        info!("No type hint found in translation metadata for '{}'", col_name);
                                        // Continue to next priority
                                        0 // Will be handled below
                                    }
                                } else {
                                    info!("No type hint found in translation metadata for '{}'", col_name);
                                    0 // Will be handled below
                                }
                            };
                            
                            // If we haven't found a type yet, continue with other priorities
                            if inferred_type != 0 {
                                inferred_types.push(inferred_type);
                                continue;
                            }
                            
                            // Third priority: Check schema table for stored type mappings
                            if let Some(pg_type) = schema_types.get(col_name) {
                                // Use basic type OID mapping (enum checking would require async which isn't allowed in closure)
                                let type_oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(pg_type);
                                inferred_types.push(type_oid);
                                continue;
                            }
                            
                            // Third priority: Check for aggregate functions
                            let col_lower = col_name.to_lowercase();
                            if let Some(oid) = crate::types::SchemaTypeMapper::get_aggregate_return_type_with_query(&col_lower, None, None, Some(&cleaned_query)) {
                                info!("Column '{}' identified with type OID {} from aggregate detection", col_name, oid);
                                inferred_types.push(oid);
                                continue;  // Important: continue here to prevent value-based inference from overriding
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
                                    inferred_types.push(PgType::Numeric.to_oid());
                                    continue;
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
                                    inferred_types.push(inferred_type);
                                } else {
                                    info!("Column '{}': NULL value, defaulting to text", col_name);
                                    inferred_types.push(PgType::Text.to_oid()); // text for NULL
                                }
                            } else {
                                // **THIS IS THE KEY FIX**: Instead of defaulting to TEXT, try schema lookup
                                info!("Column '{}': no data rows, attempting schema-based type inference", col_name);
                                
                                // Try to extract source table.column from the query
                                if let Some((source_table, source_col)) = Self::extract_source_table_column_for_alias(&cleaned_query, col_name) {
                                    info!("Resolved alias '{}' -> table '{}', column '{}'", col_name, source_table, source_col);
                                    
                                    // Use session-aware schema lookup to see uncommitted data
                                    match db.get_schema_type_with_session(&session.id, &source_table, &source_col).await {
                                        Ok(Some(pg_type_str)) => {
                                            let type_oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(&pg_type_str);
                                            info!("Column '{}': resolved type from schema '{}.{}' -> {} (OID {})", 
                                                  col_name, source_table, source_col, pg_type_str, type_oid);
                                            inferred_types.push(type_oid);
                                        }
                                        Ok(None) => {
                                            info!("Column '{}': no schema type found for '{}.{}', defaulting to text", 
                                                  col_name, source_table, source_col);
                                            inferred_types.push(PgType::Text.to_oid());
                                        }
                                        Err(_) => {
                                            // Schema lookup error, defaulting to text
                                            inferred_types.push(PgType::Text.to_oid());
                                        }
                                    }
                                } else {
                                    // Could not extract source table.column, try to infer from query structure
                                    info!("Column '{}': could not extract source table.column, analyzing query structure", col_name);
                                    
                                    // First, try to handle table_column pattern like "orders_total_amount"
                                    let mut type_found = false;
                                    if col_name.contains('_')
                                        && let Some(underscore_pos) = col_name.find('_') {
                                            let potential_table = &col_name[..underscore_pos];
                                            let potential_column = &col_name[underscore_pos + 1..];
                                            
                                            // Try to look up the type from schema
                                            match db.get_schema_type_with_session(&session.id, potential_table, potential_column).await {
                                                Ok(Some(pg_type_str)) => {
                                                    let type_oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(&pg_type_str);
                                                    info!("Column '{}': resolved type from table_column pattern '{}_{}'-> {} (OID {})", 
                                                          col_name, potential_table, potential_column, pg_type_str, type_oid);
                                                    inferred_types.push(type_oid);
                                                    type_found = true;
                                                }
                                                _ => {
                                                    // Pattern didn't match a real table.column
                                                }
                                            }
                                        }
                                    
                                    if !type_found {
                                        // For simple SELECT queries, try to extract table from FROM clause and assume column exists
                                        if let Some(table_name) = extract_table_name_from_select(&cleaned_query) {
                                            info!("Column '{}': extracted table '{}' from FROM clause, assuming column exists", col_name, table_name);
                                            
                                            match db.get_schema_type_with_session(&session.id, &table_name, col_name).await {
                                                Ok(Some(pg_type_str)) => {
                                                    let type_oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(&pg_type_str);
                                                    info!("Column '{}': resolved type from schema '{}.{}' -> {} (OID {})", 
                                                          col_name, table_name, col_name, pg_type_str, type_oid);
                                                    inferred_types.push(type_oid);
                                                }
                                                Ok(None) => {
                                                    info!("Column '{}': no schema type found for '{}.{}', defaulting to text", 
                                                          col_name, table_name, col_name);
                                                    inferred_types.push(PgType::Text.to_oid());
                                                }
                                                Err(_) => {
                                                    // Schema lookup error, defaulting to text
                                                    inferred_types.push(PgType::Text.to_oid());
                                                }
                                            }
                                        } else {
                                            info!("Column '{}': could not extract table name from query, defaulting to text", col_name);
                                            inferred_types.push(PgType::Text.to_oid());
                                        }
                                    }
                                }
                            }
                        }
                        
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
                        
                        // Special logging for orders queries
                        if cleaned_query.contains("orders") {
                            info!("PARSE: Orders query detected!");
                            info!("PARSE: Query: {}", cleaned_query);
                            info!("PARSE: Field descriptions created:");
                            for field in &fields {
                                info!("PARSE:   {} -> type OID {}", field.name, field.type_oid);
                                if field.name.contains("total_amount") && field.type_oid == 25 {
                                    info!("PARSE:   ^^^ BUG: total_amount has TEXT type!");
                                }
                            }
                        }
                        
                        info!("Parsed {} field descriptions from query with inferred types", fields.len());
                        fields
                    }
                    Err(_) => {
                        // Failed to get field descriptions - will determine during execute
                        Vec::new()
                    }
                }
            }
        } else {
            Vec::new()
        };
        
        // If param_types is empty but query has parameters, infer basic types
        if actual_param_types.is_empty() && cleaned_query.contains('$') {
            // Count parameters in the query
            let mut max_param = 0;
            for i in 1..=99 {
                if cleaned_query.contains(&format!("${i}")) {
                    max_param = i;
                } else if max_param > 0 {
                    break;
                }
            }
            
            info!("Query has {} parameters, defaulting all to text", max_param);
            // Default all to text - we'll handle type conversion during execution
            actual_param_types = vec![PgType::Text.to_oid(); max_param];
        }
        
        info!("Final param_types for statement: {:?}", actual_param_types);
        
        // Store the prepared statement
        // We already translated the query above for analysis, so just use that
        let translated_query = Some(translated_for_analysis);
        
        let stmt = PreparedStatement {
            query: query.clone(), // Store original query instead of cleaned_query
            translated_query,
            param_types: actual_param_types.clone(),
            param_formats: vec![0; actual_param_types.len()], // Default to text format
            field_descriptions,
            translation_metadata: if translation_metadata.column_mappings.is_empty() {
                None
            } else {
                Some(translation_metadata)
            },
        };
        
        session.prepared_statements.write().await.insert(name.clone(), stmt);
        
        // Send ParseComplete
        framed.send(BackendMessage::ParseComplete).await
            .map_err(PgSqliteError::Io)?;
        
        Ok(())
    }
    
    /// Try to extract the source table and column for an alias in a simple SELECT
    /// e.g., "SELECT test_users.id AS users_id" -> returns ("test_users", "id")
    /// e.g., "SELECT id AS event_id FROM test_events" -> returns ("test_events", "id")
    fn extract_source_table_column_for_alias(query: &str, alias: &str) -> Option<(String, String)> {
        // This is a simple heuristic for the common case
        // Look for "SELECT <expr> as <alias>" pattern
        info!("extract_source_table_column_for_alias: Looking for alias '{}' in query", alias);
        let query_upper = query.to_uppercase();
        let alias_upper = alias.to_uppercase();
        
        // Find "AS <alias>" in the query
        let as_pattern = format!(" AS {alias_upper}");
        info!("Looking for pattern: '{}'", as_pattern);
        if let Some(as_pos) = query_upper.find(&as_pattern) {
            info!("Found AS pattern at position {}", as_pos);
            // Work backwards to find the start of the expression
            let before_as = &query[..as_pos];
            
            // Find the column expression before AS
            // Handle both "table.column" and "column" patterns
            
            // Get the last token/word before AS (handling commas and spaces)
            let trimmed = before_as.trim_end();
            info!("Text before AS (trimmed): '{}'", trimmed);
            
            // Find where this expression starts (after SELECT or comma)
            let mut expr_start = 0;
            let select_upper = "SELECT ";
            
            // Look for the last comma or SELECT keyword
            if let Some(comma_pos) = trimmed.rfind(',') {
                expr_start = comma_pos + 1;
                info!("Found comma at position {}, expr_start = {}", comma_pos, expr_start);
            } else if let Some(select_pos) = query_upper.find(select_upper) {
                expr_start = select_pos + select_upper.len();
                info!("Found SELECT at position {}, expr_start = {}", select_pos, expr_start);
            }
            
            // Make sure expr_start is within bounds
            if expr_start >= trimmed.len() {
                info!("expr_start {} >= trimmed.len() {}, using 0", expr_start, trimmed.len());
                expr_start = 0;
            }
            
            let expr = trimmed[expr_start..].trim();
            info!("Extracted expression: '{}'", expr);
            
            // Check if it contains a dot (table.column)
            if let Some(dot_pos) = expr.rfind('.') {
                let table_part = expr[..dot_pos].trim();
                let column_part = expr[dot_pos + 1..].trim();
                info!("Found table.column pattern: '{}' . '{}'", table_part, column_part);
                
                // Validate both parts are simple identifiers
                if table_part.chars().all(|c| c.is_alphanumeric() || c == '_') &&
                   column_part.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    info!("Successfully parsed alias '{}' -> table '{}', column '{}'", alias, table_part, column_part);
                    return Some((table_part.to_string(), column_part.to_string()));
                }
            }
            // For simple column references without table prefix
            else if expr.chars().all(|c| c.is_alphanumeric() || c == '_') {
                // Try to extract table from FROM clause
                if let Some(table_name) = extract_table_name_from_select(query) {
                    info!("Successfully parsed alias '{}' -> table '{}' (from FROM clause), column '{}'", alias, table_name, expr);
                    return Some((table_name, expr.to_string()));
                }
            }
            
            info!("Failed to parse expression '{}' - not a valid table.column or simple column", expr);
        } else {
            info!("AS pattern not found for alias '{}'", alias);
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
            // Binding portal to statement
            
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
        
        // Get the prepared statement (handle unnamed statements specially)
        let statements = session.prepared_statements.read().await;
        
        // For unnamed statements, try both empty string and the actual value
        let stmt = if statement.is_empty() {
            // Try empty string key first for unnamed statements
            statements.get("")
                .or_else(|| statements.get(&statement))
        } else {
            statements.get(&statement)
        }
        .ok_or_else(|| {
            info!("Statement lookup failed for '{}', available statements: {:?}", 
                  statement, statements.keys().collect::<Vec<_>>());
            PgSqliteError::Protocol(format!("Unknown statement: {statement}"))
        })?;
            
        // Processing parameter types and formats
        
        // Check if we need to infer types (only when param types are empty or unknown)
        let needs_inference = stmt.param_types.is_empty() || 
            stmt.param_types.iter().all(|&t| t == 0);
        
        let mut inferred_types = None;
        
        if needs_inference && !values.is_empty() {
            // Inferring parameter types from values
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
        };
        
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
        println!("EXTENDED: handle_execute called for portal: '{}'", portal);

        // Get the portal
        let (query, translated_query, bound_values, param_formats, result_formats, statement_name, inferred_param_types) = {
            let portals = session.portals.read().await;
            let portal_obj = portals.get(&portal)
                .ok_or_else(|| PgSqliteError::Protocol(format!("Unknown portal: {portal}")))?;
            
            (portal_obj.query.clone(),
             portal_obj.translated_query.clone(),
             portal_obj.bound_values.clone(),
             portal_obj.param_formats.clone(),
             portal_obj.result_formats.clone(),
             portal_obj.statement_name.clone(),
             portal_obj.inferred_param_types.clone())
        };
        
        // Special logging for orders queries
        if query.contains("orders") && query.contains("customer_id") {
            info!("EXECUTE: Orders query detected!");
            info!("EXECUTE: Query: {}", query);
            info!("EXECUTE: Statement name: {}", statement_name);
            
            // Check what field_descriptions are stored for this statement
            let statements = session.prepared_statements.read().await;
            
            // For unnamed statements, try both empty string and the actual value
            let stmt_opt = if statement_name.is_empty() {
                statements.get("")
                    .or_else(|| statements.get(&statement_name))
            } else {
                statements.get(&statement_name)
            };
            
            if let Some(stmt) = stmt_opt {
                info!("EXECUTE: Statement has {} field_descriptions", stmt.field_descriptions.len());
                for fd in &stmt.field_descriptions {
                    info!("EXECUTE:   {} -> type OID {}", fd.name, fd.type_oid);
                    if fd.name.contains("total_amount") && fd.type_oid == 25 {
                        info!("EXECUTE:   ^^^ BUG DETECTED: total_amount has TEXT type!");
                    }
                }
            } else {
                info!("EXECUTE: Statement not found! Available keys: {:?}", 
                      statements.keys().collect::<Vec<_>>());
            }
        }
        
        // Use translated query if available, otherwise use original query
        let effective_query = translated_query.as_ref().unwrap_or(&query);
        
        // Get parameter types from the prepared statement
        let param_types = if let Some(inferred) = inferred_param_types {
            // Use inferred types if available
            inferred
        } else {
            let statements = session.prepared_statements.read().await;
            // Handle unnamed statements properly
            let stmt = if statement_name.is_empty() {
                statements.get("")
                    .or_else(|| statements.get(&statement_name))
                    .expect("Statement should exist")
            } else {
                statements.get(&statement_name)
                    .expect("Statement should exist")
            };
            stmt.param_types.clone()
        };
        
        // Fast path for simple parameterized SELECT queries
        // Allow :: cast operator if it's only for parameters (e.g., $1::INTEGER)
        let has_non_param_cast = if query.contains("::") {
            // Check if :: is only used with parameters ($1, $2, etc)
            let param_cast_regex = regex::Regex::new(r"\$\d+::").unwrap();
            let all_casts_regex = regex::Regex::new(r"::").unwrap();
            
            // Count total casts and parameter casts
            let total_casts = all_casts_regex.find_iter(&query).count();
            let param_casts = param_cast_regex.find_iter(&query).count();
            
            // If there are more casts than parameter casts, we have non-param casts
            total_casts > param_casts
        } else {
            false
        };
        
        if query_starts_with_ignore_case(&query, "SELECT") && 
           !query.contains("JOIN") && 
           !query.contains("GROUP BY") && 
           !query.contains("HAVING") &&
           !has_non_param_cast &&  // Allow parameter casts like $1::INTEGER
           !query.contains("UNION") &&
           !query.contains("INTERSECT") &&
           !query.contains("EXCEPT") &&
           (result_formats.is_empty() || result_formats[0] == 0) {
            
            info!("🚀 Ultra-fast path triggered for query: {}", query);
            // Using fast path for simple SELECT
            
            // Get cached connection first
            let _cached_conn = Self::get_or_cache_connection(session, db).await;
            
            // Use the original query if no translation needed
            let query_to_execute = if let Some(ref translated) = translated_query {
                translated
            } else {
                &query
            };
            
            // First, infer field types BEFORE executing the query
            // This is crucial for proper datetime conversion
            let field_types: Vec<i32> = {
                let statements = session.prepared_statements.read().await;
                if let Some(stmt) = statements.get(&statement_name) {
                    if !stmt.field_descriptions.is_empty() {
                        // Use existing field descriptions if available
                        stmt.field_descriptions.iter().map(|fd| fd.type_oid).collect()
                    } else {
                        // Need to infer types from the query structure
                        drop(statements);
                        
                        // Parse the query to get column names and their aliases
                        let mut inferred_types = Vec::new();
                        let table_name = extract_table_name_from_select(&query);
                        info!("Ultra-fast path: Inferring types for query: {}", query);
                        info!("Ultra-fast path: Extracted table name: {:?}", table_name);
                        
                        // Extract column names from SELECT clause
                        // Handle both " FROM " and "\nFROM " (queries might have newlines)
                        let query_upper = query.to_uppercase();
                        let select_end = query_upper.find(" FROM ")
                            .or_else(|| query_upper.find("\nFROM "));
                        
                        if let Some(select_end) = select_end {
                            let select_part = &query[6..select_end]; // Skip "SELECT"
                            let columns: Vec<&str> = select_part.split(',').map(|s| s.trim()).collect();
                            
                            info!("Ultra-fast path: Parsing {} columns from SELECT clause", columns.len());
                            for col_expr in columns {
                                info!("Ultra-fast path: Processing column expression: '{}'", col_expr);
                                let mut found_type = false;
                                
                                // Extract the alias name (after AS) if present
                                let col_name = if let Some(as_pos) = col_expr.to_uppercase().rfind(" AS ") {
                                    col_expr[as_pos + 4..].trim()
                                } else {
                                    // No alias, use the expression itself
                                    col_expr
                                };
                                
                                // Try to extract source table.column from the expression
                                if let Some((source_table, source_col)) = Self::extract_source_table_column_for_alias(&query, col_name) {
                                    if let Ok(Some(pg_type_str)) = db.get_schema_type_with_session(&session.id, &source_table, &source_col).await {
                                        let type_oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(&pg_type_str);
                                        info!("Ultra-fast path: Pre-execution type inference for '{}' from '{}.{}' -> {} (OID {})", 
                                              col_name, source_table, source_col, pg_type_str, type_oid);
                                        inferred_types.push(type_oid);
                                        found_type = true;
                                    }
                                } else if col_expr.contains('.') {
                                    // Handle table.column format directly
                                    let parts: Vec<&str> = col_expr.split('.').collect();
                                    if parts.len() == 2 {
                                        let table_part = parts[0].trim();
                                        let column_part = parts[1].trim();
                                        if let Ok(Some(pg_type_str)) = db.get_schema_type_with_session(&session.id, table_part, column_part).await {
                                            let type_oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(&pg_type_str);
                                            info!("Ultra-fast path: Pre-execution type inference for '{}' from '{}.{}' -> {} (OID {})", 
                                                  col_name, table_part, column_part, pg_type_str, type_oid);
                                            inferred_types.push(type_oid);
                                            found_type = true;
                                        }
                                    }
                                } else if let Some(ref table) = table_name {
                                    // Try direct column name lookup in the main table
                                    if let Ok(Some(pg_type_str)) = db.get_schema_type_with_session(&session.id, table, col_expr).await {
                                        let type_oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(&pg_type_str);
                                        info!("Ultra-fast path: Pre-execution type inference for '{}' from table '{}' -> {} (OID {})", 
                                              col_name, table, pg_type_str, type_oid);
                                        inferred_types.push(type_oid);
                                        found_type = true;
                                    }
                                }
                                
                                if !found_type {
                                    // Default to TEXT if we can't resolve
                                    info!("Ultra-fast path: Could not infer type for '{}', defaulting to TEXT", col_name);
                                    inferred_types.push(PgType::Text.to_oid());
                                }
                            }
                        } else {
                            info!("Ultra-fast path: Could not find FROM clause in query, cannot parse columns");
                        }
                        
                        info!("Ultra-fast path: Inferred {} types", inferred_types.len());
                        inferred_types
                    }
                } else {
                    Vec::new()
                }
            };
            
            // Convert binary parameters to text format for SQLite
            // SQLite doesn't understand PostgreSQL binary format
            let converted_values: Vec<Option<Vec<u8>>> = bound_values.iter()
                .enumerate()
                .map(|(i, value)| {
                    match value {
                        None => None,
                        Some(bytes) => {
                            let format = param_formats.get(i).copied().unwrap_or(0);
                            let param_type = param_types.get(i).copied().unwrap_or(0);
                            
                            if format == 1 {
                                // Binary format - need to convert to text for SQLite
                                match param_type {
                                    t if t == PgType::Int2.to_oid() => {
                                        if bytes.len() == 2 {
                                            let val = i16::from_be_bytes([bytes[0], bytes[1]]);
                                            Some(val.to_string().into_bytes())
                                        } else {
                                            Some(bytes.clone())
                                        }
                                    }
                                    t if t == PgType::Int4.to_oid() => {
                                        if bytes.len() == 4 {
                                            let val = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                                            Some(val.to_string().into_bytes())
                                        } else if bytes.len() == 2 {
                                            // INT2 sent as INT4
                                            let val = i16::from_be_bytes([bytes[0], bytes[1]]);
                                            Some(val.to_string().into_bytes())
                                        } else {
                                            Some(bytes.clone())
                                        }
                                    }
                                    t if t == PgType::Int8.to_oid() => {
                                        if bytes.len() == 8 {
                                            let val = i64::from_be_bytes([
                                                bytes[0], bytes[1], bytes[2], bytes[3],
                                                bytes[4], bytes[5], bytes[6], bytes[7]
                                            ]);
                                            Some(val.to_string().into_bytes())
                                        } else {
                                            Some(bytes.clone())
                                        }
                                    }
                                    t if t == PgType::Float4.to_oid() => {
                                        if bytes.len() == 4 {
                                            let val = f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                                            Some(val.to_string().into_bytes())
                                        } else {
                                            Some(bytes.clone())
                                        }
                                    }
                                    t if t == PgType::Float8.to_oid() => {
                                        if bytes.len() == 8 {
                                            let val = f64::from_be_bytes([
                                                bytes[0], bytes[1], bytes[2], bytes[3],
                                                bytes[4], bytes[5], bytes[6], bytes[7]
                                            ]);
                                            Some(val.to_string().into_bytes())
                                        } else {
                                            Some(bytes.clone())
                                        }
                                    }
                                    t if t == PgType::Bool.to_oid() => {
                                        if bytes.len() == 1 {
                                            let val = if bytes[0] == 0 { "f" } else { "t" };
                                            Some(val.as_bytes().to_vec())
                                        } else {
                                            Some(bytes.clone())
                                        }
                                    }
                                    t if t == PgType::Timestamp.to_oid() || t == PgType::Timestamptz.to_oid() => {
                                        // PostgreSQL sends timestamps as int64 microseconds since 2000-01-01
                                        if bytes.len() == 8 {
                                            let pg_microseconds = i64::from_be_bytes([
                                                bytes[0], bytes[1], bytes[2], bytes[3],
                                                bytes[4], bytes[5], bytes[6], bytes[7]
                                            ]);
                                            // Convert from PostgreSQL epoch (2000-01-01) to Unix epoch (1970-01-01)
                                            // Difference is 946684800 seconds = 946684800000000 microseconds
                                            let unix_microseconds = pg_microseconds + 946_684_800_000_000;
                                            // Store as microseconds for SQLite
                                            Some(unix_microseconds.to_string().into_bytes())
                                        } else {
                                            Some(bytes.clone())
                                        }
                                    }
                                    0 => {
                                        // Unknown type - try to infer from length
                                        // psycopg3 sends numeric values as 8-byte floats when type is unknown
                                        if bytes.len() == 8 {
                                            // Try interpreting as float64 (common for numeric values from psycopg3)
                                            let val = f64::from_be_bytes([
                                                bytes[0], bytes[1], bytes[2], bytes[3],
                                                bytes[4], bytes[5], bytes[6], bytes[7]
                                            ]);
                                            Some(val.to_string().into_bytes())
                                        } else if bytes.len() == 4 {
                                            // Could be int32 or float32
                                            // Try int32 first (more common)
                                            let val = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                                            Some(val.to_string().into_bytes())
                                        } else {
                                            // Keep as-is if we can't determine type
                                            Some(bytes.clone())
                                        }
                                    }
                                    _ => {
                                        // For other types or if we can't convert, keep as-is
                                        // This might fail, but at least integers will work
                                        Some(bytes.clone())
                                    }
                                }
                            } else {
                                // Text format - pass through as-is
                                Some(bytes.clone())
                            }
                        }
                    }
                })
                .collect();
            
            match db.execute_with_params(query_to_execute, &converted_values, &session.id).await {
                Ok(response) => {
                        // Send RowDescription if needed
                        let send_row_desc = {
                            let statements = session.prepared_statements.read().await;
                            if let Some(stmt) = statements.get(&statement_name) {
                                stmt.field_descriptions.is_empty()
                            } else {
                                true
                            }
                        };
                        
                        if send_row_desc {
                            // Use the pre-inferred types
                            let fields: Vec<FieldDescription> = response.columns.iter()
                                .enumerate()
                                .map(|(i, name)| FieldDescription {
                                    name: name.clone(),
                                    table_oid: 0,
                                    column_id: (i + 1) as i16,
                                    type_oid: field_types.get(i).copied().unwrap_or_else(|| PgType::Text.to_oid()),
                                    type_size: -1,
                                    type_modifier: -1,
                                    format: 0,
                                })
                                .collect();
                            framed.send(BackendMessage::RowDescription(fields)).await
                                .map_err(PgSqliteError::Io)?;
                        }
                        
                        // Send data rows
                        let row_count = response.rows.len();
                        
                        // Default to text format for ultra-fast path
                        let result_formats = vec![0i16; response.columns.len()];
                        
                        for row in response.rows {
                            // Convert row data to handle datetime types properly
                            for (i, field_type) in field_types.iter().enumerate() {
                                if let Some(Some(value)) = row.get(i)
                                    && let Ok(s) = std::str::from_utf8(value) {
                                        // Check if this is a timestamp value that needs conversion
                                        if *field_type == PgType::Timestamp.to_oid() || *field_type == PgType::Timestamptz.to_oid() {
                                            info!("  Column {}: TIMESTAMP type OID {}, raw value: '{}'", i, field_type, s);
                                        } else if *field_type == PgType::Text.to_oid() && s.parse::<i64>().is_ok() {
                                            info!("  Column {}: TEXT type with numeric value: '{}'", i, s);
                                        }
                                    }
                            }
                            let encoded_row = Self::encode_row(&row, &result_formats, &field_types)?;
                            framed.send(BackendMessage::DataRow(encoded_row)).await
                                .map_err(PgSqliteError::Io)?;
                        }
                        
                        framed.send(BackendMessage::CommandComplete { 
                            tag: format!("SELECT {row_count}") 
                        }).await.map_err(PgSqliteError::Io)?;
                        
                        // Portal management for suspended queries
                        if max_rows > 0 && row_count >= max_rows as usize {
                            // Portal suspended - but we consumed all rows
                            framed.send(BackendMessage::PortalSuspended).await
                                .map_err(PgSqliteError::Io)?;
                        }
                        
                        return Ok(());
                    }
                    Err(_) => {
                        // Fall through to regular path
                    }
                }
        }
        
        // Try optimized extended fast path first for parameterized queries
        if !bound_values.is_empty() && effective_query.contains('$') {
            let query_type = super::extended_fast_path::QueryType::from_query(effective_query);
            
            // Early check: Skip fast path for SELECT with binary results
            if matches!(query_type, super::extended_fast_path::QueryType::Select) 
                && !result_formats.is_empty() 
                && result_formats[0] == 1 {
                // Skipping fast path: binary results
                // Skip fast path entirely for binary SELECT results
            } else {
            
            // Get original types from cache if available
            let original_types = if let Some(cached_info) = GLOBAL_PARAMETER_CACHE.get(effective_query) {
                cached_info.original_types
            } else {
                param_types.clone()
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
                        effective_query,
                        &bound_values,
                        &param_formats,
                        &result_formats,
                        &param_types,
                        &original_types,
                        query_type,
                    ).await {
                        Ok(true) => {
                            return Ok(());
                        }, // Successfully executed via fast path
                        Ok(false) => {
                        }, // Fall back to normal path
                        Err(_) => {
                            // Extended fast path failed, falling back to normal path
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
            if !bound_values.is_empty() && query.contains('$')
                && let Ok(Some(result)) = Self::try_execute_fast_path_with_params(
                    framed, 
                    db, 
                    session, 
                    &portal, 
                    &query, 
                    &bound_values, 
                    &param_formats, 
                    &param_types,
                    &fast_query, 
                    max_rows
                ).await {
                    return result;
                }
        }

        // Use translated query if available, otherwise use original
        let query_to_use = translated_query.as_ref().unwrap_or(&query);
        
        // Validate numeric constraints before parameter substitution
        let validation_error = if query_starts_with_ignore_case(query_to_use, "INSERT") {
            if let Some(table_name) = Self::extract_table_name_from_insert(query_to_use) {
                // For parameterized queries, we need to check constraints with actual values
                // Build a substituted query just for validation
                let validation_query = Self::substitute_parameters(query_to_use, &bound_values, &param_formats, &param_types)?;
                
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
                let validation_query = Self::substitute_parameters(query_to_use, &bound_values, &param_formats, &param_types)?;
                
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
        
        // Convert bound values and substitute parameters
        let mut final_query = Self::substitute_parameters(query_to_use, &bound_values, &param_formats, &param_types)?;
        
        // Apply JSON operator translation if needed
        if JsonTranslator::contains_json_operations(&final_query) {
            debug!("Query needs JSON operator translation: {}", final_query);
            match JsonTranslator::translate_json_operators(&final_query) {
                Ok(translated) => {
                    debug!("Query after JSON operator translation: {}", translated);
                    final_query = translated;
                }
                Err(_) => {
                    // JSON operator translation failed
                    // Continue with original query - some operators might not be supported yet
                }
            }
        }
        
        debug!("Executing query: {}", final_query);
        debug!("Original query: {}", query);
        debug!("Final query after substitution: {}", final_query);
        debug!("Original query had {} bound values", bound_values.len());
        
        
        // Debug: Check if this is a catalog query
        if final_query.contains("pg_catalog") || final_query.contains("pg_type") ||
           final_query.contains("pg_constraint") || final_query.contains("pg_index") ||
           final_query.contains("pg_depend") {
            info!("Detected catalog query in extended protocol: {}", final_query);
        }
        
        // Execute based on query type
        if query_starts_with_ignore_case(&final_query, "SELECT") {
            Self::execute_select(framed, db, session, &portal, &final_query, max_rows).await?;
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
                                   query.contains("pg_attribute") || query.contains("pg_constraint") ||
                                   query.contains("pg_index") || query.contains("pg_depend") ||
                                   query.contains("information_schema");
            
            // Then send RowDescription or NoData
            if !stmt.field_descriptions.is_empty() {
                info!("Sending RowDescription with {} fields in Describe", stmt.field_descriptions.len());

                // Fix field types for catalog queries before sending RowDescription
                let mut corrected_fields = stmt.field_descriptions.clone();
                if is_catalog_query || query.contains("pg_attribute") || query.contains("a.attnotnull") || query.contains("a.atthasdef") {
                    for fd in &mut corrected_fields {
                        let col_lower = fd.name.to_lowercase();
                        match col_lower.as_str() {
                            // Direct pg_attribute boolean columns
                            "attnotnull" | "atthasdef" | "attbyval" | "atthasmissing" | "attisdropped" | "attislocal" |
                            // Common aliases for these columns in JOIN queries
                            "not_null" | "has_default" | "is_not_null" | "has_def" => {
                                info!("Correcting field '{}' from type_oid {} to Bool type_oid {}", fd.name, fd.type_oid, PgType::Bool.to_oid());
                                fd.type_oid = PgType::Bool.to_oid();
                            }
                            "attidentity" | "attgenerated" | "attalign" | "attstorage" | "attcompression" => {
                                info!("Correcting field '{}' from type_oid {} to Char type_oid {}", fd.name, fd.type_oid, PgType::Char.to_oid());
                                fd.type_oid = PgType::Char.to_oid();
                            }
                            _ => {}
                        }
                    }
                }

                for (i, fd) in corrected_fields.iter().enumerate() {
                    info!("Field {}: name='{}', type_oid={}, table_oid={}", i, fd.name, fd.type_oid, fd.table_oid);
                }
                framed.send(BackendMessage::RowDescription(corrected_fields)).await
                    .map_err(PgSqliteError::Io)?;
            } else if is_catalog_query && query_starts_with_ignore_case(query, "SELECT") {
                // For catalog SELECT queries, we need to provide field descriptions
                // even though we skipped them during Parse
                info!("Catalog query detected in Describe, generating field descriptions for: {}", query);
                
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
                                } else if query.contains("pg_constraint") {
                                    // Return all pg_constraint columns
                                    let all_columns = vec![
                                        ("oid", PgType::Text.to_oid()), // Returned as text for now
                                        ("conname", PgType::Text.to_oid()),
                                        ("connamespace", PgType::Text.to_oid()), // Returned as text for now
                                        ("contype", PgType::Char.to_oid()),
                                        ("condeferrable", PgType::Bool.to_oid()),
                                        ("condeferred", PgType::Bool.to_oid()),
                                        ("convalidated", PgType::Bool.to_oid()),
                                        ("conrelid", PgType::Text.to_oid()), // Returned as text for now
                                        ("contypid", PgType::Text.to_oid()), // Returned as text for now
                                        ("conindid", PgType::Text.to_oid()), // Returned as text for now
                                        ("conparentid", PgType::Text.to_oid()), // Returned as text for now
                                        ("confrelid", PgType::Text.to_oid()), // Returned as text for now
                                        ("confupdtype", PgType::Char.to_oid()),
                                        ("confdeltype", PgType::Char.to_oid()),
                                        ("confmatchtype", PgType::Char.to_oid()),
                                        ("conislocal", PgType::Bool.to_oid()),
                                        ("coninhcount", PgType::Int4.to_oid()),
                                        ("connoinherit", PgType::Bool.to_oid()),
                                        ("conkey", PgType::Text.to_oid()), // Simplified - actually int2[]
                                        ("confkey", PgType::Text.to_oid()), // Simplified - actually int2[]
                                        ("conpfeqop", PgType::Text.to_oid()), // Simplified - actually oid[]
                                        ("conppeqop", PgType::Text.to_oid()), // Simplified - actually oid[]
                                        ("conffeqop", PgType::Text.to_oid()), // Simplified - actually oid[]
                                        ("confdelsetcols", PgType::Text.to_oid()), // Simplified - actually int2[]
                                        ("conexclop", PgType::Text.to_oid()), // Simplified - actually oid[]
                                        ("conbin", PgType::Text.to_oid()), // Simplified - actually pg_node_tree
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
                                } else if query.contains("pg_depend") {
                                    // Return all pg_depend columns
                                    let all_columns = vec![
                                        ("classid", PgType::Text.to_oid()), // Returned as text for now
                                        ("objid", PgType::Text.to_oid()), // Returned as text for now
                                        ("objsubid", PgType::Int4.to_oid()),
                                        ("refclassid", PgType::Text.to_oid()), // Returned as text for now
                                        ("refobjid", PgType::Text.to_oid()), // Returned as text for now
                                        ("refobjsubid", PgType::Int4.to_oid()),
                                        ("deptype", PgType::Char.to_oid()),
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
                                } else if query.contains("information_schema.schemata") {
                                    // Return all information_schema.schemata columns
                                    let all_columns = vec![
                                        ("catalog_name", PgType::Text.to_oid()),
                                        ("schema_name", PgType::Text.to_oid()),
                                        ("schema_owner", PgType::Text.to_oid()),
                                        ("default_character_set_catalog", PgType::Text.to_oid()),
                                        ("default_character_set_schema", PgType::Text.to_oid()),
                                        ("default_character_set_name", PgType::Text.to_oid()),
                                        ("sql_path", PgType::Text.to_oid()),
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
                                } else if query.contains("information_schema.tables") {
                                    // Return all information_schema.tables columns
                                    let all_columns = vec![
                                        ("table_catalog", PgType::Text.to_oid()),
                                        ("table_schema", PgType::Text.to_oid()),
                                        ("table_name", PgType::Text.to_oid()),
                                        ("table_type", PgType::Text.to_oid()),
                                        ("self_referencing_column_name", PgType::Text.to_oid()),
                                        ("reference_generation", PgType::Text.to_oid()),
                                        ("user_defined_type_catalog", PgType::Text.to_oid()),
                                        ("user_defined_type_schema", PgType::Text.to_oid()),
                                        ("user_defined_type_name", PgType::Text.to_oid()),
                                        ("is_insertable_into", PgType::Text.to_oid()),
                                        ("is_typed", PgType::Text.to_oid()),
                                        ("commit_action", PgType::Text.to_oid()),
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
                                } else if query.contains("information_schema.columns") {
                                    // Return all information_schema.columns columns (44 total)
                                    let all_columns = vec![
                                        ("table_catalog", PgType::Text.to_oid()),
                                        ("table_schema", PgType::Text.to_oid()),
                                        ("table_name", PgType::Text.to_oid()),
                                        ("column_name", PgType::Text.to_oid()),
                                        ("ordinal_position", PgType::Int4.to_oid()),
                                        ("column_default", PgType::Text.to_oid()),
                                        ("is_nullable", PgType::Text.to_oid()),
                                        ("data_type", PgType::Text.to_oid()),
                                        ("character_maximum_length", PgType::Int4.to_oid()),
                                        ("character_octet_length", PgType::Int4.to_oid()),
                                        ("numeric_precision", PgType::Int4.to_oid()),
                                        ("numeric_precision_radix", PgType::Int4.to_oid()),
                                        ("numeric_scale", PgType::Int4.to_oid()),
                                        ("datetime_precision", PgType::Int4.to_oid()),
                                        ("interval_type", PgType::Text.to_oid()),
                                        ("interval_precision", PgType::Int4.to_oid()),
                                        ("character_set_catalog", PgType::Text.to_oid()),
                                        ("character_set_schema", PgType::Text.to_oid()),
                                        ("character_set_name", PgType::Text.to_oid()),
                                        ("collation_catalog", PgType::Text.to_oid()),
                                        ("collation_schema", PgType::Text.to_oid()),
                                        ("collation_name", PgType::Text.to_oid()),
                                        ("domain_catalog", PgType::Text.to_oid()),
                                        ("domain_schema", PgType::Text.to_oid()),
                                        ("domain_name", PgType::Text.to_oid()),
                                        ("udt_catalog", PgType::Text.to_oid()),
                                        ("udt_schema", PgType::Text.to_oid()),
                                        ("udt_name", PgType::Text.to_oid()),
                                        ("scope_catalog", PgType::Text.to_oid()),
                                        ("scope_schema", PgType::Text.to_oid()),
                                        ("scope_name", PgType::Text.to_oid()),
                                        ("maximum_cardinality", PgType::Int4.to_oid()),
                                        ("dtd_identifier", PgType::Text.to_oid()),
                                        ("is_self_referencing", PgType::Text.to_oid()),
                                        ("is_identity", PgType::Text.to_oid()),
                                        ("identity_generation", PgType::Text.to_oid()),
                                        ("identity_start", PgType::Text.to_oid()),
                                        ("identity_increment", PgType::Text.to_oid()),
                                        ("identity_maximum", PgType::Text.to_oid()),
                                        ("identity_minimum", PgType::Text.to_oid()),
                                        ("identity_cycle", PgType::Text.to_oid()),
                                        ("is_generated", PgType::Text.to_oid()),
                                        ("generation_expression", PgType::Text.to_oid()),
                                        ("is_updatable", PgType::Text.to_oid()),
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
                                } else if query.contains("information_schema.key_column_usage") {
                                    // Return all information_schema.key_column_usage columns (9 total)
                                    let all_columns = vec![
                                        ("constraint_catalog", PgType::Text.to_oid()),
                                        ("constraint_schema", PgType::Text.to_oid()),
                                        ("constraint_name", PgType::Text.to_oid()),
                                        ("table_catalog", PgType::Text.to_oid()),
                                        ("table_schema", PgType::Text.to_oid()),
                                        ("table_name", PgType::Text.to_oid()),
                                        ("column_name", PgType::Text.to_oid()),
                                        ("ordinal_position", PgType::Int4.to_oid()),
                                        ("position_in_unique_constraint", PgType::Int4.to_oid()),
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
                                } else if query.contains("information_schema.table_constraints") {
                                    // Return all information_schema.table_constraints columns
                                    let all_columns = vec![
                                        ("constraint_catalog", PgType::Text.to_oid()),
                                        ("constraint_schema", PgType::Text.to_oid()),
                                        ("constraint_name", PgType::Text.to_oid()),
                                        ("table_catalog", PgType::Text.to_oid()),
                                        ("table_schema", PgType::Text.to_oid()),
                                        ("table_name", PgType::Text.to_oid()),
                                        ("constraint_type", PgType::Text.to_oid()),
                                        ("is_deferrable", PgType::Text.to_oid()),
                                        ("initially_deferred", PgType::Text.to_oid()),
                                        ("enforced", PgType::Text.to_oid()),
                                        ("nulls_distinct", PgType::Text.to_oid()),
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
                if let Some(ref inferred_types) = portal.inferred_param_types {
                    info!("Describe portal: inferred types available: {:?}", inferred_types);
                    info!("Describe portal: field count: {}", fields.len());
                    
                    // For queries like SELECT $1, $2, $3, each parameter creates a column
                    // The columns might be named NULL, ?column?, or $1, $2, etc.
                    let mut param_column_count = 0;
                    
                    for (col_idx, field) in fields.iter_mut().enumerate() {
                        // Check if this is a parameter column
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
                
                // Update format fields based on result_formats from the portal
                for (i, field) in fields.iter_mut().enumerate() {
                    field.format = if portal.result_formats.is_empty() {
                        0 // Default to text
                    } else if portal.result_formats.len() == 1 {
                        portal.result_formats[0] // Single format for all columns
                    } else if i < portal.result_formats.len() {
                        portal.result_formats[i] // Column-specific format
                    } else {
                        0 // Default to text if not enough formats
                    };
                }
                
                info!("Describe portal: sending updated fields: {:?}", fields);
                
                // Update the statement's field_descriptions with the binary format
                // so we know we've already sent RowDescription with binary format
                let statement_name = portal.statement_name.clone();
                drop(portals);
                drop(statements);
                let mut statements = session.prepared_statements.write().await;
                if let Some(stmt) = statements.get_mut(&statement_name) {
                    stmt.field_descriptions = fields.clone();
                }
                drop(statements);
                
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
    
    #[allow(clippy::too_many_arguments)]
    async fn try_execute_fast_path_with_params<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &Arc<DbHandler>,
        session: &Arc<SessionState>,
        portal: &str,
        query: &str,
        bound_values: &[Option<Vec<u8>>],
        param_formats: &[i16],
        param_types: &[i32],
        fast_query: &crate::query::FastPathQuery,
        max_rows: i32,
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
                    let param_type = param_types.get(i).unwrap_or(&25); // Default to TEXT
                    
                    match Self::convert_parameter_to_value(bytes, *format, *param_type) {
                        Ok(sql_value) => rusqlite_params.push(sql_value),
                        Err(_) => return Ok(None), // Fall back to normal path on conversion error
                    }
                }
                None => rusqlite_params.push(rusqlite::types::Value::Null),
            }
        }
        
        // Get result formats and statement name from portal
        let (result_formats, statement_name) = {
            let portals = session.portals.read().await;
            let portal_obj = portals.get(portal).unwrap();
            (portal_obj.result_formats.clone(), portal_obj.statement_name.clone())
        };
        
        // Get field descriptions from prepared statement if available
        // Also check if we've already sent RowDescription with binary format
        let (field_types, has_binary_row_desc): (Option<Vec<i32>>, bool) = {
            let statements = session.prepared_statements.read().await;
            if let Some(stmt) = statements.get(&statement_name) {
                if !stmt.field_descriptions.is_empty() {
                    // Check if any field has binary format, which means Describe(Portal) sent RowDescription
                    let has_binary = stmt.field_descriptions.iter().any(|fd| fd.format == 1);
                    (Some(stmt.field_descriptions.iter().map(|fd| fd.type_oid).collect()), has_binary)
                } else {
                    (None, false)
                }
            } else {
                (None, false)
            }
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
                if has_binary_row_desc {
                    // Describe(Portal) already sent RowDescription with binary format
                    // Just send the data rows without RowDescription
                    Self::send_data_rows_only(framed, response, &result_formats, field_types.as_deref()).await?;
                } else {
                    // Send full response with RowDescription
                    Self::send_select_response(framed, response, max_rows, &result_formats, field_types.as_deref()).await?;
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
                if has_binary_row_desc {
                    // Describe(Portal) already sent RowDescription with binary format
                    // Just send the data rows without RowDescription
                    Self::send_data_rows_only(framed, response, &result_formats, field_types.as_deref()).await?;
                } else {
                    // Send full response with RowDescription
                    Self::send_select_response(framed, response, max_rows, &result_formats, field_types.as_deref()).await?;
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
            // Binary format - simplified for now, fall back to normal path
            Err(PgSqliteError::Protocol("Binary format not supported in fast path".to_string()))
        }
    }
    
    async fn send_data_rows_only<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        response: crate::session::db_handler::DbResponse,
        result_formats: &[i16],
        field_types: Option<&[i32]>,  // Optional field types
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        debug!("send_data_rows_only called with {} rows", response.rows.len());
        // Send only DataRow messages (no RowDescription)
        // This is used when RowDescription was already sent by Describe(Portal)
        
        // Check if we need binary encoding
        let needs_binary_encoding = !result_formats.is_empty() && 
            result_formats.contains(&1);
        
        if needs_binary_encoding {
            if let Some(types) = field_types {
                for row in response.rows {
                    let encoded_row = Self::encode_row(&row, result_formats, types)?;
                    framed.send(BackendMessage::DataRow(encoded_row)).await?;
                }
            } else {
                // Send as-is (text format) if no types available
                for row in response.rows {
                    framed.send(BackendMessage::DataRow(row)).await?;
                }
            }
        } else {
            // Send as-is (text format)
            for row in response.rows {
                framed.send(BackendMessage::DataRow(row)).await?;
            }
        }
        
        // Send CommandComplete
        framed.send(BackendMessage::CommandComplete { 
            tag: "SELECT".to_string()  // We don't have row count here
        }).await?;
        
        Ok(())
    }
    
    async fn send_select_response<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        response: crate::session::db_handler::DbResponse,
        _max_rows: i32,
        result_formats: &[i16],
        field_types: Option<&[i32]>,  // Optional field types
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        debug!("send_select_response called with {} columns: {:?}", response.columns.len(), response.columns);
        // Always send RowDescription from this function
        // This is only called from fast paths which don't use Describe(Portal)
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
            
            // Use provided type or default to TEXT
            let type_oid = field_types
                .and_then(|types| types.get(i))
                .copied()
                .unwrap_or(25); // Default to TEXT
            
            field_descriptions.push(FieldDescription {
                name: column_name.clone(),
                table_oid: 0,
                column_id: (i + 1) as i16,
                type_oid,
                type_size: -1,
                type_modifier: -1,
                format,
            });
        }
        framed.send(BackendMessage::RowDescription(field_descriptions)).await?;
        
        // Check if we need conversion for timestamps OR TEXT columns that might contain timestamps
        let needs_conversion = field_types
            .map(|types| {
                let has_timestamp = types.iter().any(|&t| 
                    t == PgType::Timestamp.to_oid() || 
                    t == PgType::Timestamptz.to_oid() ||
                    t == PgType::Text.to_oid()  // TEXT columns might contain timestamps
                );
                info!("send_select_response: field_types={:?}, needs_conversion={}", types, has_timestamp);
                has_timestamp
            })
            .unwrap_or(false);
        
        if needs_conversion {
            if let Some(types) = field_types {
                // Send DataRows with timestamp conversion
                for row in response.rows {
                let mut converted_row = Vec::new();
                for (i, cell) in row.iter().enumerate() {
                    let type_oid = types.get(i).copied().unwrap_or(25);
                    
                    // Handle explicit timestamp columns
                    if type_oid == PgType::Timestamp.to_oid() || type_oid == PgType::Timestamptz.to_oid() {
                        if let Some(bytes) = cell {
                            if let Ok(s) = std::str::from_utf8(bytes) {
                                if let Ok(micros) = s.parse::<i64>() {
                                    // Convert microseconds to formatted timestamp
                                    use crate::types::datetime_utils::format_microseconds_to_timestamp;
                                    let formatted = format_microseconds_to_timestamp(micros);
                                    converted_row.push(Some(formatted.into_bytes()));
                                } else {
                                    // Already formatted or not a timestamp
                                    converted_row.push(cell.clone());
                                }
                            } else {
                                converted_row.push(cell.clone());
                            }
                        } else {
                            converted_row.push(None);
                        }
                    }
                    // Handle TEXT columns that might contain timestamp microseconds
                    else if type_oid == PgType::Text.to_oid() {
                        if let Some(bytes) = cell {
                            if let Ok(s) = std::str::from_utf8(bytes) {
                                // Try to parse as integer microseconds
                                if let Ok(micros) = s.parse::<i64>() {
                                    // Check if this looks like microseconds since epoch
                                    // Valid timestamp range: roughly 1970-2100 (0 to ~4.1 trillion microseconds)
                                    // We check for values > 100 billion to avoid converting small integers
                                    if micros > 100_000_000_000 && micros < 4_102_444_800_000_000 {
                                        // This is likely a datetime value stored as INTEGER microseconds
                                        use crate::types::datetime_utils::format_microseconds_to_timestamp;
                                        let formatted = format_microseconds_to_timestamp(micros);
                                        info!("Converting TEXT column timestamp value {} to formatted: {}", micros, formatted);
                                        converted_row.push(Some(formatted.into_bytes()));
                                    } else {
                                        // Not a timestamp, keep as-is
                                        converted_row.push(cell.clone());
                                    }
                                } else {
                                    // Not an integer, keep as-is
                                    converted_row.push(cell.clone());
                                }
                            } else {
                                converted_row.push(cell.clone());
                            }
                        } else {
                            converted_row.push(None);
                        }
                    } else {
                        converted_row.push(cell.clone());
                    }
                }
                framed.send(BackendMessage::DataRow(converted_row)).await?;
            }
            }
        } else {
            // No conversion needed, but still need to apply binary encoding if requested
            // Check if binary format is requested
            let needs_binary_encoding = !result_formats.is_empty() && 
                (result_formats.len() == 1 && result_formats[0] == 1 || 
                 result_formats.contains(&1));
            
            if needs_binary_encoding {
                if let Some(types) = field_types {
                    // Apply binary encoding to results
                    for row in response.rows {
                        let encoded_row = Self::encode_row(&row, result_formats, types)?;
                        framed.send(BackendMessage::DataRow(encoded_row)).await?;
                    }
                } else {
                    // Send as-is (text format) if no types available
                    for row in response.rows {
                        framed.send(BackendMessage::DataRow(row)).await?;
                    }
                }
            } else {
                // Send as-is (text format)
                for row in response.rows {
                    framed.send(BackendMessage::DataRow(row)).await?;
                }
            }
        }
        
        // Send CommandComplete
        framed.send(BackendMessage::CommandComplete { tag: format!("SELECT {}", response.rows_affected) }).await?;
        
        Ok(())
    }
    
    fn substitute_parameters(query: &str, values: &[Option<Vec<u8>>], formats: &[i16], param_types: &[i32]) -> Result<String, PgSqliteError> {
        // Convert parameter values to strings for substitution
        let mut string_values = Vec::new();
        
        for (i, value) in values.iter().enumerate() {
            let format = formats.get(i).copied().unwrap_or(0); // Default to text format
            let param_type = param_types.get(i).copied().unwrap_or(PgType::Text.to_oid()); // Default to text
            
            info!("Processing parameter {}: format={}, type_oid={}, bytes_len={}", 
                i + 1, format, param_type, 
                value.as_ref().map(|v| v.len()).unwrap_or(0));
            
            let replacement = match value {
                None => "NULL".to_string(),
                Some(bytes) => {
                    if format == 1 {
                        // Binary format - decode based on expected type
                        match param_type {
                            t if t == PgType::Int2.to_oid() => {
                                // int2 (smallint)
                                if bytes.len() == 2 {
                                    let value = i16::from_be_bytes([bytes[0], bytes[1]]);
                                    info!("Decoded binary int16 parameter {}: {}", i + 1, value);
                                    value.to_string()
                                } else {
                                    format!("X'{}'", hex::encode(bytes))
                                }
                            }
                            t if t == PgType::Int4.to_oid() => {
                                // int4 - but sometimes PostgreSQL sends int2 with int4 type OID
                                if bytes.len() == 4 {
                                    let value = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                                    info!("Decoded binary int32 parameter {}: {}", i + 1, value);
                                    value.to_string()
                                } else if bytes.len() == 2 {
                                    // Actually int2 but with int4 type OID
                                    let value = i16::from_be_bytes([bytes[0], bytes[1]]);
                                    info!("Decoded binary int16 (as int4) parameter {}: {}", i + 1, value);
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
                                    info!("Decoded binary int64 parameter {}: {}", i + 1, value);
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
                                    info!("Decoded binary money parameter {}: {} cents -> {}", i + 1, cents, formatted);
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
                                        info!("Decoded binary numeric parameter {}: {}", i + 1, s);
                                        format!("'{}'", s.replace('\'', "''"))
                                    }
                                    Err(e) => {
                                        // Failed to decode binary NUMERIC parameter
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
                            t if t == PgType::Timestamp.to_oid() || t == PgType::Timestamptz.to_oid() => {
                                // timestamp/timestamptz - int8 microseconds since PostgreSQL epoch (2000-01-01)
                                if bytes.len() == 8 {
                                    let pg_micros = i64::from_be_bytes([
                                        bytes[0], bytes[1], bytes[2], bytes[3],
                                        bytes[4], bytes[5], bytes[6], bytes[7]
                                    ]);
                                    
                                    // Convert PostgreSQL microseconds to Unix microseconds
                                    const PG_EPOCH_OFFSET: i64 = 946684800 * 1_000_000; // microseconds between 1970-01-01 and 2000-01-01
                                    let unix_micros = pg_micros + PG_EPOCH_OFFSET;
                                    
                                    info!("Decoded binary timestamp parameter {}: {} PG microseconds = {} Unix microseconds", 
                                          i + 1, pg_micros, unix_micros);
                                    
                                    // Check if this is a VALUES clause that will be rewritten
                                    if query.contains("FROM (VALUES") && query.contains("SELECT CAST(p0") {
                                        // SQLAlchemy VALUES pattern - convert to formatted timestamp string
                                        // Convert microseconds to NaiveDateTime
                                        let seconds = unix_micros / 1_000_000;
                                        let nanos = ((unix_micros % 1_000_000) * 1000) as u32;
                                        
                                        if let Some(dt) = chrono::DateTime::from_timestamp(seconds, nanos).map(|dt| dt.naive_utc()) {
                                            // Format as ISO timestamp string for VALUES clause
                                            let formatted = dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
                                            info!("VALUES clause detected - formatting timestamp as: {}", formatted);
                                            format!("'{formatted}'")
                                        } else {
                                            // Fallback to raw microseconds if conversion fails
                                            unix_micros.to_string()
                                        }
                                    } else {
                                        // Normal query - use raw microseconds
                                        unix_micros.to_string()
                                    }
                                } else {
                                    format!("X'{}'", hex::encode(bytes))
                                }
                            }
                            t if t == PgType::Date.to_oid() => {
                                // date - int4 days since epoch
                                if bytes.len() == 4 {
                                    let days = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                                    info!("Decoded binary date parameter {}: {} days", i + 1, days);
                                    days.to_string()
                                } else {
                                    format!("X'{}'", hex::encode(bytes))
                                }
                            }
                            t if t == PgType::Time.to_oid() || t == PgType::Timetz.to_oid() => {
                                // time - int8 microseconds since midnight
                                if bytes.len() == 8 {
                                    let micros = i64::from_be_bytes([
                                        bytes[0], bytes[1], bytes[2], bytes[3],
                                        bytes[4], bytes[5], bytes[6], bytes[7]
                                    ]);
                                    info!("Decoded binary time parameter {}: {} microseconds", i + 1, micros);
                                    micros.to_string()
                                } else {
                                    format!("X'{}'", hex::encode(bytes))
                                }
                            }
                            0 => {
                                // No type specified - try to infer from byte pattern
                                if bytes.len() == 1 && (bytes[0] == 0 || bytes[0] == 1) {
                                    // Single byte 0 or 1 - likely boolean
                                    info!("Inferred boolean parameter {}: {}", i + 1, bytes[0]);
                                    bytes[0].to_string()
                                } else if bytes.len() == 2 {
                                    // Two bytes - likely int2
                                    let value = i16::from_be_bytes([bytes[0], bytes[1]]);
                                    info!("Inferred int16 parameter {}: {}", i + 1, value);
                                    value.to_string()
                                } else if bytes.len() == 4 {
                                    // Four bytes - likely int4
                                    let value = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                                    info!("Inferred int32 parameter {}: {}", i + 1, value);
                                    value.to_string()
                                } else if bytes.len() == 8 {
                                    // Eight bytes - could be int8 or float8
                                    // psycopg3 sends NUMERIC values as float8 when type is unknown
                                    let value = f64::from_be_bytes([
                                        bytes[0], bytes[1], bytes[2], bytes[3],
                                        bytes[4], bytes[5], bytes[6], bytes[7]
                                    ]);
                                    info!("Inferred float64 parameter {} (unknown type): {}", i + 1, value);
                                    value.to_string()
                                } else {
                                    // Unknown pattern - use hex
                                    info!("Unknown binary parameter type OID 0 for parameter {}, bytes: {}", i + 1, hex::encode(bytes));
                                    format!("X'{}'", hex::encode(bytes))
                                }
                            }
                            _ => {
                                // Other binary data - treat as blob
                                info!("Unknown binary parameter type OID {} for parameter {}, bytes: {}", param_type, i + 1, hex::encode(bytes));
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
                                                // Invalid NUMERIC parameter
                                                return Err(PgSqliteError::InvalidParameter(format!("Invalid NUMERIC value: {e}")));
                                            }
                                        }
                                    }
                                    t if t == PgType::Timestamp.to_oid() || t == PgType::Timestamptz.to_oid() => {
                                        // TIMESTAMP types - check if this is a VALUES clause that will be rewritten
                                        // If so, keep as formatted string to avoid double conversion
                                        if query.contains("FROM (VALUES") && query.contains("SELECT CAST(p0") {
                                            // SQLAlchemy VALUES pattern - keep as formatted string
                                            format!("'{}'", s.replace('\'', "''"))
                                        } else {
                                            // Normal query - convert to Unix timestamp
                                            match crate::types::ValueConverter::convert_timestamp_to_unix(&s) {
                                                Ok(unix_timestamp) => unix_timestamp,
                                                Err(e) => {
                                                    // Invalid TIMESTAMP parameter
                                                    return Err(PgSqliteError::InvalidParameter(format!("Invalid TIMESTAMP value: {e}")));
                                                }
                                            }
                                        }
                                    }
                                    t if t == PgType::Date.to_oid() => {
                                        // DATE type - convert to Unix timestamp
                                        match crate::types::ValueConverter::convert_date_to_unix(&s) {
                                            Ok(unix_timestamp) => unix_timestamp,
                                            Err(e) => {
                                                // Invalid DATE parameter
                                                return Err(PgSqliteError::InvalidParameter(format!("Invalid DATE value: {e}")));
                                            }
                                        }
                                    }
                                    t if t == PgType::Time.to_oid() || t == PgType::Timetz.to_oid() => {
                                        // TIME types - convert to seconds since midnight
                                        match crate::types::ValueConverter::convert_time_to_seconds(&s) {
                                            Ok(seconds) => seconds,
                                            Err(e) => {
                                                // Invalid TIME parameter
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
        // Also handle multi-word types like ::TIMESTAMP WITHOUT TIME ZONE, ::DOUBLE PRECISION, etc.
        let cast_regex = regex::Regex::new(r"::[a-zA-Z][a-zA-Z0-9_]*(?:\s+(?:WITHOUT|WITH)\s+TIME\s+ZONE|\s+PRECISION|\s+VARYING)?").unwrap();
        let result = cast_regex.replace_all(&result, "").to_string();
        
        // SQLite doesn't support VALUES with column aliases like "AS table_alias(col1, col2, ...)"
        // Replace ") AS imp_sen(p0, p1, p2, p3, p4, p5, p6, p7, sen_counter)" with just ")"
        let alias_regex = regex::Regex::new(r"\)\s+AS\s+\w+\s*\([^)]+\)").unwrap();
        let result = alias_regex.replace_all(&result, ")").to_string();
        
        // Handle SQLAlchemy's VALUES pattern which uses p0, p1, etc. column references
        // This pattern needs to be completely rewritten for SQLite
        if result.contains("SELECT CAST(p0") && result.contains("FROM (VALUES") {
            info!("Detected SQLAlchemy VALUES pattern, attempting to rewrite");
            
            // Find the positions of key parts
            if let (Some(table_start), Some(values_start), Some(values_end)) = (
                result.find("INSERT INTO "),
                result.find("FROM (VALUES "),
                result.rfind(")")
            ) {
                // Extract table and columns
                let table_part = &result[table_start + 12..values_start];
                if let Some(paren_pos) = table_part.find('(') {
                    let table_name = table_part[..paren_pos].trim();
                    let columns_end = table_part.find(')').unwrap_or(table_part.len());
                    let columns = &table_part[paren_pos..=columns_end];
                    
                    // Extract VALUES content
                    let values_content = &result[values_start + 13..values_end];
                    
                    // Extract RETURNING clause if present
                    let returning_clause = if let Some(ret_pos) = result.find(" RETURNING ") {
                        &result[ret_pos..]
                    } else {
                        ""
                    };
                    
                    // Parse values rows - need to handle nested parentheses properly
                    let mut all_values = Vec::new();
                    let mut current_value = String::new();
                    let mut paren_depth = 0;
                    let mut in_row = false;
                    
                    for ch in values_content.chars() {
                        match ch {
                            '(' => {
                                paren_depth += 1;
                                if paren_depth == 1 {
                                    in_row = true;
                                    current_value.clear();
                                } else {
                                    current_value.push(ch);
                                }
                            }
                            ')' => {
                                paren_depth -= 1;
                                if paren_depth == 0 && in_row {
                                    // End of row - remove the trailing counter value
                                    let values: Vec<&str> = current_value.split(", ").collect();
                                    if values.len() > 1 {
                                        // Skip the last value (sen_counter)
                                        let actual_values = &values[..values.len() - 1];
                                        all_values.push(format!("({})", actual_values.join(", ")));
                                    }
                                    in_row = false;
                                } else {
                                    current_value.push(ch);
                                }
                            }
                            _ => {
                                if in_row {
                                    current_value.push(ch);
                                }
                            }
                        }
                    }
                    
                    // Build the new query
                    if !all_values.is_empty() {
                        // Join all values and remove any remaining casts
                        let values_str = all_values.join(", ");
                        // Apply cast removal to the VALUES content
                        let values_str = cast_regex.replace_all(&values_str, "").to_string();
                        
                        let new_query = format!("INSERT INTO {table_name} {columns} VALUES {values_str}{returning_clause}");
                        info!("Rewrote SQLAlchemy VALUES pattern to: {}", new_query);
                        return Ok(new_query);
                    }
                }
            }
        }
        
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
        
        // Determine which catalog table based on column name patterns first, then query content
        // First check if this is a pg_attribute column (regardless of what tables are in the query)
        if column_name.starts_with("att") ||
           ["not_null", "has_default", "is_not_null", "has_def"].contains(&column_name) {
            match column_name {
                "attrelid" | "atttypid" | "attcollation" => OID_TYPE,
                "attname" | "attacl" | "attoptions" | "attfdwoptions" | "attmissingval" => PgType::Text.to_oid(),
                "attstattarget" | "attndims" | "attcacheoff" | "atttypmod" | "attinhcount" => PgType::Int4.to_oid(),
                "attlen" | "attnum" => PgType::Int2.to_oid(),
                "attbyval" | "attnotnull" | "atthasdef" | "atthasmissing" | "attisdropped" | "attislocal" |
                "not_null" | "has_default" | "is_not_null" | "has_def" => PgType::Bool.to_oid(),
                "attalign" | "attstorage" | "attcompression" | "attidentity" | "attgenerated" => PgType::Char.to_oid(),
                _ => PgType::Text.to_oid(),
            }
        } else if query.contains("pg_class") {
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
        } else if query.contains("pg_index") {
            match column_name {
                "indexrelid" | "indrelid" => OID_TYPE,
                "indnatts" | "indnkeyatts" => PgType::Int4.to_oid(),
                "indisunique" | "indisprimary" | "indisexclusion" | "indimmediate" |
                "indisclustered" | "indisvalid" | "indcheckxmin" | "indisready" |
                "indislive" | "indisreplident" => PgType::Bool.to_oid(),
                "indkey" | "indcollation" | "indclass" | "indoption" | "indexprs" | "indpred" => PgType::Text.to_oid(),
                _ => PgType::Text.to_oid(),
            }
        } else if query.contains("pg_constraint") {
            match column_name {
                "conname" => PgType::Text.to_oid(),
                "contype" | "confupdtype" | "confdeltype" | "confmatchtype" => PgType::Char.to_oid(),
                "condeferrable" | "condeferred" | "convalidated" | "conislocal" | "connoinherit" => PgType::Bool.to_oid(),
                "coninhcount" => PgType::Int4.to_oid(),
                // For now, return OID columns as Text since the handler returns string representations
                // This can be improved later to return proper OID binary format
                _ => PgType::Text.to_oid(),
            }
        } else if query.contains("pg_depend") {
            match column_name {
                "objsubid" | "refobjsubid" => PgType::Int4.to_oid(),
                "deptype" => PgType::Char.to_oid(),
                // For now, return OID columns as Text since the handler returns string representations
                // This can be improved later to return proper OID binary format
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
        let mut result = vec![0u8; 4];
        BigEndian::write_i32(&mut result, bit_count);
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
                        let mut buf = vec![0u8; 4];
                        BigEndian::write_i32(&mut buf, val);
                        buf
                    } else {
                        return None;
                    }
                }
                t if t == PgType::Int8.to_oid() => {
                    // int8
                    if let Ok(val) = lower_str.parse::<i64>() {
                        let mut buf = vec![0u8; 8];
                        BigEndian::write_i64(&mut buf, val);
                        buf
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
                        let mut buf = vec![0u8; 4];
                        BigEndian::write_i32(&mut buf, val);
                        buf
                    } else {
                        return None;
                    }
                }
                t if t == PgType::Int8.to_oid() => {
                    // int8
                    if let Ok(val) = upper_str.parse::<i64>() {
                        let mut buf = vec![0u8; 8];
                        BigEndian::write_i64(&mut buf, val);
                        buf
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
        info!("encode_row called with {} fields, {} result_formats, {} field_types",
              row.len(), result_formats.len(), field_types.len());
        info!("  result_formats: {:?}", result_formats);
        info!("  field_types: {:?}", field_types);

        // Log the first few values for debugging
        for (i, value) in row.iter().take(3).enumerate() {
            if let Some(bytes) = value {
                if let Ok(s) = std::str::from_utf8(bytes) {
                    debug!("  Field {}: '{}' (type OID {})", i, s, field_types.get(i).unwrap_or(&0));
                    // Extra debug for array types
                    if field_types.get(i).copied().unwrap_or(0) == 1007 {
                        info!("  DEBUG: Encoding INT4Array field {} with value '{}'", i, s);
                    }
                } else {
                    debug!("  Field {}: <binary data> (type OID {})", i, field_types.get(i).unwrap_or(&0));
                }
            } else {
                debug!("  Field {}: NULL (type OID {})", i, field_types.get(i).unwrap_or(&0));
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
                                        "1" | "t" | "true" | "TRUE" | "T" => 1u8,
                                        "0" | "f" | "false" | "FALSE" | "F" => 0u8,
                                        _ => {
                                            // Invalid boolean, keep as text
                                            encoded_row.push(Some(bytes.clone()));
                                            continue;
                                        }
                                    };
                                    Some(vec![val])
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::Int2.to_oid() => {
                                // int2 - convert text to binary
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    // Handle empty string as NULL
                                    if s.trim().is_empty() {
                                        None
                                    } else if let Ok(val) = s.parse::<i16>() {
                                        let mut buf = vec![0u8; 2];
                                        BigEndian::write_i16(&mut buf, val);
                                        Some(buf)
                                    } else {
                                        // If we can't parse it, return as text (fallback)
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::Int4.to_oid() => {
                                // int4 - convert text to binary
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    // Handle empty string as NULL
                                    if s.trim().is_empty() {
                                        None
                                    } else if let Ok(val) = s.parse::<i32>() {
                                        let mut buf = vec![0u8; 4];
                                        BigEndian::write_i32(&mut buf, val);
                                        Some(buf)
                                    } else {
                                        // If we can't parse it, return as text (fallback)
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::Int8.to_oid() => {
                                // int8 - convert text to binary
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    // Handle empty string as NULL
                                    if s.trim().is_empty() {
                                        None
                                    } else if let Ok(val) = s.parse::<i64>() {
                                        let mut buf = vec![0u8; 8];
                                        BigEndian::write_i64(&mut buf, val);
                                        Some(buf)
                                    } else {
                                        // If we can't parse it, return as text (fallback)
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::Float4.to_oid() => {
                                // float4 - convert text to binary
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Ok(val) = s.parse::<f32>() {
                                        let mut buf = vec![0u8; 4];
                                        BigEndian::write_f32(&mut buf, val);
                                        Some(buf)
                                    } else {
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::Float8.to_oid() => {
                                // float8 - convert text to binary
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Ok(val) = s.parse::<f64>() {
                                        let mut buf = vec![0u8; 8];
                                        BigEndian::write_f64(&mut buf, val);
                                        Some(buf)
                                    } else {
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            // Array types - now with binary support
                            t if t == PgType::BoolArray.to_oid() => {
                                // bool[] - encode JSON array to binary format
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Some(binary_data) = crate::protocol::binary::BinaryEncoder::encode_value(&rusqlite::types::Value::Text(s), type_oid, true) {
                                        Some(binary_data)
                                    } else {
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::Int2Array.to_oid() => {
                                // int2[] - encode JSON array to binary format
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Some(binary_data) = crate::protocol::binary::BinaryEncoder::encode_value(&rusqlite::types::Value::Text(s), type_oid, true) {
                                        Some(binary_data)
                                    } else {
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::Int4Array.to_oid() => {
                                // int4[] - encode JSON array to binary format
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    info!("DEBUG: Calling encode_value for INT4Array with value: '{}'", s);
                                    let encoded_result = crate::protocol::binary::BinaryEncoder::encode_value(
                                        &rusqlite::types::Value::Text(s.clone()), type_oid, true
                                    );
                                    if let Some(ref binary_data) = encoded_result {
                                        info!("DEBUG: Successfully encoded INT4Array to {} bytes", binary_data.len());
                                        Some(binary_data.clone())
                                    } else {
                                        info!("DEBUG: Failed to encode INT4Array, falling back to text");
                                        Some(bytes.clone())
                                    }
                                } else {
                                    info!("DEBUG: INT4Array data is not valid UTF-8");
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::Int8Array.to_oid() => {
                                // int8[] - encode JSON array to binary format
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Some(binary_data) = crate::protocol::binary::BinaryEncoder::encode_value(&rusqlite::types::Value::Text(s), type_oid, true) {
                                        Some(binary_data)
                                    } else {
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::Float4Array.to_oid() => {
                                // float4[] - encode JSON array to binary format
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Some(binary_data) = crate::protocol::binary::BinaryEncoder::encode_value(&rusqlite::types::Value::Text(s), type_oid, true) {
                                        Some(binary_data)
                                    } else {
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::Float8Array.to_oid() => {
                                // float8[] - encode JSON array to binary format
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Some(binary_data) = crate::protocol::binary::BinaryEncoder::encode_value(&rusqlite::types::Value::Text(s), type_oid, true) {
                                        Some(binary_data)
                                    } else {
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::TextArray.to_oid() || t == PgType::VarcharArray.to_oid() || t == PgType::CharArray.to_oid() => {
                                // text[]/varchar[]/char[] - encode JSON array to binary format
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Some(binary_data) = crate::protocol::binary::BinaryEncoder::encode_value(&rusqlite::types::Value::Text(s), type_oid, true) {
                                        Some(binary_data)
                                    } else {
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::NumericArray.to_oid() => {
                                // numeric[] - encode JSON array to binary format
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Some(binary_data) = crate::protocol::binary::BinaryEncoder::encode_value(&rusqlite::types::Value::Text(s), type_oid, true) {
                                        Some(binary_data)
                                    } else {
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
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
                                        // Convert from days since 1970 to days since 2000
                                        let days_since_2000 = days_since_1970 - 10957;
                                        let mut buf = vec![0u8; 4];
                                        BigEndian::write_i32(&mut buf, days_since_2000);
                                        Some(buf)
                                    } else if let Some(days) = Self::date_to_pg_days(&s) {
                                        // Handle date strings like "2025-01-01" 
                                        let mut buf = vec![0u8; 4];
                                        BigEndian::write_i32(&mut buf, days);
                                        Some(buf)
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
                                        let mut buf = vec![0u8; 8];
                                        BigEndian::write_i64(&mut buf, micros);
                                        Some(buf)
                                    } else if let Some(micros) = Self::time_to_microseconds(&s) {
                                        let mut buf = vec![0u8; 8];
                                        BigEndian::write_i64(&mut buf, micros);
                                        Some(buf)
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
                                        let pg_micros = unix_micros - (946684800 * 1_000_000);
                                        let mut buf = vec![0u8; 8];
                                        BigEndian::write_i64(&mut buf, pg_micros);
                                        Some(buf)
                                    } else if let Some(micros) = Self::timestamp_to_pg_microseconds(&s) {
                                        let mut buf = vec![0u8; 8];
                                        BigEndian::write_i64(&mut buf, micros);
                                        Some(buf)
                                    } else {
                                        // If parsing fails, keep as text
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            // Numeric type - use proper binary encoding when requested
                            t if t == PgType::Numeric.to_oid() => {
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    // Try to parse and encode as PostgreSQL numeric binary format
                                    if let Ok(decimal) = rust_decimal::Decimal::from_str(&s) {
                                        debug!("Encoding NUMERIC value '{}' as binary", s);
                                        Some(crate::protocol::binary::BinaryEncoder::encode_numeric(&decimal))
                                    } else {
                                        debug!("Failed to parse NUMERIC value '{}', keeping as text", s);
                                        Some(bytes.clone())
                                    }
                                } else {
                                    // Not valid UTF-8, keep as-is
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
                                        let mut buf = vec![0u8; 8];
                                        BigEndian::write_i64(&mut buf, cents);
                                        Some(buf)
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
                            // Text types - these are fine as-is in binary format
                            t if t == PgType::Text.to_oid() || t == PgType::Varchar.to_oid() || t == PgType::Char.to_oid() => {
                                // text/varchar/char - UTF-8 encoded text
                                Some(bytes.clone())
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
                                        let mut buf = vec![0u8; 2];
                                        BigEndian::write_i16(&mut buf, val);
                                        Some(buf)
                                    } else {
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            _ => {
                                // For unknown types, keep as-is (text)
                                Some(bytes.clone())
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
                            // Date type - convert from INTEGER days to formatted string
                            t if t == PgType::Date.to_oid() => {
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    // Check if this is an integer (days since 1970-01-01)
                                    if let Ok(days) = s.parse::<i64>() {
                                        // Convert days to formatted date
                                        use crate::types::datetime_utils::format_days_to_date;
                                        let formatted = format_days_to_date(days);
                                        Some(formatted.into_bytes())
                                    } else {
                                        // Already formatted or invalid, keep as-is
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            // Time type - convert from INTEGER microseconds to formatted string
                            t if t == PgType::Time.to_oid() || t == PgType::Timetz.to_oid() => {
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    // Check if this is an integer (microseconds since midnight)
                                    if let Ok(micros) = s.parse::<i64>() {
                                        // Convert microseconds to formatted time
                                        use crate::types::datetime_utils::format_microseconds_to_time;
                                        let formatted = format_microseconds_to_time(micros);
                                        Some(formatted.into_bytes())
                                    } else {
                                        // Already formatted or invalid, keep as-is
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            // Timestamp types - convert from INTEGER microseconds to formatted string
                            t if t == PgType::Timestamp.to_oid() || t == PgType::Timestamptz.to_oid() => {
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    eprintln!("🕐 TIMESTAMP conversion: Processing value: '{}'", s);
                                    // Check if this is already an integer (microseconds since epoch)
                                    if let Ok(micros) = s.parse::<i64>() {
                                        // Convert microseconds to formatted timestamp
                                        use crate::types::datetime_utils::format_microseconds_to_timestamp;
                                        let formatted = format_microseconds_to_timestamp(micros);
                                        eprintln!("🕐 TIMESTAMP conversion: {} -> {}", micros, formatted);
                                        Some(formatted.into_bytes())
                                    } else {
                                        eprintln!("🕐 TIMESTAMP value is not an integer, keeping as-is: '{}'", s);
                                        // Already formatted or invalid, keep as-is
                                        Some(bytes.clone())
                                    }
                                } else {
                                    eprintln!("🕐 TIMESTAMP value is not UTF-8, keeping as-is");
                                    Some(bytes.clone())
                                }
                            }
                            // NOTE: Array type handling removed for text format too
                            // Arrays are returned as JSON strings with TEXT type
                            t if t == PgType::Text.to_oid() => {
                                // Enhanced datetime detection for TEXT columns
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    info!("TEXT column value: '{}'", s);
                                    if let Ok(micros) = s.parse::<i64>() {
                                        info!("Parsed as i64: {}", micros);
                                        // Check if this looks like microseconds since epoch
                                        // Valid timestamp range: roughly 1970-2100 (0 to ~4.1 trillion microseconds)
                                        // We check for values > 100 billion to avoid converting small integers
                                        if micros > 100_000_000_000 && micros < 4_102_444_800_000_000 {
                                            info!("Value {} is in timestamp range, converting...", micros);
                                            // This is likely a datetime value stored as INTEGER microseconds, format it
                                            use crate::types::datetime_utils::format_microseconds_to_timestamp;
                                            let formatted = format_microseconds_to_timestamp(micros);
                                            info!("Converting presumed timestamp value {} to formatted timestamp: {}", micros, formatted);
                                            Some(formatted.into_bytes())
                                        } else {
                                            info!("Value {} is not in timestamp range", micros);
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
    
    async fn execute_select<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &Arc<DbHandler>,
        session: &Arc<SessionState>,
        portal_name: &str,
        query: &str,
        max_rows: i32,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // Check if this is a catalog query first
        info!("execute_select: Checking if query is catalog query: {}", query);
        if query.contains("int_array_with_nulls") {
            info!("DEBUG: execute_select called with array null query: {}", query);
        }
        let response = if let Some(catalog_result) = CatalogInterceptor::intercept_query(query, db.clone(), Some(session.clone())).await {
            info!("execute_select: Query intercepted by catalog handler");
            println!("EXTENDED: Got catalog result, about to unwrap");
            let mut catalog_response = catalog_result?;
            println!("EXTENDED: Unwrapped catalog result, columns: {}, rows: {}", catalog_response.columns.len(), catalog_response.rows.len());
            
            // For catalog queries with binary result formats, we need to ensure the data
            // is in the correct format for binary encoding
            let portals = session.portals.read().await;
            let portal = portals.get(portal_name).unwrap();
            let has_binary_format = portal.result_formats.contains(&1);
            println!("EXTENDED: result_formats = {:?}, has_binary_format = {}", portal.result_formats, has_binary_format);
            drop(portals);
            
            if has_binary_format {
                if query.contains("pg_attribute") {
                    info!("Converting catalog text data for binary encoding");
                    // pg_attribute specific handling - ensure numeric columns are properly formatted
                    for row in &mut catalog_response.rows {
                        // attnum is at index 5
                        if row.len() > 5
                            && let Some(Some(attnum_bytes)) = row.get_mut(5)
                                && let Ok(attnum_str) = String::from_utf8(attnum_bytes.clone()) {
                                    // Ensure it's just the numeric value without extra formatting
                                    *attnum_bytes = attnum_str.trim().as_bytes().to_vec();
                                }
                    }
                } else if query.contains("information_schema") {
                    info!("Converting information_schema text data for binary encoding");
                    // For information_schema queries, the data is already in text format
                    // which is what binary encoding expects for text columns
                    // No additional conversion needed - the binary encoder will handle it
                    println!("EXTENDED: information_schema binary format - no conversion needed");
                }
            }
            
            catalog_response
        } else {
            info!("Query not intercepted, executing normally");
            let cached_conn = Self::get_or_cache_connection(session, db).await;
            db.query_with_session_cached(query, &session.id, cached_conn.as_ref()).await?
        };

        println!("EXTENDED: About to process response, columns: {}, rows: {}", response.columns.len(), response.rows.len());
        println!("EXTENDED: Query contains 'int_array_with_nulls': {}", query.contains("int_array_with_nulls"));

        // Check if we need to send RowDescription
        // We send it if:
        // 1. The prepared statement had no field descriptions (wasn't Described or Describe sent NoData)
        // BUT NOT for catalog queries - they should already have field descriptions from Describe
        let send_row_desc = {
            let portals = session.portals.read().await;
            let portal = portals.get(portal_name).unwrap();
            let statements = session.prepared_statements.read().await;
            let stmt = statements.get(&portal.statement_name).unwrap();
            
            // We need to send RowDescription only if:
            // - The statement has no field descriptions (wasn't Described)
            // - AND we have columns to describe
            // Note: We do NOT send RowDescription if switching to binary format because
            // Describe(Portal) would have already sent it with the correct format
            let needs_row_desc = stmt.field_descriptions.is_empty() && !response.columns.is_empty();
            
            drop(statements);
            drop(portals);
            needs_row_desc
        };
        
        if send_row_desc {
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
                        // Try direct lookup first
                        if let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, table, col_name).await {
                            schema_types.insert(col_name.clone(), pg_type);
                        } else {
                            // Parse the query to find the source column for this alias
                            // Look for pattern like "table.column AS alias" in the SELECT clause
                            let pattern = format!(r"(?i)(\w+)\.(\w+)\s+AS\s+{}", regex::escape(col_name));
                            if let Ok(re) = regex::Regex::new(&pattern)
                                && let Some(captures) = re.captures(query)
                                    && let Some(src_table) = captures.get(1)
                                        && let Some(src_col) = captures.get(2) {
                                            let src_table_name = src_table.as_str();
                                            let src_col_name = src_col.as_str();
                                            // Only use if it's the same table we identified
                                            if src_table_name == table
                                                && let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, table, src_col_name).await {
                                                    info!("Found type for aliased column '{}' from query pattern '{}.{}': {}", col_name, src_table_name, src_col_name, pg_type);
                                                    schema_types.insert(col_name.clone(), pg_type);
                                                }
                                        }
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
                // First pass: identify columns that need async lookups
                let mut async_lookups_needed = Vec::new();
                for (i, col_name) in response.columns.iter().enumerate() {
                    let col_lower = col_name.to_lowercase();
                    if col_lower.contains("max(") || col_lower.contains("min(") || 
                       col_lower.contains("sum(") || col_lower.contains("avg(") {
                        async_lookups_needed.push((i, col_name.clone()));
                    }
                }
                
                // Perform async lookups for aggregate functions
                let mut aggregate_types = std::collections::HashMap::new();

                // Pre-compile regex patterns to avoid recompilation in loop
                let max_regex = regex::Regex::new(r"\(\s*SELECT\s+MAX\s*\(\s*(\w+)\s*\)\s+FROM\s+(\w+)\s*\)").ok();
                let min_regex = regex::Regex::new(r"\(\s*SELECT\s+MIN\s*\(\s*(\w+)\s*\)\s+FROM\s+(\w+)\s*\)").ok();

                for (idx, col_name) in async_lookups_needed {
                    // Extract the aggregate function and column
                    let col_lower = col_name.to_lowercase();
                    
                    // Try to find the table from a scalar subquery
                    let mut lookup_table = table_name.clone();
                    
                    // Look for scalar subquery pattern: (SELECT MAX(col) FROM table)
                    if let Some(re) = &max_regex
                        && let Some(captures) = re.captures(query)
                            && let Some(table_match) = captures.get(2) {
                                lookup_table = Some(table_match.as_str().to_string());
                                if let Some(col_match) = captures.get(1) {
                                    // We found the exact column and table
                                    let col_name_inner = col_match.as_str();
                                    if let Some(ref table) = lookup_table {
                                        // Now we can look up the type with the session connection
                                        if let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, table, col_name_inner).await {
                                            let type_oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(&pg_type);
                                            info!("Scalar subquery MAX({}) from table {} has type {} (OID {})", 
                                                  col_name_inner, table, pg_type, type_oid);
                                            aggregate_types.insert(idx, type_oid);
                                            continue;
                                        }
                                    }
                                }
                            }
                    
                    // Also check for MIN
                    if let Some(re) = &min_regex
                        && let Some(captures) = re.captures(query)
                            && let Some(table_match) = captures.get(2) {
                                lookup_table = Some(table_match.as_str().to_string());
                                if let Some(col_match) = captures.get(1) {
                                    let col_name_inner = col_match.as_str();
                                    if let Some(ref table) = lookup_table
                                        && let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, table, col_name_inner).await {
                                            let type_oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(&pg_type);
                                            info!("Scalar subquery MIN({}) from table {} has type {} (OID {})", 
                                                  col_name_inner, table, pg_type, type_oid);
                                            aggregate_types.insert(idx, type_oid);
                                            continue;
                                        }
                                }
                            }
                    
                    // Fallback to generic aggregate type detection
                    if let Some(oid) = crate::types::SchemaTypeMapper::get_aggregate_return_type_with_query(
                        &col_lower, None, lookup_table.as_deref(), Some(query)
                    ) {
                        aggregate_types.insert(idx, oid);
                    }
                }
                
                let field_types = response.columns.iter()
                    .enumerate()
                    .map(|(i, col_name)| {
                        // Special handling for parameter columns (e.g., $1, ?column?)
                        if col_name.starts_with('$') || col_name == "?column?" {
                            // This is a parameter column, get type from portal's inferred types
                            if let Some(ref inferred_types) = portal_inferred_types {
                                // Try to extract parameter number from column name
                                let param_idx = if let Some(stripped) = col_name.strip_prefix('$') {
                                    stripped.parse::<usize>().ok().map(|n| n - 1)
                                } else {
                                    Some(i) // Use column index for ?column?
                                };
                                
                                if let Some(idx) = param_idx
                                    && let Some(&type_oid) = inferred_types.get(idx) {
                                        info!("Column '{}' is parameter with inferred type OID {}", col_name, type_oid);
                                        return type_oid;
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
                        
                        // Second priority: Check for pre-computed aggregate functions
                        if let Some(&type_oid) = aggregate_types.get(&i) {
                            info!("Column '{}' has pre-computed aggregate type OID {}", col_name, type_oid);
                            return type_oid;
                        }
                        
                        // Fallback: Check for aggregate functions without async lookup
                        let col_lower = col_name.to_lowercase();
                        if let Some(oid) = crate::types::SchemaTypeMapper::get_aggregate_return_type_with_query(
                            &col_lower, None, table_name.as_deref(), Some(query)
                        ) {
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
                                debug!("Column '{}' in table '{}' not found in __pgsqlite_schema. Using type inference.", col_name, table);
                                debug!("Falling back to type inference, but this may cause type compatibility issues.");
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

                // Fix field types for catalog queries - both single table and JOINs
                let mut corrected_field_types = field_types;
                let should_correct = query.contains("pg_attribute") || (query.contains("a.attnotnull") || query.contains("a.atthasdef"));
                info!("EXECUTE_SELECT: Boolean type correction check - should_correct={}, query contains pg_attribute={}, query contains a.attnotnull={}",
                      should_correct, query.contains("pg_attribute"), query.contains("a.attnotnull"));
                if should_correct {
                    for (i, col_name) in response.columns.iter().enumerate() {
                        let col_lower = col_name.to_lowercase();
                        match col_lower.as_str() {
                            // Direct pg_attribute boolean columns
                            "attnotnull" | "atthasdef" | "attbyval" | "atthasmissing" | "attisdropped" | "attislocal" |
                            // Common aliases for these columns in JOIN queries
                            "not_null" | "has_default" | "is_not_null" | "has_def" => {
                                if i < corrected_field_types.len() {
                                    let old_type = corrected_field_types[i];
                                    corrected_field_types[i] = 16; // PgType::Bool.to_oid()
                                    info!("EXECUTE_SELECT: Corrected column '{}' type from {} to BOOL (16)", col_name, old_type);
                                }
                            }
                            "attidentity" | "attgenerated" | "attalign" | "attstorage" | "attcompression" => {
                                if i < corrected_field_types.len() {
                                    let old_type = corrected_field_types[i];
                                    corrected_field_types[i] = 18; // PgType::Char.to_oid()
                                    info!("EXECUTE_SELECT: Corrected column '{}' type from {} to CHAR (18)", col_name, old_type);
                                }
                            }
                            _ => {}
                        }
                    }
                }
                let field_types = corrected_field_types;

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
        }
        
        // Get result formats and field types from the portal and statement
        let (result_formats, field_types) = {
            let portals = session.portals.read().await;
            let portal = portals.get(portal_name).unwrap();
            let statements = session.prepared_statements.read().await;
            let stmt = statements.get(&portal.statement_name).unwrap();
            let mut field_types: Vec<i32> = if stmt.field_descriptions.is_empty() {
                // Try to infer types - we need async for schema lookup, so collect field descriptions first
                let mut field_types = Vec::new();
                
                // Get table name for schema lookup
                let table_name = extract_table_name_from_select(&portal.query);
                
                for (i, col_name) in response.columns.iter().enumerate() {
                    // Check for aggregate functions first
                    let col_lower = col_name.to_lowercase();
                    
                    if let Some(oid) = crate::types::SchemaTypeMapper::get_aggregate_return_type_with_query(&col_lower, None, None, Some(&portal.query)) {
                        info!("Column '{}' is aggregate function with type OID {} (field_types)", col_name, oid);
                        field_types.push(oid);
                        continue;
                    }
                    
                    // Try schema-based type inference ALWAYS (not just for empty result sets)
                    // This is crucial for datetime types which are stored as INTEGER in SQLite
                    let mut found_type = false;
                    if let Some(ref table) = table_name {
                        // Try to extract source table.column from alias
                        if let Some((source_table, source_col)) = Self::extract_source_table_column_for_alias(&portal.query, col_name) {
                            if let Ok(Some(pg_type_str)) = db.get_schema_type_with_session(&session.id, &source_table, &source_col).await {
                                let type_oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(&pg_type_str);
                                info!("Column '{}': resolved type from schema '{}.{}' -> {} (OID {}) in execute_select", 
                                      col_name, source_table, source_col, pg_type_str, type_oid);
                                field_types.push(type_oid);
                                found_type = true;
                            }
                        } else if col_name.contains('.') {
                            // Handle columns with table prefix like "users.id"
                            let parts: Vec<&str> = col_name.split('.').collect();
                            if parts.len() == 2 {
                                let table_part = parts[0];
                                let column_part = parts[1];
                                info!("Column '{}': Attempting to resolve type from '{}.{}'", col_name, table_part, column_part);
                                if let Ok(Some(pg_type_str)) = db.get_schema_type_with_session(&session.id, table_part, column_part).await {
                                    let type_oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(&pg_type_str);
                                    info!("Column '{}': resolved type from schema '{}.{}' -> {} (OID {}) in execute_select", 
                                          col_name, table_part, column_part, pg_type_str, type_oid);
                                    field_types.push(type_oid);
                                    found_type = true;
                                } else {
                                    info!("Column '{}': Could not find type for '{}.{}'", col_name, table_part, column_part);
                                }
                            } else {
                                info!("Column '{}': Contains dot but not in expected format", col_name);
                            }
                        } else if let Ok(Some(pg_type_str)) = db.get_schema_type_with_session(&session.id, table, col_name).await {
                            let type_oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(&pg_type_str);
                            info!("Column '{}': resolved type from schema '{}.{}' -> {} (OID {}) in execute_select", 
                                  col_name, table, col_name, pg_type_str, type_oid);
                            field_types.push(type_oid);
                            found_type = true;
                        }
                    }
                    
                    if !found_type {
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
                        field_types.push(type_oid);
                    }
                }
                field_types
            } else {
                stmt.field_descriptions.iter().map(|fd| fd.type_oid).collect()
            };

            // Fix field types for catalog queries - both single table and JOINs
            if query.contains("pg_attribute") || (query.contains("a.attnotnull") || query.contains("a.atthasdef")) {
                for (i, col_name) in response.columns.iter().enumerate() {
                    let col_lower = col_name.to_lowercase();
                    match col_lower.as_str() {
                        // Direct pg_attribute boolean columns
                        "attnotnull" | "atthasdef" | "attbyval" | "atthasmissing" | "attisdropped" | "attislocal" |
                        // Common aliases for these columns in JOIN queries
                        "not_null" | "has_default" | "is_not_null" | "has_def" => {
                            if i < field_types.len() {
                                field_types[i] = PgType::Bool.to_oid();
                            }
                        }
                        "attidentity" | "attgenerated" | "attalign" | "attstorage" | "attcompression" => {
                            if i < field_types.len() {
                                field_types[i] = PgType::Char.to_oid();
                            }
                        }
                        _ => {}
                    }
                }
            }

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

        if query.contains("int_array_with_nulls") {
            println!("DEBUG: About to process {} rows for array query", rows_to_send.len());
            println!("DEBUG: field_types = {:?}", field_types);
            println!("DEBUG: result_formats = {:?}", result_formats);
        }

        for (row_idx, row) in rows_to_send.into_iter().enumerate() {
            // Debug: Log the raw data being retrieved
            if query.contains("int_array_with_nulls") {
                info!("DEBUG: Processing row {} for array query", row_idx);
                info!("DEBUG: field_types length: {}, contents: {:?}", field_types.len(), field_types);
                info!("DEBUG: result_formats length: {}, contents: {:?}", result_formats.len(), result_formats);
                for (col_idx, col_data) in row.iter().enumerate() {
                    if let Some(data) = col_data {
                        let data_str = String::from_utf8_lossy(data);
                        info!("  Column {}: '{}' (type OID: {})", col_idx, data_str,
                              field_types.get(col_idx).copied().unwrap_or(25));
                    } else {
                        info!("  Column {}: NULL", col_idx);
                    }
                }
            }

            // Convert row data based on result formats
            let encoded_row = Self::encode_row(&row, &result_formats, &field_types)?;

            // TODO: Fix boolean field type conversion for catalog queries
            if query.contains("int_array_with_nulls") {
                println!("DEBUG: encoded_row has {} fields", encoded_row.len());
                if let Some(first_field) = encoded_row.first() {
                    if let Some(data) = first_field {
                        println!("DEBUG: First field has {} bytes, hex: {:02x?}", data.len(), &data[..std::cmp::min(50, data.len())]);
                    } else {
                        println!("DEBUG: First field is NULL");
                    }
                }
            }
            framed.send(BackendMessage::DataRow(encoded_row)).await
                .map_err(PgSqliteError::Io)?;
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
            debug!("Extended protocol: Query has RETURNING clause, using execute_dml_with_returning: {}", query);
            // Get result formats from portal
            let result_formats = {
                let portals = session.portals.read().await;
                let portal = portals.get(portal_name).unwrap();
                portal.result_formats.clone()
            };
            return Self::execute_dml_with_returning(framed, db, session, query, &result_formats).await;
        }
        
        // Validation is now done in handle_execute before parameter substitution
        
        debug!("Extended protocol: Executing DML query without RETURNING: {}", query);
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
    
    /// Helper function to build field descriptions for RETURNING clause with proper type detection
    async fn build_returning_field_descriptions(
        db: &Arc<DbHandler>,
        session: &Arc<SessionState>,
        table_name: &str,
        columns: &[String],
        result_formats: &[i16],
        returning_clause: &str,
    ) -> Vec<FieldDescription> {
        let mut fields = Vec::new();
        
        for (i, col_name) in columns.iter().enumerate() {
            let format = if result_formats.is_empty() {
                0 // Default to text if no formats specified
            } else if result_formats.len() == 1 {
                result_formats[0] // Single format applies to all columns
            } else if i < result_formats.len() {
                result_formats[i] // Use column-specific format
            } else {
                0 // Default to text if not enough formats
            };
            
            // Try to get the actual type from schema
            let type_oid = if returning_clause == "*" || col_name == &col_name.to_lowercase() {
                // Direct column reference or wildcard - look up in schema
                if let Ok(Some(pg_type_str)) = db.get_schema_type_with_session(&session.id, table_name, col_name).await {
                    // Convert PostgreSQL type name to OID
                    match pg_type_str.to_uppercase().as_str() {
                        "BOOL" | "BOOLEAN" => 16,
                        "INT2" | "SMALLINT" => 21,
                        "INT4" | "INTEGER" | "INT" => 23,
                        "INT8" | "BIGINT" => 20,
                        "FLOAT4" | "REAL" => 700,
                        "FLOAT8" | "DOUBLE PRECISION" => 701,
                        "TEXT" => 25,
                        "VARCHAR" | "CHARACTER VARYING" => 1043,
                        "CHAR" | "CHARACTER" => 1042,
                        "UUID" => 2950,
                        "JSON" => 114,
                        "JSONB" => 3802,
                        "DATE" => 1082,
                        "TIME" | "TIME WITHOUT TIME ZONE" => 1083,
                        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => 1114,
                        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => 1184,
                        "TIMETZ" | "TIME WITH TIME ZONE" => 1266,
                        "INTERVAL" => 1186,
                        "NUMERIC" | "DECIMAL" => 1700,
                        "BYTEA" => 17,
                        "MONEY" => 790,
                        _ => 25, // Default to TEXT for unknown types
                    }
                } else {
                    25 // Default to TEXT if not found
                }
            } else {
                // Could be an expression, default to TEXT
                25
            };
            
            fields.push(FieldDescription {
                name: col_name.clone(),
                table_oid: 0,
                column_id: (i + 1) as i16,
                type_oid,
                type_size: -1,
                type_modifier: -1,
                format,
            });
        }
        
        fields
    }

    /// Helper function to convert timestamp columns in RETURNING results
    async fn convert_returning_timestamps(
        db: &Arc<DbHandler>,
        session: &Arc<SessionState>,
        table_name: &str,
        columns: &[String],
        rows: Vec<Vec<Option<Vec<u8>>>>,
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, PgSqliteError> {
        // Get schema types for all columns
        let mut is_timestamp = vec![false; columns.len()];
        for (i, col_name) in columns.iter().enumerate() {
            if let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, table_name, col_name).await {
                is_timestamp[i] = matches!(pg_type.as_str(), "TIMESTAMP" | "TIMESTAMPTZ");
            }
        }
        
        // Convert timestamps in rows
        let mut converted_rows = Vec::new();
        for row in rows {
            let mut converted_row = Vec::new();
            for (i, cell) in row.iter().enumerate() {
                if is_timestamp[i]
                    && let Some(data) = cell
                        && let Ok(value_str) = String::from_utf8(data.clone()) {
                            // Check if it's a raw microseconds value
                            if value_str.chars().all(|c| c.is_ascii_digit() || c == '-')
                                && let Ok(micros) = value_str.parse::<i64>() {
                                    // Convert microseconds to formatted timestamp
                                    let formatted = crate::types::datetime_utils::format_microseconds_to_timestamp(micros);
                                    converted_row.push(Some(formatted.into_bytes()));
                                    continue;
                                }
                        }
                converted_row.push(cell.clone());
            }
            converted_rows.push(converted_row);
        }
        
        Ok(converted_rows)
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
        let (base_query, returning_clause) = ReturningTranslator::extract_returning_clause(query)
            .ok_or_else(|| PgSqliteError::Protocol("Failed to parse RETURNING clause".to_string()))?;
        
        if query_starts_with_ignore_case(&base_query, "INSERT") {
            // For INSERT, execute the insert and then query by last_insert_rowid
            let table_name = ReturningTranslator::extract_table_from_insert(&base_query)
                .ok_or_else(|| PgSqliteError::Protocol("Failed to extract table name".to_string()))?;
            
            // Execute the INSERT
            let cached_conn = Self::get_or_cache_connection(session, db).await;
            let response = db.execute_with_session_cached(&base_query, &session.id, cached_conn.as_ref()).await?;
            
            debug!("INSERT executed, rows_affected: {}", response.rows_affected);
            
            // Get the last inserted rowid
            let last_rowid_query = "SELECT last_insert_rowid()";
            let cached_conn = Self::get_or_cache_connection(session, db).await;
            let last_rowid_response = db.query_with_session_cached(last_rowid_query, &session.id, cached_conn.as_ref()).await?;
            
            let last_rowid: i64 = if !last_rowid_response.rows.is_empty() && !last_rowid_response.rows[0].is_empty() {
                // Parse the rowid from the response
                if let Some(data) = &last_rowid_response.rows[0][0] {
                    match String::from_utf8(data.clone()) {
                        Ok(s) => s.parse().unwrap_or(0),
                        Err(_) => 0,
                    }
                } else {
                    0
                }
            } else {
                0
            };
            
            // Query for RETURNING data for all inserted rows
            // For multi-row inserts, we need to get all rows from (last_rowid - rows_affected + 1) to last_rowid
            let returning_query = if response.rows_affected > 1 && last_rowid > 0 {
                // Multi-row insert: get all inserted rows
                let first_rowid = last_rowid - response.rows_affected as i64 + 1;
                debug!("Multi-row INSERT RETURNING: fetching rows from rowid {} to {}", first_rowid, last_rowid);
                format!(
                    "SELECT {returning_clause} FROM {table_name} WHERE rowid >= {} AND rowid <= {} ORDER BY rowid",
                    first_rowid, last_rowid
                )
            } else {
                // Single row insert: just get the last rowid
                debug!("Single-row INSERT RETURNING: fetching row with rowid {}", last_rowid);
                format!(
                    "SELECT {returning_clause} FROM {table_name} WHERE rowid = {}",
                    last_rowid
                )
            };
            
            let cached_conn = Self::get_or_cache_connection(session, db).await;
            let returning_response = db.query_with_session_cached(&returning_query, &session.id, cached_conn.as_ref()).await?;
            
            // Build field descriptions with proper type detection
            let fields = Self::build_returning_field_descriptions(
                db,
                session,
                &table_name,
                &returning_response.columns,
                result_formats,
                &returning_clause,
            ).await;
            
            framed.send(BackendMessage::RowDescription(fields)).await
                .map_err(PgSqliteError::Io)?;
            
            // Convert timestamps and send data rows
            let converted_rows = Self::convert_returning_timestamps(
                db,
                session,
                &table_name,
                &returning_response.columns,
                returning_response.rows,
            ).await?;
            
            for row in converted_rows {
                framed.send(BackendMessage::DataRow(row)).await
                    .map_err(PgSqliteError::Io)?;
            }
            
            // Send command complete
            let tag = format!("INSERT 0 {}", response.rows_affected);
            framed.send(BackendMessage::CommandComplete { tag }).await
                .map_err(PgSqliteError::Io)?;
        } else if query_starts_with_ignore_case(&base_query, "UPDATE") {
            // For UPDATE, we need a different approach
            let table_name = ReturningTranslator::extract_table_from_update(&base_query)
                .ok_or_else(|| PgSqliteError::Protocol("Failed to extract table name".to_string()))?;
            
            // First, get the rowids of rows that will be updated
            let where_clause = ReturningTranslator::extract_where_clause(&base_query);
            let rowid_query = format!(
                "SELECT rowid FROM {table_name} {where_clause}"
            );
            let cached_conn = Self::get_or_cache_connection(session, db).await;
            let rowid_response = db.query_with_session_cached(&rowid_query, &session.id, cached_conn.as_ref()).await?;
            let rowids: Vec<String> = rowid_response.rows.iter()
                .filter_map(|row| row[0].as_ref())
                .map(|bytes| String::from_utf8_lossy(bytes).to_string())
                .collect();
            
            // Execute the UPDATE
            let cached_conn = Self::get_or_cache_connection(session, db).await;
            let response = db.execute_with_session_cached(&base_query, &session.id, cached_conn.as_ref()).await?;
            
            // Now query the updated rows
            if !rowids.is_empty() {
                let rowid_list = rowids.join(",");
                let returning_query = format!(
                    "SELECT {returning_clause} FROM {table_name} WHERE rowid IN ({rowid_list})"
                );
                
                let cached_conn = Self::get_or_cache_connection(session, db).await;
                let returning_response = db.query_with_session_cached(&returning_query, &session.id, cached_conn.as_ref()).await?;
                
                // Build field descriptions with proper type detection
                let fields = Self::build_returning_field_descriptions(
                    db,
                    session,
                    &table_name,
                    &returning_response.columns,
                    result_formats,
                    &returning_clause,
                ).await;
                
                framed.send(BackendMessage::RowDescription(fields)).await
                    .map_err(PgSqliteError::Io)?;
                
                // Convert timestamps and send data rows
                let converted_rows = Self::convert_returning_timestamps(
                    db,
                    session,
                    &table_name,
                    &returning_response.columns,
                    returning_response.rows,
                ).await?;
                
                for row in converted_rows {
                    framed.send(BackendMessage::DataRow(row)).await
                        .map_err(PgSqliteError::Io)?;
                }
            }
            
            // Send command complete
            let tag = format!("UPDATE {}", response.rows_affected);
            framed.send(BackendMessage::CommandComplete { tag }).await
                .map_err(PgSqliteError::Io)?;
        } else if query_starts_with_ignore_case(&base_query, "DELETE") {
            // For DELETE, capture rows before deletion
            let table_name = ReturningTranslator::extract_table_from_delete(&base_query)
                .ok_or_else(|| PgSqliteError::Protocol("Failed to extract table name".to_string()))?;
            
            let capture_query = ReturningTranslator::generate_capture_query(
                &base_query,
                &table_name,
                &returning_clause
            )?;
            
            // Capture the rows that will be affected
            let cached_conn = Self::get_or_cache_connection(session, db).await;
            let captured_rows = db.query_with_session_cached(&capture_query, &session.id, cached_conn.as_ref()).await?;
            
            // Execute the actual DELETE
            let cached_conn = Self::get_or_cache_connection(session, db).await;
            let response = db.execute_with_session_cached(&base_query, &session.id, cached_conn.as_ref()).await?;
            
            // Build field descriptions with proper type detection (skip rowid column)
            let columns_without_rowid: Vec<String> = captured_rows.columns.iter()
                .skip(1) // Skip rowid column
                .cloned()
                .collect();
            
            let fields = Self::build_returning_field_descriptions(
                db,
                session,
                &table_name,
                &columns_without_rowid,
                result_formats,
                &returning_clause,
            ).await;
            
            framed.send(BackendMessage::RowDescription(fields)).await
                .map_err(PgSqliteError::Io)?;
            
            // Convert timestamps in captured rows (skip rowid column)
            let rows_without_rowid: Vec<Vec<Option<Vec<u8>>>> = captured_rows.rows.into_iter()
                .map(|row| row.into_iter().skip(1).collect())
                .collect();
            
            let converted_rows = Self::convert_returning_timestamps(
                db,
                session,
                &table_name,
                &columns_without_rowid,
                rows_without_rowid,
            ).await?;
            
            // Send converted rows
            for row in converted_rows {
                framed.send(BackendMessage::DataRow(row)).await
                    .map_err(PgSqliteError::Io)?;
            }
            
            // Send command complete
            let tag = format!("DELETE {}", response.rows_affected);
            framed.send(BackendMessage::CommandComplete { tag }).await
                .map_err(PgSqliteError::Io)?;
        }
        
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
        if query_starts_with_ignore_case(query, "CREATE TABLE") {
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
            debug!("Type mappings count: {}", type_mappings.len());
            if !type_mappings.is_empty() {
                // Extract table name from query
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
                                        Err(_) => {
                                            // Failed to store numeric constraint
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    debug!("Stored type mappings for table {} (extended query protocol)", table_name);
                    
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

            // Populate PostgreSQL catalog tables with constraint information
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

            // Send CommandComplete and return
            framed.send(BackendMessage::CommandComplete { tag: "CREATE TABLE".to_string() }).await
                .map_err(PgSqliteError::Io)?;

            return Ok(());
        };
        
        // Handle other DDL with potential translation
        let translated_query = if query.to_lowercase().contains("json") || query.to_lowercase().contains("jsonb") {
            JsonTranslator::translate_statement(query)?
        } else if query_starts_with_ignore_case(query, "CREATE INDEX") {
            // Translate CREATE INDEX with operator classes
            crate::translator::CreateIndexTranslator::translate(query)
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
    async fn analyze_insert_params(query: &str, db: &Arc<DbHandler>) -> Result<(Vec<i32>, Vec<i32>), PgSqliteError> {
        // Use QueryContextAnalyzer to extract table and column info
        let (table_name, columns) = crate::types::QueryContextAnalyzer::get_insert_column_info(query)
            .ok_or_else(|| PgSqliteError::Protocol("Failed to parse INSERT query".to_string()))?;
        
        info!("Analyzing INSERT for table '{}' with columns: {:?}", table_name, columns);
        
        // Get cached table schema
        let table_schema = db.get_table_schema(&table_name).await
            .map_err(|e| PgSqliteError::Protocol(format!("Failed to get table schema: {e}")))?;
        
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
    async fn analyze_select_params(query: &str, db: &Arc<DbHandler>, session: &Arc<SessionState>) -> Result<Vec<i32>, PgSqliteError> {
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
            
            if let Some(captures) = cast_regex.captures(query)
                && let Some(type_match) = captures.get(1) {
                    let cast_type = type_match.as_str();
                    let oid = Self::pg_type_name_to_oid(cast_type);
                    param_types.push(oid);
                    info!("Found explicit cast for parameter {}: {} (OID {})", i, cast_type, oid);
                    found_type = true;
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
                if let Some(captures) = regex.captures(&query_lower)
                    && let Some(column_match) = captures.get(1) {
                        let column = column_match.as_str();
                        
                        // Look up the type for this column
                        if let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, &table_name, column).await {
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
                                    if let (Some(Some(name_bytes)), Some(Some(type_bytes))) = (row.get(1), row.get(2))
                                        && let (Ok(col_name), Ok(sqlite_type)) = (
                                            String::from_utf8(name_bytes.clone()),
                                            String::from_utf8(type_bytes.clone())
                                        )
                                            && col_name.to_lowercase() == column {
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
                        
                        if found_type {
                            break;
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
        
        debug!("analyze_column_casts: Processing query: {}", query);
        
        // Find the SELECT clause - use case-insensitive search
        let select_pos = if let Some(pos) = find_keyword_position(query, "SELECT") {
            pos
        } else {
            debug!("analyze_column_casts: No SELECT found, returning empty map");
            return cast_map; // No SELECT found
        };
        
        let after_select = &query[select_pos + 6..];
        debug!("analyze_column_casts: after_select = {}", after_select);
        
        // Find the FROM clause to know where SELECT list ends
        // Use a more robust approach that handles any whitespace
        let from_pos = {
            let query_upper = after_select.to_uppercase();
            if let Some(pos) = query_upper.find("FROM") {
                // Verify this is a word boundary, not part of another word
                let before_ok = pos == 0 || query_upper.chars().nth(pos.saturating_sub(1))
                    .map(|c| c.is_whitespace()).unwrap_or(true);
                let after_ok = pos + 4 >= query_upper.len() || query_upper.chars().nth(pos + 4)
                    .map(|c| c.is_whitespace()).unwrap_or(true);
                
                if before_ok && after_ok {
                    debug!("analyze_column_casts: Found FROM at position {}", pos);
                    pos
                } else {
                    debug!("analyze_column_casts: FROM found but not word boundary, using end");
                    after_select.len()
                }
            } else {
                debug!("analyze_column_casts: No FROM found, using end of query");
                after_select.len()
            }
        };
        
        let select_list = &after_select[..from_pos];
        debug!("analyze_column_casts: select_list = {}", select_list);
        
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
                    debug!("analyze_column_casts: Processing expression {}: '{}'", column_idx, current_expr);
                    if let Some(cast_type) = Self::extract_cast_from_expression(&current_expr) {
                        debug!("analyze_column_casts: Found cast: column {} -> {}", column_idx, cast_type);
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
            debug!("analyze_column_casts: Processing last expression: '{}'", current_expr);
            if let Some(cast_type) = Self::extract_cast_from_expression(&current_expr) {
                debug!("analyze_column_casts: Found cast in last expression: column {} -> {}", column_idx, cast_type);
                cast_map.insert(column_idx, cast_type);
            }
        }
        
        debug!("analyze_column_casts: Returning cast_map: {:?}", cast_map);
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
}


/// Extract table name from SELECT query
fn extract_table_name_from_select(query: &str) -> Option<String> {
    // Look for FROM clause using case-insensitive search
    if let Some(from_pos) = find_keyword_position(query, " from ") {
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
    } else {
        None
    }
}

/// Extract table name from CREATE TABLE statement
fn extract_table_name_from_create(query: &str) -> Option<String> {
    // Look for CREATE TABLE pattern
    if let Some(table_pos) = find_keyword_position(query, "CREATE TABLE") {
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
            Some(table_name.to_string())
        } else {
            None
        }
    } else {
        None
    }
}