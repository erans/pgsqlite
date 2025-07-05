use crate::protocol::{BackendMessage, FieldDescription};
use crate::session::{DbHandler, SessionState, PreparedStatement, Portal, GLOBAL_QUERY_CACHE};
use crate::catalog::CatalogInterceptor;
use crate::translator::{JsonTranslator, ReturningTranslator};
use crate::types::{DecimalHandler, PgType};
use crate::cache::{RowDescriptionKey, GLOBAL_ROW_DESCRIPTION_CACHE, GLOBAL_PARAMETER_CACHE, CachedParameterInfo};
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
    pub async fn handle_parse<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        session: &Arc<SessionState>,
        name: String,
        query: String,
        param_types: Vec<i32>,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // Strip SQL comments first to avoid parsing issues
        let cleaned_query = crate::query::strip_sql_comments(&query);
        
        // Check if query is empty after comment stripping
        if cleaned_query.trim().is_empty() {
            return Err(PgSqliteError::Protocol("Empty query".to_string()));
        }
        
        info!("Parsing statement '{}': {}", name, cleaned_query);
        info!("Provided param_types: {:?}", param_types);
        
        // Check if this is a simple parameter SELECT (e.g., SELECT $1, $2)
        let is_simple_param_select = query_starts_with_ignore_case(&query, "SELECT") && 
            !query.to_uppercase().contains("FROM") && 
            query.contains('$');
        
        // For INSERT and SELECT queries, we need to determine parameter types from the target table schema
        let mut actual_param_types = param_types.clone();
        if param_types.is_empty() && cleaned_query.contains('$') {
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
                        created_at: std::time::Instant::now(),
                    });
                } else {
                    // Need to analyze the query
                    let (analyzed_types, original_types_opt, table_name, column_names) = if query_starts_with_ignore_case(&query, "INSERT") {
                        match Self::analyze_insert_params(&query, db).await {
                            Ok((types, orig_types)) => {
                                info!("Analyzed INSERT parameter types: {:?} (original: {:?})", types, orig_types);
                                
                                // Extract table and columns for caching
                                let (table, cols) = crate::types::QueryContextAnalyzer::get_insert_column_info(&query)
                                    .unwrap_or_else(|| (String::new(), Vec::new()));
                                
                                (types, Some(orig_types), Some(table), cols)
                            }
                            Err(_) => {
                                // If we can't determine types, default to text
                                let param_count = (1..=99).filter(|i| query.contains(&format!("${}", i))).count();
                                let types = vec![PgType::Text.to_oid(); param_count];
                                (types.clone(), Some(types), None, Vec::new())
                            }
                        }
                    } else if query_starts_with_ignore_case(&query, "SELECT") {
                        let types = Self::analyze_select_params(&query, db).await.unwrap_or_else(|_| {
                            // If we can't determine types, default to text
                            let param_count = (1..=99).filter(|i| query.contains(&format!("${}", i))).count();
                            vec![PgType::Text.to_oid(); param_count]
                        });
                        info!("Analyzed SELECT parameter types: {:?}", types);
                        
                        let table = extract_table_name_from_select(&query);
                        (types.clone(), Some(types), table, Vec::new())
                    } else {
                        // Other query types - just count parameters
                        let param_count = (1..=99).filter(|i| query.contains(&format!("${}", i))).count();
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
                    
                    // Also update query cache if it's a parseable query
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
        
        // For now, we'll just analyze the query to get field descriptions
        // In a real implementation, we'd parse the SQL and validate it
        info!("Analyzing query '{}' for field descriptions", cleaned_query);
        info!("Is simple param select: {}", is_simple_param_select);
        let field_descriptions = if query_starts_with_ignore_case(&cleaned_query, "SELECT") {
            // Don't try to get field descriptions if this is a catalog query
            // These queries are handled specially and don't need real field info
            if cleaned_query.contains("pg_catalog") || cleaned_query.contains("pg_type") {
                info!("Skipping field description for catalog query");
                Vec::new()
            } else {
                // Try to get field descriptions
                // For parameterized queries, substitute dummy values
                let mut test_query = cleaned_query.to_string();
                let param_count = (1..=99).filter(|i| cleaned_query.contains(&format!("${}", i))).count();
                
                if param_count > 0 {
                    // Replace parameters with dummy values
                    for i in 1..=param_count {
                        test_query = test_query.replace(&format!("${}", i), "NULL");
                    }
                }
                
                // First, analyze the original query for type casts in the SELECT clause
                let cast_info = Self::analyze_column_casts(&cleaned_query);
                info!("Detected column casts: {:?}", cast_info);
                
                // Remove PostgreSQL-style type casts before executing
                // Be careful not to match IPv6 addresses like ::1
                let cast_regex = regex::Regex::new(r"::[a-zA-Z]\w*").unwrap();
                test_query = cast_regex.replace_all(&test_query, "").to_string();
                
                // Add LIMIT 1 to avoid processing too much data
                test_query = format!("{} LIMIT 1", test_query);
                let test_response = db.query(&test_query).await;
                
                match test_response {
                    Ok(response) => {
                        info!("Test query returned {} columns: {:?}", response.columns.len(), response.columns);
                        // Extract table name from query to look up schema
                        let table_name = extract_table_name_from_select(&query);
                        
                        // Pre-fetch schema types for all columns if we have a table name
                        let mut schema_types = std::collections::HashMap::new();
                        if let Some(ref table) = table_name {
                            for col_name in &response.columns {
                                if let Ok(Some(pg_type)) = db.get_schema_type(table, col_name).await {
                                    schema_types.insert(col_name.clone(), pg_type);
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
                                
                                // Second priority: Check schema table for stored type mappings
                                if let Some(pg_type) = schema_types.get(col_name) {
                                    // Need to check if this is an ENUM type
                                    if let Ok(conn) = db.get_mut_connection() {
                                        return crate::types::SchemaTypeMapper::pg_type_string_to_oid_with_enum_check(pg_type, &*conn);
                                    }
                                    return crate::types::SchemaTypeMapper::pg_type_string_to_oid(pg_type);
                                }
                                
                                // Third priority: Check for aggregate functions
                                let col_lower = col_name.to_lowercase();
                                if let Some(oid) = crate::types::SchemaTypeMapper::get_aggregate_return_type(&col_lower, None, None) {
                                    return oid;
                                }
                                
                                // Last resort: Try to infer from value if we have data
                                if !response.rows.is_empty() {
                                    if let Some(value) = response.rows[0].get(i) {
                                        crate::types::SchemaTypeMapper::infer_type_from_value(value.as_deref())
                                    } else {
                                        PgType::Text.to_oid() // text for NULL
                                    }
                                } else {
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
        
        // If param_types is empty but query has parameters, infer basic types
        if actual_param_types.is_empty() && cleaned_query.contains('$') {
            // Count parameters in the query
            let mut max_param = 0;
            for i in 1..=99 {
                if cleaned_query.contains(&format!("${}", i)) {
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
        let stmt = PreparedStatement {
            query: cleaned_query.clone(),
            param_types: actual_param_types.clone(),
            param_formats: vec![0; actual_param_types.len()], // Default to text format
            field_descriptions,
        };
        
        session.prepared_statements.write().await.insert(name.clone(), stmt);
        
        // Send ParseComplete
        framed.send(BackendMessage::ParseComplete).await
            .map_err(|e| PgSqliteError::Io(e))?;
        
        Ok(())
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
        info!("Binding portal '{}' to statement '{}' with {} values", portal, statement, values.len());
        
        // Get the prepared statement
        let statements = session.prepared_statements.read().await;
        let stmt = statements.get(&statement)
            .ok_or_else(|| PgSqliteError::Protocol(format!("Unknown statement: {}", statement)))?;
            
        info!("Statement has param_types: {:?}", stmt.param_types);
        info!("Received param formats: {:?}", formats);
        
        // Check if we need to infer types (only when param types are empty or unknown)
        let needs_inference = stmt.param_types.is_empty() || 
            stmt.param_types.iter().all(|&t| t == 0);
        
        let mut inferred_types = None;
        
        if needs_inference && !values.is_empty() {
            info!("Need to infer parameter types from values");
            info!("Statement param_types: {:?}", stmt.param_types);
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
                let hex_preview = v.iter().take(20).map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join(" ");
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
        session.portals.write().await.insert(portal.clone(), portal_obj);
        
        // Send BindComplete
        framed.send(BackendMessage::BindComplete).await
            .map_err(|e| PgSqliteError::Io(e))?;
        
        Ok(())
    }
    
    pub async fn handle_execute<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        session: &Arc<SessionState>,
        portal: String,
        max_rows: i32,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        info!("Executing portal '{}' with max_rows: {}", portal, max_rows);
        
        // Get the portal
        let (query, bound_values, param_formats, result_formats, statement_name, inferred_param_types) = {
            let portals = session.portals.read().await;
            let portal_obj = portals.get(&portal)
                .ok_or_else(|| PgSqliteError::Protocol(format!("Unknown portal: {}", portal)))?;
            
            (portal_obj.query.clone(), 
             portal_obj.bound_values.clone(),
             portal_obj.param_formats.clone(),
             portal_obj.result_formats.clone(),
             portal_obj.statement_name.clone(),
             portal_obj.inferred_param_types.clone())
        };
        
        // Get parameter types from the prepared statement
        let param_types = if let Some(inferred) = inferred_param_types {
            // Use inferred types if available
            inferred
        } else {
            let statements = session.prepared_statements.read().await;
            let stmt = statements.get(&statement_name).unwrap();
            stmt.param_types.clone()
        };
        
        // Try optimized extended fast path first for parameterized queries
        if !bound_values.is_empty() && query.contains('$') {
            let query_type = super::extended_fast_path::QueryType::from_query(&query);
            
            // Early check: Skip fast path for SELECT with binary results
            if matches!(query_type, super::extended_fast_path::QueryType::Select) 
                && !result_formats.is_empty() 
                && result_formats[0] == 1 {
                // Skip fast path entirely for binary SELECT results
            } else {
            
            // Get original types from cache if available
            let original_types = if let Some(cached_info) = GLOBAL_PARAMETER_CACHE.get(&query) {
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
                        &query,
                        &bound_values,
                        &param_formats,
                        &result_formats,
                        &param_types,
                        &original_types,
                        query_type,
                    ).await {
                        Ok(true) => return Ok(()), // Successfully executed via fast path
                        Ok(false) => {}, // Fall back to normal path
                        Err(e) => {
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
                    &fast_query, 
                    max_rows
                ).await {
                    return result;
                }
            }
        }

        // Convert bound values and substitute parameters
        let final_query = Self::substitute_parameters(&query, &bound_values, &param_formats, &param_types)?;
        
        info!("Executing query: {}", final_query);
        debug!("Original query: {}", query);
        debug!("Final query after substitution: {}", final_query);
        debug!("Original query had {} bound values", bound_values.len());
        
        
        // Debug: Check if this is a catalog query
        if final_query.contains("pg_catalog") || final_query.contains("pg_type") {
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
            Self::execute_ddl(framed, db, &final_query).await?;
        } else if query_starts_with_ignore_case(&final_query, "BEGIN") 
            || query_starts_with_ignore_case(&final_query, "COMMIT") 
            || query_starts_with_ignore_case(&final_query, "ROLLBACK") {
            Self::execute_transaction(framed, db, &final_query).await?;
        } else {
            Self::execute_generic(framed, db, &final_query).await?;
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
                .ok_or_else(|| PgSqliteError::Protocol(format!("Unknown statement: {}", name)))?;
            
            // Send ParameterDescription first
            framed.send(BackendMessage::ParameterDescription(stmt.param_types.clone())).await
                .map_err(|e| PgSqliteError::Io(e))?;
            
            // Check if this is a catalog query that needs special handling
            let query = &stmt.query;
            let is_catalog_query = query.contains("pg_catalog") || query.contains("pg_type") || 
                                   query.contains("pg_namespace") || query.contains("pg_class") || 
                                   query.contains("pg_attribute");
            
            // Then send RowDescription or NoData
            if !stmt.field_descriptions.is_empty() {
                info!("Sending RowDescription with {} fields in Describe", stmt.field_descriptions.len());
                framed.send(BackendMessage::RowDescription(stmt.field_descriptions.clone())).await
                    .map_err(|e| PgSqliteError::Io(e))?;
            } else if is_catalog_query && query_starts_with_ignore_case(query, "SELECT") {
                // For catalog SELECT queries, we need to provide field descriptions
                // even though we skipped them during Parse
                info!("Catalog query detected in Describe, generating field descriptions");
                
                // Parse the query to extract the selected columns
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
                        .map_err(|e| PgSqliteError::Io(e))?;
                } else {
                    // Fallback to NoData if we couldn't parse the query
                    info!("Could not determine catalog fields, sending NoData in Describe");
                    framed.send(BackendMessage::NoData).await
                        .map_err(|e| PgSqliteError::Io(e))?;
                }
            } else {
                info!("Sending NoData in Describe");
                framed.send(BackendMessage::NoData).await
                    .map_err(|e| PgSqliteError::Io(e))?;
            }
        } else {
            // Describe portal
            let portals = session.portals.read().await;
            let portal = portals.get(&name)
                .ok_or_else(|| PgSqliteError::Protocol(format!("Unknown portal: {}", name)))?;
            
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
                info!("Describe portal: sending updated fields: {:?}", fields);
                framed.send(BackendMessage::RowDescription(fields)).await
                    .map_err(|e| PgSqliteError::Io(e))?;
            } else {
                framed.send(BackendMessage::NoData).await
                    .map_err(|e| PgSqliteError::Io(e))?;
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
            session.portals.write().await.remove(&name);
        }
        
        // Send CloseComplete
        framed.send(BackendMessage::CloseComplete).await
            .map_err(|e| PgSqliteError::Io(e))?;
        
        Ok(())
    }
    
    async fn try_execute_fast_path_with_params<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
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
        
        // Get result formats from portal
        let result_formats = {
            let portals = session.portals.read().await;
            let portal_obj = portals.get(portal).unwrap();
            portal_obj.result_formats.clone()
        };
        
        // Try fast path execution first
        if let Ok(Some(response)) = db.try_execute_fast_path_with_params(query, &rusqlite_params).await {
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
                // SELECT operation - send full response
                Self::send_select_response(framed, response, max_rows, &result_formats).await?;
            }
            return Ok(Some(Ok(())));
        }
        
        // Try statement pool execution for parameterized queries
        if let Ok(response) = Self::try_statement_pool_execution(db, query, &rusqlite_params, &fast_query).await {
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
                // SELECT operation
                Self::send_select_response(framed, response, max_rows, &result_formats).await?;
            }
            return Ok(Some(Ok(())));
        }
        
        Ok(None) // Fast path didn't work, fall back to normal execution
    }
    
    async fn try_statement_pool_execution(
        db: &DbHandler,
        query: &str,
        params: &[rusqlite::types::Value],
        fast_query: &crate::query::FastPathQuery,
    ) -> Result<crate::session::db_handler::DbResponse, PgSqliteError> {
        // Only try statement pool for queries without decimal columns
        // (decimal queries need rewriting which complicates caching)
        match fast_query.operation {
            crate::query::FastPathOperation::Select => {
                db.query_with_statement_pool_params(query, params)
                    .await
                    .map_err(|e| PgSqliteError::Sqlite(e))
            }
            _ => {
                db.execute_with_statement_pool_params(query, params)
                    .await
                    .map_err(|e| PgSqliteError::Sqlite(e))
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
                _ => Ok(rusqlite::types::Value::Text(text.to_string())), // Default to TEXT
            }
        } else {
            // Binary format - simplified for now, fall back to normal path
            Err(PgSqliteError::Protocol("Binary format not supported in fast path".to_string()))
        }
    }
    
    async fn send_select_response<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        response: crate::session::db_handler::DbResponse,
        _max_rows: i32,
        result_formats: &[i16],
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // Send RowDescription
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
        framed.send(BackendMessage::RowDescription(field_descriptions)).await?;
        
        // Send DataRows
        for row in response.rows {
            let mut values = Vec::new();
            for cell in row {
                values.push(cell);
            }
            framed.send(BackendMessage::DataRow(values)).await?;
        }
        
        // Send CommandComplete
        framed.send(BackendMessage::CommandComplete { tag: format!("SELECT {}", response.rows_affected) }).await?;
        
        Ok(())
    }
    
    fn substitute_parameters(query: &str, values: &[Option<Vec<u8>>], formats: &[i16], param_types: &[i32]) -> Result<String, PgSqliteError> {
        let mut result = query.to_string();
        
        
        // Simple parameter substitution - replace $1, $2, etc. with actual values
        // This is a simplified version - a real implementation would parse the SQL
        for (i, value) in values.iter().enumerate() {
            let param = format!("${}", i + 1);
            let format = formats.get(i).copied().unwrap_or(0); // Default to text format
            let param_type = param_types.get(i).copied().unwrap_or(PgType::Text.to_oid()); // Default to text
            
            
            
            let replacement = match value {
                None => "NULL".to_string(),
                Some(bytes) => {
                    if format == 1 {
                        // Binary format - decode based on expected type
                        match param_type {
                            t if t == PgType::Int4.to_oid() => {
                                // int4
                                if bytes.len() == 4 {
                                    let value = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                                    info!("Decoded binary int32 parameter {}: {}", i + 1, value);
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
                                    let formatted = format!("'${:.2}'", dollars);
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
                                        debug!("Failed to decode binary NUMERIC parameter: {}", e);
                                        return Err(PgSqliteError::InvalidParameter(format!("Invalid binary NUMERIC: {}", e)));
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
                                                debug!("Invalid NUMERIC parameter: {}", e);
                                                return Err(PgSqliteError::InvalidParameter(format!("Invalid NUMERIC value: {}", e)));
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
            result = result.replace(&param, &replacement);
        }
        
        // Remove PostgreSQL-style casts (::type) as SQLite doesn't support them
        // Be careful not to match IPv6 addresses like ::1
        let cast_regex = regex::Regex::new(r"::[a-zA-Z]\w*").unwrap();
        result = cast_regex.replace_all(&result, "").to_string();
        
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
                    current_byte = (current_byte << 1) | 0;
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
                                        _ => return Ok(encoded_row), // Invalid boolean
                                    };
                                    Some(vec![val])
                                } else {
                                    Some(bytes.clone())
                                }
                            }
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
                            t if t == PgType::Int4.to_oid() => {
                                // int4 - convert text to binary
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Ok(val) = s.parse::<i32>() {
                                        let mut buf = vec![0u8; 4];
                                        BigEndian::write_i32(&mut buf, val);
                                        Some(buf)
                                    } else {
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            t if t == PgType::Int8.to_oid() => {
                                // int8 - convert text to binary
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Ok(val) = s.parse::<i64>() {
                                        let mut buf = vec![0u8; 8];
                                        BigEndian::write_i64(&mut buf, val);
                                        Some(buf)
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
                                    if let Some(days) = Self::date_to_pg_days(&s) {
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
                                    if let Some(micros) = Self::time_to_microseconds(&s) {
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
                                    if let Some(micros) = Self::timestamp_to_pg_microseconds(&s) {
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
                            // Numeric type
                            t if t == PgType::Numeric.to_oid() => {
                                // numeric - use DecimalHandler for proper encoding
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    match DecimalHandler::parse_decimal(&s) {
                                        Ok(decimal) => {
                                            let encoded = DecimalHandler::encode_numeric(&decimal);
                                            Some(encoded)
                                        }
                                        Err(_) => {
                                            // If parsing fails, keep as text
                                            Some(bytes.clone())
                                        }
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
                                result.extend_from_slice(&bytes);
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
        db: &DbHandler,
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
        let response = if let Some(catalog_result) = CatalogInterceptor::intercept_query(query, Arc::new(db.clone())).await {
            info!("execute_select: Query intercepted by catalog handler");
            let mut catalog_response = catalog_result?;
            
            // For catalog queries with binary result formats, we need to ensure the data
            // is in the correct format for binary encoding
            let portals = session.portals.read().await;
            let portal = portals.get(portal_name).unwrap();
            let has_binary_format = portal.result_formats.iter().any(|&f| f == 1);
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
            db.query(query).await?
        };
        
        // Check if we need to send RowDescription
        // We send it if:
        // 1. The prepared statement had no field descriptions (wasn't Described or Describe sent NoData)
        // BUT NOT for catalog queries - they should already have field descriptions from Describe
        let send_row_desc = {
            let portals = session.portals.read().await;
            let portal = portals.get(portal_name).unwrap();
            let statements = session.prepared_statements.read().await;
            let stmt = statements.get(&portal.statement_name).unwrap();
            let needs_row_desc = stmt.field_descriptions.is_empty() && !response.columns.is_empty();
            drop(statements);
            drop(portals);
            needs_row_desc
        };
        
        if send_row_desc {
            // Extract table name from query to look up schema
            let table_name = extract_table_name_from_select(&query);
            
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
                            // Need to check if this is an ENUM type
                            let oid = if let Ok(conn) = db.get_mut_connection() {
                                crate::types::SchemaTypeMapper::pg_type_string_to_oid_with_enum_check(pg_type, &*conn)
                            } else {
                                crate::types::SchemaTypeMapper::pg_type_string_to_oid(pg_type)
                            };
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
                .map_err(|e| PgSqliteError::Io(e))?;
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
        
        // Send data rows (respecting max_rows if specified)
        let rows_to_send = if max_rows > 0 {
            response.rows.into_iter().take(max_rows as usize).collect()
        } else {
            response.rows
        };
        
        let sent_count = rows_to_send.len();
        
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
                            format!("{:?}", data)
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
        
        for row in rows_to_send {
            // Convert row data based on result formats
            let encoded_row = Self::encode_row(&row, &result_formats, &field_types)?;
            framed.send(BackendMessage::DataRow(encoded_row)).await
                .map_err(|e| PgSqliteError::Io(e))?;
        }
        
        // Send appropriate completion message
        if max_rows > 0 && sent_count == max_rows as usize {
            framed.send(BackendMessage::PortalSuspended).await
                .map_err(|e| PgSqliteError::Io(e))?;
        } else {
            let tag = format!("SELECT {}", sent_count);
            framed.send(BackendMessage::CommandComplete { tag }).await
                .map_err(|e| PgSqliteError::Io(e))?;
        }
        
        Ok(())
    }
    
    async fn execute_dml<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        query: &str,
        portal_name: &str,
        session: &Arc<SessionState>,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // Check for RETURNING clause
        if ReturningTranslator::has_returning_clause(query) {
            // Get result formats from portal
            let result_formats = {
                let portals = session.portals.read().await;
                let portal = portals.get(portal_name).unwrap();
                portal.result_formats.clone()
            };
            return Self::execute_dml_with_returning(framed, db, query, &result_formats).await;
        }
        
        let response = db.execute(query).await?;
        
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
            .map_err(|e| PgSqliteError::Io(e))?;
        
        Ok(())
    }
    
    async fn execute_dml_with_returning<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
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
            let response = db.execute(&base_query).await?;
            
            // Get the last inserted rowid and query for RETURNING data
            let returning_query = format!(
                "SELECT {} FROM {} WHERE rowid = last_insert_rowid()",
                returning_clause,
                table_name
            );
            
            let returning_response = db.query(&returning_query).await?;
            
            // Send row description
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
                        type_oid: 25, // Default to text
                        type_size: -1,
                        type_modifier: -1,
                        format,
                    }
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
        } else if query_starts_with_ignore_case(&base_query, "UPDATE") {
            // For UPDATE, we need a different approach
            let table_name = ReturningTranslator::extract_table_from_update(&base_query)
                .ok_or_else(|| PgSqliteError::Protocol("Failed to extract table name".to_string()))?;
            
            // First, get the rowids of rows that will be updated
            let where_clause = ReturningTranslator::extract_where_clause(&base_query);
            let rowid_query = format!(
                "SELECT rowid FROM {} {}",
                table_name,
                where_clause
            );
            let rowid_response = db.query(&rowid_query).await?;
            let rowids: Vec<String> = rowid_response.rows.iter()
                .filter_map(|row| row[0].as_ref())
                .map(|bytes| String::from_utf8_lossy(bytes).to_string())
                .collect();
            
            // Execute the UPDATE
            let response = db.execute(&base_query).await?;
            
            // Now query the updated rows
            if !rowids.is_empty() {
                let rowid_list = rowids.join(",");
                let returning_query = format!(
                    "SELECT {} FROM {} WHERE rowid IN ({})",
                    returning_clause,
                    table_name,
                    rowid_list
                );
                
                let returning_response = db.query(&returning_query).await?;
                
                // Send row description
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
                            type_oid: 25,
                            type_size: -1,
                            type_modifier: -1,
                            format,
                        }
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
            let captured_rows = db.query(&capture_query).await?;
            
            // Execute the actual DELETE
            let response = db.execute(&base_query).await?;
            
            // Send row description
            let fields: Vec<FieldDescription> = captured_rows.columns.iter()
                .skip(1) // Skip rowid column
                .enumerate()
                .map(|(i, name)| FieldDescription {
                    name: name.clone(),
                    table_oid: 0,
                    column_id: (i + 1) as i16,
                    type_oid: 25,
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
        query: &str,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        use crate::ddl::EnumDdlHandler;
        
        // Check if this is an ENUM DDL statement first
        if EnumDdlHandler::is_enum_ddl(query) {
            // Handle the ENUM DDL
            {
                let mut conn = db.get_mut_connection()
                    .map_err(|e| PgSqliteError::Protocol(format!("Failed to get connection: {}", e)))?;
                
                EnumDdlHandler::handle_enum_ddl(&mut conn, query)?;
            } // Mutex guard is dropped here
            
            // Send command complete
            let command_tag = if query.trim().to_uppercase().starts_with("CREATE TYPE") {
                "CREATE TYPE"
            } else if query.trim().to_uppercase().starts_with("ALTER TYPE") {
                "ALTER TYPE"
            } else if query.trim().to_uppercase().starts_with("DROP TYPE") {
                "DROP TYPE"
            } else {
                "OK"
            };
            
            framed.send(BackendMessage::CommandComplete { tag: command_tag.to_string() }).await?;
            return Ok(());
        }
        
        // Handle CREATE TABLE translation
        let translated_query = if query_starts_with_ignore_case(query, "CREATE TABLE") {
            // Use translator with connection for ENUM support
            let (sqlite_sql, type_mappings) = {
                let conn = db.get_mut_connection()
                    .map_err(|e| PgSqliteError::Protocol(format!("Failed to get connection: {}", e)))?;
                
                crate::translator::CreateTableTranslator::translate_with_connection(query, Some(&*conn))
                    .map_err(|e| PgSqliteError::Protocol(e))?
            }; // Drop connection guard here
            
            // Execute the translated CREATE TABLE
            db.execute(&sqlite_sql).await?;
            
            // Store the type mappings if we have any
            info!("Type mappings count: {}", type_mappings.len());
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
                    let _ = db.execute(init_query).await;
                    
                    // Store each type mapping
                    for (full_column, type_mapping) in type_mappings {
                        // Split table.column format
                        let parts: Vec<&str> = full_column.split('.').collect();
                        if parts.len() == 2 && parts[0] == table_name {
                            let insert_query = format!(
                                "INSERT OR REPLACE INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES ('{}', '{}', '{}', '{}')",
                                table_name, parts[1], type_mapping.pg_type, type_mapping.sqlite_type
                            );
                            let _ = db.execute(&insert_query).await;
                        }
                    }
                    
                    info!("Stored type mappings for table {} (extended query protocol)", table_name);
                }
            }
            
            // Send CommandComplete and return
            framed.send(BackendMessage::CommandComplete { tag: "CREATE TABLE".to_string() }).await
                .map_err(|e| PgSqliteError::Io(e))?;
            
            return Ok(());
        } else if query.to_lowercase().contains("json") || query.to_lowercase().contains("jsonb") {
            JsonTranslator::translate_statement(query)?
        } else {
            query.to_string()
        };
        
        db.execute(&translated_query).await?;
        
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
            .map_err(|e| PgSqliteError::Io(e))?;
        
        Ok(())
    }
    
    async fn execute_transaction<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        query: &str,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        if query_starts_with_ignore_case(query, "BEGIN") {
            db.execute("BEGIN").await?;
            framed.send(BackendMessage::CommandComplete { tag: "BEGIN".to_string() }).await
                .map_err(|e| PgSqliteError::Io(e))?;
        } else if query_starts_with_ignore_case(query, "COMMIT") {
            db.execute("COMMIT").await?;
            framed.send(BackendMessage::CommandComplete { tag: "COMMIT".to_string() }).await
                .map_err(|e| PgSqliteError::Io(e))?;
        } else if query_starts_with_ignore_case(query, "ROLLBACK") {
            db.execute("ROLLBACK").await?;
            framed.send(BackendMessage::CommandComplete { tag: "ROLLBACK".to_string() }).await
                .map_err(|e| PgSqliteError::Io(e))?;
        }
        
        Ok(())
    }
    
    async fn execute_generic<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        query: &str,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        db.execute(query).await?;
        
        framed.send(BackendMessage::CommandComplete { tag: "OK".to_string() }).await
            .map_err(|e| PgSqliteError::Io(e))?;
        
        Ok(())
    }
    
    /// Analyze INSERT query to determine parameter types from schema
    async fn analyze_insert_params(query: &str, db: &DbHandler) -> Result<(Vec<i32>, Vec<i32>), PgSqliteError> {
        // Use QueryContextAnalyzer to extract table and column info
        let (table_name, columns) = crate::types::QueryContextAnalyzer::get_insert_column_info(query)
            .ok_or_else(|| PgSqliteError::Protocol("Failed to parse INSERT query".to_string()))?;
        
        info!("Analyzing INSERT for table '{}' with columns: {:?}", table_name, columns);
        
        // Get cached table schema
        let table_schema = db.get_table_schema(&table_name).await
            .map_err(|e| PgSqliteError::Protocol(format!("Failed to get table schema: {}", e)))?;
        
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
    async fn analyze_select_params(query: &str, db: &DbHandler) -> Result<Vec<i32>, PgSqliteError> {
        // First, check for explicit parameter casts like $1::int4
        let mut param_types = Vec::new();
        
        // Count parameters and try to determine their types
        for i in 1..=99 {
            let param = format!("${}", i);
            if !query.contains(&param) {
                break;
            }
            
            // Check for explicit cast first (e.g., $1::int4)
            let cast_pattern = format!(r"\${}::\s*(\w+)", i);
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
                            let schema_query = format!("PRAGMA table_info({})", table_name);
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
                if let Ok(_) = trimmed.parse::<i32>() {
                    return PgType::Int4.to_oid();
                }
                
                // Check for bigint
                if let Ok(_) = trimmed.parse::<i64>() {
                    return PgType::Int8.to_oid();
                }
                
                // Check for float
                if let Ok(_) = trimmed.parse::<f64>() {
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