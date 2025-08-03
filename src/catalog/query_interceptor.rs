use crate::session::db_handler::{DbHandler, DbResponse};
use crate::session::SessionState;
use crate::PgSqliteError;
use crate::translator::{RegexTranslator, SchemaPrefixTranslator};
use sqlparser::ast::{Statement, TableFactor, Select, SetExpr, SelectItem, Expr, FunctionArg, FunctionArgExpr};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Location, Span};
use tracing::{debug, info};
use super::{pg_class::PgClassHandler, pg_attribute::PgAttributeHandler, pg_enum::PgEnumHandler, system_functions::SystemFunctions};
use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;

/// Intercepts and handles queries to pg_catalog tables
pub struct CatalogInterceptor;

impl CatalogInterceptor {
    /// Check if a query is targeting pg_catalog and handle it
    pub async fn intercept_query(query: &str, db: Arc<DbHandler>, session: Option<Arc<SessionState>>) -> Option<Result<DbResponse, PgSqliteError>> {
        // Quick check to avoid parsing if not a catalog query
        let lower_query = query.to_lowercase();
        
        // Check for cache status query
        if lower_query.contains("select * from pgsqlite_cache_status") {
            let (columns, rows) = crate::cache::format_cache_status_as_table();
            let rows_affected = rows.len();
            return Some(Ok(DbResponse {
                columns,
                rows,
                rows_affected,
            }));
        }
        
        if !lower_query.contains("pg_catalog") && !lower_query.contains("pg_type") && 
           !lower_query.contains("pg_namespace") && !lower_query.contains("pg_range") &&
           !lower_query.contains("pg_class") && !lower_query.contains("pg_attribute") &&
           !lower_query.contains("pg_enum") && !lower_query.contains("information_schema") {
            return None;
        }
        
        info!("Intercepting catalog query: {}", query);
        
        // Special handling for LIMIT 0 queries used for metadata
        if query.contains("LIMIT 0") {
            debug!("Skipping LIMIT 0 catalog query");
            return None;
        }
        
        // First, remove schema prefixes from catalog tables
        let schema_translated = SchemaPrefixTranslator::translate_query(query);
        
        // Then, try to translate regex operators if present
        let query_to_parse = match RegexTranslator::translate_query(&schema_translated) {
            Ok(translated) => {
                if translated != query {
                    debug!("Translated regex operators in catalog query: {}", translated);
                }
                translated
            }
            Err(e) => {
                debug!("Failed to translate regex operators: {}", e);
                query.to_string()
            }
        };

        // Parse the query (keep JSON path placeholders for now)
        let dialect = PostgreSqlDialect {};
        match Parser::parse_sql(&dialect, &query_to_parse) {
            Ok(mut statements) => {
                if statements.len() == 1 {
                    if let Statement::Query(query_stmt) = &mut statements[0] {
                        // First check if query contains system functions that need processing
                        if Self::query_contains_system_functions(query_stmt) {
                            // Clone the query and process system functions
                            match Self::process_system_functions_in_query(query_stmt.clone(), db.clone()).await {
                                Ok(processed_query) => {
                                    // Convert the processed query back to SQL and execute
                                    let mut processed_sql = processed_query.to_string();
                                    debug!("Processed query with system functions: {}", processed_sql);
                                    
                                    // Also translate regex operators
                                    match RegexTranslator::translate_query(&processed_sql) {
                                        Ok(translated) => {
                                            processed_sql = translated;
                                            debug!("Translated regex operators: {}", processed_sql);
                                        }
                                        Err(e) => {
                                            debug!("Failed to translate regex operators: {}", e);
                                        }
                                    }
                                    
                                    // Execute the rewritten query directly
                                    match db.query(&processed_sql).await {
                                        Ok(response) => return Some(Ok(response)),
                                        Err(e) => return Some(Err(PgSqliteError::Sqlite(e))),
                                    }
                                }
                                Err(e) => {
                                    debug!("Error processing system functions: {}", e);
                                    // Continue with normal catalog handling
                                }
                            }
                        }
                        
                        // Normal catalog table handling
                        if let Some(response) = Self::handle_catalog_query(query_stmt, db.clone(), session.clone()).await {
                            return Some(Ok(response));
                        }
                    }
                }
                
                // If we translated the query but it's not a special catalog query,
                // execute the translated query directly
                if query_to_parse != query {
                    match db.query(&query_to_parse).await {
                        Ok(response) => return Some(Ok(response)),
                        Err(e) => return Some(Err(PgSqliteError::Sqlite(e))),
                    }
                }
            }
            Err(_) => return None,
        }

        None
    }

    async fn handle_catalog_query(query: &sqlparser::ast::Query, db: Arc<DbHandler>, session: Option<Arc<SessionState>>) -> Option<DbResponse> {
        // Check if this is a SELECT from pg_catalog tables
        if let SetExpr::Select(select) = &*query.body {
            // Check if this is a JOIN query involving catalog tables
            if !select.from.is_empty() && !select.from[0].joins.is_empty() {
                // For JOIN queries on catalog tables, return None to let SQLite handle it
                // This allows the views to work properly with JOINs
                if let TableFactor::Table { name, .. } = &select.from[0].relation {
                    let table_name = name.to_string().to_lowercase();
                    if table_name.contains("pg_") && (table_name.contains("pg_class") || 
                        table_name.contains("pg_namespace") || table_name.contains("pg_attribute") ||
                        table_name.contains("pg_constraint") || table_name.contains("pg_index")) {
                        debug!("Passing JOIN query on catalog tables to SQLite views");
                        return None;
                    }
                    // Keep special handling for pg_type JOINs since they need custom logic
                    if table_name.contains("pg_type") || table_name.contains("pg_catalog.pg_type") {
                        // This is a pg_type JOIN query - handle it specially
                        return Some(Self::handle_pg_type_join_query(select));
                    }
                }
            }
            
            // For simple queries, check each table
            for table_ref in &select.from {
                // Check main table
                if let Some(response) = Self::check_table_factor(&table_ref.relation, select, db.clone(), session.clone()).await {
                    return Some(response);
                }
                
                // Check joined tables
                for join in &table_ref.joins {
                    if let Some(response) = Self::check_table_factor(&join.relation, select, db.clone(), session.clone()).await {
                        return Some(response);
                    }
                }
            }
        }
        
        None
    }
    
    async fn check_table_factor(table_factor: &TableFactor, select: &Select, db: Arc<DbHandler>, session: Option<Arc<SessionState>>) -> Option<DbResponse> {
        if let TableFactor::Table { name, .. } = table_factor {
            let table_name = name.to_string().to_lowercase();
            
            // Handle pg_type queries
            if table_name.contains("pg_type") || table_name.contains("pg_catalog.pg_type") {
                return Some(Self::handle_pg_type_query(select, db.clone(), session.clone()).await);
            }
            
            // Handle pg_namespace queries
            if table_name.contains("pg_namespace") || table_name.contains("pg_catalog.pg_namespace") {
                return Some(Self::handle_pg_namespace_query(select));
            }
            
            // Handle pg_range queries (usually empty)
            if table_name.contains("pg_range") || table_name.contains("pg_catalog.pg_range") {
                return Some(Self::handle_pg_range_query(select));
            }
            
            // Handle pg_class queries
            if table_name.contains("pg_class") || table_name.contains("pg_catalog.pg_class") {
                return (PgClassHandler::handle_query(select, &db).await).ok();
            }
            
            // Handle pg_attribute queries
            if table_name.contains("pg_attribute") || table_name.contains("pg_catalog.pg_attribute") {
                info!("Routing to PgAttributeHandler for table: {}", table_name);
                return match PgAttributeHandler::handle_query(select, &db).await {
                    Ok(response) => {
                        debug!("PgAttributeHandler returned {} rows", response.rows.len());
                        Some(response)
                    },
                    Err(e) => {
                        debug!("PgAttributeHandler error: {}", e);
                        None
                    },
                };
            }
            
            // Handle pg_enum queries
            if table_name.contains("pg_enum") || table_name.contains("pg_catalog.pg_enum") {
                return (PgEnumHandler::handle_query(select, &db).await).ok();
            }
            
            // Handle information_schema.tables queries
            if table_name.contains("information_schema.tables") {
                return Some(Self::handle_information_schema_tables_query(select, &db).await);
            }
        }
        None
    }

    async fn handle_pg_type_query(select: &Select, db: Arc<DbHandler>, session: Option<Arc<SessionState>>) -> DbResponse {
        // Extract which columns are being selected
        let mut columns = Vec::new();
        let mut column_indices = Vec::new();
        
        debug!("Processing pg_type query with {} projections", select.projection.len());
        
        for (i, item) in select.projection.iter().enumerate() {
            match item {
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                    let col_name = parts.last().unwrap().value.to_lowercase();
                    debug!("  Column {}: {}", i, col_name);
                    columns.push(col_name.clone());
                    column_indices.push(i);
                }
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let col_name = ident.value.to_lowercase();
                    debug!("  Column {}: {}", i, col_name);
                    columns.push(col_name.clone());
                    column_indices.push(i);
                }
                SelectItem::UnnamedExpr(Expr::Cast { expr, .. }) => {
                    // Handle CAST expressions like CAST(oid AS TEXT)
                    match expr.as_ref() {
                        Expr::Identifier(ident) => {
                            let col_name = ident.value.to_lowercase();
                            debug!("  Column {} (CAST): {}", i, col_name);
                            columns.push(col_name.clone());
                            column_indices.push(i);
                        }
                        Expr::CompoundIdentifier(parts) => {
                            let col_name = parts.last().unwrap().value.to_lowercase();
                            debug!("  Column {} (CAST): {}", i, col_name);
                            columns.push(col_name.clone());
                            column_indices.push(i);
                        }
                        _ => {
                            debug!("  Column {}: unknown CAST expression", i);
                        }
                    }
                }
                SelectItem::Wildcard(_) => {
                    // Handle SELECT * queries - return all columns
                    debug!("  Wildcard selection - returning all columns");
                    columns = vec![
                        "oid".to_string(),
                        "typname".to_string(),
                        "typtype".to_string(),
                        "typelem".to_string(),
                        "typbasetype".to_string(),
                        "typnamespace".to_string(),
                        "typrelid".to_string(),
                    ];
                    break;
                }
                _ => {
                    debug!("  Column {}: unknown projection type", i);
                }
            }
        }
        
        // If no columns were detected, default to common columns for pg_type
        if columns.is_empty() && !select.projection.is_empty() {
            debug!("No columns detected from projections, using default pg_type columns");
            columns = vec!["oid".to_string(), "typname".to_string()];
        }

        // Check if there's a WHERE clause filtering by OID or typtype
        let mut filter_oid = None;
        let mut has_placeholder = false;
        let mut filter_typtype = None;
        
        if let Some(selection) = &select.selection {
            Self::extract_filters(selection, &mut filter_oid, &mut has_placeholder, &mut filter_typtype);
        }
        
        debug!("Filters - OID: {:?}, typtype: {:?}, has_placeholder: {}", filter_oid, filter_typtype, has_placeholder);
        
        // If query has a placeholder, we need to handle it differently
        // Don't return empty result as tokio-postgres needs the type info
        debug!("Query has placeholder: {}, filter_oid: {:?}", has_placeholder, filter_oid);

        // Build response based on columns requested
        let mut rows = Vec::new();
        
        // Define our basic types - matching all types from type_mapper.rs
        let types = vec![
            // Basic types
            (16, "bool", "b", 0, 0, 11, 0),        // bool
            (17, "bytea", "b", 0, 0, 11, 0),       // bytea
            (20, "int8", "b", 0, 0, 11, 0),        // bigint
            (21, "int2", "b", 0, 0, 11, 0),        // smallint
            (23, "int4", "b", 0, 0, 11, 0),        // integer
            (25, "text", "b", 0, 0, 11, 0),        // text
            (114, "json", "b", 0, 0, 11, 0),       // json
            (700, "float4", "b", 0, 0, 11, 0),     // real
            (701, "float8", "b", 0, 0, 11, 0),     // double precision
            (1042, "char", "b", 0, 0, 11, 0),      // char
            (1043, "varchar", "b", 0, 0, 11, 0),   // varchar
            (1082, "date", "b", 0, 0, 11, 0),      // date
            (1083, "time", "b", 0, 0, 11, 0),      // time
            (1114, "timestamp", "b", 0, 0, 11, 0), // timestamp
            (1184, "timestamptz", "b", 0, 0, 11, 0), // timestamptz
            (1700, "numeric", "b", 0, 0, 11, 0),   // numeric
            (2950, "uuid", "b", 0, 0, 11, 0),      // uuid
            (3802, "jsonb", "b", 0, 0, 11, 0),     // jsonb
            // Array types
            (1000, "_bool", "b", 16, 0, 11, 0),    // bool array
            (1001, "_bytea", "b", 17, 0, 11, 0),   // bytea array
            (1005, "_int2", "b", 21, 0, 11, 0),    // int2 array
            (1007, "_int4", "b", 23, 0, 11, 0),    // int4 array
            (1009, "_text", "b", 25, 0, 11, 0),    // text array
            (1014, "_char", "b", 1042, 0, 11, 0),  // char array
            (1015, "_varchar", "b", 1043, 0, 11, 0), // varchar array
            (1016, "_int8", "b", 20, 0, 11, 0),    // int8 array
            (1021, "_float4", "b", 700, 0, 11, 0), // float4 array
            (1022, "_float8", "b", 701, 0, 11, 0), // float8 array
            (1115, "_timestamp", "b", 1114, 0, 11, 0), // timestamp array
            (1182, "_date", "b", 1082, 0, 11, 0),  // date array
            (1183, "_time", "b", 1083, 0, 11, 0),  // time array
            (1185, "_timestamptz", "b", 1184, 0, 11, 0), // timestamptz array
            (1231, "_numeric", "b", 1700, 0, 11, 0), // numeric array
            (2951, "_uuid", "b", 2950, 0, 11, 0),  // uuid array
            (199, "_json", "b", 114, 0, 11, 0),    // json array
            (3807, "_jsonb", "b", 3802, 0, 11, 0), // jsonb array
        ];

        for (oid, typname, typtype, typelem, typbasetype, _typnamespace, typrelid) in types {
            // Apply OID filter if specified
            if let Some(filter) = filter_oid {
                if oid != filter {
                    continue;
                }
            }
            
            // Apply typtype filter if specified
            if let Some(ref filter) = filter_typtype {
                if typtype != filter {
                    continue;
                }
            }

            let mut row = Vec::new();
            for col in &columns {
                let value = match col.as_str() {
                    "oid" => Some(oid.to_string().into_bytes()),
                    "typname" => Some(typname.to_string().into_bytes()),
                    "typtype" => Some(typtype.to_string().into_bytes()),
                    "typelem" => Some(typelem.to_string().into_bytes()),
                    "typbasetype" => Some(typbasetype.to_string().into_bytes()),
                    "typnamespace" => Some(_typnamespace.to_string().into_bytes()),
                    "typrelid" => Some(typrelid.to_string().into_bytes()),
                    "nspname" => Some("pg_catalog".to_string().into_bytes()),
                    "rngsubtype" => None, // NULL for non-range types
                    _ => None,
                };
                row.push(value);
            }
            
            if !row.is_empty() {
                rows.push(row);
            }
        }
        
        // Add ENUM types from metadata only if typtype filter allows it
        if filter_typtype.is_none() || filter_typtype.as_ref() == Some(&"e".to_string()) {
            // Use session connection if available, otherwise fall back to get_mut_connection
            let enum_types_result = if let Some(ref session) = session {
                db.with_session_connection(&session.id, |conn| {
                    crate::metadata::EnumMetadata::get_all_enum_types(conn)
                        .map_err(|e| rusqlite::Error::SqliteFailure(
                            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                            Some(format!("Failed to get enum types: {e}"))
                        ))
                }).await
            } else {
                db.get_mut_connection()
                    .and_then(|conn| crate::metadata::EnumMetadata::get_all_enum_types(&conn))
                    .map_err(|e| PgSqliteError::Sqlite(e))
            };
            
            if let Ok(enum_types) = enum_types_result {
                debug!("Found {} enum types in metadata", enum_types.len());
                for enum_type in enum_types {
                        debug!("Processing enum type: {} (OID: {})", enum_type.type_name, enum_type.type_oid);
                        // Apply OID filter if specified
                        if let Some(filter) = filter_oid {
                            if enum_type.type_oid != filter {
                                continue;
                            }
                        }
                        
                        let mut row = Vec::new();
                        for col in &columns {
                            let value = match col.as_str() {
                                "oid" => Some(enum_type.type_oid.to_string().into_bytes()),
                                "typname" => Some(enum_type.type_name.clone().into_bytes()),
                                "typtype" => Some("e".to_string().into_bytes()), // 'e' for enum
                                "typelem" => Some("0".to_string().into_bytes()),
                                "typbasetype" => Some("0".to_string().into_bytes()),
                                "typnamespace" => Some(enum_type.namespace_oid.to_string().into_bytes()),
                                "typrelid" => Some("0".to_string().into_bytes()),
                                "nspname" => Some("public".to_string().into_bytes()),
                                "rngsubtype" => None, // NULL for non-range types
                                _ => None,
                            };
                            row.push(value);
                        }
                        
                        if !row.is_empty() {
                            rows.push(row);
                        }
                    }
            }
        }

        let rows_affected = rows.len();
        info!("pg_type query: filter_oid={:?}, filter_typtype={:?}, has_placeholder={}", filter_oid, filter_typtype, has_placeholder);
        info!("Returning {} rows for pg_type query with {} columns: {:?}", rows_affected, columns.len(), columns);
        DbResponse {
            columns,
            rows,
            rows_affected,
        }
    }

    fn handle_pg_namespace_query(_select: &Select) -> DbResponse {
        // Return basic namespaces
        let columns = vec!["oid".to_string(), "nspname".to_string()];
        let rows = vec![
            vec![
                Some("11".to_string().into_bytes()),
                Some("pg_catalog".to_string().into_bytes()),
            ],
            vec![
                Some("2200".to_string().into_bytes()),
                Some("public".to_string().into_bytes()),
            ],
        ];

        let rows_affected = rows.len();
        debug!("Returning {} rows for pg_type query with {} columns: {:?}", rows_affected, columns.len(), columns);
        DbResponse {
            columns,
            rows,
            rows_affected,
        }
    }

    fn handle_pg_range_query(_select: &Select) -> DbResponse {
        // pg_range is typically empty for basic types
        let columns = vec!["rngtypid".to_string(), "rngsubtype".to_string()];
        let rows = vec![];
        let rows_affected = rows.len();

        DbResponse {
            columns,
            rows,
            rows_affected,
        }
    }
    
    fn handle_pg_type_join_query(select: &Select) -> DbResponse {
        // Handle the complex JOIN query that tokio-postgres uses
        // Extract which columns are being selected
        let mut columns = Vec::new();
        
        debug!("Processing pg_type JOIN query with {} projections", select.projection.len());
        
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                    let col_name = parts.last().unwrap().value.to_lowercase();
                    debug!("  Column: {}", col_name);
                    columns.push(col_name);
                }
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let col_name = ident.value.to_lowercase();
                    debug!("  Column: {}", col_name);
                    columns.push(col_name);
                }
                _ => {
                    debug!("  Unknown projection type");
                }
            }
        }

        // Check if there's a WHERE clause filtering by OID
        let mut filter_oid = None;
        
        if let Some(selection) = &select.selection {
            if let Expr::BinaryOp { left, op, right } = selection {
                if matches!(op, sqlparser::ast::BinaryOperator::Eq) {
                    let is_oid_column = if let Expr::CompoundIdentifier(left_parts) = left.as_ref() {
                        left_parts.last().unwrap().value.to_lowercase() == "oid"
                    } else if let Expr::Identifier(ident) = left.as_ref() {
                        ident.value.to_lowercase() == "oid"
                    } else {
                        false
                    };
                    
                    if is_oid_column {
                        // Check if right side is a number or placeholder
                        if let Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::Number(n, _), .. }) = right.as_ref() {
                            filter_oid = n.parse::<i32>().ok();
                        } else if let Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::Placeholder(_), .. }) = right.as_ref() {
                            // For placeholders in JOIN queries, we return all types
                            // tokio-postgres will filter client-side
                            filter_oid = None;
                        }
                    }
                }
            }
        }
        
        // Build response with all requested columns
        let mut rows = Vec::new();
        
        // Define our types with proper values for JOIN query
        let types = vec![
            // Basic types
            (16, "bool", "b", 0, 0, 11, 0),
            (17, "bytea", "b", 0, 0, 11, 0),
            (20, "int8", "b", 0, 0, 11, 0),
            (21, "int2", "b", 0, 0, 11, 0),
            (23, "int4", "b", 0, 0, 11, 0),
            (25, "text", "b", 0, 0, 11, 0),
            (114, "json", "b", 0, 0, 11, 0),
            (700, "float4", "b", 0, 0, 11, 0),
            (701, "float8", "b", 0, 0, 11, 0),
            (1042, "char", "b", 0, 0, 11, 0),
            (1043, "varchar", "b", 0, 0, 11, 0),
            (1082, "date", "b", 0, 0, 11, 0),
            (1083, "time", "b", 0, 0, 11, 0),
            (1114, "timestamp", "b", 0, 0, 11, 0),
            (1184, "timestamptz", "b", 0, 0, 11, 0),
            (1700, "numeric", "b", 0, 0, 11, 0),
            (2950, "uuid", "b", 0, 0, 11, 0),
            (3802, "jsonb", "b", 0, 0, 11, 0),
            // Array types - typtype is still 'b' for arrays in PostgreSQL
            (1000, "_bool", "b", 16, 0, 11, 0),
            (1001, "_bytea", "b", 17, 0, 11, 0),
            (1005, "_int2", "b", 21, 0, 11, 0),
            (1007, "_int4", "b", 23, 0, 11, 0),
            (1009, "_text", "b", 25, 0, 11, 0),
            (1014, "_char", "b", 1042, 0, 11, 0),
            (1015, "_varchar", "b", 1043, 0, 11, 0),
            (1016, "_int8", "b", 20, 0, 11, 0),
            (1021, "_float4", "b", 700, 0, 11, 0),
            (1022, "_float8", "b", 701, 0, 11, 0),
            (1115, "_timestamp", "b", 1114, 0, 11, 0),
            (1182, "_date", "b", 1082, 0, 11, 0),
            (1183, "_time", "b", 1083, 0, 11, 0),
            (1185, "_timestamptz", "b", 1184, 0, 11, 0),
            (1231, "_numeric", "b", 1700, 0, 11, 0),
            (2951, "_uuid", "b", 2950, 0, 11, 0),
            (199, "_json", "b", 114, 0, 11, 0),
            (3807, "_jsonb", "b", 3802, 0, 11, 0),
        ];

        for (oid, typname, typtype, typelem, typbasetype, _typnamespace, typrelid) in types {
            // Apply filter if specified
            if let Some(filter) = filter_oid {
                if oid != filter {
                    continue;
                }
            }

            let mut row = Vec::new();
            for col in &columns {
                let value = match col.as_str() {
                    "typname" => Some(typname.to_string().into_bytes()),
                    "typtype" => Some(typtype.to_string().into_bytes()),
                    "typelem" => Some(typelem.to_string().into_bytes()),
                    "rngsubtype" => None, // NULL for non-range types
                    "typbasetype" => Some(typbasetype.to_string().into_bytes()),
                    "nspname" => Some("pg_catalog".to_string().into_bytes()),
                    "typrelid" => Some(typrelid.to_string().into_bytes()),
                    _ => None,
                };
                row.push(value);
            }
            
            if !row.is_empty() {
                rows.push(row);
            }
        }

        let rows_affected = rows.len();
        debug!("Returning {} rows for pg_type JOIN query", rows_affected);
        DbResponse {
            columns,
            rows,
            rows_affected,
        }
    }

    /// Check if a query contains system function calls
    fn query_contains_system_functions(query: &sqlparser::ast::Query) -> bool {
        if let SetExpr::Select(select) = &*query.body {
            // Check projections
            for item in &select.projection {
                if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
                    if Self::expression_contains_system_function(expr) {
                        return true;
                    }
                }
            }
            
            // Check WHERE clause
            if let Some(selection) = &select.selection {
                if Self::expression_contains_system_function(selection) {
                    return true;
                }
            }
        }
        false
    }

    /// Check if an expression contains system function calls
    fn expression_contains_system_function(expr: &Expr) -> bool {
        match expr {
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_lowercase();
                // Check if it's a known system function
                matches!(func_name.as_str(), 
                    "pg_get_constraintdef" | "pg_table_is_visible" | "format_type" |
                    "pg_get_expr" | "pg_get_userbyid" | "pg_get_indexdef" |
                    "pg_catalog.pg_get_constraintdef" | "pg_catalog.pg_table_is_visible" |
                    "pg_catalog.format_type" | "pg_catalog.pg_get_expr" |
                    "pg_catalog.pg_get_userbyid" | "pg_catalog.pg_get_indexdef"
                )
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::expression_contains_system_function(left) || 
                Self::expression_contains_system_function(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expression_contains_system_function(expr),
            Expr::Cast { expr, .. } => Self::expression_contains_system_function(expr),
            Expr::Case { operand, conditions, else_result, .. } => {
                operand.as_ref().is_some_and(|e| Self::expression_contains_system_function(e)) ||
                conditions.iter().any(|when| Self::expression_contains_system_function(&when.condition) || 
                                           Self::expression_contains_system_function(&when.result)) ||
                else_result.as_ref().is_some_and(|e| Self::expression_contains_system_function(e))
            }
            Expr::InList { expr, list, .. } => {
                Self::expression_contains_system_function(expr) ||
                list.iter().any(Self::expression_contains_system_function)
            }
            Expr::InSubquery { expr, subquery: _, .. } => Self::expression_contains_system_function(expr),
            Expr::Between { expr, low, high, .. } => {
                Self::expression_contains_system_function(expr) ||
                Self::expression_contains_system_function(low) ||
                Self::expression_contains_system_function(high)
            }
            _ => false,
        }
    }

    /// Process system functions in a query by replacing them with their results
    async fn process_system_functions_in_query(
        mut query: Box<sqlparser::ast::Query>,
        db: Arc<DbHandler>,
    ) -> Result<Box<sqlparser::ast::Query>, Box<dyn std::error::Error + Send + Sync>> {
        
        if let SetExpr::Select(select) = &mut *query.body {
            // Process projections
            for item in &mut select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        Self::process_expression(expr, db.clone()).await?;
                    }
                    SelectItem::ExprWithAlias { expr, .. } => {
                        Self::process_expression(expr, db.clone()).await?;
                    }
                    _ => {}
                }
            }
            
            // Process WHERE clause
            if let Some(selection) = &mut select.selection {
                Self::process_expression(selection, db.clone()).await?;
            }
        }
        
        Ok(query)
    }

    /// Extract filter conditions from WHERE clause
    fn extract_filters(expr: &Expr, filter_oid: &mut Option<i32>, has_placeholder: &mut bool, filter_typtype: &mut Option<String>) {
        if let Expr::BinaryOp { left, op, right } = expr {
            if matches!(op, sqlparser::ast::BinaryOperator::Eq) {
                // Check for OID filter
                let is_oid_column = if let Expr::CompoundIdentifier(left_parts) = left.as_ref() {
                    left_parts.last().unwrap().value.to_lowercase() == "oid"
                } else if let Expr::Identifier(ident) = left.as_ref() {
                    ident.value.to_lowercase() == "oid"
                } else {
                    false
                };
                
                if is_oid_column {
                    // Check if right side is a number, placeholder, or function
                    match right.as_ref() {
                        Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::Number(n, _), .. }) => {
                            *filter_oid = n.parse::<i32>().ok();
                            debug!("Extracted numeric OID filter: {:?}", filter_oid);
                        }
                        Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::SingleQuotedString(s), .. }) => {
                            // Handle quoted numeric strings (from parameter substitution)
                            *filter_oid = s.parse::<i32>().ok();
                            debug!("Extracted string OID filter: {:?}", filter_oid);
                        }
                        Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::Placeholder(_), .. }) => {
                            *has_placeholder = true;
                            debug!("Found placeholder for OID filter");
                        }
                        Expr::Function(func) => {
                            let func_name = func.name.to_string().to_lowercase();
                            if func_name == "to_regtype" || func_name == "pg_catalog.to_regtype" {
                                // to_regtype('typename') returns NULL for non-existent types
                                // Since we don't have hstore or other extensions, return -1 which won't match any OID
                                *filter_oid = Some(-1);
                                debug!("Found to_regtype() function, setting filter to -1 (no match)");
                            } else {
                                debug!("Unknown function for OID filter: {}", func_name);
                            }
                        }
                        _ => {
                            debug!("Unknown expression type for OID filter: {:?}", right);
                        }
                    }
                }
                
                // Check for typtype filter
                let is_typtype_column = if let Expr::CompoundIdentifier(left_parts) = left.as_ref() {
                    left_parts.last().unwrap().value.to_lowercase() == "typtype"
                } else if let Expr::Identifier(ident) = left.as_ref() {
                    ident.value.to_lowercase() == "typtype"
                } else {
                    false
                };
                
                if is_typtype_column {
                    if let Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::SingleQuotedString(s), .. }) = right.as_ref() {
                        *filter_typtype = Some(s.clone());
                    }
                }
            } else if matches!(op, sqlparser::ast::BinaryOperator::And) {
                // Recursively check both sides of AND
                Self::extract_filters(left, filter_oid, has_placeholder, filter_typtype);
                Self::extract_filters(right, filter_oid, has_placeholder, filter_typtype);
            }
        }
    }

    /// Process an expression and replace system function calls with their results
    fn process_expression<'a>(
        expr: &'a mut Expr,
        db: Arc<DbHandler>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'a>> {
        Box::pin(async move {
        match expr {
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_lowercase();
                let base_name = if let Some(pos) = func_name.rfind('.') {
                    &func_name[pos + 1..]
                } else {
                    &func_name
                };
                
                // Extract arguments
                let mut args = Vec::new();
                if let sqlparser::ast::FunctionArguments::List(func_arg_list) = &func.args {
                    for arg in &func_arg_list.args {
                        match arg {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => args.push(e.clone()),
                            FunctionArg::Named { arg: FunctionArgExpr::Expr(e), .. } => args.push(e.clone()),
                            _ => {}
                        }
                    }
                }
                
                // Process the function call
                if let Some(result) = SystemFunctions::process_function_call(base_name, &args, db).await? {
                    // Replace the function call with its result
                    *expr = Expr::Value(sqlparser::ast::ValueWithSpan { 
                        value: sqlparser::ast::Value::SingleQuotedString(result),
                        span: Span {
                            start: Location { line: 1, column: 1 },
                            end: Location { line: 1, column: 1 }
                        }
                    });
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::process_expression(left, db.clone()).await?;
                Self::process_expression(right, db.clone()).await?;
            }
            Expr::UnaryOp { expr: inner_expr, .. } => {
                Self::process_expression(inner_expr, db.clone()).await?;
            }
            Expr::Cast { expr: inner_expr, .. } => {
                Self::process_expression(inner_expr, db.clone()).await?;
            }
            Expr::Case { operand, conditions, else_result, .. } => {
                if let Some(op) = operand {
                    Self::process_expression(op, db.clone()).await?;
                }
                for when in conditions.iter_mut() {
                    Self::process_expression(&mut when.condition, db.clone()).await?;
                    Self::process_expression(&mut when.result, db.clone()).await?;
                }
                if let Some(else_res) = else_result {
                    Self::process_expression(else_res, db.clone()).await?;
                }
            }
            Expr::InList { expr: inner_expr, list, .. } => {
                Self::process_expression(inner_expr, db.clone()).await?;
                for item in list {
                    Self::process_expression(item, db.clone()).await?;
                }
            }
            Expr::Between { expr: inner_expr, low, high, .. } => {
                Self::process_expression(inner_expr, db.clone()).await?;
                Self::process_expression(low, db.clone()).await?;
                Self::process_expression(high, db.clone()).await?;
            }
            _ => {}
        }
        Ok(())
        })
    }

    async fn handle_information_schema_tables_query(select: &Select, db: &DbHandler) -> DbResponse {
        debug!("Handling information_schema.tables query");
        
        // Get list of tables from SQLite
        let tables_response = match db.query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__pgsqlite_%'").await {
            Ok(response) => response,
            Err(_) => return DbResponse {
                columns: vec!["table_name".to_string()],
                rows: vec![],
                rows_affected: 0,
            },
        };
        
        // Define information_schema.tables columns
        let all_columns = vec![
            "table_catalog".to_string(),
            "table_schema".to_string(),
            "table_name".to_string(),
            "table_type".to_string(),
            "self_referencing_column_name".to_string(),
            "reference_generation".to_string(),
            "user_defined_type_catalog".to_string(),
            "user_defined_type_schema".to_string(),
            "user_defined_type_name".to_string(),
        ];
        
        // Determine which columns to return based on SELECT clause
        let (selected_columns, column_indices) = if select.projection.len() == 1 {
            if let SelectItem::Wildcard(_) = &select.projection[0] {
                // SELECT * - return all columns
                (all_columns.clone(), (0..all_columns.len()).collect::<Vec<_>>())
            } else {
                // Extract specific columns
                let mut cols = Vec::new();
                let mut indices = Vec::new();
                for item in &select.projection {
                    if let SelectItem::UnnamedExpr(Expr::Identifier(ident)) = item {
                        let col_name = ident.value.to_string();
                        if let Some(idx) = all_columns.iter().position(|c| c == &col_name) {
                            cols.push(col_name);
                            indices.push(idx);
                        }
                    }
                }
                (cols, indices)
            }
        } else {
            // Multiple specific columns
            let mut cols = Vec::new();
            let mut indices = Vec::new();
            for item in &select.projection {
                if let SelectItem::UnnamedExpr(Expr::Identifier(ident)) = item {
                    let col_name = ident.value.to_string();
                    if let Some(idx) = all_columns.iter().position(|c| c == &col_name) {
                        cols.push(col_name);
                        indices.push(idx);
                    }
                }
            }
            (cols, indices)
        };
        
        // Build rows
        let mut rows = Vec::new();
        for table_row in &tables_response.rows {
            if let Some(Some(table_name_bytes)) = table_row.get(0) {
                let table_name = String::from_utf8_lossy(table_name_bytes).to_string();
                
                // Create full row with all columns
                let full_row: Vec<Option<Vec<u8>>> = vec![
                    Some("main".to_string().into_bytes()),        // table_catalog
                    Some("public".to_string().into_bytes()),      // table_schema  
                    Some(table_name.into_bytes()),                // table_name
                    Some("BASE TABLE".to_string().into_bytes()),  // table_type
                    None,                                         // self_referencing_column_name
                    None,                                         // reference_generation
                    None,                                         // user_defined_type_catalog
                    None,                                         // user_defined_type_schema
                    None,                                         // user_defined_type_name
                ];
                
                // Project only the requested columns
                let projected_row: Vec<Option<Vec<u8>>> = column_indices.iter()
                    .map(|&idx| full_row[idx].clone())
                    .collect();
                
                rows.push(projected_row);
            }
        }
        
        let rows_count = rows.len();
        DbResponse {
            columns: selected_columns,
            rows,
            rows_affected: rows_count,
        }
    }
}