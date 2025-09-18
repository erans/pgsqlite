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
        
        // Special case: pg_catalog.version() should be handled by SQLite function, not catalog interceptor
        if lower_query.trim() == "select pg_catalog.version()" || 
           lower_query.trim() == "select version()" {
            return None;
        }
        
        // Check for catalog tables
        let has_catalog_tables = lower_query.contains("pg_catalog") || lower_query.contains("pg_type") ||
           lower_query.contains("pg_namespace") || lower_query.contains("pg_range") ||
           lower_query.contains("pg_class") || lower_query.contains("pg_attribute") ||
           lower_query.contains("pg_enum") || lower_query.contains("information_schema") ||
           lower_query.contains("pg_stat_") || lower_query.contains("pg_database") ||
           lower_query.contains("pg_foreign_data_wrapper");
           
        // Check for system functions
        let has_system_functions = lower_query.contains("to_regtype") ||
           lower_query.contains("pg_get_constraintdef") || lower_query.contains("pg_table_is_visible") ||
           lower_query.contains("format_type") || lower_query.contains("pg_get_expr") ||
           lower_query.contains("pg_get_userbyid") || lower_query.contains("pg_get_indexdef") ||
           lower_query.contains("pg_size_pretty");
           
        if !has_catalog_tables && !has_system_functions {
            return None;
        }
        
        debug!("Intercepting catalog query: {}", query);
        
        // Special handling for LIMIT 0 queries used for metadata
        if query.contains("LIMIT 0") {
            // Skipping LIMIT 0 catalog query
            return None;
        }
        
        // First, remove schema prefixes from catalog tables
        let schema_translated = SchemaPrefixTranslator::translate_query(query);
        
        // Then, try to translate regex operators if present
        let query_to_parse = match RegexTranslator::translate_query(&schema_translated) {
            Ok(translated) => {
                if translated != query {
                    // Translated regex operators
                }
                translated
            }
            Err(_) => {
                // Failed to translate regex operators
                query.to_string()
            }
        };

        // Parse the query (keep JSON path placeholders for now)
        let dialect = PostgreSqlDialect {};
        // Parsing query for system functions
        match Parser::parse_sql(&dialect, &query_to_parse) {
            Ok(mut statements) => {
                if statements.len() == 1
                    && let Statement::Query(query_stmt) = &mut statements[0] {
                        // First check if query contains system functions that need processing
                        let contains_functions = Self::query_contains_system_functions(query_stmt);
                        // Query contains system functions
                        if contains_functions {
                            // Clone the query and process system functions
                            match Self::process_system_functions_in_query(query_stmt.clone(), db.clone()).await {
                                Ok(processed_query) => {
                                    // Convert the processed query back to SQL and execute
                                    let mut processed_sql = processed_query.to_string();
                                    // Processed system functions
                                    
                                    // Also translate regex operators
                                    match RegexTranslator::translate_query(&processed_sql) {
                                        Ok(translated) => {
                                            processed_sql = translated;
                                            debug!("Translated regex operators: {}", processed_sql);
                                        }
                                        Err(_) => {
                                            // Failed to translate regex operators
                                        }
                                    }
                                    
                                    // Update the query_to_parse with the processed SQL and continue
                                    // This ensures that catalog queries are handled properly after system function processing
                                    debug!("System functions processed, continuing with catalog handling");
                                    
                                    // Check if the query contains catalog tables
                                    let contains_catalog_tables = processed_sql.to_lowercase().contains("pg_") || 
                                                                 processed_sql.to_lowercase().contains("information_schema");
                                    
                                    if !contains_catalog_tables {
                                        // This is a standalone system function query, execute it directly
                                        debug!("Executing standalone system function query: {}", processed_sql);
                                        match db.query(&processed_sql).await {
                                            Ok(response) => return Some(Ok(response)),
                                            Err(e) => return Some(Err(PgSqliteError::Sqlite(e))),
                                        }
                                    } else {
                                        // Re-parse the processed query to continue with catalog handling
                                        match Parser::parse_sql(&dialect, &processed_sql) {
                                            Ok(mut new_statements) => {
                                                if new_statements.len() == 1
                                                    && let Statement::Query(new_query) = &mut new_statements[0] {
                                                        // Replace the current query with the processed one
                                                        *query_stmt = new_query.clone();
                                                        // Continue to the catalog handling below
                                                    }
                                            }
                                            Err(e) => {
                                                // Failed to re-parse processed query
                                                return Some(Err(PgSqliteError::Protocol(format!("Failed to parse processed query: {e}"))));
                                            }
                                        }
                                    }
                                }
                                Err(_) => {
                                    // Error processing system functions
                                    // Continue with normal catalog handling
                                }
                            }
                        }
                        
                        // Normal catalog table handling
                        if let Some(response) = Self::handle_catalog_query(query_stmt, db.clone(), session.clone()).await {
                            return Some(Ok(response));
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
                // Check if the query contains system functions that need special handling
                let query_str = query.to_string();
                let contains_system_functions = query_str.contains("pg_table_is_visible") || 
                                              query_str.contains("pg_get_constraintdef") ||
                                              query_str.contains("format_type") ||
                                              query_str.contains("pg_get_expr");
                
                if contains_system_functions {
                    // This query contains system functions that need special handling
                    debug!("JOIN query contains system functions, handling specially");
                    
                    // For SQLAlchemy table existence checks, we can handle this specially
                    // The query pattern is checking if a table exists by joining pg_class and pg_namespace
                    // and using pg_table_is_visible
                    if query_str.contains("pg_table_is_visible") && query_str.contains("pg_class.relname") {
                        // Extract the table name being checked
                        let table_name_pattern = regex::Regex::new(r"relname\s*=\s*'([^']+)'").unwrap();
                        if let Some(captures) = table_name_pattern.captures(&query_str)
                            && let Some(table_name) = captures.get(1) {
                                let table_name_str = table_name.as_str();
                                debug!("Checking existence of table: {}", table_name_str);
                                
                                // Check if the table exists in SQLite
                                let check_query = format!(
                                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '{table_name_str}' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__pgsqlite_%'"
                                );
                                
                                match db.query(&check_query).await {
                                    Ok(response) => {
                                        // If we found the table, return a result with the table name
                                        if !response.rows.is_empty() {
                                            debug!("Table {} exists", table_name_str);
                                            return Some(DbResponse {
                                                columns: vec!["relname".to_string()],
                                                rows: vec![vec![Some(table_name_str.as_bytes().to_vec())]],
                                                rows_affected: 1,
                                            });
                                        } else {
                                            debug!("Table {} does not exist", table_name_str);
                                            return Some(DbResponse {
                                                columns: vec!["relname".to_string()],
                                                rows: vec![],
                                                rows_affected: 0,
                                            });
                                        }
                                    }
                                    Err(_) => {
                                        // Error checking table existence
                                    }
                                }
                            }
                    }
                    
                    // For other system functions, fall through to default handling
                    debug!("Unable to handle this specific system function query pattern");
                } else {
                    // For JOIN queries on catalog tables without system functions, 
                    // return None to let SQLite handle it
                    // This allows the views to work properly with JOINs
                    if let TableFactor::Table { name, .. } = &select.from[0].relation {
                        let table_name = name.to_string().to_lowercase();
                        if table_name.contains("pg_") && (table_name.contains("pg_class") || 
                            table_name.contains("pg_namespace") || table_name.contains("pg_attribute") ||
                            table_name.contains("pg_constraint") || table_name.contains("pg_index")) {
                            debug!("Passing JOIN query on catalog tables to SQLite views");
                            return None;
                        }
                    }
                    
                    // Keep special handling for pg_type JOINs since they need custom logic
                    if let TableFactor::Table { name, .. } = &select.from[0].relation {
                        let table_name = name.to_string().to_lowercase();
                        if table_name.contains("pg_type") || table_name.contains("pg_catalog.pg_type") {
                            // This is a pg_type JOIN query - handle it specially
                            return Some(Self::handle_pg_type_join_query(select));
                        }
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
                    Err(_) => {
                        // PgAttributeHandler error
                        None
                    },
                };
            }
            
            // Handle pg_enum queries
            if table_name.contains("pg_enum") || table_name.contains("pg_catalog.pg_enum") {
                return (PgEnumHandler::handle_query(select, &db).await).ok();
            }
            
            // Handle information_schema.schemata queries
            if table_name.contains("information_schema.schemata") {
                return Some(Self::handle_information_schema_schemata_query(select, &db).await);
            }

            // Handle information_schema.tables queries
            if table_name.contains("information_schema.tables") {
                return Some(Self::handle_information_schema_tables_query(select, &db).await);
            }

            // Handle pg_database queries
            if table_name.contains("pg_database") || table_name.contains("pg_catalog.pg_database") {
                return Some(Self::handle_pg_database_query(select, &db).await);
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
                SelectItem::ExprWithAlias { expr, alias } => {
                    // For aliased expressions, we still need the source column for data lookup
                    let source_col = match expr {
                        Expr::Identifier(ident) => Some(ident.value.to_lowercase()),
                        Expr::CompoundIdentifier(parts) => Some(parts.last().unwrap().value.to_lowercase()),
                        Expr::Cast { expr, .. } => {
                            match expr.as_ref() {
                                Expr::Identifier(ident) => Some(ident.value.to_lowercase()),
                                Expr::CompoundIdentifier(parts) => Some(parts.last().unwrap().value.to_lowercase()),
                                _ => None,
                            }
                        }
                        _ => None,
                    };
                    
                    if let Some(col) = source_col {
                        debug!("  Column {} (aliased as {}): {}", i, alias, col);
                        columns.push(col);
                        column_indices.push(i);
                    } else {
                        debug!("  Column {}: unknown aliased expression", i);
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
        
        // Special case: if filter_oid is -1 (our sentinel for NULL), return empty result
        if filter_oid == Some(-1) {
            debug!("NULL OID filter detected - returning empty result set");
            let rows_affected = 0;
            info!("pg_type query with NULL filter: returning 0 rows");
            return DbResponse {
                columns,
                rows,
                rows_affected,
            };
        }
        
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
            if let Some(filter) = filter_oid
                && oid != filter {
                    continue;
                }
            
            // Apply typtype filter if specified
            if let Some(ref filter) = filter_typtype
                && typtype != filter {
                    continue;
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
                    "typarray" => {
                        // Find the array type OID for this base type
                        let array_oid = match oid {
                            16 => 1000,   // bool -> _bool
                            17 => 1001,   // bytea -> _bytea
                            20 => 1016,   // int8 -> _int8
                            21 => 1005,   // int2 -> _int2
                            23 => 1007,   // int4 -> _int4
                            25 => 1009,   // text -> _text
                            700 => 1021,  // float4 -> _float4
                            701 => 1022,  // float8 -> _float8
                            1042 => 1014, // char -> _char
                            1043 => 1015, // varchar -> _varchar
                            1082 => 1182, // date -> _date
                            1083 => 1183, // time -> _time
                            1114 => 1115, // timestamp -> _timestamp
                            1184 => 1185, // timestamptz -> _timestamptz
                            1700 => 1231, // numeric -> _numeric
                            2950 => 2951, // uuid -> _uuid
                            114 => 199,   // json -> _json
                            3802 => 3807, // jsonb -> _jsonb
                            _ => 0,       // No array type
                        };
                        Some(array_oid.to_string().into_bytes())
                    }
                    "typdelim" => Some(",".to_string().into_bytes()), // Default delimiter
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
                    .map_err(PgSqliteError::Sqlite)
            };
            
            if let Ok(enum_types) = enum_types_result {
                debug!("Found {} enum types in metadata", enum_types.len());
                for enum_type in enum_types {
                        debug!("Processing enum type: {} (OID: {})", enum_type.type_name, enum_type.type_oid);
                        // Apply OID filter if specified
                        if let Some(filter) = filter_oid
                            && enum_type.type_oid != filter {
                                continue;
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
                                "typarray" => Some("0".to_string().into_bytes()), // ENUMs don't have array types
                                "typdelim" => Some(",".to_string().into_bytes()), // Default delimiter
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
        
        if let Some(selection) = &select.selection
            && let Expr::BinaryOp { left, op, right } = selection
                && matches!(op, sqlparser::ast::BinaryOperator::Eq) {
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
            if let Some(filter) = filter_oid
                && oid != filter {
                    continue;
                }

            let mut row = Vec::new();
            for col in &columns {
                let value = match col.as_str() {
                    "oid" => Some(oid.to_string().into_bytes()),
                    "typname" => Some(typname.to_string().into_bytes()),
                    "typtype" => Some(typtype.to_string().into_bytes()),
                    "typelem" => Some(typelem.to_string().into_bytes()),
                    "rngsubtype" => None, // NULL for non-range types
                    "typbasetype" => Some(typbasetype.to_string().into_bytes()),
                    "nspname" => Some("pg_catalog".to_string().into_bytes()),
                    "typrelid" => Some(typrelid.to_string().into_bytes()),
                    "typarray" => {
                        // Find the array type OID for this base type
                        let array_oid = match oid {
                            16 => 1000,   // bool -> _bool
                            17 => 1001,   // bytea -> _bytea
                            20 => 1016,   // int8 -> _int8
                            21 => 1005,   // int2 -> _int2
                            23 => 1007,   // int4 -> _int4
                            25 => 1009,   // text -> _text
                            700 => 1021,  // float4 -> _float4
                            701 => 1022,  // float8 -> _float8
                            1042 => 1014, // char -> _char
                            1043 => 1015, // varchar -> _varchar
                            1082 => 1182, // date -> _date
                            1083 => 1183, // time -> _time
                            1114 => 1115, // timestamp -> _timestamp
                            1184 => 1185, // timestamptz -> _timestamptz
                            1700 => 1231, // numeric -> _numeric
                            2950 => 2951, // uuid -> _uuid
                            114 => 199,   // json -> _json
                            3802 => 3807, // jsonb -> _jsonb
                            _ => 0,       // No array type
                        };
                        Some(array_oid.to_string().into_bytes())
                    }
                    "typdelim" => Some(",".to_string().into_bytes()), // Default delimiter
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
    pub fn query_contains_system_functions(query: &sqlparser::ast::Query) -> bool {
        if let SetExpr::Select(select) = &*query.body {
            // Check projections
            for item in &select.projection {
                if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item
                    && Self::expression_contains_system_function(expr) {
                        return true;
                    }
            }
            
            // Check WHERE clause
            if let Some(selection) = &select.selection
                && Self::expression_contains_system_function(selection) {
                    return true;
                }
        }
        false
    }

    /// Check if an expression contains system function calls
    fn expression_contains_system_function(expr: &Expr) -> bool {
        match expr {
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_lowercase();
                debug!("Found function in expression: {}", func_name);
                // Check if it's a known system function
                matches!(func_name.as_str(), 
                    "pg_get_constraintdef" | "pg_table_is_visible" | "format_type" |
                    "pg_get_expr" | "pg_get_userbyid" | "pg_get_indexdef" | "to_regtype" |
                    "pg_catalog.pg_get_constraintdef" | "pg_catalog.pg_table_is_visible" |
                    "pg_catalog.format_type" | "pg_catalog.pg_get_expr" |
                    "pg_catalog.pg_get_userbyid" | "pg_catalog.pg_get_indexdef" | "pg_catalog.to_regtype"
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
    pub async fn process_system_functions_in_query(
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
                    // Check if right side is a number (not a placeholder)
                    match right.as_ref() {
                        Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::Number(n, _), .. }) => {
                            *filter_oid = n.parse::<i32>().ok();
                            debug!("Extracted numeric OID filter: {:?}", filter_oid);
                        }
                        Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::SingleQuotedString(s), .. }) => {
                            // Handle quoted numeric strings (from parameter substitution)
                            if s.to_uppercase() == "NULL" {
                                // Handle 'NULL' string literal as NULL filter
                                *filter_oid = Some(-1); // Use -1 as a sentinel value for NULL
                                debug!("Found 'NULL' string literal - treating as NULL filter");
                            } else {
                                *filter_oid = s.parse::<i32>().ok();
                                debug!("Extracted string OID filter: {:?}", filter_oid);
                            }
                        }
                        Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::Placeholder(_), .. }) => {
                            *has_placeholder = true;
                            debug!("Found placeholder for OID filter");
                        }
                        Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::Null, .. }) => {
                            // NULL filter means no rows should be returned
                            *filter_oid = Some(-1); // Use -1 as a sentinel value for NULL
                            debug!("Found NULL OID filter - no rows will match");
                        }
                        Expr::Function(func) if func.name.to_string().to_lowercase() == "to_regtype" => {
                            // This is a to_regtype function call that hasn't been processed yet
                            debug!("Found to_regtype function in OID filter - needs processing");
                            *has_placeholder = true; // Treat it like a placeholder for now
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
                
                if is_typtype_column
                    && let Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::SingleQuotedString(s), .. }) = right.as_ref() {
                        *filter_typtype = Some(s.clone());
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

    /// Extract selected columns from a SELECT query for information_schema views
    fn extract_selected_columns(select: &Select, all_columns: &[String]) -> (Vec<String>, Vec<usize>) {
        if select.projection.len() == 1
            && let SelectItem::Wildcard(_) = &select.projection[0] {
                // SELECT * - return all columns
                return (all_columns.to_vec(), (0..all_columns.len()).collect::<Vec<_>>());
            }

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

    async fn handle_information_schema_schemata_query(select: &Select, _db: &DbHandler) -> DbResponse {
        debug!("Handling information_schema.schemata query");

        // Define information_schema.schemata columns
        let all_columns = vec![
            "catalog_name".to_string(),
            "schema_name".to_string(),
            "schema_owner".to_string(),
            "default_character_set_catalog".to_string(),
            "default_character_set_schema".to_string(),
            "default_character_set_name".to_string(),
            "sql_path".to_string(),
        ];

        // Extract selected columns
        let (selected_columns, column_indices) = Self::extract_selected_columns(select, &all_columns);

        // Define available schemas
        let schemas = vec![
            ("main", "public", "postgres"),      // Default public schema
            ("main", "pg_catalog", "postgres"),  // System catalog schema
            ("main", "information_schema", "postgres"), // Information schema
        ];

        let mut rows = Vec::new();
        for (catalog, schema, owner) in schemas {
            let full_row: Vec<Option<Vec<u8>>> = vec![
                Some(catalog.to_string().into_bytes()),     // catalog_name
                Some(schema.to_string().into_bytes()),      // schema_name
                Some(owner.to_string().into_bytes()),       // schema_owner
                None,                                       // default_character_set_catalog
                None,                                       // default_character_set_schema
                None,                                       // default_character_set_name
                None,                                       // sql_path
            ];

            let projected_row: Vec<Option<Vec<u8>>> = column_indices.iter()
                .map(|&idx| full_row[idx].clone())
                .collect();

            rows.push(projected_row);
        }

        let rows_affected = rows.len();
        DbResponse {
            columns: selected_columns,
            rows,
            rows_affected,
        }
    }

    async fn handle_information_schema_tables_query(select: &Select, db: &DbHandler) -> DbResponse {
        debug!("Handling information_schema.tables query");

        // Get list of tables and views from SQLite
        let tables_response = match db.query("SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__pgsqlite_%'").await {
            Ok(response) => response,
            Err(_) => return DbResponse {
                columns: vec!["table_name".to_string()],
                rows: vec![],
                rows_affected: 0,
            },
        };

        // Define information_schema.tables columns (enhanced with all PostgreSQL standard columns)
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
            "is_insertable_into".to_string(),
            "is_typed".to_string(),
            "commit_action".to_string(),
        ];

        // Extract selected columns
        let (selected_columns, column_indices) = Self::extract_selected_columns(select, &all_columns);
        
        // Build rows
        let mut rows = Vec::new();
        for table_row in &tables_response.rows {
            if table_row.len() >= 2
                && let (Some(Some(table_name_bytes)), Some(Some(table_type_bytes))) =
                    (table_row.first(), table_row.get(1)) {
                    let table_name = String::from_utf8_lossy(table_name_bytes).to_string();
                    let sqlite_type = String::from_utf8_lossy(table_type_bytes).to_string();

                    // Map SQLite type to PostgreSQL table_type
                    let table_type = match sqlite_type.as_str() {
                        "table" => "BASE TABLE",
                        "view" => "VIEW",
                        _ => "BASE TABLE", // Default fallback
                    };

                    // Determine if table is insertable (views are not)
                    let is_insertable = if table_type == "VIEW" { "NO" } else { "YES" };

                    // Create full row with all columns
                    let full_row: Vec<Option<Vec<u8>>> = vec![
                        Some("main".to_string().into_bytes()),        // table_catalog
                        Some("public".to_string().into_bytes()),      // table_schema
                        Some(table_name.into_bytes()),                // table_name
                        Some(table_type.to_string().into_bytes()),    // table_type
                        None,                                         // self_referencing_column_name
                        None,                                         // reference_generation
                        None,                                         // user_defined_type_catalog
                        None,                                         // user_defined_type_schema
                        None,                                         // user_defined_type_name
                        Some(is_insertable.to_string().into_bytes()), // is_insertable_into
                        Some("NO".to_string().into_bytes()),          // is_typed
                        None,                                         // commit_action
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

    async fn handle_pg_database_query(select: &Select, _db: &DbHandler) -> DbResponse {
        debug!("Handling pg_database query");

        // Define pg_database columns (PostgreSQL 17 compatible)
        let all_columns = vec![
            "oid".to_string(),
            "datname".to_string(),
            "datdba".to_string(),
            "encoding".to_string(),
            "datlocprovider".to_string(),
            "datistemplate".to_string(),
            "datallowconn".to_string(),
            "dathasloginevt".to_string(),
            "datconnlimit".to_string(),
            "datfrozenxid".to_string(),
            "datminmxid".to_string(),
            "dattablespace".to_string(),
            "datcollate".to_string(),
            "datctype".to_string(),
            "datlocale".to_string(),
            "daticurules".to_string(),
            "datcollversion".to_string(),
            "datacl".to_string(),
        ];

        // Extract selected columns
        let (selected_columns, column_indices) = Self::extract_selected_columns(select, &all_columns);

        // Single database entry representing the current SQLite database
        let full_row: Vec<Option<Vec<u8>>> = vec![
            Some("1".to_string().into_bytes()),                        // oid
            Some("main".to_string().into_bytes()),                     // datname - the key field!
            Some("10".to_string().into_bytes()),                       // datdba (owner)
            Some("6".to_string().into_bytes()),                        // encoding (UTF8)
            Some("d".to_string().into_bytes()),                        // datlocprovider (default)
            Some("false".to_string().into_bytes()),                    // datistemplate (false)
            Some("true".to_string().into_bytes()),                     // datallowconn (true)
            Some("false".to_string().into_bytes()),                    // dathasloginevt (false)
            Some("-1".to_string().into_bytes()),                       // datconnlimit (no limit)
            Some("1".to_string().into_bytes()),                        // datfrozenxid
            Some("1".to_string().into_bytes()),                        // datminmxid
            Some("1663".to_string().into_bytes()),                     // dattablespace (default)
            Some("en_US.UTF-8".to_string().into_bytes()),              // datcollate
            Some("en_US.UTF-8".to_string().into_bytes()),              // datctype
            None,                                                      // datlocale
            None,                                                      // daticurules
            None,                                                      // datcollversion
            None,                                                      // datacl
        ];

        // Project only requested columns
        let projected_row: Vec<Option<Vec<u8>>> = column_indices.iter()
            .map(|&idx| full_row[idx].clone())
            .collect();

        DbResponse {
            columns: selected_columns,
            rows: vec![projected_row],
            rows_affected: 1,
        }
    }
}