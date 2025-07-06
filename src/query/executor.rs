use crate::protocol::{BackendMessage, FieldDescription};
use crate::session::DbHandler;
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
        query: &str,
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
                    Self::execute_single_statement(framed, db, stmt).await?;
                }
                return Ok(());
            }
        }
        
        // Single statement execution
        Self::execute_single_statement(framed, db, query_to_execute).await
    }
    
    async fn execute_single_statement<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        query: &str,
    ) -> Result<(), PgSqliteError> 
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        // Translate PostgreSQL cast syntax if present
        let translated_query = if crate::translator::CastTranslator::needs_translation(query) {
            use crate::translator::CastTranslator;
            let conn = db.get_mut_connection()
                .map_err(|e| PgSqliteError::Protocol(format!("Failed to get connection: {}", e)))?;
            let translated = CastTranslator::translate_query(query, Some(&conn));
            drop(conn); // Release the connection
            translated
        } else {
            query.to_string()
        };
        
        let query_to_execute = translated_query.as_str();
        
        // Simple query routing using optimized detection
        use crate::query::{QueryTypeDetector, QueryType};
        
        match QueryTypeDetector::detect_query_type(query_to_execute) {
            QueryType::Select => Self::execute_select(framed, db, query_to_execute).await,
            QueryType::Insert | QueryType::Update | QueryType::Delete => {
                Self::execute_dml(framed, db, query_to_execute).await
            }
            QueryType::Create | QueryType::Drop | QueryType::Alter => {
                Self::execute_ddl(framed, db, query_to_execute).await
            }
            QueryType::Begin | QueryType::Commit | QueryType::Rollback => {
                Self::execute_transaction(framed, db, query_to_execute).await
            }
            _ => {
                // Try to execute as-is
                Self::execute_generic(framed, db, query_to_execute).await
            }
        }
    }
    
    async fn execute_select<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        query: &str,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        // Check if this is a catalog query first
        let response = if let Some(catalog_result) = CatalogInterceptor::intercept_query(query, Arc::new(db.clone())).await {
            catalog_result?
        } else {
            db.query(query).await?
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
            if let Some(ref table) = table_name {
                for col_name in &response.columns {
                    if let Ok(Some(pg_type)) = db.get_schema_type(table, col_name).await {
                        schema_types.insert(col_name.clone(), pg_type);
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
                    } else if let Some(aggregate_oid) = crate::types::SchemaTypeMapper::get_aggregate_return_type(name, None, None) {
                        // Second priority: Check for aggregate functions
                        aggregate_oid
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
        framed.send(BackendMessage::RowDescription(fields)).await
            .map_err(|e| PgSqliteError::Io(e))?;
        
        // Optimized data row sending for better SELECT performance
        if response.rows.len() > 5 {
            // Use batch sending for larger result sets
            Self::send_data_rows_batched(framed, response.rows).await?;
        } else {
            // Use individual sending for small result sets
            for row in response.rows {
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
        query: &str,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        // Check for RETURNING clause
        if ReturningTranslator::has_returning_clause(query) {
            return Self::execute_dml_with_returning(framed, db, query).await;
        }
        
        let response = db.execute(query).await?;
        
        // Optimized tag creation with static strings for common cases and buffer pooling for larger counts
        use crate::query::{QueryTypeDetector, QueryType};
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
        query: &str,
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
        query: &str,
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
        
        let (translated_query, type_mappings, enum_columns) = if matches!(QueryTypeDetector::detect_query_type(query), QueryType::Create) && query.trim_start()[6..].trim_start().to_uppercase().starts_with("TABLE") {
            // Use CREATE TABLE translator with connection for ENUM support
            let conn = db.get_mut_connection()
                .map_err(|e| PgSqliteError::Protocol(format!("Failed to get connection: {}", e)))?;
            
            let result = CreateTableTranslator::translate_with_connection_full(query, Some(&conn))
                .map_err(|e| PgSqliteError::Protocol(format!("CREATE TABLE translation failed: {}", e)))?;
            
            // Connection guard is dropped here
            (result.sql, result.type_mappings, result.enum_columns)
        } else {
            // For other DDL, check for JSON/JSONB types
            let translated = if query.to_lowercase().contains("json") || query.to_lowercase().contains("jsonb") {
                JsonTranslator::translate_statement(query)?
            } else {
                query.to_string()
            };
            (translated, std::collections::HashMap::new(), Vec::new())
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
                for (full_column, type_mapping) in type_mappings {
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
        query: &str,
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
        query: &str,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        // Try to execute as a simple statement
        db.execute(query).await?;
        
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
}

/// Extract table name from SELECT statement
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