use crate::protocol::{BackendMessage, FieldDescription};
use crate::session::DbHandler;
use crate::catalog::CatalogInterceptor;
use crate::translator::{JsonTranslator, ReturningTranslator};
use crate::types::PgType;
use crate::PgSqliteError;
use tokio_util::codec::Framed;
use futures::SinkExt;
use tracing::{info, debug};

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
        info!("Executing query: {}", query);
        
        // Check if query contains multiple statements
        let trimmed = query.trim();
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
        Self::execute_single_statement(framed, db, query).await
    }
    
    async fn execute_single_statement<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        db: &DbHandler,
        query: &str,
    ) -> Result<(), PgSqliteError> 
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        // Simple query routing
        let query_upper = query.trim().to_uppercase();
        
        if query_upper.starts_with("SELECT") {
            Self::execute_select(framed, db, query).await
        } else if query_upper.starts_with("INSERT") 
            || query_upper.starts_with("UPDATE") 
            || query_upper.starts_with("DELETE") {
            Self::execute_dml(framed, db, query).await
        } else if query_upper.starts_with("CREATE") 
            || query_upper.starts_with("DROP") 
            || query_upper.starts_with("ALTER") {
            Self::execute_ddl(framed, db, query).await
        } else if query_upper.starts_with("BEGIN") 
            || query_upper.starts_with("COMMIT") 
            || query_upper.starts_with("ROLLBACK") {
            Self::execute_transaction(framed, db, query).await
        } else {
            // Try to execute as-is
            Self::execute_generic(framed, db, query).await
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
        let response = if let Some(catalog_result) = CatalogInterceptor::intercept_query(query) {
            catalog_result?
        } else {
            db.query(query).await?
        };
        
        // Extract table name from query to look up schema
        let table_name = extract_table_name_from_select(query);
        
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
                    crate::types::SchemaTypeMapper::pg_type_string_to_oid(pg_type)
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
        
        // Send RowDescription
        framed.send(BackendMessage::RowDescription(fields)).await
            .map_err(|e| PgSqliteError::Io(e))?;
        
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
        
        #[cfg(feature = "zero-copy-protocol")]
        {
            // Use optimized path if zero-copy is enabled
            if crate::query::should_use_zero_copy() {
                use crate::query::QueryExecutorZeroCopy;
                
                // DbHandler contains Arc internally, so we can pass it directly
                return Self::execute_dml_optimized(framed, db, query).await;
            }
        }
        
        let response = db.execute(query).await?;
        
        let tag = if query.trim_start().to_uppercase().starts_with("INSERT") {
            format!("INSERT 0 {}", response.rows_affected)
        } else if query.trim_start().to_uppercase().starts_with("UPDATE") {
            format!("UPDATE {}", response.rows_affected)
        } else if query.trim_start().to_uppercase().starts_with("DELETE") {
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
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        let (base_query, returning_clause) = ReturningTranslator::extract_returning_clause(query)
            .ok_or_else(|| PgSqliteError::Protocol("Failed to parse RETURNING clause".to_string()))?;
        
        let query_upper = base_query.trim_start().to_uppercase();
        
        if query_upper.starts_with("INSERT") {
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
        } else if query_upper.starts_with("UPDATE") {
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
        } else if query_upper.starts_with("DELETE") {
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
        
        let (translated_query, type_mappings) = if query.trim_start().to_uppercase().starts_with("CREATE TABLE") {
            // Use CREATE TABLE translator
            CreateTableTranslator::translate(query)
                .map_err(|e| PgSqliteError::Protocol(format!("CREATE TABLE translation failed: {}", e)))?
        } else {
            // For other DDL, check for JSON/JSONB types
            let translated = if query.to_lowercase().contains("json") || query.to_lowercase().contains("jsonb") {
                JsonTranslator::translate_statement(query)?
            } else {
                query.to_string()
            };
            (translated, std::collections::HashMap::new())
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
            }
        }
        
        let tag = if query.trim_start().to_uppercase().starts_with("CREATE TABLE") {
            "CREATE TABLE".to_string()
        } else if query.trim_start().to_uppercase().starts_with("DROP TABLE") {
            "DROP TABLE".to_string()
        } else if query.trim_start().to_uppercase().starts_with("CREATE INDEX") {
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
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        let query_upper = query.trim().to_uppercase();
        
        if query_upper.starts_with("BEGIN") {
            db.execute("BEGIN").await?;
            framed.send(BackendMessage::CommandComplete { tag: "BEGIN".to_string() }).await
                .map_err(|e| PgSqliteError::Io(e))?;
        } else if query_upper.starts_with("COMMIT") {
            db.execute("COMMIT").await?;
            framed.send(BackendMessage::CommandComplete { tag: "COMMIT".to_string() }).await
                .map_err(|e| PgSqliteError::Io(e))?;
        } else if query_upper.starts_with("ROLLBACK") {
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
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        // Try to execute as a simple statement
        db.execute(query).await?;
        
        framed.send(BackendMessage::CommandComplete { tag: "OK".to_string() }).await
            .map_err(|e| PgSqliteError::Io(e))?;
        
        Ok(())
    }
}

/// Extract table name from SELECT statement
fn extract_table_name_from_select(query: &str) -> Option<String> {
    let query_lower = query.to_lowercase();
    
    // Look for FROM clause
    if let Some(from_pos) = query_lower.find(" from ") {
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
    let query_upper = query.to_uppercase();
    
    // Look for CREATE TABLE pattern
    if let Some(table_pos) = query_upper.find("CREATE TABLE") {
        let after_create = &query[table_pos + 12..].trim();
        
        // Skip IF NOT EXISTS if present
        let after_create = if after_create.to_uppercase().starts_with("IF NOT EXISTS") {
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