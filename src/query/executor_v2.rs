use crate::protocol::{ProtocolWriter, FieldDescription};
use crate::session::DbHandler;
use crate::catalog::CatalogInterceptor;
use crate::translator::{JsonTranslator, ReturningTranslator};
use crate::types::PgType;
use crate::PgSqliteError;
use tracing::{info, warn, debug};

/// QueryExecutor V2 - Uses ProtocolWriter trait for zero-copy support
pub struct QueryExecutorV2;

impl QueryExecutorV2 {
    /// Execute a query using the ProtocolWriter trait
    pub async fn execute_query(
        writer: &mut dyn ProtocolWriter,
        db: &DbHandler,
        query: &str,
    ) -> Result<(), PgSqliteError> {
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
                    Self::execute_single_statement(writer, db, stmt).await?;
                }
                return Ok(());
            }
        }
        
        // Single statement execution
        Self::execute_single_statement(writer, db, query).await
    }
    
    async fn execute_single_statement(
        writer: &mut dyn ProtocolWriter,
        db: &DbHandler,
        query: &str,
    ) -> Result<(), PgSqliteError> {
        // Simple query routing
        let query_upper = query.trim().to_uppercase();
        
        if query_upper.starts_with("SELECT") {
            Self::execute_select(writer, db, query).await
        } else if query_upper.starts_with("INSERT") 
            || query_upper.starts_with("UPDATE") 
            || query_upper.starts_with("DELETE") {
            Self::execute_dml(writer, db, query).await
        } else if query_upper.starts_with("CREATE") 
            || query_upper.starts_with("DROP") 
            || query_upper.starts_with("ALTER") {
            Self::execute_ddl(writer, db, query).await
        } else if query_upper.starts_with("BEGIN") 
            || query_upper.starts_with("COMMIT") 
            || query_upper.starts_with("ROLLBACK") {
            Self::execute_transaction(writer, db, query).await
        } else {
            // Try to execute as-is
            Self::execute_generic(writer, db, query).await
        }
    }
    
    async fn execute_select(
        writer: &mut dyn ProtocolWriter,
        db: &DbHandler,
        query: &str,
    ) -> Result<(), PgSqliteError> {
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
                            // For user tables, missing metadata is an error
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
        writer.send_row_description(&fields).await?;
        
        // Send data rows with batching if enabled
        #[cfg(feature = "zero-copy-protocol")]
        {
            use crate::query::BatchConfig;
            let batch_config = BatchConfig::from_env();
            
            if batch_config.enabled && response.rows.len() > batch_config.row_batch_size {
                // Send rows in batches
                for (i, row) in response.rows.iter().enumerate() {
                    writer.send_data_row(row).await?;
                    
                    if (i + 1) % batch_config.row_batch_size == 0 {
                        writer.flush().await?;
                    }
                }
                
                // Final flush if needed
                if response.rows.len() % batch_config.row_batch_size != 0 {
                    writer.flush().await?;
                }
            } else {
                // Send all rows without batching
                for row in &response.rows {
                    writer.send_data_row(row).await?;
                }
            }
        }
        
        #[cfg(not(feature = "zero-copy-protocol"))]
        {
            // Send all rows without batching
            for row in &response.rows {
                writer.send_data_row(row).await?;
            }
        }
        
        // Send CommandComplete
        let tag = format!("SELECT {}", response.rows_affected);
        writer.send_command_complete(&tag).await?;
        
        Ok(())
    }
    
    async fn execute_dml(
        writer: &mut dyn ProtocolWriter,
        db: &DbHandler,
        query: &str,
    ) -> Result<(), PgSqliteError> {
        // Check for RETURNING clause
        if ReturningTranslator::has_returning_clause(query) {
            return Self::execute_dml_with_returning(writer, db, query).await;
        }
        
        let response = db.execute(query).await?;
        
        // Use optimized tag creation for common cases
        let query_upper = query.trim_start().to_uppercase();
        
        if query_upper.starts_with("INSERT") {
            match response.rows_affected {
                0 => writer.send_command_complete("INSERT 0 0").await?,
                1 => writer.send_command_complete("INSERT 0 1").await?,
                n => writer.send_command_complete(&format!("INSERT 0 {}", n)).await?,
            }
        } else if query_upper.starts_with("UPDATE") {
            match response.rows_affected {
                0 => writer.send_command_complete("UPDATE 0").await?,
                1 => writer.send_command_complete("UPDATE 1").await?,
                n => writer.send_command_complete(&format!("UPDATE {}", n)).await?,
            }
        } else if query_upper.starts_with("DELETE") {
            match response.rows_affected {
                0 => writer.send_command_complete("DELETE 0").await?,
                1 => writer.send_command_complete("DELETE 1").await?,
                n => writer.send_command_complete(&format!("DELETE {}", n)).await?,
            }
        } else {
            writer.send_command_complete(&format!("OK {}", response.rows_affected)).await?;
        }
        
        Ok(())
    }
    
    async fn execute_dml_with_returning(
        writer: &mut dyn ProtocolWriter,
        db: &DbHandler,
        query: &str,
    ) -> Result<(), PgSqliteError> {
        let (base_query, returning_clause) = ReturningTranslator::extract_returning_clause(query)
            .ok_or_else(|| PgSqliteError::Protocol("Failed to parse RETURNING clause".to_string()))?;
        
        let query_upper = base_query.trim_start().to_uppercase();
        
        if query_upper.starts_with("INSERT") {
            // For INSERT, execute the insert and then query by last_insert_rowid
            let table_name = ReturningTranslator::extract_table_from_insert(&base_query)
                .ok_or_else(|| PgSqliteError::Protocol("Failed to extract table name".to_string()))?;
            
            // Execute the INSERT
            let response = db.execute(&base_query).await?;
            
            // Get the last inserted row using RETURNING columns
            let capture_query = format!(
                "SELECT {} FROM {} WHERE rowid = last_insert_rowid()",
                returning_clause, table_name
            );
            let result = db.query(&capture_query).await?;
            
            // Send row description
            let fields: Vec<FieldDescription> = result.columns.iter()
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
            
            writer.send_row_description(&fields).await?;
            
            // Send data rows
            for row in &result.rows {
                writer.send_data_row(row).await?;
            }
            
            // Send command complete
            writer.send_command_complete(&format!("INSERT 0 {}", response.rows_affected)).await?;
        } else {
            // For UPDATE/DELETE, we need different handling
            return Err(PgSqliteError::Protocol(
                "RETURNING clause is only supported for INSERT statements currently".to_string()
            ));
        }
        
        Ok(())
    }
    
    async fn execute_ddl(
        writer: &mut dyn ProtocolWriter,
        db: &DbHandler,
        query: &str,
    ) -> Result<(), PgSqliteError> {
        // Handle JSON/JSONB types
        let translated_query = JsonTranslator::translate_statement(query)?;
        
        db.execute(&translated_query).await?;
        
        // Return appropriate command tag
        let query_upper = query.trim().to_uppercase();
        let tag = if query_upper.starts_with("CREATE TABLE") {
            "CREATE TABLE"
        } else if query_upper.starts_with("DROP TABLE") {
            "DROP TABLE"
        } else if query_upper.starts_with("ALTER TABLE") {
            "ALTER TABLE"
        } else if query_upper.starts_with("CREATE INDEX") {
            "CREATE INDEX"
        } else if query_upper.starts_with("DROP INDEX") {
            "DROP INDEX"
        } else {
            "OK"
        };
        
        writer.send_command_complete(tag).await?;
        Ok(())
    }
    
    async fn execute_transaction(
        writer: &mut dyn ProtocolWriter,
        db: &DbHandler,
        query: &str,
    ) -> Result<(), PgSqliteError> {
        db.execute(query).await?;
        
        let query_upper = query.trim().to_uppercase();
        let tag = if query_upper.starts_with("BEGIN") {
            "BEGIN"
        } else if query_upper.starts_with("COMMIT") {
            "COMMIT"
        } else if query_upper.starts_with("ROLLBACK") {
            "ROLLBACK"
        } else {
            "OK"
        };
        
        writer.send_command_complete(tag).await?;
        Ok(())
    }
    
    async fn execute_generic(
        writer: &mut dyn ProtocolWriter,
        db: &DbHandler,
        query: &str,
    ) -> Result<(), PgSqliteError> {
        db.execute(query).await?;
        writer.send_command_complete("OK").await?;
        Ok(())
    }
}

/// Extract table name from SELECT query (simplified version)
fn extract_table_name_from_select(query: &str) -> Option<String> {
    use sqlparser::parser::Parser;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::ast::{Statement, TableFactor};
    
    let dialect = PostgreSqlDialect {};
    match Parser::parse_sql(&dialect, query) {
        Ok(statements) => {
            if let Some(Statement::Query(query)) = statements.first() {
                if let Some(from) = &query.body.as_select()?.from.first() {
                    if let TableFactor::Table { name, .. } = &from.relation {
                        return Some(name.0.first()?.as_ident()?.value.clone());
                    }
                }
            }
        }
        Err(e) => {
            warn!("Failed to parse query for table extraction: {}", e);
        }
    }
    None
}