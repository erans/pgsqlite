use crate::protocol::{ProtocolWriter, FieldDescription, ValueHandler, ValueHandlerConfig, ValueHandlerStats, MappedValue};
use crate::session::DbHandler;
use crate::catalog::CatalogInterceptor;
use crate::translator::{JsonTranslator, ReturningTranslator};
use crate::types::PgType;
use crate::PgSqliteError;
use rusqlite::types::Value as SqliteValue;
use std::collections::HashMap;
use tracing::{info, warn, debug};

/// Memory-mapped query executor that optimizes large value transmission
pub struct MemoryMappedQueryExecutor {
    value_handler: ValueHandler,
}

impl MemoryMappedQueryExecutor {
    /// Create a new memory-mapped query executor with default configuration
    pub fn new() -> Self {
        Self {
            value_handler: ValueHandler::new(),
        }
    }
    
    /// Create a memory-mapped query executor with custom configuration
    pub fn with_config(config: ValueHandlerConfig) -> Self {
        Self {
            value_handler: ValueHandler::with_config(config),
        }
    }
    
    /// Execute a query using memory-mapped value optimization
    pub async fn execute_query(
        &self,
        writer: &mut dyn ProtocolWriter,
        db: &DbHandler,
        query: &str,
    ) -> Result<(), PgSqliteError> {
        info!("Executing memory-mapped query: {}", query);
        
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
                    self.execute_single_statement(writer, db, stmt).await?;
                }
                return Ok(());
            }
        }
        
        // Single statement execution
        self.execute_single_statement(writer, db, query).await
    }
    
    async fn execute_single_statement(
        &self,
        writer: &mut dyn ProtocolWriter,
        db: &DbHandler,
        query: &str,
    ) -> Result<(), PgSqliteError> {
        // Simple query routing
        let query_upper = query.trim().to_uppercase();
        
        if query_upper.starts_with("SELECT") {
            self.execute_select(writer, db, query).await
        } else if query_upper.starts_with("INSERT") 
            || query_upper.starts_with("UPDATE") 
            || query_upper.starts_with("DELETE") {
            self.execute_dml(writer, db, query).await
        } else if query_upper.starts_with("CREATE") 
            || query_upper.starts_with("DROP") 
            || query_upper.starts_with("ALTER") {
            self.execute_ddl(writer, db, query).await
        } else if query_upper.starts_with("BEGIN") 
            || query_upper.starts_with("COMMIT") 
            || query_upper.starts_with("ROLLBACK") {
            self.execute_transaction(writer, db, query).await
        } else {
            // Try to execute as-is
            self.execute_generic(writer, db, query).await
        }
    }
    
    async fn execute_select(
        &self,
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
        let mut schema_types = HashMap::new();
        let mut type_oids = Vec::new();
        
        if let Some(ref table) = table_name {
            for col_name in &response.columns {
                if let Ok(Some(pg_type)) = db.get_schema_type(table, col_name).await {
                    let type_oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(&pg_type);
                    schema_types.insert(col_name.clone(), pg_type);
                    type_oids.push(type_oid);
                } else {
                    // Default to text for missing schema info
                    type_oids.push(PgType::Text.to_oid());
                }
            }
        } else {
            // No table name, use defaults
            for _ in &response.columns {
                type_oids.push(PgType::Text.to_oid());
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
        
        // Convert rows to memory-mapped values and send
        debug!("Converting {} rows with memory-mapped optimization", response.rows.len());
        
        for (row_idx, row) in response.rows.iter().enumerate() {
            // Convert row values to SQLite values for proper type handling
            let sqlite_values: Vec<SqliteValue> = row.iter()
                .map(|opt_bytes| {
                    match opt_bytes {
                        Some(bytes) => {
                            // Try to convert bytes back to appropriate SQLite type
                            if let Ok(text) = String::from_utf8(bytes.clone()) {
                                SqliteValue::Text(text)
                            } else {
                                SqliteValue::Blob(bytes.clone())
                            }
                        }
                        None => SqliteValue::Null,
                    }
                })
                .collect();
            
            // Convert to memory-mapped values
            let mapped_values = self.value_handler.convert_row(&sqlite_values, &type_oids, false)?;
            
            // Prepare values for sending
            let value_refs: Vec<Option<&MappedValue>> = mapped_values.iter()
                .map(|opt| opt.as_ref())
                .collect();
            
            // Send using memory-mapped data row method
            writer.send_data_row_mapped(&value_refs).await?;
            
            // Log large values for monitoring
            for (col_idx, mapped_value) in mapped_values.iter().enumerate() {
                if let Some(value) = mapped_value {
                    if value.len() > self.value_handler.get_memory_stats().mmap_threshold {
                        debug!("Large value in row {} col {}: {} bytes", 
                               row_idx, col_idx, value.len());
                    }
                }
            }
        }
        
        // Send CommandComplete
        let tag = format!("SELECT {}", response.rows_affected);
        writer.send_command_complete(&tag).await?;
        
        Ok(())
    }
    
    async fn execute_dml(
        &self,
        writer: &mut dyn ProtocolWriter,
        db: &DbHandler,
        query: &str,
    ) -> Result<(), PgSqliteError> {
        // Check for RETURNING clause
        if ReturningTranslator::has_returning_clause(query) {
            return self.execute_dml_with_returning(writer, db, query).await;
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
        &self,
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
            
            // Send row description with basic field info
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
            
            // Send data rows using memory-mapped optimization
            for row in &result.rows {
                let sqlite_values: Vec<SqliteValue> = row.iter()
                    .map(|opt_bytes| {
                        match opt_bytes {
                            Some(bytes) => {
                                if let Ok(text) = String::from_utf8(bytes.clone()) {
                                    SqliteValue::Text(text)
                                } else {
                                    SqliteValue::Blob(bytes.clone())
                                }
                            }
                            None => SqliteValue::Null,
                        }
                    })
                    .collect();
                
                let type_oids: Vec<i32> = fields.iter().map(|f| f.type_oid).collect();
                let mapped_values = self.value_handler.convert_row(&sqlite_values, &type_oids, false)?;
                let value_refs: Vec<Option<&MappedValue>> = mapped_values.iter()
                    .map(|opt| opt.as_ref())
                    .collect();
                
                writer.send_data_row_mapped(&value_refs).await?;
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
        &self,
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
        &self,
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
        &self,
        writer: &mut dyn ProtocolWriter,
        db: &DbHandler,
        query: &str,
    ) -> Result<(), PgSqliteError> {
        db.execute(query).await?;
        writer.send_command_complete("OK").await?;
        Ok(())
    }
    
    /// Get memory usage statistics for this executor
    pub fn get_stats(&self) -> ValueHandlerStats {
        self.value_handler.get_memory_stats()
    }
}

impl Default for MemoryMappedQueryExecutor {
    fn default() -> Self {
        Self::new()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::ValueHandlerConfig;
    // use crate::protocol::MemoryMappedConfig; // Used for custom configurations
    
    #[test]
    fn test_memory_mapped_executor_creation() {
        let executor = MemoryMappedQueryExecutor::new();
        let stats = executor.get_stats();
        
        assert_eq!(stats.mmap_threshold, 32 * 1024);
        assert!(!stats.mmap_enabled); // Default is disabled
    }
    
    #[test]
    fn test_memory_mapped_executor_with_config() {
        let mut config = ValueHandlerConfig::default();
        config.enable_mmap = true;
        config.large_value_threshold = 1024;
        
        let executor = MemoryMappedQueryExecutor::with_config(config);
        let stats = executor.get_stats();
        
        assert_eq!(stats.mmap_threshold, 1024);
        assert!(stats.mmap_enabled);
    }
    
    #[test]
    fn test_table_name_extraction() {
        let query = "SELECT * FROM users WHERE id = 1";
        let table_name = extract_table_name_from_select(query);
        assert_eq!(table_name, Some("users".to_string()));
        
        let complex_query = "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id";
        let table_name = extract_table_name_from_select(complex_query);
        assert_eq!(table_name, Some("users".to_string()));
        
        let invalid_query = "INVALID SQL";
        let table_name = extract_table_name_from_select(invalid_query);
        assert!(table_name.is_none());
    }
}