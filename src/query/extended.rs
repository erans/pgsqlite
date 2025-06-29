use crate::protocol::{BackendMessage, FieldDescription};
use crate::session::{DbHandler, SessionState, PreparedStatement, Portal};
use crate::catalog::CatalogInterceptor;
use crate::translator::{JsonTranslator, ReturningTranslator};
use crate::types::DecimalHandler;
use crate::PgSqliteError;
use tokio_util::codec::Framed;
use futures::SinkExt;
use tracing::{info, warn, error};
use std::sync::Arc;
use byteorder::{BigEndian, ByteOrder};
use chrono::{NaiveDate, NaiveTime, NaiveDateTime, Timelike};

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
        info!("Parsing statement '{}': {}", name, query);
        info!("Provided param_types: {:?}", param_types);
        
        // For INSERT and SELECT queries, we need to determine parameter types from the target table schema
        let mut actual_param_types = param_types.clone();
        if param_types.is_empty() && query.contains('$') {
            if query.trim().to_uppercase().starts_with("INSERT") {
                actual_param_types = Self::analyze_insert_params(&query, db).await.unwrap_or_else(|_| {
                    // If we can't determine types, default to text
                    let param_count = (1..=99).filter(|i| query.contains(&format!("${}", i))).count();
                    vec![25; param_count]
                });
                info!("Analyzed INSERT parameter types: {:?}", actual_param_types);
            } else if query.trim().to_uppercase().starts_with("SELECT") {
                actual_param_types = Self::analyze_select_params(&query, db).await.unwrap_or_else(|_| {
                    // If we can't determine types, default to text
                    let param_count = (1..=99).filter(|i| query.contains(&format!("${}", i))).count();
                    vec![25; param_count]
                });
                info!("Analyzed SELECT parameter types: {:?}", actual_param_types);
            }
        }
        
        // For now, we'll just analyze the query to get field descriptions
        // In a real implementation, we'd parse the SQL and validate it
        let field_descriptions = if query.trim().to_uppercase().starts_with("SELECT") {
            // Don't try to get field descriptions if this is a catalog query
            // These queries are handled specially and don't need real field info
            if query.contains("pg_catalog") || query.contains("pg_type") {
                info!("Skipping field description for catalog query");
                Vec::new()
            } else {
                // Try to get field descriptions
                // For parameterized queries, substitute dummy values
                let mut test_query = query.to_string();
                let param_count = (1..=99).filter(|i| query.contains(&format!("${}", i))).count();
                
                if param_count > 0 {
                    // Replace parameters with dummy values
                    for i in 1..=param_count {
                        test_query = test_query.replace(&format!("${}", i), "NULL");
                    }
                }
                
                // First, analyze the original query for type casts in the SELECT clause
                let cast_info = Self::analyze_column_casts(&query);
                info!("Detected column casts: {:?}", cast_info);
                
                // Remove PostgreSQL-style type casts before executing
                let cast_regex = regex::Regex::new(r"::\w+").unwrap();
                test_query = cast_regex.replace_all(&test_query, "").to_string();
                
                // Add LIMIT 1 to avoid processing too much data
                test_query = format!("{} LIMIT 1", test_query);
                let test_response = db.query(&test_query).await;
                
                match test_response {
                    Ok(response) => {
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
                                
                                // Second priority: Check schema table for stored type mappings
                                if let Some(pg_type) = schema_types.get(col_name) {
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
                                        25 // text for NULL
                                    }
                                } else {
                                    25 // text default when no data
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
        if actual_param_types.is_empty() && query.contains('$') {
            // Count parameters in the query
            let mut max_param = 0;
            for i in 1..=99 {
                if query.contains(&format!("${}", i)) {
                    max_param = i;
                } else if max_param > 0 {
                    break;
                }
            }
            
            info!("Query has {} parameters, defaulting all to text", max_param);
            // Default all to text - we'll handle type conversion during execution
            actual_param_types = vec![25; max_param];
        }
        
        info!("Final param_types for statement: {:?}", actual_param_types);
        
        // Store the prepared statement
        let stmt = PreparedStatement {
            query: query.clone(),
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
        let (query, bound_values, param_formats, statement_name) = {
            let portals = session.portals.read().await;
            let portal_obj = portals.get(&portal)
                .ok_or_else(|| PgSqliteError::Protocol(format!("Unknown portal: {}", portal)))?;
            
            (portal_obj.query.clone(), 
             portal_obj.bound_values.clone(),
             portal_obj.param_formats.clone(),
             portal_obj.statement_name.clone())
        };
        
        // Get parameter types from the prepared statement
        let param_types = {
            let statements = session.prepared_statements.read().await;
            let stmt = statements.get(&statement_name).unwrap();
            stmt.param_types.clone()
        };
        
        // Convert bound values and substitute parameters
        let final_query = Self::substitute_parameters(&query, &bound_values, &param_formats, &param_types)?;
        
        info!("Executing query: {}", final_query);
        info!("Original query had {} bound values", bound_values.len());
        
        // Debug: Check if this is a catalog query
        if final_query.contains("pg_catalog") || final_query.contains("pg_type") {
            info!("Detected catalog query in extended protocol: {}", final_query);
        }
        
        // Execute based on query type
        let query_upper = final_query.trim().to_uppercase();
        
        if query_upper.starts_with("SELECT") {
            Self::execute_select(framed, db, session, &portal, &final_query, max_rows).await?;
        } else if query_upper.starts_with("INSERT") 
            || query_upper.starts_with("UPDATE") 
            || query_upper.starts_with("DELETE") {
            Self::execute_dml(framed, db, &final_query).await?;
        } else if query_upper.starts_with("CREATE") 
            || query_upper.starts_with("DROP") 
            || query_upper.starts_with("ALTER") {
            Self::execute_ddl(framed, db, &final_query).await?;
        } else if query_upper.starts_with("BEGIN") 
            || query_upper.starts_with("COMMIT") 
            || query_upper.starts_with("ROLLBACK") {
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
            
            // Then send RowDescription or NoData
            if !stmt.field_descriptions.is_empty() {
                info!("Sending RowDescription with {} fields in Describe", stmt.field_descriptions.len());
                framed.send(BackendMessage::RowDescription(stmt.field_descriptions.clone())).await
                    .map_err(|e| PgSqliteError::Io(e))?;
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
                framed.send(BackendMessage::RowDescription(stmt.field_descriptions.clone())).await
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
    
    fn substitute_parameters(query: &str, values: &[Option<Vec<u8>>], formats: &[i16], param_types: &[i32]) -> Result<String, PgSqliteError> {
        let mut result = query.to_string();
        
        // Simple parameter substitution - replace $1, $2, etc. with actual values
        // This is a simplified version - a real implementation would parse the SQL
        for (i, value) in values.iter().enumerate() {
            let param = format!("${}", i + 1);
            let format = formats.get(i).copied().unwrap_or(0); // Default to text format
            let param_type = param_types.get(i).copied().unwrap_or(25); // Default to text
            
            let replacement = match value {
                None => "NULL".to_string(),
                Some(bytes) => {
                    if format == 1 {
                        // Binary format - decode based on expected type
                        match param_type {
                            23 => {
                                // int4
                                if bytes.len() == 4 {
                                    let value = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                                    info!("Decoded binary int32 parameter {}: {}", i + 1, value);
                                    value.to_string()
                                } else {
                                    format!("X'{}'", hex::encode(bytes))
                                }
                            }
                            20 => {
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
                            790 => {
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
                            1700 => {
                                // numeric - decode binary format
                                match DecimalHandler::decode_numeric(bytes) {
                                    Ok(decimal) => {
                                        let s = decimal.to_string();
                                        info!("Decoded binary numeric parameter {}: {}", i + 1, s);
                                        format!("'{}'", s.replace('\'', "''"))
                                    }
                                    Err(e) => {
                                        error!("Failed to decode binary NUMERIC parameter: {}", e);
                                        return Err(PgSqliteError::InvalidParameter(format!("Invalid binary NUMERIC: {}", e)));
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
                                    23 | 20 | 21 | 700 | 701 => {
                                        // Integer and float types - use as-is if valid number
                                        if s.parse::<i64>().is_ok() || s.parse::<f64>().is_ok() {
                                            s
                                        } else {
                                            format!("'{}'", s.replace('\'', "''"))
                                        }
                                    }
                                    790 => {
                                        // MONEY type - always quote
                                        format!("'{}'", s.replace('\'', "''"))
                                    }
                                    1700 => {
                                        // NUMERIC type - validate and quote
                                        match DecimalHandler::validate_numeric_string(&s) {
                                            Ok(_) => {
                                                // Valid numeric value - quote it for SQLite TEXT storage
                                                format!("'{}'", s.replace('\'', "''"))
                                            }
                                            Err(e) => {
                                                error!("Invalid NUMERIC parameter: {}", e);
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
        let cast_regex = regex::Regex::new(r"::\w+").unwrap();
        result = cast_regex.replace_all(&result, "").to_string();
        
        Ok(result)
    }
    
    // PostgreSQL epoch is 2000-01-01 00:00:00
    const _PG_EPOCH: i64 = 946684800; // Unix timestamp for 2000-01-01
    
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
                23 => {
                    // int4
                    if let Ok(val) = lower_str.parse::<i32>() {
                        let mut buf = vec![0u8; 4];
                        BigEndian::write_i32(&mut buf, val);
                        buf
                    } else {
                        return None;
                    }
                }
                20 => {
                    // int8
                    if let Ok(val) = lower_str.parse::<i64>() {
                        let mut buf = vec![0u8; 8];
                        BigEndian::write_i64(&mut buf, val);
                        buf
                    } else {
                        return None;
                    }
                }
                1700 => {
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
                23 => {
                    // int4
                    if let Ok(val) = upper_str.parse::<i32>() {
                        let mut buf = vec![0u8; 4];
                        BigEndian::write_i32(&mut buf, val);
                        buf
                    } else {
                        return None;
                    }
                }
                20 => {
                    // int8
                    if let Ok(val) = upper_str.parse::<i64>() {
                        let mut buf = vec![0u8; 8];
                        BigEndian::write_i64(&mut buf, val);
                        buf
                    } else {
                        return None;
                    }
                }
                1700 => {
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
            let type_oid = field_types.get(i).copied().unwrap_or(25);
            
            let encoded_value = match value {
                None => None,
                Some(bytes) => {
                    if format == 1 {
                        // Binary format requested
                        match type_oid {
                            16 => {
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
                            23 => {
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
                            20 => {
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
                            700 => {
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
                            701 => {
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
                            2950 => {
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
                            1082 => {
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
                            1083 => {
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
                            1114 | 1184 => {
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
                            1700 => {
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
                            790 => {
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
                            650 | 869 => {
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
                            829 => {
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
                            774 => {
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
                            1560 | 1562 => {
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
                            3904 => {
                                // int4range
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Some(range_bytes) = Self::encode_range(&s, 23) {
                                        Some(range_bytes)
                                    } else {
                                        // If parsing fails, keep as text
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            3926 => {
                                // int8range
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Some(range_bytes) = Self::encode_range(&s, 20) {
                                        Some(range_bytes)
                                    } else {
                                        // If parsing fails, keep as text
                                        Some(bytes.clone())
                                    }
                                } else {
                                    Some(bytes.clone())
                                }
                            }
                            3906 => {
                                // numrange
                                if let Ok(s) = String::from_utf8(bytes.clone()) {
                                    if let Some(range_bytes) = Self::encode_range(&s, 1700) {
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
                            25 | 1043 | 1042 => {
                                // text/varchar/char - UTF-8 encoded text
                                Some(bytes.clone())
                            }
                            // JSON types
                            114 => {
                                // json - UTF-8 encoded JSON text
                                Some(bytes.clone())
                            }
                            3802 => {
                                // jsonb - version byte (1) + UTF-8 encoded JSON text
                                let mut result = vec![1u8]; // Version 1
                                result.extend_from_slice(&bytes);
                                Some(result)
                            }
                            // Bytea - already binary
                            17 => {
                                // bytea - raw bytes
                                Some(bytes.clone())
                            }
                            // Small integers
                            21 => {
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
                        // Text format - keep as-is
                        Some(bytes.clone())
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
        info!("Checking if query is catalog query: {}", query);
        let response = if let Some(catalog_result) = CatalogInterceptor::intercept_query(query) {
            info!("Query intercepted by catalog handler");
            catalog_result?
        } else {
            info!("Query not intercepted, executing normally");
            db.query(query).await?
        };
        
        // Check if we need to send RowDescription
        // We send it if:
        // 1. The prepared statement had no field descriptions (wasn't Described or Describe sent NoData)
        // 2. This is a catalog query (which always needs fresh field info)
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
            
            // Try to infer field types from data
            let field_types = response.columns.iter()
                .enumerate()
                .map(|(i, col_name)| {
                    // First priority: Check schema table for stored type mappings
                    if let Some(pg_type) = schema_types.get(col_name) {
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
                            error!("MISSING METADATA: Column '{}' in table '{}' not found in __pgsqlite_schema. This indicates the table was not created through PostgreSQL protocol.", col_name, table);
                            error!("Tables must be created using PostgreSQL CREATE TABLE syntax to ensure proper type metadata.");
                            error!("Falling back to type inference, but this may cause type compatibility issues.");
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
            
            let fields: Vec<FieldDescription> = response.columns.iter()
                .enumerate()
                .map(|(i, col_name)| FieldDescription {
                    name: col_name.clone(),
                    table_oid: 0,
                    column_id: (i + 1) as i16,
                    type_oid: *field_types.get(i).unwrap_or(&25),
                    type_size: -1,
                    type_modifier: -1,
                    format: 0,
                })
                .collect();
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
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        // Check for RETURNING clause
        if ReturningTranslator::has_returning_clause(query) {
            return Self::execute_dml_with_returning(framed, db, query).await;
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
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
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
                    type_oid: 25, // Default to text
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
                        type_oid: 25,
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
        // Handle CREATE TABLE translation
        let translated_query = if query.trim_start().to_uppercase().starts_with("CREATE TABLE") {
            let (sqlite_sql, type_mappings) = crate::translator::CreateTableTranslator::translate(query)
                .map_err(|e| PgSqliteError::Protocol(e))?;
            
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
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
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
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        db.execute(query).await?;
        
        framed.send(BackendMessage::CommandComplete { tag: "OK".to_string() }).await
            .map_err(|e| PgSqliteError::Io(e))?;
        
        Ok(())
    }
    
    /// Analyze INSERT query to determine parameter types from schema
    async fn analyze_insert_params(query: &str, db: &DbHandler) -> Result<Vec<i32>, PgSqliteError> {
        // Use QueryContextAnalyzer to extract table and column info
        let (table_name, columns) = crate::types::QueryContextAnalyzer::get_insert_column_info(query)
            .ok_or_else(|| PgSqliteError::Protocol("Failed to parse INSERT query".to_string()))?;
        
        info!("Analyzing INSERT for table '{}' with columns: {:?}", table_name, columns);
        
        // If no explicit columns, we need to get all columns from the table
        let columns = if columns.is_empty() {
            // Get all columns from the table schema
            let schema_query = format!("PRAGMA table_info({})", table_name);
            match db.query(&schema_query).await {
                Ok(response) => {
                    response.rows.iter()
                        .filter_map(|row| {
                            row.get(1)?.as_ref().and_then(|name_bytes| {
                                String::from_utf8(name_bytes.clone()).ok()
                            })
                        })
                        .collect()
                }
                Err(_) => return Err(PgSqliteError::Protocol("Failed to get table schema".to_string())),
            }
        } else {
            columns
        };
        
        // Look up types for each column
        let mut param_types = Vec::new();
        for column in &columns {
            // First check metadata table for stored PostgreSQL types
            if let Ok(Some(pg_type)) = db.get_schema_type(&table_name, column).await {
                let oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(&pg_type);
                
                // For certain PostgreSQL types that tokio-postgres doesn't support in binary format,
                // use TEXT as the parameter type to allow string representation
                let param_oid = match oid {
                    774 => 25, // MACADDR8 -> TEXT
                    829 => 25, // MACADDR -> TEXT  
                    869 => 25, // INET -> TEXT
                    650 => 25, // CIDR -> TEXT
                    790 => 25, // MONEY -> TEXT
                    3904 => 25, // INT4RANGE -> TEXT
                    3926 => 25, // INT8RANGE -> TEXT
                    3906 => 25, // NUMRANGE -> TEXT
                    1560 => 25, // BIT -> TEXT
                    1562 => 25, // VARBIT -> TEXT
                    _ => oid, // Use original OID for supported types
                };
                
                param_types.push(param_oid);
                if param_oid != oid {
                    info!("Mapped parameter type for {}.{}: {} (OID {}) -> TEXT (OID 25) for binary protocol compatibility", table_name, column, pg_type, oid);
                } else {
                    info!("Found stored type for {}.{}: {} (OID {})", table_name, column, pg_type, oid);
                }
            } else {
                // Fall back to SQLite schema
                let schema_query = format!("PRAGMA table_info({})", table_name);
                if let Ok(response) = db.query(&schema_query).await {
                    let mut found = false;
                    for row in &response.rows {
                        if let (Some(Some(name_bytes)), Some(Some(type_bytes))) = (row.get(1), row.get(2)) {
                            if let (Ok(col_name), Ok(sqlite_type)) = (
                                String::from_utf8(name_bytes.clone()),
                                String::from_utf8(type_bytes.clone())
                            ) {
                                if col_name.to_lowercase() == column.to_lowercase() {
                                    let pg_type = crate::types::SchemaTypeMapper::sqlite_type_to_pg_oid(&sqlite_type);
                                    param_types.push(pg_type);
                                    info!("Mapped SQLite type for {}.{}: {} -> PG OID {}", 
                                          table_name, column, sqlite_type, pg_type);
                                    found = true;
                                    break;
                                }
                            }
                        }
                    }
                    if !found {
                        // Default to text if column not found
                        param_types.push(25);
                        info!("Column {}.{} not found, defaulting to text", table_name, column);
                    }
                } else {
                    // Default to text if we can't query schema
                    param_types.push(25);
                }
            }
        }
        
        Ok(param_types)
    }
    
    /// Convert PostgreSQL type name to OID
    fn pg_type_name_to_oid(type_name: &str) -> i32 {
        match type_name.to_lowercase().as_str() {
            "bool" | "boolean" => 16,
            "bytea" => 17,
            "char" => 18,
            "name" => 19,
            "int8" | "bigint" => 20,
            "int2" | "smallint" => 21,
            "int4" | "integer" | "int" => 23,
            "text" => 25,
            "oid" => 26,
            "float4" | "real" => 700,
            "float8" | "double" | "double precision" => 701,
            "varchar" | "character varying" => 1043,
            "date" => 1082,
            "time" => 1083,
            "timestamp" => 1114,
            "timestamptz" | "timestamp with time zone" => 1184,
            "interval" => 1186,
            "numeric" | "decimal" => 1700,
            "uuid" => 2950,
            "json" => 114,
            "jsonb" => 3802,
            "money" => 790,
            "int4range" => 3904,
            "int8range" => 3926,
            "numrange" => 3906,
            "cidr" => 650,
            "inet" => 869,
            "macaddr" => 829,
            "macaddr8" => 774,
            "bit" => 1560,
            "varbit" | "bit varying" => 1562,
            _ => {
                info!("Unknown PostgreSQL type '{}', defaulting to text", type_name);
                25 // Default to text
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
        
        // Find the SELECT clause
        let query_upper = query.to_uppercase();
        if let Some(select_pos) = query_upper.find("SELECT") {
            let after_select = &query[select_pos + 6..];
            
            // Find the FROM clause to know where SELECT list ends
            let from_pos = after_select.to_uppercase().find(" FROM ")
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
            "text" => 25,
            "int4" | "int" | "integer" => 23,
            "int8" | "bigint" => 20,
            "int2" | "smallint" => 21,
            "float4" | "real" => 700,
            "float8" | "double precision" => 701,
            "bool" | "boolean" => 16,
            "bytea" => 17,
            "char" => 18,
            "varchar" => 1043,
            "date" => 1082,
            "time" => 1083,
            "timestamp" => 1114,
            "timestamptz" => 1184,
            "numeric" | "decimal" => 1700,
            "json" => 114,
            "jsonb" => 3802,
            "uuid" => 2950,
            "money" => 790,
            "int4range" => 3904,
            "int8range" => 3926,
            "numrange" => 3906,
            "cidr" => 650,
            "inet" => 869,
            "macaddr" => 829,
            "macaddr8" => 774,
            "bit" => 1560,
            "varbit" | "bit varying" => 1562,
            _ => 25, // Default to text for unknown types
        }
    }
}


/// Extract table name from SELECT query
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