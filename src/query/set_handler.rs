use crate::protocol::BackendMessage;
use crate::session::SessionState;
use std::sync::Arc;
use crate::PgSqliteError;
use tokio_util::codec::Framed;
use futures::SinkExt;
use regex::Regex;
use once_cell::sync::Lazy;
use tracing::{debug, info};

static SET_TIMEZONE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*SET\s+TIME\s*ZONE\s+(.+)$").unwrap()
});

static SET_PARAMETER_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*SET\s+(\w+)\s+(?:TO|=)\s+(.+)$").unwrap()
});

static SHOW_PARAMETER_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*SHOW\s+(.+?)\s*$").unwrap()
});

pub struct SetHandler;

impl SetHandler {
    /// Check if this is a SET command that we need to handle
    pub fn is_set_command(query: &str) -> bool {
        let trimmed = query.trim();
        let upper = trimmed.to_uppercase();
        upper.starts_with("SET ") || upper.starts_with("SHOW ")
    }

    /// Handle SET and SHOW commands
    pub async fn handle_set_command<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        session: &Arc<SessionState>,
        query: &str,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        Self::handle_set_command_with_options(framed, session, query, false).await
    }
    
    /// Handle SET and SHOW commands with extended protocol support
    pub async fn handle_set_command_extended<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        session: &Arc<SessionState>,
        query: &str,
        skip_row_description: bool,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        Self::handle_set_command_with_options(framed, session, query, skip_row_description).await
    }
    
    async fn handle_set_command_with_options<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        session: &Arc<SessionState>,
        query: &str,
        skip_row_description: bool,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let trimmed = query.trim();
        debug!("Handling SET command: {}", trimmed);
        
        // Handle SET TIME ZONE
        if let Some(caps) = SET_TIMEZONE_PATTERN.captures(trimmed) {
            let timezone = caps[1].trim().trim_matches('\'').trim_matches('"');
            info!("Setting timezone to: {}", timezone);
            Self::set_timezone(session, timezone).await?;
            
            framed.send(BackendMessage::CommandComplete { 
                tag: "SET".to_string() 
            }).await.map_err(PgSqliteError::Io)?;
            
            return Ok(());
        }
        
        // Handle general SET parameter
        if let Some(caps) = SET_PARAMETER_PATTERN.captures(trimmed) {
            let param_name = caps[1].to_uppercase();
            let param_value = caps[2].trim().trim_matches('\'').trim_matches('"');
            
            // Update session parameter
            let mut params = session.parameters.write().await;
            params.insert(param_name.clone(), param_value.to_string());
            drop(params);
            
            framed.send(BackendMessage::CommandComplete { 
                tag: "SET".to_string() 
            }).await.map_err(PgSqliteError::Io)?;
            
            return Ok(());
        }
        
        // Handle SHOW parameter
        if let Some(caps) = SHOW_PARAMETER_PATTERN.captures(trimmed) {
            let param_name = caps[1].to_uppercase();
            info!("SHOW parameter: {}", param_name);
            
            // Handle special PostgreSQL SHOW commands
            let value = match param_name.as_str() {
                "TRANSACTION ISOLATION LEVEL" => "read committed".to_string(),
                "DEFAULT_TRANSACTION_ISOLATION" => "read committed".to_string(), 
                "TRANSACTION_ISOLATION" => "read committed".to_string(),
                "SERVER_VERSION" => "15.0".to_string(),
                "SERVER_VERSION_NUM" => "150000".to_string(),
                "IS_SUPERUSER" => "on".to_string(),
                "SESSION_AUTHORIZATION" => "postgres".to_string(),
                "STANDARD_CONFORMING_STRINGS" => "on".to_string(),
                "CLIENT_ENCODING" => "UTF8".to_string(),
                "SERVER_ENCODING" => "UTF8".to_string(),
                _ => {
                    // Fall back to session parameters
                    let params = session.parameters.read().await;
                    params.get(&param_name)
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "unset".to_string())
                }
            };
            info!("Parameter {} = {}", param_name, value);
            
            // Send row description only if not in extended protocol with pre-described statement
            if !skip_row_description {
                let field = crate::protocol::FieldDescription {
                    name: param_name.to_lowercase(),
                    table_oid: 0,
                    column_id: 1,
                    type_oid: crate::types::PgType::Text.to_oid(),
                    type_size: -1,
                    type_modifier: -1,
                    format: 0,
                };
                
                framed.send(BackendMessage::RowDescription(vec![field])).await
                    .map_err(PgSqliteError::Io)?;
            }
            
            // Send data row
            let row = vec![Some(value.as_bytes().to_vec())];
            framed.send(BackendMessage::DataRow(row)).await
                .map_err(PgSqliteError::Io)?;
            
            framed.send(BackendMessage::CommandComplete { 
                tag: "SHOW".to_string() 
            }).await.map_err(PgSqliteError::Io)?;
            
            return Ok(());
        }
        
        Err(PgSqliteError::Protocol(format!("Unrecognized SET command: {query}")))
    }
    
    /// Set the session timezone
    async fn set_timezone(session: &Arc<SessionState>, timezone: &str) -> Result<(), PgSqliteError> {
        // Validate timezone (basic validation)
        let valid_timezone = match timezone.to_uppercase().as_str() {
            "UTC" | "GMT" => "UTC",
            "EST" => "America/New_York",
            "PST" => "America/Los_Angeles",
            "CST" => "America/Chicago",
            "MST" => "America/Denver",
            _ => {
                // Check if it's a numeric offset like '+05:30' or '-08:00'
                if Self::is_valid_offset(timezone) {
                    timezone
                } else {
                    // For now, default to the provided value
                    // In a full implementation, we'd validate against a timezone database
                    timezone
                }
            }
        };
        
        let mut params = session.parameters.write().await;
        params.insert("TIMEZONE".to_string(), valid_timezone.to_string());
        
        Ok(())
    }
    
    /// Check if a string is a valid timezone offset
    fn is_valid_offset(offset: &str) -> bool {
        let offset_pattern = Regex::new(r"^[+-]\d{2}:\d{2}$").unwrap();
        offset_pattern.is_match(offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_is_set_command() {
        assert!(SetHandler::is_set_command("SET TIME ZONE 'UTC'"));
        assert!(SetHandler::is_set_command("set time zone 'UTC'"));
        assert!(SetHandler::is_set_command("SET search_path TO public"));
        assert!(SetHandler::is_set_command("SHOW TimeZone"));
        assert!(SetHandler::is_set_command("show timezone"));
        
        assert!(!SetHandler::is_set_command("SELECT * FROM users"));
        assert!(!SetHandler::is_set_command("INSERT INTO test VALUES (1)"));
    }
    
    #[test]
    fn test_set_timezone_pattern() {
        let query = "SET TIME ZONE 'America/New_York'";
        assert!(SET_TIMEZONE_PATTERN.is_match(query));
        
        let query = "set time zone UTC";
        assert!(SET_TIMEZONE_PATTERN.is_match(query));
        
        let query = "SET TIME ZONE '+05:30'";
        assert!(SET_TIMEZONE_PATTERN.is_match(query));
    }
    
    #[test]
    fn test_show_parameter_pattern() {
        let query = "SHOW TimeZone";
        assert!(SHOW_PARAMETER_PATTERN.is_match(query));
        
        let query = "show search_path";
        assert!(SHOW_PARAMETER_PATTERN.is_match(query));
    }
}