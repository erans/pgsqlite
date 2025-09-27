use std::io::{self, Cursor, Read};
use std::collections::HashMap;
use byteorder::{BigEndian, ReadBytesExt};
use thiserror::Error;
use tracing::warn;

use crate::protocol::messages::{
    FrontendMessage, StartupMessage, AuthenticationMessage, ErrorResponse
};
use crate::security::events;

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Invalid message type: {0}")]
    InvalidMessageType(u8),
    #[error("Message too large: {0} bytes")]
    MessageTooLarge(u32),
    #[error("Malformed message: {0}")]
    MalformedMessage(String),
    #[error("Protocol violation: {0}")]
    ProtocolViolation(String),
    #[error("Buffer underrun")]
    BufferUnderrun,
    #[error("Invalid encoding")]
    InvalidEncoding,
}

pub struct MessageParser {
    max_message_size: u32,
    max_param_count: usize,
    max_string_length: usize,
}

impl Default for MessageParser {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageParser {
    pub fn new() -> Self {
        Self {
            max_message_size: 16 * 1024 * 1024, // 16MB max message
            max_param_count: 10000,
            max_string_length: 1024 * 1024, // 1MB max string
        }
    }

    pub fn with_limits(max_message_size: u32, max_param_count: usize, max_string_length: usize) -> Self {
        Self {
            max_message_size,
            max_param_count,
            max_string_length,
        }
    }

    pub async fn parse_message(&self, cursor: &mut Cursor<&[u8]>) -> Result<FrontendMessage, ParseError> {
        if cursor.position() >= cursor.get_ref().len() as u64 {
            return Err(ParseError::BufferUnderrun);
        }

        let message_type = cursor.read_u8()?;

        // Handle SSL request separately (no length header)
        if message_type == 0x80 {
            return self.parse_ssl_request(cursor).await;
        }

        let length = cursor.read_u32::<BigEndian>()?;

        // Security check: length must be at least 4 (includes length field itself)
        if length < 4 {
            warn!("Message length too small: {} bytes", length);
            events::protocol_violation(None, &format!("Message length too small: {} bytes", length));
            return Err(ParseError::ProtocolViolation("Message length too small".to_string()));
        }

        // Security check: prevent DoS via huge messages
        if length > self.max_message_size {
            warn!("Message too large: {} bytes, max: {}", length, self.max_message_size);
            events::protocol_violation(None, &format!("Message too large: {} bytes", length));
            return Err(ParseError::MessageTooLarge(length));
        }

        // Ensure we have enough data
        let remaining = cursor.get_ref().len() as u64 - cursor.position();
        if remaining < (length - 4) as u64 { // -4 because length includes the length field
            return Err(ParseError::BufferUnderrun);
        }

        let payload_length = length - 4;

        match message_type {
            b'Q' => self.parse_query(cursor, payload_length).await,
            b'P' => self.parse_parse(cursor, payload_length).await,
            b'B' => self.parse_bind(cursor, payload_length).await,
            b'E' => self.parse_execute(cursor, payload_length).await,
            b'S' => Ok(FrontendMessage::Sync),
            b'X' => Ok(FrontendMessage::Terminate),
            b'C' => self.parse_close(cursor, payload_length).await,
            b'D' => self.parse_describe(cursor, payload_length).await,
            b'H' => Ok(FrontendMessage::Flush),
            _ => {
                events::protocol_violation(None, &format!("Unknown message type: {}", message_type as char));
                Err(ParseError::InvalidMessageType(message_type))
            }
        }
    }

    pub async fn parse_startup_message(&self, cursor: &mut Cursor<&[u8]>) -> Result<StartupMessage, ParseError> {
        let length = cursor.read_u32::<BigEndian>()?;

        if length > self.max_message_size {
            return Err(ParseError::MessageTooLarge(length));
        }

        let major = cursor.read_u16::<BigEndian>()? as i32;
        let minor = cursor.read_u16::<BigEndian>()? as i32;
        let protocol_version = (major << 16) | minor;

        // Validate protocol version
        if major != 3 || minor != 0 {
            events::protocol_violation(None, &format!("Unsupported protocol version: {}.{}", major, minor));
            return Err(ParseError::ProtocolViolation(format!("Unsupported protocol version: {}.{}", major, minor)));
        }

        let mut parameters = HashMap::new();
        let mut param_count = 0;

        // Parse parameter key-value pairs
        loop {
            param_count += 1;
            if param_count > self.max_param_count {
                return Err(ParseError::ProtocolViolation("Too many parameters".to_string()));
            }

            let key = self.read_cstring(cursor).await?;
            if key.is_empty() {
                break; // End of parameters
            }

            let value = self.read_cstring(cursor).await?;

            // Security: validate parameter names and values
            if key.len() > 100 || value.len() > 1000 {
                events::protocol_violation(None, "Parameter name or value too long");
                return Err(ParseError::ProtocolViolation("Parameter too long".to_string()));
            }

            parameters.insert(key, value);
        }

        Ok(StartupMessage {
            protocol_version,
            parameters,
        })
    }

    pub async fn parse_auth_request(&self, cursor: &mut Cursor<&[u8]>) -> Result<AuthenticationRequest, ParseError> {
        let auth_type = cursor.read_u32::<BigEndian>()?;

        match auth_type {
            0 => Ok(AuthenticationRequest::Ok),
            3 => Ok(AuthenticationRequest::Password),
            5 => {
                let mut salt = [0u8; 4];
                cursor.read_exact(&mut salt)?;
                Ok(AuthenticationRequest::MD5Password { salt })
            }
            10 => {
                // SASL authentication
                let mut mechanisms = Vec::new();
                loop {
                    let mechanism = self.read_cstring(cursor).await?;
                    if mechanism.is_empty() {
                        break;
                    }
                    if mechanisms.len() >= 10 {
                        return Err(ParseError::ProtocolViolation("Too many SASL mechanisms".to_string()));
                    }
                    mechanisms.push(mechanism);
                }
                Ok(AuthenticationRequest::SASL { mechanisms })
            }
            11 => {
                // SASL continue
                let remaining = cursor.get_ref().len() - cursor.position() as usize;
                let mut data = vec![0u8; remaining];
                cursor.read_exact(&mut data)?;
                Ok(AuthenticationRequest::SASLContinue { data })
            }
            12 => {
                // SASL final
                let remaining = cursor.get_ref().len() - cursor.position() as usize;
                let mut data = vec![0u8; remaining];
                cursor.read_exact(&mut data)?;
                Ok(AuthenticationRequest::SASLFinal { data })
            }
            _ => Err(ParseError::ProtocolViolation(format!("Unknown auth type: {}", auth_type)))
        }
    }

    pub async fn parse_query(&self, cursor: &mut Cursor<&[u8]>, length: u32) -> Result<FrontendMessage, ParseError> {
        let sql = self.read_cstring_with_length(cursor, length as usize).await?;

        // Security: basic SQL injection detection
        let sql_lower = sql.to_lowercase();
        if sql_lower.contains("drop table") ||
           sql_lower.contains("delete from") ||
           sql_lower.contains("truncate") ||
           sql_lower.contains("' or '1'='1") ||
           sql_lower.contains("union select") {
            events::sql_injection_attempt(None, None, &sql, "Suspicious SQL pattern detected");
        }

        Ok(FrontendMessage::Query(sql))
    }

    async fn parse_ssl_request(&self, cursor: &mut Cursor<&[u8]>) -> Result<FrontendMessage, ParseError> {
        // SSL request has a specific format: 8 bytes total
        let length = cursor.read_u32::<BigEndian>()?;
        if length != 8 {
            return Err(ParseError::ProtocolViolation("Invalid SSL request length".to_string()));
        }

        let ssl_code = cursor.read_u32::<BigEndian>()?;
        if ssl_code != 80877103 { // Magic SSL request code
            return Err(ParseError::ProtocolViolation("Invalid SSL request code".to_string()));
        }

        Ok(FrontendMessage::SslRequest)
    }

    async fn parse_parse(&self, cursor: &mut Cursor<&[u8]>, _length: u32) -> Result<FrontendMessage, ParseError> {
        let statement_name = self.read_cstring(cursor).await?;
        let query = self.read_cstring(cursor).await?;
        let param_count = cursor.read_u16::<BigEndian>()? as usize;

        if param_count > self.max_param_count {
            return Err(ParseError::ProtocolViolation("Too many parameters".to_string()));
        }

        let mut param_types = Vec::with_capacity(param_count);
        for _ in 0..param_count {
            param_types.push(cursor.read_u32::<BigEndian>()? as i32);
        }

        Ok(FrontendMessage::Parse {
            name: statement_name,
            query,
            param_types,
        })
    }

    async fn parse_bind(&self, cursor: &mut Cursor<&[u8]>, _length: u32) -> Result<FrontendMessage, ParseError> {
        let portal_name = self.read_cstring(cursor).await?;
        let statement_name = self.read_cstring(cursor).await?;

        let format_count = cursor.read_u16::<BigEndian>()? as usize;
        if format_count > self.max_param_count {
            return Err(ParseError::ProtocolViolation("Too many format codes".to_string()));
        }

        let mut formats = Vec::with_capacity(format_count);
        for _ in 0..format_count {
            formats.push(cursor.read_i16::<BigEndian>()?);
        }

        let param_count = cursor.read_u16::<BigEndian>()? as usize;
        if param_count > self.max_param_count {
            return Err(ParseError::ProtocolViolation("Too many parameters".to_string()));
        }

        let mut values = Vec::with_capacity(param_count);
        for _ in 0..param_count {
            let value_length = cursor.read_i32::<BigEndian>()?;
            if value_length == -1 {
                values.push(None); // NULL value
            } else {
                if value_length < 0 || value_length as usize > self.max_string_length {
                    return Err(ParseError::ProtocolViolation("Invalid parameter length".to_string()));
                }
                let mut value = vec![0u8; value_length as usize];
                cursor.read_exact(&mut value)?;
                values.push(Some(value));
            }
        }

        let result_format_count = cursor.read_u16::<BigEndian>()? as usize;
        if result_format_count > self.max_param_count {
            return Err(ParseError::ProtocolViolation("Too many result format codes".to_string()));
        }

        let mut result_formats = Vec::with_capacity(result_format_count);
        for _ in 0..result_format_count {
            result_formats.push(cursor.read_i16::<BigEndian>()?);
        }

        Ok(FrontendMessage::Bind {
            portal: portal_name,
            statement: statement_name,
            formats,
            values,
            result_formats,
        })
    }

    async fn parse_execute(&self, cursor: &mut Cursor<&[u8]>, _length: u32) -> Result<FrontendMessage, ParseError> {
        let portal_name = self.read_cstring(cursor).await?;
        let max_rows = cursor.read_i32::<BigEndian>()?;

        Ok(FrontendMessage::Execute {
            portal: portal_name,
            max_rows,
        })
    }

    async fn parse_close(&self, cursor: &mut Cursor<&[u8]>, _length: u32) -> Result<FrontendMessage, ParseError> {
        let close_type = cursor.read_u8()?;
        let name = self.read_cstring(cursor).await?;

        if close_type != b'S' && close_type != b'P' {
            return Err(ParseError::ProtocolViolation("Invalid close type".to_string()));
        }

        Ok(FrontendMessage::Close {
            typ: close_type,
            name,
        })
    }

    async fn parse_describe(&self, cursor: &mut Cursor<&[u8]>, _length: u32) -> Result<FrontendMessage, ParseError> {
        let describe_type = cursor.read_u8()?;
        let name = self.read_cstring(cursor).await?;

        if describe_type != b'S' && describe_type != b'P' {
            return Err(ParseError::ProtocolViolation("Invalid describe type".to_string()));
        }

        Ok(FrontendMessage::Describe {
            typ: describe_type,
            name,
        })
    }

    async fn read_cstring(&self, cursor: &mut Cursor<&[u8]>) -> Result<String, ParseError> {
        let mut bytes = Vec::new();
        loop {
            if bytes.len() > self.max_string_length {
                return Err(ParseError::ProtocolViolation("String too long".to_string()));
            }

            let byte = cursor.read_u8()?;
            if byte == 0 {
                break;
            }
            bytes.push(byte);
        }

        String::from_utf8(bytes).map_err(|_| ParseError::InvalidEncoding)
    }

    async fn read_cstring_with_length(&self, cursor: &mut Cursor<&[u8]>, max_length: usize) -> Result<String, ParseError> {
        let mut bytes = Vec::new();
        let limited_length = max_length.min(self.max_string_length);

        for _ in 0..limited_length {
            let byte = cursor.read_u8()?;
            if byte == 0 {
                break;
            }
            bytes.push(byte);
        }

        String::from_utf8(bytes).map_err(|_| ParseError::InvalidEncoding)
    }
}

// Simplified message types for fuzzing
#[derive(Debug, Clone)]
pub enum PostgresMessage {
    Frontend(FrontendMessage),
    Backend(BackendMessage),
}

#[derive(Debug, Clone)]
pub enum BackendMessage {
    Authentication(AuthenticationMessage),
    ErrorResponse(ErrorResponse),
}

#[derive(Debug, Clone)]
pub struct Query {
    pub sql: String,
}

#[derive(Debug, Clone)]
pub struct Parse {
    pub statement_name: String,
    pub query: String,
    pub parameter_types: Vec<i32>,
}

#[derive(Debug, Clone)]
pub struct Bind {
    pub portal_name: String,
    pub statement_name: String,
    pub parameter_values: Vec<Option<Vec<u8>>>,
}

#[derive(Debug, Clone)]
pub struct Execute {
    pub portal_name: String,
}

#[derive(Debug, Clone)]
pub struct Sync;

#[derive(Debug, Clone)]
pub struct Terminate;

#[derive(Debug, Clone)]
pub enum AuthenticationRequest {
    Ok,
    Password,
    MD5Password { salt: [u8; 4] },
    SASL { mechanisms: Vec<String> },
    SASLContinue { data: Vec<u8> },
    SASLFinal { data: Vec<u8> },
}

#[derive(Debug, Clone)]
pub enum PostgresMessageType {
    Query,
    Parse,
    Bind,
    Execute,
    Sync,
    Terminate,
    Startup,
    Authentication,
    Close,
    Describe,
    Flush,
}

#[derive(Debug, Clone)]
pub enum ProtocolState {
    Startup,
    Authentication,
    Normal,
    Extended,
}