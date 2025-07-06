use tokio_util::codec::{Decoder, Encoder};
use bytes::{BytesMut, BufMut, Buf};
use std::io;
use std::collections::HashMap;
use super::messages::*;
use tracing::debug;

#[derive(Clone)]
pub struct PostgresCodec {
    state: CodecState,
}

#[derive(Debug, Clone)]
enum CodecState {
    WaitingForStartup,
    Normal,
}

impl PostgresCodec {
    pub fn new() -> Self {
        PostgresCodec {
            state: CodecState::WaitingForStartup,
        }
    }
}

impl Default for PostgresCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for PostgresCodec {
    type Item = FrontendMessage;
    type Error = io::Error;
    
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.state {
            CodecState::WaitingForStartup => {
                if let Some(msg) = decode_startup_message(src)? {
                    self.state = CodecState::Normal;
                    Ok(Some(msg))
                } else {
                    Ok(None)
                }
            }
            CodecState::Normal => decode_normal_message(src),
        }
    }
}

impl Encoder<BackendMessage> for PostgresCodec {
    type Error = io::Error;
    
    fn encode(&mut self, msg: BackendMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        debug!("Encoding message: {:?}", msg);
        match msg {
            BackendMessage::Authentication(auth) => encode_authentication(auth, dst),
            BackendMessage::ParameterStatus { name, value } => encode_parameter_status(&name, &value, dst),
            BackendMessage::BackendKeyData { process_id, secret_key } => encode_backend_key_data(process_id, secret_key, dst),
            BackendMessage::ReadyForQuery { status } => encode_ready_for_query(status, dst),
            BackendMessage::RowDescription(fields) => encode_row_description(fields, dst),
            BackendMessage::DataRow(values) => encode_data_row(values, dst),
            BackendMessage::CommandComplete { tag } => encode_command_complete(&tag, dst),
            BackendMessage::EmptyQueryResponse => encode_empty_query_response(dst),
            BackendMessage::ErrorResponse(err) => encode_error_response(err, dst),
            BackendMessage::NoticeResponse(notice) => encode_notice_response(notice, dst),
            BackendMessage::ParseComplete => encode_parse_complete(dst),
            BackendMessage::BindComplete => encode_bind_complete(dst),
            BackendMessage::CloseComplete => encode_close_complete(dst),
            BackendMessage::PortalSuspended => encode_portal_suspended(dst),
            BackendMessage::NoData => encode_no_data(dst),
            BackendMessage::ParameterDescription(oids) => encode_parameter_description(oids, dst),
        }
        Ok(())
    }
}

fn decode_startup_message(src: &mut BytesMut) -> io::Result<Option<FrontendMessage>> {
    if src.len() < 4 {
        return Ok(None);
    }
    
    let len = (&src[0..4]).get_i32() as usize;
    
    if src.len() < len {
        return Ok(None);
    }
    
    let msg_bytes = src.split_to(len);
    let mut msg_buf = &msg_bytes[4..]; // Skip length
    
    let protocol_version = msg_buf.get_i32();
    
    // Check for SSL request (protocol version 80877103)
    if protocol_version == 80877103 {
        return Ok(Some(FrontendMessage::SslRequest));
    }
    
    let mut parameters = HashMap::new();
    
    // Read parameter pairs until we hit null terminator
    while msg_buf.has_remaining() && msg_buf[0] != 0 {
        let key = read_cstring(&mut msg_buf)?;
        let value = read_cstring(&mut msg_buf)?;
        parameters.insert(key, value);
    }
    
    Ok(Some(FrontendMessage::StartupMessage(StartupMessage {
        protocol_version,
        parameters,
    })))
}

fn decode_normal_message(src: &mut BytesMut) -> io::Result<Option<FrontendMessage>> {
    if src.len() < 5 {
        return Ok(None);
    }
    
    let msg_type = src[0];
    let len = (&src[1..5]).get_i32() as usize;
    
    if src.len() < len + 1 {
        return Ok(None);
    }
    
    let msg_bytes = src.split_to(len + 1);
    let mut msg_buf = &msg_bytes[5..]; // Skip type and length
    
    match msg_type {
        b'Q' => {
            let query = read_cstring(&mut msg_buf)?;
            Ok(Some(FrontendMessage::Query(query)))
        }
        b'P' => {
            let name = read_cstring(&mut msg_buf)?;
            let query = read_cstring(&mut msg_buf)?;
            let param_count = msg_buf.get_i16();
            let mut param_types = Vec::new();
            for _ in 0..param_count {
                param_types.push(msg_buf.get_i32());
            }
            Ok(Some(FrontendMessage::Parse { name, query, param_types }))
        }
        b'B' => {
            let portal = read_cstring(&mut msg_buf)?;
            let statement = read_cstring(&mut msg_buf)?;
            
            let format_count = msg_buf.get_i16();
            let mut formats = Vec::new();
            for _ in 0..format_count {
                formats.push(msg_buf.get_i16());
            }
            
            let value_count = msg_buf.get_i16();
            let mut values = Vec::new();
            for _ in 0..value_count {
                let len = msg_buf.get_i32();
                if len == -1 {
                    values.push(None);
                } else {
                    let mut value = vec![0u8; len as usize];
                    msg_buf.copy_to_slice(&mut value);
                    values.push(Some(value));
                }
            }
            
            let result_format_count = msg_buf.get_i16();
            let mut result_formats = Vec::new();
            for _ in 0..result_format_count {
                result_formats.push(msg_buf.get_i16());
            }
            
            Ok(Some(FrontendMessage::Bind {
                portal,
                statement,
                formats,
                values,
                result_formats,
            }))
        }
        b'E' => {
            let portal = read_cstring(&mut msg_buf)?;
            let max_rows = msg_buf.get_i32();
            Ok(Some(FrontendMessage::Execute { portal, max_rows }))
        }
        b'S' => Ok(Some(FrontendMessage::Sync)),
        b'X' => Ok(Some(FrontendMessage::Terminate)),
        b'C' => {
            let typ = msg_buf.get_u8();
            let name = read_cstring(&mut msg_buf)?;
            Ok(Some(FrontendMessage::Close { typ, name }))
        }
        b'D' => {
            let typ = msg_buf.get_u8();
            let name = read_cstring(&mut msg_buf)?;
            Ok(Some(FrontendMessage::Describe { typ, name }))
        }
        b'H' => Ok(Some(FrontendMessage::Flush)),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Unknown message type: {}", msg_type as char),
        )),
    }
}

fn encode_authentication(auth: AuthenticationMessage, dst: &mut BytesMut) {
    dst.put_u8(b'R');
    let len_pos = dst.len();
    dst.put_i32(0); // Placeholder for length
    
    match auth {
        AuthenticationMessage::Ok => dst.put_i32(0),
        AuthenticationMessage::CleartextPassword => dst.put_i32(3),
        AuthenticationMessage::MD5Password { salt } => {
            dst.put_i32(5);
            dst.put_slice(&salt);
        }
    }
    
    update_message_length(dst, len_pos);
}

fn encode_parameter_status(name: &str, value: &str, dst: &mut BytesMut) {
    dst.put_u8(b'S');
    let len_pos = dst.len();
    dst.put_i32(0); // Placeholder
    
    put_cstring(dst, name);
    put_cstring(dst, value);
    
    update_message_length(dst, len_pos);
}

fn encode_backend_key_data(process_id: i32, secret_key: i32, dst: &mut BytesMut) {
    dst.put_u8(b'K');
    dst.put_i32(12); // Fixed length
    dst.put_i32(process_id);
    dst.put_i32(secret_key);
}

fn encode_ready_for_query(status: TransactionStatus, dst: &mut BytesMut) {
    dst.put_u8(b'Z');
    dst.put_i32(5); // Fixed length
    dst.put_u8(status.as_byte());
}

fn encode_row_description(fields: Vec<FieldDescription>, dst: &mut BytesMut) {
    dst.put_u8(b'T');
    let len_pos = dst.len();
    dst.put_i32(0); // Placeholder
    
    dst.put_i16(fields.len() as i16);
    
    for field in fields {
        put_cstring(dst, &field.name);
        dst.put_i32(field.table_oid);
        dst.put_i16(field.column_id);
        dst.put_i32(field.type_oid);
        dst.put_i16(field.type_size);
        dst.put_i32(field.type_modifier);
        dst.put_i16(field.format);
    }
    
    update_message_length(dst, len_pos);
}

fn encode_data_row(values: Vec<Option<Vec<u8>>>, dst: &mut BytesMut) {
    dst.put_u8(b'D');
    let len_pos = dst.len();
    dst.put_i32(0); // Placeholder
    
    dst.put_i16(values.len() as i16);
    
    for value in values {
        match value {
            None => dst.put_i32(-1),
            Some(data) => {
                dst.put_i32(data.len() as i32);
                dst.put_slice(&data);
            }
        }
    }
    
    update_message_length(dst, len_pos);
}

fn encode_command_complete(tag: &str, dst: &mut BytesMut) {
    dst.put_u8(b'C');
    let len_pos = dst.len();
    dst.put_i32(0); // Placeholder
    
    put_cstring(dst, tag);
    
    update_message_length(dst, len_pos);
}

fn encode_empty_query_response(dst: &mut BytesMut) {
    dst.put_u8(b'I');
    dst.put_i32(4); // Fixed length
}

fn encode_error_response(err: ErrorResponse, dst: &mut BytesMut) {
    dst.put_u8(b'E');
    let len_pos = dst.len();
    dst.put_i32(0); // Placeholder
    
    // Required fields
    dst.put_u8(b'S');
    put_cstring(dst, &err.severity);
    
    dst.put_u8(b'C');
    put_cstring(dst, &err.code);
    
    dst.put_u8(b'M');
    put_cstring(dst, &err.message);
    
    // Optional fields
    if let Some(ref detail) = err.detail {
        dst.put_u8(b'D');
        put_cstring(dst, detail);
    }
    
    if let Some(ref hint) = err.hint {
        dst.put_u8(b'H');
        put_cstring(dst, hint);
    }
    
    if let Some(position) = err.position {
        dst.put_u8(b'P');
        put_cstring(dst, &position.to_string());
    }
    
    // Null terminator
    dst.put_u8(0);
    
    update_message_length(dst, len_pos);
}

fn encode_notice_response(notice: NoticeResponse, dst: &mut BytesMut) {
    dst.put_u8(b'N');
    let len_pos = dst.len();
    dst.put_i32(0); // Placeholder
    
    dst.put_u8(b'S');
    put_cstring(dst, &notice.severity);
    
    dst.put_u8(b'C');
    put_cstring(dst, &notice.code);
    
    dst.put_u8(b'M');
    put_cstring(dst, &notice.message);
    
    if let Some(ref detail) = notice.detail {
        dst.put_u8(b'D');
        put_cstring(dst, detail);
    }
    
    if let Some(ref hint) = notice.hint {
        dst.put_u8(b'H');
        put_cstring(dst, hint);
    }
    
    dst.put_u8(0);
    
    update_message_length(dst, len_pos);
}

fn encode_parse_complete(dst: &mut BytesMut) {
    dst.put_u8(b'1');
    dst.put_i32(4); // Fixed length
}

fn encode_bind_complete(dst: &mut BytesMut) {
    dst.put_u8(b'2');
    dst.put_i32(4); // Fixed length
}

fn encode_close_complete(dst: &mut BytesMut) {
    dst.put_u8(b'3');
    dst.put_i32(4); // Fixed length
}

fn encode_portal_suspended(dst: &mut BytesMut) {
    dst.put_u8(b's');
    dst.put_i32(4); // Fixed length
}

fn encode_no_data(dst: &mut BytesMut) {
    dst.put_u8(b'n');
    dst.put_i32(4); // Fixed length
}

fn encode_parameter_description(oids: Vec<i32>, dst: &mut BytesMut) {
    dst.put_u8(b't');
    let len_pos = dst.len();
    dst.put_i32(0); // Placeholder
    
    dst.put_i16(oids.len() as i16);
    for oid in oids {
        dst.put_i32(oid);
    }
    
    update_message_length(dst, len_pos);
}

// Helper functions
fn read_cstring(buf: &mut &[u8]) -> io::Result<String> {
    let null_pos = buf.iter().position(|&b| b == 0)
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "Missing null terminator"))?;
    
    let string = String::from_utf8(buf[..null_pos].to_vec())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    
    *buf = &buf[null_pos + 1..];
    Ok(string)
}

fn put_cstring(dst: &mut BytesMut, s: &str) {
    dst.put_slice(s.as_bytes());
    dst.put_u8(0);
}

fn update_message_length(dst: &mut BytesMut, len_pos: usize) {
    let len = (dst.len() - len_pos) as i32;
    dst[len_pos..len_pos+4].copy_from_slice(&len.to_be_bytes());
}