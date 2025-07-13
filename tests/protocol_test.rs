use pgsqlite::protocol::{
    PostgresCodec, FrontendMessage, BackendMessage, 
    AuthenticationMessage, TransactionStatus,
    ErrorResponse, FieldDescription
};
use tokio_util::codec::{Encoder, Decoder};
use bytes::{BytesMut, BufMut};

#[test]
fn test_encode_authentication() {
    let mut codec = PostgresCodec::new();
    let mut buf = BytesMut::new();
    
    let msg = BackendMessage::Authentication(AuthenticationMessage::Ok);
    codec.encode(msg, &mut buf).unwrap();
    
    // Check message format: 'R' + length(4) + auth_type(4)
    assert_eq!(buf[0], b'R');
    assert_eq!(&buf[1..5], &8i32.to_be_bytes()); // Length = 8
    assert_eq!(&buf[5..9], &0i32.to_be_bytes()); // Auth OK = 0
}

#[test]
fn test_encode_ready_for_query() {
    let mut codec = PostgresCodec::new();
    let mut buf = BytesMut::new();
    
    let msg = BackendMessage::ReadyForQuery { status: TransactionStatus::Idle };
    codec.encode(msg, &mut buf).unwrap();
    
    assert_eq!(buf[0], b'Z');
    assert_eq!(&buf[1..5], &5i32.to_be_bytes()); // Fixed length
    assert_eq!(buf[5], b'I'); // Idle status
}

#[test]
fn test_encode_error_response() {
    let mut codec = PostgresCodec::new();
    let mut buf = BytesMut::new();
    
    let err = ErrorResponse::new(
        "ERROR".to_string(),
        "42P01".to_string(),
        "relation \"users\" does not exist".to_string(),
    );
    
    let msg = BackendMessage::ErrorResponse(Box::new(err));
    codec.encode(msg, &mut buf).unwrap();
    
    assert_eq!(buf[0], b'E');
    // Should contain severity, code, and message fields
    assert!(buf.len() > 20);
}

#[test]
fn test_decode_startup_message() {
    let mut codec = PostgresCodec::new();
    let mut buf = BytesMut::new();
    
    // Create a startup message
    // Length includes: 4 (length) + 4 (version) + "user\0postgres\0\0" (15 bytes)
    let len = 23i32; // Total message length including length field
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&196608i32.to_be_bytes()); // Protocol version 3.0
    buf.extend_from_slice(b"user\0postgres\0\0");
    
    let msg = codec.decode(&mut buf).unwrap();
    
    match msg {
        Some(FrontendMessage::StartupMessage(startup)) => {
            assert_eq!(startup.protocol_version, 196608);
            assert_eq!(startup.parameters.get("user"), Some(&"postgres".to_string()));
        }
        None => panic!("No message decoded"),
        Some(other) => panic!("Expected StartupMessage, got {:?}", other),
    }
}

#[test]
fn test_decode_query() {
    let mut codec = PostgresCodec::new();
    let mut buf = BytesMut::new();
    
    // First decode a startup message to change state
    let startup_len = 9i32; // 4 (len) + 4 (version) + 1 (null terminator)
    buf.extend_from_slice(&startup_len.to_be_bytes());
    buf.extend_from_slice(&196608i32.to_be_bytes());
    buf.extend_from_slice(b"\0");
    
    codec.decode(&mut buf).unwrap();
    
    // Now decode a query
    buf.clear();
    buf.put_u8(b'Q'); // Query message
    buf.extend_from_slice(&13i32.to_be_bytes()); // Length: 4 (len) + 9 ("SELECT 1\0")
    buf.extend_from_slice(b"SELECT 1\0");
    
    let msg = codec.decode(&mut buf).unwrap();
    
    match msg {
        Some(FrontendMessage::Query(query)) => {
            assert_eq!(query, "SELECT 1");
        }
        None => panic!("No message decoded"),
        Some(other) => panic!("Expected Query message, got {:?}", other),
    }
}

#[test]
fn test_row_description_encoding() {
    let mut codec = PostgresCodec::new();
    let mut buf = BytesMut::new();
    
    let fields = vec![
        FieldDescription {
            name: "id".to_string(),
            table_oid: 0,
            column_id: 1,
            type_oid: 23, // int4
            type_size: 4,
            type_modifier: -1,
            format: 0, // text
        },
        FieldDescription {
            name: "name".to_string(),
            table_oid: 0,
            column_id: 2,
            type_oid: 25, // text
            type_size: -1,
            type_modifier: -1,
            format: 0, // text
        },
    ];
    
    let msg = BackendMessage::RowDescription(fields);
    codec.encode(msg, &mut buf).unwrap();
    
    assert_eq!(buf[0], b'T'); // RowDescription type
    // Verify it contains field count and field data
    assert!(buf.len() > 30);
}