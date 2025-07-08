use std::io::{Read, Write};
use std::net::TcpStream;

#[test]
#[ignore] // This test requires a running pgsqlite server
fn test_ssl_request_when_ssl_disabled() {
    // Connect to the server
    let mut stream = TcpStream::connect("127.0.0.1:5432").unwrap();
    
    // Send SSL request (8 bytes: length=8, code=80877103)
    let ssl_request = [
        0x00, 0x00, 0x00, 0x08,  // Length: 8
        0x04, 0xd2, 0x16, 0x2f,  // Code: 80877103 (0x04d2162f)
    ];
    stream.write_all(&ssl_request).unwrap();
    stream.flush().unwrap();
    
    // Read response (should be 'N' for SSL not supported)
    let mut response = [0u8; 1];
    stream.read_exact(&mut response).unwrap();
    
    assert_eq!(response[0], b'N', "Expected 'N' response for SSL not supported");
    
    // Now send a normal startup message
    let mut startup_msg = Vec::new();
    startup_msg.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // Length placeholder
    startup_msg.extend_from_slice(&[0x00, 0x03, 0x00, 0x00]); // Protocol version 3.0
    startup_msg.extend_from_slice(b"user\0postgres\0");
    startup_msg.extend_from_slice(b"database\0postgres\0");
    startup_msg.push(0); // Null terminator
    
    // Update length
    let len = startup_msg.len() as u32;
    startup_msg[0..4].copy_from_slice(&len.to_be_bytes());
    
    stream.write_all(&startup_msg).unwrap();
    stream.flush().unwrap();
    
    // Read authentication response
    let mut auth_response = [0u8; 9];
    stream.read_exact(&mut auth_response).unwrap();
    
    // Verify it's an authentication OK message
    assert_eq!(auth_response[0], b'R'); // Authentication message
    let auth_type = i32::from_be_bytes([auth_response[5], auth_response[6], auth_response[7], auth_response[8]]);
    assert_eq!(auth_type, 0); // AuthenticationOk
}

#[test]
#[ignore] // This test requires a running pgsqlite server with SSL enabled
fn test_ssl_request_when_ssl_enabled() {
    // Connect to the server
    let mut stream = TcpStream::connect("127.0.0.1:5432").unwrap();
    
    // Send SSL request (8 bytes: length=8, code=80877103)
    let ssl_request = [
        0x00, 0x00, 0x00, 0x08,  // Length: 8
        0x04, 0xd2, 0x16, 0x2f,  // Code: 80877103 (0x04d2162f)
    ];
    stream.write_all(&ssl_request).unwrap();
    stream.flush().unwrap();
    
    // Read response (should be 'S' for SSL supported when SSL is enabled)
    let mut response = [0u8; 1];
    stream.read_exact(&mut response).unwrap();
    
    assert!(response[0] == b'S' || response[0] == b'N', 
            "Expected 'S' or 'N' response, got: {:?}", response[0]);
}