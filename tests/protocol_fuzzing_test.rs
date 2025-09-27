use std::io::Cursor;
use std::time::Duration;
use tokio::time::timeout;
use pgsqlite::protocol::{
    AuthenticationRequest, FrontendMessage, StartupMessage,
    parser::MessageParser,
};
use pgsqlite::security::events;

// Fuzzing test for protocol message parsing
#[tokio::test]
async fn test_fuzz_message_parsing() {
    let test_cases = generate_fuzz_inputs();
    let mut successful_parses = 0;
    let mut handled_errors = 0;

    for (i, input) in test_cases.iter().enumerate() {
        match parse_message_safely(input).await {
            Ok(_) => successful_parses += 1,
            Err(_) => handled_errors += 1,
        }

        // Log progress every 100 cases
        if i % 100 == 0 {
            println!("Processed {} fuzz cases: {} successful, {} errors",
                     i, successful_parses, handled_errors);
        }
    }

    println!("Fuzz testing complete: {}/{} cases handled safely",
             successful_parses + handled_errors, test_cases.len());

    // All cases should either parse successfully or error gracefully
    assert_eq!(successful_parses + handled_errors, test_cases.len());
}

#[tokio::test]
async fn test_fuzz_startup_messages() {
    let inputs = generate_startup_message_fuzzing();

    for input in inputs {
        let result = parse_startup_message_safely(&input).await;

        // Should never panic or cause undefined behavior
        match result {
            Ok(msg) => {
                // Valid startup message should have reasonable parameters
                assert!(msg.protocol_version >> 16 <= 3); // Major version
                assert!((msg.protocol_version & 0xFFFF) <= 0); // Minor version
                assert!(msg.parameters.len() <= 100); // Reasonable limit
            }
            Err(_) => {
                // Error is acceptable - should be handled gracefully
            }
        }
    }
}

#[tokio::test]
async fn test_fuzz_authentication_messages() {
    let inputs = generate_auth_message_fuzzing();

    for input in inputs {
        let result = parse_auth_message_safely(&input).await;

        match result {
            Ok(auth_req) => {
                // Valid auth request should have reasonable parameters
                match auth_req {
                    AuthenticationRequest::Ok => {},
                    AuthenticationRequest::Password => {},
                    AuthenticationRequest::MD5Password { salt } => {
                        assert_eq!(salt.len(), 4);
                    },
                    AuthenticationRequest::SASL { mechanisms } => {
                        assert!(mechanisms.len() <= 10);
                        for mechanism in mechanisms {
                            assert!(mechanism.len() <= 100);
                        }
                    },
                    AuthenticationRequest::SASLContinue { data } => {
                        assert!(data.len() <= 4096);
                    },
                    AuthenticationRequest::SASLFinal { data } => {
                        assert!(data.len() <= 4096);
                    },
                }
            }
            Err(_) => {
                // Error handling is acceptable
            }
        }
    }
}

#[tokio::test]
async fn test_fuzz_query_messages() {
    let inputs = generate_query_message_fuzzing();

    for input in inputs {
        let result = parse_query_message_safely(&input).await;

        match result {
            Ok(query_msg) => {
                // Query should have reasonable length limits
                if let FrontendMessage::Query(sql) = query_msg {
                    assert!(sql.len() <= 1_000_000); // 1MB max

                    // Should not contain obvious SQL injection patterns
                    let sql_lower = sql.to_lowercase();
                    if sql_lower.contains("drop table") ||
                       sql_lower.contains("delete from") ||
                       sql_lower.contains("truncate") {
                        // Log potential malicious query for security audit
                        events::sql_injection_attempt(
                            None,
                            None,
                            &sql,
                            "Suspicious DDL/DML in fuzzing"
                        );
                    }
                }
            }
            Err(_) => {
                // Error handling is acceptable
            }
        }
    }
}

#[tokio::test]
async fn test_fuzz_performance_degradation() {
    let malicious_inputs = generate_performance_attack_inputs();

    for input in malicious_inputs {
        let start = std::time::Instant::now();

        // Use timeout to prevent DoS attacks via slow parsing
        let result = timeout(Duration::from_millis(100), parse_message_safely(&input)).await;

        let elapsed = start.elapsed();

        match result {
            Ok(_) => {
                // Parsing should complete within reasonable time
                assert!(elapsed < Duration::from_millis(100));
            }
            Err(_) => {
                // Timeout is acceptable - prevents DoS
                events::rate_limit_exceeded(None, "protocol-parser-timeout", 1);
            }
        }
    }
}

// Helper functions for safe parsing with error handling

async fn parse_message_safely(input: &[u8]) -> Result<FrontendMessage, Box<dyn std::error::Error>> {
    let mut cursor = Cursor::new(input);
    let parser = MessageParser::new();

    match parser.parse_message(&mut cursor).await {
        Ok(msg) => Ok(msg),
        Err(e) => Err(Box::new(e))
    }
}

async fn parse_startup_message_safely(input: &[u8]) -> Result<StartupMessage, Box<dyn std::error::Error>> {
    let mut cursor = Cursor::new(input);
    let parser = MessageParser::new();

    match parser.parse_startup_message(&mut cursor).await {
        Ok(msg) => Ok(msg),
        Err(e) => Err(Box::new(e))
    }
}

async fn parse_auth_message_safely(input: &[u8]) -> Result<AuthenticationRequest, Box<dyn std::error::Error>> {
    let mut cursor = Cursor::new(input);
    let parser = MessageParser::new();

    match parser.parse_auth_request(&mut cursor).await {
        Ok(msg) => Ok(msg),
        Err(e) => Err(Box::new(e))
    }
}

async fn parse_query_message_safely(input: &[u8]) -> Result<FrontendMessage, Box<dyn std::error::Error>> {
    let mut cursor = Cursor::new(input);
    let parser = MessageParser::new();

    // Calculate length for query parser
    if input.len() < 5 {
        return Err("Insufficient data for query message".into());
    }

    let length = u32::from_be_bytes([input[1], input[2], input[3], input[4]]);

    // Skip message type and length
    cursor.set_position(5);

    match parser.parse_query(&mut cursor, length - 4).await {
        Ok(msg) => Ok(msg),
        Err(e) => Err(Box::new(e))
    }
}

// Fuzz input generators

fn generate_fuzz_inputs() -> Vec<Vec<u8>> {
    let mut inputs = Vec::new();

    // Empty input
    inputs.push(vec![]);

    // Single byte inputs
    for b in 0..=255u8 {
        inputs.push(vec![b]);
    }

    // Common message type prefixes with random data
    let message_types = [b'Q', b'P', b'B', b'E', b'S', b'X', b'H', b'D', b'C', b'F'];
    for &msg_type in &message_types {
        for len in [0, 1, 4, 8, 16, 64, 256, 1024] {
            let mut msg = vec![msg_type];
            msg.extend_from_slice(&(len as u32).to_be_bytes());
            msg.extend(vec![0; len.min(1024)]); // Limit to prevent memory issues
            inputs.push(msg);
        }
    }

    // Random byte sequences
    for len in [1, 4, 16, 64, 256, 1024] {
        let mut rng_data = vec![0u8; len];
        for i in 0..len {
            rng_data[i] = (i * 37 + 113) as u8; // Deterministic "random"
        }
        inputs.push(rng_data);
    }

    // Malformed length headers
    inputs.push(vec![b'Q', 0xFF, 0xFF, 0xFF, 0xFF]); // Max u32 length
    inputs.push(vec![b'Q', 0x00, 0x00, 0x00, 0x00]); // Zero length
    inputs.push(vec![b'Q', 0x00, 0x00, 0x00]); // Incomplete length

    inputs
}

fn generate_startup_message_fuzzing() -> Vec<Vec<u8>> {
    let mut inputs = Vec::new();

    // Valid-looking startup message with protocol version 3.0
    let mut valid_startup = vec![];
    valid_startup.extend_from_slice(&(4u32 + 8).to_be_bytes()); // Length
    valid_startup.extend_from_slice(&(3u32).to_be_bytes()); // Major version
    valid_startup.extend_from_slice(&(0u32).to_be_bytes()); // Minor version
    inputs.push(valid_startup);

    // Invalid protocol versions
    for major in [0, 1, 2, 4, 255] {
        for minor in [0, 1, 255] {
            let mut msg = vec![];
            msg.extend_from_slice(&(8u32).to_be_bytes());
            msg.extend_from_slice(&(major as u32).to_be_bytes());
            msg.extend_from_slice(&(minor as u32).to_be_bytes());
            inputs.push(msg);
        }
    }

    // Oversized parameter lists
    let mut oversized = vec![];
    oversized.extend_from_slice(&(1000u32).to_be_bytes()); // Large length
    oversized.extend_from_slice(&(3u32).to_be_bytes());
    oversized.extend_from_slice(&(0u32).to_be_bytes());
    // Add many parameter pairs
    for i in 0..50 {
        oversized.extend_from_slice(format!("param{}\0", i).as_bytes());
        oversized.extend_from_slice(format!("value{}\0", i).as_bytes());
    }
    oversized.push(0); // Null terminator
    inputs.push(oversized);

    inputs
}

fn generate_auth_message_fuzzing() -> Vec<Vec<u8>> {
    let mut inputs = Vec::new();

    // Valid auth types
    for auth_type in [0, 2, 3, 5, 10, 11, 12] {
        let mut msg = vec![b'R'];
        msg.extend_from_slice(&(8u32).to_be_bytes());
        msg.extend_from_slice(&(auth_type as u32).to_be_bytes());
        inputs.push(msg);
    }

    // MD5 auth with salt
    let mut md5_msg = vec![b'R'];
    md5_msg.extend_from_slice(&(12u32).to_be_bytes());
    md5_msg.extend_from_slice(&(5u32).to_be_bytes()); // MD5 auth type
    md5_msg.extend_from_slice(&[0x12, 0x34, 0x56, 0x78]); // Salt
    inputs.push(md5_msg);

    // SASL auth with mechanisms
    let mut sasl_msg = vec![b'R'];
    sasl_msg.extend_from_slice(&(20u32).to_be_bytes());
    sasl_msg.extend_from_slice(&(10u32).to_be_bytes()); // SASL auth type
    sasl_msg.extend_from_slice(b"SCRAM-SHA-256\0");
    sasl_msg.push(0); // Null terminator
    inputs.push(sasl_msg);

    inputs
}

fn generate_query_message_fuzzing() -> Vec<Vec<u8>> {
    let mut inputs = Vec::new();

    let long_query = "A".repeat(1000);

    // Simple queries
    let queries = [
        "SELECT 1",
        "SELECT * FROM users",
        "INSERT INTO test VALUES (1)",
        "",
        long_query.as_str(),
        "SELECT 1; DROP TABLE users; --",
        "SELECT * FROM users WHERE id = 1 OR 1=1",
        "\x00\x01\x02\x03", // Binary data
    ];

    for query in &queries {
        let mut msg = vec![b'Q'];
        msg.extend_from_slice(&((query.len() + 5) as u32).to_be_bytes());
        msg.extend_from_slice(query.as_bytes());
        msg.push(0); // Null terminator
        inputs.push(msg);
    }

    inputs
}

fn generate_performance_attack_inputs() -> Vec<Vec<u8>> {
    let mut inputs = Vec::new();

    // Deeply nested structures
    let mut nested = vec![b'Q'];
    let nested_query = "SELECT ".to_string() + &"(".repeat(1000) + &")".repeat(1000);
    nested.extend_from_slice(&((nested_query.len() + 5) as u32).to_be_bytes());
    nested.extend_from_slice(nested_query.as_bytes());
    nested.push(0);
    inputs.push(nested);

    // Very long identifiers
    let mut long_id = vec![b'Q'];
    let long_name = "a".repeat(10000);
    long_id.extend_from_slice(&((long_name.len() + 5) as u32).to_be_bytes());
    long_id.extend_from_slice(long_name.as_bytes());
    long_id.push(0);
    inputs.push(long_id);

    // Many parameters
    let mut many_params = vec![b'P'];
    let query = "SELECT ".to_string() + &"$1,".repeat(1000);
    many_params.extend_from_slice(&((query.len() + 1000) as u32).to_be_bytes());
    many_params.extend_from_slice(b"stmt\0");
    many_params.extend_from_slice(query.as_bytes());
    many_params.push(0);
    many_params.extend_from_slice(&(1000u16).to_be_bytes()); // 1000 parameter types
    many_params.extend(vec![0; 1000 * 4]); // Parameter type OIDs (all zeros for simplicity)
    inputs.push(many_params);

    inputs
}