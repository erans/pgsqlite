use std::sync::Arc;
use pgsqlite::session::DbHandler;
use pgsqlite::session::SessionState;

#[tokio::test]
async fn test_pg_size_pretty_bytes() {
    // Create a temporary file database for the test
    let temp_file = format!("/tmp/test_pg_size_pretty_bytes_{}.db", uuid::Uuid::new_v4());
    let db = Arc::new(DbHandler::new(&temp_file).expect("Failed to create database"));
    let _session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));

    // Test various byte sizes
    let test_cases = vec![
        ("SELECT pg_size_pretty(0)", "0 bytes"),
        ("SELECT pg_size_pretty(1)", "1 bytes"),
        ("SELECT pg_size_pretty(512)", "512 bytes"),
        ("SELECT pg_size_pretty(1023)", "1023 bytes"),
        ("SELECT pg_size_pretty(1024)", "1024 bytes"),    // Less than 10KB
        ("SELECT pg_size_pretty(5120)", "5120 bytes"),    // 5KB but less than 10KB
        ("SELECT pg_size_pretty(9216)", "9216 bytes"),    // 9KB but less than 10KB
    ];

    for (query, expected) in test_cases {
        let result = db.query(query).await;
        assert!(result.is_ok(), "Failed to execute query {}: {:?}", query, result);

        let response = result.unwrap();
        assert_eq!(response.rows.len(), 1, "Query should return 1 row: {}", query);

        if let Some(first_row) = response.rows.first() {
            if let Some(value) = &first_row[0] {
                let actual = String::from_utf8_lossy(value);
                assert_eq!(actual, expected, "Query: {} - expected: {}, got: {}", query, expected, actual);
            } else {
                panic!("Query {} returned NULL when expecting: {}", query, expected);
            }
        }
    }

    // Clean up
    std::fs::remove_file(&temp_file).ok();
}

#[tokio::test]
async fn test_pg_size_pretty_kilobytes() {
    // Create a temporary file database for the test
    let temp_file = format!("/tmp/test_pg_size_pretty_kb_{}.db", uuid::Uuid::new_v4());
    let db = Arc::new(DbHandler::new(&temp_file).expect("Failed to create database"));
    let _session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));

    // Test kilobyte sizes
    let test_cases = vec![
        ("SELECT pg_size_pretty(10240)", "10 kB"),        // 10KB
        ("SELECT pg_size_pretty(15360)", "15 kB"),        // 15KB
        ("SELECT pg_size_pretty(512000)", "500 kB"),      // 500KB
        ("SELECT pg_size_pretty(1048576)", "1048576 bytes"), // 1MB but less than 10MB
        ("SELECT pg_size_pretty(5242880)", "5242880 bytes"), // 5MB but less than 10MB
        ("SELECT pg_size_pretty(9437184)", "9437184 bytes"), // 9MB but less than 10MB
    ];

    for (query, expected) in test_cases {
        let result = db.query(query).await;
        assert!(result.is_ok(), "Failed to execute query {}: {:?}", query, result);

        let response = result.unwrap();
        assert_eq!(response.rows.len(), 1, "Query should return 1 row: {}", query);

        if let Some(first_row) = response.rows.first() {
            if let Some(value) = &first_row[0] {
                let actual = String::from_utf8_lossy(value);
                assert_eq!(actual, expected, "Query: {} - expected: {}, got: {}", query, expected, actual);
            } else {
                panic!("Query {} returned NULL when expecting: {}", query, expected);
            }
        }
    }

    // Clean up
    std::fs::remove_file(&temp_file).ok();
}

#[tokio::test]
async fn test_pg_size_pretty_megabytes() {
    // Create a temporary file database for the test
    let temp_file = format!("/tmp/test_pg_size_pretty_mb_{}.db", uuid::Uuid::new_v4());
    let db = Arc::new(DbHandler::new(&temp_file).expect("Failed to create database"));
    let _session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));

    // Test megabyte sizes
    let test_cases = vec![
        ("SELECT pg_size_pretty(10485760)", "10 MB"),      // 10MB
        ("SELECT pg_size_pretty(52428800)", "50 MB"),      // 50MB
        ("SELECT pg_size_pretty(524288000)", "500 MB"),    // 500MB
        ("SELECT pg_size_pretty(1073741824)", "1073741824 bytes"), // 1GB but less than 10GB
    ];

    for (query, expected) in test_cases {
        let result = db.query(query).await;
        assert!(result.is_ok(), "Failed to execute query {}: {:?}", query, result);

        let response = result.unwrap();
        assert_eq!(response.rows.len(), 1, "Query should return 1 row: {}", query);

        if let Some(first_row) = response.rows.first() {
            if let Some(value) = &first_row[0] {
                let actual = String::from_utf8_lossy(value);
                assert_eq!(actual, expected, "Query: {} - expected: {}, got: {}", query, expected, actual);
            } else {
                panic!("Query {} returned NULL when expecting: {}", query, expected);
            }
        }
    }

    // Clean up
    std::fs::remove_file(&temp_file).ok();
}

#[tokio::test]
async fn test_pg_size_pretty_gigabytes() {
    // Create a temporary file database for the test
    let temp_file = format!("/tmp/test_pg_size_pretty_gb_{}.db", uuid::Uuid::new_v4());
    let db = Arc::new(DbHandler::new(&temp_file).expect("Failed to create database"));
    let _session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));

    // Test gigabyte sizes
    let test_cases = vec![
        ("SELECT pg_size_pretty(10737418240)", "10 GB"),    // 10GB
        ("SELECT pg_size_pretty(53687091200)", "50 GB"),    // 50GB
        ("SELECT pg_size_pretty(536870912000)", "500 GB"),  // 500GB
    ];

    for (query, expected) in test_cases {
        let result = db.query(query).await;
        assert!(result.is_ok(), "Failed to execute query {}: {:?}", query, result);

        let response = result.unwrap();
        assert_eq!(response.rows.len(), 1, "Query should return 1 row: {}", query);

        if let Some(first_row) = response.rows.first() {
            if let Some(value) = &first_row[0] {
                let actual = String::from_utf8_lossy(value);
                assert_eq!(actual, expected, "Query: {} - expected: {}, got: {}", query, expected, actual);
            } else {
                panic!("Query {} returned NULL when expecting: {}", query, expected);
            }
        }
    }

    // Clean up
    std::fs::remove_file(&temp_file).ok();
}

#[tokio::test]
async fn test_pg_size_pretty_terabytes() {
    // Create a temporary file database for the test
    let temp_file = format!("/tmp/test_pg_size_pretty_tb_{}.db", uuid::Uuid::new_v4());
    let db = Arc::new(DbHandler::new(&temp_file).expect("Failed to create database"));
    let _session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));

    // Test terabyte sizes
    let test_cases = vec![
        ("SELECT pg_size_pretty(10995116277760)", "10 TB"),  // 10TB
        ("SELECT pg_size_pretty(54975581388800)", "50 TB"),  // 50TB
    ];

    for (query, expected) in test_cases {
        let result = db.query(query).await;
        assert!(result.is_ok(), "Failed to execute query {}: {:?}", query, result);

        let response = result.unwrap();
        assert_eq!(response.rows.len(), 1, "Query should return 1 row: {}", query);

        if let Some(first_row) = response.rows.first() {
            if let Some(value) = &first_row[0] {
                let actual = String::from_utf8_lossy(value);
                assert_eq!(actual, expected, "Query: {} - expected: {}, got: {}", query, expected, actual);
            } else {
                panic!("Query {} returned NULL when expecting: {}", query, expected);
            }
        }
    }

    // Clean up
    std::fs::remove_file(&temp_file).ok();
}

#[tokio::test]
async fn test_pg_size_pretty_edge_cases() {
    // Create a temporary file database for the test
    let temp_file = format!("/tmp/test_pg_size_pretty_edge_{}.db", uuid::Uuid::new_v4());
    let db = Arc::new(DbHandler::new(&temp_file).expect("Failed to create database"));
    let _session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));

    // Test edge cases
    let test_cases = vec![
        ("SELECT pg_size_pretty(-1)", "-1 bytes"),        // Negative number
        ("SELECT pg_size_pretty(-1024)", "-1024 bytes"),  // Negative KB
        // Note: PostgreSQL boundary testing
        ("SELECT pg_size_pretty(1023)", "1023 bytes"),    // Just under 1KB
        ("SELECT pg_size_pretty(1024)", "1024 bytes"),    // Exactly 1KB but less than 10KB
        ("SELECT pg_size_pretty(10239)", "10239 bytes"),  // Just under 10KB
        ("SELECT pg_size_pretty(10240)", "10 kB"),        // Exactly 10KB
    ];

    for (query, expected) in test_cases {
        let result = db.query(query).await;
        assert!(result.is_ok(), "Failed to execute query {}: {:?}", query, result);

        let response = result.unwrap();
        assert_eq!(response.rows.len(), 1, "Query should return 1 row: {}", query);

        if let Some(first_row) = response.rows.first() {
            if let Some(value) = &first_row[0] {
                let actual = String::from_utf8_lossy(value);
                assert_eq!(actual, expected, "Query: {} - expected: {}, got: {}", query, expected, actual);
            } else {
                panic!("Query {} returned NULL when expecting: {}", query, expected);
            }
        }
    }

    // Clean up
    std::fs::remove_file(&temp_file).ok();
}

#[tokio::test]
async fn test_pg_size_pretty_null_and_invalid() {
    // Create a temporary file database for the test
    let temp_file = format!("/tmp/test_pg_size_pretty_null_{}.db", uuid::Uuid::new_v4());
    let db = Arc::new(DbHandler::new(&temp_file).expect("Failed to create database"));
    let _session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));

    // Test NULL and invalid inputs
    let null_cases = vec![
        "SELECT pg_size_pretty()",                    // No arguments
        "SELECT pg_size_pretty(NULL)",                // NULL argument
        "SELECT pg_size_pretty('invalid')",           // Invalid string
    ];

    for query in null_cases {
        let result = db.query(query).await;
        assert!(result.is_ok(), "Failed to execute query {}: {:?}", query, result);

        let response = result.unwrap();
        assert_eq!(response.rows.len(), 1, "Query should return 1 row: {}", query);

        if let Some(first_row) = response.rows.first() {
            if let Some(value) = &first_row[0] {
                let actual = String::from_utf8_lossy(value);
                assert_eq!(actual, "NULL", "Query: {} should return NULL, got: {}", query, actual);
            } else {
                // This is fine too - SQLite NULL representation
            }
        }
    }

    // Clean up
    std::fs::remove_file(&temp_file).ok();
}

#[tokio::test]
async fn test_pg_size_pretty_with_string_numbers() {
    // Create a temporary file database for the test
    let temp_file = format!("/tmp/test_pg_size_pretty_strings_{}.db", uuid::Uuid::new_v4());
    let db = Arc::new(DbHandler::new(&temp_file).expect("Failed to create database"));
    let _session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));

    // Test string representations of numbers
    let test_cases = vec![
        ("SELECT pg_size_pretty('1024')", "1024 bytes"),
        ("SELECT pg_size_pretty('10240')", "10 kB"),
        ("SELECT pg_size_pretty('1048576')", "1048576 bytes"), // 1MB but less than 10MB
        ("SELECT pg_size_pretty('10485760')", "10 MB"),
    ];

    for (query, expected) in test_cases {
        let result = db.query(query).await;
        assert!(result.is_ok(), "Failed to execute query {}: {:?}", query, result);

        let response = result.unwrap();
        assert_eq!(response.rows.len(), 1, "Query should return 1 row: {}", query);

        if let Some(first_row) = response.rows.first() {
            if let Some(value) = &first_row[0] {
                let actual = String::from_utf8_lossy(value);
                assert_eq!(actual, expected, "Query: {} - expected: {}, got: {}", query, expected, actual);
            } else {
                panic!("Query {} returned NULL when expecting: {}", query, expected);
            }
        }
    }

    // Clean up
    std::fs::remove_file(&temp_file).ok();
}