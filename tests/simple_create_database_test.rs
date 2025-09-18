use pgsqlite::query::{QueryTypeDetector, QueryType};

#[test]
fn test_create_database_query_detection() {
    // Test that CREATE DATABASE is properly detected as a Create query type
    assert_eq!(QueryTypeDetector::detect_query_type("CREATE DATABASE testdb"), QueryType::Create);
    assert_eq!(QueryTypeDetector::detect_query_type("create database testdb"), QueryType::Create);
    assert_eq!(QueryTypeDetector::detect_query_type("Create Database testdb"), QueryType::Create);
    assert_eq!(QueryTypeDetector::detect_query_type("CREATE database testdb"), QueryType::Create);
    assert_eq!(QueryTypeDetector::detect_query_type("create DATABASE testdb"), QueryType::Create);

    // Test with options
    assert_eq!(QueryTypeDetector::detect_query_type("CREATE DATABASE testdb WITH ENCODING 'UTF8'"), QueryType::Create);

    // Test with whitespace
    assert_eq!(QueryTypeDetector::detect_query_type("  CREATE DATABASE testdb  "), QueryType::Create);
    assert_eq!(QueryTypeDetector::detect_query_type("\tCREATE DATABASE testdb\n"), QueryType::Create);
}

#[test]
fn test_create_database_vs_other_creates() {
    // Ensure we still detect other CREATE statements correctly
    assert_eq!(QueryTypeDetector::detect_query_type("CREATE TABLE test (id INTEGER)"), QueryType::Create);
    assert_eq!(QueryTypeDetector::detect_query_type("CREATE INDEX idx ON test (id)"), QueryType::Create);
    assert_eq!(QueryTypeDetector::detect_query_type("CREATE VIEW v AS SELECT * FROM test"), QueryType::Create);
}