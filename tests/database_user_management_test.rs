use pgsqlite::query::{QueryTypeDetector, QueryType};

#[test]
fn test_database_commands_query_detection() {
    // CREATE DATABASE tests
    assert_eq!(QueryTypeDetector::detect_query_type("CREATE DATABASE testdb"), QueryType::Create);
    assert_eq!(QueryTypeDetector::detect_query_type("create database testdb"), QueryType::Create);
    assert_eq!(QueryTypeDetector::detect_query_type("Create Database testdb"), QueryType::Create);
    assert_eq!(QueryTypeDetector::detect_query_type("CREATE DATABASE testdb WITH ENCODING 'UTF8'"), QueryType::Create);

    // DROP DATABASE tests
    assert_eq!(QueryTypeDetector::detect_query_type("DROP DATABASE testdb"), QueryType::Drop);
    assert_eq!(QueryTypeDetector::detect_query_type("drop database testdb"), QueryType::Drop);
    assert_eq!(QueryTypeDetector::detect_query_type("Drop Database testdb"), QueryType::Drop);
    assert_eq!(QueryTypeDetector::detect_query_type("DROP DATABASE IF EXISTS testdb"), QueryType::Drop);
}

#[test]
fn test_user_commands_query_detection() {
    // CREATE USER tests
    assert_eq!(QueryTypeDetector::detect_query_type("CREATE USER testuser"), QueryType::Create);
    assert_eq!(QueryTypeDetector::detect_query_type("create user testuser"), QueryType::Create);
    assert_eq!(QueryTypeDetector::detect_query_type("Create User testuser"), QueryType::Create);
    assert_eq!(QueryTypeDetector::detect_query_type("CREATE USER testuser WITH PASSWORD 'secret'"), QueryType::Create);

    // CREATE ROLE tests
    assert_eq!(QueryTypeDetector::detect_query_type("CREATE ROLE testrole"), QueryType::Create);
    assert_eq!(QueryTypeDetector::detect_query_type("create role testrole"), QueryType::Create);
    assert_eq!(QueryTypeDetector::detect_query_type("Create Role testrole"), QueryType::Create);

    // DROP USER tests
    assert_eq!(QueryTypeDetector::detect_query_type("DROP USER testuser"), QueryType::Drop);
    assert_eq!(QueryTypeDetector::detect_query_type("drop user testuser"), QueryType::Drop);
    assert_eq!(QueryTypeDetector::detect_query_type("Drop User testuser"), QueryType::Drop);
    assert_eq!(QueryTypeDetector::detect_query_type("DROP USER IF EXISTS testuser"), QueryType::Drop);

    // DROP ROLE tests
    assert_eq!(QueryTypeDetector::detect_query_type("DROP ROLE testrole"), QueryType::Drop);
    assert_eq!(QueryTypeDetector::detect_query_type("drop role testrole"), QueryType::Drop);
    assert_eq!(QueryTypeDetector::detect_query_type("Drop Role testrole"), QueryType::Drop);
}

#[test]
fn test_permission_commands_query_detection() {
    // GRANT tests - these should be detected as QueryType::Other
    assert_eq!(QueryTypeDetector::detect_query_type("GRANT SELECT ON table TO user"), QueryType::Other);
    assert_eq!(QueryTypeDetector::detect_query_type("grant select on table to user"), QueryType::Other);
    assert_eq!(QueryTypeDetector::detect_query_type("Grant Select On table To user"), QueryType::Other);
    assert_eq!(QueryTypeDetector::detect_query_type("GRANT ALL PRIVILEGES ON DATABASE testdb TO testuser"), QueryType::Other);

    // REVOKE tests - these should be detected as QueryType::Other
    assert_eq!(QueryTypeDetector::detect_query_type("REVOKE SELECT ON table FROM user"), QueryType::Other);
    assert_eq!(QueryTypeDetector::detect_query_type("revoke select on table from user"), QueryType::Other);
    assert_eq!(QueryTypeDetector::detect_query_type("Revoke Select On table From user"), QueryType::Other);

    // FLUSH tests - these should be detected as QueryType::Other
    assert_eq!(QueryTypeDetector::detect_query_type("FLUSH PRIVILEGES"), QueryType::Other);
    assert_eq!(QueryTypeDetector::detect_query_type("flush privileges"), QueryType::Other);
    assert_eq!(QueryTypeDetector::detect_query_type("Flush Privileges"), QueryType::Other);
    assert_eq!(QueryTypeDetector::detect_query_type("FLUSH TABLES"), QueryType::Other);
}

#[test]
fn test_commands_vs_other_creates_drops() {
    // Ensure we still detect other CREATE/DROP statements correctly
    assert_eq!(QueryTypeDetector::detect_query_type("CREATE TABLE test (id INTEGER)"), QueryType::Create);
    assert_eq!(QueryTypeDetector::detect_query_type("CREATE INDEX idx ON test (id)"), QueryType::Create);
    assert_eq!(QueryTypeDetector::detect_query_type("CREATE VIEW v AS SELECT * FROM test"), QueryType::Create);

    assert_eq!(QueryTypeDetector::detect_query_type("DROP TABLE test"), QueryType::Drop);
    assert_eq!(QueryTypeDetector::detect_query_type("DROP INDEX idx"), QueryType::Drop);
    assert_eq!(QueryTypeDetector::detect_query_type("DROP VIEW v"), QueryType::Drop);
}