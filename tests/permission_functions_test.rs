use pgsqlite::session::db_handler::DbHandler;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_pg_has_role_basic() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("permission_test.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test pg_has_role - 2 parameter version (current user assumed)
    let result = db_handler.query_with_session("SELECT pg_has_role('pg_read_all_data', 'USAGE')", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.rows.len(), 1);

    let has_role = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(has_role, "1"); // true

    // Test pg_has_role - 3 parameter version
    let result = db_handler.query_with_session("SELECT pg_has_role('postgres', 'pg_read_all_data', 'USAGE')", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.rows.len(), 1);

    let has_role = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(has_role, "1"); // true
}

#[tokio::test]
async fn test_pg_has_role_security_sensitive() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("permission_security_test.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test security-sensitive roles should return false
    let result = db_handler.query_with_session("SELECT pg_has_role('pg_read_server_files', 'USAGE')", &session_id).await.unwrap();

    let has_role = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(has_role, "0"); // false

    let result = db_handler.query_with_session("SELECT pg_has_role('postgres', 'pg_write_server_files', 'USAGE')", &session_id).await.unwrap();

    let has_role = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(has_role, "0"); // false
}

#[tokio::test]
async fn test_pg_has_role_privilege_types() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("permission_privilege_test.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test different privilege types
    let privileges = vec!["MEMBER", "USAGE", "SET"];

    for privilege in privileges {
        let query = format!("SELECT pg_has_role('pg_read_all_data', '{}')", privilege);
        let result = db_handler.query_with_session(&query, &session_id).await.unwrap();

        let has_role = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
        assert_eq!(has_role, "1"); // Should work for all valid privileges
    }
}

#[tokio::test]
async fn test_has_table_privilege_basic() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("table_privilege_test.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create a test table
    db_handler.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)").await.unwrap();

    // Test has_table_privilege - 2 parameter version (current user assumed)
    let result = db_handler.query_with_session("SELECT has_table_privilege('test_table', 'SELECT')", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.rows.len(), 1);

    let has_privilege = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(has_privilege, "1"); // true - can access user tables

    // Test has_table_privilege - 3 parameter version
    let result = db_handler.query_with_session("SELECT has_table_privilege('postgres', 'test_table', 'INSERT')", &session_id).await.unwrap();

    let has_privilege = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(has_privilege, "1"); // true - postgres can modify user tables
}

#[tokio::test]
async fn test_has_table_privilege_system_tables() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("system_table_privilege_test.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test SELECT privilege on system catalogs (should be allowed)
    let result = db_handler.query_with_session("SELECT has_table_privilege('pg_class', 'SELECT')", &session_id).await.unwrap();

    let has_privilege = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(has_privilege, "1"); // true - can read system catalogs

    // Test INSERT privilege on system catalogs (should be denied)
    let result = db_handler.query_with_session("SELECT has_table_privilege('pg_class', 'INSERT')", &session_id).await.unwrap();

    let has_privilege = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(has_privilege, "0"); // false - cannot modify system catalogs

    // Test information_schema tables
    let result = db_handler.query_with_session("SELECT has_table_privilege('information_schema.tables', 'SELECT')", &session_id).await.unwrap();

    let has_privilege = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(has_privilege, "1"); // true - can read information_schema

    let result = db_handler.query_with_session("SELECT has_table_privilege('information_schema.tables', 'UPDATE')", &session_id).await.unwrap();

    let has_privilege = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(has_privilege, "0"); // false - cannot modify information_schema
}

#[tokio::test]
async fn test_has_table_privilege_public_user() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("public_privilege_test.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create a test table
    db_handler.execute("CREATE TABLE user_table (id INTEGER PRIMARY KEY, data TEXT)").await.unwrap();

    // Test public user can read user tables
    let result = db_handler.query_with_session("SELECT has_table_privilege('public', 'user_table', 'SELECT')", &session_id).await.unwrap();

    let has_privilege = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(has_privilege, "1"); // true - public can read user tables

    // Test public user cannot modify user tables
    let result = db_handler.query_with_session("SELECT has_table_privilege('public', 'user_table', 'INSERT')", &session_id).await.unwrap();

    let has_privilege = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(has_privilege, "0"); // false - public cannot modify user tables

    // Test public user can read system catalogs
    let result = db_handler.query_with_session("SELECT has_table_privilege('public', 'pg_class', 'SELECT')", &session_id).await.unwrap();

    let has_privilege = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(has_privilege, "1"); // true - public can read system catalogs
}

#[tokio::test]
async fn test_privilege_types_comprehensive() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("privilege_types_test.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create a test table
    db_handler.execute("CREATE TABLE privilege_test_table (id INTEGER PRIMARY KEY, value INTEGER)").await.unwrap();

    // Test all valid table privilege types on user tables
    let privileges = vec!["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"];

    for privilege in privileges {
        let query = format!("SELECT has_table_privilege('privilege_test_table', '{}')", privilege);
        let result = db_handler.query_with_session(&query, &session_id).await.unwrap();

        let has_privilege = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
        assert_eq!(has_privilege, "1", "Should have {} privilege on user table", privilege);
    }

    // Test ALL and ALL PRIVILEGES
    let result = db_handler.query_with_session("SELECT has_table_privilege('privilege_test_table', 'ALL')", &session_id).await.unwrap();
    let has_privilege = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(has_privilege, "1"); // true for user tables

    let result = db_handler.query_with_session("SELECT has_table_privilege('privilege_test_table', 'ALL PRIVILEGES')", &session_id).await.unwrap();
    let has_privilege = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(has_privilege, "1"); // true for user tables
}

#[tokio::test]
async fn test_orm_compatibility_patterns() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("orm_compatibility_test.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create tables that ORMs typically work with
    db_handler.execute("CREATE TABLE django_user (id INTEGER PRIMARY KEY, username TEXT, email TEXT)").await.unwrap();
    db_handler.execute("CREATE TABLE rails_post (id INTEGER PRIMARY KEY, title TEXT, content TEXT)").await.unwrap();

    // Django-style permission checks
    let result = db_handler.query_with_session(
        "SELECT has_table_privilege('django_user', 'SELECT') AND has_table_privilege('django_user', 'INSERT')",
        &session_id
    ).await.unwrap();

    let django_access = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(django_access, "1"); // true - Django user should have full access

    // Rails-style role checking
    let result = db_handler.query_with_session(
        "SELECT pg_has_role('pg_read_all_data', 'MEMBER')",
        &session_id
    ).await.unwrap();

    let rails_role = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(rails_role, "1"); // true - Rails should be able to check roles

    // SQLAlchemy-style complex permission query
    let result = db_handler.query_with_session(
        "SELECT has_table_privilege('rails_post', 'SELECT') AS can_read, has_table_privilege('rails_post', 'UPDATE') AS can_write",
        &session_id
    ).await.unwrap();

    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.columns[0], "can_read");
    assert_eq!(result.columns[1], "can_write");

    let can_read = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    let can_write = String::from_utf8(result.rows[0][1].as_ref().unwrap().clone()).unwrap();
    assert_eq!(can_read, "1"); // true
    assert_eq!(can_write, "1"); // true

    // Ecto-style schema introspection with permissions
    let result = db_handler.query_with_session(
        "SELECT
            pg_has_role('pg_read_all_settings', 'USAGE') AS can_read_settings,
            has_table_privilege('django_user', 'TRIGGER') AS can_create_triggers",
        &session_id
    ).await.unwrap();

    let can_read_settings = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    let can_create_triggers = String::from_utf8(result.rows[0][1].as_ref().unwrap().clone()).unwrap();
    assert_eq!(can_read_settings, "1"); // true
    assert_eq!(can_create_triggers, "1"); // true
}

#[tokio::test]
async fn test_error_handling() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("error_handling_test.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test invalid privilege for pg_has_role
    let result = db_handler.query_with_session("SELECT pg_has_role('pg_read_all_data', 'INVALID')", &session_id).await;
    assert!(result.is_err()); // Should fail with invalid privilege

    // Test invalid privilege for has_table_privilege
    let result = db_handler.query_with_session("SELECT has_table_privilege('test_table', 'INVALID')", &session_id).await;
    assert!(result.is_err()); // Should fail with invalid privilege

    // Test case insensitive privilege names (should work)
    let result = db_handler.query_with_session("SELECT pg_has_role('pg_read_all_data', 'usage')", &session_id).await.unwrap();
    let has_role = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(has_role, "1"); // true - should handle case insensitive

    let result = db_handler.query_with_session("SELECT has_table_privilege('test_table', 'select')", &session_id).await.unwrap();
    let has_privilege = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(has_privilege, "1"); // true - should handle case insensitive
}