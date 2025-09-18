use std::sync::Arc;
use pgsqlite::session::DbHandler;
use pgsqlite::session::SessionState;

#[tokio::test]
async fn test_pg_stat_activity_view() {
    // Create a temporary file database for the test
    let temp_file = format!("/tmp/test_pg_stat_activity_{}.db", uuid::Uuid::new_v4());
    let db = Arc::new(DbHandler::new(&temp_file).expect("Failed to create database"));
    let _session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));

    // Test will run migrations automatically via DbHandler::new

    // Test pg_stat_activity view basic structure
    let result = db.query("SELECT datid, datname, pid, usename, application_name, state, backend_type FROM pg_stat_activity").await;
    assert!(result.is_ok(), "Failed to query pg_stat_activity: {:?}", result);

    let response = result.unwrap();
    assert_eq!(response.columns.len(), 7, "pg_stat_activity should have 7 columns in this query");
    assert_eq!(response.rows.len(), 1, "pg_stat_activity should return 1 row");

    // Verify column values
    if let Some(first_row) = response.rows.first() {
        // datid should be 1
        assert_eq!(first_row[0], Some(b"1".to_vec()));
        // datname should be 'main'
        assert_eq!(first_row[1], Some(b"main".to_vec()));
        // usename should be 'postgres'
        assert_eq!(first_row[3], Some(b"postgres".to_vec()));
        // application_name should be 'pgsqlite'
        assert_eq!(first_row[4], Some(b"pgsqlite".to_vec()));
        // state should be 'idle'
        assert_eq!(first_row[5], Some(b"idle".to_vec()));
        // backend_type should be 'client backend'
        assert_eq!(first_row[6], Some(b"client backend".to_vec()));
    }

    // Clean up
    std::fs::remove_file(&temp_file).ok();
}

#[tokio::test]
async fn test_pg_stat_database_view() {
    // Create a temporary file database for the test
    let temp_file = format!("/tmp/test_pg_stat_database_{}.db", uuid::Uuid::new_v4());
    let db = Arc::new(DbHandler::new(&temp_file).expect("Failed to create database"));
    let _session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));

    // Test will run migrations automatically via DbHandler::new

    // Test pg_stat_database view
    let result = db.query("SELECT datid, datname, numbackends, xact_commit, xact_rollback FROM pg_stat_database").await;
    assert!(result.is_ok(), "Failed to query pg_stat_database: {:?}", result);

    let response = result.unwrap();
    assert_eq!(response.columns.len(), 5, "Query should return 5 columns");
    assert_eq!(response.rows.len(), 1, "pg_stat_database should return 1 row");

    // Verify some key values
    if let Some(first_row) = response.rows.first() {
        // datid should be 1
        assert_eq!(first_row[0], Some(b"1".to_vec()));
        // datname should be 'main'
        assert_eq!(first_row[1], Some(b"main".to_vec()));
        // numbackends should be 1
        assert_eq!(first_row[2], Some(b"1".to_vec()));
    }

    // Clean up
    std::fs::remove_file(&temp_file).ok();
}

#[tokio::test]
async fn test_pg_stat_user_tables_view() {
    // Create a temporary file database for the test
    let temp_file = format!("/tmp/test_pg_stat_user_tables_{}.db", uuid::Uuid::new_v4());
    let db = Arc::new(DbHandler::new(&temp_file).expect("Failed to create database"));
    let _session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));

    // Test will run migrations automatically via DbHandler::new

    // Create a test table
    let result = db.query("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)").await;
    assert!(result.is_ok(), "Failed to create test table: {:?}", result);

    // Test pg_stat_user_tables view
    let result = db.query("SELECT relid, schemaname, relname, seq_scan, n_tup_ins FROM pg_stat_user_tables WHERE relname = 'test_table'").await;
    assert!(result.is_ok(), "Failed to query pg_stat_user_tables: {:?}", result);

    let response = result.unwrap();
    assert_eq!(response.columns.len(), 5, "Query should return 5 columns");
    assert_eq!(response.rows.len(), 1, "Should find our test_table");

    // Verify values
    if let Some(first_row) = response.rows.first() {
        // Should have a relid (OID)
        assert!(first_row[0].is_some(), "relid should not be null");
        // schemaname should be 'public'
        assert_eq!(first_row[1], Some(b"public".to_vec()));
        // relname should be 'test_table'
        assert_eq!(first_row[2], Some(b"test_table".to_vec()));
        // seq_scan should be 0 (initial)
        assert_eq!(first_row[3], Some(b"0".to_vec()));
        // n_tup_ins should be 0 (initial)
        assert_eq!(first_row[4], Some(b"0".to_vec()));
    }

    // Clean up
    std::fs::remove_file(&temp_file).ok();
}

#[tokio::test]
async fn test_pg_database_view() {
    // Create a temporary file database for the test
    let temp_file = format!("/tmp/test_pg_database_{}.db", uuid::Uuid::new_v4());
    let db = Arc::new(DbHandler::new(&temp_file).expect("Failed to create database"));
    let _session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));

    // Test will run migrations automatically via DbHandler::new

    // Test pg_database view
    let result = db.query("SELECT oid, datname, datdba, encoding, datistemplate, datallowconn FROM pg_database").await;
    assert!(result.is_ok(), "Failed to query pg_database: {:?}", result);

    let response = result.unwrap();
    assert_eq!(response.columns.len(), 6, "Query should return 6 columns");
    assert_eq!(response.rows.len(), 1, "pg_database should return 1 row");

    // Verify values
    if let Some(first_row) = response.rows.first() {
        // oid should be 1
        assert_eq!(first_row[0], Some(b"1".to_vec()));
        // datname should be 'main'
        assert_eq!(first_row[1], Some(b"main".to_vec()));
        // datdba should be 10
        assert_eq!(first_row[2], Some(b"10".to_vec()));
        // encoding should be 6 (UTF8)
        assert_eq!(first_row[3], Some(b"6".to_vec()));
        // datistemplate should be false (0)
        assert_eq!(first_row[4], Some(b"0".to_vec()));
        // datallowconn should be true (1)
        assert_eq!(first_row[5], Some(b"1".to_vec()));
    }

    // Clean up
    std::fs::remove_file(&temp_file).ok();
}

#[tokio::test]
async fn test_pg_foreign_data_wrapper_view() {
    // Create a temporary file database for the test
    let temp_file = format!("/tmp/test_pg_foreign_data_wrapper_{}.db", uuid::Uuid::new_v4());
    let db = Arc::new(DbHandler::new(&temp_file).expect("Failed to create database"));
    let _session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));

    // Test will run migrations automatically via DbHandler::new

    // Test pg_foreign_data_wrapper view
    let result = db.query("SELECT oid, fdwname, fdwowner, fdwhandler FROM pg_foreign_data_wrapper").await;
    assert!(result.is_ok(), "Failed to query pg_foreign_data_wrapper: {:?}", result);

    let response = result.unwrap();
    assert_eq!(response.columns.len(), 4, "Query should return 4 columns");
    assert_eq!(response.rows.len(), 0, "pg_foreign_data_wrapper should return 0 rows (empty view)");

    // Clean up
    std::fs::remove_file(&temp_file).ok();
}

#[tokio::test]
async fn test_pg_stat_all_tables_view() {
    // Create a temporary file database for the test
    let temp_file = format!("/tmp/test_pg_stat_all_tables_{}.db", uuid::Uuid::new_v4());
    let db = Arc::new(DbHandler::new(&temp_file).expect("Failed to create database"));
    let _session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));

    // Test will run migrations automatically via DbHandler::new

    // Create a test table
    let result = db.query("CREATE TABLE another_test (id INTEGER)").await;
    assert!(result.is_ok(), "Failed to create test table: {:?}", result);

    // Test pg_stat_all_tables view (should be same as pg_stat_user_tables)
    let result = db.query("SELECT schemaname, relname FROM pg_stat_all_tables WHERE relname = 'another_test'").await;
    assert!(result.is_ok(), "Failed to query pg_stat_all_tables: {:?}", result);

    let response = result.unwrap();
    assert_eq!(response.columns.len(), 2, "Query should return 2 columns");
    assert_eq!(response.rows.len(), 1, "Should find our test table");

    // Clean up
    std::fs::remove_file(&temp_file).ok();
}

#[tokio::test]
async fn test_pg_stat_user_indexes_view() {
    // Create a temporary file database for the test
    let temp_file = format!("/tmp/test_pg_stat_user_indexes_{}.db", uuid::Uuid::new_v4());
    let db = Arc::new(DbHandler::new(&temp_file).expect("Failed to create database"));
    let _session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));

    // Test will run migrations automatically via DbHandler::new

    // Test pg_stat_user_indexes view (should be empty)
    let result = db.query("SELECT relid, indexrelid, schemaname FROM pg_stat_user_indexes").await;
    assert!(result.is_ok(), "Failed to query pg_stat_user_indexes: {:?}", result);

    let response = result.unwrap();
    assert_eq!(response.columns.len(), 3, "Query should return 3 columns");
    assert_eq!(response.rows.len(), 0, "pg_stat_user_indexes should return 0 rows (empty view)");

    // Clean up
    std::fs::remove_file(&temp_file).ok();
}