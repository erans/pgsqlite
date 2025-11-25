use pgsqlite::session::db_handler::DbHandler;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_pg_replication_slots_empty() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("pg_replication_slots.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Should return empty result with correct schema
    let result = db_handler.query_with_session("SELECT slot_name, plugin, slot_type FROM pg_replication_slots", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 3);
    assert_eq!(result.columns[0], "slot_name");
    assert_eq!(result.columns[1], "plugin");
    assert_eq!(result.columns[2], "slot_type");
    assert_eq!(result.rows.len(), 0, "pg_replication_slots should be empty for SQLite");
}

#[tokio::test]
async fn test_pg_replication_slots_all_columns() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("pg_replication_slots.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test SELECT * to ensure all columns work
    let result = db_handler.query_with_session("SELECT * FROM pg_replication_slots", &session_id).await.unwrap();

    // Should have all 16 columns according to PostgreSQL 16
    assert!(result.columns.len() >= 12, "Should have at least 12 columns");
    assert_eq!(result.rows.len(), 0, "pg_replication_slots should be empty for SQLite");
}
