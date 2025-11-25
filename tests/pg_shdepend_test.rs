use pgsqlite::session::db_handler::DbHandler;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_pg_shdepend_empty() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("pg_shdepend.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    let result = db_handler.query_with_session("SELECT dbid, classid, objid FROM pg_shdepend", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 3);
    assert_eq!(result.columns[0], "dbid");
    assert_eq!(result.columns[1], "classid");
    assert_eq!(result.columns[2], "objid");
    assert_eq!(result.rows.len(), 0, "pg_shdepend should be empty for SQLite");
}

#[tokio::test]
async fn test_pg_shdepend_all_columns() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("pg_shdepend.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    let result = db_handler.query_with_session("SELECT * FROM pg_shdepend", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 7, "Should have 7 columns");
    assert!(result.columns.contains(&"dbid".to_string()));
    assert!(result.columns.contains(&"classid".to_string()));
    assert!(result.columns.contains(&"objid".to_string()));
    assert!(result.columns.contains(&"objsubid".to_string()));
    assert!(result.columns.contains(&"refclassid".to_string()));
    assert!(result.columns.contains(&"refobjid".to_string()));
    assert!(result.columns.contains(&"deptype".to_string()));
    assert_eq!(result.rows.len(), 0, "pg_shdepend should be empty for SQLite");
}
