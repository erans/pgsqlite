use pgsqlite::session::db_handler::DbHandler;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_pg_statistic_empty() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("pg_statistic.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    let result = db_handler.query_with_session("SELECT starelid, staattnum FROM pg_statistic", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.columns[0], "starelid");
    assert_eq!(result.columns[1], "staattnum");
    assert_eq!(result.rows.len(), 0, "pg_statistic should be empty (use pg_stats view instead)");
}

#[tokio::test]
async fn test_pg_statistic_all_columns() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("pg_statistic.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    let result = db_handler.query_with_session("SELECT * FROM pg_statistic", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 31, "Should have 31 columns");
    assert!(result.columns.contains(&"starelid".to_string()));
    assert!(result.columns.contains(&"staattnum".to_string()));
    assert!(result.columns.contains(&"stainherit".to_string()));
    assert!(result.columns.contains(&"stanullfrac".to_string()));
    assert!(result.columns.contains(&"stawidth".to_string()));
    assert!(result.columns.contains(&"stadistinct".to_string()));
    assert!(result.columns.contains(&"stakind1".to_string()));
    assert!(result.columns.contains(&"staop1".to_string()));
    assert!(result.columns.contains(&"stacoll1".to_string()));
    assert!(result.columns.contains(&"stanumbers1".to_string()));
    assert!(result.columns.contains(&"stavalues1".to_string()));
    assert_eq!(result.rows.len(), 0, "pg_statistic should be empty (use pg_stats view instead)");
}
