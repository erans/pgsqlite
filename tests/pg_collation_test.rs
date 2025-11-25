use pgsqlite::session::db_handler::DbHandler;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_pg_collation_basic() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("pg_collation.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test basic query
    let result = db_handler.query_with_session("SELECT oid, collname FROM pg_collation", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.columns[0], "oid");
    assert_eq!(result.columns[1], "collname");
    assert!(result.rows.len() >= 3, "Should have at least 3 collations (default, C, POSIX)");

    // Test filtering by name
    let result = db_handler.query_with_session("SELECT * FROM pg_collation WHERE collname = 'C'", &session_id).await.unwrap();
    assert_eq!(result.rows.len(), 1);

    let collname = String::from_utf8(result.rows[0][1].as_ref().unwrap().clone()).unwrap();
    assert_eq!(collname, "C");
}

