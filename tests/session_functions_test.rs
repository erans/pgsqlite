use pgsqlite::session::db_handler::DbHandler;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_current_user_basic() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("session_test.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test current_user function
    let result = db_handler.query_with_session("SELECT current_user", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.rows.len(), 1);

    let user = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(user, "postgres");
}

#[tokio::test]
async fn test_current_user_with_parentheses() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("session_parentheses_test.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test current_user() function with parentheses
    let result = db_handler.query_with_session("SELECT current_user()", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.rows.len(), 1);

    let user = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(user, "postgres");
}

#[tokio::test]
async fn test_current_database_basic() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("database_test.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test current_database function
    let result = db_handler.query_with_session("SELECT current_database()", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.rows.len(), 1);

    let database = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(database, "main");
}

#[tokio::test]
async fn test_session_user_basic() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("session_user_test.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test session_user function
    let result = db_handler.query_with_session("SELECT session_user()", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.rows.len(), 1);

    let session_user = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(session_user, "postgres");
}

#[tokio::test]
async fn test_current_schema_basic() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("schema_test.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test current_schema function
    let result = db_handler.query_with_session("SELECT current_schema()", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.rows.len(), 1);

    let schema = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(schema, "public");
}

#[tokio::test]
async fn test_current_schemas_with_implicit() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("schemas_implicit_test.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test current_schemas with implicit=true
    let result = db_handler.query_with_session("SELECT current_schemas(true)", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.rows.len(), 1);

    let schemas = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(schemas, r#"["pg_catalog","public"]"#);
}

#[tokio::test]
async fn test_current_schemas_without_implicit() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("schemas_no_implicit_test.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test current_schemas with implicit=false
    let result = db_handler.query_with_session("SELECT current_schemas(false)", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.rows.len(), 1);

    let schemas = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(schemas, r#"["public"]"#);
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

    // Django-style connection validation query
    let result = db_handler.query_with_session(
        "SELECT current_database(), current_user, current_schema()",
        &session_id
    ).await.unwrap();

    assert_eq!(result.columns.len(), 3);
    assert_eq!(result.rows.len(), 1);

    let database = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    let user = String::from_utf8(result.rows[0][1].as_ref().unwrap().clone()).unwrap();
    let schema = String::from_utf8(result.rows[0][2].as_ref().unwrap().clone()).unwrap();

    assert_eq!(database, "main");
    assert_eq!(user, "postgres");
    assert_eq!(schema, "public");

    // SQLAlchemy-style session information query
    let result = db_handler.query_with_session(
        "SELECT current_user AS username, current_database() AS db_name, current_schema() AS schema_name",
        &session_id
    ).await.unwrap();

    assert_eq!(result.columns.len(), 3);
    assert_eq!(result.columns[0], "username");
    assert_eq!(result.columns[1], "db_name");
    assert_eq!(result.columns[2], "schema_name");

    let username = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    let db_name = String::from_utf8(result.rows[0][1].as_ref().unwrap().clone()).unwrap();
    let schema_name = String::from_utf8(result.rows[0][2].as_ref().unwrap().clone()).unwrap();

    assert_eq!(username, "postgres");
    assert_eq!(db_name, "main");
    assert_eq!(schema_name, "public");

    // Rails-style environment check
    let result = db_handler.query_with_session(
        "SELECT session_user() = current_user AS same_user, current_schemas(false) AS user_schemas",
        &session_id
    ).await.unwrap();

    assert_eq!(result.columns.len(), 2);

    let same_user = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    let user_schemas = String::from_utf8(result.rows[0][1].as_ref().unwrap().clone()).unwrap();

    assert_eq!(same_user, "1"); // true - session_user equals current_user
    assert_eq!(user_schemas, r#"["public"]"#);

    // Ecto-style connection info
    let result = db_handler.query_with_session(
        "SELECT
            current_database() AS database,
            current_user AS user_name,
            current_schema() AS default_schema,
            session_user() AS session_user_name
        ",
        &session_id
    ).await.unwrap();

    assert_eq!(result.columns.len(), 4);
    assert_eq!(result.columns[0], "database");
    assert_eq!(result.columns[1], "user_name");
    assert_eq!(result.columns[2], "default_schema");
    assert_eq!(result.columns[3], "session_user_name");

    let database = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    let user_name = String::from_utf8(result.rows[0][1].as_ref().unwrap().clone()).unwrap();
    let default_schema = String::from_utf8(result.rows[0][2].as_ref().unwrap().clone()).unwrap();
    let session_user_name = String::from_utf8(result.rows[0][3].as_ref().unwrap().clone()).unwrap();

    assert_eq!(database, "main");
    assert_eq!(user_name, "postgres");
    assert_eq!(default_schema, "public");
    assert_eq!(session_user_name, "postgres");
}

#[tokio::test]
async fn test_session_functions_consistency() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("consistency_test.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create multiple sessions to test consistency
    let session1 = Uuid::new_v4();
    let session2 = Uuid::new_v4();

    db_handler.create_session_connection(session1).await.unwrap();
    db_handler.create_session_connection(session2).await.unwrap();

    // Test that session functions return the same values across different sessions
    let result1 = db_handler.query_with_session("SELECT current_user, current_database(), current_schema()", &session1).await.unwrap();
    let result2 = db_handler.query_with_session("SELECT current_user, current_database(), current_schema()", &session2).await.unwrap();

    assert_eq!(result1.columns.len(), 3);
    assert_eq!(result2.columns.len(), 3);

    // Values should be consistent across sessions
    assert_eq!(result1.rows[0], result2.rows[0]);

    let user1 = String::from_utf8(result1.rows[0][0].as_ref().unwrap().clone()).unwrap();
    let user2 = String::from_utf8(result2.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(user1, user2);
    assert_eq!(user1, "postgres");

    let db1 = String::from_utf8(result1.rows[0][1].as_ref().unwrap().clone()).unwrap();
    let db2 = String::from_utf8(result2.rows[0][1].as_ref().unwrap().clone()).unwrap();
    assert_eq!(db1, db2);
    assert_eq!(db1, "main");

    let schema1 = String::from_utf8(result1.rows[0][2].as_ref().unwrap().clone()).unwrap();
    let schema2 = String::from_utf8(result2.rows[0][2].as_ref().unwrap().clone()).unwrap();
    assert_eq!(schema1, schema2);
    assert_eq!(schema1, "public");
}

#[tokio::test]
async fn test_session_functions_with_table_operations() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("table_ops_test.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create a table
    db_handler.execute("CREATE TABLE test_sessions (id INTEGER PRIMARY KEY, created_by TEXT, db_name TEXT)").await.unwrap();

    // Insert data using session functions
    let result = db_handler.query_with_session(
        "INSERT INTO test_sessions (created_by, db_name) VALUES (current_user, current_database()) RETURNING id, created_by, db_name",
        &session_id
    ).await.unwrap();

    assert_eq!(result.columns.len(), 3);
    assert_eq!(result.rows.len(), 1);

    let created_by = String::from_utf8(result.rows[0][1].as_ref().unwrap().clone()).unwrap();
    let db_name = String::from_utf8(result.rows[0][2].as_ref().unwrap().clone()).unwrap();

    assert_eq!(created_by, "postgres");
    assert_eq!(db_name, "main");

    // Query the data back
    let result = db_handler.query_with_session(
        "SELECT created_by, db_name FROM test_sessions WHERE created_by = current_user AND db_name = current_database()",
        &session_id
    ).await.unwrap();

    assert_eq!(result.rows.len(), 1);

    let queried_user = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    let queried_db = String::from_utf8(result.rows[0][1].as_ref().unwrap().clone()).unwrap();

    assert_eq!(queried_user, "postgres");
    assert_eq!(queried_db, "main");
}

#[tokio::test]
async fn test_logging_and_audit_patterns() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("audit_test.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create audit table
    db_handler.execute("CREATE TABLE audit_log (
        id INTEGER PRIMARY KEY,
        action TEXT,
        table_name TEXT,
        user_name TEXT,
        database_name TEXT,
        schema_name TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )").await.unwrap();

    // ORM-style audit logging using session functions
    let result = db_handler.query_with_session(
        "INSERT INTO audit_log (action, table_name, user_name, database_name, schema_name)
         VALUES ('CREATE_TABLE', 'users', current_user, current_database(), current_schema())
         RETURNING id, user_name, database_name, schema_name",
        &session_id
    ).await.unwrap();

    assert_eq!(result.columns.len(), 4);
    assert_eq!(result.rows.len(), 1);

    let user_name = String::from_utf8(result.rows[0][1].as_ref().unwrap().clone()).unwrap();
    let database_name = String::from_utf8(result.rows[0][2].as_ref().unwrap().clone()).unwrap();
    let schema_name = String::from_utf8(result.rows[0][3].as_ref().unwrap().clone()).unwrap();

    assert_eq!(user_name, "postgres");
    assert_eq!(database_name, "main");
    assert_eq!(schema_name, "public");

    // Test complex logging query with multiple session functions
    let result = db_handler.query_with_session(
        "SELECT
            'User: ' || current_user ||
            ', Database: ' || current_database() ||
            ', Schema: ' || current_schema() ||
            ', Session User: ' || session_user() AS audit_info",
        &session_id
    ).await.unwrap();

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.rows.len(), 1);

    let audit_info = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert_eq!(audit_info, "User: postgres, Database: main, Schema: public, Session User: postgres");
}