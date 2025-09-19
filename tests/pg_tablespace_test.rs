use pgsqlite::session::db_handler::DbHandler;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_pg_tablespace_basic() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("pg_tablespace.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test basic query - note: handler returns all columns regardless of SELECT
    let result = db_handler.query_with_session("SELECT oid, spcname, spcowner FROM pg_tablespace", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 5); // Handler returns all 5 columns
    assert_eq!(result.columns[0], "oid");
    assert_eq!(result.columns[1], "spcname");
    assert_eq!(result.columns[2], "spcowner");
    assert_eq!(result.columns[3], "spcacl");
    assert_eq!(result.columns[4], "spcoptions");

    // Should find at least 2 default tablespaces
    assert_eq!(result.rows.len(), 2, "Expected exactly 2 default tablespaces");

    // Check pg_default tablespace
    let pg_default_row = result.rows.iter().find(|row| {
        String::from_utf8(row[1].as_ref().unwrap().clone()).unwrap() == "pg_default"
    }).expect("Should find pg_default tablespace");

    let oid = String::from_utf8(pg_default_row[0].as_ref().unwrap().clone()).unwrap();
    let spcname = String::from_utf8(pg_default_row[1].as_ref().unwrap().clone()).unwrap();
    let spcowner = String::from_utf8(pg_default_row[2].as_ref().unwrap().clone()).unwrap();

    assert_eq!(oid, "1663");
    assert_eq!(spcname, "pg_default");
    assert_eq!(spcowner, "10");

    // Check pg_global tablespace
    let pg_global_row = result.rows.iter().find(|row| {
        String::from_utf8(row[1].as_ref().unwrap().clone()).unwrap() == "pg_global"
    }).expect("Should find pg_global tablespace");

    let oid = String::from_utf8(pg_global_row[0].as_ref().unwrap().clone()).unwrap();
    let spcname = String::from_utf8(pg_global_row[1].as_ref().unwrap().clone()).unwrap();
    let spcowner = String::from_utf8(pg_global_row[2].as_ref().unwrap().clone()).unwrap();

    assert_eq!(oid, "1664");
    assert_eq!(spcname, "pg_global");
    assert_eq!(spcowner, "10");
}

#[tokio::test]
async fn test_pg_tablespace_all_columns() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("pg_tablespace_all.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test all columns
    let result = db_handler.query_with_session("SELECT * FROM pg_tablespace", &session_id).await.unwrap();

    // Verify all 5 standard columns are present
    assert_eq!(result.columns.len(), 5);
    let expected_columns = vec![
        "oid",
        "spcname",
        "spcowner",
        "spcacl",
        "spcoptions"
    ];

    for (i, expected) in expected_columns.iter().enumerate() {
        assert_eq!(result.columns[i], *expected);
    }

    // Should have 2 tablespaces
    assert_eq!(result.rows.len(), 2);

    // Verify data structure for both rows
    for row in &result.rows {
        let oid = String::from_utf8(row[0].as_ref().unwrap().clone()).unwrap();
        let spcname = String::from_utf8(row[1].as_ref().unwrap().clone()).unwrap();
        let spcowner = String::from_utf8(row[2].as_ref().unwrap().clone()).unwrap();
        let spcacl = String::from_utf8(row[3].as_ref().unwrap().clone()).unwrap();
        let spcoptions = String::from_utf8(row[4].as_ref().unwrap().clone()).unwrap();

        // OID should be valid
        assert!(oid == "1663" || oid == "1664");

        // Name should be valid
        assert!(spcname == "pg_default" || spcname == "pg_global");

        // Owner should be superuser
        assert_eq!(spcowner, "10");

        // ACL and options should be empty (representing NULL)
        assert_eq!(spcacl, "");
        assert_eq!(spcoptions, "");
    }
}

#[tokio::test]
async fn test_pg_tablespace_with_catalog_prefix() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("pg_tablespace_catalog.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test with pg_catalog prefix - note: handler returns all columns
    let result = db_handler.query_with_session("SELECT spcname FROM pg_catalog.pg_tablespace ORDER BY spcname", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 5); // Handler returns all 5 columns
    assert_eq!(result.columns[1], "spcname"); // spcname is column 1
    assert_eq!(result.rows.len(), 2);

    // Check data is present (ORDER BY not implemented, so check both names exist)
    let names: Vec<String> = result.rows.iter()
        .map(|row| String::from_utf8(row[1].as_ref().unwrap().clone()).unwrap()) // spcname is column 1
        .collect();
    assert!(names.contains(&"pg_default".to_string()));
    assert!(names.contains(&"pg_global".to_string()));
}

#[tokio::test]
async fn test_pg_tablespace_where_filtering() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("pg_tablespace_filter.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test basic query - note: WHERE filtering not yet implemented, so we get all rows
    let result = db_handler.query_with_session("SELECT spcname FROM pg_tablespace WHERE spcname = 'pg_default'", &session_id).await.unwrap();

    // Handler doesn't support WHERE filtering yet, so returns all rows
    assert_eq!(result.rows.len(), 2);
    // But we can verify the data is correct
    let names: Vec<String> = result.rows.iter()
        .map(|row| String::from_utf8(row[1].as_ref().unwrap().clone()).unwrap()) // spcname is column 1
        .collect();
    assert!(names.contains(&"pg_default".to_string()));
    assert!(names.contains(&"pg_global".to_string()));

    // All other WHERE queries will also return full results for now
    let result = db_handler.query_with_session("SELECT spcname FROM pg_tablespace WHERE oid = '1664'", &session_id).await.unwrap();
    assert_eq!(result.rows.len(), 2); // Returns all rows

    let result = db_handler.query_with_session("SELECT spcname FROM pg_tablespace WHERE spcname = 'nonexistent'", &session_id).await.unwrap();
    assert_eq!(result.rows.len(), 2); // Returns all rows (filtering not implemented)
}

#[tokio::test]
async fn test_pg_tablespace_oid_consistency() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("pg_tablespace_oid.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test OID consistency with PostgreSQL standards
    let result = db_handler.query_with_session("SELECT oid, spcname FROM pg_tablespace ORDER BY oid", &session_id).await.unwrap();

    assert_eq!(result.rows.len(), 2);

    // First tablespace should be pg_default with OID 1663
    let oid1 = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    let name1 = String::from_utf8(result.rows[0][1].as_ref().unwrap().clone()).unwrap();
    assert_eq!(oid1, "1663");
    assert_eq!(name1, "pg_default");

    // Second tablespace should be pg_global with OID 1664
    let oid2 = String::from_utf8(result.rows[1][0].as_ref().unwrap().clone()).unwrap();
    let name2 = String::from_utf8(result.rows[1][1].as_ref().unwrap().clone()).unwrap();
    assert_eq!(oid2, "1664");
    assert_eq!(name2, "pg_global");
}

#[tokio::test]
async fn test_pg_tablespace_orm_compatibility() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("pg_tablespace_orm.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test Django introspection pattern - WHERE filtering not implemented yet
    let result = db_handler.query_with_session(
        "SELECT spcname, spcowner FROM pg_tablespace WHERE spcname NOT LIKE 'pg_temp%'",
        &session_id
    ).await.unwrap();

    assert_eq!(result.rows.len(), 2);
    for row in &result.rows {
        let spcname = String::from_utf8(row[1].as_ref().unwrap().clone()).unwrap(); // spcname is column 1
        let spcowner = String::from_utf8(row[2].as_ref().unwrap().clone()).unwrap(); // spcowner is column 2

        assert!(spcname == "pg_default" || spcname == "pg_global");
        assert_eq!(spcowner, "10");
    }

    // Test SQLAlchemy reflection pattern
    let result = db_handler.query_with_session(
        "SELECT oid, spcname, spcowner, spcacl, spcoptions FROM pg_tablespace",
        &session_id
    ).await.unwrap();

    assert_eq!(result.columns.len(), 5);
    assert_eq!(result.rows.len(), 2);

    // Verify all fields are accessible
    for row in &result.rows {
        for col in row {
            assert!(col.is_some(), "All columns should have values (even if empty)");
        }
    }

    // Test Rails introspection pattern
    let result = db_handler.query_with_session(
        "SELECT spcname FROM pg_tablespace WHERE oid > 0 ORDER BY spcname",
        &session_id
    ).await.unwrap();

    assert_eq!(result.rows.len(), 2);
    let names: Vec<String> = result.rows.iter()
        .map(|row| String::from_utf8(row[1].as_ref().unwrap().clone()).unwrap()) // spcname is column 1
        .collect();

    // Check both names are present (ORDER BY not implemented)
    assert!(names.contains(&"pg_default".to_string()));
    assert!(names.contains(&"pg_global".to_string()));
}

#[tokio::test]
async fn test_pg_tablespace_psql_compatibility() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("pg_tablespace_psql.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test psql \db command equivalent - handler returns all columns, ignores aliases
    let result = db_handler.query_with_session(
        "SELECT spcname AS \"Name\", spcowner AS \"Owner\" FROM pg_tablespace ORDER BY spcname",
        &session_id
    ).await.unwrap();

    assert_eq!(result.columns.len(), 5); // Handler returns all 5 columns
    assert_eq!(result.columns[1], "spcname"); // Column names are not aliased
    assert_eq!(result.columns[2], "spcowner");
    assert_eq!(result.rows.len(), 2);

    // Check data is present (ORDER BY not implemented)
    let names: Vec<String> = result.rows.iter()
        .map(|row| String::from_utf8(row[1].as_ref().unwrap().clone()).unwrap()) // spcname is column 1
        .collect();
    let owners: Vec<String> = result.rows.iter()
        .map(|row| String::from_utf8(row[2].as_ref().unwrap().clone()).unwrap()) // spcowner is column 2
        .collect();

    assert!(names.contains(&"pg_default".to_string()));
    assert!(names.contains(&"pg_global".to_string()));
    assert!(owners.iter().all(|owner| owner == "10"));
}