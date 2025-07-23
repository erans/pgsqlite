/// Integration test for system functions that SQLAlchemy commonly uses
use pgsqlite::session::db_handler::DbHandler;

#[tokio::test]
async fn test_sqlalchemy_system_functions() {
    // Create an in-memory database for testing
    let db = DbHandler::new_for_test(":memory:").unwrap();
    
    // Test version() function - SQLAlchemy uses this to detect PostgreSQL
    let response = db.query("SELECT version()").await.unwrap();
    assert_eq!(response.rows.len(), 1);
    let version_bytes = &response.rows[0][0];
    let version_str = std::str::from_utf8(version_bytes.as_ref().unwrap()).unwrap();
    assert!(version_str.starts_with("PostgreSQL"));
    assert!(version_str.contains("pgsqlite"));
    
    // Test current_database() function
    let response = db.query("SELECT current_database()").await.unwrap();
    assert_eq!(response.rows.len(), 1);
    let db_name_bytes = &response.rows[0][0];
    let db_name = std::str::from_utf8(db_name_bytes.as_ref().unwrap()).unwrap();
    assert_eq!(db_name, "main");
    
    // Test current_schema() function
    let response = db.query("SELECT current_schema()").await.unwrap();
    assert_eq!(response.rows.len(), 1);
    let schema_bytes = &response.rows[0][0];
    let schema = std::str::from_utf8(schema_bytes.as_ref().unwrap()).unwrap();
    assert_eq!(schema, "public");
    
    // Test current_user() function
    let response = db.query("SELECT current_user()").await.unwrap();
    assert_eq!(response.rows.len(), 1);
    let user_bytes = &response.rows[0][0];
    let user = std::str::from_utf8(user_bytes.as_ref().unwrap()).unwrap();
    assert_eq!(user, "postgres");
    
    // Test pg_backend_pid() function
    let response = db.query("SELECT pg_backend_pid()").await.unwrap();
    assert_eq!(response.rows.len(), 1);
    let pid_bytes = &response.rows[0][0];
    assert!(pid_bytes.is_some());
    
    // Test pg_is_in_recovery() function
    let response = db.query("SELECT pg_is_in_recovery()").await.unwrap();
    assert_eq!(response.rows.len(), 1);
    let recovery_bytes = &response.rows[0][0];
    let recovery_str = std::str::from_utf8(recovery_bytes.as_ref().unwrap()).unwrap();
    assert_eq!(recovery_str, "0"); // Should be false (0) for SQLite
    
    // Test privilege functions that SQLAlchemy might use
    let response = db.query("SELECT has_table_privilege('postgres', 'pg_class', 'SELECT')").await.unwrap();
    assert_eq!(response.rows.len(), 1);
    let privilege_bytes = &response.rows[0][0];
    let privilege_str = std::str::from_utf8(privilege_bytes.as_ref().unwrap()).unwrap();
    assert_eq!(privilege_str, "1"); // Should be true (1)
    
    // Test network functions
    let response = db.query("SELECT inet_client_addr(), inet_server_port()").await.unwrap();
    assert_eq!(response.rows.len(), 1);
    let client_addr_bytes = &response.rows[0][0];
    let client_addr = std::str::from_utf8(client_addr_bytes.as_ref().unwrap()).unwrap();
    assert_eq!(client_addr, "127.0.0.1");
    
    let server_port_bytes = &response.rows[0][1];
    let server_port_str = std::str::from_utf8(server_port_bytes.as_ref().unwrap()).unwrap();
    assert_eq!(server_port_str, "5432");
    
    // Test current_schemas() function - returns JSON array
    let response = db.query("SELECT current_schemas(true)").await.unwrap();
    assert_eq!(response.rows.len(), 1);
    let schemas_bytes = &response.rows[0][0];
    let schemas = std::str::from_utf8(schemas_bytes.as_ref().unwrap()).unwrap();
    assert!(schemas.contains("pg_catalog"));
    assert!(schemas.contains("public"));
    
    println!("✅ All SQLAlchemy system functions are working correctly!");
}

#[tokio::test]
async fn test_system_functions_combined_query() {
    // Test a more complex query that might be used by SQLAlchemy for introspection
    let db = DbHandler::new_for_test(":memory:").unwrap();
    
    let query = "
        SELECT 
            version() as server_version,
            current_database() as database_name,
            current_schema() as schema_name,
            current_user() as username,
            pg_backend_pid() as process_id,
            pg_is_in_recovery() as in_recovery
    ";
    
    let response = db.query(query).await.unwrap();
    assert_eq!(response.rows.len(), 1);
    
    // Verify all fields are present and non-null
    let row = &response.rows[0];
    assert_eq!(row.len(), 6); // 6 columns
    
    for (i, field) in row.iter().enumerate() {
        assert!(field.is_some(), "Field {} should not be null", i);
    }
    
    // Verify specific values
    let version = std::str::from_utf8(row[0].as_ref().unwrap()).unwrap();
    assert!(version.starts_with("PostgreSQL"));
    
    let database = std::str::from_utf8(row[1].as_ref().unwrap()).unwrap();
    assert_eq!(database, "main");
    
    let schema = std::str::from_utf8(row[2].as_ref().unwrap()).unwrap();
    assert_eq!(schema, "public");
    
    let username = std::str::from_utf8(row[3].as_ref().unwrap()).unwrap();
    assert_eq!(username, "postgres");
    
    let in_recovery = std::str::from_utf8(row[5].as_ref().unwrap()).unwrap();
    assert_eq!(in_recovery, "0");
    
    println!("✅ Combined system functions query works correctly!");
}

#[tokio::test]
async fn test_postgresql_compatibility_functions() {
    // Test additional PostgreSQL compatibility functions
    let db = DbHandler::new_for_test(":memory:").unwrap();
    
    // Test session_user() - should be same as current_user()
    let response = db.query("SELECT session_user()").await.unwrap();
    assert_eq!(response.rows.len(), 1);
    let session_user_bytes = &response.rows[0][0];
    let session_user = std::str::from_utf8(session_user_bytes.as_ref().unwrap()).unwrap();
    assert_eq!(session_user, "postgres");
    
    // Test pg_database_size() function
    let response = db.query("SELECT pg_database_size('main')").await.unwrap();
    assert_eq!(response.rows.len(), 1);
    let size_bytes = &response.rows[0][0];
    assert!(size_bytes.is_some());
    let size_str = std::str::from_utf8(size_bytes.as_ref().unwrap()).unwrap();
    let size: i64 = size_str.parse().unwrap();
    assert!(size > 0); // Should return a positive size
    
    // Test timestamp functions - these should return valid timestamps
    let response = db.query("SELECT pg_postmaster_start_time(), pg_conf_load_time()").await.unwrap();
    assert_eq!(response.rows.len(), 1);
    
    let start_time_bytes = &response.rows[0][0];
    let start_time = std::str::from_utf8(start_time_bytes.as_ref().unwrap()).unwrap();
    assert!(start_time.len() > 10); // Should be a valid timestamp string
    
    let conf_time_bytes = &response.rows[0][1];
    let conf_time = std::str::from_utf8(conf_time_bytes.as_ref().unwrap()).unwrap();
    assert!(conf_time.len() > 10); // Should be a valid timestamp string
    
    // Test pg_has_role() function
    let response = db.query("SELECT pg_has_role('postgres', 'superuser', 'USAGE')").await.unwrap();
    assert_eq!(response.rows.len(), 1);
    let has_role_bytes = &response.rows[0][0];
    let has_role = std::str::from_utf8(has_role_bytes.as_ref().unwrap()).unwrap();
    assert_eq!(has_role, "1"); // Should return true (1)
    
    println!("✅ PostgreSQL compatibility functions work correctly!");
}