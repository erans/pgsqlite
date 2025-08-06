use pgsqlite::catalog::CatalogInterceptor;
use pgsqlite::session::db_handler::DbHandler;
use pgsqlite::session::SessionState;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn test_catalog_interceptor() {
    // Create a test database handler
    let db = Arc::new(DbHandler::new(":memory:").unwrap());
    
    // Test simple pg_type query
    let query = "SELECT oid, typname FROM pg_catalog.pg_type WHERE oid = 25";
    let result = CatalogInterceptor::intercept_query(query, db.clone(), None).await;
    assert!(result.is_some());
    
    let response = result.unwrap().unwrap();
    assert_eq!(response.columns, vec!["oid", "typname"]);
    assert_eq!(response.rows.len(), 1);
    assert_eq!(response.rows[0][0], Some("25".as_bytes().to_vec()));
    assert_eq!(response.rows[0][1], Some("text".as_bytes().to_vec()));
    
    // Test pg_type query with parameter placeholder
    let query = "SELECT oid, typname FROM pg_catalog.pg_type WHERE oid = $1";
    let result = CatalogInterceptor::intercept_query(query, db.clone(), None).await;
    assert!(result.is_some());
    
    let response = result.unwrap().unwrap();
    assert_eq!(response.columns, vec!["oid", "typname"]);
    assert_eq!(response.rows.len(), 36); // Should return all types (18 basic + 18 array types)
    
    // Test complex JOIN query
    let query = "SELECT t.typname, t.typtype, n.nspname 
                 FROM pg_catalog.pg_type t 
                 INNER JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid 
                 WHERE t.oid = $1";
    let result = CatalogInterceptor::intercept_query(query, db.clone(), None).await;
    assert!(result.is_some());
    
    let response = result.unwrap().unwrap();
    assert_eq!(response.columns, vec!["typname", "typtype", "nspname"]);
    
    // Test non-catalog query
    let query = "SELECT * FROM users";
    let result = CatalogInterceptor::intercept_query(query, db.clone(), None).await;
    assert!(result.is_none());
}

#[tokio::test]
async fn test_catalog_with_joins() {
    // Create a test database handler
    let db = Arc::new(DbHandler::new(":memory:").unwrap());
    
    let query = "SELECT t.typname, t.typtype, t.typelem, r.rngsubtype, t.typbasetype, n.nspname, t.typrelid
                 FROM pg_catalog.pg_type t
                 LEFT OUTER JOIN pg_catalog.pg_range r ON r.rngtypid = t.oid
                 INNER JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
                 WHERE t.oid = $1";
    
    let result = CatalogInterceptor::intercept_query(query, db.clone(), None).await;
    assert!(result.is_some());
    
    let response = result.unwrap().unwrap();
    assert_eq!(response.columns.len(), 7);
    assert_eq!(response.columns[0], "typname");
    assert_eq!(response.columns[1], "typtype");
    assert_eq!(response.columns[2], "typelem");
    assert_eq!(response.columns[3], "rngsubtype");
    assert_eq!(response.columns[4], "typbasetype");
    assert_eq!(response.columns[5], "nspname");
    assert_eq!(response.columns[6], "typrelid");
    
    // Should return all types since we can't filter by parameter
    assert!(!response.rows.is_empty());
}

#[tokio::test]
async fn test_pg_class_queries() {
    // Create a test database handler with temporary file
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let db_path = format!("/tmp/catalog_test_pg_class_{timestamp}.db");
    let db = Arc::new(DbHandler::new(&db_path).unwrap());
    
    // Create a session
    let session = Arc::new(SessionState::new("test_user".to_string(), "test_db".to_string()));
    
    // Create a connection for the session
    db.create_session_connection(session.id).await.unwrap();
    
    // Create a test table in the session
    db.execute_with_session("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)", &session.id).await.unwrap();
    
    // Test pg_class query
    let query = "SELECT relname, relkind FROM pg_catalog.pg_class";
    let result = CatalogInterceptor::intercept_query(query, db.clone(), Some(session.clone())).await;
    assert!(result.is_some());
    
    let response = result.unwrap().unwrap();
    // Now we properly implement column projection
    assert_eq!(response.columns, vec!["relname", "relkind"]);
    assert_eq!(response.columns.len(), 2);
    
    // Find our test table
    let mut found_table = false;
    for row in &response.rows {
        assert_eq!(row.len(), 2, "Should only have 2 columns");
        if let Some(Some(name_bytes)) = row.first() { // relname is at index 0 now
            let name = String::from_utf8_lossy(name_bytes);
            if name == "test_table" {
                found_table = true;
                // Check relkind is 'r' for regular table
                if let Some(Some(kind_bytes)) = row.get(1) { // relkind is at index 1 now
                    assert_eq!(kind_bytes, b"r");
                }
            }
        }
    }
    assert!(found_table, "test_table should be in pg_class");
    
    // Cleanup
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
}

#[tokio::test]
async fn test_pg_attribute_queries() {
    // Create a test database handler with temporary file
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let db_path = format!("/tmp/catalog_test_pg_attr_{timestamp}.db");
    let db = Arc::new(DbHandler::new(&db_path).unwrap());
    
    // Create a session
    let session = Arc::new(SessionState::new("test_user".to_string(), "test_db".to_string()));
    
    // Create a connection for the session
    db.create_session_connection(session.id).await.unwrap();
    
    // Create a test table in the session
    db.execute_with_session("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)", &session.id).await.unwrap();
    
    // Test pg_attribute query
    let query = "SELECT attname, atttypid, attnotnull FROM pg_catalog.pg_attribute";
    let result = CatalogInterceptor::intercept_query(query, db.clone(), Some(session.clone())).await;
    assert!(result.is_some());
    
    let response = result.unwrap().unwrap();
    // With column projection, we now only get the requested columns
    assert_eq!(response.columns, vec!["attname", "atttypid", "attnotnull"]);
    
    // Count columns for test_table
    let mut column_count = 0;
    let mut found_id = false;
    let mut found_name = false;
    
    for row in &response.rows {
        if let Some(Some(name_bytes)) = row.first() { // attname is at index 0 (first selected column)
            let col_name = String::from_utf8_lossy(name_bytes);
            if col_name == "id" {
                found_id = true;
                column_count += 1;
            } else if col_name == "name" {
                found_name = true;
                column_count += 1;
                // Check NOT NULL constraint
                if let Some(Some(notnull_bytes)) = row.get(2) { // attnotnull is at index 2 (third selected column)
                    assert_eq!(notnull_bytes, b"t");
                }
            } else if col_name == "created_at" {
                column_count += 1;
            }
        }
    }
    
    assert!(found_id, "id column should be in pg_attribute");
    assert!(found_name, "name column should be in pg_attribute");
    assert!(column_count >= 3, "Should have at least 3 columns for test_table");
    
    // Cleanup
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
}