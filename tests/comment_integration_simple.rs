use pgsqlite::ddl::CommentDdlHandler;
use pgsqlite::functions::comment_functions::CommentFunctionHandler;
use pgsqlite::metadata::ObjectResolver;
use pgsqlite::query::query_type_detection::{QueryType, QueryTypeDetector};
use rusqlite::Connection;

#[test]
fn test_complete_comment_system_simple() {
    let mut conn = Connection::open_in_memory().unwrap();
    
    // Create comments table (simulate migration)
    conn.execute(
        "CREATE TABLE __pgsqlite_comments (
            object_oid INTEGER NOT NULL,
            catalog_name TEXT NOT NULL,
            subobject_id INTEGER DEFAULT 0,
            comment_text TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (object_oid, catalog_name, subobject_id)
        )",
        [],
    ).unwrap();
    
    // Create test table
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)", []).unwrap();
    
    // Test 1: Query type detection
    assert_eq!(QueryTypeDetector::detect_query_type("COMMENT ON TABLE users IS 'User table'"), QueryType::Comment);
    assert_eq!(QueryTypeDetector::detect_query_type("comment on column users.name is 'Name field'"), QueryType::Comment);
    assert!(QueryTypeDetector::is_ddl("COMMENT ON TABLE users IS 'User table'"));
    
    // Test 2: Set table comment
    CommentDdlHandler::handle_comment_ddl(
        &mut conn,
        "COMMENT ON TABLE users IS 'This stores user information'"
    ).unwrap();
    
    // Test 3: Set column comments
    CommentDdlHandler::handle_comment_ddl(
        &mut conn,
        "COMMENT ON COLUMN users.name IS 'Full name of the user'"
    ).unwrap();
    
    CommentDdlHandler::handle_comment_ddl(
        &mut conn,
        "COMMENT ON COLUMN users.email IS 'User email address'"
    ).unwrap();
    
    // Test 4: Verify comments are stored correctly
    let table_oid = ObjectResolver::resolve_table_oid("users");
    let (_, name_col_num) = ObjectResolver::resolve_column_oid(&conn, "users", "name").unwrap();
    let (_, email_col_num) = ObjectResolver::resolve_column_oid(&conn, "users", "email").unwrap();
    
    // Check table comment
    let table_comment = CommentDdlHandler::get_comment(&conn, table_oid, "pg_class", 0).unwrap();
    assert_eq!(table_comment, Some("This stores user information".to_string()));
    
    // Check column comments
    let name_comment = CommentDdlHandler::get_comment(&conn, table_oid, "pg_class", name_col_num).unwrap();
    assert_eq!(name_comment, Some("Full name of the user".to_string()));
    
    let email_comment = CommentDdlHandler::get_comment(&conn, table_oid, "pg_class", email_col_num).unwrap();
    assert_eq!(email_comment, Some("User email address".to_string()));
    
    // Test 5: Comment function processing
    let query = format!(
        "SELECT c.relname, obj_description({}, 'pg_class'), col_description({}, {}) FROM pg_class c",
        table_oid, table_oid, name_col_num
    );
    
    let processed = CommentFunctionHandler::process_comment_functions(&conn, &query).unwrap();
    let expected = "SELECT c.relname, 'This stores user information', 'Full name of the user' FROM pg_class c";
    assert_eq!(processed, expected);
    
    // Test 6: Comment removal
    CommentDdlHandler::handle_comment_ddl(
        &mut conn,
        "COMMENT ON TABLE users IS NULL"
    ).unwrap();
    
    let table_comment = CommentDdlHandler::get_comment(&conn, table_oid, "pg_class", 0).unwrap();
    assert_eq!(table_comment, None);
    
    // Test 7: Quote escaping
    CommentDdlHandler::handle_comment_ddl(
        &mut conn,
        "COMMENT ON TABLE users IS 'User''s information table with quotes'"
    ).unwrap();
    
    let query = format!("SELECT obj_description({}, 'pg_class')", table_oid);
    let processed = CommentFunctionHandler::process_comment_functions(&conn, &query).unwrap();
    
    // Debug: Check what was actually stored
    let stored_comment = CommentDdlHandler::get_comment(&conn, table_oid, "pg_class", 0).unwrap();
    println!("DEBUG: Stored comment: {:?}", stored_comment);
    println!("DEBUG: Query: {}", query);
    println!("DEBUG: Processed: {}", processed);
    
    assert_eq!(processed, "SELECT 'User''s information table with quotes'");
    
    // Test 8: Error handling for non-existent objects
    let result = CommentDdlHandler::handle_comment_ddl(
        &mut conn,
        "COMMENT ON TABLE nonexistent IS 'Should fail'"
    );
    assert!(result.is_err());
    
    let result = CommentDdlHandler::handle_comment_ddl(
        &mut conn,
        "COMMENT ON COLUMN users.nonexistent IS 'Should fail'"
    );
    assert!(result.is_err());
    
    println!("✅ All comment system integration tests passed!");
}

#[test]
fn test_comment_function_detection() {
    // Test comment function detection with variables (should detect but not process)
    assert!(CommentFunctionHandler::contains_comment_functions(
        "SELECT c.relname, obj_description(c.oid, 'pg_class') FROM pg_class c"
    ));
    
    assert!(CommentFunctionHandler::contains_comment_functions(
        "SELECT col_description(t.oid, a.attnum) FROM pg_attribute a JOIN pg_class t ON t.oid = a.attrelid"
    ));
    
    assert!(CommentFunctionHandler::contains_comment_functions(
        "SELECT obj_description(123, 'pg_class')"  
    ));
    
    assert!(!CommentFunctionHandler::contains_comment_functions(
        "SELECT * FROM users"
    ));
    
    println!("✅ Comment function detection tests passed!");
}

#[test]
fn test_comment_regex_patterns() {
    // Test DDL detection
    assert!(CommentDdlHandler::is_comment_ddl("COMMENT ON TABLE users IS 'test'"));
    assert!(CommentDdlHandler::is_comment_ddl("comment on column users.name is 'test'"));
    assert!(CommentDdlHandler::is_comment_ddl("COMMENT ON FUNCTION foo() IS NULL"));
    
    assert!(!CommentDdlHandler::is_comment_ddl("SELECT * FROM users"));
    assert!(!CommentDdlHandler::is_comment_ddl("CREATE TABLE test (id INT)"));
    
    println!("✅ Comment regex pattern tests passed!");
}

#[test]
fn test_object_resolver_integration() {
    let conn = Connection::open_in_memory().unwrap();
    
    // Create test table
    conn.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, email TEXT)", []).unwrap();
    
    // Test OID generation consistency
    let oid1 = ObjectResolver::resolve_table_oid("test_table");
    let oid2 = ObjectResolver::resolve_table_oid("test_table");
    assert_eq!(oid1, oid2);
    
    // Test column resolution
    let (table_oid, col_num) = ObjectResolver::resolve_column_oid(&conn, "test_table", "id").unwrap();
    assert_eq!(table_oid, oid1);
    assert_eq!(col_num, 1); // First column (1-based)
    
    let (_, col_num2) = ObjectResolver::resolve_column_oid(&conn, "test_table", "name").unwrap();
    assert_eq!(col_num2, 2); // Second column
    
    let (_, col_num3) = ObjectResolver::resolve_column_oid(&conn, "test_table", "email").unwrap();
    assert_eq!(col_num3, 3); // Third column
    
    // Test table existence
    assert!(ObjectResolver::table_exists(&conn, "test_table").unwrap());
    assert!(!ObjectResolver::table_exists(&conn, "nonexistent").unwrap());
    
    println!("✅ Object resolver integration tests passed!");
}