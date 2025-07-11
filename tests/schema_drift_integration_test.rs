use pgsqlite::session::DbHandler;
use rusqlite::Connection;
use pgsqlite::metadata::TypeMetadata;

#[test]
fn test_db_handler_fails_on_drift() {
    // Create a temporary database with drift
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    
    // First, create database with correct schema
    {
        let mut conn = Connection::open(&db_path).unwrap();
        
        // Initialize metadata tables
        TypeMetadata::init(&conn).unwrap();
        
        // Create migration metadata table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS __pgsqlite_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                created_at REAL DEFAULT (strftime('%s', 'now')),
                updated_at REAL DEFAULT (strftime('%s', 'now'))
            )",
            []
        ).unwrap();
        
        // Create table
        conn.execute(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT
            )",
            []
        ).unwrap();
        
        // Store matching metadata
        let tx = conn.transaction().unwrap();
        tx.execute(
            "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type)
             VALUES 
             ('users', 'id', 'int4', 'INTEGER'),
             ('users', 'name', 'text', 'TEXT'),
             ('users', 'email', 'text', 'TEXT')",
            []
        ).unwrap();
        tx.commit().unwrap();
        
        // Add version to bypass migration check
        conn.execute(
            "INSERT INTO __pgsqlite_metadata (key, value) VALUES ('schema_version', '7')",
            []
        ).unwrap();
    }
    
    // Now modify the schema directly to create drift
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute("ALTER TABLE users ADD COLUMN phone TEXT", []).unwrap();
    }
    
    // Try to open with DbHandler - should fail due to drift
    let result = DbHandler::new(db_path.to_str().unwrap());
    assert!(result.is_err());
    
    if let Err(e) = result {
        let error_msg = e.to_string();
        assert!(error_msg.contains("Schema drift detected"));
        assert!(error_msg.contains("phone"));
    }
}

#[test]
fn test_db_handler_succeeds_without_drift() {
    // Create a temporary database without drift
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    
    {
        let mut conn = Connection::open(&db_path).unwrap();
        
        // Initialize metadata tables
        TypeMetadata::init(&conn).unwrap();
        
        // Create migration metadata table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS __pgsqlite_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                created_at REAL DEFAULT (strftime('%s', 'now')),
                updated_at REAL DEFAULT (strftime('%s', 'now'))
            )",
            []
        ).unwrap();
        
        // Create table
        conn.execute(
            "CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL
            )",
            []
        ).unwrap();
        
        // Store matching metadata
        let tx = conn.transaction().unwrap();
        tx.execute(
            "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type)
             VALUES 
             ('products', 'id', 'int4', 'INTEGER'),
             ('products', 'name', 'text', 'TEXT'),
             ('products', 'price', 'float8', 'REAL')",
            []
        ).unwrap();
        tx.commit().unwrap();
        
        // Add version to bypass migration check
        conn.execute(
            "INSERT INTO __pgsqlite_metadata (key, value) VALUES ('schema_version', '7')",
            []
        ).unwrap();
    }
    
    // Open with DbHandler - should succeed
    let result = DbHandler::new(db_path.to_str().unwrap());
    assert!(result.is_ok());
}

#[test]
fn test_drift_detection_with_type_mismatch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");
    
    {
        let mut conn = Connection::open(&db_path).unwrap();
        
        // Initialize metadata tables
        TypeMetadata::init(&conn).unwrap();
        
        // Create migration metadata table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS __pgsqlite_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                created_at REAL DEFAULT (strftime('%s', 'now')),
                updated_at REAL DEFAULT (strftime('%s', 'now'))
            )",
            []
        ).unwrap();
        
        // Create table with INTEGER column
        conn.execute(
            "CREATE TABLE stats (
                id INTEGER PRIMARY KEY,
                count INTEGER
            )",
            []
        ).unwrap();
        
        // Store metadata with TEXT type (mismatch)
        let tx = conn.transaction().unwrap();
        tx.execute(
            "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type)
             VALUES 
             ('stats', 'id', 'int4', 'INTEGER'),
             ('stats', 'count', 'text', 'TEXT')",
            []
        ).unwrap();
        tx.commit().unwrap();
        
        // Add version to bypass migration check
        conn.execute(
            "INSERT INTO __pgsqlite_metadata (key, value) VALUES ('schema_version', '7')",
            []
        ).unwrap();
    }
    
    // Try to open with DbHandler - should fail due to type mismatch
    let result = DbHandler::new(db_path.to_str().unwrap());
    assert!(result.is_err());
    
    if let Err(e) = result {
        let error_msg = e.to_string();
        assert!(error_msg.contains("Schema drift detected"));
        assert!(error_msg.contains("Type mismatches"));
        assert!(error_msg.contains("count"));
    }
}

#[test]
fn test_in_memory_db_no_drift_check() {
    // In-memory databases should not check for drift (they're always fresh)
    let result = DbHandler::new(":memory:");
    assert!(result.is_ok());
}