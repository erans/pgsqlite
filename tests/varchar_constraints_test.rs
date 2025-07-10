use rusqlite::Connection;
use pgsqlite::metadata::TypeMetadata;

#[test]
fn test_varchar_constraint_migration() {
    let conn = Connection::open_in_memory().unwrap();
    
    // Initialize metadata
    TypeMetadata::init(&conn).unwrap();
    
    // Run migrations to get the type_modifier column
    let _ = conn.execute(
        "ALTER TABLE __pgsqlite_schema ADD COLUMN type_modifier INTEGER",
        []
    );
    
    // Create string constraints table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS __pgsqlite_string_constraints (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            max_length INTEGER NOT NULL,
            is_char_type BOOLEAN NOT NULL DEFAULT 0,
            PRIMARY KEY (table_name, column_name)
        )",
        []
    ).unwrap();
    
    // Verify table was created
    let table_exists: i32 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='__pgsqlite_string_constraints'",
        [],
        |row| row.get(0)
    ).unwrap();
    assert_eq!(table_exists, 1);
    
    // Test storing constraints
    conn.execute(
        "INSERT INTO __pgsqlite_string_constraints (table_name, column_name, max_length, is_char_type)
         VALUES ('users', 'name', 50, 0), ('users', 'code', 10, 1)",
        []
    ).unwrap();
    
    // Verify constraints were stored
    let count: i32 = conn.query_row(
        "SELECT COUNT(*) FROM __pgsqlite_string_constraints",
        [],
        |row| row.get(0)
    ).unwrap();
    assert_eq!(count, 2);
}

#[test]
fn test_type_modifier_storage() {
    let conn = Connection::open_in_memory().unwrap();
    
    // Initialize metadata
    TypeMetadata::init(&conn).unwrap();
    
    // Add type_modifier column
    let _ = conn.execute(
        "ALTER TABLE __pgsqlite_schema ADD COLUMN type_modifier INTEGER",
        []
    );
    
    // Store type mapping with modifier
    conn.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type, type_modifier)
         VALUES ('test', 'name', 'varchar(50)', 'TEXT', 50)",
        []
    ).unwrap();
    
    // Verify it was stored
    let modifier: Option<i32> = conn.query_row(
        "SELECT type_modifier FROM __pgsqlite_schema WHERE table_name = 'test' AND column_name = 'name'",
        [],
        |row| row.get(0)
    ).unwrap();
    assert_eq!(modifier, Some(50));
}

#[test]
fn test_char_vs_varchar_distinction() {
    let conn = Connection::open_in_memory().unwrap();
    
    // Create constraints table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS __pgsqlite_string_constraints (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            max_length INTEGER NOT NULL,
            is_char_type BOOLEAN NOT NULL DEFAULT 0,
            PRIMARY KEY (table_name, column_name)
        )",
        []
    ).unwrap();
    
    // Insert both CHAR and VARCHAR constraints
    conn.execute(
        "INSERT INTO __pgsqlite_string_constraints (table_name, column_name, max_length, is_char_type)
         VALUES 
         ('test', 'varchar_col', 100, 0),
         ('test', 'char_col', 10, 1)",
        []
    ).unwrap();
    
    // Verify CHAR type is marked correctly
    let is_char: bool = conn.query_row(
        "SELECT is_char_type FROM __pgsqlite_string_constraints 
         WHERE table_name = 'test' AND column_name = 'char_col'",
        [],
        |row| row.get(0)
    ).unwrap();
    assert!(is_char);
    
    // Verify VARCHAR type is marked correctly
    let is_char: bool = conn.query_row(
        "SELECT is_char_type FROM __pgsqlite_string_constraints 
         WHERE table_name = 'test' AND column_name = 'varchar_col'",
        [],
        |row| row.get(0)
    ).unwrap();
    assert!(!is_char);
}

#[test]
fn test_constraint_primary_key() {
    let conn = Connection::open_in_memory().unwrap();
    
    // Create constraints table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS __pgsqlite_string_constraints (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            max_length INTEGER NOT NULL,
            is_char_type BOOLEAN NOT NULL DEFAULT 0,
            PRIMARY KEY (table_name, column_name)
        )",
        []
    ).unwrap();
    
    // Insert a constraint
    conn.execute(
        "INSERT INTO __pgsqlite_string_constraints (table_name, column_name, max_length, is_char_type)
         VALUES ('users', 'email', 255, 0)",
        []
    ).unwrap();
    
    // Try to insert duplicate - should fail due to primary key
    let result = conn.execute(
        "INSERT INTO __pgsqlite_string_constraints (table_name, column_name, max_length, is_char_type)
         VALUES ('users', 'email', 100, 0)",
        []
    );
    assert!(result.is_err());
    
    // Verify original constraint is unchanged
    let max_length: i32 = conn.query_row(
        "SELECT max_length FROM __pgsqlite_string_constraints 
         WHERE table_name = 'users' AND column_name = 'email'",
        [],
        |row| row.get(0)
    ).unwrap();
    assert_eq!(max_length, 255);
}

#[test]
fn test_migration_compatibility() {
    let conn = Connection::open_in_memory().unwrap();
    
    // Initialize base metadata
    TypeMetadata::init(&conn).unwrap();
    
    // Verify base schema exists
    let schema_exists: i32 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='__pgsqlite_schema'",
        [],
        |row| row.get(0)
    ).unwrap();
    assert_eq!(schema_exists, 1);
    
    // Add type_modifier column (simulating migration v6)
    let result = conn.execute(
        "ALTER TABLE __pgsqlite_schema ADD COLUMN type_modifier INTEGER",
        []
    );
    
    // Should succeed on first run
    assert!(result.is_ok() || result.unwrap_err().to_string().contains("duplicate column"));
    
    // Verify column exists
    let has_column: i32 = conn.query_row(
        "SELECT COUNT(*) FROM pragma_table_info('__pgsqlite_schema') WHERE name = 'type_modifier'",
        [],
        |row| row.get(0)
    ).unwrap();
    assert_eq!(has_column, 1);
}