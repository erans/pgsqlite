use rusqlite::Connection;
use pgsqlite::schema_drift::SchemaDriftDetector;
use pgsqlite::metadata::TypeMetadata;

#[test]
fn test_no_drift_empty_database() -> rusqlite::Result<()> {
    let conn = Connection::open_in_memory()?;
    
    // Initialize metadata table
    TypeMetadata::init(&conn)?;
    
    // Should detect no drift
    let drift = SchemaDriftDetector::detect_drift(&conn).unwrap();
    assert!(drift.is_empty());
    
    Ok(())
}

#[test]
fn test_no_drift_matching_schema() -> rusqlite::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    
    // Initialize metadata table
    TypeMetadata::init(&conn)?;
    
    // Create a table
    conn.execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT
        )",
        []
    )?;
    
    // Store matching metadata
    let tx = conn.transaction()?;
    tx.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type)
         VALUES 
         ('users', 'id', 'int4', 'INTEGER'),
         ('users', 'name', 'text', 'TEXT'),
         ('users', 'email', 'text', 'TEXT')",
        []
    )?;
    tx.commit()?;
    
    // Should detect no drift
    let drift = SchemaDriftDetector::detect_drift(&conn).unwrap();
    assert!(drift.is_empty());
    
    Ok(())
}

#[test]
fn test_drift_missing_column_in_sqlite() -> rusqlite::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    
    // Initialize metadata table
    TypeMetadata::init(&conn)?;
    
    // Create a table missing a column
    conn.execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )",
        []
    )?;
    
    // Store metadata with an extra column
    let tx = conn.transaction()?;
    tx.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type)
         VALUES 
         ('users', 'id', 'int4', 'INTEGER'),
         ('users', 'name', 'text', 'TEXT'),
         ('users', 'email', 'text', 'TEXT')",
        []
    )?;
    tx.commit()?;
    
    // Should detect drift
    let drift = SchemaDriftDetector::detect_drift(&conn).unwrap();
    assert!(!drift.is_empty());
    assert_eq!(drift.table_drifts.len(), 1);
    
    let table_drift = &drift.table_drifts[0];
    assert_eq!(table_drift.table_name, "users");
    assert_eq!(table_drift.missing_in_sqlite.len(), 1);
    assert_eq!(table_drift.missing_in_sqlite[0].name, "email");
    
    Ok(())
}

#[test]
fn test_drift_missing_column_in_metadata() -> rusqlite::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    
    // Initialize metadata table
    TypeMetadata::init(&conn)?;
    
    // Create a table with extra column
    conn.execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            created_at TIMESTAMP
        )",
        []
    )?;
    
    // Store metadata missing a column
    let tx = conn.transaction()?;
    tx.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type)
         VALUES 
         ('users', 'id', 'int4', 'INTEGER'),
         ('users', 'name', 'text', 'TEXT'),
         ('users', 'email', 'text', 'TEXT')",
        []
    )?;
    tx.commit()?;
    
    // Should detect drift
    let drift = SchemaDriftDetector::detect_drift(&conn).unwrap();
    assert!(!drift.is_empty());
    assert_eq!(drift.table_drifts.len(), 1);
    
    let table_drift = &drift.table_drifts[0];
    assert_eq!(table_drift.table_name, "users");
    assert_eq!(table_drift.missing_in_metadata.len(), 1);
    assert_eq!(table_drift.missing_in_metadata[0].name, "created_at");
    
    Ok(())
}

#[test]
fn test_drift_type_mismatch() -> rusqlite::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    
    // Initialize metadata table
    TypeMetadata::init(&conn)?;
    
    // Create a table
    conn.execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER
        )",
        []
    )?;
    
    // Store metadata with different type
    let tx = conn.transaction()?;
    tx.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type)
         VALUES 
         ('users', 'id', 'int4', 'INTEGER'),
         ('users', 'name', 'text', 'TEXT'),
         ('users', 'age', 'text', 'TEXT')",
        []
    )?;
    tx.commit()?;
    
    // Should detect drift
    let drift = SchemaDriftDetector::detect_drift(&conn).unwrap();
    assert!(!drift.is_empty());
    assert_eq!(drift.table_drifts.len(), 1);
    
    let table_drift = &drift.table_drifts[0];
    assert_eq!(table_drift.table_name, "users");
    assert_eq!(table_drift.type_mismatches.len(), 1);
    assert_eq!(table_drift.type_mismatches[0].column_name, "age");
    assert_eq!(table_drift.type_mismatches[0].metadata_sqlite_type, "TEXT");
    assert_eq!(table_drift.type_mismatches[0].actual_sqlite_type, "INTEGER");
    
    Ok(())
}

#[test]
fn test_drift_multiple_tables() -> rusqlite::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    
    // Initialize metadata table
    TypeMetadata::init(&conn)?;
    
    // Create tables
    conn.execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT
        )",
        []
    )?;
    
    conn.execute(
        "CREATE TABLE posts (
            id INTEGER PRIMARY KEY,
            title TEXT
        )",
        []
    )?;
    
    // Store metadata with drift in both tables
    let tx = conn.transaction()?;
    tx.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type)
         VALUES 
         ('users', 'id', 'int4', 'INTEGER'),
         ('users', 'name', 'text', 'TEXT'),
         ('users', 'email', 'text', 'TEXT'),
         ('posts', 'id', 'int4', 'INTEGER'),
         ('posts', 'title', 'text', 'REAL'),
         ('posts', 'content', 'text', 'TEXT')",
        []
    )?;
    tx.commit()?;
    
    // Should detect drift in both tables
    let drift = SchemaDriftDetector::detect_drift(&conn).unwrap();
    assert!(!drift.is_empty());
    assert_eq!(drift.table_drifts.len(), 2);
    
    // Find users drift
    let users_drift = drift.table_drifts.iter()
        .find(|d| d.table_name == "users")
        .expect("users drift not found");
    assert_eq!(users_drift.missing_in_sqlite.len(), 1);
    assert_eq!(users_drift.missing_in_sqlite[0].name, "email");
    
    // Find posts drift
    let posts_drift = drift.table_drifts.iter()
        .find(|d| d.table_name == "posts")
        .expect("posts drift not found");
    assert_eq!(posts_drift.missing_in_sqlite.len(), 1);
    assert_eq!(posts_drift.missing_in_sqlite[0].name, "content");
    assert_eq!(posts_drift.type_mismatches.len(), 1);
    assert_eq!(posts_drift.type_mismatches[0].column_name, "title");
    
    Ok(())
}

#[test]
fn test_drift_report_formatting() -> rusqlite::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    
    // Initialize metadata table
    TypeMetadata::init(&conn)?;
    
    // Create a table with drift
    conn.execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT
        )",
        []
    )?;
    
    // Store metadata with drift
    let tx = conn.transaction()?;
    tx.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type)
         VALUES 
         ('users', 'id', 'int4', 'INTEGER'),
         ('users', 'name', 'int4', 'INTEGER'),
         ('users', 'email', 'text', 'TEXT')",
        []
    )?;
    tx.commit()?;
    
    // Check report format
    let drift = SchemaDriftDetector::detect_drift(&conn).unwrap();
    let report = drift.format_report();
    
    assert!(report.contains("Table 'users' has schema drift"));
    assert!(report.contains("Columns in metadata but missing from SQLite"));
    assert!(report.contains("email (text)"));
    assert!(report.contains("Type mismatches"));
    assert!(report.contains("name expected SQLite type 'INTEGER' but found 'TEXT'"));
    
    Ok(())
}

#[test]
fn test_type_normalization() -> rusqlite::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    
    // Initialize metadata table
    TypeMetadata::init(&conn)?;
    
    // Create a table with various type variations
    conn.execute(
        "CREATE TABLE test_types (
            id INT PRIMARY KEY,
            big_id BIGINT,
            small_id SMALLINT,
            is_active BOOLEAN,
            price FLOAT,
            description VARCHAR(255),
            data BYTEA
        )",
        []
    )?;
    
    // Store metadata with normalized types
    let tx = conn.transaction()?;
    tx.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type)
         VALUES 
         ('test_types', 'id', 'int4', 'INTEGER'),
         ('test_types', 'big_id', 'int8', 'INTEGER'),
         ('test_types', 'small_id', 'int2', 'INTEGER'),
         ('test_types', 'is_active', 'bool', 'INTEGER'),
         ('test_types', 'price', 'float8', 'REAL'),
         ('test_types', 'description', 'varchar', 'TEXT'),
         ('test_types', 'data', 'bytea', 'BLOB')",
        []
    )?;
    tx.commit()?;
    
    // Should detect no drift despite type variations
    let drift = SchemaDriftDetector::detect_drift(&conn).unwrap();
    assert!(drift.is_empty());
    
    Ok(())
}