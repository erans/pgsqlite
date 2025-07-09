use rusqlite::Connection;
use pgsqlite::schema_drift::SchemaDriftDetector;
use pgsqlite::metadata::TypeMetadata;

#[test]
fn test_table_exists_in_metadata_but_not_sqlite() -> rusqlite::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    
    // Initialize metadata table
    TypeMetadata::init(&conn)?;
    
    // Don't create any table, but add metadata
    let tx = conn.transaction()?;
    tx.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type)
         VALUES 
         ('phantom_table', 'id', 'int4', 'INTEGER'),
         ('phantom_table', 'name', 'text', 'TEXT')",
        []
    )?;
    tx.commit()?;
    
    // Should detect drift - PRAGMA table_info will fail for non-existent table
    let result = SchemaDriftDetector::detect_drift(&conn);
    assert!(result.is_err() || !result.unwrap().is_empty());
    
    Ok(())
}

#[test]
fn test_nullable_constraints_ignored() -> rusqlite::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    
    // Initialize metadata table
    TypeMetadata::init(&conn)?;
    
    // Create table with NOT NULL constraint
    conn.execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT
        )",
        []
    )?;
    
    // Metadata doesn't track nullable constraints
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
    
    // Should detect no drift (nullable is not checked)
    let drift = SchemaDriftDetector::detect_drift(&conn).unwrap();
    assert!(drift.is_empty());
    
    Ok(())
}

#[test]
fn test_case_sensitivity_in_type_names() -> rusqlite::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    
    // Initialize metadata table
    TypeMetadata::init(&conn)?;
    
    // Create table with mixed case types
    conn.execute(
        "CREATE TABLE test_case (
            id integer PRIMARY KEY,
            name text,
            amount REAL
        )",
        []
    )?;
    
    // Store metadata with different case
    let tx = conn.transaction()?;
    tx.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type)
         VALUES 
         ('test_case', 'id', 'int4', 'INTEGER'),
         ('test_case', 'name', 'text', 'TEXT'),
         ('test_case', 'amount', 'float8', 'real')",
        []
    )?;
    tx.commit()?;
    
    // Should detect no drift (case insensitive comparison)
    let drift = SchemaDriftDetector::detect_drift(&conn).unwrap();
    assert!(drift.is_empty());
    
    Ok(())
}

#[test]
fn test_decimal_type_variations() -> rusqlite::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    
    // Initialize metadata table
    TypeMetadata::init(&conn)?;
    
    // Create table with NUMERIC type
    conn.execute(
        "CREATE TABLE finances (
            id INTEGER PRIMARY KEY,
            amount NUMERIC(10,2),
            total DECIMAL
        )",
        []
    )?;
    
    // Store metadata with DECIMAL type
    let tx = conn.transaction()?;
    tx.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type)
         VALUES 
         ('finances', 'id', 'int4', 'INTEGER'),
         ('finances', 'amount', 'numeric', 'DECIMAL'),
         ('finances', 'total', 'numeric', 'DECIMAL')",
        []
    )?;
    tx.commit()?;
    
    // Should detect no drift (NUMERIC and DECIMAL normalize to DECIMAL)
    let drift = SchemaDriftDetector::detect_drift(&conn).unwrap();
    assert!(drift.is_empty());
    
    Ok(())
}

#[test]
fn test_column_order_doesnt_matter() -> rusqlite::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    
    // Initialize metadata table
    TypeMetadata::init(&conn)?;
    
    // Create table
    conn.execute(
        "CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT,
            price REAL,
            stock INTEGER
        )",
        []
    )?;
    
    // Store metadata in different order
    let tx = conn.transaction()?;
    tx.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type)
         VALUES 
         ('products', 'stock', 'int4', 'INTEGER'),
         ('products', 'id', 'int4', 'INTEGER'),
         ('products', 'price', 'float8', 'REAL'),
         ('products', 'name', 'text', 'TEXT')",
        []
    )?;
    tx.commit()?;
    
    // Should detect no drift (order doesn't matter)
    let drift = SchemaDriftDetector::detect_drift(&conn).unwrap();
    assert!(drift.is_empty());
    
    Ok(())
}

#[test]
fn test_complex_drift_scenario() -> rusqlite::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    
    // Initialize metadata table
    TypeMetadata::init(&conn)?;
    
    // Create table with some columns
    conn.execute(
        "CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_name TEXT,
            amount REAL,
            status TEXT
        )",
        []
    )?;
    
    // Store metadata with:
    // - Missing column (created_at)
    // - Extra column (status not in metadata)
    // - Type mismatch (amount as TEXT instead of REAL)
    let tx = conn.transaction()?;
    tx.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type)
         VALUES 
         ('orders', 'id', 'int4', 'INTEGER'),
         ('orders', 'customer_name', 'text', 'TEXT'),
         ('orders', 'amount', 'text', 'TEXT'),
         ('orders', 'created_at', 'timestamp', 'INTEGER')",
        []
    )?;
    tx.commit()?;
    
    // Should detect multiple drift types
    let drift = SchemaDriftDetector::detect_drift(&conn).unwrap();
    assert!(!drift.is_empty());
    
    let table_drift = &drift.table_drifts[0];
    assert_eq!(table_drift.table_name, "orders");
    assert_eq!(table_drift.missing_in_sqlite.len(), 1);
    assert_eq!(table_drift.missing_in_sqlite[0].name, "created_at");
    assert_eq!(table_drift.missing_in_metadata.len(), 1);
    assert_eq!(table_drift.missing_in_metadata[0].name, "status");
    assert_eq!(table_drift.type_mismatches.len(), 1);
    assert_eq!(table_drift.type_mismatches[0].column_name, "amount");
    
    Ok(())
}

#[test]
fn test_empty_metadata_table() -> rusqlite::Result<()> {
    let conn = Connection::open_in_memory()?;
    
    // Initialize metadata table
    TypeMetadata::init(&conn)?;
    
    // Create a table but don't add any metadata
    conn.execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT
        )",
        []
    )?;
    
    // Should detect no drift (no metadata = no drift check)
    let drift = SchemaDriftDetector::detect_drift(&conn).unwrap();
    assert!(drift.is_empty());
    
    Ok(())
}

#[test]
fn test_datetime_type_drift() -> rusqlite::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    
    // Initialize metadata table
    TypeMetadata::init(&conn)?;
    
    // Create table with datetime columns
    conn.execute(
        "CREATE TABLE events (
            id INTEGER PRIMARY KEY,
            event_date INTEGER,
            event_time INTEGER,
            created_at INTEGER
        )",
        []
    )?;
    
    // Store metadata with mismatched types
    let tx = conn.transaction()?;
    tx.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type)
         VALUES 
         ('events', 'id', 'int4', 'INTEGER'),
         ('events', 'event_date', 'date', 'TEXT'),
         ('events', 'event_time', 'time', 'TEXT'),
         ('events', 'created_at', 'timestamp', 'INTEGER')",
        []
    )?;
    tx.commit()?;
    
    // Should detect drift for date/time columns with wrong storage type
    let drift = SchemaDriftDetector::detect_drift(&conn).unwrap();
    assert!(!drift.is_empty());
    
    let table_drift = &drift.table_drifts[0];
    assert_eq!(table_drift.type_mismatches.len(), 2);
    
    Ok(())
}

#[test]
fn test_parametric_types_normalization() -> rusqlite::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    
    // Initialize metadata table
    TypeMetadata::init(&conn)?;
    
    // Create table with parametric types
    conn.execute(
        "CREATE TABLE test_params (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            code CHAR(10),
            amount NUMERIC(15,2)
        )",
        []
    )?;
    
    // Store metadata without parameters
    let tx = conn.transaction()?;
    tx.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type)
         VALUES 
         ('test_params', 'id', 'int4', 'INTEGER'),
         ('test_params', 'name', 'varchar', 'TEXT'),
         ('test_params', 'code', 'char', 'TEXT'),
         ('test_params', 'amount', 'numeric', 'DECIMAL')",
        []
    )?;
    tx.commit()?;
    
    // Should detect no drift (parameters are ignored)
    let drift = SchemaDriftDetector::detect_drift(&conn).unwrap();
    assert!(drift.is_empty());
    
    Ok(())
}