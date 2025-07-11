use rusqlite::Connection;
use pgsqlite::metadata::TypeMetadata;
use pgsqlite::translator::CreateTableTranslator;
use pgsqlite::validator::{NumericConstraintValidator, NumericTriggers};

#[test]
fn test_numeric_constraint_migration() {
    let conn = Connection::open_in_memory().unwrap();
    
    // Initialize metadata
    TypeMetadata::init(&conn).unwrap();
    
    // Run migrations to get the type_modifier column
    let _ = conn.execute(
        "ALTER TABLE __pgsqlite_schema ADD COLUMN type_modifier INTEGER",
        []
    );
    
    // Create numeric constraints table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS __pgsqlite_numeric_constraints (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            precision INTEGER NOT NULL,
            scale INTEGER NOT NULL,
            PRIMARY KEY (table_name, column_name)
        )",
        []
    ).unwrap();
    
    // Verify table was created
    let table_exists: i32 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='__pgsqlite_numeric_constraints'",
        [],
        |row| row.get(0)
    ).unwrap();
    assert_eq!(table_exists, 1);
    
    // Test storing constraints
    conn.execute(
        "INSERT INTO __pgsqlite_numeric_constraints (table_name, column_name, precision, scale)
         VALUES ('products', 'price', 10, 2), ('products', 'tax_rate', 5, 4)",
        []
    ).unwrap();
    
    // Verify constraints were stored
    let count: i32 = conn.query_row(
        "SELECT COUNT(*) FROM __pgsqlite_numeric_constraints",
        [],
        |row| row.get(0)
    ).unwrap();
    assert_eq!(count, 2);
}

#[test]
fn test_create_table_translator_numeric_parsing() {
    let create_sql = "CREATE TABLE test_numeric (
        id SERIAL PRIMARY KEY,
        price NUMERIC(10,2),
        quantity NUMERIC(5,0),
        rate DECIMAL(5,3),
        plain_numeric NUMERIC
    )";
    
    let (translated_sql, type_mappings) = CreateTableTranslator::translate(create_sql).unwrap();
    
    // Verify NUMERIC types are translated to DECIMAL for SQLite
    assert!(translated_sql.contains("DECIMAL"));
    
    // Check type modifiers are extracted correctly
    let price_mapping = type_mappings.get("test_numeric.price").unwrap();
    assert_eq!(price_mapping.pg_type, "NUMERIC(10,2)");
    assert_eq!(price_mapping.sqlite_type, "DECIMAL");
    
    // Decode type modifier for price (10,2)
    let modifier = price_mapping.type_modifier.unwrap();
    let tmp_typmod = modifier - 4;
    let precision = (tmp_typmod >> 16) & 0xFFFF;
    let scale = tmp_typmod & 0xFFFF;
    assert_eq!(precision, 10);
    assert_eq!(scale, 2);
    
    // Check quantity (5,0)
    let quantity_mapping = type_mappings.get("test_numeric.quantity").unwrap();
    let modifier = quantity_mapping.type_modifier.unwrap();
    let tmp_typmod = modifier - 4;
    let precision = (tmp_typmod >> 16) & 0xFFFF;
    let scale = tmp_typmod & 0xFFFF;
    assert_eq!(precision, 5);
    assert_eq!(scale, 0);
    
    // Check plain numeric has no modifier
    let plain_mapping = type_mappings.get("test_numeric.plain_numeric").unwrap();
    assert!(plain_mapping.type_modifier.is_none());
}

#[test]
fn test_numeric_constraint_validator() {
    let conn = Connection::open_in_memory().unwrap();
    
    // Setup constraints table
    conn.execute(
        "CREATE TABLE __pgsqlite_numeric_constraints (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            precision INTEGER NOT NULL,
            scale INTEGER NOT NULL,
            PRIMARY KEY (table_name, column_name)
        )",
        []
    ).unwrap();
    
    // Add test constraints
    conn.execute(
        "INSERT INTO __pgsqlite_numeric_constraints VALUES ('test', 'amount', 10, 2)",
        []
    ).unwrap();
    
    let validator = NumericConstraintValidator::new();
    validator.load_table_constraints(&conn, "test").unwrap();
    
    // Test valid values
    assert!(validator.validate_value("test", "amount", "123.45").is_ok());
    assert!(validator.validate_value("test", "amount", "0.99").is_ok());
    assert!(validator.validate_value("test", "amount", "99999999.99").is_ok());
    assert!(validator.validate_value("test", "amount", "").is_ok()); // NULL
    
    // Test invalid precision (too many total digits)
    assert!(validator.validate_value("test", "amount", "99999999.999").is_err());
    
    // Test invalid scale (too many decimal places)
    assert!(validator.validate_value("test", "amount", "123.456").is_err());
}

#[test]
fn test_numeric_formatting() {
    let conn = Connection::open_in_memory().unwrap();
    
    // Setup constraints table
    conn.execute(
        "CREATE TABLE __pgsqlite_numeric_constraints (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            precision INTEGER NOT NULL,
            scale INTEGER NOT NULL,
            PRIMARY KEY (table_name, column_name)
        )",
        []
    ).unwrap();
    
    // Add constraints with different scales
    conn.execute(
        "INSERT INTO __pgsqlite_numeric_constraints VALUES 
         ('test', 'two_dec', 10, 2),
         ('test', 'four_dec', 10, 4),
         ('test', 'no_dec', 10, 0)",
        []
    ).unwrap();
    
    let validator = NumericConstraintValidator::new();
    validator.load_table_constraints(&conn, "test").unwrap();
    
    // Test formatting
    assert_eq!(validator.format_value("test", "two_dec", "123"), "123.00");
    assert_eq!(validator.format_value("test", "two_dec", "123.4"), "123.40");
    assert_eq!(validator.format_value("test", "four_dec", "123"), "123.0000");
    assert_eq!(validator.format_value("test", "four_dec", "123.456789"), "123.4568"); // Rounded
    assert_eq!(validator.format_value("test", "no_dec", "123.99"), "124"); // Rounded
    
    // NULL values pass through
    assert_eq!(validator.format_value("test", "two_dec", ""), "");
}

#[test]
fn test_numeric_triggers() {
    let conn = Connection::open_in_memory().unwrap();
    
    // Create test table
    conn.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, price DECIMAL)", []).unwrap();
    
    // Create numeric validation triggers
    NumericTriggers::create_numeric_validation_triggers(&conn, "products", "price", 10, 2).unwrap();
    
    // Test that triggers were created
    assert!(NumericTriggers::has_numeric_triggers(&conn, "products", "price").unwrap());
    
    // Valid insert should work
    conn.execute("INSERT INTO products (price) VALUES (99.99)", []).unwrap();
    conn.execute("INSERT INTO products (price) VALUES (0.01)", []).unwrap();
    
    // Invalid inserts should fail
    let result = conn.execute("INSERT INTO products (price) VALUES (99999999.999)", []);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("numeric field overflow"));
    
    // Too many decimal places
    let result = conn.execute("INSERT INTO products (price) VALUES (123.456)", []);
    assert!(result.is_err());
    
    // Test updates
    conn.execute("UPDATE products SET price = 88.88 WHERE id = 1", []).unwrap();
    
    // Invalid update
    let result = conn.execute("UPDATE products SET price = 123.456 WHERE id = 1", []);
    assert!(result.is_err());
    
    // Drop triggers
    NumericTriggers::drop_numeric_validation_triggers(&conn, "products", "price").unwrap();
    assert!(!NumericTriggers::has_numeric_triggers(&conn, "products", "price").unwrap());
}