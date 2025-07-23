use pgsqlite::migration::{MigrationRunner, MIGRATIONS};
use rusqlite::Connection;
use tempfile::TempDir;

#[test]
fn test_fresh_database_migration() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    
    // Create a fresh database
    let conn = Connection::open(&db_path).unwrap();
    let runner = MigrationRunner::new(conn);
    
    // Check should fail on fresh database
    let check_result = runner.check_schema_version();
    assert!(check_result.is_err());
    assert!(check_result.unwrap_err().to_string().contains("Database schema is outdated"));
    
    // Now run migrations
    let conn = runner.into_connection();
    let mut runner = MigrationRunner::new(conn);
    let applied = runner.run_pending_migrations().unwrap();
    
    // Should apply all migrations
    assert_eq!(applied.len(), MIGRATIONS.len());
    assert_eq!(applied, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    
    // Verify schema version
    let conn = runner.into_connection();
    let version: String = conn.query_row(
        "SELECT value FROM __pgsqlite_metadata WHERE key = 'schema_version'",
        [],
        |row| row.get(0)
    ).unwrap();
    assert_eq!(version, "9");
    
    // Now check should pass
    let runner2 = MigrationRunner::new(conn);
    assert!(runner2.check_schema_version().is_ok());
    
    // Verify all tables exist
    let conn = runner2.into_connection();
    let tables: Vec<String> = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '__pgsqlite_%' ORDER BY name"
    ).unwrap()
    .query_map([], |row| row.get(0)).unwrap()
    .collect::<Result<Vec<_>, _>>().unwrap();
    
    assert!(tables.contains(&"__pgsqlite_enum_types".to_string()));
    assert!(tables.contains(&"__pgsqlite_enum_usage".to_string()));
    assert!(tables.contains(&"__pgsqlite_enum_values".to_string()));
    assert!(tables.contains(&"__pgsqlite_metadata".to_string()));
    assert!(tables.contains(&"__pgsqlite_migration_locks".to_string()));
    assert!(tables.contains(&"__pgsqlite_migrations".to_string()));
    assert!(tables.contains(&"__pgsqlite_schema".to_string()));
    assert!(tables.contains(&"__pgsqlite_array_types".to_string()));
    assert!(tables.contains(&"__pgsqlite_fts_metadata".to_string()));
    assert!(tables.contains(&"__pgsqlite_type_map".to_string()));
}

#[test]
fn test_idempotent_migrations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    
    // First run
    let conn = Connection::open(&db_path).unwrap();
    let mut runner = MigrationRunner::new(conn);
    let applied = runner.run_pending_migrations().unwrap();
    assert_eq!(applied.len(), 9);
    drop(runner);
    
    // Second run - should apply nothing
    let conn = Connection::open(&db_path).unwrap();
    let mut runner = MigrationRunner::new(conn);
    let applied = runner.run_pending_migrations().unwrap();
    assert_eq!(applied.len(), 0);
}

#[test]
fn test_existing_schema_detection() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    
    // Create a database with the old schema (pre-migration)
    let conn = Connection::open(&db_path).unwrap();
    conn.execute(
        "CREATE TABLE __pgsqlite_schema (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            pg_type TEXT NOT NULL,
            sqlite_type TEXT NOT NULL,
            PRIMARY KEY (table_name, column_name)
        )",
        []
    ).unwrap();
    drop(conn);
    
    // Check should fail on pre-migration database
    let conn = Connection::open(&db_path).unwrap();
    let runner = MigrationRunner::new(conn);
    let check_result = runner.check_schema_version();
    assert!(check_result.is_err());
    assert!(check_result.unwrap_err().to_string().contains("Database schema is outdated"));
    
    // Run migrations
    let conn = runner.into_connection();
    let mut runner = MigrationRunner::new(conn);
    let applied = runner.run_pending_migrations().unwrap();
    
    // Should recognize existing schema as version 1 and only apply version 2, 3, 4, 5, 6, 7, 8, and 9
    assert_eq!(applied.len(), 8);
    assert_eq!(applied[0], 2);
    assert_eq!(applied[1], 3);
    assert_eq!(applied[2], 4);
    assert_eq!(applied[3], 5);
    assert_eq!(applied[4], 6);
    assert_eq!(applied[5], 7);
    assert_eq!(applied[6], 8);
    assert_eq!(applied[7], 9);
    
    // Verify final version
    let conn = runner.into_connection();
    let version: String = conn.query_row(
        "SELECT value FROM __pgsqlite_metadata WHERE key = 'schema_version'",
        [],
        |row| row.get(0)
    ).unwrap();
    assert_eq!(version, "9");
    
    // Now check should pass
    let runner2 = MigrationRunner::new(conn);
    assert!(runner2.check_schema_version().is_ok());
}

#[test]
fn test_migration_history() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    
    let conn = Connection::open(&db_path).unwrap();
    let mut runner = MigrationRunner::new(conn);
    runner.run_pending_migrations().unwrap();
    
    // Check migration history
    let conn = runner.into_connection();
    let migrations: Vec<(i32, String, String)> = conn.prepare(
        "SELECT version, name, status FROM __pgsqlite_migrations ORDER BY version"
    ).unwrap()
    .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
    .unwrap()
    .collect::<Result<Vec<_>, _>>().unwrap();
    
    assert_eq!(migrations.len(), 9);
    assert_eq!(migrations[0], (1, "initial_schema".to_string(), "completed".to_string()));
    assert_eq!(migrations[1], (2, "enum_type_support".to_string(), "completed".to_string()));
    assert_eq!(migrations[2], (3, "datetime_timezone_support".to_string(), "completed".to_string()));
    assert_eq!(migrations[3], (4, "datetime_integer_storage".to_string(), "completed".to_string()));
    assert_eq!(migrations[4], (5, "pg_catalog_tables".to_string(), "completed".to_string()));
    assert_eq!(migrations[5], (6, "varchar_constraints".to_string(), "completed".to_string()));
    assert_eq!(migrations[6], (7, "numeric_constraints".to_string(), "completed".to_string()));
    assert_eq!(migrations[7], (8, "array_support".to_string(), "completed".to_string()));
    assert_eq!(migrations[8], (9, "fts_support".to_string(), "completed".to_string()));
}

#[test] 
fn test_concurrent_migration_lock() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    
    // First runner acquires lock
    let conn1 = Connection::open(&db_path).unwrap();
    let mut runner1 = MigrationRunner::new(conn1);
    
    // Manually acquire lock to simulate long-running migration
    runner1.run_pending_migrations().unwrap();
    let conn = runner1.into_connection();
    
    // Insert a manual lock that hasn't expired
    let now = chrono::Utc::now().timestamp() as f64;
    conn.execute(
        "INSERT OR REPLACE INTO __pgsqlite_migration_locks (id, locked_by, locked_at, expires_at) 
         VALUES (1, 'test-process', ?1, ?2)",
        rusqlite::params![now, now + 300.0]
    ).unwrap();
    drop(conn);
    
    // Second runner should fail to acquire lock
    let conn2 = Connection::open(&db_path).unwrap();
    let mut runner2 = MigrationRunner::new(conn2);
    let result = runner2.run_pending_migrations();
    
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Migration lock held"));
}

#[test]
fn test_check_up_to_date_database() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    
    // Create and migrate database
    let conn = Connection::open(&db_path).unwrap();
    let mut runner = MigrationRunner::new(conn);
    runner.run_pending_migrations().unwrap();
    drop(runner);
    
    // Open database again and check version
    let conn = Connection::open(&db_path).unwrap();
    let runner = MigrationRunner::new(conn);
    
    // Check should pass for up-to-date database
    assert!(runner.check_schema_version().is_ok());
}