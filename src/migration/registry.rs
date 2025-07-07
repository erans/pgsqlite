use super::{Migration, MigrationAction};
use lazy_static::lazy_static;
use std::collections::BTreeMap;

lazy_static! {
    pub static ref MIGRATIONS: BTreeMap<u32, Migration> = {
        let mut registry = BTreeMap::new();
        
        // Register all migrations
        register_v1_initial_schema(&mut registry);
        register_v2_enum_support(&mut registry);
        
        registry
    };
}

/// Version 1: Initial schema
fn register_v1_initial_schema(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(1, Migration {
        version: 1,
        name: "initial_schema",
        description: "Create initial pgsqlite system tables",
        up: MigrationAction::Sql(r#"
            -- Core schema tracking (matching existing structure)
            CREATE TABLE IF NOT EXISTS __pgsqlite_schema (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                pg_type TEXT NOT NULL,
                sqlite_type TEXT NOT NULL,
                PRIMARY KEY (table_name, column_name)
            );
            
            -- System metadata
            CREATE TABLE IF NOT EXISTS __pgsqlite_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                created_at REAL DEFAULT (strftime('%s', 'now')),
                updated_at REAL DEFAULT (strftime('%s', 'now'))
            );
            
            -- Migration tracking
            CREATE TABLE IF NOT EXISTS __pgsqlite_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                applied_at REAL NOT NULL,
                execution_time_ms INTEGER,
                checksum TEXT NOT NULL,
                status TEXT CHECK(status IN ('pending', 'running', 'completed', 'failed', 'rolled_back')),
                error_message TEXT,
                rolled_back_at REAL
            );
            
            -- Migration locks table (prevent concurrent migrations)
            CREATE TABLE IF NOT EXISTS __pgsqlite_migration_locks (
                id INTEGER PRIMARY KEY CHECK (id = 1),  -- Only one row allowed
                locked_by TEXT NOT NULL,  -- Process/connection identifier
                locked_at REAL NOT NULL,
                expires_at REAL NOT NULL  -- Timeout for stale locks
            );
            
            -- Set initial version
            INSERT INTO __pgsqlite_metadata (key, value) VALUES 
                ('schema_version', '1'),
                ('pgsqlite_version', '0.1.0');
        "#),
        down: None,  // Cannot rollback initial schema
        dependencies: vec![],
    });
}

/// Version 2: ENUM support (matching existing schema)
fn register_v2_enum_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(2, Migration {
        version: 2,
        name: "enum_type_support",
        description: "Add PostgreSQL ENUM type support",
        up: MigrationAction::SqlBatch(&[
            r#"
            -- Track ENUM type definitions
            CREATE TABLE IF NOT EXISTS __pgsqlite_enum_types (
                type_oid INTEGER PRIMARY KEY,
                type_name TEXT NOT NULL UNIQUE,
                namespace_oid INTEGER DEFAULT 2200, -- public schema
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            "#,
            r#"
            -- Track ENUM values with ordering
            CREATE TABLE IF NOT EXISTS __pgsqlite_enum_values (
                value_oid INTEGER PRIMARY KEY,
                type_oid INTEGER NOT NULL,
                label TEXT NOT NULL,
                sort_order REAL NOT NULL,
                FOREIGN KEY (type_oid) REFERENCES __pgsqlite_enum_types(type_oid),
                UNIQUE (type_oid, label)
            );
            "#,
            r#"
            -- Index for efficient lookups
            CREATE INDEX IF NOT EXISTS idx_enum_values_type ON __pgsqlite_enum_values(type_oid);
            CREATE INDEX IF NOT EXISTS idx_enum_values_label ON __pgsqlite_enum_values(type_oid, label);
            "#,
            r#"
            -- Track ENUM usage in tables
            CREATE TABLE IF NOT EXISTS __pgsqlite_enum_usage (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                enum_type TEXT NOT NULL,
                PRIMARY KEY (table_name, column_name),
                FOREIGN KEY (enum_type) REFERENCES __pgsqlite_enum_types(type_name) ON DELETE CASCADE
            );
            "#,
            r#"
            -- Update schema version
            UPDATE __pgsqlite_metadata 
            SET value = '2', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::Sql(r#"
            DROP TABLE IF EXISTS __pgsqlite_enum_usage;
            DROP INDEX IF EXISTS idx_enum_values_label;
            DROP INDEX IF EXISTS idx_enum_values_type;
            DROP TABLE IF EXISTS __pgsqlite_enum_values;
            DROP TABLE IF EXISTS __pgsqlite_enum_types;
            UPDATE __pgsqlite_metadata 
            SET value = '1', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![1],
    });
}