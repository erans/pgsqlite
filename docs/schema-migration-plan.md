# Schema Migration Plan for pgsqlite

> **Implementation Status**: ✅ COMPLETED (2025-07-06)  
> This plan has been fully implemented with one key change: migrations require explicit `--migrate` flag for safety.

## Executive Summary

This document outlines the implementation plan for an internal schema migration system in pgsqlite. The system embeds migrations in the binary, tracks migration history, and provides controlled schema upgrades while maintaining backward compatibility.

## Motivation

As pgsqlite evolves, we need to:
1. **Modify internal schema** - Add columns, create tables, change data types
2. **Maintain compatibility** - Support databases created with older versions
3. **Track changes** - Know what migrations have been applied and when
4. **Enable rollbacks** - Support downgrading when possible
5. **Ensure safety** - Migrations must be atomic and recoverable

## Design Overview

### Core Components

1. **Migration Registry** - Embedded migrations in the binary
2. **Version Tracking** - Database metadata for current schema version
3. **Migration History** - Audit trail of applied migrations
4. **Migration Runner** - Orchestrates migration execution
5. **Safety Mechanisms** - Transactions, validation, and recovery

### Migration Execution Timing

**IMPORTANT UPDATE (2025-07-06)**: Migrations are **NOT executed automatically** for safety reasons. Instead:

1. **Schema Version Check** - On startup, pgsqlite checks if the database schema is outdated
2. **Error on Outdated Schema** - If migrations are needed, pgsqlite exits with an error message
3. **Explicit Migration** - Users must run `pgsqlite --migrate` to apply pending migrations
4. **Migration and Exit** - The `--migrate` flag runs migrations and exits immediately

This approach ensures:
- No unexpected schema changes during normal operation
- Users have full control over when migrations occur
- Clear visibility into migration requirements
- Safe operation in production environments

Example usage:
```bash
# Normal operation (will error if schema is outdated)
pgsqlite --database mydb.db

# Error message if outdated:
# Database schema is outdated. Current version: 0, Required version: 2.
# Please run with --migrate to update the schema.

# Run migrations explicitly
pgsqlite --database mydb.db --migrate
```

**Exception**: In-memory databases during tests automatically run migrations for convenience.

### Database Schema

```sql
-- System metadata table
CREATE TABLE IF NOT EXISTS __pgsqlite_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    created_at REAL DEFAULT (strftime('%s', 'now')),
    updated_at REAL DEFAULT (strftime('%s', 'now'))
);

-- Migration history table
CREATE TABLE IF NOT EXISTS __pgsqlite_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    applied_at REAL NOT NULL,
    execution_time_ms INTEGER,
    checksum TEXT NOT NULL,  -- SHA256 of migration content
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
```

## Implementation Details

### Migration Structure

```rust
// src/migration/mod.rs
use std::collections::BTreeMap;
use sha2::{Sha256, Digest};

#[derive(Debug, Clone)]
pub struct Migration {
    pub version: u32,
    pub name: &'static str,
    pub description: &'static str,
    pub up: MigrationAction,
    pub down: Option<MigrationAction>,
    pub dependencies: Vec<u32>,  // Other migrations this depends on
}

#[derive(Debug, Clone)]
pub enum MigrationAction {
    // Simple SQL migration
    Sql(&'static str),
    
    // Multiple SQL statements
    SqlBatch(&'static [&'static str]),
    
    // Complex migration requiring code
    Function(fn(&Connection) -> Result<()>),
    
    // Combination of SQL and code
    Combined {
        pre_sql: Option<&'static str>,
        function: fn(&Connection) -> Result<()>,
        post_sql: Option<&'static str>,
    },
}

impl Migration {
    pub fn checksum(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.version.to_string());
        hasher.update(self.name);
        hasher.update(self.description);
        // Hash the migration content
        match &self.up {
            MigrationAction::Sql(sql) => hasher.update(sql),
            MigrationAction::SqlBatch(batch) => {
                for sql in batch.iter() {
                    hasher.update(sql);
                }
            }
            MigrationAction::Function(_) => hasher.update("function"),
            MigrationAction::Combined { pre_sql, post_sql, .. } => {
                if let Some(sql) = pre_sql {
                    hasher.update(sql);
                }
                hasher.update("function");
                if let Some(sql) = post_sql {
                    hasher.update(sql);
                }
            }
        }
        format!("{:x}", hasher.finalize())
    }
}
```

### Migration Registry

```rust
// src/migration/registry.rs
use lazy_static::lazy_static;
use std::collections::BTreeMap;

lazy_static! {
    pub static ref MIGRATIONS: BTreeMap<u32, Migration> = {
        let mut registry = BTreeMap::new();
        
        // Register all migrations
        register_v1_initial_schema(&mut registry);
        register_v2_datetime_support(&mut registry);
        register_v3_enum_support(&mut registry);
        register_v4_improved_decimal(&mut registry);
        
        registry
    };
}

// Version 1: Initial schema
fn register_v1_initial_schema(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(1, Migration {
        version: 1,
        name: "initial_schema",
        description: "Create initial pgsqlite system tables",
        up: MigrationAction::Sql(r#"
            -- Core schema tracking
            CREATE TABLE IF NOT EXISTS __pgsqlite_schema (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                pg_type TEXT NOT NULL,
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
            
            -- Set initial version
            INSERT INTO __pgsqlite_metadata (key, value) VALUES 
                ('schema_version', '1'),
                ('pgsqlite_version', '0.1.0');
        "#),
        down: None,  // Cannot rollback initial schema
        dependencies: vec![],
    });
}

// Version 2: DateTime support
fn register_v2_datetime_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(2, Migration {
        version: 2,
        name: "datetime_unix_timestamp",
        description: "Add datetime format tracking and migrate to Unix timestamps",
        up: MigrationAction::Combined {
            pre_sql: Some(r#"
                -- Add new columns for datetime tracking
                ALTER TABLE __pgsqlite_schema ADD COLUMN datetime_format TEXT;
                ALTER TABLE __pgsqlite_schema ADD COLUMN timezone_offset INTEGER;
                
                -- Create temporary mapping table
                CREATE TEMP TABLE datetime_columns AS
                SELECT table_name, column_name, pg_type 
                FROM __pgsqlite_schema 
                WHERE pg_type IN ('timestamp', 'timestamptz', 'date', 'time', 'timetz');
            "#),
            function: migrate_datetime_columns_to_unix,
            post_sql: Some(r#"
                -- Update metadata
                UPDATE __pgsqlite_metadata 
                SET value = '2', updated_at = strftime('%s', 'now')
                WHERE key = 'schema_version';
            "#),
        },
        down: Some(MigrationAction::Function(rollback_datetime_migration)),
        dependencies: vec![1],
    });
}

// Version 3: ENUM support (already in codebase)
fn register_v3_enum_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(3, Migration {
        version: 3,
        name: "enum_type_support",
        description: "Add PostgreSQL ENUM type support",
        up: MigrationAction::SqlBatch(&[
            r#"
            CREATE TABLE IF NOT EXISTS __pgsqlite_enum_types (
                type_name TEXT PRIMARY KEY,
                values TEXT NOT NULL
            );
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS __pgsqlite_enum_values (
                type_name TEXT NOT NULL,
                value TEXT NOT NULL,
                position INTEGER NOT NULL,
                PRIMARY KEY (type_name, value),
                FOREIGN KEY (type_name) REFERENCES __pgsqlite_enum_types(type_name)
            );
            "#,
            r#"
            CREATE INDEX idx_enum_values_position ON __pgsqlite_enum_values(type_name, position);
            "#,
        ]),
        down: Some(MigrationAction::Sql(r#"
            DROP TABLE IF EXISTS __pgsqlite_enum_values;
            DROP TABLE IF EXISTS __pgsqlite_enum_types;
        "#)),
        dependencies: vec![1],
    });
}

// Complex migration function example
fn migrate_datetime_columns_to_unix(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT table_name, column_name, pg_type FROM datetime_columns"
    )?;
    
    let columns: Vec<(String, String, String)> = stmt
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    
    for (table, column, pg_type) in columns {
        // Check column type in actual table
        let sql = format!(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='{}'", 
            table
        );
        let table_sql: String = conn.query_row(&sql, [], |row| row.get(0))?;
        
        // Only migrate if column exists and is TEXT type
        if table_sql.contains(&column) {
            info!("Migrating {}.{} from TEXT to REAL ({})", table, column, pg_type);
            
            // Create new column
            conn.execute(&format!(
                "ALTER TABLE {} ADD COLUMN {}_new REAL", 
                table, column
            ), [])?;
            
            // Convert data based on type
            let conversion_sql = match pg_type.as_str() {
                "date" => format!(
                    "UPDATE {} SET {}_new = strftime('%s', {}, 'start of day')",
                    table, column, column
                ),
                "timestamp" | "timestamptz" => format!(
                    "UPDATE {} SET {}_new = strftime('%s', {}) + 
                     CASE 
                        WHEN {} LIKE '%.%' 
                        THEN CAST(substr({}, instr({}, '.') + 1, 6) AS REAL) / 1000000.0
                        ELSE 0
                     END",
                    table, column, column, column, column, column
                ),
                "time" | "timetz" => format!(
                    "UPDATE {} SET {}_new = 
                     CAST(substr({}, 1, 2) AS INTEGER) * 3600 +
                     CAST(substr({}, 4, 2) AS INTEGER) * 60 +
                     CAST(substr({}, 7, 2) AS INTEGER) +
                     CASE 
                        WHEN {} LIKE '%.%' 
                        THEN CAST(substr({}, 10, 6) AS REAL) / 1000000.0
                        ELSE 0
                     END",
                    table, column, column, column, column, column, column
                ),
                _ => continue,
            };
            
            conn.execute(&conversion_sql, [])?;
            
            // Update schema metadata
            conn.execute(
                "UPDATE __pgsqlite_schema SET datetime_format = ? 
                 WHERE table_name = ? AND column_name = ?",
                params![pg_type, table, column]
            )?;
        }
    }
    
    Ok(())
}
```

### Migration Runner

```rust
// src/migration/runner.rs
use std::time::Instant;

pub struct MigrationRunner {
    conn: Connection,
    process_id: String,
}

impl MigrationRunner {
    pub fn new(conn: Connection) -> Self {
        Self {
            conn,
            process_id: format!("{}:{}", std::process::id(), uuid::Uuid::new_v4()),
        }
    }
    
    pub fn run_pending_migrations(&mut self) -> Result<Vec<u32>> {
        // Acquire migration lock
        self.acquire_lock()?;
        
        let result = self.run_migrations_internal();
        
        // Always release lock
        self.release_lock()?;
        
        result
    }
    
    fn run_migrations_internal(&mut self) -> Result<Vec<u32>> {
        let current_version = self.get_current_version()?;
        let target_version = *MIGRATIONS.keys().max().unwrap_or(&0);
        
        if current_version >= target_version {
            info!("Schema is up to date (version {})", current_version);
            return Ok(vec![]);
        }
        
        // Handle pre-migration database
        if current_version == 1 && !self.metadata_table_exists()? {
            info!("Detected pre-migration database at version 1, creating metadata tables");
            self.create_metadata_tables_for_existing_db()?;
        }
        
        let mut applied = Vec::new();
        
        // Check for required migrations
        for version in (current_version + 1)..=target_version {
            if let Some(migration) = MIGRATIONS.get(&version) {
                // Verify dependencies
                for dep in &migration.dependencies {
                    if !self.is_migration_applied(*dep)? {
                        return Err(anyhow!(
                            "Migration {} depends on {}, which hasn't been applied",
                            version, dep
                        ));
                    }
                }
                
                // Verify checksum if migration was partially applied
                if let Some(existing_checksum) = self.get_migration_checksum(version)? {
                    let current_checksum = migration.checksum();
                    if existing_checksum != current_checksum {
                        return Err(anyhow!(
                            "Migration {} has been modified! Expected checksum: {}, got: {}",
                            version, existing_checksum, current_checksum
                        ));
                    }
                }
                
                // Apply migration
                self.apply_migration(migration)?;
                applied.push(version);
            }
        }
        
        Ok(applied)
    }
    
    fn apply_migration(&mut self, migration: &Migration) -> Result<()> {
        info!("Applying migration {}: {}", migration.version, migration.description);
        let start = Instant::now();
        
        // Start transaction
        self.conn.execute("BEGIN EXCLUSIVE TRANSACTION", [])?;
        
        // Record migration start
        self.conn.execute(
            "INSERT OR REPLACE INTO __pgsqlite_migrations 
             (version, name, description, applied_at, checksum, status) 
             VALUES (?1, ?2, ?3, ?4, ?5, 'running')",
            params![
                migration.version,
                migration.name,
                migration.description,
                chrono::Utc::now().timestamp() as f64,
                migration.checksum()
            ]
        )?;
        
        // Execute migration
        let result = match &migration.up {
            MigrationAction::Sql(sql) => {
                self.conn.execute_batch(sql)
            }
            MigrationAction::SqlBatch(batch) => {
                for sql in batch.iter() {
                    self.conn.execute_batch(sql)?;
                }
                Ok(())
            }
            MigrationAction::Function(f) => {
                f(&self.conn)
            }
            MigrationAction::Combined { pre_sql, function, post_sql } => {
                if let Some(sql) = pre_sql {
                    self.conn.execute_batch(sql)?;
                }
                function(&self.conn)?;
                if let Some(sql) = post_sql {
                    self.conn.execute_batch(sql)?;
                }
                Ok(())
            }
        };
        
        match result {
            Ok(()) => {
                // Update migration status
                let elapsed = start.elapsed().as_millis() as i64;
                self.conn.execute(
                    "UPDATE __pgsqlite_migrations 
                     SET status = 'completed', execution_time_ms = ?1 
                     WHERE version = ?2",
                    params![elapsed, migration.version]
                )?;
                
                // Update schema version
                self.conn.execute(
                    "INSERT OR REPLACE INTO __pgsqlite_metadata (key, value, updated_at) 
                     VALUES ('schema_version', ?1, ?2)",
                    params![migration.version, chrono::Utc::now().timestamp() as f64]
                )?;
                
                // Commit transaction
                self.conn.execute("COMMIT", [])?;
                
                info!("Migration {} completed in {}ms", migration.version, elapsed);
                Ok(())
            }
            Err(e) => {
                // Record failure
                self.conn.execute(
                    "UPDATE __pgsqlite_migrations 
                     SET status = 'failed', error_message = ?1 
                     WHERE version = ?2",
                    params![e.to_string(), migration.version]
                )?;
                
                // Rollback transaction
                self.conn.execute("ROLLBACK", [])?;
                
                error!("Migration {} failed: {}", migration.version, e);
                Err(e)
            }
        }
    }
    
    fn acquire_lock(&mut self) -> Result<()> {
        let now = chrono::Utc::now().timestamp() as f64;
        let expires = now + 300.0; // 5 minute timeout
        
        // Try to acquire lock
        match self.conn.execute(
            "INSERT INTO __pgsqlite_migration_locks (id, locked_by, locked_at, expires_at) 
             VALUES (1, ?1, ?2, ?3)",
            params![self.process_id, now, expires]
        ) {
            Ok(_) => Ok(()),
            Err(_) => {
                // Check if lock is expired
                let (locked_by, expires_at): (String, f64) = self.conn.query_row(
                    "SELECT locked_by, expires_at FROM __pgsqlite_migration_locks WHERE id = 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?))
                )?;
                
                if expires_at < now {
                    // Lock expired, forcefully acquire
                    self.conn.execute(
                        "UPDATE __pgsqlite_migration_locks 
                         SET locked_by = ?1, locked_at = ?2, expires_at = ?3 
                         WHERE id = 1",
                        params![self.process_id, now, expires]
                    )?;
                    Ok(())
                } else {
                    Err(anyhow!(
                        "Migration lock held by process: {}. Expires at: {}", 
                        locked_by, 
                        chrono::DateTime::<chrono::Utc>::from_timestamp(expires_at as i64, 0)
                            .unwrap_or_default()
                    ))
                }
            }
        }
    }
    
    fn release_lock(&mut self) -> Result<()> {
        self.conn.execute(
            "DELETE FROM __pgsqlite_migration_locks WHERE id = 1 AND locked_by = ?1",
            params![self.process_id]
        )?;
        Ok(())
    }
    
    fn get_current_version(&self) -> Result<u32> {
        // First check if metadata table exists
        let metadata_exists = self.conn.query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='__pgsqlite_metadata'",
            [],
            |_| Ok(true)
        ).unwrap_or(false);
        
        if !metadata_exists {
            // Check if we have the original __pgsqlite_schema table
            let has_pgsqlite_schema = self.conn.query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='__pgsqlite_schema'",
                [],
                |_| Ok(true)
            ).unwrap_or(false);
            
            if has_pgsqlite_schema {
                // This is a pre-migration database, assume version 1
                return Ok(1);
            } else {
                // Brand new database
                return Ok(0);
            }
        }
        
        // Metadata table exists, get version from it
        match self.conn.query_row(
            "SELECT value FROM __pgsqlite_metadata WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0)
        ) {
            Ok(version) => Ok(version.parse::<u32>()?),
            Err(_) => Ok(0), // Metadata exists but no version set
        }
    }
    
    fn is_migration_applied(&self, version: u32) -> Result<bool> {
        Ok(self.conn.query_row(
            "SELECT 1 FROM __pgsqlite_migrations WHERE version = ?1 AND status = 'completed'",
            params![version],
            |_| Ok(())
        ).is_ok())
    }
    
    fn get_migration_checksum(&self, version: u32) -> Result<Option<String>> {
        match self.conn.query_row(
            "SELECT checksum FROM __pgsqlite_migrations WHERE version = ?1",
            params![version],
            |row| row.get::<_, String>(0)
        ) {
            Ok(checksum) => Ok(Some(checksum)),
            Err(_) => Ok(None),
        }
    }
}
```

### Integration with DbHandler

```rust
// src/session/db_handler.rs
impl DbHandler {
    pub fn new(path: &str) -> Result<Self> {
        let mut conn = Connection::open(path)?;
        
        // Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON", [])?;
        
        // IMPORTANT: Run migrations on first database load only
        // This ensures all internal tables are created/updated before any other operations
        let mut runner = MigrationRunner::new(conn);
        let applied = runner.run_pending_migrations()?;
        
        if !applied.is_empty() {
            info!("Applied {} migrations on database initialization: {:?}", applied.len(), applied);
        }
        
        // Return connection after migrations
        let conn = runner.into_connection();
        
        Ok(DbHandler {
            conn: Arc::new(Mutex::new(conn)),
            schema_version: runner.get_final_version(),
            // ... other fields
        })
    }
    
    // Subsequent operations can rely on schema being up-to-date
    pub fn execute_query(&self, query: &str) -> Result<QueryResult> {
        // No need to check migrations here - already done on initialization
        self.conn.lock().execute(query, [])?;
        // ...
    }
}

// For connection pooling scenarios
pub struct ConnectionPool {
    migrations_applied: Arc<AtomicBool>,
    connections: Vec<Arc<Mutex<Connection>>>,
}

impl ConnectionPool {
    pub fn new(path: &str, size: usize) -> Result<Self> {
        let migrations_applied = Arc::new(AtomicBool::new(false));
        let mut connections = Vec::new();
        
        for i in 0..size {
            let mut conn = Connection::open(path)?;
            
            // Only run migrations on the first connection
            if i == 0 && !migrations_applied.load(Ordering::Relaxed) {
                let mut runner = MigrationRunner::new(conn);
                runner.run_pending_migrations()?;
                conn = runner.into_connection();
                migrations_applied.store(true, Ordering::Relaxed);
                info!("Schema migrations completed for connection pool");
            }
            
            connections.push(Arc::new(Mutex::new(conn)));
        }
        
        Ok(ConnectionPool { migrations_applied, connections })
    }
}
```

### Migration Behavior

#### Version Detection Logic
The migration system uses the following logic to determine the current schema version:
1. If `__pgsqlite_metadata` table doesn't exist:
   - If `__pgsqlite_schema` table exists → **Version 1** (pre-migration database)
   - If no system tables exist → **Version 0** (brand new database)
2. If `__pgsqlite_metadata` exists, read version from it

This ensures backward compatibility - any database with the original `__pgsqlite_schema` table is automatically recognized as version 1.

#### New Database
When opening a brand new database file (or `:memory:` database):
1. No system tables exist, so current version = 0
2. Migration runner detects this and applies ALL migrations sequentially (1, 2, 3, ..., N)
3. The database starts with the latest schema structure
4. Each migration is recorded in `__pgsqlite_migrations` table

#### Existing Database (Pre-migration)
For databases created before the migration system was introduced:
```rust
fn detect_pre_migration_database(&self) -> Result<bool> {
    // Check if we have the old schema table but no metadata
    let has_old_schema = self.conn.query_row(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='__pgsqlite_schema'",
        [],
        |_| Ok(true)
    ).unwrap_or(false);
    
    let has_metadata = self.conn.query_row(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='__pgsqlite_metadata'",
        [],
        |_| Ok(true)
    ).unwrap_or(false);
    
    Ok(has_old_schema && !has_metadata)
}

    fn metadata_table_exists(&self) -> Result<bool> {
        Ok(self.conn.query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='__pgsqlite_metadata'",
            [],
            |_| Ok(true)
        ).unwrap_or(false))
    }
    
    fn create_metadata_tables_for_existing_db(&mut self) -> Result<()> {
        info!("Creating metadata tables for existing database");
        
        // Create metadata tables without full migration
        self.conn.execute_batch(r#"
            CREATE TABLE IF NOT EXISTS __pgsqlite_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                created_at REAL DEFAULT (strftime('%s', 'now')),
                updated_at REAL DEFAULT (strftime('%s', 'now'))
            );
            
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
            
            CREATE TABLE IF NOT EXISTS __pgsqlite_migration_locks (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                locked_by TEXT NOT NULL,
                locked_at REAL NOT NULL,
                expires_at REAL NOT NULL
            );
            
            -- Mark version 1 as already applied
            INSERT OR REPLACE INTO __pgsqlite_metadata (key, value) VALUES ('schema_version', '1');
            INSERT OR REPLACE INTO __pgsqlite_migrations 
                (version, name, description, applied_at, checksum, status, execution_time_ms)
            VALUES 
                (1, 'initial_schema', 'Create initial pgsqlite system tables', 
                 strftime('%s', 'now'), 'pre-existing', 'completed', 0);
        "#)?;
        
        Ok(())
    }
```

#### Partially Migrated Database
For databases where migrations were interrupted:
1. Migration runner checks the `status` column in `__pgsqlite_migrations`
2. If a migration is marked as 'running' or 'failed', it can be retried
3. Checksums ensure migrations haven't been modified

#### Example Scenarios

**Scenario 1: Brand new database with 5 migrations available**
```
Opening new.db
Current version: 0
Target version: 5
Applying migration 1: initial_schema
Applying migration 2: datetime_unix_timestamp  
Applying migration 3: enum_type_support
Applying migration 4: improved_decimal
Applying migration 5: array_support
Database ready at version 5
```

**Scenario 2: Existing v3 database with 5 migrations available**
```
Opening existing.db
Current version: 3
Target version: 5
Applying migration 4: improved_decimal
Applying migration 5: array_support
Database ready at version 5
```

**Scenario 3: Pre-migration database**
```
Opening legacy.db
Detected pre-migration database
Marking as version 1
Current version: 1
Target version: 5
Applying migration 2: datetime_unix_timestamp
Applying migration 3: enum_type_support
Applying migration 4: improved_decimal
Applying migration 5: array_support
Database ready at version 5
```

## Safety and Recovery

### Automatic Backup

```rust
fn backup_before_migration(conn: &Connection, version: u32) -> Result<()> {
    let backup_path = format!("{}.pre_migration_v{}.backup", 
        conn.path().unwrap_or("memory"), version);
    
    let mut backup_conn = Connection::open(&backup_path)?;
    conn.backup(DatabaseName::Main, &mut backup_conn, DatabaseName::Main)?;
    
    info!("Created backup at: {}", backup_path);
    Ok(())
}
```

### Migration Validation

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_migration_sequence() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        // Test fresh database
        let mut conn = Connection::open(&db_path).unwrap();
        let mut runner = MigrationRunner::new(conn);
        let applied = runner.run_pending_migrations().unwrap();
        assert_eq!(applied.len(), MIGRATIONS.len());
        
        // Test idempotency
        let mut conn = Connection::open(&db_path).unwrap();
        let mut runner = MigrationRunner::new(conn);
        let applied = runner.run_pending_migrations().unwrap();
        assert_eq!(applied.len(), 0);
    }
    
    #[test]
    fn test_migration_rollback() {
        // Test migrations with down() functions
        for (version, migration) in MIGRATIONS.iter() {
            if migration.down.is_some() {
                // Apply migration
                // Then rollback
                // Verify state
            }
        }
    }
}
```

## CLI Support

```rust
// Add migration commands to CLI
enum MigrationCommand {
    /// Show current schema version and pending migrations
    Status,
    
    /// Apply all pending migrations
    Up,
    
    /// Rollback to a specific version
    Down { version: u32 },
    
    /// Show migration history
    History,
    
    /// Validate migration checksums
    Validate,
}
```

## Best Practices

1. **Always test migrations** on a copy of production data
2. **Keep migrations small** and focused on a single change
3. **Make migrations idempotent** when possible
4. **Document complex migrations** thoroughly
5. **Never modify existing migrations** after release
6. **Test both up and down** migrations
7. **Use transactions** for atomic changes
8. **Validate data** after migration
9. **Run migrations early** - Always during database initialization, never during normal operations
10. **Cache schema version** - Store the version in memory after initialization to avoid repeated checks

## Future Considerations

1. **Parallel migrations** - Some migrations could run in parallel
2. **Conditional migrations** - Skip based on data/environment
3. **Data migrations** - Separate DDL from DML migrations
4. **Migration squashing** - Combine old migrations for performance
5. **Online migrations** - Migrations without downtime
6. **Migration metrics** - Track performance and success rates

## Conclusion

This migration system provides a robust, embedded solution for schema evolution in pgsqlite. By tracking versions, maintaining history, and ensuring atomic operations, we can safely evolve the database schema while maintaining compatibility across versions.