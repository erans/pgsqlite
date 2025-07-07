use super::{Migration, MigrationAction, MIGRATIONS};
use anyhow::{anyhow, Result};
use rusqlite::{params, Connection};
use std::time::Instant;
use tracing::{error, info};
use uuid::Uuid;

pub struct MigrationRunner {
    conn: Connection,
    process_id: String,
}

impl MigrationRunner {
    pub fn new(conn: Connection) -> Self {
        Self {
            conn,
            process_id: format!("{}:{}", std::process::id(), Uuid::new_v4()),
        }
    }
    
    pub fn into_connection(self) -> Connection {
        self.conn
    }
    
    pub fn check_schema_version(&self) -> Result<()> {
        // Ensure metadata tables exist to check version
        self.ensure_metadata_tables()?;
        
        let current_version = self.get_current_version()?;
        let target_version = *MIGRATIONS.keys().max().unwrap_or(&0);
        
        if current_version < target_version {
            return Err(anyhow!(
                "Database schema is outdated. Current version: {}, Required version: {}. \
                 Please run with --migrate to update the schema.",
                current_version, target_version
            ));
        }
        
        Ok(())
    }
    
    pub fn run_pending_migrations(&mut self) -> Result<Vec<u32>> {
        // Create metadata tables if they don't exist (for new databases)
        self.ensure_metadata_tables()?;
        
        // Acquire migration lock
        self.acquire_lock()?;
        
        let result = self.run_migrations_internal();
        
        // Always release lock
        let _ = self.release_lock();
        
        result
    }
    
    fn ensure_metadata_tables(&self) -> Result<()> {
        // Check if metadata tables exist
        let metadata_exists = self.conn.query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='__pgsqlite_metadata'",
            [],
            |_| Ok(true)
        ).unwrap_or(false);
        
        if !metadata_exists {
            // Create the basic metadata tables structure
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
            "#)?;
        }
        
        Ok(())
    }
    
    fn run_migrations_internal(&mut self) -> Result<Vec<u32>> {
        let current_version = self.get_current_version()?;
        let target_version = *MIGRATIONS.keys().max().unwrap_or(&0);
        
        if current_version >= target_version {
            info!("Schema is up to date (version {})", current_version);
            return Ok(vec![]);
        }
        
        // Handle pre-migration database
        if current_version == 0 && self.has_legacy_schema()? {
            info!("Detected pre-migration database with existing schema");
            self.mark_existing_schema_as_version_1()?;
            return self.run_migrations_internal();
        }
        
        let mut applied = Vec::new();
        
        // Apply migrations in order
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
                    .map_err(|e| anyhow::anyhow!("SQL execution failed: {}", e))
            }
            MigrationAction::SqlBatch(batch) => {
                let mut batch_result = Ok(());
                for sql in batch.iter() {
                    if let Err(e) = self.conn.execute_batch(sql) {
                        batch_result = Err(anyhow::anyhow!("SQL batch execution failed: {}", e));
                        break;
                    }
                }
                batch_result
            }
            MigrationAction::Function(f) => {
                f(&self.conn)
            }
            MigrationAction::Combined { pre_sql, function, post_sql } => {
                let mut combined_result = Ok(());
                if let Some(sql) = pre_sql {
                    if let Err(e) = self.conn.execute_batch(sql) {
                        combined_result = Err(anyhow::anyhow!("Pre-SQL execution failed: {}", e));
                    }
                }
                if combined_result.is_ok() {
                    combined_result = function(&self.conn);
                }
                if combined_result.is_ok() {
                    if let Some(sql) = post_sql {
                        if let Err(e) = self.conn.execute_batch(sql) {
                            combined_result = Err(anyhow::anyhow!("Post-SQL execution failed: {}", e));
                        }
                    }
                }
                combined_result
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
                let _ = self.conn.execute(
                    "UPDATE __pgsqlite_migrations 
                     SET status = 'failed', error_message = ?1 
                     WHERE version = ?2",
                    params![e.to_string(), migration.version]
                );
                
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
                // This is a pre-migration database, but we need to verify
                // it hasn't been marked as version 1 yet
                return Ok(0);
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
    
    fn has_legacy_schema(&self) -> Result<bool> {
        // Check if we have the original __pgsqlite_schema table
        Ok(self.conn.query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='__pgsqlite_schema'",
            [],
            |_| Ok(true)
        ).unwrap_or(false))
    }
    
    fn mark_existing_schema_as_version_1(&mut self) -> Result<()> {
        info!("Marking existing database as version 1");
        
        // Mark version 1 as already applied
        self.conn.execute(
            "INSERT OR REPLACE INTO __pgsqlite_metadata (key, value) VALUES ('schema_version', '1')",
            []
        )?;
        
        self.conn.execute(
            "INSERT OR REPLACE INTO __pgsqlite_migrations 
             (version, name, description, applied_at, checksum, status, execution_time_ms)
             VALUES 
             (1, 'initial_schema', 'Create initial pgsqlite system tables', 
              ?1, 'pre-existing', 'completed', 0)",
            params![chrono::Utc::now().timestamp() as f64]
        )?;
        
        Ok(())
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