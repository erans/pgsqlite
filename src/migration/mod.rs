pub mod registry;
pub mod runner;

use anyhow::Result;
use rusqlite::Connection;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone)]
pub struct Migration {
    pub version: u32,
    pub name: &'static str,
    pub description: &'static str,
    pub up: MigrationAction,
    pub down: Option<MigrationAction>,
    pub dependencies: Vec<u32>,
}

#[derive(Debug, Clone)]
pub enum MigrationAction {
    /// Simple SQL migration
    Sql(&'static str),
    
    /// Multiple SQL statements
    SqlBatch(&'static [&'static str]),
    
    /// Complex migration requiring code
    Function(fn(&Connection) -> Result<()>),
    
    /// Combination of SQL and code
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

pub use registry::MIGRATIONS;
pub use runner::MigrationRunner;