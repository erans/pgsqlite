use rusqlite::{Connection, OpenFlags};

use crate::PgSqliteError;

pub struct SystemDb {
    conn: Connection,
}

impl SystemDb {
    pub fn open(data_dir: &std::path::Path) -> Result<Self, PgSqliteError> {
        std::fs::create_dir_all(data_dir)
            .map_err(|e| PgSqliteError::Protocol(format!("Failed to create data dir: {e}")))?;

        let system_path = data_dir.join("system.db");
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_FULL_MUTEX
            | OpenFlags::SQLITE_OPEN_URI;

        let conn =
            Connection::open_with_flags(system_path, flags).map_err(PgSqliteError::Sqlite)?;

        crate::functions::register_all_functions(&conn).map_err(PgSqliteError::Sqlite)?;
        Self::init_schema(&conn)?;

        Ok(Self { conn })
    }

    fn init_schema(conn: &Connection) -> Result<(), PgSqliteError> {
        conn.execute_batch(
            "BEGIN;
             CREATE TABLE IF NOT EXISTS __pgsqlite_cluster_databases (
               name TEXT PRIMARY KEY,
               created_at TEXT DEFAULT (datetime('now'))
             );
             CREATE TABLE IF NOT EXISTS __pgsqlite_cluster_schemas (
               database_name TEXT NOT NULL,
               schema_name TEXT NOT NULL,
               created_at TEXT DEFAULT (datetime('now')),
               PRIMARY KEY (database_name, schema_name)
             );
             COMMIT;",
        )
        .map_err(PgSqliteError::Sqlite)?;

        Ok(())
    }

    pub fn ensure_database(&self, name: &str) -> Result<(), PgSqliteError> {
        self.conn
            .execute(
                "INSERT OR IGNORE INTO __pgsqlite_cluster_databases(name) VALUES (?1)",
                [name],
            )
            .map_err(PgSqliteError::Sqlite)?;
        Ok(())
    }

    pub fn ensure_schema(
        &self,
        database_name: &str,
        schema_name: &str,
    ) -> Result<(), PgSqliteError> {
        self.conn
            .execute(
                "INSERT OR IGNORE INTO __pgsqlite_cluster_schemas(database_name, schema_name) VALUES (?1, ?2)",
                [database_name, schema_name],
            )
            .map_err(PgSqliteError::Sqlite)?;
        Ok(())
    }

    pub fn drop_schema(&self, database_name: &str, schema_name: &str) -> Result<(), PgSqliteError> {
        self.conn
            .execute(
                "DELETE FROM __pgsqlite_cluster_schemas WHERE database_name = ?1 AND schema_name = ?2",
                [database_name, schema_name],
            )
            .map_err(PgSqliteError::Sqlite)?;
        Ok(())
    }

    pub fn drop_database(&self, name: &str) -> Result<(), PgSqliteError> {
        self.conn
            .execute(
                "DELETE FROM __pgsqlite_cluster_schemas WHERE database_name = ?1",
                [name],
            )
            .map_err(PgSqliteError::Sqlite)?;
        self.conn
            .execute(
                "DELETE FROM __pgsqlite_cluster_databases WHERE name = ?1",
                [name],
            )
            .map_err(PgSqliteError::Sqlite)?;
        Ok(())
    }
}
