//! ALTER TABLE ... ADD / DROP / RENAME COLUMN with `__pgsqlite_schema` kept in
//! step. Without this the column change reaches SQLite but not the metadata,
//! and the next start fails the schema drift check.

use once_cell::sync::Lazy;
use regex::Regex;
use rusqlite::Connection;

use crate::translator::CreateTableTranslator;

static ALTER_COLUMN_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?is)^\s*ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?("?[A-Za-z_][\w$]*"?)\s+(ADD|DROP|RENAME)\s+(?:COLUMN\s+)?(.+?)\s*;?\s*$"#,
    )
    .unwrap()
});

static IF_EXISTS_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^IF\s+(NOT\s+)?EXISTS\s+").unwrap());

static RENAME_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?i)^("?[A-Za-z_][\w$]*"?)\s+TO\s+("?[A-Za-z_][\w$]*"?)$"#).unwrap()
});

#[derive(Debug, PartialEq)]
pub enum AlterColumn {
    Add { table: String, definition: String, if_not_exists: bool },
    Drop { table: String, column: String, if_exists: bool },
    Rename { table: String, from: String, to: String },
}

fn unquote(s: &str) -> String {
    s.trim_matches('"').to_string()
}

/// Recognise a single-action column ALTER. Constraint actions (ADD CONSTRAINT,
/// DROP CONSTRAINT), table renames and multi-action ALTERs return None and keep
/// their existing handling.
pub fn parse(query: &str) -> Option<AlterColumn> {
    let caps = ALTER_COLUMN_REGEX.captures(query)?;
    let table = unquote(&caps[1]);
    let action = caps[2].to_uppercase();
    let mut rest = caps[3].trim().to_string();
    let first_word = rest.split_whitespace().next().unwrap_or("").to_uppercase();
    if matches!(first_word.as_str(), "CONSTRAINT" | "PRIMARY" | "UNIQUE" | "FOREIGN" | "CHECK") {
        return None;
    }
    if has_top_level_comma(&rest) {
        return None;
    }
    let guard = IF_EXISTS_REGEX.find(&rest).map(|m| m.end());
    if let Some(end) = guard {
        rest = rest[end..].trim().to_string();
    }
    match action.as_str() {
        "ADD" => Some(AlterColumn::Add { table, definition: rest, if_not_exists: guard.is_some() }),
        "DROP" => {
            let column = unquote(rest.split_whitespace().next()?);
            Some(AlterColumn::Drop { table, column, if_exists: guard.is_some() })
        }
        "RENAME" => {
            if first_word == "TO" {
                return None; // table rename
            }
            let r = RENAME_REGEX.captures(&rest)?;
            Some(AlterColumn::Rename { table, from: unquote(&r[1]), to: unquote(&r[2]) })
        }
        _ => None,
    }
}

fn has_top_level_comma(s: &str) -> bool {
    let (mut depth, mut in_quotes) = (0i32, false);
    for c in s.chars() {
        match c {
            '\'' => in_quotes = !in_quotes,
            '(' if !in_quotes => depth += 1,
            ')' if !in_quotes => depth -= 1,
            ',' if !in_quotes && depth == 0 => return true,
            _ => {}
        }
    }
    false
}

fn column_exists(conn: &Connection, table: &str, column: &str) -> rusqlite::Result<bool> {
    let n: i64 = conn.query_row(
        "SELECT COUNT(*) FROM pragma_table_info(?1) WHERE name = ?2 COLLATE NOCASE",
        [table, column],
        |row| row.get(0),
    )?;
    Ok(n > 0)
}

fn schema_table_exists(conn: &Connection) -> rusqlite::Result<bool> {
    let n: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '__pgsqlite_schema'",
        [],
        |row| row.get(0),
    )?;
    Ok(n > 0)
}

fn sql_err(msg: String) -> rusqlite::Error {
    rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR), Some(msg))
}

/// Execute the ALTER on `conn` and update `__pgsqlite_schema` to match.
pub fn execute(conn: &Connection, alter: &AlterColumn) -> rusqlite::Result<()> {
    match alter {
        AlterColumn::Add { table, definition, if_not_exists } => {
            let column = unquote(definition.split_whitespace().next().unwrap_or(""));
            if *if_not_exists && column_exists(conn, table, &column)? {
                return Ok(());
            }
            // Reuse the CREATE TABLE translation for the column's type, default
            // and constraints, then lift the column back out of the result.
            let fake = format!("CREATE TABLE {table} ({definition})");
            let result = CreateTableTranslator::translate_with_connection_full(&fake, Some(conn))
                .map_err(|e| sql_err(format!("ALTER TABLE ADD COLUMN translation failed: {e}")))?;
            let open = result.sql.find('(').ok_or_else(|| sql_err("ADD COLUMN: no column list".into()))?;
            let close = result.sql.rfind(')').ok_or_else(|| sql_err("ADD COLUMN: no column list".into()))?;
            let column_sql = result.sql[open + 1..close].trim();
            conn.execute(&format!("ALTER TABLE {table} ADD COLUMN {column_sql}"), [])?;
            if schema_table_exists(conn)? {
                let has_type_modifier = conn
                    .query_row(
                        "SELECT COUNT(*) FROM pragma_table_info('__pgsqlite_schema') WHERE name = 'type_modifier'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap_or(0)
                    > 0;
                for (full_column, m) in &result.type_mappings {
                    let Some((_, col)) = full_column.split_once('.') else { continue };
                    if has_type_modifier {
                        conn.execute(
                            "INSERT OR REPLACE INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type, type_modifier) VALUES (?1, ?2, ?3, ?4, ?5)",
                            rusqlite::params![table, col, &m.pg_type, &m.sqlite_type, m.type_modifier],
                        )?;
                    } else {
                        conn.execute(
                            "INSERT OR REPLACE INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES (?1, ?2, ?3, ?4)",
                            [table.as_str(), col, &m.pg_type, &m.sqlite_type],
                        )?;
                    }
                }
            }
        }
        AlterColumn::Drop { table, column, if_exists } => {
            if *if_exists && !column_exists(conn, table, column)? {
                return Ok(());
            }
            conn.execute(&format!("ALTER TABLE {table} DROP COLUMN {column}"), [])?;
            if schema_table_exists(conn)? {
                conn.execute(
                    "DELETE FROM __pgsqlite_schema WHERE table_name = ?1 AND column_name = ?2 COLLATE NOCASE",
                    [table, column],
                )?;
            }
        }
        AlterColumn::Rename { table, from, to } => {
            conn.execute(&format!("ALTER TABLE {table} RENAME COLUMN {from} TO {to}"), [])?;
            if schema_table_exists(conn)? {
                conn.execute(
                    "UPDATE __pgsqlite_schema SET column_name = ?3 WHERE table_name = ?1 AND column_name = ?2 COLLATE NOCASE",
                    [table, from, to],
                )?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        crate::metadata::TypeMetadata::init(&conn).unwrap();
        conn.execute("CREATE TABLE users (id TEXT PRIMARY KEY, legacy_role TEXT)", []).unwrap();
        conn.execute(
            "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES ('users','id','UUID','TEXT'), ('users','legacy_role','TEXT','TEXT')",
            [],
        )
        .unwrap();
        conn
    }

    fn meta(conn: &Connection) -> Vec<(String, String)> {
        let mut st = conn
            .prepare("SELECT column_name, pg_type FROM __pgsqlite_schema WHERE table_name='users' ORDER BY column_name")
            .unwrap();
        st.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().map(|r| r.unwrap()).collect()
    }

    #[test]
    fn test_internal_tables_get_no_metadata() {
        use crate::query::executor::extract_table_name_from_create;
        assert_eq!(extract_table_name_from_create("CREATE TABLE IF NOT EXISTS __pgsqlite_schema (a TEXT)"), None);
        assert_eq!(extract_table_name_from_create("CREATE TABLE IF NOT EXISTS users (a TEXT)").as_deref(), Some("users"));
    }

    #[test]
    fn test_parse() {
        assert_eq!(
            parse("ALTER TABLE users DROP COLUMN IF EXISTS legacy_role;"),
            Some(AlterColumn::Drop { table: "users".into(), column: "legacy_role".into(), if_exists: true })
        );
        assert_eq!(
            parse("ALTER TABLE users ADD COLUMN accepted_tos_at TIMESTAMPTZ"),
            Some(AlterColumn::Add { table: "users".into(), definition: "accepted_tos_at TIMESTAMPTZ".into(), if_not_exists: false })
        );
        assert_eq!(
            parse("alter table users rename column a to b"),
            Some(AlterColumn::Rename { table: "users".into(), from: "a".into(), to: "b".into() })
        );
        assert_eq!(parse("ALTER TABLE plans ADD CONSTRAINT fk FOREIGN KEY (x) REFERENCES y(id)"), None);
        assert_eq!(parse("ALTER TABLE t ADD COLUMN a INT, ADD COLUMN b INT"), None);
        assert_eq!(parse("ALTER TABLE t RENAME TO u"), None);
    }

    #[test]
    fn test_execute_keeps_metadata_in_step() {
        let conn = setup();
        execute(&conn, &parse("ALTER TABLE users DROP COLUMN IF EXISTS legacy_role").unwrap()).unwrap();
        execute(&conn, &parse("ALTER TABLE users DROP COLUMN IF EXISTS legacy_role").unwrap()).unwrap();
        execute(&conn, &parse("ALTER TABLE users ADD COLUMN accepted_tos_version TEXT NOT NULL DEFAULT ''").unwrap()).unwrap();
        execute(&conn, &parse("ALTER TABLE users ADD COLUMN accepted_tos_at TIMESTAMPTZ").unwrap()).unwrap();
        assert!(!column_exists(&conn, "users", "legacy_role").unwrap());
        assert!(column_exists(&conn, "users", "accepted_tos_at").unwrap());
        let m = meta(&conn);
        let names: Vec<&str> = m.iter().map(|(c, _)| c.as_str()).collect();
        assert_eq!(names, vec!["accepted_tos_at", "accepted_tos_version", "id"]);
        assert!(m.iter().any(|(c, t)| c == "accepted_tos_at" && t.eq_ignore_ascii_case("timestamptz")));
        execute(&conn, &parse("ALTER TABLE users RENAME COLUMN accepted_tos_at TO tos_at").unwrap()).unwrap();
        assert!(meta(&conn).iter().any(|(c, _)| c == "tos_at"));
    }
}
