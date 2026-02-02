use crate::types::{generate_uuid_v4, UuidHandler};
use once_cell::sync::Lazy;
use rand::RngCore;
use rusqlite::functions::FunctionFlags;
use rusqlite::{Connection, Result};
use uuid::v1::{Context, Timestamp};
use uuid::Uuid;

/// Register UUID-related functions in SQLite
pub fn register_uuid_functions(conn: &Connection) -> Result<()> {
    // gen_random_uuid() - PostgreSQL compatible UUID v4 generator
    // Note: SQLite may cache function results in certain contexts, but each call generates a new UUID
    conn.create_scalar_function("gen_random_uuid", 0, FunctionFlags::SQLITE_UTF8, |_ctx| {
        Ok(generate_uuid_v4())
    })?;

    // uuid_generate_v4() - Alternative name for compatibility
    conn.create_scalar_function("uuid_generate_v4", 0, FunctionFlags::SQLITE_UTF8, |_ctx| {
        Ok(generate_uuid_v4())
    })?;

    // uuid_generate_v1() - Version 1 UUID (time + node)
    conn.create_scalar_function("uuid_generate_v1", 0, FunctionFlags::SQLITE_UTF8, |_ctx| {
        Ok(generate_uuid_v1(false))
    })?;

    // uuid_generate_v1mc() - Version 1 UUID with random multicast node
    conn.create_scalar_function(
        "uuid_generate_v1mc",
        0,
        FunctionFlags::SQLITE_UTF8,
        |_ctx| Ok(generate_uuid_v1(true)),
    )?;

    // uuid_generate_v3(namespace uuid, name text) - Version 3 UUID (MD5)
    conn.create_scalar_function(
        "uuid_generate_v3",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let ns: String = ctx.get(0)?;
            let name: String = ctx.get(1)?;
            let ns_uuid =
                parse_uuid_arg(&ns).map_err(|e| rusqlite::Error::UserFunctionError(e.into()))?;
            Ok(Uuid::new_v3(&ns_uuid, name.as_bytes()).to_string())
        },
    )?;

    // uuid_generate_v5(namespace uuid, name text) - Version 5 UUID (SHA-1)
    conn.create_scalar_function(
        "uuid_generate_v5",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let ns: String = ctx.get(0)?;
            let name: String = ctx.get(1)?;
            let ns_uuid =
                parse_uuid_arg(&ns).map_err(|e| rusqlite::Error::UserFunctionError(e.into()))?;
            Ok(Uuid::new_v5(&ns_uuid, name.as_bytes()).to_string())
        },
    )?;

    // uuid_nil() - Nil UUID constant
    conn.create_scalar_function(
        "uuid_nil",
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| Ok(Uuid::nil().to_string()),
    )?;

    // Namespace constants
    conn.create_scalar_function(
        "uuid_ns_dns",
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| Ok(Uuid::NAMESPACE_DNS.to_string()),
    )?;
    conn.create_scalar_function(
        "uuid_ns_url",
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| Ok(Uuid::NAMESPACE_URL.to_string()),
    )?;
    conn.create_scalar_function(
        "uuid_ns_oid",
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| Ok(Uuid::NAMESPACE_OID.to_string()),
    )?;
    conn.create_scalar_function(
        "uuid_ns_x500",
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| Ok(Uuid::NAMESPACE_X500.to_string()),
    )?;

    // is_valid_uuid(text) - Check if a string is a valid UUID
    conn.create_scalar_function("is_valid_uuid", 1, FunctionFlags::SQLITE_UTF8, |ctx| {
        let value: String = ctx.get(0)?;
        Ok(UuidHandler::validate_uuid(&value))
    })?;

    // uuid_normalize(text) - Normalize UUID to lowercase
    conn.create_scalar_function("uuid_normalize", 1, FunctionFlags::SQLITE_UTF8, |ctx| {
        let value: Result<String> = ctx.get(0);
        match value {
            Ok(v) if UuidHandler::validate_uuid(&v) => Ok(Some(UuidHandler::normalize_uuid(&v))),
            _ => Ok(None),
        }
    })?;

    // Create a collation for UUID comparison (case-insensitive)
    conn.create_collation("uuid", |a, b| a.to_lowercase().cmp(&b.to_lowercase()))?;

    Ok(())
}

static UUID_V1_CONTEXT: Lazy<Context> = Lazy::new(|| Context::new(rand::random::<u16>()));
static UUID_V1_NODE_ID: Lazy<[u8; 6]> = Lazy::new(|| {
    let mut node = [0u8; 6];
    let mut rng = rand::rng();
    rng.fill_bytes(&mut node);
    // Set multicast bit to indicate random node ID
    node[0] |= 0x01;
    node
});

fn generate_uuid_v1(multicast_node: bool) -> String {
    let ts = chrono::Utc::now();
    let secs = ts.timestamp() as u64;
    let nanos = ts.timestamp_subsec_nanos();
    let timestamp = Timestamp::from_unix(&*UUID_V1_CONTEXT, secs, nanos);

    let node_id = if multicast_node {
        let mut node = [0u8; 6];
        let mut rng = rand::rng();
        rng.fill_bytes(&mut node);
        node[0] |= 0x01;
        node
    } else {
        *UUID_V1_NODE_ID
    };

    Uuid::new_v1(timestamp, &node_id).to_string()
}

fn parse_uuid_arg(value: &str) -> std::result::Result<Uuid, String> {
    Uuid::parse_str(value).map_err(|_| format!("invalid UUID: {value}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn test_uuid_functions() {
        let conn = Connection::open_in_memory().unwrap();
        register_uuid_functions(&conn).unwrap();

        // Test gen_random_uuid
        let uuid: String = conn
            .query_row("SELECT gen_random_uuid()", [], |row| row.get(0))
            .unwrap();
        assert!(UuidHandler::validate_uuid(&uuid));

        // Test uuid_generate_v4
        let uuid2: String = conn
            .query_row("SELECT uuid_generate_v4()", [], |row| row.get(0))
            .unwrap();
        assert!(UuidHandler::validate_uuid(&uuid2));
        assert_ne!(uuid, uuid2); // Should generate different UUIDs

        // Test uuid_generate_v1
        let uuid_v1: String = conn
            .query_row("SELECT uuid_generate_v1()", [], |row| row.get(0))
            .unwrap();
        assert!(UuidHandler::validate_uuid(&uuid_v1));
        assert_eq!(&uuid_v1[14..15], "1");

        // Test uuid_generate_v1mc
        let uuid_v1mc: String = conn
            .query_row("SELECT uuid_generate_v1mc()", [], |row| row.get(0))
            .unwrap();
        assert!(UuidHandler::validate_uuid(&uuid_v1mc));
        assert_eq!(&uuid_v1mc[14..15], "1");

        // Test uuid_nil
        let nil: String = conn
            .query_row("SELECT uuid_nil()", [], |row| row.get(0))
            .unwrap();
        assert_eq!(nil, "00000000-0000-0000-0000-000000000000");

        // Test namespace constants and v3/v5
        let dns: String = conn
            .query_row("SELECT uuid_ns_dns()", [], |row| row.get(0))
            .unwrap();
        assert!(UuidHandler::validate_uuid(&dns));
        let v3: String = conn
            .query_row(
                "SELECT uuid_generate_v3(uuid_ns_url(), 'http://www.postgresql.org')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(UuidHandler::validate_uuid(&v3));
        assert_eq!(&v3[14..15], "3");
        let v5: String = conn
            .query_row(
                "SELECT uuid_generate_v5(uuid_ns_url(), 'http://www.postgresql.org')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(UuidHandler::validate_uuid(&v5));
        assert_eq!(&v5[14..15], "5");

        // Test is_valid_uuid
        let valid: bool = conn
            .query_row(
                "SELECT is_valid_uuid(?)",
                ["550e8400-e29b-41d4-a716-446655440000"],
                |row| row.get(0),
            )
            .unwrap();
        assert!(valid);

        let invalid: bool = conn
            .query_row("SELECT is_valid_uuid(?)", ["not-a-uuid"], |row| row.get(0))
            .unwrap();
        assert!(!invalid);

        // Test uuid_normalize
        let normalized: String = conn
            .query_row(
                "SELECT uuid_normalize(?)",
                ["550E8400-E29B-41D4-A716-446655440000"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(normalized, "550e8400-e29b-41d4-a716-446655440000");

        // Test UUID collation
        conn.execute("CREATE TABLE test_uuid (id TEXT COLLATE uuid)", [])
            .unwrap();
        conn.execute(
            "INSERT INTO test_uuid VALUES (?), (?)",
            [
                "550E8400-E29B-41D4-A716-446655440000",
                "550e8400-e29b-41d4-a716-446655440000",
            ],
        )
        .unwrap();

        let count: i32 = conn
            .query_row("SELECT COUNT(DISTINCT id) FROM test_uuid", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 1); // Should be treated as same UUID due to collation
    }
}
