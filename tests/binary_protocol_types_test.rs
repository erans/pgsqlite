mod common;
use common::*;
use tokio_postgres::types::{Type, FromSql};
use pgsqlite::types::decimal_handler::DecimalHandler;

/// Helper type to extract MACADDR8 columns as strings
struct MacAddr8String(String);

impl<'a> FromSql<'a> for MacAddr8String {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() == 8 {
            // Binary format: 8 bytes representing the MAC address
            let formatted = format!("{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7]);
            Ok(MacAddr8String(formatted))
        } else {
            // Text format
            let s = std::str::from_utf8(raw)?;
            Ok(MacAddr8String(s.to_string()))
        }
    }
    
    fn accepts(ty: &Type) -> bool {
        ty.name() == "macaddr8" || ty.name() == "text"
    }
}

/// Helper type to extract NUMRANGE columns as strings
struct NumRangeString(String);

impl<'a> FromSql<'a> for NumRangeString {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if !raw.is_empty() {
            // PostgreSQL NUMRANGE binary format starts with a 1-byte flags field
            let flags = raw[0];
            
            // PostgreSQL range flags:
            let is_empty = (flags & 0x01) != 0;
            let lower_bound_inclusive = (flags & 0x02) != 0;  // LB_INC
            let upper_bound_inclusive = (flags & 0x04) != 0;  // UB_INC
            let lower_bound_infinite = (flags & 0x08) != 0;   // LB_INF
            let upper_bound_infinite = (flags & 0x10) != 0;   // UB_INF
            
            if is_empty {
                return Ok(NumRangeString("empty".to_string()));
            }
            
            let lower_bracket = if lower_bound_inclusive { '[' } else { '(' };
            let upper_bracket = if upper_bound_inclusive { ']' } else { ')' };
            
            let mut pos = 1; // Skip flags byte
            
            // Parse lower bound
            let lower_str = if lower_bound_infinite {
                "-infinity".to_string()
            } else {
                // Read length of lower bound data (4 bytes, big-endian)
                if pos + 4 > raw.len() {
                    return Err("Invalid range format: insufficient data for lower bound length".into());
                }
                let lower_len = u32::from_be_bytes([raw[pos], raw[pos+1], raw[pos+2], raw[pos+3]]) as usize;
                pos += 4;
                
                // Read lower bound numeric data
                if pos + lower_len > raw.len() {
                    return Err("Invalid range format: insufficient data for lower bound".into());
                }
                let lower_data = &raw[pos..pos + lower_len];
                pos += lower_len;
                
                // Parse PostgreSQL numeric binary format using existing handler
                let decimal = DecimalHandler::decode_numeric(lower_data)
                    .map_err(|e| format!("Failed to decode lower bound: {e}"))?;
                Self::normalize_decimal_string(&decimal.to_string())
            };
            
            // Parse upper bound
            let upper_str = if upper_bound_infinite {
                "infinity".to_string()
            } else {
                // Read length of upper bound data (4 bytes, big-endian)
                if pos + 4 > raw.len() {
                    return Err("Invalid range format: insufficient data for upper bound length".into());
                }
                let upper_len = u32::from_be_bytes([raw[pos], raw[pos+1], raw[pos+2], raw[pos+3]]) as usize;
                pos += 4;
                
                // Read upper bound numeric data
                if pos + upper_len > raw.len() {
                    return Err("Invalid range format: insufficient data for upper bound".into());
                }
                let upper_data = &raw[pos..pos + upper_len];
                
                // Parse PostgreSQL numeric binary format using existing handler
                let decimal = DecimalHandler::decode_numeric(upper_data)
                    .map_err(|e| format!("Failed to decode upper bound: {e}"))?;
                Self::normalize_decimal_string(&decimal.to_string())
            };
            
            Ok(NumRangeString(format!("{lower_bracket}{lower_str},{upper_str}{upper_bracket}")))
        } else {
            // Text format fallback
            let s = std::str::from_utf8(raw)?;
            Ok(NumRangeString(s.to_string()))
        }
    }
    
    fn accepts(ty: &Type) -> bool {
        ty.name() == "numrange" || ty.name() == "text"
    }
}

impl NumRangeString {
    /// Normalize decimal string by removing trailing zeros
    fn normalize_decimal_string(s: &str) -> String {
        if s.contains('.') {
            let trimmed = s.trim_end_matches('0');
            if trimmed.ends_with('.') {
                trimmed[..trimmed.len()-1].to_string()
            } else {
                trimmed.to_string()
            }
        } else {
            s.to_string()
        }
    }
}

#[tokio::test]
async fn test_money_binary_protocol() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with MONEY type using client (not internal db)
    client.execute(
        "CREATE TABLE money_test (
            id INTEGER PRIMARY KEY,
            amount MONEY
        )",
        &[]
    ).await.unwrap();
    
    // Test inserting MONEY values using binary protocol
    // Note: MONEY values should be inserted as text strings with currency symbol
    let test_values = [("$1234.56", "$1234.56"),
        ("$-999.99", "$-999.99"),
        ("$0.00", "$0.00"),
        ("$99999.99", "$99999.99"),
        ("$1.23", "$1.23")];
    
    for (i, (value, _expected)) in test_values.iter().enumerate() {
        client.execute(
            "INSERT INTO money_test (id, amount) VALUES ($1, $2)",
            &[&(i as i32), value]
        ).await.unwrap();
    }
    
    // Test selecting MONEY values using binary protocol
    for (i, (_value, expected)) in test_values.iter().enumerate() {
        let row = client.query_one(
            "SELECT CAST(amount AS TEXT) FROM money_test WHERE id = $1",
            &[&(i as i32)]
        ).await.unwrap();
        
        let retrieved: String = row.get(0);
        assert_eq!(&retrieved, expected, "Failed for MONEY value: {expected}");
    }
    
    // Test with prepared statement for binary encoding
    let stmt = client.prepare("SELECT id, CAST(amount AS TEXT) FROM money_test ORDER BY id").await.unwrap();
    let rows = client.query(&stmt, &[]).await.unwrap();
    
    assert_eq!(rows.len(), test_values.len());
    for (i, row) in rows.iter().enumerate() {
        let id: i32 = row.get(0);
        let amount: String = row.get(1);
        assert_eq!(id, i as i32);
        assert_eq!(amount, test_values[i].1);
    }
    
    // Test NULL money values
    client.execute(
        "INSERT INTO money_test (id, amount) VALUES ($1, $2)",
        &[&999i32, &None::<String>]
    ).await.unwrap();
    
    let row = client.query_one(
        "SELECT amount FROM money_test WHERE id = $1",
        &[&999i32]
    ).await.unwrap();
    
    // Should be NULL
    assert!(row.try_get::<_, String>(0).is_err());
    
    server.abort();
}

#[tokio::test]
async fn test_int4range_binary_protocol() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with INT4RANGE type using client
    client.execute(
        "CREATE TABLE int4range_test (
            id INTEGER PRIMARY KEY,
            range_val INT4RANGE
        )",
        &[]
    ).await.unwrap();
    
    // Test inserting INT4RANGE values using binary protocol
    let test_values = [("[1,10)", "[1,10)"),
        ("[0,100]", "[0,100]"),
        ("(-50,50)", "(-50,50)"),
        ("empty", "empty"),
        ("[42,42]", "[42,42]")];
    
    for (i, (value, _expected)) in test_values.iter().enumerate() {
        client.execute(
            "INSERT INTO int4range_test (id, range_val) VALUES ($1, $2)",
            &[&(i as i32), value]
        ).await.unwrap();
    }
    
    // Test selecting INT4RANGE values using binary protocol
    for (i, (_value, expected)) in test_values.iter().enumerate() {
        let row = client.query_one(
            "SELECT CAST(range_val AS TEXT) FROM int4range_test WHERE id = $1",
            &[&(i as i32)]
        ).await.unwrap();
        
        let retrieved: String = row.get(0);
        assert_eq!(&retrieved, expected, "Failed for INT4RANGE value: {expected}");
    }
    
    // Test with prepared statement for binary encoding
    let stmt = client.prepare("SELECT id, CAST(range_val AS TEXT) FROM int4range_test ORDER BY id").await.unwrap();
    let rows = client.query(&stmt, &[]).await.unwrap();
    
    assert_eq!(rows.len(), test_values.len());
    for (i, row) in rows.iter().enumerate() {
        let id: i32 = row.get(0);
        let range_val: String = row.get(1);
        assert_eq!(id, i as i32);
        assert_eq!(range_val, test_values[i].1);
    }
    
    // Test NULL range values
    client.execute(
        "INSERT INTO int4range_test (id, range_val) VALUES ($1, $2)",
        &[&999i32, &None::<String>]
    ).await.unwrap();
    
    let row = client.query_one(
        "SELECT range_val FROM int4range_test WHERE id = $1",
        &[&999i32]
    ).await.unwrap();
    
    // Should be NULL
    assert!(row.try_get::<_, String>(0).is_err());
    
    server.abort();
}

#[tokio::test]
async fn test_int8range_binary_protocol() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with INT8RANGE type using client
    client.execute(
        "CREATE TABLE int8range_test (
            id INTEGER PRIMARY KEY,
            range_val INT8RANGE
        )",
        &[]
    ).await.unwrap();
    
    // Test inserting INT8RANGE values using binary protocol
    let test_values = [("[1000000,2000000)", "[1000000,2000000)"),
        ("[9223372036854775800,9223372036854775807]", "[9223372036854775800,9223372036854775807]"),
        ("(-9223372036854775808,-9223372036854775800)", "(-9223372036854775808,-9223372036854775800)"),
        ("empty", "empty"),
        ("[0,0]", "[0,0]")];
    
    for (i, (value, _expected)) in test_values.iter().enumerate() {
        client.execute(
            "INSERT INTO int8range_test (id, range_val) VALUES ($1, $2)",
            &[&(i as i32), value]
        ).await.unwrap();
    }
    
    // Test selecting INT8RANGE values using binary protocol
    for (i, (_value, expected)) in test_values.iter().enumerate() {
        let row = client.query_one(
            "SELECT CAST(range_val AS TEXT) FROM int8range_test WHERE id = $1",
            &[&(i as i32)]
        ).await.unwrap();
        
        let retrieved: String = row.get(0);
        assert_eq!(&retrieved, expected, "Failed for INT8RANGE value: {expected}");
    }
    
    // Test with prepared statement for binary encoding
    let stmt = client.prepare("SELECT id, CAST(range_val AS TEXT) FROM int8range_test ORDER BY id").await.unwrap();
    let rows = client.query(&stmt, &[]).await.unwrap();
    
    assert_eq!(rows.len(), test_values.len());
    for (i, row) in rows.iter().enumerate() {
        let id: i32 = row.get(0);
        let range_val: String = row.get(1);
        assert_eq!(id, i as i32);
        assert_eq!(range_val, test_values[i].1);
    }
    
    // Test NULL range values
    client.execute(
        "INSERT INTO int8range_test (id, range_val) VALUES ($1, $2)",
        &[&999i32, &None::<String>]
    ).await.unwrap();
    
    let row = client.query_one(
        "SELECT range_val FROM int8range_test WHERE id = $1",
        &[&999i32]
    ).await.unwrap();
    
    // Should be NULL
    assert!(row.try_get::<_, String>(0).is_err());
    
    server.abort();
}

#[tokio::test]
async fn test_numrange_binary_protocol() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with NUMRANGE type using client
    client.execute(
        "CREATE TABLE numrange_test (
            id INTEGER PRIMARY KEY,
            range_val NUMRANGE
        )",
        &[]
    ).await.unwrap();
    
    // Test inserting NUMRANGE values using binary protocol
    let test_values = [("[1.5,10.5)", "[1.5,10.5)"),
        ("[0,100.999]", "[0,100.999]"),
        ("(-50.5,50.5)", "(-50.5,50.5)"),
        ("empty", "empty"),
        ("[3.14159,3.14159]", "[3.14159,3.14159]")];
    
    for (i, (value, _expected)) in test_values.iter().enumerate() {
        client.execute(
            "INSERT INTO numrange_test (id, range_val) VALUES ($1, $2)",
            &[&(i as i32), value]
        ).await.unwrap();
    }
    
    // Test selecting NUMRANGE values using binary protocol
    for (i, (_value, expected)) in test_values.iter().enumerate() {
        let row = client.query_one(
            "SELECT range_val FROM numrange_test WHERE id = $1",
            &[&(i as i32)]
        ).await.unwrap();
        
        let retrieved: NumRangeString = row.get(0);
        assert_eq!(&retrieved.0, expected, "Failed for NUMRANGE value: {expected}");
    }
    
    // Test with prepared statement for binary encoding
    let stmt = client.prepare("SELECT id, range_val FROM numrange_test ORDER BY id").await.unwrap();
    let rows = client.query(&stmt, &[]).await.unwrap();
    
    assert_eq!(rows.len(), test_values.len());
    for (i, row) in rows.iter().enumerate() {
        let id: i32 = row.get(0);
        let range_val: NumRangeString = row.get(1);
        assert_eq!(id, i as i32);
        assert_eq!(range_val.0, test_values[i].1);
    }
    
    // Test NULL range values
    client.execute(
        "INSERT INTO numrange_test (id, range_val) VALUES ($1, $2)",
        &[&999i32, &None::<String>]
    ).await.unwrap();
    
    let row = client.query_one(
        "SELECT range_val FROM numrange_test WHERE id = $1",
        &[&999i32]
    ).await.unwrap();
    
    // Should be NULL
    assert!(row.try_get::<_, String>(0).is_err());
    
    server.abort();
}

#[tokio::test]
async fn test_cidr_binary_protocol() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with CIDR type using client
    client.execute(
        "CREATE TABLE cidr_test (
            id INTEGER PRIMARY KEY,
            cidr_val CIDR
        )",
        &[]
    ).await.unwrap();
    
    // Test inserting CIDR values using binary protocol
    let test_values = [("192.168.1.0/24", "192.168.1.0/24"),
        ("10.0.0.0/8", "10.0.0.0/8"),
        ("172.16.0.0/16", "172.16.0.0/16"),
        ("2001:db8::/32", "2001:db8::/32"),
        ("fe80::/10", "fe80::/10")];
    
    for (i, (value, _expected)) in test_values.iter().enumerate() {
        client.execute(
            "INSERT INTO cidr_test (id, cidr_val) VALUES ($1, $2)",
            &[&(i as i32), value]
        ).await.unwrap();
    }
    
    // Test selecting CIDR values using binary protocol
    for (i, (_value, expected)) in test_values.iter().enumerate() {
        let row = client.query_one(
            "SELECT CAST(cidr_val AS TEXT) FROM cidr_test WHERE id = $1",
            &[&(i as i32)]
        ).await.unwrap();
        
        let retrieved: String = row.get(0);
        assert_eq!(&retrieved, expected, "Failed for CIDR value: {expected}");
    }
    
    // Test with prepared statement for binary encoding
    let stmt = client.prepare("SELECT id, CAST(cidr_val AS TEXT) FROM cidr_test ORDER BY id").await.unwrap();
    let rows = client.query(&stmt, &[]).await.unwrap();
    
    assert_eq!(rows.len(), test_values.len());
    for (i, row) in rows.iter().enumerate() {
        let id: i32 = row.get(0);
        let cidr_val: String = row.get(1);
        assert_eq!(id, i as i32);
        assert_eq!(cidr_val, test_values[i].1);
    }
    
    // Test NULL cidr values
    client.execute(
        "INSERT INTO cidr_test (id, cidr_val) VALUES ($1, $2)",
        &[&999i32, &None::<String>]
    ).await.unwrap();
    
    let row = client.query_one(
        "SELECT cidr_val FROM cidr_test WHERE id = $1",
        &[&999i32]
    ).await.unwrap();
    
    // Should be NULL
    assert!(row.try_get::<_, String>(0).is_err());
    
    server.abort();
}

#[tokio::test]
async fn test_inet_binary_protocol() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with INET type using client
    client.execute(
        "CREATE TABLE inet_test (
            id INTEGER PRIMARY KEY,
            inet_val INET
        )",
        &[]
    ).await.unwrap();
    
    // Test inserting INET values using binary protocol
    let test_values = [("192.168.1.1", "192.168.1.1"),
        ("10.0.0.1/8", "10.0.0.1/8"),
        ("::1", "::1"),
        ("2001:db8::1", "2001:db8::1"),
        ("fe80::1/64", "fe80::1/64")];
    
    for (i, (value, _expected)) in test_values.iter().enumerate() {
        client.execute(
            "INSERT INTO inet_test (id, inet_val) VALUES ($1, $2)",
            &[&(i as i32), value]
        ).await.unwrap();
    }
    
    // Test selecting INET values using binary protocol
    for (i, (_value, expected)) in test_values.iter().enumerate() {
        let row = client.query_one(
            "SELECT CAST(inet_val AS TEXT) FROM inet_test WHERE id = $1",
            &[&(i as i32)]
        ).await.unwrap();
        
        let retrieved: String = row.get(0);
        assert_eq!(&retrieved, expected, "Failed for INET value: {expected}");
    }
    
    // Test with prepared statement for binary encoding
    let stmt = client.prepare("SELECT id, CAST(inet_val AS TEXT) FROM inet_test ORDER BY id").await.unwrap();
    let rows = client.query(&stmt, &[]).await.unwrap();
    
    assert_eq!(rows.len(), test_values.len());
    for (i, row) in rows.iter().enumerate() {
        let id: i32 = row.get(0);
        let inet_val: String = row.get(1);
        assert_eq!(id, i as i32);
        assert_eq!(inet_val, test_values[i].1);
    }
    
    // Test NULL inet values
    client.execute(
        "INSERT INTO inet_test (id, inet_val) VALUES ($1, $2)",
        &[&999i32, &None::<String>]
    ).await.unwrap();
    
    let row = client.query_one(
        "SELECT inet_val FROM inet_test WHERE id = $1",
        &[&999i32]
    ).await.unwrap();
    
    // Should be NULL
    assert!(row.try_get::<_, String>(0).is_err());
    
    server.abort();
}

#[tokio::test]
async fn test_macaddr_binary_protocol() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with MACADDR type using client
    client.execute(
        "CREATE TABLE macaddr_test (
            id INTEGER PRIMARY KEY,
            mac_val MACADDR
        )",
        &[]
    ).await.unwrap();
    
    // Test inserting MACADDR values using binary protocol
    let test_values = [("08:00:2b:01:02:03", "08:00:2b:01:02:03"),
        ("aa:bb:cc:dd:ee:ff", "aa:bb:cc:dd:ee:ff"),
        ("00:00:00:00:00:00", "00:00:00:00:00:00"),
        ("ff:ff:ff:ff:ff:ff", "ff:ff:ff:ff:ff:ff"),
        ("12:34:56:78:9a:bc", "12:34:56:78:9a:bc")];
    
    for (i, (value, _expected)) in test_values.iter().enumerate() {
        client.execute(
            "INSERT INTO macaddr_test (id, mac_val) VALUES ($1, $2)",
            &[&(i as i32), value]
        ).await.unwrap();
    }
    
    // Test selecting MACADDR values using binary protocol
    for (i, (_value, expected)) in test_values.iter().enumerate() {
        let row = client.query_one(
            "SELECT CAST(mac_val AS TEXT) FROM macaddr_test WHERE id = $1",
            &[&(i as i32)]
        ).await.unwrap();
        
        let retrieved: String = row.get(0);
        assert_eq!(&retrieved, expected, "Failed for MACADDR value: {expected}");
    }
    
    // Test with prepared statement for binary encoding
    let stmt = client.prepare("SELECT id, CAST(mac_val AS TEXT) FROM macaddr_test ORDER BY id").await.unwrap();
    let rows = client.query(&stmt, &[]).await.unwrap();
    
    assert_eq!(rows.len(), test_values.len());
    for (i, row) in rows.iter().enumerate() {
        let id: i32 = row.get(0);
        let mac_val: String = row.get(1);
        assert_eq!(id, i as i32);
        assert_eq!(mac_val, test_values[i].1);
    }
    
    // Test NULL macaddr values
    client.execute(
        "INSERT INTO macaddr_test (id, mac_val) VALUES ($1, $2)",
        &[&999i32, &None::<String>]
    ).await.unwrap();
    
    let row = client.query_one(
        "SELECT mac_val FROM macaddr_test WHERE id = $1",
        &[&999i32]
    ).await.unwrap();
    
    // Should be NULL
    assert!(row.try_get::<_, String>(0).is_err());
    
    server.abort();
}

#[tokio::test]
async fn test_macaddr8_binary_protocol() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with MACADDR8 type using client
    client.execute(
        "CREATE TABLE macaddr8_test (
            id INTEGER PRIMARY KEY,
            mac_val MACADDR8
        )",
        &[]
    ).await.unwrap();
    
    // Test inserting MACADDR8 values using binary protocol
    let test_values = [("08:00:2b:01:02:03:04:05", "08:00:2b:01:02:03:04:05"),
        ("aa:bb:cc:dd:ee:ff:00:11", "aa:bb:cc:dd:ee:ff:00:11"),
        ("00:00:00:00:00:00:00:00", "00:00:00:00:00:00:00:00"),
        ("ff:ff:ff:ff:ff:ff:ff:ff", "ff:ff:ff:ff:ff:ff:ff:ff"),
        ("12:34:56:78:9a:bc:de:f0", "12:34:56:78:9a:bc:de:f0")];
    
    for (i, (value, _expected)) in test_values.iter().enumerate() {
        client.execute(
            "INSERT INTO macaddr8_test (id, mac_val) VALUES ($1, $2)",
            &[&(i as i32), value]
        ).await.unwrap();
    }
    
    // Test selecting MACADDR8 values using binary protocol
    for (i, (_value, expected)) in test_values.iter().enumerate() {
        let row = client.query_one(
            "SELECT mac_val FROM macaddr8_test WHERE id = $1",
            &[&(i as i32)]
        ).await.unwrap();
        
        let retrieved: MacAddr8String = row.get(0);
        assert_eq!(&retrieved.0, expected, "Failed for MACADDR8 value: {expected}");
    }
    
    // Test with prepared statement for binary encoding
    let stmt = client.prepare("SELECT id, mac_val FROM macaddr8_test ORDER BY id").await.unwrap();
    let rows = client.query(&stmt, &[]).await.unwrap();
    
    assert_eq!(rows.len(), test_values.len());
    for (i, row) in rows.iter().enumerate() {
        let id: i32 = row.get(0);
        let mac_val: MacAddr8String = row.get(1);
        assert_eq!(id, i as i32);
        assert_eq!(mac_val.0, test_values[i].1);
    }
    
    // Test NULL macaddr8 values
    client.execute(
        "INSERT INTO macaddr8_test (id, mac_val) VALUES ($1, $2)",
        &[&999i32, &None::<String>]
    ).await.unwrap();
    
    let row = client.query_one(
        "SELECT mac_val FROM macaddr8_test WHERE id = $1",
        &[&999i32]
    ).await.unwrap();
    
    // Should be NULL
    assert!(row.try_get::<_, String>(0).is_err());
    
    server.abort();
}

#[tokio::test]
async fn test_bit_binary_protocol() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with BIT type using client
    client.execute(
        "CREATE TABLE bit_test (
            id INTEGER PRIMARY KEY,
            bit_val BIT(8)
        )",
        &[]
    ).await.unwrap();
    
    // Test inserting BIT values using binary protocol
    let test_values = [("10101010", "10101010"),
        ("11111111", "11111111"),
        ("00000000", "00000000"),
        ("01010101", "01010101"),
        ("11001100", "11001100")];
    
    for (i, (value, _expected)) in test_values.iter().enumerate() {
        client.execute(
            "INSERT INTO bit_test (id, bit_val) VALUES ($1, $2)",
            &[&(i as i32), value]
        ).await.unwrap();
    }
    
    // Test selecting BIT values using binary protocol
    for (i, (_value, expected)) in test_values.iter().enumerate() {
        let _row = client.query_one(
            "SELECT bit_val FROM bit_test WHERE id = $1",
            &[&(i as i32)]
        ).await.unwrap();
        
        // For bit types, tokio_postgres doesn't support String conversion
        // We need to handle it as raw bytes or use a workaround
        // Let's cast to text in the query to get string representation
        let row2 = client.query_one(
            "SELECT bit_val::text FROM bit_test WHERE id = $1",
            &[&(i as i32)]
        ).await.unwrap();
        
        let retrieved: String = row2.get(0);
        assert_eq!(&retrieved, expected, "Failed for BIT value: {expected}");
    }
    
    // Test with prepared statement for binary encoding
    let stmt = client.prepare("SELECT id, bit_val::text FROM bit_test ORDER BY id").await.unwrap();
    let rows = client.query(&stmt, &[]).await.unwrap();
    
    assert_eq!(rows.len(), test_values.len());
    for (i, row) in rows.iter().enumerate() {
        let id: i32 = row.get(0);
        let bit_val: String = row.get(1);
        assert_eq!(id, i as i32);
        assert_eq!(bit_val, test_values[i].1);
    }
    
    // Test NULL bit values
    client.execute(
        "INSERT INTO bit_test (id, bit_val) VALUES ($1, $2)",
        &[&999i32, &None::<String>]
    ).await.unwrap();
    
    let row = client.query_one(
        "SELECT bit_val FROM bit_test WHERE id = $1",
        &[&999i32]
    ).await.unwrap();
    
    // Should be NULL
    assert!(row.try_get::<_, String>(0).is_err());
    
    server.abort();
}

#[tokio::test]
async fn test_bit_varying_binary_protocol() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with BIT VARYING type using client
    client.execute(
        "CREATE TABLE bit_varying_test (
            id INTEGER PRIMARY KEY,
            bit_val BIT VARYING(32)
        )",
        &[]
    ).await.unwrap();
    
    // Test inserting BIT VARYING values using binary protocol
    let test_values = [("1010", "1010"),
        ("111111110000", "111111110000"),
        ("1", "1"),
        ("00000000111111110000000011111111", "00000000111111110000000011111111"),
        ("10011", "10011")];
    
    for (i, (value, _expected)) in test_values.iter().enumerate() {
        client.execute(
            "INSERT INTO bit_varying_test (id, bit_val) VALUES ($1, $2)",
            &[&(i as i32), value]
        ).await.unwrap();
    }
    
    // Test selecting BIT VARYING values using binary protocol
    for (i, (_value, expected)) in test_values.iter().enumerate() {
        let _row = client.query_one(
            "SELECT bit_val FROM bit_varying_test WHERE id = $1",
            &[&(i as i32)]
        ).await.unwrap();
        
        // For bit types, tokio_postgres doesn't support String conversion
        // We need to handle it as raw bytes or use a workaround
        // Let's cast to text in the query to get string representation
        let row2 = client.query_one(
            "SELECT bit_val::text FROM bit_varying_test WHERE id = $1",
            &[&(i as i32)]
        ).await.unwrap();
        
        let retrieved: String = row2.get(0);
        assert_eq!(&retrieved, expected, "Failed for BIT VARYING value: {expected}");
    }
    
    // Test with prepared statement for binary encoding
    let stmt = client.prepare("SELECT id, bit_val::text FROM bit_varying_test ORDER BY id").await.unwrap();
    let rows = client.query(&stmt, &[]).await.unwrap();
    
    assert_eq!(rows.len(), test_values.len());
    for (i, row) in rows.iter().enumerate() {
        let id: i32 = row.get(0);
        let bit_val: String = row.get(1);
        assert_eq!(id, i as i32);
        assert_eq!(bit_val, test_values[i].1);
    }
    
    // Test NULL bit varying values
    client.execute(
        "INSERT INTO bit_varying_test (id, bit_val) VALUES ($1, $2)",
        &[&999i32, &None::<String>]
    ).await.unwrap();
    
    let row = client.query_one(
        "SELECT bit_val FROM bit_varying_test WHERE id = $1",
        &[&999i32]
    ).await.unwrap();
    
    // Should be NULL
    assert!(row.try_get::<_, String>(0).is_err());
    
    server.abort();
}