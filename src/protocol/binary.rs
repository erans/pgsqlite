use bytes::{BufMut, BytesMut};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use std::convert::TryInto;
use std::str::FromStr;
use crate::types::{PgType, DecimalHandler};

/// Binary format encoders for PostgreSQL types
pub struct BinaryEncoder;

impl BinaryEncoder {
    /// Encode a boolean value (OID 16)
    #[inline]
    pub fn encode_bool(value: bool) -> Vec<u8> {
        vec![if value { 1 } else { 0 }]
    }

    /// Encode an int2/smallint value (OID 21)
    #[inline]
    pub fn encode_int2(value: i16) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    /// Encode an int4/integer value (OID 23)
    #[inline]
    pub fn encode_int4(value: i32) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    /// Encode an int8/bigint value (OID 20)
    #[inline]
    pub fn encode_int8(value: i64) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    /// Encode a float4/real value (OID 700)
    #[inline]
    pub fn encode_float4(value: f32) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    /// Encode a float8/double precision value (OID 701)
    #[inline]
    pub fn encode_float8(value: f64) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    /// Encode a text/varchar value (OID 25, 1043)
    /// Binary format is the same as text format for these types
    #[inline]
    pub fn encode_text(value: &str) -> Vec<u8> {
        value.as_bytes().to_vec()
    }

    /// Encode a bytea value (OID 17)
    /// Binary format is just the raw bytes
    #[inline]
    pub fn encode_bytea(value: &[u8]) -> Vec<u8> {
        value.to_vec()
    }

    /// Encode a numeric/decimal value (OID 1700)
    /// Uses PostgreSQL's binary NUMERIC format
    pub fn encode_numeric(value: &Decimal) -> Vec<u8> {
        DecimalHandler::encode_numeric(value)
    }
    
    /// Encode a UUID value (OID 2950)
    /// Binary format is 16 bytes raw UUID
    pub fn encode_uuid(uuid_str: &str) -> Result<Vec<u8>, String> {
        // Remove hyphens and validate length
        let hex_str = uuid_str.replace('-', "");
        if hex_str.len() != 32 {
            return Err("Invalid UUID format".to_string());
        }
        
        // Convert hex string to bytes
        let mut bytes = Vec::with_capacity(16);
        for i in (0..32).step_by(2) {
            let byte = u8::from_str_radix(&hex_str[i..i+2], 16)
                .map_err(|_| "Invalid UUID hex characters")?;
            bytes.push(byte);
        }
        
        Ok(bytes)
    }
    
    /// Encode JSON value (OID 114)
    /// Binary format is the same as text for JSON
    pub fn encode_json(json_str: &str) -> Vec<u8> {
        json_str.as_bytes().to_vec()
    }
    
    /// Encode JSONB value (OID 3802)
    /// Binary format has a 1-byte version header
    pub fn encode_jsonb(json_str: &str) -> Vec<u8> {
        let mut result = Vec::with_capacity(json_str.len() + 1);
        result.push(1); // JSONB version 1
        result.extend_from_slice(json_str.as_bytes());
        result
    }
    
    /// Encode MONEY value (OID 790)
    /// Binary format is 8-byte integer representing cents * 100
    pub fn encode_money(amount_str: &str) -> Result<Vec<u8>, String> {
        // Parse the string, removing currency symbols and commas
        let clean_str = amount_str
            .replace('$', "")
            .replace(',', "")
            .trim()
            .to_string();
        
        // Parse as decimal to handle fractional cents
        let decimal = Decimal::from_str(&clean_str)
            .map_err(|e| format!("Invalid money value: {e}"))?;
        
        // Convert to cents (multiply by 100)
        let cents = decimal * Decimal::from(100);
        
        // Convert to i64
        let cents_i64 = cents.round().to_i64()
            .ok_or_else(|| "Money value too large".to_string())?;
        
        Ok(cents_i64.to_be_bytes().to_vec())
    }
    
    /// Encode an array value
    /// PostgreSQL array binary format:
    /// - ndim (i32): number of dimensions
    /// - dataoffset (i32): offset to data, 0 if no NULLs
    /// - elemtype (i32): element type OID
    /// - For each dimension:
    ///   - dim_size (i32): number of elements in this dimension
    ///   - lower_bound (i32): lower bound (typically 1)
    /// - NULL bitmap (optional): bit array indicating NULL positions
    /// - Elements: each prefixed with length (i32), -1 for NULL
    pub fn encode_array(
        json_array_str: &str,
        elem_type_oid: i32,
    ) -> Result<Vec<u8>, String> {
        // Parse JSON array
        let array: serde_json::Value = serde_json::from_str(json_array_str)
            .map_err(|e| format!("Invalid JSON array: {e}"))?;
        
        let elements = array.as_array()
            .ok_or_else(|| "Not a JSON array".to_string())?;
        
        if elements.is_empty() {
            // Empty array
            let mut result = Vec::new();
            result.extend_from_slice(&0i32.to_be_bytes()); // ndim = 0
            result.extend_from_slice(&0i32.to_be_bytes()); // dataoffset = 0
            result.extend_from_slice(&elem_type_oid.to_be_bytes()); // elemtype
            return Ok(result);
        }
        
        // Check for NULLs
        let has_nulls = elements.iter().any(|e| e.is_null());
        
        let mut result = Vec::new();
        
        // Header
        result.extend_from_slice(&1i32.to_be_bytes()); // ndim = 1 (1D array)
        result.extend_from_slice(&(if has_nulls { 1i32 } else { 0i32 }).to_be_bytes()); // dataoffset placeholder
        result.extend_from_slice(&elem_type_oid.to_be_bytes()); // elemtype
        
        // Dimension info
        result.extend_from_slice(&(elements.len() as i32).to_be_bytes()); // dim_size
        result.extend_from_slice(&1i32.to_be_bytes()); // lower_bound = 1
        
        // NULL bitmap if needed
        let bitmap_start = result.len();
        if has_nulls {
            // Create bitmap (1 bit per element, padded to byte boundary)
            let bitmap_bytes = (elements.len() + 7) / 8;
            let mut bitmap = vec![0u8; bitmap_bytes];
            
            for (i, elem) in elements.iter().enumerate() {
                if !elem.is_null() {
                    let byte_idx = i / 8;
                    let bit_idx = i % 8;
                    bitmap[byte_idx] |= 1 << (7 - bit_idx);
                }
            }
            
            result.extend_from_slice(&bitmap);
        }
        
        // Update dataoffset if we have nulls
        if has_nulls {
            let dataoffset = (bitmap_start + ((elements.len() + 7) / 8)) as i32;
            result[4..8].copy_from_slice(&dataoffset.to_be_bytes());
        }
        
        // Encode elements
        for elem in elements {
            if elem.is_null() {
                // NULL element
                result.extend_from_slice(&(-1i32).to_be_bytes());
            } else {
                // Encode element based on type
                let elem_bytes = match elem_type_oid {
                    t if t == PgType::Int4.to_oid() => {
                        elem.as_i64()
                            .and_then(|v| v.try_into().ok())
                            .map(|v: i32| v.to_be_bytes().to_vec())
                    }
                    t if t == PgType::Int8.to_oid() => {
                        elem.as_i64()
                            .map(|v| v.to_be_bytes().to_vec())
                    }
                    t if t == PgType::Text.to_oid() || t == PgType::Varchar.to_oid() => {
                        elem.as_str()
                            .map(|s| s.as_bytes().to_vec())
                    }
                    t if t == PgType::Float8.to_oid() => {
                        elem.as_f64()
                            .map(|v| v.to_be_bytes().to_vec())
                    }
                    t if t == PgType::Bool.to_oid() => {
                        elem.as_bool()
                            .map(|v| vec![if v { 1 } else { 0 }])
                    }
                    _ => {
                        // Fall back to string representation
                        Some(elem.to_string().into_bytes())
                    }
                };
                
                match elem_bytes {
                    Some(bytes) => {
                        result.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                        result.extend_from_slice(&bytes);
                    }
                    None => {
                        return Err(format!("Cannot encode array element: {:?}", elem));
                    }
                }
            }
        }
        
        Ok(result)
    }
    
    /// Encode DATE (days since 2000-01-01)
    pub fn encode_date(unix_timestamp: f64) -> Vec<u8> {
        // For dates stored as INTEGER days since epoch in SQLite, treat as days
        // For dates stored as REAL Unix timestamps, convert from seconds
        if unix_timestamp < 100000.0 {
            // This looks like days since epoch (1970-01-01), convert to PostgreSQL days since 2000-01-01
            let days_since_1970 = unix_timestamp as i32;
            let days_since_2000 = days_since_1970 - 10957; // 10957 days between 1970-01-01 and 2000-01-01
            days_since_2000.to_be_bytes().to_vec()
        } else {
            // This looks like seconds since epoch, convert to days since 2000-01-01
            const PG_EPOCH_OFFSET: i64 = 946684800; // seconds between 1970-01-01 and 2000-01-01
            const SECS_PER_DAY: i64 = 86400;
            let unix_secs = unix_timestamp.trunc() as i64;
            let pg_days = ((unix_secs - PG_EPOCH_OFFSET) / SECS_PER_DAY) as i32;
            pg_days.to_be_bytes().to_vec()
        }
    }
    
    /// Encode TIME (microseconds since midnight)
    pub fn encode_time(microseconds_since_midnight: f64) -> Vec<u8> {
        // The input is already in microseconds, just convert to i64
        let micros = microseconds_since_midnight.round() as i64;
        micros.to_be_bytes().to_vec()
    }
    
    /// Encode TIMESTAMP/TIMESTAMPTZ (microseconds since epoch to PostgreSQL format)
    pub fn encode_timestamp(unix_microseconds: f64) -> Vec<u8> {
        const PG_EPOCH_OFFSET: i64 = 946684800 * 1_000_000; // microseconds between 1970-01-01 and 2000-01-01
        let unix_micros = unix_microseconds.round() as i64;
        let pg_micros = unix_micros - PG_EPOCH_OFFSET;
        pg_micros.to_be_bytes().to_vec()
    }
    
    /// Encode INTERVAL (microseconds, days, months)
    pub fn encode_interval(total_seconds: f64) -> Vec<u8> {
        // For simple intervals, encode as microseconds + 0 days + 0 months
        let micros = (total_seconds * 1_000_000.0).round() as i64;
        let mut bytes = Vec::with_capacity(16);
        bytes.extend_from_slice(&micros.to_be_bytes());
        bytes.extend_from_slice(&0i32.to_be_bytes()); // days
        bytes.extend_from_slice(&0i32.to_be_bytes()); // months
        bytes
    }

    /// Encode a value based on its PostgreSQL type OID
    pub fn encode_value(value: &rusqlite::types::Value, type_oid: i32, binary_format: bool) -> Option<Vec<u8>> {
        if !binary_format {
            // Text format - use existing converters
            return None;
        }

        // Handle NULL values
        if matches!(value, rusqlite::types::Value::Null) {
            return Some(vec![]);
        }

        // Binary format encoding based on type OID
        match type_oid {
            t if t == PgType::Bool.to_oid() => {
                // BOOL
                match value {
                    rusqlite::types::Value::Integer(i) => Some(Self::encode_bool(*i != 0)),
                    _ => None,
                }
            }
            t if t == PgType::Int2.to_oid() => {
                // INT2
                match value {
                    rusqlite::types::Value::Integer(i) => {
                        if let Ok(v) = (*i).try_into() {
                            Some(Self::encode_int2(v))
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }
            t if t == PgType::Int4.to_oid() => {
                // INT4
                match value {
                    rusqlite::types::Value::Integer(i) => {
                        if let Ok(v) = (*i).try_into() {
                            Some(Self::encode_int4(v))
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }
            t if t == PgType::Int8.to_oid() => {
                // INT8
                match value {
                    rusqlite::types::Value::Integer(i) => Some(Self::encode_int8(*i)),
                    _ => None,
                }
            }
            t if t == PgType::Float4.to_oid() => {
                // FLOAT4
                match value {
                    rusqlite::types::Value::Real(f) => Some(Self::encode_float4(*f as f32)),
                    rusqlite::types::Value::Integer(i) => Some(Self::encode_float4(*i as f32)),
                    _ => None,
                }
            }
            t if t == PgType::Float8.to_oid() => {
                // FLOAT8
                match value {
                    rusqlite::types::Value::Real(f) => Some(Self::encode_float8(*f)),
                    rusqlite::types::Value::Integer(i) => Some(Self::encode_float8(*i as f64)),
                    _ => None,
                }
            }
            t if t == PgType::Bytea.to_oid() => {
                // BYTEA
                match value {
                    rusqlite::types::Value::Blob(b) => Some(Self::encode_bytea(b)),
                    _ => None,
                }
            }
            t if t == PgType::Text.to_oid() || t == PgType::Varchar.to_oid() => {
                // TEXT, VARCHAR - binary format is the same as text
                match value {
                    rusqlite::types::Value::Text(s) => Some(Self::encode_text(s)),
                    _ => None,
                }
            }
            t if t == PgType::Date.to_oid() => {
                // DATE - stored as INTEGER days since epoch (1970-01-01)
                match value {
                    rusqlite::types::Value::Real(f) => Some(Self::encode_date(*f)),
                    rusqlite::types::Value::Integer(i) => {
                        // Convert days since 1970-01-01 to PostgreSQL days since 2000-01-01
                        let days_since_1970 = *i as i32;
                        let days_since_2000 = days_since_1970 - 10957; // 10957 days between 1970-01-01 and 2000-01-01
                        Some(days_since_2000.to_be_bytes().to_vec())
                    },
                    _ => None,
                }
            }
            t if t == PgType::Time.to_oid() || t == PgType::Timetz.to_oid() => {
                // TIME/TIMETZ - stored as microseconds since midnight
                match value {
                    rusqlite::types::Value::Real(f) => Some(Self::encode_time(*f)),
                    rusqlite::types::Value::Integer(i) => Some(Self::encode_time(*i as f64)),
                    _ => None,
                }
            }
            t if t == PgType::Timestamp.to_oid() || t == PgType::Timestamptz.to_oid() => {
                // TIMESTAMP/TIMESTAMPTZ - stored as microseconds since Unix epoch
                match value {
                    rusqlite::types::Value::Real(f) => Some(Self::encode_timestamp(*f)),
                    rusqlite::types::Value::Integer(i) => Some(Self::encode_timestamp(*i as f64)),
                    _ => None,
                }
            }
            t if t == PgType::Interval.to_oid() => {
                // INTERVAL - stored as total seconds
                match value {
                    rusqlite::types::Value::Real(f) => Some(Self::encode_interval(*f)),
                    rusqlite::types::Value::Integer(i) => Some(Self::encode_interval(*i as f64)),
                    _ => None,
                }
            }
            t if t == PgType::Numeric.to_oid() => {
                // NUMERIC/DECIMAL - use proper binary encoding
                match value {
                    rusqlite::types::Value::Text(s) => {
                        // Parse and encode as PostgreSQL numeric
                        match Decimal::from_str(s) {
                            Ok(decimal) => Some(Self::encode_numeric(&decimal)),
                            Err(_) => None,
                        }
                    }
                    rusqlite::types::Value::Real(f) => {
                        // Convert float to decimal (may lose precision)
                        match Decimal::from_f64_retain(*f) {
                            Some(decimal) => Some(Self::encode_numeric(&decimal)),
                            None => None,
                        }
                    }
                    rusqlite::types::Value::Integer(i) => {
                        // Convert integer to decimal
                        match Decimal::from_i64(*i) {
                            Some(decimal) => Some(Self::encode_numeric(&decimal)),
                            None => None,
                        }
                    }
                    _ => None,
                }
            }
            t if t == PgType::Uuid.to_oid() => {
                // UUID - 16 bytes binary
                match value {
                    rusqlite::types::Value::Text(s) => {
                        match Self::encode_uuid(s) {
                            Ok(bytes) => Some(bytes),
                            Err(_) => None,
                        }
                    }
                    _ => None,
                }
            }
            t if t == PgType::Json.to_oid() => {
                // JSON - same as text in binary format
                match value {
                    rusqlite::types::Value::Text(s) => Some(Self::encode_json(s)),
                    _ => None,
                }
            }
            t if t == PgType::Jsonb.to_oid() => {
                // JSONB - with version header
                match value {
                    rusqlite::types::Value::Text(s) => Some(Self::encode_jsonb(s)),
                    _ => None,
                }
            }
            t if t == PgType::Money.to_oid() => {
                // MONEY - 8-byte integer
                match value {
                    rusqlite::types::Value::Text(s) => {
                        match Self::encode_money(s) {
                            Ok(bytes) => Some(bytes),
                            Err(_) => None,
                        }
                    }
                    _ => None,
                }
            }
            // Array types
            t if t == PgType::Int4Array.to_oid() => {
                // INT4 array
                match value {
                    rusqlite::types::Value::Text(s) => {
                        match Self::encode_array(s, PgType::Int4.to_oid()) {
                            Ok(bytes) => Some(bytes),
                            Err(_) => None,
                        }
                    }
                    _ => None,
                }
            }
            t if t == PgType::Int8Array.to_oid() => {
                // INT8 array
                match value {
                    rusqlite::types::Value::Text(s) => {
                        match Self::encode_array(s, PgType::Int8.to_oid()) {
                            Ok(bytes) => Some(bytes),
                            Err(_) => None,
                        }
                    }
                    _ => None,
                }
            }
            t if t == PgType::TextArray.to_oid() => {
                // TEXT array
                match value {
                    rusqlite::types::Value::Text(s) => {
                        match Self::encode_array(s, PgType::Text.to_oid()) {
                            Ok(bytes) => Some(bytes),
                            Err(_) => None,
                        }
                    }
                    _ => None,
                }
            }
            t if t == PgType::Float8Array.to_oid() => {
                // FLOAT8 array
                match value {
                    rusqlite::types::Value::Text(s) => {
                        match Self::encode_array(s, PgType::Float8.to_oid()) {
                            Ok(bytes) => Some(bytes),
                            Err(_) => None,
                        }
                    }
                    _ => None,
                }
            }
            t if t == PgType::BoolArray.to_oid() => {
                // BOOL array
                match value {
                    rusqlite::types::Value::Text(s) => {
                        match Self::encode_array(s, PgType::Bool.to_oid()) {
                            Ok(bytes) => Some(bytes),
                            Err(_) => None,
                        }
                    }
                    _ => None,
                }
            }
            _ => {
                // For other types, fall back to text format
                None
            }
        }
    }
}

/// Zero-copy binary format encoder using BytesMut
pub struct ZeroCopyBinaryEncoder<'a> {
    buffer: &'a mut BytesMut,
}

impl<'a> ZeroCopyBinaryEncoder<'a> {
    pub fn new(buffer: &'a mut BytesMut) -> Self {
        Self { buffer }
    }

    /// Encode a boolean value directly into buffer
    #[inline]
    pub fn encode_bool(&mut self, value: bool) -> usize {
        let start = self.buffer.len();
        self.buffer.put_u8(if value { 1 } else { 0 });
        start
    }

    /// Encode an int2 value directly into buffer
    #[inline]
    pub fn encode_int2(&mut self, value: i16) -> usize {
        let start = self.buffer.len();
        self.buffer.put_i16(value);
        start
    }

    /// Encode an int4 value directly into buffer
    #[inline]
    pub fn encode_int4(&mut self, value: i32) -> usize {
        let start = self.buffer.len();
        self.buffer.put_i32(value);
        start
    }

    /// Encode an int8 value directly into buffer
    #[inline]
    pub fn encode_int8(&mut self, value: i64) -> usize {
        let start = self.buffer.len();
        self.buffer.put_i64(value);
        start
    }

    /// Encode a float4 value directly into buffer
    #[inline]
    pub fn encode_float4(&mut self, value: f32) -> usize {
        let start = self.buffer.len();
        self.buffer.put_f32(value);
        start
    }

    /// Encode a float8 value directly into buffer
    #[inline]
    pub fn encode_float8(&mut self, value: f64) -> usize {
        let start = self.buffer.len();
        self.buffer.put_f64(value);
        start
    }

    /// Encode text value directly into buffer
    #[inline]
    pub fn encode_text(&mut self, value: &str) -> usize {
        let start = self.buffer.len();
        self.buffer.put_slice(value.as_bytes());
        start
    }

    /// Encode bytea value directly into buffer
    #[inline]
    pub fn encode_bytea(&mut self, value: &[u8]) -> usize {
        let start = self.buffer.len();
        self.buffer.put_slice(value);
        start
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_bool() {
        assert_eq!(BinaryEncoder::encode_bool(true), vec![1]);
        assert_eq!(BinaryEncoder::encode_bool(false), vec![0]);
    }

    #[test]
    fn test_binary_integers() {
        assert_eq!(BinaryEncoder::encode_int2(42), vec![0, 42]);
        assert_eq!(BinaryEncoder::encode_int4(0x01020304), vec![1, 2, 3, 4]);
        assert_eq!(
            BinaryEncoder::encode_int8(0x0102030405060708),
            vec![1, 2, 3, 4, 5, 6, 7, 8]
        );
    }

    #[test]
    fn test_binary_floats() {
        let f4_bytes = BinaryEncoder::encode_float4(1.5);
        assert_eq!(f4_bytes.len(), 4);
        
        let f8_bytes = BinaryEncoder::encode_float8(1.5);
        assert_eq!(f8_bytes.len(), 8);
    }

    #[test]
    fn test_zero_copy_encoder() {
        let mut buffer = BytesMut::with_capacity(1024);
        let mut encoder = ZeroCopyBinaryEncoder::new(&mut buffer);

        let pos1 = encoder.encode_bool(true);
        let pos2 = encoder.encode_int4(42);
        let pos3 = encoder.encode_text("hello");

        assert_eq!(&buffer[pos1..pos1 + 1], &[1]);
        assert_eq!(&buffer[pos2..pos2 + 4], &[0, 0, 0, 42]);
        assert_eq!(&buffer[pos3..pos3 + 5], b"hello");
    }
    
    #[test]
    fn test_date_encoding() {
        // Test DATE encoding
        // 2024-01-15 00:00:00 UTC = 1705276800 Unix timestamp
        let encoded = BinaryEncoder::encode_date(1705276800.0);
        // Days since 2000-01-01: (1705276800 - 946684800) / 86400 = 8780
        let expected: i32 = 8780;
        assert_eq!(encoded, expected.to_be_bytes().to_vec());
    }
    
    #[test]
    fn test_time_encoding() {
        // Test TIME encoding
        // 14:30:45.123456 = 52245123456 microseconds since midnight
        let encoded = BinaryEncoder::encode_time(52245123456.0);
        // Microseconds: 52245123456
        let expected: i64 = 52245123456;
        assert_eq!(encoded, expected.to_be_bytes().to_vec());
    }
    
    #[test]
    fn test_timestamp_encoding() {
        // Test TIMESTAMP encoding
        // 2024-01-15 14:30:45.123456 UTC = 1705329045123456 microseconds since Unix epoch
        let encoded = BinaryEncoder::encode_timestamp(1705329045123456.0);
        // Microseconds since 2000-01-01: 1705329045123456 - 946684800000000
        let expected: i64 = 758644245123456;
        assert_eq!(encoded, expected.to_be_bytes().to_vec());
    }
    
    #[test]
    fn test_interval_encoding() {
        // Test INTERVAL encoding
        // 1 day 2:30:00 = 95400 seconds
        let encoded = BinaryEncoder::encode_interval(95400.0);
        assert_eq!(encoded.len(), 16); // 8 bytes microseconds + 4 bytes days + 4 bytes months
        
        // Check microseconds part
        let micros = i64::from_be_bytes(encoded[0..8].try_into().unwrap());
        assert_eq!(micros, 95400000000); // 95400 * 1_000_000
        
        // Check days and months (should be 0)
        let days = i32::from_be_bytes(encoded[8..12].try_into().unwrap());
        let months = i32::from_be_bytes(encoded[12..16].try_into().unwrap());
        assert_eq!(days, 0);
        assert_eq!(months, 0);
    }
    
    #[test]
    fn test_uuid_encoding() {
        // Test UUID encoding
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let encoded = BinaryEncoder::encode_uuid(uuid_str).unwrap();
        assert_eq!(encoded.len(), 16);
        
        // Verify first few bytes
        assert_eq!(encoded[0], 0x55);
        assert_eq!(encoded[1], 0x0e);
        assert_eq!(encoded[2], 0x84);
        assert_eq!(encoded[3], 0x00);
    }
    
    #[test]
    fn test_json_jsonb_encoding() {
        let json_str = r#"{"key": "value"}"#;
        
        // JSON encoding - same as text
        let json_encoded = BinaryEncoder::encode_json(json_str);
        assert_eq!(json_encoded, json_str.as_bytes());
        
        // JSONB encoding - with version header
        let jsonb_encoded = BinaryEncoder::encode_jsonb(json_str);
        assert_eq!(jsonb_encoded[0], 1); // version
        assert_eq!(&jsonb_encoded[1..], json_str.as_bytes());
    }
    
    #[test]
    fn test_money_encoding() {
        // Test various money formats
        let encoded1 = BinaryEncoder::encode_money("123.45").unwrap();
        let money1 = i64::from_be_bytes(encoded1.try_into().unwrap());
        assert_eq!(money1, 12345); // $123.45 = 12345 cents
        
        let encoded2 = BinaryEncoder::encode_money("$1,234.56").unwrap();
        let money2 = i64::from_be_bytes(encoded2.try_into().unwrap());
        assert_eq!(money2, 123456); // $1,234.56 = 123456 cents
        
        let encoded3 = BinaryEncoder::encode_money("-99.99").unwrap();
        let money3 = i64::from_be_bytes(encoded3.try_into().unwrap());
        assert_eq!(money3, -9999); // -$99.99 = -9999 cents
    }
    
    #[test]
    fn test_numeric_encoding() {
        // Test is already covered by decimal_handler tests
        // Just verify the function is accessible
        let decimal = Decimal::from_str("123.45").unwrap();
        let encoded = BinaryEncoder::encode_numeric(&decimal);
        assert!(!encoded.is_empty());
    }
    
    #[test]
    fn test_array_encoding() {
        // Test empty array
        let empty = BinaryEncoder::encode_array("[]", PgType::Int4.to_oid()).unwrap();
        assert_eq!(empty.len(), 12); // 3 * 4 bytes for header
        assert_eq!(&empty[0..4], &0i32.to_be_bytes()); // ndim = 0
        
        // Test simple int array
        let int_array = BinaryEncoder::encode_array("[1, 2, 3]", PgType::Int4.to_oid()).unwrap();
        // Verify header
        assert_eq!(i32::from_be_bytes(int_array[0..4].try_into().unwrap()), 1); // ndim = 1
        assert_eq!(i32::from_be_bytes(int_array[4..8].try_into().unwrap()), 0); // no nulls
        assert_eq!(i32::from_be_bytes(int_array[8..12].try_into().unwrap()), PgType::Int4.to_oid()); // elemtype
        assert_eq!(i32::from_be_bytes(int_array[12..16].try_into().unwrap()), 3); // dim size
        assert_eq!(i32::from_be_bytes(int_array[16..20].try_into().unwrap()), 1); // lower bound
        
        // Test array with nulls
        let null_array = BinaryEncoder::encode_array("[1, null, 3]", PgType::Int4.to_oid()).unwrap();
        assert_eq!(i32::from_be_bytes(null_array[0..4].try_into().unwrap()), 1); // ndim = 1
        assert!(i32::from_be_bytes(null_array[4..8].try_into().unwrap()) > 0); // has nulls
        
        // Test text array
        let text_array = BinaryEncoder::encode_array(r#"["hello", "world"]"#, PgType::Text.to_oid()).unwrap();
        assert_eq!(i32::from_be_bytes(text_array[8..12].try_into().unwrap()), PgType::Text.to_oid());
        
        // Test bool array
        let bool_array = BinaryEncoder::encode_array("[true, false, true]", PgType::Bool.to_oid()).unwrap();
        assert_eq!(i32::from_be_bytes(bool_array[8..12].try_into().unwrap()), PgType::Bool.to_oid());
    }
}