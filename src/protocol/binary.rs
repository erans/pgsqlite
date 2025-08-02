use bytes::{BufMut, BytesMut};
use rust_decimal::Decimal;
use std::convert::TryInto;
use std::str::FromStr;
use crate::types::PgType;

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

    /// Encode a JSON value (OID 114)
    /// Binary format is the same as text format for JSON
    #[inline]
    pub fn encode_json(value: &str) -> Vec<u8> {
        value.as_bytes().to_vec()
    }

    /// Encode a JSONB value (OID 3802)
    /// Binary format includes version byte (0x01) + JSON text
    #[inline]
    pub fn encode_jsonb(value: &str) -> Vec<u8> {
        let mut result = vec![1u8]; // Version 1
        result.extend_from_slice(value.as_bytes());
        result
    }

    /// Encode a numeric/decimal value (OID 1700)
    /// PostgreSQL uses a custom binary format with 4-digit groups
    pub fn encode_numeric(value: &Decimal) -> Vec<u8> {
        // Handle special cases
        if value.is_zero() {
            // Zero value: ndigits=0, weight=0, sign=0, dscale=0
            return vec![0, 0, 0, 0, 0, 0, 0, 0];
        }

        let is_negative = value.is_sign_negative();
        let abs_value = value.abs();
        
        // Convert to string to parse digits
        let value_str = abs_value.to_string();
        let (integer_part, fractional_part) = if let Some(dot_pos) = value_str.find('.') {
            (&value_str[..dot_pos], &value_str[dot_pos + 1..])
        } else {
            (value_str.as_str(), "")
        };
        
        // Calculate display scale (decimal places)
        let dscale = fractional_part.len() as i16;
        
        // Combine all digits
        let all_digits = if integer_part == "0" {
            // For fractional-only numbers, skip the leading zero
            fractional_part.to_string()
        } else {
            format!("{}{}", integer_part, fractional_part)
        };
        
        // Group digits into 4-digit chunks (from right to left for proper weight calculation)
        let mut digit_groups = Vec::new();
        let mut remaining = all_digits.as_str();
        
        // Calculate weight (position of leftmost group relative to decimal point)
        // Weight = (number_of_integer_digits - 1) / 4
        let weight = if integer_part == "0" {
            // For fractional values like 0.0001, weight is negative
            -((fractional_part.len() as i16 + 3) / 4)
        } else {
            (integer_part.len() as i16 - 1) / 4
        };
        
        // Process digits from left to right, grouping into 4-digit chunks
        while !remaining.is_empty() {
            let chunk_size = if remaining.len() >= 4 { 4 } else { remaining.len() };
            let chunk = &remaining[..chunk_size];
            remaining = &remaining[chunk_size..];
            
            // Parse the chunk and pad with zeros if needed
            let mut digit_value = chunk.parse::<u16>().unwrap_or(0);
            if chunk.len() < 4 {
                // Right-pad with zeros for the last chunk
                for _ in chunk.len()..4 {
                    digit_value *= 10;
                }
            }
            
            digit_groups.push(digit_value);
        }
        
        // Remove trailing zeros
        while let Some(&0) = digit_groups.last() {
            digit_groups.pop();
        }
        
        let ndigits = digit_groups.len() as i16;
        let sign = if is_negative { 0x4000u16 } else { 0x0000u16 };
        
        // Build the binary format
        let mut result = Vec::with_capacity(8 + digit_groups.len() * 2);
        
        // Header (8 bytes)
        result.extend_from_slice(&ndigits.to_be_bytes());
        result.extend_from_slice(&weight.to_be_bytes());
        result.extend_from_slice(&(sign as i16).to_be_bytes());
        result.extend_from_slice(&dscale.to_be_bytes());
        
        // Digit groups (2 bytes each)
        for digit in digit_groups {
            result.extend_from_slice(&digit.to_be_bytes());
        }
        
        result
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
            t if t == PgType::Json.to_oid() => {
                // JSON - binary format is the same as text
                match value {
                    rusqlite::types::Value::Text(s) => Some(Self::encode_json(s)),
                    _ => None,
                }
            }
            t if t == PgType::Jsonb.to_oid() => {
                // JSONB - binary format includes version byte
                match value {
                    rusqlite::types::Value::Text(s) => Some(Self::encode_jsonb(s)),
                    _ => None,
                }
            }
            t if t == PgType::Numeric.to_oid() => {
                // NUMERIC - custom PostgreSQL binary format
                match value {
                    rusqlite::types::Value::Text(s) => {
                        // Parse the text as decimal
                        if let Ok(decimal) = Decimal::from_str(s) {
                            Some(Self::encode_numeric(&decimal))
                        } else {
                            None
                        }
                    }
                    rusqlite::types::Value::Real(f) => {
                        // Convert float to decimal
                        if let Some(decimal) = Decimal::from_f64_retain(*f) {
                            Some(Self::encode_numeric(&decimal))
                        } else {
                            None
                        }
                    }
                    rusqlite::types::Value::Integer(i) => {
                        // Convert integer to decimal
                        let decimal = Decimal::from(*i);
                        Some(Self::encode_numeric(&decimal))
                    }
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
    fn test_numeric_encoding() {
        // Test zero
        let zero = Decimal::from(0);
        let encoded = BinaryEncoder::encode_numeric(&zero);
        assert_eq!(encoded, vec![0, 0, 0, 0, 0, 0, 0, 0]);
        
        // Test 123.45
        let num = Decimal::from_str("123.45").unwrap();
        let encoded = BinaryEncoder::encode_numeric(&num);
        
        // Parse header
        let ndigits = i16::from_be_bytes([encoded[0], encoded[1]]);
        let weight = i16::from_be_bytes([encoded[2], encoded[3]]);
        let sign = i16::from_be_bytes([encoded[4], encoded[5]]);
        let dscale = i16::from_be_bytes([encoded[6], encoded[7]]);
        
        
        assert_eq!(ndigits, 2); // Two digit groups
        assert_eq!(weight, 0);   // First group at 10^0
        assert_eq!(sign, 0);     // Positive
        assert_eq!(dscale, 2);   // Two decimal places
        
        // Parse digit groups - correct PostgreSQL format
        let digit1 = u16::from_be_bytes([encoded[8], encoded[9]]);
        let digit2 = u16::from_be_bytes([encoded[10], encoded[11]]);
        
        // For 123.45, PostgreSQL should encode as: 1234, 5000
        // because digits are grouped from left to right in 4-digit chunks
        assert_eq!(digit1, 1234);   // First group: 1234 
        assert_eq!(digit2, 5000);  // Second group: 5 -> 5000
        
        // Test negative number -999.123
        let neg_num = Decimal::from_str("-999.123").unwrap();
        let encoded = BinaryEncoder::encode_numeric(&neg_num);
        
        let sign = i16::from_be_bytes([encoded[4], encoded[5]]);
        assert_eq!(sign, 0x4000); // Negative flag
        
        // Test small fractional number 0.0001
        let small = Decimal::from_str("0.0001").unwrap();
        let encoded = BinaryEncoder::encode_numeric(&small);
        
        let ndigits = i16::from_be_bytes([encoded[0], encoded[1]]);
        let weight = i16::from_be_bytes([encoded[2], encoded[3]]);
        let dscale = i16::from_be_bytes([encoded[6], encoded[7]]);
        
        
        // For 0.0001, our algorithm produces "0001" -> one group "1000"
        assert_eq!(ndigits, 1);  // One digit group  
        assert_eq!(weight, -1);  // Weight for 10^-4 (first group at 10^-4)
        assert_eq!(dscale, 4);   // Four decimal places
    }
    
    #[test]
    fn test_json_encoding() {
        let json_str = r#"{"key": "value", "number": 42}"#;
        let encoded = BinaryEncoder::encode_json(json_str);
        assert_eq!(encoded, json_str.as_bytes().to_vec());
    }
    
    #[test]
    fn test_jsonb_encoding() {
        let json_str = r#"{"key": "value", "number": 42}"#;
        let encoded = BinaryEncoder::encode_jsonb(json_str);
        
        // Should start with version byte 0x01
        assert_eq!(encoded[0], 1);
        
        // Rest should be the JSON text
        assert_eq!(&encoded[1..], json_str.as_bytes());
        
        // Total length should be JSON length + 1 for version byte
        assert_eq!(encoded.len(), json_str.len() + 1);
    }
}