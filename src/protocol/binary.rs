use bytes::{BufMut, BytesMut};
use rust_decimal::Decimal;
use std::convert::TryInto;
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

    /// Encode a numeric/decimal value (OID 1700)
    /// This is complex - PostgreSQL uses a custom format
    pub fn encode_numeric(value: &Decimal) -> Vec<u8> {
        // For now, fall back to text representation
        // Full binary numeric encoding is complex and requires
        // converting to PostgreSQL's internal numeric format
        value.to_string().into_bytes()
    }
    
    /// Encode DATE (days since 2000-01-01)
    pub fn encode_date(unix_timestamp: f64) -> Vec<u8> {
        const PG_EPOCH_OFFSET: i64 = 946684800; // seconds between 1970-01-01 and 2000-01-01
        const SECS_PER_DAY: i64 = 86400;
        let unix_secs = unix_timestamp.trunc() as i64;
        let pg_days = ((unix_secs - PG_EPOCH_OFFSET) / SECS_PER_DAY) as i32;
        pg_days.to_be_bytes().to_vec()
    }
    
    /// Encode TIME (microseconds since midnight)
    pub fn encode_time(seconds_since_midnight: f64) -> Vec<u8> {
        let micros = (seconds_since_midnight * 1_000_000.0).round() as i64;
        micros.to_be_bytes().to_vec()
    }
    
    /// Encode TIMESTAMP/TIMESTAMPTZ (microseconds since 2000-01-01 00:00:00)
    pub fn encode_timestamp(unix_timestamp: f64) -> Vec<u8> {
        const PG_EPOCH_OFFSET: i64 = 946684800; // seconds between 1970-01-01 and 2000-01-01
        let unix_secs = unix_timestamp.trunc() as i64;
        let unix_micros = (unix_timestamp.fract() * 1_000_000.0).round() as i64;
        let pg_micros = (unix_secs - PG_EPOCH_OFFSET) * 1_000_000 + unix_micros;
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
                // DATE - stored as Unix timestamp
                match value {
                    rusqlite::types::Value::Real(f) => Some(Self::encode_date(*f)),
                    rusqlite::types::Value::Integer(i) => Some(Self::encode_date(*i as f64)),
                    _ => None,
                }
            }
            t if t == PgType::Time.to_oid() || t == PgType::Timetz.to_oid() => {
                // TIME/TIMETZ - stored as seconds since midnight
                match value {
                    rusqlite::types::Value::Real(f) => Some(Self::encode_time(*f)),
                    rusqlite::types::Value::Integer(i) => Some(Self::encode_time(*i as f64)),
                    _ => None,
                }
            }
            t if t == PgType::Timestamp.to_oid() || t == PgType::Timestamptz.to_oid() => {
                // TIMESTAMP/TIMESTAMPTZ - stored as Unix timestamp
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
        // 14:30:45.123456 = 52245.123456 seconds since midnight
        let encoded = BinaryEncoder::encode_time(52245.123456);
        // Microseconds: 52245123456
        let expected: i64 = 52245123456;
        assert_eq!(encoded, expected.to_be_bytes().to_vec());
    }
    
    #[test]
    fn test_timestamp_encoding() {
        // Test TIMESTAMP encoding
        // 2024-01-15 14:30:45.123456 UTC = 1705329045.123456 Unix timestamp
        let encoded = BinaryEncoder::encode_timestamp(1705329045.123456);
        // Microseconds since 2000-01-01: (1705329045 - 946684800) * 1000000 + 123456
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
}