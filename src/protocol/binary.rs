use bytes::{BufMut, BytesMut};
use rust_decimal::Decimal;
use std::convert::TryInto;

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
            16 => {
                // BOOL
                match value {
                    rusqlite::types::Value::Integer(i) => Some(Self::encode_bool(*i != 0)),
                    _ => None,
                }
            }
            21 => {
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
            23 => {
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
            20 => {
                // INT8
                match value {
                    rusqlite::types::Value::Integer(i) => Some(Self::encode_int8(*i)),
                    _ => None,
                }
            }
            700 => {
                // FLOAT4
                match value {
                    rusqlite::types::Value::Real(f) => Some(Self::encode_float4(*f as f32)),
                    rusqlite::types::Value::Integer(i) => Some(Self::encode_float4(*i as f32)),
                    _ => None,
                }
            }
            701 => {
                // FLOAT8
                match value {
                    rusqlite::types::Value::Real(f) => Some(Self::encode_float8(*f)),
                    rusqlite::types::Value::Integer(i) => Some(Self::encode_float8(*i as f64)),
                    _ => None,
                }
            }
            17 => {
                // BYTEA
                match value {
                    rusqlite::types::Value::Blob(b) => Some(Self::encode_bytea(b)),
                    _ => None,
                }
            }
            25 | 1043 => {
                // TEXT, VARCHAR - binary format is the same as text
                match value {
                    rusqlite::types::Value::Text(s) => Some(Self::encode_text(s)),
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
}