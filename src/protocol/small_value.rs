use crate::types::PgType;

/// Small value storage that avoids heap allocation for common small values
#[derive(Debug, Clone)]
pub enum SmallValue {
    /// Static boolean values
    BoolTrue,
    BoolFalse,
    /// Small integers stored inline (up to 20 characters in text form)
    SmallInt {
        value: i64,
        /// Pre-computed text length to avoid recomputation
        text_len: u8,
    },
    /// Common small integers with static representations
    Zero,
    One,
    MinusOne,
    /// Small floats that fit in a stack buffer
    SmallFloat {
        value: f64,
        /// Pre-computed text length
        text_len: u8,
    },
    /// Empty string
    Empty,
}

impl SmallValue {
    /// Try to create a small value from an integer
    pub fn from_integer(value: i64) -> Option<Self> {
        match value {
            0 => Some(SmallValue::Zero),
            1 => Some(SmallValue::One),
            -1 => Some(SmallValue::MinusOne),
            _ => {
                // Check if the text representation is small enough
                let text_len = Self::integer_text_length(value);
                if text_len <= 20 {
                    Some(SmallValue::SmallInt { value, text_len })
                } else {
                    None
                }
            }
        }
    }

    /// Try to create a small value from a float
    pub fn from_float(value: f64) -> Option<Self> {
        // For now, only handle reasonably small float representations
        // This is a conservative estimate - actual formatting might be shorter
        let text_len = if value.abs() < 1e6 && value.abs() > 1e-6 {
            20 // Conservative estimate
        } else {
            30 // Scientific notation
        };
        
        if text_len <= 24 {
            Some(SmallValue::SmallFloat { value, text_len: text_len as u8 })
        } else {
            None
        }
    }

    /// Try to create a small value from a boolean
    pub fn from_bool(value: bool) -> Self {
        if value {
            SmallValue::BoolTrue
        } else {
            SmallValue::BoolFalse
        }
    }

    /// Get the text representation of this small value
    pub fn as_text(&self) -> &[u8] {
        match self {
            SmallValue::BoolTrue => b"t",
            SmallValue::BoolFalse => b"f",
            SmallValue::Zero => b"0",
            SmallValue::One => b"1",
            SmallValue::MinusOne => b"-1",
            SmallValue::Empty => b"",
            SmallValue::SmallInt { .. } | SmallValue::SmallFloat { .. } => {
                // These need dynamic formatting, will be handled differently
                unreachable!("SmallInt and SmallFloat need special handling")
            }
        }
    }

    /// Write the text representation to a buffer, returning the number of bytes written
    pub fn write_text_to_buffer(&self, buffer: &mut [u8]) -> usize {
        match self {
            SmallValue::BoolTrue => {
                buffer[0] = b't';
                1
            }
            SmallValue::BoolFalse => {
                buffer[0] = b'f';
                1
            }
            SmallValue::Zero => {
                buffer[0] = b'0';
                1
            }
            SmallValue::One => {
                buffer[0] = b'1';
                1
            }
            SmallValue::MinusOne => {
                buffer[0] = b'-';
                buffer[1] = b'1';
                2
            }
            SmallValue::Empty => 0,
            SmallValue::SmallInt { value, .. } => {
                // Use itoa for fast integer formatting
                let mut itoa_buf = itoa::Buffer::new();
                let formatted = itoa_buf.format(*value);
                let bytes = formatted.as_bytes();
                buffer[..bytes.len()].copy_from_slice(bytes);
                bytes.len()
            }
            SmallValue::SmallFloat { value, .. } => {
                // Use standard library formatting for now
                // TODO: Consider using ryu crate for faster float formatting
                use std::io::Write;
                let mut cursor = std::io::Cursor::new(&mut buffer[..]);
                write!(&mut cursor, "{value}").unwrap();
                cursor.position() as usize
            }
        }
    }

    /// Get the maximum possible text length for this value
    pub fn max_text_length(&self) -> usize {
        match self {
            SmallValue::BoolTrue | SmallValue::BoolFalse => 1,
            SmallValue::Zero | SmallValue::One => 1,
            SmallValue::MinusOne => 2,
            SmallValue::Empty => 0,
            SmallValue::SmallInt { text_len, .. } => *text_len as usize,
            SmallValue::SmallFloat { text_len, .. } => *text_len as usize,
        }
    }

    /// Convert to binary PostgreSQL format if applicable
    pub fn to_binary(&self, pg_type_oid: i32) -> Option<Vec<u8>> {
        use crate::protocol::BinaryEncoder;

        match self {
            SmallValue::BoolTrue => {
                if pg_type_oid == PgType::Bool.to_oid() {
                    Some(BinaryEncoder::encode_bool(true))
                } else {
                    None
                }
            }
            SmallValue::BoolFalse => {
                if pg_type_oid == PgType::Bool.to_oid() {
                    Some(BinaryEncoder::encode_bool(false))
                } else {
                    None
                }
            }
            SmallValue::Zero | SmallValue::One | SmallValue::MinusOne | SmallValue::SmallInt { .. } => {
                let int_value = match self {
                    SmallValue::Zero => 0,
                    SmallValue::One => 1,
                    SmallValue::MinusOne => -1,
                    SmallValue::SmallInt { value, .. } => *value,
                    _ => unreachable!(),
                };

                match pg_type_oid {
                    t if t == PgType::Int2.to_oid() => Some(BinaryEncoder::encode_int2(int_value as i16)),
                    t if t == PgType::Int4.to_oid() => Some(BinaryEncoder::encode_int4(int_value as i32)),
                    t if t == PgType::Int8.to_oid() => Some(BinaryEncoder::encode_int8(int_value)),
                    t if t == PgType::Bool.to_oid() => Some(BinaryEncoder::encode_bool(int_value != 0)),
                    _ => None,
                }
            }
            SmallValue::SmallFloat { value, .. } => {
                match pg_type_oid {
                    t if t == PgType::Float4.to_oid() => Some(BinaryEncoder::encode_float4(*value as f32)),
                    t if t == PgType::Float8.to_oid() => Some(BinaryEncoder::encode_float8(*value)),
                    _ => None,
                }
            }
            SmallValue::Empty => None,
        }
    }

    /// Calculate the text length of an integer
    fn integer_text_length(value: i64) -> u8 {
        if value == 0 {
            return 1;
        }
        let mut len = 0;
        let mut n = value.abs();
        while n > 0 {
            len += 1;
            n /= 10;
        }
        if value < 0 {
            len += 1; // For minus sign
        }
        len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small_value_creation() {
        // Test common integers
        assert!(matches!(SmallValue::from_integer(0), Some(SmallValue::Zero)));
        assert!(matches!(SmallValue::from_integer(1), Some(SmallValue::One)));
        assert!(matches!(SmallValue::from_integer(-1), Some(SmallValue::MinusOne)));
        
        // Test small integer
        let small = SmallValue::from_integer(42).unwrap();
        assert!(matches!(small, SmallValue::SmallInt { value: 42, text_len: 2 }));
        
        // Test that i64::MAX is still considered small (19 digits)
        assert!(SmallValue::from_integer(i64::MAX).is_some());
        
        // Test a large but valid i64 number
        let large_num = 999_999_999_999_999_999i64; // 18 digits
        assert!(SmallValue::from_integer(large_num).is_some());
    }

    #[test]
    fn test_text_formatting() {
        let mut buffer = [0u8; 32];
        
        // Test static values
        assert_eq!(SmallValue::BoolTrue.write_text_to_buffer(&mut buffer), 1);
        assert_eq!(&buffer[..1], b"t");
        
        assert_eq!(SmallValue::Zero.write_text_to_buffer(&mut buffer), 1);
        assert_eq!(&buffer[..1], b"0");
        
        assert_eq!(SmallValue::MinusOne.write_text_to_buffer(&mut buffer), 2);
        assert_eq!(&buffer[..2], b"-1");
        
        // Test dynamic integer
        let small_int = SmallValue::from_integer(12345).unwrap();
        assert_eq!(small_int.write_text_to_buffer(&mut buffer), 5);
        assert_eq!(&buffer[..5], b"12345");
    }

    #[test]
    fn test_binary_conversion() {
        // Test boolean binary conversion
        let bool_true = SmallValue::BoolTrue;
        let binary = bool_true.to_binary(PgType::Bool.to_oid()).unwrap();
        assert_eq!(binary, vec![1]);
        
        // Test integer binary conversion
        let one = SmallValue::One;
        let binary = one.to_binary(PgType::Int4.to_oid()).unwrap();
        assert_eq!(binary, vec![0, 0, 0, 1]);
    }
}