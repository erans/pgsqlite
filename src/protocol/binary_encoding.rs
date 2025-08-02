use bytes::{BytesMut, BufMut};
use crate::protocol::binary::BinaryEncoder;
use crate::types::PgType;
use crate::PgSqliteError;
use rust_decimal::Decimal;
use std::str::FromStr;

/// Optimized binary result encoder that minimizes allocations
pub struct BinaryResultEncoder {
    buffer: BytesMut,
    row_offsets: Vec<(usize, usize)>, // (start, length) for each row
}

impl BinaryResultEncoder {
    /// Create a new encoder with pre-allocated buffer
    pub fn new(estimated_rows: usize, estimated_cols: usize) -> Self {
        // Estimate buffer size: assume average 8 bytes per value + overhead
        let estimated_size = estimated_rows * estimated_cols * 10;
        Self {
            buffer: BytesMut::with_capacity(estimated_size),
            row_offsets: Vec::with_capacity(estimated_rows),
        }
    }

    /// Encode a complete row into the buffer
    pub fn encode_row(
        &mut self,
        row: &[Option<Vec<u8>>],
        result_formats: &[i16],
        field_types: &[i32],
    ) -> Result<Vec<Option<Vec<u8>>>, PgSqliteError> {
        let row_start = self.buffer.len();
        let mut encoded_row = Vec::with_capacity(row.len());

        for (col_idx, value) in row.iter().enumerate() {
            // If result_formats has only one element, it applies to all columns
            let format = if result_formats.len() == 1 {
                result_formats[0]
            } else {
                result_formats.get(col_idx).copied().unwrap_or(0)
            };
            let type_oid = field_types.get(col_idx).copied().unwrap_or(PgType::Text.to_oid());

            if format == 0 {
                // Text format - pass through
                encoded_row.push(value.clone());
            } else {
                // Binary format - encode directly into buffer
                match value {
                    None => encoded_row.push(None),
                    Some(bytes) => {
                        let value_start = self.buffer.len();
                        
                        // Try to parse and encode based on type
                        let encoded = self.encode_value_into_buffer(bytes, type_oid);
                        
                        if encoded {
                            let value_end = self.buffer.len();
                            // Extract the encoded bytes as a new Vec (for now)
                            // In future, we could pass references
                            let encoded_bytes = self.buffer[value_start..value_end].to_vec();
                            encoded_row.push(Some(encoded_bytes));
                        } else {
                            // Fallback to text format
                            encoded_row.push(Some(bytes.clone()));
                        }
                    }
                }
            }
        }

        let row_end = self.buffer.len();
        self.row_offsets.push((row_start, row_end - row_start));
        
        Ok(encoded_row)
    }

    /// Encode a value directly into the buffer
    fn encode_value_into_buffer(
        &mut self,
        bytes: &[u8],
        type_oid: i32,
    ) -> bool {
        // Try to parse text value and encode to binary
        if let Ok(text) = std::str::from_utf8(bytes) {
            match type_oid {
                t if t == PgType::Bool.to_oid() => {
                    match text {
                        "t" | "true" | "1" => {
                            self.buffer.put_u8(1);
                            true
                        }
                        "f" | "false" | "0" => {
                            self.buffer.put_u8(0);
                            true
                        }
                        _ => false
                    }
                }
                t if t == PgType::Int2.to_oid() => {
                    if let Ok(val) = text.parse::<i16>() {
                        self.buffer.put_i16(val);
                        true
                    } else {
                        false
                    }
                }
                t if t == PgType::Int4.to_oid() => {
                    if let Ok(val) = text.parse::<i32>() {
                        self.buffer.put_i32(val);
                        true
                    } else {
                        false
                    }
                }
                t if t == PgType::Int8.to_oid() => {
                    if let Ok(val) = text.parse::<i64>() {
                        self.buffer.put_i64(val);
                        true
                    } else {
                        false
                    }
                }
                t if t == PgType::Float4.to_oid() => {
                    if let Ok(val) = text.parse::<f32>() {
                        self.buffer.put_f32(val);
                        true
                    } else {
                        false
                    }
                }
                t if t == PgType::Float8.to_oid() => {
                    if let Ok(val) = text.parse::<f64>() {
                        self.buffer.put_f64(val);
                        true
                    } else {
                        false
                    }
                }
                t if t == PgType::Text.to_oid() || t == PgType::Varchar.to_oid() => {
                    self.buffer.put_slice(text.as_bytes());
                    true
                }
                t if t == PgType::Numeric.to_oid() => {
                    // Parse text as decimal and encode
                    if let Ok(decimal) = Decimal::from_str(text) {
                        let encoded = BinaryEncoder::encode_numeric(&decimal);
                        self.buffer.put_slice(&encoded);
                        true
                    } else {
                        false
                    }
                }
                _ => false
            }
        } else if type_oid == PgType::Bytea.to_oid() {
            // Binary data - copy directly
            self.buffer.put_slice(bytes);
            true
        } else {
            false
        }
    }

    /// Get buffer statistics for monitoring
    pub fn stats(&self) -> (usize, usize, usize) {
        (
            self.buffer.len(),        // Current buffer size
            self.buffer.capacity(),   // Buffer capacity
            self.row_offsets.len(),   // Number of rows encoded
        )
    }

    /// Clear the buffer for reuse
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.row_offsets.clear();
    }
}

/// Cache for binary-encoded results
pub struct BinaryResultCache {
    cache: std::collections::HashMap<String, CachedBinaryResult>,
    max_size: usize,
    current_size: usize,
}

struct CachedBinaryResult {
    encoded_rows: Vec<Vec<Option<Vec<u8>>>>,
    #[allow(dead_code)]
    field_types: Vec<i32>,
    last_access: std::time::Instant,
}

impl BinaryResultCache {
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: std::collections::HashMap::new(),
            max_size,
            current_size: 0,
        }
    }

    pub fn get(&mut self, query: &str) -> Option<&Vec<Vec<Option<Vec<u8>>>>> {
        self.cache.get_mut(query).map(|entry| {
            entry.last_access = std::time::Instant::now();
            &entry.encoded_rows
        })
    }

    pub fn insert(
        &mut self,
        query: String,
        encoded_rows: Vec<Vec<Option<Vec<u8>>>>,
        field_types: Vec<i32>,
    ) {
        let size = Self::estimate_size(&encoded_rows);
        
        // Evict old entries if needed
        while self.current_size + size > self.max_size && !self.cache.is_empty() {
            // Find oldest entry
            let oldest = self.cache.iter()
                .min_by_key(|(_, v)| v.last_access)
                .map(|(k, _)| k.clone());
            
            if let Some(key) = oldest {
                if let Some(entry) = self.cache.remove(&key) {
                    self.current_size -= Self::estimate_size(&entry.encoded_rows);
                }
            }
        }

        // Insert new entry
        self.cache.insert(query, CachedBinaryResult {
            encoded_rows,
            field_types,
            last_access: std::time::Instant::now(),
        });
        self.current_size += size;
    }

    fn estimate_size(rows: &[Vec<Option<Vec<u8>>>]) -> usize {
        rows.iter()
            .flat_map(|row| row.iter())
            .map(|cell| cell.as_ref().map_or(0, |v| v.len()))
            .sum()
    }
}