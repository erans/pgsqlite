use bytes::{BytesMut, BufMut};
use crate::protocol::binary::BinaryEncoder;
use crate::types::PgType;
use crate::types::decimal_handler::DecimalHandler;
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
                        let encoded = DecimalHandler::encode_numeric(&decimal);
                        self.buffer.put_slice(&encoded);
                        true
                    } else {
                        false
                    }
                }
                t if t == PgType::Timestamp.to_oid() || t == PgType::Timestamptz.to_oid() => {
                    // Handle timestamp stored as text (e.g., "2025-08-03 04:40:12")
                    // First try to parse as integer (microseconds)
                    if let Ok(micros) = text.parse::<i64>() {
                        let encoded = BinaryEncoder::encode_timestamp(micros as f64);
                        self.buffer.put_slice(&encoded);
                        true
                    } else {
                        // Try to parse as ISO timestamp string
                        use chrono::{DateTime, NaiveDateTime};
                        
                        // Try various timestamp formats
                        let parsed = if let Ok(dt) = DateTime::parse_from_rfc3339(text) {
                            Some(dt.timestamp_micros())
                        } else if let Ok(dt) = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f") {
                            Some(dt.and_utc().timestamp_micros())
                        } else if let Ok(dt) = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S") {
                            Some(dt.and_utc().timestamp_micros())
                        } else {
                            None
                        };
                        
                        if let Some(micros) = parsed {
                            let encoded = BinaryEncoder::encode_timestamp(micros as f64);
                            self.buffer.put_slice(&encoded);
                            true
                        } else {
                            false
                        }
                    }
                }
                t if t == PgType::Date.to_oid() => {
                    // Handle date stored as text (e.g., "2025-08-03")
                    if let Ok(days) = text.parse::<i32>() {
                        // Already in days format
                        let encoded = BinaryEncoder::encode_date(days as f64);
                        self.buffer.put_slice(&encoded);
                        true
                    } else {
                        // Try to parse as date string
                        use chrono::NaiveDate;
                        if let Ok(date) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
                            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                            let days_since_epoch = (date - epoch).num_days();
                            let encoded = BinaryEncoder::encode_date(days_since_epoch as f64);
                            self.buffer.put_slice(&encoded);
                            true
                        } else {
                            false
                        }
                    }
                }
                t if t == PgType::Time.to_oid() || t == PgType::Timetz.to_oid() => {
                    // Handle time stored as text (e.g., "14:30:45.123456")
                    if let Ok(micros) = text.parse::<i64>() {
                        // Already in microseconds format
                        let encoded = BinaryEncoder::encode_time(micros as f64);
                        self.buffer.put_slice(&encoded);
                        true
                    } else {
                        // Try to parse as time string
                        use chrono::{NaiveTime, Timelike};
                        if let Ok(time) = NaiveTime::parse_from_str(text, "%H:%M:%S%.f") {
                            let micros = time.num_seconds_from_midnight() as i64 * 1_000_000 
                                       + (time.nanosecond() as i64 / 1000);
                            let encoded = BinaryEncoder::encode_time(micros as f64);
                            self.buffer.put_slice(&encoded);
                            true
                        } else if let Ok(time) = NaiveTime::parse_from_str(text, "%H:%M:%S") {
                            let micros = time.num_seconds_from_midnight() as i64 * 1_000_000;
                            let encoded = BinaryEncoder::encode_time(micros as f64);
                            self.buffer.put_slice(&encoded);
                            true
                        } else {
                            false
                        }
                    }
                }
                t if t == PgType::Numrange.to_oid() => {
                    // Encode NUMRANGE in PostgreSQL binary format
                    self.encode_numrange(text)
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

    /// Encode NUMRANGE value in PostgreSQL binary format
    fn encode_numrange(&mut self, text: &str) -> bool {
        // Parse NUMRANGE text format: "empty", "[1.5,10.5)", etc.
        let trimmed = text.trim();
        
        if trimmed == "empty" {
            // Empty range - just the flags byte with EMPTY flag set
            self.buffer.put_u8(0x01); // EMPTY flag
            return true;
        }
        
        // Parse range format: [lower,upper) or (lower,upper] etc.
        if trimmed.len() < 3 {
            return false;
        }
        
        let lower_inclusive = trimmed.starts_with('[');
        let upper_inclusive = trimmed.ends_with(']');
        
        // Extract the bounds part (remove brackets)
        let bounds = &trimmed[1..trimmed.len()-1];
        
        // Split on comma to get lower and upper bounds
        let parts: Vec<&str> = bounds.split(',').collect();
        if parts.len() != 2 {
            return false;
        }
        
        let lower_str = parts[0].trim();
        let upper_str = parts[1].trim();
        
        // Check for infinite bounds (PostgreSQL uses empty string or special values)
        let lower_infinite = lower_str.is_empty() || lower_str == "-infinity";
        let upper_infinite = upper_str.is_empty() || upper_str == "infinity";
        
        // Build flags byte
        let mut flags = 0u8;
        if lower_inclusive {
            flags |= 0x02; // LB_INC
        }
        if upper_inclusive {
            flags |= 0x04; // UB_INC
        }
        if lower_infinite {
            flags |= 0x08; // LB_INF
        }
        if upper_infinite {
            flags |= 0x10; // UB_INF
        }
        
        // Write flags
        self.buffer.put_u8(flags);
        
        // Write bounds (only if not infinite)
        if !lower_infinite {
            if let Ok(lower_val) = Decimal::from_str(lower_str) {
                // Encode as NUMERIC
                let encoded = DecimalHandler::encode_numeric(&lower_val);
                self.buffer.put_i32(encoded.len() as i32);
                self.buffer.put_slice(&encoded);
            } else {
                return false;
            }
        }
        
        if !upper_infinite {
            if let Ok(upper_val) = Decimal::from_str(upper_str) {
                // Encode as NUMERIC
                let encoded = DecimalHandler::encode_numeric(&upper_val);
                self.buffer.put_i32(encoded.len() as i32);
                self.buffer.put_slice(&encoded);
            } else {
                return false;
            }
        }
        
        true
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