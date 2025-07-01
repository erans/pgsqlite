use bytes::{BytesMut, BufMut};
use crate::protocol::FieldDescription;

/// Zero-copy protocol message builder for efficient message construction
pub struct ZeroCopyMessageBuilder {
    buffer: BytesMut,
}

impl ZeroCopyMessageBuilder {
    /// Create a new message builder with specified initial capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity),
        }
    }

    /// Create a new message builder with default capacity (4KB)
    pub fn new() -> Self {
        Self::with_capacity(4096)
    }

    /// Clear the buffer for reuse
    pub fn clear(&mut self) {
        self.buffer.clear();
    }

    /// Get the underlying buffer
    pub fn into_bytes(self) -> BytesMut {
        self.buffer
    }

    /// Get a reference to the buffer
    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }

    /// Get the current buffer length
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Build a RowDescription message
    pub fn build_row_description(&mut self, fields: &[FieldDescription]) -> &mut Self {
        self.buffer.put_u8(b'T');
        let len_pos = self.buffer.len();
        self.buffer.put_i32(0); // Placeholder for length
        
        self.buffer.put_i16(fields.len() as i16);
        
        for field in fields {
            // Field name
            self.buffer.put_slice(field.name.as_bytes());
            self.buffer.put_u8(0); // Null terminator
            
            // Field attributes
            self.buffer.put_i32(field.table_oid);
            self.buffer.put_i16(field.column_id);
            self.buffer.put_i32(field.type_oid);
            self.buffer.put_i16(field.type_size);
            self.buffer.put_i32(field.type_modifier);
            self.buffer.put_i16(field.format);
        }
        
        self.update_message_length(len_pos);
        self
    }

    /// Build a DataRow message with pre-encoded values
    pub fn build_data_row(&mut self, values: &[Option<&[u8]>]) -> &mut Self {
        self.buffer.put_u8(b'D');
        let len_pos = self.buffer.len();
        self.buffer.put_i32(0); // Placeholder for length
        
        self.buffer.put_i16(values.len() as i16);
        
        for value in values {
            match value {
                None => self.buffer.put_i32(-1),
                Some(data) => {
                    self.buffer.put_i32(data.len() as i32);
                    self.buffer.put_slice(data);
                }
            }
        }
        
        self.update_message_length(len_pos);
        self
    }

    /// Build a DataRow message with zero-copy encoding
    pub fn build_data_row_zero_copy<F>(&mut self, 
        num_columns: usize,
        mut encode_fn: F
    ) -> &mut Self 
    where
        F: FnMut(usize, &mut BytesMut) -> Option<(usize, usize)>
    {
        self.buffer.put_u8(b'D');
        let len_pos = self.buffer.len();
        self.buffer.put_i32(0); // Placeholder for length
        
        self.buffer.put_i16(num_columns as i16);
        
        // Track value positions for length updates
        let mut value_positions = Vec::with_capacity(num_columns);
        
        for i in 0..num_columns {
            let len_field_pos = self.buffer.len();
            self.buffer.put_i32(0); // Placeholder for value length
            
            // Call the encoding function which writes directly to buffer
            if let Some((start_pos, end_pos)) = encode_fn(i, &mut self.buffer) {
                // Update the length field with actual length
                let value_len = (end_pos - start_pos) as i32;
                let len_bytes = value_len.to_be_bytes();
                self.buffer[len_field_pos..len_field_pos + 4].copy_from_slice(&len_bytes);
            } else {
                // NULL value - update length to -1
                let null_bytes = (-1i32).to_be_bytes();
                self.buffer[len_field_pos..len_field_pos + 4].copy_from_slice(&null_bytes);
                // Remove the extra 4 bytes we reserved
                self.buffer.truncate(len_field_pos + 4);
            }
            
            value_positions.push(len_field_pos);
        }
        
        self.update_message_length(len_pos);
        self
    }

    /// Build a CommandComplete message
    pub fn build_command_complete(&mut self, tag: &str) -> &mut Self {
        self.buffer.put_u8(b'C');
        let len_pos = self.buffer.len();
        self.buffer.put_i32(0); // Placeholder for length
        
        self.buffer.put_slice(tag.as_bytes());
        self.buffer.put_u8(0); // Null terminator
        
        self.update_message_length(len_pos);
        self
    }

    /// Build a ReadyForQuery message
    pub fn build_ready_for_query(&mut self, status: crate::protocol::TransactionStatus) -> &mut Self {
        self.buffer.put_u8(b'Z');
        self.buffer.put_i32(5); // Fixed length: 4 + 1
        
        let status_byte = match status {
            crate::protocol::TransactionStatus::Idle => b'I',
            crate::protocol::TransactionStatus::InTransaction => b'T',
            crate::protocol::TransactionStatus::InFailedTransaction => b'E',
        };
        self.buffer.put_u8(status_byte);
        self
    }

    /// Update the message length field at the specified position
    fn update_message_length(&mut self, len_pos: usize) {
        let total_len = self.buffer.len() - len_pos + 1; // +1 for type byte, -4 for length field
        let len_bytes = (total_len as i32).to_be_bytes();
        self.buffer[len_pos..len_pos + 4].copy_from_slice(&len_bytes);
    }
}

/// Extension trait for zero-copy encoding of values
pub trait ZeroCopyValue {
    /// Encode the value directly into the buffer, returning start and end positions
    fn encode_zero_copy(&self, buffer: &mut BytesMut) -> (usize, usize);
}

impl ZeroCopyValue for i32 {
    fn encode_zero_copy(&self, buffer: &mut BytesMut) -> (usize, usize) {
        let start = buffer.len();
        buffer.put_i32(*self);
        (start, buffer.len())
    }
}

impl ZeroCopyValue for i64 {
    fn encode_zero_copy(&self, buffer: &mut BytesMut) -> (usize, usize) {
        let start = buffer.len();
        buffer.put_i64(*self);
        (start, buffer.len())
    }
}

impl ZeroCopyValue for f32 {
    fn encode_zero_copy(&self, buffer: &mut BytesMut) -> (usize, usize) {
        let start = buffer.len();
        buffer.put_f32(*self);
        (start, buffer.len())
    }
}

impl ZeroCopyValue for f64 {
    fn encode_zero_copy(&self, buffer: &mut BytesMut) -> (usize, usize) {
        let start = buffer.len();
        buffer.put_f64(*self);
        (start, buffer.len())
    }
}

impl ZeroCopyValue for &str {
    fn encode_zero_copy(&self, buffer: &mut BytesMut) -> (usize, usize) {
        let start = buffer.len();
        buffer.put_slice(self.as_bytes());
        (start, buffer.len())
    }
}

impl ZeroCopyValue for &[u8] {
    fn encode_zero_copy(&self, buffer: &mut BytesMut) -> (usize, usize) {
        let start = buffer.len();
        buffer.put_slice(self);
        (start, buffer.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_copy_data_row() {
        let mut builder = ZeroCopyMessageBuilder::new();
        
        // Test with simple values
        let values: Vec<Option<&[u8]>> = vec![
            Some(b"hello"),
            None,
            Some(b"world"),
        ];
        
        builder.build_data_row(&values);
        
        let bytes = builder.as_bytes();
        assert_eq!(bytes[0], b'D'); // Message type
        
        // Check number of columns
        assert_eq!(&bytes[5..7], &[0, 3]); // 3 columns
        
        // First value: "hello" (5 bytes)
        assert_eq!(&bytes[7..11], &[0, 0, 0, 5]); // Length
        assert_eq!(&bytes[11..16], b"hello");
        
        // Second value: NULL
        assert_eq!(&bytes[16..20], &[255, 255, 255, 255]); // -1 for NULL
        
        // Third value: "world" (5 bytes)
        assert_eq!(&bytes[20..24], &[0, 0, 0, 5]); // Length
        assert_eq!(&bytes[24..29], b"world");
    }

    #[test]
    fn test_zero_copy_encoding() {
        let mut builder = ZeroCopyMessageBuilder::new();
        
        builder.build_data_row_zero_copy(3, |col_idx, buffer| {
            match col_idx {
                0 => {
                    let start = buffer.len();
                    buffer.put_i32(42);
                    Some((start, buffer.len()))
                },
                1 => None, // NULL
                2 => {
                    let start = buffer.len();
                    buffer.put_slice(b"test");
                    Some((start, buffer.len()))
                },
                _ => unreachable!(),
            }
        });
        
        let bytes = builder.as_bytes();
        assert_eq!(bytes[0], b'D'); // Message type
        
        // Check the encoded values
        assert_eq!(&bytes[7..11], &[0, 0, 0, 4]); // First value length (4 bytes for i32)
        assert_eq!(&bytes[11..15], &42i32.to_be_bytes()); // Value: 42
        
        assert_eq!(&bytes[15..19], &[255, 255, 255, 255]); // NULL
        
        assert_eq!(&bytes[19..23], &[0, 0, 0, 4]); // Third value length
        assert_eq!(&bytes[23..27], b"test"); // Value: "test"
    }
}