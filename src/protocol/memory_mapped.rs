use std::io::{self, Write};
use memmap2::Mmap;
use tempfile::NamedTempFile;
use tokio::io::AsyncWrite;
use tracing::debug;

/// Configuration for memory-mapped value handling
#[derive(Debug, Clone)]
pub struct MemoryMappedConfig {
    /// Minimum size in bytes to use memory mapping (default: 64KB)
    pub min_size_for_mmap: usize,
    /// Maximum size for in-memory values before using temp files (default: 1MB)
    pub max_memory_size: usize,
    /// Directory for temporary files (default: system temp)
    pub temp_dir: Option<String>,
}

impl Default for MemoryMappedConfig {
    fn default() -> Self {
        Self {
            min_size_for_mmap: 64 * 1024, // 64KB
            max_memory_size: 1024 * 1024, // 1MB
            temp_dir: None,
        }
    }
}

impl MemoryMappedConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();
        
        if let Ok(val) = std::env::var("PGSQLITE_MMAP_MIN_SIZE") {
            if let Ok(size) = val.parse::<usize>() {
                config.min_size_for_mmap = size;
            }
        }
        
        if let Ok(val) = std::env::var("PGSQLITE_MMAP_MAX_MEMORY") {
            if let Ok(size) = val.parse::<usize>() {
                config.max_memory_size = size;
            }
        }
        
        if let Ok(dir) = std::env::var("PGSQLITE_TEMP_DIR") {
            config.temp_dir = Some(dir);
        }
        
        config
    }
}

/// Represents a value that can be transmitted using zero-copy techniques
pub enum MappedValue {
    /// Small values stored in memory
    Memory(Vec<u8>),
    /// Large values stored in memory-mapped files
    Mapped {
        mmap: Mmap,
        offset: usize,
        length: usize,
        _temp_file: Option<NamedTempFile>, // Keep temp file alive
    },
    /// Direct reference to existing data (zero-copy)
    Reference(&'static [u8]),
    /// Small values with optimized storage (no heap allocation)
    Small(crate::protocol::SmallValue),
}

impl MappedValue {
    /// Create a mapped value from a byte slice
    pub fn from_slice(data: &[u8], config: &MemoryMappedConfig) -> io::Result<Self> {
        if data.len() < config.min_size_for_mmap || data.len() <= config.max_memory_size {
            // Store in memory for small values
            Ok(MappedValue::Memory(data.to_vec()))
        } else {
            // Create temporary file and memory map it
            Self::create_temp_mapped(data, config)
        }
    }
    
    /// Create a mapped value from a static reference (zero-copy)
    pub fn from_static(data: &'static [u8]) -> Self {
        MappedValue::Reference(data)
    }
    
    /// Create a memory-mapped temporary file
    fn create_temp_mapped(data: &[u8], config: &MemoryMappedConfig) -> io::Result<Self> {
        let mut temp_file = if let Some(ref dir) = config.temp_dir {
            NamedTempFile::new_in(dir)?
        } else {
            NamedTempFile::new()?
        };
        
        // Write data to temp file
        temp_file.write_all(data)?;
        temp_file.flush()?;
        
        // Memory map the file
        let file = temp_file.reopen()?;
        let mmap = unsafe { Mmap::map(&file)? };
        
        debug!("Created memory-mapped value: {} bytes", data.len());
        
        Ok(MappedValue::Mapped {
            mmap,
            offset: 0,
            length: data.len(),
            _temp_file: Some(temp_file),
        })
    }
    
    /// Get a slice view of the data
    pub fn as_slice(&self) -> &[u8] {
        match self {
            MappedValue::Memory(data) => data,
            MappedValue::Mapped { mmap, offset, length, .. } => {
                &mmap[*offset..*offset + *length]
            }
            MappedValue::Reference(data) => data,
            MappedValue::Small(small) => {
                // For static small values, return their static representation
                match small {
                    crate::protocol::SmallValue::BoolTrue => b"t",
                    crate::protocol::SmallValue::BoolFalse => b"f",
                    crate::protocol::SmallValue::Zero => b"0",
                    crate::protocol::SmallValue::One => b"1",
                    crate::protocol::SmallValue::MinusOne => b"-1",
                    crate::protocol::SmallValue::Empty => b"",
                    _ => panic!("Dynamic small values cannot be converted to slice"),
                }
            }
        }
    }
    
    /// Get the length of the data
    pub fn len(&self) -> usize {
        match self {
            MappedValue::Memory(data) => data.len(),
            MappedValue::Mapped { length, .. } => *length,
            MappedValue::Reference(data) => data.len(),
            MappedValue::Small(small) => small.max_text_length(),
        }
    }
    
    /// Check if the value is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    
    /// Write the value to an async writer with zero-copy optimization
    pub async fn write_to<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> io::Result<()> {
        use tokio::io::AsyncWriteExt;
        
        match self {
            MappedValue::Memory(data) => {
                writer.write_all(data).await
            }
            MappedValue::Mapped { mmap, offset, length, .. } => {
                let slice = &mmap[*offset..*offset + *length];
                writer.write_all(slice).await
            }
            MappedValue::Reference(data) => {
                writer.write_all(data).await
            }
            MappedValue::Small(small) => {
                // Use a stack buffer for small values
                let mut buffer = [0u8; 32];
                let len = small.write_text_to_buffer(&mut buffer);
                writer.write_all(&buffer[..len]).await
            }
        }
    }
    
    /// Create a slice view of a portion of the data (not implemented for mapped values)
    pub fn slice(&self, start: usize, len: usize) -> Option<MappedValue> {
        if start + len > self.len() {
            return None;
        }
        
        match self {
            MappedValue::Memory(data) => {
                Some(MappedValue::Memory(data[start..start + len].to_vec()))
            }
            MappedValue::Mapped { .. } => {
                // Slicing mapped values is complex and not implemented yet
                // Would require additional reference counting for the mmap
                None
            }
            MappedValue::Reference(data) => {
                Some(MappedValue::Reference(&data[start..start + len]))
            }
            MappedValue::Small(_) => {
                // Small values cannot be sliced
                None
            }
        }
    }
}

/// Reader for memory-mapped values that provides zero-copy access
pub struct MappedValueReader {
    value: MappedValue,
    position: usize,
}

impl MappedValueReader {
    /// Create a new reader for a mapped value
    pub fn new(value: MappedValue) -> Self {
        Self { value, position: 0 }
    }
    
    /// Read data from the current position into a buffer
    pub fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let data = self.value.as_slice();
        let remaining = data.len().saturating_sub(self.position);
        let to_read = buf.len().min(remaining);
        
        if to_read > 0 {
            buf[..to_read].copy_from_slice(&data[self.position..self.position + to_read]);
            self.position += to_read;
        }
        
        Ok(to_read)
    }
    
    /// Get remaining bytes in the reader
    pub fn remaining(&self) -> usize {
        self.value.len().saturating_sub(self.position)
    }
    
    /// Check if the reader has reached the end
    pub fn is_at_end(&self) -> bool {
        self.position >= self.value.len()
    }
    
    /// Reset position to the beginning
    pub fn reset(&mut self) {
        self.position = 0;
    }
    
    /// Get a slice of the remaining data
    pub fn remaining_slice(&self) -> &[u8] {
        let data = self.value.as_slice();
        &data[self.position..]
    }
    
    /// Write remaining data to an async writer
    pub async fn write_remaining_to<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> io::Result<()> {
        use tokio::io::AsyncWriteExt;
        writer.write_all(self.remaining_slice()).await
    }
}

/// Factory for creating mapped values with optimal storage strategy
pub struct MappedValueFactory {
    config: MemoryMappedConfig,
}

impl MappedValueFactory {
    /// Create a new factory with default configuration
    pub fn new() -> Self {
        Self {
            config: MemoryMappedConfig::from_env(),
        }
    }
    
    /// Create a factory with custom configuration
    pub fn with_config(config: MemoryMappedConfig) -> Self {
        Self { config }
    }
    
    /// Create a mapped value from SQLite BLOB data
    pub fn create_from_blob(&self, blob_data: &[u8]) -> io::Result<MappedValue> {
        if blob_data.is_empty() {
            return Ok(MappedValue::Memory(Vec::new()));
        }
        
        MappedValue::from_slice(blob_data, &self.config)
    }
    
    /// Create a mapped value from large text data
    pub fn create_from_text(&self, text_data: &str) -> io::Result<MappedValue> {
        if text_data.is_empty() {
            return Ok(MappedValue::Memory(Vec::new()));
        }
        
        MappedValue::from_slice(text_data.as_bytes(), &self.config)
    }
    
    /// Check if a value size should use memory mapping
    pub fn should_use_mmap(&self, size: usize) -> bool {
        size >= self.config.min_size_for_mmap
    }
    
    /// Get the current configuration
    pub fn config(&self) -> &MemoryMappedConfig {
        &self.config
    }
}

impl Default for MappedValueFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // use tokio::io::AsyncWriteExt; // Used by async write tests
    
    #[test]
    fn test_memory_mapped_config() {
        let config = MemoryMappedConfig::default();
        assert_eq!(config.min_size_for_mmap, 64 * 1024);
        assert_eq!(config.max_memory_size, 1024 * 1024);
        assert!(config.temp_dir.is_none());
    }
    
    #[test]
    fn test_small_value_storage() {
        let config = MemoryMappedConfig::default();
        let data = b"hello world";
        let value = MappedValue::from_slice(data, &config).unwrap();
        
        match value {
            MappedValue::Memory(ref mem_data) => {
                assert_eq!(mem_data, data);
            }
            _ => panic!("Expected memory storage for small value"),
        }
        
        assert_eq!(value.as_slice(), data);
        assert_eq!(value.len(), data.len());
    }
    
    #[tokio::test]
    async fn test_memory_mapped_value_read() {
        let data = vec![1, 2, 3, 4, 5];
        let value = MappedValue::Memory(data.clone());
        let mut reader = MappedValueReader::new(value);
        
        let mut buf = [0u8; 3];
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf, &[1, 2, 3]);
        
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf[..2], &[4, 5]);
        
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(n, 0);
    }
    
    #[tokio::test]
    async fn test_memory_mapped_value_write() {
        let data = b"test data for writing";
        let value = MappedValue::Memory(data.to_vec());
        
        let mut output = Vec::new();
        value.write_to(&mut output).await.unwrap();
        
        assert_eq!(output, data);
    }
    
    #[test]
    fn test_mapped_value_factory() {
        let factory = MappedValueFactory::new();
        
        // Small blob should use memory
        let small_blob = b"small";
        let value = factory.create_from_blob(small_blob).unwrap();
        match value {
            MappedValue::Memory(_) => {}
            _ => panic!("Expected memory storage for small blob"),
        }
        
        // Test empty blob
        let empty_value = factory.create_from_blob(&[]).unwrap();
        assert!(empty_value.is_empty());
    }
    
    #[test]
    fn test_value_slicing() {
        let data = b"hello world test";
        let value = MappedValue::Memory(data.to_vec());
        
        let slice = value.slice(6, 5).unwrap();
        assert_eq!(slice.as_slice(), b"world");
        
        let invalid_slice = value.slice(10, 20);
        assert!(invalid_slice.is_none());
    }
    
    #[tokio::test]
    async fn test_large_value_temp_file() {
        let config = MemoryMappedConfig {
            min_size_for_mmap: 10, // Very small threshold for testing
            max_memory_size: 5,    // Force temp file usage
            temp_dir: None,
        };
        
        let data = b"this is larger than the memory limit";
        let value = MappedValue::from_slice(data, &config).unwrap();
        
        match value {
            MappedValue::Mapped { .. } => {
                assert_eq!(value.as_slice(), data);
                assert_eq!(value.len(), data.len());
            }
            _ => panic!("Expected mapped storage for large value"),
        }
    }
}