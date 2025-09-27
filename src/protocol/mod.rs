// Module for PostgreSQL wire protocol implementation
pub mod messages;
pub mod codec;
pub mod binary;
pub mod memory_mapped;
pub mod value_handler;
pub mod buffer_pool;
pub mod memory_monitor;
pub mod small_value;
pub mod rate_limiter;
pub mod parser;


pub use messages::*;
pub use codec::PostgresCodec;
pub use binary::{BinaryEncoder, ZeroCopyBinaryEncoder};
pub use memory_mapped::{MappedValue, MappedValueReader, MappedValueFactory, MemoryMappedConfig};
pub use value_handler::{ValueHandler, ValueHandlerConfig, ValueHandlerStats};
pub use buffer_pool::{BufferPool, BufferPoolConfig, BufferPoolStats, PooledBytesMut, global_buffer_pool, get_pooled_buffer};
pub use memory_monitor::{MemoryMonitor, MemoryMonitorConfig, MemoryStats, MemoryPressure, global_memory_monitor};
pub use small_value::SmallValue;
pub use rate_limiter::{RateLimiter, RateLimitConfig, CircuitBreakerConfig, RateLimitError, CircuitState, global_rate_limiter, check_global_rate_limit, record_global_failure};
pub use parser::{MessageParser, ParseError, PostgresMessage, PostgresMessageType, ProtocolState, AuthenticationRequest};

