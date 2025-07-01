// Module for PostgreSQL wire protocol implementation
pub mod messages;
pub mod codec;
pub mod binary;
pub mod zero_copy;
pub mod writer;
pub mod connection;
pub mod memory_mapped;
pub mod value_handler;

#[cfg(feature = "zero-copy-protocol")]
pub mod connection_direct;

pub use messages::*;
pub use codec::PostgresCodec;
pub use binary::{BinaryEncoder, ZeroCopyBinaryEncoder};
pub use zero_copy::{ZeroCopyMessageBuilder, ZeroCopyValue};
pub use writer::{ProtocolWriter, FramedWriter, DirectWriter, WriterType};
pub use connection::{Connection, ConnectionExt};
pub use memory_mapped::{MappedValue, MappedValueReader, MappedValueFactory, MemoryMappedConfig};
pub use value_handler::{ValueHandler, ValueHandlerConfig, ValueHandlerStats};

#[cfg(feature = "zero-copy-protocol")]
pub use connection_direct::DirectConnection;