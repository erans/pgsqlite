// Module for PostgreSQL wire protocol implementation
pub mod messages;
pub mod codec;
pub mod binary;
pub mod zero_copy;
pub mod writer;
pub mod connection;

#[cfg(feature = "zero-copy-protocol")]
pub mod connection_direct;

pub use messages::*;
pub use codec::PostgresCodec;
pub use binary::{BinaryEncoder, ZeroCopyBinaryEncoder};
pub use zero_copy::{ZeroCopyMessageBuilder, ZeroCopyValue};
pub use writer::{ProtocolWriter, FramedWriter, DirectWriter, WriterType};
pub use connection::{Connection, ConnectionExt};

#[cfg(feature = "zero-copy-protocol")]
pub use connection_direct::DirectConnection;