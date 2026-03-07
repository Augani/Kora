//! # kora-protocol
//!
//! RESP2/RESP3 protocol parser and serializer for Kōra.
//!
//! Provides streaming, incremental parsing of the Redis Serialization Protocol
//! and serialization of responses back to RESP format.

#![warn(clippy::all)]

mod command;
mod error;
mod parser;
mod resp;
mod serializer;

pub use command::parse_command;
pub use error::ProtocolError;
pub use parser::RespParser;
pub use resp::RespValue;
pub use serializer::serialize_response;
