//! # kora-protocol
//!
//! RESP2/RESP3 wire protocol parser and serializer for Kōra.
//!
//! This crate handles the complete lifecycle of RESP protocol messages:
//! parsing raw bytes from the network into structured values, translating
//! those values into typed [`kora_core::command::Command`] variants, and
//! serializing [`kora_core::command::CommandResponse`] back into RESP wire
//! format.
//!
//! The parser is fully incremental -- callers feed arbitrary byte chunks
//! and extract complete frames as they become available. A dedicated
//! fast-path recognizes high-frequency commands (GET, SET, INCR, PUBLISH)
//! directly from raw bytes, skipping intermediate allocation.
//!
//! Both RESP2 and RESP3 wire formats are supported. The serializer can
//! emit native RESP3 types (Map, Set, Double, Boolean) or automatically
//! downgrade them to RESP2 equivalents.

#![warn(clippy::all)]

mod command;
mod error;
mod parser;
mod resp;
mod serializer;

pub use command::parse_command;
pub use error::ProtocolError;
pub use parser::{HotCommand, HotCommandRef, RespParser};
pub use resp::RespValue;
pub use serializer::{serialize_response, serialize_response_versioned};
