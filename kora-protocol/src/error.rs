//! Protocol-level error types.
//!
//! Defines [`ProtocolError`], the single error enum for everything that can go
//! wrong between receiving raw bytes on the wire and producing a typed
//! [`kora_core::command::Command`]. Variants cover malformed RESP data,
//! incomplete frames (used for incremental parsing flow control), unknown
//! command names, and arity mismatches.

use thiserror::Error;

/// Errors that can occur during RESP parsing or command translation.
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// Malformed or unexpected RESP data.
    #[error("ERR Protocol error: {0}")]
    InvalidData(String),

    /// Not enough data to parse a complete frame.
    ///
    /// This is not a true error -- the parser uses it as a signal that the
    /// caller should feed more bytes before retrying.
    #[error("incomplete frame")]
    Incomplete,

    /// The command name is not recognized.
    #[error("ERR unknown command '{0}'")]
    UnknownCommand(String),

    /// The command was recognized but supplied the wrong number of arguments.
    #[error("ERR wrong number of arguments for '{0}' command")]
    WrongArity(String),
}
