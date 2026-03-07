//! Protocol-level error types.

use thiserror::Error;

/// Errors that can occur during RESP parsing or command translation.
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// Invalid RESP data.
    #[error("ERR Protocol error: {0}")]
    InvalidData(String),

    /// Not enough data to parse a complete frame.
    #[error("incomplete frame")]
    Incomplete,

    /// Unknown command name.
    #[error("ERR unknown command '{0}'")]
    UnknownCommand(String),

    /// Wrong number of arguments for a command.
    #[error("ERR wrong number of arguments for '{0}' command")]
    WrongArity(String),
}
