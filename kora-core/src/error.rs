//! Core error types for the Kōra engine.

use thiserror::Error;

/// Errors that can occur during cache operations.
#[derive(Debug, Error)]
pub enum KoraError {
    /// The operation was performed on a key with an incompatible value type.
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    /// The command received an invalid number of arguments.
    #[error("ERR wrong number of arguments for '{0}' command")]
    WrongArity(String),

    /// A value could not be parsed as an integer.
    #[error("ERR value is not an integer or out of range")]
    NotAnInteger,

    /// The engine is shutting down.
    #[error("ERR server is shutting down")]
    ShuttingDown,

    /// A generic error with a message.
    #[error("ERR {0}")]
    Other(String),
}

/// Convenience result type for core operations.
pub type Result<T> = std::result::Result<T, KoraError>;
