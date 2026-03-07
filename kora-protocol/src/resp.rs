//! RESP2 data types.

/// A RESP2 protocol value.
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    /// Simple string: +OK\r\n
    SimpleString(Vec<u8>),
    /// Error: -ERR message\r\n
    Error(Vec<u8>),
    /// Integer: :42\r\n
    Integer(i64),
    /// Bulk string: $N\r\n...data...\r\n (None = $-1\r\n null)
    BulkString(Option<Vec<u8>>),
    /// Array: *N\r\n... (None = *-1\r\n null)
    Array(Option<Vec<RespValue>>),
}
