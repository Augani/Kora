//! RESP2/RESP3 data types.

/// A RESP protocol value (supports both RESP2 and RESP3).
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    // ─── RESP2 types ─────────────────────────────────────────────
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

    // ─── RESP3 types ─────────────────────────────────────────────
    /// Null: _\r\n
    Null,
    /// Double: ,3.14\r\n
    Double(f64),
    /// Boolean: #t\r\n or #f\r\n
    Boolean(bool),
    /// Map: %N\r\n key value key value ...
    Map(Vec<(RespValue, RespValue)>),
    /// Set: ~N\r\n member member ...
    Set(Vec<RespValue>),
    /// Big number: (12345678901234567890\r\n
    BigNumber(Vec<u8>),
    /// Verbatim string: =N\r\ntxt:data\r\n
    VerbatimString {
        /// 3-byte encoding (e.g., "txt", "mkd").
        encoding: [u8; 3],
        /// The string data.
        data: Vec<u8>,
    },
    /// Push: >N\r\n elements...
    Push(Vec<RespValue>),
}
