//! RESP wire protocol value types.
//!
//! Defines [`RespValue`], the in-memory representation of a single RESP frame.
//! Both RESP2 and RESP3 type prefixes are represented as enum variants so the
//! parser and serializer can handle either protocol version through one unified
//! type. RESP2 clients see only the first five variants; RESP3-aware clients
//! may additionally encounter Null, Double, Boolean, Map, Set, BigNumber,
//! VerbatimString, and Push.

/// A single RESP wire protocol value.
///
/// Covers all RESP2 and RESP3 type prefixes. Used as the intermediate
/// representation between raw bytes on the wire and typed Kora commands.
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    /// RESP2 simple string (`+OK\r\n`).
    SimpleString(Vec<u8>),
    /// RESP2 error (`-ERR message\r\n`).
    Error(Vec<u8>),
    /// RESP2 integer (`:42\r\n`).
    Integer(i64),
    /// RESP2 bulk string (`$N\r\n...data...\r\n`). `None` represents the null bulk string (`$-1\r\n`).
    BulkString(Option<Vec<u8>>),
    /// RESP2 array (`*N\r\n...`). `None` represents the null array (`*-1\r\n`).
    Array(Option<Vec<RespValue>>),

    /// RESP3 null (`_\r\n`).
    Null,
    /// RESP3 double (`,3.14\r\n`).
    Double(f64),
    /// RESP3 boolean (`#t\r\n` or `#f\r\n`).
    Boolean(bool),
    /// RESP3 map (`%N\r\n key value key value ...`).
    Map(Vec<(RespValue, RespValue)>),
    /// RESP3 set (`~N\r\n member member ...`).
    Set(Vec<RespValue>),
    /// RESP3 big number (`(12345678901234567890\r\n`).
    BigNumber(Vec<u8>),
    /// RESP3 verbatim string (`=N\r\ntxt:data\r\n`).
    VerbatimString {
        /// 3-byte encoding prefix (e.g., `txt`, `mkd`).
        encoding: [u8; 3],
        /// The string payload after the encoding prefix.
        data: Vec<u8>,
    },
    /// RESP3 push message (`>N\r\n elements...`).
    Push(Vec<RespValue>),
}
