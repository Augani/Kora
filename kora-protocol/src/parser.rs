//! Streaming incremental RESP2/RESP3 parser.

use bytes::BytesMut;

use crate::error::ProtocolError;
use crate::resp::RespValue;

/// A streaming RESP parser that handles incremental data.
///
/// Supports both RESP2 and RESP3 wire formats.
pub struct RespParser {
    buffer: BytesMut,
}

impl RespParser {
    /// Create a new parser.
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(4096),
        }
    }

    /// Append raw bytes from a TCP read.
    pub fn feed(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Attempt to parse one complete RESP frame from the buffer.
    ///
    /// Returns `Ok(Some(frame))` if complete, `Ok(None)` if more data needed,
    /// `Err` on malformed input.
    pub fn try_parse(&mut self) -> Result<Option<RespValue>, ProtocolError> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        match parse_value(&self.buffer) {
            Ok((value, consumed)) => {
                let _ = self.buffer.split_to(consumed);
                Ok(Some(value))
            }
            Err(ProtocolError::Incomplete) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Get the current buffer length.
    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }
}

impl Default for RespParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse a single RESP value from a byte slice.
/// Returns the parsed value and the number of bytes consumed.
fn parse_value(data: &[u8]) -> Result<(RespValue, usize), ProtocolError> {
    if data.is_empty() {
        return Err(ProtocolError::Incomplete);
    }

    match data[0] {
        b'+' => parse_simple_string(data),
        b'-' => parse_error(data),
        b':' => parse_integer(data),
        b'$' => parse_bulk_string(data),
        b'*' => parse_array(data),
        // RESP3 types
        b'_' => parse_null(data),
        b',' => parse_double(data),
        b'#' => parse_boolean(data),
        b'%' => parse_map(data),
        b'~' => parse_set(data),
        b'(' => parse_big_number(data),
        b'=' => parse_verbatim_string(data),
        b'>' => parse_push(data),
        _ => {
            // Try to parse as inline command (plain text like "PING\r\n")
            parse_inline(data)
        }
    }
}

fn find_crlf(data: &[u8]) -> Option<usize> {
    data.windows(2).position(|w| w == b"\r\n")
}

fn parse_simple_string(data: &[u8]) -> Result<(RespValue, usize), ProtocolError> {
    match find_crlf(data) {
        Some(pos) => {
            let content = data[1..pos].to_vec();
            Ok((RespValue::SimpleString(content), pos + 2))
        }
        None => Err(ProtocolError::Incomplete),
    }
}

fn parse_error(data: &[u8]) -> Result<(RespValue, usize), ProtocolError> {
    match find_crlf(data) {
        Some(pos) => {
            let content = data[1..pos].to_vec();
            Ok((RespValue::Error(content), pos + 2))
        }
        None => Err(ProtocolError::Incomplete),
    }
}

fn parse_integer(data: &[u8]) -> Result<(RespValue, usize), ProtocolError> {
    match find_crlf(data) {
        Some(pos) => {
            let s = std::str::from_utf8(&data[1..pos])
                .map_err(|_| ProtocolError::InvalidData("invalid integer encoding".into()))?;
            let n: i64 = s
                .parse()
                .map_err(|_| ProtocolError::InvalidData(format!("invalid integer: {}", s)))?;
            Ok((RespValue::Integer(n), pos + 2))
        }
        None => Err(ProtocolError::Incomplete),
    }
}

fn parse_bulk_string(data: &[u8]) -> Result<(RespValue, usize), ProtocolError> {
    let crlf_pos = find_crlf(data).ok_or(ProtocolError::Incomplete)?;
    let len_str = std::str::from_utf8(&data[1..crlf_pos])
        .map_err(|_| ProtocolError::InvalidData("invalid bulk string length".into()))?;
    let len: i64 = len_str
        .parse()
        .map_err(|_| ProtocolError::InvalidData(format!("invalid length: {}", len_str)))?;

    if len == -1 {
        return Ok((RespValue::BulkString(None), crlf_pos + 2));
    }

    if len < 0 {
        return Err(ProtocolError::InvalidData(format!(
            "invalid bulk string length: {}",
            len
        )));
    }

    let len = len as usize;
    let data_start = crlf_pos + 2;
    let data_end = data_start + len;
    let total = data_end + 2; // +2 for trailing \r\n

    if data.len() < total {
        return Err(ProtocolError::Incomplete);
    }

    let content = data[data_start..data_end].to_vec();
    Ok((RespValue::BulkString(Some(content)), total))
}

fn parse_array(data: &[u8]) -> Result<(RespValue, usize), ProtocolError> {
    let crlf_pos = find_crlf(data).ok_or(ProtocolError::Incomplete)?;
    let len_str = std::str::from_utf8(&data[1..crlf_pos])
        .map_err(|_| ProtocolError::InvalidData("invalid array length".into()))?;
    let len: i64 = len_str
        .parse()
        .map_err(|_| ProtocolError::InvalidData(format!("invalid array length: {}", len_str)))?;

    if len == -1 {
        return Ok((RespValue::Array(None), crlf_pos + 2));
    }

    if len < 0 {
        return Err(ProtocolError::InvalidData(format!(
            "invalid array length: {}",
            len
        )));
    }

    let mut offset = crlf_pos + 2;
    let mut items = Vec::with_capacity(len as usize);

    for _ in 0..len {
        let (value, consumed) = parse_value(&data[offset..])?;
        items.push(value);
        offset += consumed;
    }

    Ok((RespValue::Array(Some(items)), offset))
}

// ─── RESP3 parsers ─────────────────────────────────────────────────────

fn parse_null(data: &[u8]) -> Result<(RespValue, usize), ProtocolError> {
    let crlf_pos = find_crlf(data).ok_or(ProtocolError::Incomplete)?;
    Ok((RespValue::Null, crlf_pos + 2))
}

fn parse_double(data: &[u8]) -> Result<(RespValue, usize), ProtocolError> {
    let crlf_pos = find_crlf(data).ok_or(ProtocolError::Incomplete)?;
    let s = std::str::from_utf8(&data[1..crlf_pos])
        .map_err(|_| ProtocolError::InvalidData("invalid double encoding".into()))?;

    let val = match s {
        "inf" => f64::INFINITY,
        "-inf" => f64::NEG_INFINITY,
        _ => s
            .parse::<f64>()
            .map_err(|_| ProtocolError::InvalidData(format!("invalid double: {}", s)))?,
    };
    Ok((RespValue::Double(val), crlf_pos + 2))
}

fn parse_boolean(data: &[u8]) -> Result<(RespValue, usize), ProtocolError> {
    let crlf_pos = find_crlf(data).ok_or(ProtocolError::Incomplete)?;
    if crlf_pos != 2 {
        return Err(ProtocolError::InvalidData("invalid boolean".into()));
    }
    match data[1] {
        b't' => Ok((RespValue::Boolean(true), crlf_pos + 2)),
        b'f' => Ok((RespValue::Boolean(false), crlf_pos + 2)),
        _ => Err(ProtocolError::InvalidData("invalid boolean value".into())),
    }
}

fn parse_map(data: &[u8]) -> Result<(RespValue, usize), ProtocolError> {
    let crlf_pos = find_crlf(data).ok_or(ProtocolError::Incomplete)?;
    let len_str = std::str::from_utf8(&data[1..crlf_pos])
        .map_err(|_| ProtocolError::InvalidData("invalid map length".into()))?;
    let len: i64 = len_str
        .parse()
        .map_err(|_| ProtocolError::InvalidData(format!("invalid map length: {}", len_str)))?;

    if len < 0 {
        return Err(ProtocolError::InvalidData(format!(
            "invalid map length: {}",
            len
        )));
    }

    let mut offset = crlf_pos + 2;
    let mut pairs = Vec::with_capacity(len as usize);

    for _ in 0..len {
        let (key, consumed_key) = parse_value(&data[offset..])?;
        offset += consumed_key;
        let (value, consumed_val) = parse_value(&data[offset..])?;
        offset += consumed_val;
        pairs.push((key, value));
    }

    Ok((RespValue::Map(pairs), offset))
}

fn parse_set(data: &[u8]) -> Result<(RespValue, usize), ProtocolError> {
    let crlf_pos = find_crlf(data).ok_or(ProtocolError::Incomplete)?;
    let len_str = std::str::from_utf8(&data[1..crlf_pos])
        .map_err(|_| ProtocolError::InvalidData("invalid set length".into()))?;
    let len: i64 = len_str
        .parse()
        .map_err(|_| ProtocolError::InvalidData(format!("invalid set length: {}", len_str)))?;

    if len < 0 {
        return Err(ProtocolError::InvalidData(format!(
            "invalid set length: {}",
            len
        )));
    }

    let mut offset = crlf_pos + 2;
    let mut items = Vec::with_capacity(len as usize);

    for _ in 0..len {
        let (value, consumed) = parse_value(&data[offset..])?;
        items.push(value);
        offset += consumed;
    }

    Ok((RespValue::Set(items), offset))
}

fn parse_big_number(data: &[u8]) -> Result<(RespValue, usize), ProtocolError> {
    let crlf_pos = find_crlf(data).ok_or(ProtocolError::Incomplete)?;
    let content = data[1..crlf_pos].to_vec();
    Ok((RespValue::BigNumber(content), crlf_pos + 2))
}

fn parse_verbatim_string(data: &[u8]) -> Result<(RespValue, usize), ProtocolError> {
    let crlf_pos = find_crlf(data).ok_or(ProtocolError::Incomplete)?;
    let len_str = std::str::from_utf8(&data[1..crlf_pos])
        .map_err(|_| ProtocolError::InvalidData("invalid verbatim string length".into()))?;
    let len: usize = len_str.parse().map_err(|_| {
        ProtocolError::InvalidData(format!("invalid verbatim string length: {}", len_str))
    })?;

    let data_start = crlf_pos + 2;
    let data_end = data_start + len;
    let total = data_end + 2;

    if data.len() < total {
        return Err(ProtocolError::Incomplete);
    }

    let content = &data[data_start..data_end];
    if content.len() < 4 || content[3] != b':' {
        return Err(ProtocolError::InvalidData(
            "verbatim string missing encoding prefix".into(),
        ));
    }

    let mut encoding = [0u8; 3];
    encoding.copy_from_slice(&content[..3]);
    let string_data = content[4..].to_vec();

    Ok((
        RespValue::VerbatimString {
            encoding,
            data: string_data,
        },
        total,
    ))
}

fn parse_push(data: &[u8]) -> Result<(RespValue, usize), ProtocolError> {
    let crlf_pos = find_crlf(data).ok_or(ProtocolError::Incomplete)?;
    let len_str = std::str::from_utf8(&data[1..crlf_pos])
        .map_err(|_| ProtocolError::InvalidData("invalid push length".into()))?;
    let len: i64 = len_str
        .parse()
        .map_err(|_| ProtocolError::InvalidData(format!("invalid push length: {}", len_str)))?;

    if len < 0 {
        return Err(ProtocolError::InvalidData(format!(
            "invalid push length: {}",
            len
        )));
    }

    let mut offset = crlf_pos + 2;
    let mut items = Vec::with_capacity(len as usize);

    for _ in 0..len {
        let (value, consumed) = parse_value(&data[offset..])?;
        items.push(value);
        offset += consumed;
    }

    Ok((RespValue::Push(items), offset))
}

fn parse_inline(data: &[u8]) -> Result<(RespValue, usize), ProtocolError> {
    match find_crlf(data) {
        Some(pos) => {
            let line = &data[..pos];
            let parts: Vec<&[u8]> = line
                .split(|b| *b == b' ')
                .filter(|p| !p.is_empty())
                .collect();

            if parts.is_empty() {
                return Err(ProtocolError::InvalidData("empty inline command".into()));
            }

            let items: Vec<RespValue> = parts
                .into_iter()
                .map(|p| RespValue::BulkString(Some(p.to_vec())))
                .collect();

            Ok((RespValue::Array(Some(items)), pos + 2))
        }
        None => Err(ProtocolError::Incomplete),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let mut parser = RespParser::new();
        parser.feed(b"+OK\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(val, RespValue::SimpleString(b"OK".to_vec()));
    }

    #[test]
    fn test_parse_error() {
        let mut parser = RespParser::new();
        parser.feed(b"-ERR unknown\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(val, RespValue::Error(b"ERR unknown".to_vec()));
    }

    #[test]
    fn test_parse_integer() {
        let mut parser = RespParser::new();
        parser.feed(b":42\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(val, RespValue::Integer(42));
    }

    #[test]
    fn test_parse_negative_integer() {
        let mut parser = RespParser::new();
        parser.feed(b":-1\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(val, RespValue::Integer(-1));
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut parser = RespParser::new();
        parser.feed(b"$5\r\nhello\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(val, RespValue::BulkString(Some(b"hello".to_vec())));
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let mut parser = RespParser::new();
        parser.feed(b"$-1\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(val, RespValue::BulkString(None));
    }

    #[test]
    fn test_parse_empty_bulk_string() {
        let mut parser = RespParser::new();
        parser.feed(b"$0\r\n\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(val, RespValue::BulkString(Some(vec![])));
    }

    #[test]
    fn test_parse_array() {
        let mut parser = RespParser::new();
        parser.feed(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(
            val,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"GET".to_vec())),
                RespValue::BulkString(Some(b"key".to_vec())),
            ]))
        );
    }

    #[test]
    fn test_parse_null_array() {
        let mut parser = RespParser::new();
        parser.feed(b"*-1\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(val, RespValue::Array(None));
    }

    #[test]
    fn test_parse_incomplete_returns_none() {
        let mut parser = RespParser::new();
        parser.feed(b"$5\r\nhel");
        assert!(parser.try_parse().unwrap().is_none());
    }

    #[test]
    fn test_parse_byte_by_byte() {
        let mut parser = RespParser::new();
        let data = b"+OK\r\n";
        for &byte in data.iter().take(data.len() - 1) {
            parser.feed(&[byte]);
            assert!(parser.try_parse().unwrap().is_none());
        }
        parser.feed(&[data[data.len() - 1]]);
        assert!(parser.try_parse().unwrap().is_some());
    }

    #[test]
    fn test_parse_multiple_frames() {
        let mut parser = RespParser::new();
        parser.feed(b"+OK\r\n:42\r\n");
        assert!(parser.try_parse().unwrap().is_some());
        assert!(parser.try_parse().unwrap().is_some());
        assert!(parser.try_parse().unwrap().is_none());
    }

    #[test]
    fn test_parse_inline_command() {
        let mut parser = RespParser::new();
        parser.feed(b"PING\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(
            val,
            RespValue::Array(Some(vec![RespValue::BulkString(Some(b"PING".to_vec()))]))
        );
    }

    #[test]
    fn test_parse_inline_with_args() {
        let mut parser = RespParser::new();
        parser.feed(b"SET foo bar\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(
            val,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"SET".to_vec())),
                RespValue::BulkString(Some(b"foo".to_vec())),
                RespValue::BulkString(Some(b"bar".to_vec())),
            ]))
        );
    }

    #[test]
    fn test_parse_nested_array() {
        let mut parser = RespParser::new();
        parser.feed(b"*2\r\n*1\r\n:1\r\n*1\r\n:2\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(
            val,
            RespValue::Array(Some(vec![
                RespValue::Array(Some(vec![RespValue::Integer(1)])),
                RespValue::Array(Some(vec![RespValue::Integer(2)])),
            ]))
        );
    }

    // ─── RESP3 tests ─────────────────────────────────────────────

    #[test]
    fn test_parse_null() {
        let mut parser = RespParser::new();
        parser.feed(b"_\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(val, RespValue::Null);
    }

    #[test]
    fn test_parse_double() {
        let mut parser = RespParser::new();
        parser.feed(b",3.14\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(val, RespValue::Double(3.14));
    }

    #[test]
    fn test_parse_double_inf() {
        let mut parser = RespParser::new();
        parser.feed(b",inf\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(val, RespValue::Double(f64::INFINITY));
    }

    #[test]
    fn test_parse_boolean_true() {
        let mut parser = RespParser::new();
        parser.feed(b"#t\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(val, RespValue::Boolean(true));
    }

    #[test]
    fn test_parse_boolean_false() {
        let mut parser = RespParser::new();
        parser.feed(b"#f\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(val, RespValue::Boolean(false));
    }

    #[test]
    fn test_parse_map() {
        let mut parser = RespParser::new();
        parser.feed(b"%2\r\n+key1\r\n:1\r\n+key2\r\n:2\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(
            val,
            RespValue::Map(vec![
                (
                    RespValue::SimpleString(b"key1".to_vec()),
                    RespValue::Integer(1)
                ),
                (
                    RespValue::SimpleString(b"key2".to_vec()),
                    RespValue::Integer(2)
                ),
            ])
        );
    }

    #[test]
    fn test_parse_set() {
        let mut parser = RespParser::new();
        parser.feed(b"~3\r\n:1\r\n:2\r\n:3\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(
            val,
            RespValue::Set(vec![
                RespValue::Integer(1),
                RespValue::Integer(2),
                RespValue::Integer(3),
            ])
        );
    }

    #[test]
    fn test_parse_big_number() {
        let mut parser = RespParser::new();
        parser.feed(b"(12345678901234567890\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(val, RespValue::BigNumber(b"12345678901234567890".to_vec()));
    }

    #[test]
    fn test_parse_verbatim_string() {
        let mut parser = RespParser::new();
        parser.feed(b"=9\r\ntxt:hello\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(
            val,
            RespValue::VerbatimString {
                encoding: *b"txt",
                data: b"hello".to_vec(),
            }
        );
    }

    #[test]
    fn test_parse_push() {
        let mut parser = RespParser::new();
        parser.feed(b">2\r\n+subscribe\r\n+channel\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(
            val,
            RespValue::Push(vec![
                RespValue::SimpleString(b"subscribe".to_vec()),
                RespValue::SimpleString(b"channel".to_vec()),
            ])
        );
    }
}
