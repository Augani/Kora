//! Streaming incremental RESP2 parser.

use bytes::BytesMut;

use crate::error::ProtocolError;
use crate::resp::RespValue;

/// A streaming RESP2 parser that handles incremental data.
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
}
