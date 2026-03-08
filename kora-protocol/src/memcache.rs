//! Memcached text protocol parser and serializer.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::BytesMut;

use crate::error::ProtocolError;

const MAX_KEY_LEN: usize = 250;
const THIRTY_DAYS_SECS: u64 = 60 * 60 * 24 * 30;

/// Storage command mode for memcached text protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemcacheStoreMode {
    /// `set`
    Set,
    /// `add`
    Add,
    /// `replace`
    Replace,
}

/// A parsed memcached text protocol command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemcacheCommand {
    /// `get key [key ...]`
    Get {
        /// Keys to fetch.
        keys: Vec<Vec<u8>>,
    },
    /// `set` / `add` / `replace`
    Store {
        /// Storage mode.
        mode: MemcacheStoreMode,
        /// Item key.
        key: Vec<u8>,
        /// Item flags.
        flags: u32,
        /// Optional TTL in seconds. `Some(0)` means immediate expiry.
        ttl_seconds: Option<u64>,
        /// Raw value bytes.
        value: Vec<u8>,
        /// Suppress the response when true.
        noreply: bool,
    },
    /// `delete key [noreply]`
    Delete {
        /// Item key.
        key: Vec<u8>,
        /// Suppress the response when true.
        noreply: bool,
    },
    /// `incr key value [noreply]`
    Incr {
        /// Item key.
        key: Vec<u8>,
        /// Increment amount.
        value: u64,
        /// Suppress the response when true.
        noreply: bool,
    },
    /// `decr key value [noreply]`
    Decr {
        /// Item key.
        key: Vec<u8>,
        /// Decrement amount.
        value: u64,
        /// Suppress the response when true.
        noreply: bool,
    },
    /// `touch key exptime [noreply]`
    Touch {
        /// Item key.
        key: Vec<u8>,
        /// Optional TTL in seconds. `Some(0)` means immediate expiry.
        ttl_seconds: Option<u64>,
        /// Suppress the response when true.
        noreply: bool,
    },
    /// `flush_all [delay] [noreply]`
    FlushAll {
        /// Optional flush delay in seconds.
        delay_seconds: Option<u64>,
        /// Suppress the response when true.
        noreply: bool,
    },
    /// `stats [section]`
    Stats {
        /// Optional stats section.
        section: Option<String>,
    },
    /// `version`
    Version,
    /// `quit`
    Quit,
}

/// A returned memcached item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemcacheValue {
    /// Item key.
    pub key: Vec<u8>,
    /// Item flags.
    pub flags: u32,
    /// Item payload.
    pub data: Vec<u8>,
}

/// A memcached text protocol response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemcacheResponse {
    /// `VALUE ...` rows terminated by `END`.
    Values(Vec<MemcacheValue>),
    /// `STORED`
    Stored,
    /// `NOT_STORED`
    NotStored,
    /// `EXISTS`
    Exists,
    /// `DELETED`
    Deleted,
    /// `NOT_FOUND`
    NotFound,
    /// `TOUCHED`
    Touched,
    /// `OK`
    Ok,
    /// A decimal numeric response, used by `incr`/`decr`.
    Numeric(u64),
    /// `STAT key value` rows terminated by `END`.
    Stats(Vec<(String, String)>),
    /// `VERSION <version>`
    Version(String),
    /// `ERROR`
    Error,
    /// `CLIENT_ERROR <message>`
    ClientError(String),
    /// `SERVER_ERROR <message>`
    ServerError(String),
}

/// Streaming parser for the memcached text protocol.
pub struct MemcacheParser {
    buffer: BytesMut,
}

impl MemcacheParser {
    /// Create a new parser.
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(4096),
        }
    }

    /// Append raw bytes from the connection.
    pub fn feed(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Attempt to parse one complete command.
    pub fn try_parse(&mut self) -> Result<Option<MemcacheCommand>, ProtocolError> {
        let Some(line_end) = find_crlf(&self.buffer) else {
            return Ok(None);
        };

        let line = &self.buffer[..line_end];
        if line.is_empty() {
            return Err(ProtocolError::InvalidData("empty command".into()));
        }

        let parts = split_ascii_words(line);
        if parts.is_empty() {
            return Err(ProtocolError::InvalidData("empty command".into()));
        }

        let cmd = parts[0];
        let consumed = line_end + 2;
        let parsed = if eq_ascii(cmd, b"get") {
            parse_get(&parts, consumed)
        } else if eq_ascii(cmd, b"set") || eq_ascii(cmd, b"add") || eq_ascii(cmd, b"replace") {
            parse_store(&self.buffer, line_end, &parts)
        } else if eq_ascii(cmd, b"delete") {
            parse_delete(&parts, consumed)
        } else if eq_ascii(cmd, b"incr") {
            parse_delta(&parts, true, consumed)
        } else if eq_ascii(cmd, b"decr") {
            parse_delta(&parts, false, consumed)
        } else if eq_ascii(cmd, b"touch") {
            parse_touch(&parts, consumed)
        } else if eq_ascii(cmd, b"flush_all") {
            parse_flush_all(&parts, consumed)
        } else if eq_ascii(cmd, b"stats") {
            parse_stats(&parts, consumed)
        } else if eq_ascii(cmd, b"version") {
            parse_simple(&parts, MemcacheCommand::Version, consumed)
        } else if eq_ascii(cmd, b"quit") {
            parse_simple(&parts, MemcacheCommand::Quit, consumed)
        } else {
            Err(ProtocolError::UnknownCommand(
                String::from_utf8_lossy(cmd).into_owned(),
            ))
        }?;

        match parsed {
            Some((command, consumed)) => {
                let _ = self.buffer.split_to(consumed);
                Ok(Some(command))
            }
            None => Ok(None),
        }
    }
}

impl Default for MemcacheParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Serialize a memcached response into the provided buffer.
pub fn serialize_memcache_response(resp: &MemcacheResponse, buf: &mut BytesMut) {
    match resp {
        MemcacheResponse::Values(items) => {
            for item in items {
                buf.extend_from_slice(b"VALUE ");
                buf.extend_from_slice(&item.key);
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(item.flags.to_string().as_bytes());
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(item.data.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(&item.data);
                buf.extend_from_slice(b"\r\n");
            }
            buf.extend_from_slice(b"END\r\n");
        }
        MemcacheResponse::Stored => buf.extend_from_slice(b"STORED\r\n"),
        MemcacheResponse::NotStored => buf.extend_from_slice(b"NOT_STORED\r\n"),
        MemcacheResponse::Exists => buf.extend_from_slice(b"EXISTS\r\n"),
        MemcacheResponse::Deleted => buf.extend_from_slice(b"DELETED\r\n"),
        MemcacheResponse::NotFound => buf.extend_from_slice(b"NOT_FOUND\r\n"),
        MemcacheResponse::Touched => buf.extend_from_slice(b"TOUCHED\r\n"),
        MemcacheResponse::Ok => buf.extend_from_slice(b"OK\r\n"),
        MemcacheResponse::Numeric(value) => {
            buf.extend_from_slice(value.to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        MemcacheResponse::Stats(entries) => {
            for (key, value) in entries {
                buf.extend_from_slice(b"STAT ");
                buf.extend_from_slice(key.as_bytes());
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(value.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            buf.extend_from_slice(b"END\r\n");
        }
        MemcacheResponse::Version(version) => {
            buf.extend_from_slice(b"VERSION ");
            buf.extend_from_slice(version.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        MemcacheResponse::Error => buf.extend_from_slice(b"ERROR\r\n"),
        MemcacheResponse::ClientError(message) => {
            buf.extend_from_slice(b"CLIENT_ERROR ");
            buf.extend_from_slice(message.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        MemcacheResponse::ServerError(message) => {
            buf.extend_from_slice(b"SERVER_ERROR ");
            buf.extend_from_slice(message.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
    }
}

fn parse_get(
    parts: &[&[u8]],
    consumed: usize,
) -> Result<Option<(MemcacheCommand, usize)>, ProtocolError> {
    if parts.len() < 2 {
        return Err(ProtocolError::WrongArity("get".into()));
    }
    let keys = parts[1..]
        .iter()
        .map(|part| parse_key(part))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some((MemcacheCommand::Get { keys }, consumed)))
}

fn parse_store(
    buffer: &[u8],
    line_end: usize,
    parts: &[&[u8]],
) -> Result<Option<(MemcacheCommand, usize)>, ProtocolError> {
    if parts.len() != 5 && parts.len() != 6 {
        return Err(ProtocolError::WrongArity(
            String::from_utf8_lossy(parts[0]).into_owned(),
        ));
    }

    let noreply = parse_noreply(parts.get(5).copied())?;
    let mode = match parts[0] {
        cmd if eq_ascii(cmd, b"set") => MemcacheStoreMode::Set,
        cmd if eq_ascii(cmd, b"add") => MemcacheStoreMode::Add,
        _ => MemcacheStoreMode::Replace,
    };
    let key = parse_key(parts[1])?;
    let flags = parse_u32(parts[2], "flags")?;
    let ttl_seconds = parse_ttl_seconds(parts[3])?;
    let bytes = parse_usize(parts[4], "bytes")?;
    let header_consumed = line_end + 2;
    let total_consumed = header_consumed
        .checked_add(bytes)
        .and_then(|n| n.checked_add(2))
        .ok_or_else(|| ProtocolError::InvalidData("command too large".into()))?;
    if buffer.len() < total_consumed {
        return Ok(None);
    }
    if &buffer[header_consumed + bytes..total_consumed] != b"\r\n" {
        return Err(ProtocolError::InvalidData(
            "storage value missing terminating CRLF".into(),
        ));
    }
    let value = buffer[header_consumed..header_consumed + bytes].to_vec();
    Ok(Some((
        MemcacheCommand::Store {
            mode,
            key,
            flags,
            ttl_seconds,
            value,
            noreply,
        },
        total_consumed,
    )))
}

fn parse_delete(
    parts: &[&[u8]],
    consumed: usize,
) -> Result<Option<(MemcacheCommand, usize)>, ProtocolError> {
    if parts.len() != 2 && parts.len() != 3 {
        return Err(ProtocolError::WrongArity("delete".into()));
    }
    let key = parse_key(parts[1])?;
    let noreply = parse_noreply(parts.get(2).copied())?;
    Ok(Some((MemcacheCommand::Delete { key, noreply }, consumed)))
}

fn parse_delta(
    parts: &[&[u8]],
    incr: bool,
    consumed: usize,
) -> Result<Option<(MemcacheCommand, usize)>, ProtocolError> {
    if parts.len() != 3 && parts.len() != 4 {
        let name = if incr { "incr" } else { "decr" };
        return Err(ProtocolError::WrongArity(name.into()));
    }
    let key = parse_key(parts[1])?;
    let value = parse_u64(parts[2], "value")?;
    let noreply = parse_noreply(parts.get(3).copied())?;
    let command = if incr {
        MemcacheCommand::Incr {
            key,
            value,
            noreply,
        }
    } else {
        MemcacheCommand::Decr {
            key,
            value,
            noreply,
        }
    };
    Ok(Some((command, consumed)))
}

fn parse_touch(
    parts: &[&[u8]],
    consumed: usize,
) -> Result<Option<(MemcacheCommand, usize)>, ProtocolError> {
    if parts.len() != 3 && parts.len() != 4 {
        return Err(ProtocolError::WrongArity("touch".into()));
    }
    let key = parse_key(parts[1])?;
    let ttl_seconds = parse_ttl_seconds(parts[2])?;
    let noreply = parse_noreply(parts.get(3).copied())?;
    Ok(Some((
        MemcacheCommand::Touch {
            key,
            ttl_seconds,
            noreply,
        },
        consumed,
    )))
}

fn parse_flush_all(
    parts: &[&[u8]],
    consumed: usize,
) -> Result<Option<(MemcacheCommand, usize)>, ProtocolError> {
    if parts.len() > 3 {
        return Err(ProtocolError::WrongArity("flush_all".into()));
    }
    let mut delay_seconds = None;
    let mut noreply = false;
    for part in &parts[1..] {
        if eq_ascii(part, b"noreply") {
            if noreply {
                return Err(ProtocolError::InvalidData("duplicate noreply".into()));
            }
            noreply = true;
        } else if delay_seconds.is_none() {
            delay_seconds = Some(parse_u64(part, "delay")?);
        } else {
            return Err(ProtocolError::WrongArity("flush_all".into()));
        }
    }
    Ok(Some((
        MemcacheCommand::FlushAll {
            delay_seconds,
            noreply,
        },
        consumed,
    )))
}

fn parse_stats(
    parts: &[&[u8]],
    consumed: usize,
) -> Result<Option<(MemcacheCommand, usize)>, ProtocolError> {
    if parts.len() > 2 {
        return Err(ProtocolError::WrongArity("stats".into()));
    }
    let section = parts
        .get(1)
        .map(|part| String::from_utf8_lossy(part).into_owned());
    Ok(Some((MemcacheCommand::Stats { section }, consumed)))
}

fn parse_simple(
    parts: &[&[u8]],
    command: MemcacheCommand,
    consumed: usize,
) -> Result<Option<(MemcacheCommand, usize)>, ProtocolError> {
    if parts.len() != 1 {
        let name = match command {
            MemcacheCommand::Version => "version",
            MemcacheCommand::Quit => "quit",
            _ => unreachable!("simple parser used with unsupported command"),
        };
        return Err(ProtocolError::WrongArity(name.into()));
    }
    Ok(Some((command, consumed)))
}

fn parse_key(bytes: &[u8]) -> Result<Vec<u8>, ProtocolError> {
    if bytes.is_empty() {
        return Err(ProtocolError::InvalidData("key must not be empty".into()));
    }
    if bytes.len() > MAX_KEY_LEN {
        return Err(ProtocolError::InvalidData("key exceeds 250 bytes".into()));
    }
    if bytes
        .iter()
        .any(|b| b.is_ascii_whitespace() || *b == 0 || *b < 0x20 || *b == 0x7f)
    {
        return Err(ProtocolError::InvalidData(
            "key contains invalid characters".into(),
        ));
    }
    Ok(bytes.to_vec())
}

fn parse_ttl_seconds(bytes: &[u8]) -> Result<Option<u64>, ProtocolError> {
    let raw = parse_u64(bytes, "exptime")?;
    if raw == 0 {
        return Ok(None);
    }
    if raw <= THIRTY_DAYS_SECS {
        return Ok(Some(raw));
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();
    Ok(Some(raw.saturating_sub(now)))
}

fn parse_noreply(part: Option<&[u8]>) -> Result<bool, ProtocolError> {
    match part {
        None => Ok(false),
        Some(word) if eq_ascii(word, b"noreply") => Ok(true),
        Some(_) => Err(ProtocolError::InvalidData(
            "expected optional noreply token".into(),
        )),
    }
}

fn parse_u32(bytes: &[u8], field: &str) -> Result<u32, ProtocolError> {
    let s = std::str::from_utf8(bytes)
        .map_err(|_| ProtocolError::InvalidData(format!("{} must be ASCII decimal", field)))?;
    s.parse::<u32>()
        .map_err(|_| ProtocolError::InvalidData(format!("{} is not a valid u32", field)))
}

fn parse_u64(bytes: &[u8], field: &str) -> Result<u64, ProtocolError> {
    let s = std::str::from_utf8(bytes)
        .map_err(|_| ProtocolError::InvalidData(format!("{} must be ASCII decimal", field)))?;
    s.parse::<u64>()
        .map_err(|_| ProtocolError::InvalidData(format!("{} is not a valid u64", field)))
}

fn parse_usize(bytes: &[u8], field: &str) -> Result<usize, ProtocolError> {
    let s = std::str::from_utf8(bytes)
        .map_err(|_| ProtocolError::InvalidData(format!("{} must be ASCII decimal", field)))?;
    s.parse::<usize>()
        .map_err(|_| ProtocolError::InvalidData(format!("{} is not a valid usize", field)))
}

fn split_ascii_words(line: &[u8]) -> Vec<&[u8]> {
    line.split(|b| *b == b' ')
        .filter(|part| !part.is_empty())
        .collect()
}

fn eq_ascii(lhs: &[u8], rhs: &[u8]) -> bool {
    lhs.eq_ignore_ascii_case(rhs)
}

fn find_crlf(data: &[u8]) -> Option<usize> {
    data.windows(2).position(|window| window == b"\r\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &[u8]) -> Result<Option<MemcacheCommand>, ProtocolError> {
        let mut parser = MemcacheParser::new();
        parser.feed(input);
        parser.try_parse()
    }

    fn serialize(resp: &MemcacheResponse) -> Vec<u8> {
        let mut buf = BytesMut::new();
        serialize_memcache_response(resp, &mut buf);
        buf.to_vec()
    }

    #[test]
    fn test_parse_get() {
        let cmd = parse(b"get alpha beta\r\n").unwrap().unwrap();
        assert_eq!(
            cmd,
            MemcacheCommand::Get {
                keys: vec![b"alpha".to_vec(), b"beta".to_vec()],
            }
        );
    }

    #[test]
    fn test_parse_store() {
        let cmd = parse(b"set alpha 7 60 5\r\nvalue\r\n").unwrap().unwrap();
        assert_eq!(
            cmd,
            MemcacheCommand::Store {
                mode: MemcacheStoreMode::Set,
                key: b"alpha".to_vec(),
                flags: 7,
                ttl_seconds: Some(60),
                value: b"value".to_vec(),
                noreply: false,
            }
        );
    }

    #[test]
    fn test_parse_store_waits_for_body() {
        assert!(matches!(parse(b"set alpha 7 60 5\r\nval"), Ok(None)));
    }

    #[test]
    fn test_parse_flush_all_noreply() {
        let cmd = parse(b"flush_all 10 noreply\r\n").unwrap().unwrap();
        assert_eq!(
            cmd,
            MemcacheCommand::FlushAll {
                delay_seconds: Some(10),
                noreply: true,
            }
        );
    }

    #[test]
    fn test_parse_version() {
        assert_eq!(
            parse(b"version\r\n").unwrap(),
            Some(MemcacheCommand::Version)
        );
    }

    #[test]
    fn test_serialize_values() {
        let resp = MemcacheResponse::Values(vec![MemcacheValue {
            key: b"alpha".to_vec(),
            flags: 3,
            data: b"value".to_vec(),
        }]);
        assert_eq!(
            serialize(&resp),
            b"VALUE alpha 3 5\r\nvalue\r\nEND\r\n".to_vec()
        );
    }

    #[test]
    fn test_serialize_stats() {
        let resp = MemcacheResponse::Stats(vec![("cmd_get".into(), "10".into())]);
        assert_eq!(serialize(&resp), b"STAT cmd_get 10\r\nEND\r\n".to_vec());
    }

    #[test]
    fn test_serialize_client_error() {
        let resp = MemcacheResponse::ClientError("bad command line format".into());
        assert_eq!(
            serialize(&resp),
            b"CLIENT_ERROR bad command line format\r\n".to_vec()
        );
    }
}
