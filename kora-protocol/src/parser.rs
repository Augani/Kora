//! Streaming incremental RESP2/RESP3 parser.
//!
//! [`RespParser`] accepts arbitrary byte chunks via [`feed`](RespParser::feed)
//! and yields complete [`RespValue`] frames via [`try_parse`](RespParser::try_parse).
//! Incomplete input is buffered transparently -- callers simply keep feeding
//! bytes until a frame is ready.
//!
//! For the highest-throughput command paths, the parser also exposes zero-copy
//! fast-path extractors ([`try_parse_hot_command_with`](RespParser::try_parse_hot_command_with),
//! [`try_parse_publish_with`](RespParser::try_parse_publish_with)) that hand
//! borrowed slices directly from the internal buffer to a caller-supplied
//! callback, avoiding per-command heap allocation entirely.

use bytes::BytesMut;
use memchr::memchr;

use crate::error::ProtocolError;
use crate::resp::RespValue;

/// A streaming, incremental RESP parser.
///
/// Maintains an internal byte buffer. Callers [`feed`](Self::feed) raw bytes
/// from the network and then call [`try_parse`](Self::try_parse) (or one of the
/// fast-path methods) to extract complete frames.
pub struct RespParser {
    buffer: BytesMut,
}

/// Fast parsed hot-path commands extracted directly from wire format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HotCommand {
    /// GET key
    Get { key: Vec<u8> },
    /// INCR key
    Incr { key: Vec<u8> },
    /// SET key value
    Set { key: Vec<u8>, value: Vec<u8> },
    /// PUBLISH channel message
    Publish { channel: Vec<u8>, message: Vec<u8> },
}

/// Borrowed view of a hot-path command extracted directly from the parser buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HotCommandRef<'a> {
    /// GET key
    Get { key: &'a [u8] },
    /// INCR key
    Incr { key: &'a [u8] },
    /// SET key value
    Set { key: &'a [u8], value: &'a [u8] },
    /// PUBLISH channel message
    Publish {
        channel: &'a [u8],
        message: &'a [u8],
    },
}

impl RespParser {
    /// Create a new parser with a default 4 KiB initial buffer.
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(4096),
        }
    }

    /// Append raw bytes received from the network.
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

    /// Fast-path parse for RESP `PUBLISH channel message`.
    ///
    /// Returns `Some((channel, message))` only when the first buffered frame is a
    /// complete `PUBLISH` command in RESP array/bulk form, otherwise returns `None`.
    pub fn try_parse_publish(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.try_parse_publish_with(|channel, message| (channel.to_vec(), message.to_vec()))
    }

    /// Fast-path parse for RESP `PUBLISH channel message`, executing a callback
    /// with borrowed payload slices to avoid per-command allocations.
    ///
    /// Returns `Some(result)` only when the first buffered frame is a complete
    /// `PUBLISH` command in RESP array/bulk form, otherwise returns `None`.
    pub fn try_parse_publish_with<R, F>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce(&[u8], &[u8]) -> R,
    {
        let spans = parse_publish_command_fast_spans(&self.buffer)?;
        let result = {
            let data = self.buffer.as_ref();
            f(
                &data[spans.channel_start..spans.channel_end],
                &data[spans.message_start..spans.message_end],
            )
        };
        let _ = self.buffer.split_to(spans.consumed);
        Some(result)
    }

    /// Fast-path parse for high-frequency command subset.
    ///
    /// This recognizes `GET key`, `INCR key`, `SET key value`, and
    /// `PUBLISH channel message` in both RESP and inline forms.
    pub fn try_parse_hot_command(&mut self) -> Option<HotCommand> {
        self.try_parse_hot_command_with(|cmd| match cmd {
            HotCommandRef::Get { key } => HotCommand::Get { key: key.to_vec() },
            HotCommandRef::Incr { key } => HotCommand::Incr { key: key.to_vec() },
            HotCommandRef::Set { key, value } => HotCommand::Set {
                key: key.to_vec(),
                value: value.to_vec(),
            },
            HotCommandRef::Publish { channel, message } => HotCommand::Publish {
                channel: channel.to_vec(),
                message: message.to_vec(),
            },
        })
    }

    /// Fast-path parse for high-frequency commands, borrowing payloads from the parser buffer.
    ///
    /// The callback must not retain the borrowed slices after it returns.
    pub fn try_parse_hot_command_with<R, F>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce(HotCommandRef<'_>) -> R,
    {
        let spans = parse_hot_command_fast_spans(&self.buffer)?;
        let result = {
            let data = self.buffer.as_ref();
            match spans {
                HotCommandSpans::Get {
                    key_start, key_end, ..
                } => f(HotCommandRef::Get {
                    key: &data[key_start..key_end],
                }),
                HotCommandSpans::Incr {
                    key_start, key_end, ..
                } => f(HotCommandRef::Incr {
                    key: &data[key_start..key_end],
                }),
                HotCommandSpans::Set {
                    key_start,
                    key_end,
                    value_start,
                    value_end,
                    ..
                } => f(HotCommandRef::Set {
                    key: &data[key_start..key_end],
                    value: &data[value_start..value_end],
                }),
                HotCommandSpans::Publish {
                    channel_start,
                    channel_end,
                    message_start,
                    message_end,
                    ..
                } => f(HotCommandRef::Publish {
                    channel: &data[channel_start..channel_end],
                    message: &data[message_start..message_end],
                }),
            }
        };
        let _ = self.buffer.split_to(spans.consumed());
        Some(result)
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

fn parse_ascii_usize(data: &[u8]) -> Option<usize> {
    if data.is_empty() {
        return None;
    }
    let mut n = 0usize;
    for &b in data {
        if !b.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add((b - b'0') as usize)?;
    }
    Some(n)
}

fn parse_bulk_span(data: &[u8], idx: usize) -> Option<(usize, usize, usize)> {
    if data.get(idx) != Some(&b'$') {
        return None;
    }
    let len_start = idx + 1;
    let rel_crlf = find_crlf(&data[len_start..])?;
    let len_end = len_start + rel_crlf;
    let len = parse_ascii_usize(&data[len_start..len_end])?;
    let start = len_end + 2;
    let end = start.checked_add(len)?;
    if data.get(end) != Some(&b'\r') || data.get(end + 1) != Some(&b'\n') {
        return None;
    }
    Some((end + 2, start, end))
}

struct PublishCommandSpans {
    consumed: usize,
    channel_start: usize,
    channel_end: usize,
    message_start: usize,
    message_end: usize,
}

enum HotCommandSpans {
    Get {
        consumed: usize,
        key_start: usize,
        key_end: usize,
    },
    Incr {
        consumed: usize,
        key_start: usize,
        key_end: usize,
    },
    Set {
        consumed: usize,
        key_start: usize,
        key_end: usize,
        value_start: usize,
        value_end: usize,
    },
    Publish {
        consumed: usize,
        channel_start: usize,
        channel_end: usize,
        message_start: usize,
        message_end: usize,
    },
}

impl HotCommandSpans {
    fn consumed(&self) -> usize {
        match self {
            HotCommandSpans::Get { consumed, .. }
            | HotCommandSpans::Incr { consumed, .. }
            | HotCommandSpans::Set { consumed, .. }
            | HotCommandSpans::Publish { consumed, .. } => *consumed,
        }
    }
}

fn parse_publish_command_fast_spans(data: &[u8]) -> Option<PublishCommandSpans> {
    if data.first() == Some(&b'*') {
        return parse_publish_resp_command_fast_spans(data);
    }
    parse_publish_inline_command_fast_spans(data)
}

fn parse_publish_resp_command_fast_spans(data: &[u8]) -> Option<PublishCommandSpans> {
    if data.first() != Some(&b'*') {
        return None;
    }

    let rel_crlf = find_crlf(&data[1..])?;
    let count_end = 1 + rel_crlf;
    let count = parse_ascii_usize(&data[1..count_end])?;
    if count != 3 {
        return None;
    }

    let mut idx = count_end + 2;
    let (next, cmd_start, cmd_end) = parse_bulk_span(data, idx)?;
    if !data[cmd_start..cmd_end].eq_ignore_ascii_case(b"PUBLISH") {
        return None;
    }
    idx = next;

    let (next, channel_start, channel_end) = parse_bulk_span(data, idx)?;
    idx = next;
    let (consumed, message_start, message_end) = parse_bulk_span(data, idx)?;

    Some(PublishCommandSpans {
        consumed,
        channel_start,
        channel_end,
        message_start,
        message_end,
    })
}

fn parse_publish_inline_command_fast_spans(data: &[u8]) -> Option<PublishCommandSpans> {
    let line_end = find_crlf(data)?;
    let line = &data[..line_end];
    let mut idx = 0usize;

    while idx < line.len() && line[idx] == b' ' {
        idx += 1;
    }
    if idx >= line.len() {
        return None;
    }

    let cmd_start = idx;
    while idx < line.len() && line[idx] != b' ' {
        idx += 1;
    }
    let cmd_end = idx;
    if !line[cmd_start..cmd_end].eq_ignore_ascii_case(b"PUBLISH") {
        return None;
    }

    while idx < line.len() && line[idx] == b' ' {
        idx += 1;
    }
    if idx >= line.len() {
        return None;
    }
    let channel_start = idx;
    while idx < line.len() && line[idx] != b' ' {
        idx += 1;
    }
    let channel_end = idx;

    while idx < line.len() && line[idx] == b' ' {
        idx += 1;
    }
    if idx >= line.len() {
        return None;
    }
    let message_start = idx;
    while idx < line.len() && line[idx] != b' ' {
        idx += 1;
    }
    let message_end = idx;

    while idx < line.len() && line[idx] == b' ' {
        idx += 1;
    }
    if idx != line.len() {
        return None;
    }

    Some(PublishCommandSpans {
        consumed: line_end + 2,
        channel_start,
        channel_end,
        message_start,
        message_end,
    })
}

fn parse_hot_command_fast_spans(data: &[u8]) -> Option<HotCommandSpans> {
    if data.first() == Some(&b'*') {
        parse_hot_resp_command_fast_spans(data)
    } else {
        parse_hot_inline_command_fast_spans(data)
    }
}

fn parse_hot_resp_command_fast_spans(data: &[u8]) -> Option<HotCommandSpans> {
    if data.first() != Some(&b'*') {
        return None;
    }

    let rel_crlf = find_crlf(&data[1..])?;
    let count_end = 1 + rel_crlf;
    let count = parse_ascii_usize(&data[1..count_end])?;
    let mut idx = count_end + 2;

    let (next, cmd_start, cmd_end) = parse_bulk_span(data, idx)?;
    let cmd = &data[cmd_start..cmd_end];
    idx = next;

    if cmd.eq_ignore_ascii_case(b"GET") {
        if count != 2 {
            return None;
        }
        let (consumed, key_start, key_end) = parse_bulk_span(data, idx)?;
        return Some(HotCommandSpans::Get {
            consumed,
            key_start,
            key_end,
        });
    }

    if cmd.eq_ignore_ascii_case(b"INCR") {
        if count != 2 {
            return None;
        }
        let (consumed, key_start, key_end) = parse_bulk_span(data, idx)?;
        return Some(HotCommandSpans::Incr {
            consumed,
            key_start,
            key_end,
        });
    }

    if cmd.eq_ignore_ascii_case(b"SET") {
        if count != 3 {
            return None;
        }
        let (next, key_start, key_end) = parse_bulk_span(data, idx)?;
        let (consumed, value_start, value_end) = parse_bulk_span(data, next)?;
        return Some(HotCommandSpans::Set {
            consumed,
            key_start,
            key_end,
            value_start,
            value_end,
        });
    }

    if cmd.eq_ignore_ascii_case(b"PUBLISH") {
        if count != 3 {
            return None;
        }
        let (next, channel_start, channel_end) = parse_bulk_span(data, idx)?;
        let (consumed, message_start, message_end) = parse_bulk_span(data, next)?;
        return Some(HotCommandSpans::Publish {
            consumed,
            channel_start,
            channel_end,
            message_start,
            message_end,
        });
    }

    None
}

fn parse_hot_inline_command_fast_spans(data: &[u8]) -> Option<HotCommandSpans> {
    let line_end = find_crlf(data)?;
    let line = &data[..line_end];
    let mut token_spans = [(0usize, 0usize); 3];
    let mut token_count = 0usize;
    let mut idx = 0usize;

    while idx < line.len() {
        while idx < line.len() && line[idx] == b' ' {
            idx += 1;
        }
        if idx >= line.len() {
            break;
        }
        if token_count == token_spans.len() {
            return None;
        }
        let start = idx;
        while idx < line.len() && line[idx] != b' ' {
            idx += 1;
        }
        token_spans[token_count] = (start, idx);
        token_count += 1;
    }

    if token_count < 2 {
        return None;
    }

    let cmd = &line[token_spans[0].0..token_spans[0].1];
    let consumed = line_end + 2;

    if cmd.eq_ignore_ascii_case(b"GET") {
        if token_count != 2 {
            return None;
        }
        let (s, e) = token_spans[1];
        return Some(HotCommandSpans::Get {
            consumed,
            key_start: s,
            key_end: e,
        });
    }

    if cmd.eq_ignore_ascii_case(b"INCR") {
        if token_count != 2 {
            return None;
        }
        let (s, e) = token_spans[1];
        return Some(HotCommandSpans::Incr {
            consumed,
            key_start: s,
            key_end: e,
        });
    }

    if cmd.eq_ignore_ascii_case(b"SET") {
        if token_count != 3 {
            return None;
        }
        let (ks, ke) = token_spans[1];
        let (vs, ve) = token_spans[2];
        return Some(HotCommandSpans::Set {
            consumed,
            key_start: ks,
            key_end: ke,
            value_start: vs,
            value_end: ve,
        });
    }

    if cmd.eq_ignore_ascii_case(b"PUBLISH") {
        if token_count != 3 {
            return None;
        }
        let (cs, ce) = token_spans[1];
        let (ms, me) = token_spans[2];
        return Some(HotCommandSpans::Publish {
            consumed,
            channel_start: cs,
            channel_end: ce,
            message_start: ms,
            message_end: me,
        });
    }

    None
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
        b'_' => parse_null(data),
        b',' => parse_double(data),
        b'#' => parse_boolean(data),
        b'%' => parse_map(data),
        b'~' => parse_set(data),
        b'(' => parse_big_number(data),
        b'=' => parse_verbatim_string(data),
        b'>' => parse_push(data),
        _ => parse_inline(data),
    }
}

fn find_crlf(data: &[u8]) -> Option<usize> {
    let mut offset = 0usize;
    while let Some(pos) = memchr(b'\r', &data[offset..]) {
        let idx = offset + pos;
        if idx + 1 >= data.len() {
            return None;
        }
        if data[idx + 1] == b'\n' {
            return Some(idx);
        }
        offset = idx + 1;
    }
    None
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
    fn test_try_parse_publish_fast_path() {
        let mut parser = RespParser::new();
        parser.feed(b"*3\r\n$7\r\nPUBLISH\r\n$4\r\nchan\r\n$5\r\nhello\r\n");
        let (channel, message) = parser.try_parse_publish().expect("expected publish");
        assert_eq!(channel, b"chan".to_vec());
        assert_eq!(message, b"hello".to_vec());
        assert!(parser.try_parse().unwrap().is_none());
    }

    #[test]
    fn test_try_parse_publish_fast_path_incomplete() {
        let mut parser = RespParser::new();
        parser.feed(b"*3\r\n$7\r\nPUBLISH\r\n$4\r\nchan\r\n$5\r\nhel");
        assert!(parser.try_parse_publish().is_none());
        assert!(parser.try_parse().unwrap().is_none());
        parser.feed(b"lo\r\n");
        let (channel, message) = parser.try_parse_publish().expect("expected publish");
        assert_eq!(channel, b"chan".to_vec());
        assert_eq!(message, b"hello".to_vec());
    }

    #[test]
    fn test_try_parse_publish_fast_path_non_publish_fallback() {
        let mut parser = RespParser::new();
        parser.feed(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
        assert!(parser.try_parse_publish().is_none());
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
    fn test_try_parse_publish_inline_fast_path() {
        let mut parser = RespParser::new();
        parser.feed(b"PUBLISH chan hello\r\n");
        let (channel, message) = parser.try_parse_publish().expect("expected inline publish");
        assert_eq!(channel, b"chan".to_vec());
        assert_eq!(message, b"hello".to_vec());
        assert!(parser.try_parse().unwrap().is_none());
    }

    #[test]
    fn test_try_parse_hot_command_resp() {
        let mut parser = RespParser::new();
        parser.feed(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
        assert_eq!(
            parser.try_parse_hot_command(),
            Some(HotCommand::Get {
                key: b"key".to_vec()
            })
        );

        parser.feed(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
        assert_eq!(
            parser.try_parse_hot_command(),
            Some(HotCommand::Set {
                key: b"key".to_vec(),
                value: b"value".to_vec()
            })
        );
    }

    #[test]
    fn test_try_parse_hot_command_inline() {
        let mut parser = RespParser::new();
        parser.feed(b"INCR ctr\r\n");
        assert_eq!(
            parser.try_parse_hot_command(),
            Some(HotCommand::Incr {
                key: b"ctr".to_vec()
            })
        );

        parser.feed(b"SET a b\r\n");
        assert_eq!(
            parser.try_parse_hot_command(),
            Some(HotCommand::Set {
                key: b"a".to_vec(),
                value: b"b".to_vec()
            })
        );
    }

    #[test]
    fn test_try_parse_hot_command_with_borrowed_resp() {
        let mut parser = RespParser::new();
        parser.feed(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");

        let parsed = parser.try_parse_hot_command_with(|cmd| match cmd {
            HotCommandRef::Set { key, value } => (key.to_vec(), value.to_vec()),
            other => panic!("unexpected hot command: {:?}", other),
        });

        assert_eq!(parsed, Some((b"key".to_vec(), b"value".to_vec())));
        assert!(parser.try_parse().unwrap().is_none());
    }

    #[test]
    fn test_try_parse_hot_command_with_borrowed_inline() {
        let mut parser = RespParser::new();
        parser.feed(b"GET borrowed\r\n");

        let parsed = parser.try_parse_hot_command_with(|cmd| match cmd {
            HotCommandRef::Get { key } => key.to_vec(),
            other => panic!("unexpected hot command: {:?}", other),
        });

        assert_eq!(parsed, Some(b"borrowed".to_vec()));
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
        parser.feed(b",3.25\r\n");
        let val = parser.try_parse().unwrap().unwrap();
        assert_eq!(val, RespValue::Double(3.25));
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
