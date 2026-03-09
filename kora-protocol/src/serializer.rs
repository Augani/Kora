//! RESP2/RESP3 response serializer.
//!
//! Converts [`CommandResponse`] values into RESP wire bytes, appending directly
//! into a caller-supplied [`BytesMut`] buffer to minimize allocation.
//!
//! Two entry points are provided:
//! - [`serialize_response`] always emits RESP2.
//! - [`serialize_response_versioned`] accepts a `resp3` flag; when set, it
//!   uses native RESP3 type prefixes for Map (`%`), Set (`~`), Double (`,`),
//!   and Boolean (`#`). When cleared, those types are automatically downgraded
//!   to RESP2-compatible representations (flat arrays, bulk strings, integers).

use bytes::BytesMut;
use kora_core::command::CommandResponse;
use std::fmt::Write;

/// Serialize a [`CommandResponse`] into RESP wire format.
///
/// When `resp3` is true, uses native RESP3 type prefixes for Map, Set, Double,
/// and Boolean. When `resp3` is false, these are downgraded to RESP2
/// equivalents (flat arrays, bulk strings, integers).
pub fn serialize_response_versioned(resp: &CommandResponse, buf: &mut BytesMut, resp3: bool) {
    match resp {
        CommandResponse::Ok => buf.extend_from_slice(b"+OK\r\n"),
        CommandResponse::Nil => {
            if resp3 {
                buf.extend_from_slice(b"_\r\n");
            } else {
                buf.extend_from_slice(b"$-1\r\n");
            }
        }
        CommandResponse::Integer(n) => {
            let _ = write!(buf, ":{}\r\n", n);
        }
        CommandResponse::BulkString(data) => {
            let _ = write!(buf, "${}\r\n", data.len());
            buf.extend_from_slice(data);
            buf.extend_from_slice(b"\r\n");
        }
        CommandResponse::BulkStringShared(data) => {
            let _ = write!(buf, "${}\r\n", data.len());
            buf.extend_from_slice(data);
            buf.extend_from_slice(b"\r\n");
        }
        CommandResponse::SimpleString(s) => {
            buf.extend_from_slice(b"+");
            buf.extend_from_slice(s.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        CommandResponse::Array(items) => {
            let _ = write!(buf, "*{}\r\n", items.len());
            for item in items {
                serialize_response_versioned(item, buf, resp3);
            }
        }
        CommandResponse::Error(msg) => {
            buf.extend_from_slice(b"-");
            buf.extend_from_slice(msg.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        CommandResponse::Map(pairs) => {
            if resp3 {
                let _ = write!(buf, "%{}\r\n", pairs.len());
                for (k, v) in pairs {
                    serialize_response_versioned(k, buf, resp3);
                    serialize_response_versioned(v, buf, resp3);
                }
            } else {
                let _ = write!(buf, "*{}\r\n", pairs.len() * 2);
                for (k, v) in pairs {
                    serialize_response_versioned(k, buf, false);
                    serialize_response_versioned(v, buf, false);
                }
            }
        }
        CommandResponse::Set(items) => {
            if resp3 {
                let _ = write!(buf, "~{}\r\n", items.len());
                for item in items {
                    serialize_response_versioned(item, buf, resp3);
                }
            } else {
                let _ = write!(buf, "*{}\r\n", items.len());
                for item in items {
                    serialize_response_versioned(item, buf, false);
                }
            }
        }
        CommandResponse::Double(val) => {
            if resp3 {
                if val.is_infinite() {
                    if val.is_sign_positive() {
                        buf.extend_from_slice(b",inf\r\n");
                    } else {
                        buf.extend_from_slice(b",-inf\r\n");
                    }
                } else {
                    let _ = write!(buf, ",{}\r\n", val);
                }
            } else {
                let s = val.to_string();
                let _ = write!(buf, "${}\r\n", s.len());
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
        }
        CommandResponse::Boolean(b) => {
            if resp3 {
                if *b {
                    buf.extend_from_slice(b"#t\r\n");
                } else {
                    buf.extend_from_slice(b"#f\r\n");
                }
            } else {
                let _ = write!(buf, ":{}\r\n", if *b { 1 } else { 0 });
            }
        }
    }
}

/// Serialize a [`CommandResponse`] into RESP2 wire format.
pub fn serialize_response(resp: &CommandResponse, buf: &mut BytesMut) {
    serialize_response_versioned(resp, buf, false);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn serialize(resp: &CommandResponse) -> Vec<u8> {
        let mut buf = BytesMut::new();
        serialize_response(resp, &mut buf);
        buf.to_vec()
    }

    fn serialize_v3(resp: &CommandResponse) -> Vec<u8> {
        let mut buf = BytesMut::new();
        serialize_response_versioned(resp, &mut buf, true);
        buf.to_vec()
    }

    #[test]
    fn test_serialize_ok() {
        assert_eq!(serialize(&CommandResponse::Ok), b"+OK\r\n");
    }

    #[test]
    fn test_serialize_nil() {
        assert_eq!(serialize(&CommandResponse::Nil), b"$-1\r\n");
    }

    #[test]
    fn test_serialize_nil_resp3() {
        assert_eq!(serialize_v3(&CommandResponse::Nil), b"_\r\n");
    }

    #[test]
    fn test_serialize_integer() {
        assert_eq!(serialize(&CommandResponse::Integer(42)), b":42\r\n");
    }

    #[test]
    fn test_serialize_bulk_string() {
        assert_eq!(
            serialize(&CommandResponse::BulkString(b"hello".to_vec())),
            b"$5\r\nhello\r\n"
        );
    }

    #[test]
    fn test_serialize_shared_bulk_string() {
        assert_eq!(
            serialize(&CommandResponse::BulkStringShared(std::sync::Arc::from(
                b"hello".as_slice()
            ))),
            b"$5\r\nhello\r\n"
        );
    }

    #[test]
    fn test_serialize_error() {
        assert_eq!(
            serialize(&CommandResponse::Error("ERR test".into())),
            b"-ERR test\r\n"
        );
    }

    #[test]
    fn test_serialize_array() {
        let resp = CommandResponse::Array(vec![
            CommandResponse::BulkString(b"a".to_vec()),
            CommandResponse::BulkString(b"b".to_vec()),
        ]);
        assert_eq!(serialize(&resp), b"*2\r\n$1\r\na\r\n$1\r\nb\r\n");
    }

    #[test]
    fn test_serialize_map_resp3() {
        let resp = CommandResponse::Map(vec![(
            CommandResponse::SimpleString("key".into()),
            CommandResponse::Integer(1),
        )]);
        assert_eq!(serialize_v3(&resp), b"%1\r\n+key\r\n:1\r\n");
    }

    #[test]
    fn test_serialize_map_resp2_downgrade() {
        let resp = CommandResponse::Map(vec![(
            CommandResponse::SimpleString("key".into()),
            CommandResponse::Integer(1),
        )]);
        assert_eq!(serialize(&resp), b"*2\r\n+key\r\n:1\r\n");
    }

    #[test]
    fn test_serialize_set_resp3() {
        let resp = CommandResponse::Set(vec![
            CommandResponse::Integer(1),
            CommandResponse::Integer(2),
        ]);
        assert_eq!(serialize_v3(&resp), b"~2\r\n:1\r\n:2\r\n");
    }

    #[test]
    fn test_serialize_double_resp3() {
        let resp = CommandResponse::Double(3.25);
        assert_eq!(serialize_v3(&resp), b",3.25\r\n");
    }

    #[test]
    fn test_serialize_boolean_resp3() {
        assert_eq!(serialize_v3(&CommandResponse::Boolean(true)), b"#t\r\n");
        assert_eq!(serialize_v3(&CommandResponse::Boolean(false)), b"#f\r\n");
    }

    #[test]
    fn test_serialize_boolean_resp2_downgrade() {
        assert_eq!(serialize(&CommandResponse::Boolean(true)), b":1\r\n");
        assert_eq!(serialize(&CommandResponse::Boolean(false)), b":0\r\n");
    }
}
