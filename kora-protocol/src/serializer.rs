//! RESP2 response serializer.

use bytes::BytesMut;
use kora_core::command::CommandResponse;
use std::fmt::Write;

/// Serialize a CommandResponse into RESP2 wire format.
pub fn serialize_response(resp: &CommandResponse, buf: &mut BytesMut) {
    match resp {
        CommandResponse::Ok => buf.extend_from_slice(b"+OK\r\n"),
        CommandResponse::Nil => buf.extend_from_slice(b"$-1\r\n"),
        CommandResponse::Integer(n) => {
            let _ = write!(buf, ":{}\r\n", n);
        }
        CommandResponse::BulkString(data) => {
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
                serialize_response(item, buf);
            }
        }
        CommandResponse::Error(msg) => {
            buf.extend_from_slice(b"-");
            buf.extend_from_slice(msg.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn serialize(resp: &CommandResponse) -> Vec<u8> {
        let mut buf = BytesMut::new();
        serialize_response(resp, &mut buf);
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
}
