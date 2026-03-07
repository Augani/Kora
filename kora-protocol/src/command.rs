//! Translate parsed RESP arrays into typed kora_core::Command values.

use kora_core::command::Command;

use crate::error::ProtocolError;
use crate::resp::RespValue;

/// Parse a RESP value (expected to be an array of bulk strings) into a Command.
pub fn parse_command(frame: RespValue) -> Result<Command, ProtocolError> {
    let parts = match frame {
        RespValue::Array(Some(parts)) => parts,
        _ => return Err(ProtocolError::InvalidData("expected array".into())),
    };

    if parts.is_empty() {
        return Err(ProtocolError::InvalidData("empty command".into()));
    }

    let cmd_name = extract_string(&parts[0])?.to_ascii_uppercase();
    let args = &parts[1..];

    match cmd_name.as_slice() {
        // String commands
        b"GET" => {
            check_arity("GET", args, 1)?;
            Ok(Command::Get {
                key: extract_bytes(&args[0])?,
            })
        }
        b"SET" => parse_set(args),
        b"GETSET" => {
            check_arity("GETSET", args, 2)?;
            Ok(Command::GetSet {
                key: extract_bytes(&args[0])?,
                value: extract_bytes(&args[1])?,
            })
        }
        b"APPEND" => {
            check_arity("APPEND", args, 2)?;
            Ok(Command::Append {
                key: extract_bytes(&args[0])?,
                value: extract_bytes(&args[1])?,
            })
        }
        b"STRLEN" => {
            check_arity("STRLEN", args, 1)?;
            Ok(Command::Strlen {
                key: extract_bytes(&args[0])?,
            })
        }
        b"INCR" => {
            check_arity("INCR", args, 1)?;
            Ok(Command::Incr {
                key: extract_bytes(&args[0])?,
            })
        }
        b"DECR" => {
            check_arity("DECR", args, 1)?;
            Ok(Command::Decr {
                key: extract_bytes(&args[0])?,
            })
        }
        b"INCRBY" => {
            check_arity("INCRBY", args, 2)?;
            Ok(Command::IncrBy {
                key: extract_bytes(&args[0])?,
                delta: parse_i64(&args[1])?,
            })
        }
        b"DECRBY" => {
            check_arity("DECRBY", args, 2)?;
            Ok(Command::DecrBy {
                key: extract_bytes(&args[0])?,
                delta: parse_i64(&args[1])?,
            })
        }
        b"SETNX" => {
            check_arity("SETNX", args, 2)?;
            Ok(Command::SetNx {
                key: extract_bytes(&args[0])?,
                value: extract_bytes(&args[1])?,
            })
        }
        b"MGET" => {
            check_min_arity("MGET", args, 1)?;
            let keys = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
            Ok(Command::MGet { keys })
        }
        b"MSET" => {
            if args.len() < 2 || args.len() % 2 != 0 {
                return Err(ProtocolError::WrongArity("MSET".into()));
            }
            let entries = args
                .chunks(2)
                .map(|chunk| Ok((extract_bytes(&chunk[0])?, extract_bytes(&chunk[1])?)))
                .collect::<Result<_, ProtocolError>>()?;
            Ok(Command::MSet { entries })
        }

        // Key commands
        b"DEL" => {
            check_min_arity("DEL", args, 1)?;
            let keys = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
            Ok(Command::Del { keys })
        }
        b"EXISTS" => {
            check_min_arity("EXISTS", args, 1)?;
            let keys = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
            Ok(Command::Exists { keys })
        }
        b"EXPIRE" => {
            check_arity("EXPIRE", args, 2)?;
            Ok(Command::Expire {
                key: extract_bytes(&args[0])?,
                seconds: parse_u64(&args[1])?,
            })
        }
        b"PEXPIRE" => {
            check_arity("PEXPIRE", args, 2)?;
            Ok(Command::PExpire {
                key: extract_bytes(&args[0])?,
                millis: parse_u64(&args[1])?,
            })
        }
        b"PERSIST" => {
            check_arity("PERSIST", args, 1)?;
            Ok(Command::Persist {
                key: extract_bytes(&args[0])?,
            })
        }
        b"TTL" => {
            check_arity("TTL", args, 1)?;
            Ok(Command::Ttl {
                key: extract_bytes(&args[0])?,
            })
        }
        b"PTTL" => {
            check_arity("PTTL", args, 1)?;
            Ok(Command::PTtl {
                key: extract_bytes(&args[0])?,
            })
        }
        b"TYPE" => {
            check_arity("TYPE", args, 1)?;
            Ok(Command::Type {
                key: extract_bytes(&args[0])?,
            })
        }
        b"KEYS" => {
            check_arity("KEYS", args, 1)?;
            Ok(Command::Keys {
                pattern: String::from_utf8_lossy(&extract_bytes(&args[0])?).into_owned(),
            })
        }
        b"SCAN" => parse_scan(args),
        b"DBSIZE" => {
            check_arity("DBSIZE", args, 0)?;
            Ok(Command::DbSize)
        }
        b"FLUSHDB" | b"FLUSHALL" => Ok(Command::FlushDb),

        // List commands
        b"LPUSH" => {
            check_min_arity("LPUSH", args, 2)?;
            Ok(Command::LPush {
                key: extract_bytes(&args[0])?,
                values: args[1..]
                    .iter()
                    .map(extract_bytes)
                    .collect::<Result<_, _>>()?,
            })
        }
        b"RPUSH" => {
            check_min_arity("RPUSH", args, 2)?;
            Ok(Command::RPush {
                key: extract_bytes(&args[0])?,
                values: args[1..]
                    .iter()
                    .map(extract_bytes)
                    .collect::<Result<_, _>>()?,
            })
        }
        b"LPOP" => {
            check_arity("LPOP", args, 1)?;
            Ok(Command::LPop {
                key: extract_bytes(&args[0])?,
            })
        }
        b"RPOP" => {
            check_arity("RPOP", args, 1)?;
            Ok(Command::RPop {
                key: extract_bytes(&args[0])?,
            })
        }
        b"LLEN" => {
            check_arity("LLEN", args, 1)?;
            Ok(Command::LLen {
                key: extract_bytes(&args[0])?,
            })
        }
        b"LRANGE" => {
            check_arity("LRANGE", args, 3)?;
            Ok(Command::LRange {
                key: extract_bytes(&args[0])?,
                start: parse_i64(&args[1])?,
                stop: parse_i64(&args[2])?,
            })
        }
        b"LINDEX" => {
            check_arity("LINDEX", args, 2)?;
            Ok(Command::LIndex {
                key: extract_bytes(&args[0])?,
                index: parse_i64(&args[1])?,
            })
        }

        // Hash commands
        b"HSET" => {
            if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                return Err(ProtocolError::WrongArity("HSET".into()));
            }
            let key = extract_bytes(&args[0])?;
            let fields = args[1..]
                .chunks(2)
                .map(|chunk| Ok((extract_bytes(&chunk[0])?, extract_bytes(&chunk[1])?)))
                .collect::<Result<_, ProtocolError>>()?;
            Ok(Command::HSet { key, fields })
        }
        b"HGET" => {
            check_arity("HGET", args, 2)?;
            Ok(Command::HGet {
                key: extract_bytes(&args[0])?,
                field: extract_bytes(&args[1])?,
            })
        }
        b"HDEL" => {
            check_min_arity("HDEL", args, 2)?;
            Ok(Command::HDel {
                key: extract_bytes(&args[0])?,
                fields: args[1..]
                    .iter()
                    .map(extract_bytes)
                    .collect::<Result<_, _>>()?,
            })
        }
        b"HGETALL" => {
            check_arity("HGETALL", args, 1)?;
            Ok(Command::HGetAll {
                key: extract_bytes(&args[0])?,
            })
        }
        b"HLEN" => {
            check_arity("HLEN", args, 1)?;
            Ok(Command::HLen {
                key: extract_bytes(&args[0])?,
            })
        }
        b"HEXISTS" => {
            check_arity("HEXISTS", args, 2)?;
            Ok(Command::HExists {
                key: extract_bytes(&args[0])?,
                field: extract_bytes(&args[1])?,
            })
        }
        b"HINCRBY" => {
            check_arity("HINCRBY", args, 3)?;
            Ok(Command::HIncrBy {
                key: extract_bytes(&args[0])?,
                field: extract_bytes(&args[1])?,
                delta: parse_i64(&args[2])?,
            })
        }

        // Set commands
        b"SADD" => {
            check_min_arity("SADD", args, 2)?;
            Ok(Command::SAdd {
                key: extract_bytes(&args[0])?,
                members: args[1..]
                    .iter()
                    .map(extract_bytes)
                    .collect::<Result<_, _>>()?,
            })
        }
        b"SREM" => {
            check_min_arity("SREM", args, 2)?;
            Ok(Command::SRem {
                key: extract_bytes(&args[0])?,
                members: args[1..]
                    .iter()
                    .map(extract_bytes)
                    .collect::<Result<_, _>>()?,
            })
        }
        b"SMEMBERS" => {
            check_arity("SMEMBERS", args, 1)?;
            Ok(Command::SMembers {
                key: extract_bytes(&args[0])?,
            })
        }
        b"SISMEMBER" => {
            check_arity("SISMEMBER", args, 2)?;
            Ok(Command::SIsMember {
                key: extract_bytes(&args[0])?,
                member: extract_bytes(&args[1])?,
            })
        }
        b"SCARD" => {
            check_arity("SCARD", args, 1)?;
            Ok(Command::SCard {
                key: extract_bytes(&args[0])?,
            })
        }

        // Server commands
        b"PING" => Ok(Command::Ping {
            message: args.first().and_then(|a| extract_bytes(a).ok()),
        }),
        b"ECHO" => {
            check_arity("ECHO", args, 1)?;
            Ok(Command::Echo {
                message: extract_bytes(&args[0])?,
            })
        }
        b"INFO" => Ok(Command::Info {
            section: args
                .first()
                .and_then(|a| extract_bytes(a).ok())
                .map(|b| String::from_utf8_lossy(&b).into_owned()),
        }),
        b"COMMAND" => {
            // Minimal COMMAND support — just return OK
            Ok(Command::Ping { message: None })
        }

        _ => Err(ProtocolError::UnknownCommand(
            String::from_utf8_lossy(&cmd_name).into_owned(),
        )),
    }
}

fn parse_set(args: &[RespValue]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::WrongArity("SET".into()));
    }
    let key = extract_bytes(&args[0])?;
    let value = extract_bytes(&args[1])?;
    let mut ex = None;
    let mut px = None;
    let mut nx = false;
    let mut xx = false;

    let mut i = 2;
    while i < args.len() {
        let flag = extract_string(&args[i])?.to_ascii_uppercase();
        match flag.as_slice() {
            b"EX" => {
                i += 1;
                ex = Some(parse_u64(&args[i])?);
            }
            b"PX" => {
                i += 1;
                px = Some(parse_u64(&args[i])?);
            }
            b"NX" => nx = true,
            b"XX" => xx = true,
            _ => {
                return Err(ProtocolError::InvalidData(format!(
                    "unsupported SET flag: {}",
                    String::from_utf8_lossy(&flag)
                )));
            }
        }
        i += 1;
    }

    Ok(Command::Set {
        key,
        value,
        ex,
        px,
        nx,
        xx,
    })
}

fn parse_scan(args: &[RespValue]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(ProtocolError::WrongArity("SCAN".into()));
    }
    let cursor = parse_u64(&args[0])?;
    let mut pattern = None;
    let mut count = None;
    let mut i = 1;
    while i < args.len() {
        let flag = extract_string(&args[i])?.to_ascii_uppercase();
        match flag.as_slice() {
            b"MATCH" => {
                i += 1;
                pattern = Some(String::from_utf8_lossy(&extract_bytes(&args[i])?).into_owned());
            }
            b"COUNT" => {
                i += 1;
                count = Some(parse_u64(&args[i])? as usize);
            }
            _ => {}
        }
        i += 1;
    }
    Ok(Command::Scan {
        cursor,
        pattern,
        count,
    })
}

fn extract_bytes(val: &RespValue) -> Result<Vec<u8>, ProtocolError> {
    match val {
        RespValue::BulkString(Some(data)) => Ok(data.clone()),
        RespValue::SimpleString(data) => Ok(data.clone()),
        RespValue::Integer(n) => Ok(n.to_string().into_bytes()),
        _ => Err(ProtocolError::InvalidData("expected bulk string".into())),
    }
}

fn extract_string(val: &RespValue) -> Result<Vec<u8>, ProtocolError> {
    extract_bytes(val)
}

fn parse_i64(val: &RespValue) -> Result<i64, ProtocolError> {
    match val {
        RespValue::Integer(n) => Ok(*n),
        RespValue::BulkString(Some(data)) | RespValue::SimpleString(data) => {
            std::str::from_utf8(data)
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| ProtocolError::InvalidData("expected integer".into()))
        }
        _ => Err(ProtocolError::InvalidData("expected integer".into())),
    }
}

fn parse_u64(val: &RespValue) -> Result<u64, ProtocolError> {
    let n = parse_i64(val)?;
    if n < 0 {
        Err(ProtocolError::InvalidData(
            "expected non-negative integer".into(),
        ))
    } else {
        Ok(n as u64)
    }
}

fn check_arity(cmd: &str, args: &[RespValue], expected: usize) -> Result<(), ProtocolError> {
    if args.len() != expected {
        Err(ProtocolError::WrongArity(cmd.into()))
    } else {
        Ok(())
    }
}

fn check_min_arity(cmd: &str, args: &[RespValue], min: usize) -> Result<(), ProtocolError> {
    if args.len() < min {
        Err(ProtocolError::WrongArity(cmd.into()))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cmd(parts: &[&[u8]]) -> RespValue {
        RespValue::Array(Some(
            parts
                .iter()
                .map(|p| RespValue::BulkString(Some(p.to_vec())))
                .collect(),
        ))
    }

    #[test]
    fn test_parse_get() {
        let cmd = parse_command(make_cmd(&[b"GET", b"mykey"])).unwrap();
        assert!(matches!(cmd, Command::Get { key } if key == b"mykey"));
    }

    #[test]
    fn test_parse_set_simple() {
        let cmd = parse_command(make_cmd(&[b"SET", b"k", b"v"])).unwrap();
        assert!(matches!(cmd, Command::Set { key, value, .. } if key == b"k" && value == b"v"));
    }

    #[test]
    fn test_parse_set_with_ex() {
        let cmd = parse_command(make_cmd(&[b"SET", b"k", b"v", b"EX", b"60"])).unwrap();
        match cmd {
            Command::Set { ex: Some(60), .. } => {}
            other => panic!("Expected SET with EX 60, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_set_with_nx() {
        let cmd = parse_command(make_cmd(&[b"SET", b"k", b"v", b"NX"])).unwrap();
        match cmd {
            Command::Set { nx: true, .. } => {}
            other => panic!("Expected SET with NX, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_wrong_arity() {
        let result = parse_command(make_cmd(&[b"GET"]));
        assert!(matches!(result, Err(ProtocolError::WrongArity(_))));
    }

    #[test]
    fn test_parse_unknown_command() {
        let result = parse_command(make_cmd(&[b"FOOBAR"]));
        assert!(matches!(result, Err(ProtocolError::UnknownCommand(_))));
    }

    #[test]
    fn test_parse_del_multiple() {
        let cmd = parse_command(make_cmd(&[b"DEL", b"a", b"b", b"c"])).unwrap();
        match cmd {
            Command::Del { keys } => assert_eq!(keys.len(), 3),
            other => panic!("Expected DEL, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_lpush() {
        let cmd = parse_command(make_cmd(&[b"LPUSH", b"list", b"a", b"b"])).unwrap();
        match cmd {
            Command::LPush { key, values } => {
                assert_eq!(key, b"list");
                assert_eq!(values.len(), 2);
            }
            other => panic!("Expected LPUSH, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_hset() {
        let cmd = parse_command(make_cmd(&[b"HSET", b"h", b"f1", b"v1", b"f2", b"v2"])).unwrap();
        match cmd {
            Command::HSet { key, fields } => {
                assert_eq!(key, b"h");
                assert_eq!(fields.len(), 2);
            }
            other => panic!("Expected HSET, got {:?}", other),
        }
    }

    #[test]
    fn test_case_insensitive() {
        let cmd = parse_command(make_cmd(&[b"get", b"mykey"])).unwrap();
        assert!(matches!(cmd, Command::Get { .. }));
    }
}
