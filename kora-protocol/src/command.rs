//! Command translation from RESP frames to typed Kora commands.
//!
//! The single public entry point, [`parse_command`], accepts a [`RespValue`]
//! (expected to be an array of bulk strings representing a client request) and
//! returns a fully validated [`Command`] variant from `kora-core`.
//!
//! High-frequency commands (GET, SET, PING, PUBLISH, and the pub/sub family)
//! are matched first via fast-path byte comparisons that avoid normalizing the
//! command name or cloning arguments. All remaining commands fall through to a
//! case-insensitive match table. Arity checking is enforced for every command
//! before construction.

use kora_core::command::{
    BitFieldEncoding, BitFieldOffset, BitFieldOperation, BitFieldOverflow, BitOperation, Command,
    DocUpdateMutation, GeoUnit,
};

use crate::error::ProtocolError;
use crate::resp::RespValue;

mod blocking_parsing;
mod doc_parsing;

use blocking_parsing::{parse_blmove, parse_blpop, parse_brpop, parse_bzpopmax, parse_bzpopmin};
use doc_parsing::{
    parse_doc_count, parse_doc_create, parse_doc_createindex, parse_doc_find, parse_doc_get,
    parse_doc_mget, parse_doc_mset, parse_doc_update,
};

/// Parse a RESP value (expected to be an array of bulk strings) into a Command.
pub fn parse_command(frame: RespValue) -> Result<Command, ProtocolError> {
    let parts = match frame {
        RespValue::Array(Some(parts)) => parts,
        _ => return Err(ProtocolError::InvalidData("expected array".into())),
    };

    if parts.is_empty() {
        return Err(ProtocolError::InvalidData("empty command".into()));
    }

    // Fast path for high-frequency pub/sub commands:
    // avoid command-name normalization and argument cloning.
    if is_command_name(&parts[0], b"PUBLISH") {
        if parts.len() != 3 {
            return Err(ProtocolError::WrongArity("PUBLISH".into()));
        }
        let mut iter = parts.into_iter();
        let _ = iter.next();
        let channel = extract_bytes_owned(iter.next().expect("len checked"))?;
        let message = extract_bytes_owned(iter.next().expect("len checked"))?;
        return Ok(Command::Publish { channel, message });
    }

    if is_command_name(&parts[0], b"SUBSCRIBE") {
        if parts.len() < 2 {
            return Err(ProtocolError::WrongArity("SUBSCRIBE".into()));
        }
        let mut iter = parts.into_iter();
        let _ = iter.next();
        let mut channels = Vec::with_capacity(iter.len());
        for arg in iter {
            channels.push(extract_bytes_owned(arg)?);
        }
        return Ok(Command::Subscribe { channels });
    }

    if is_command_name(&parts[0], b"UNSUBSCRIBE") {
        let mut iter = parts.into_iter();
        let _ = iter.next();
        let mut channels = Vec::with_capacity(iter.len());
        for arg in iter {
            channels.push(extract_bytes_owned(arg)?);
        }
        return Ok(Command::Unsubscribe { channels });
    }

    if is_command_name(&parts[0], b"PSUBSCRIBE") {
        if parts.len() < 2 {
            return Err(ProtocolError::WrongArity("PSUBSCRIBE".into()));
        }
        let mut iter = parts.into_iter();
        let _ = iter.next();
        let mut patterns = Vec::with_capacity(iter.len());
        for arg in iter {
            patterns.push(extract_bytes_owned(arg)?);
        }
        return Ok(Command::PSubscribe { patterns });
    }

    if is_command_name(&parts[0], b"PUNSUBSCRIBE") {
        let mut iter = parts.into_iter();
        let _ = iter.next();
        let mut patterns = Vec::with_capacity(iter.len());
        for arg in iter {
            patterns.push(extract_bytes_owned(arg)?);
        }
        return Ok(Command::PUnsubscribe { patterns });
    }

    if is_command_name(&parts[0], b"GET") {
        if parts.len() != 2 {
            return Err(ProtocolError::WrongArity("GET".into()));
        }
        let mut iter = parts.into_iter();
        let _ = iter.next();
        let key = extract_bytes_owned(iter.next().expect("len checked"))?;
        return Ok(Command::Get { key });
    }

    if is_command_name(&parts[0], b"PING") {
        if parts.len() == 1 {
            return Ok(Command::Ping { message: None });
        }
        if parts.len() == 2 {
            let mut iter = parts.into_iter();
            let _ = iter.next();
            let message = extract_bytes_owned(iter.next().expect("len checked"))?;
            return Ok(Command::Ping {
                message: Some(message),
            });
        }
        return Err(ProtocolError::WrongArity("PING".into()));
    }

    if is_command_name(&parts[0], b"SET") {
        if parts.len() < 3 {
            return Err(ProtocolError::WrongArity("SET".into()));
        }
        if parts.len() == 3 {
            let mut iter = parts.into_iter();
            let _ = iter.next();
            let key = extract_bytes_owned(iter.next().expect("len checked"))?;
            let value = extract_bytes_owned(iter.next().expect("len checked"))?;
            return Ok(Command::Set {
                key,
                value,
                ex: None,
                px: None,
                nx: false,
                xx: false,
            });
        }
    }

    let cmd_name = extract_string(&parts[0])?.to_ascii_uppercase();
    let args = &parts[1..];

    match cmd_name.as_slice() {
        b"DOC.CREATE" => parse_doc_create(args),
        b"DOC.DROP" => {
            check_arity("DOC.DROP", args, 1)?;
            Ok(Command::DocDrop {
                collection: extract_bytes(&args[0])?,
            })
        }
        b"DOC.INFO" => {
            check_arity("DOC.INFO", args, 1)?;
            Ok(Command::DocInfo {
                collection: extract_bytes(&args[0])?,
            })
        }
        b"DOC.DICTINFO" => {
            check_arity("DOC.DICTINFO", args, 1)?;
            Ok(Command::DocDictInfo {
                collection: extract_bytes(&args[0])?,
            })
        }
        b"DOC.STORAGE" => {
            check_arity("DOC.STORAGE", args, 1)?;
            Ok(Command::DocStorage {
                collection: extract_bytes(&args[0])?,
            })
        }
        b"DOC.SET" => {
            check_arity("DOC.SET", args, 3)?;
            Ok(Command::DocSet {
                collection: extract_bytes(&args[0])?,
                doc_id: extract_bytes(&args[1])?,
                json: extract_bytes(&args[2])?,
            })
        }
        b"DOC.MSET" => parse_doc_mset(args),
        b"DOC.GET" => parse_doc_get(args),
        b"DOC.MGET" => parse_doc_mget(args),
        b"DOC.UPDATE" => parse_doc_update(args),
        b"DOC.DEL" => {
            check_arity("DOC.DEL", args, 2)?;
            Ok(Command::DocDel {
                collection: extract_bytes(&args[0])?,
                doc_id: extract_bytes(&args[1])?,
            })
        }
        b"DOC.EXISTS" => {
            check_arity("DOC.EXISTS", args, 2)?;
            Ok(Command::DocExists {
                collection: extract_bytes(&args[0])?,
                doc_id: extract_bytes(&args[1])?,
            })
        }
        b"DOC.CREATEINDEX" => parse_doc_createindex(args),
        b"DOC.DROPINDEX" => {
            check_arity("DOC.DROPINDEX", args, 2)?;
            Ok(Command::DocDropIndex {
                collection: extract_bytes(&args[0])?,
                field: extract_bytes(&args[1])?,
            })
        }
        b"DOC.INDEXES" => {
            check_arity("DOC.INDEXES", args, 1)?;
            Ok(Command::DocIndexes {
                collection: extract_bytes(&args[0])?,
            })
        }
        b"DOC.FIND" => parse_doc_find(args),
        b"DOC.COUNT" => parse_doc_count(args),

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
        b"SETEX" => {
            check_arity("SETEX", args, 3)?;
            Ok(Command::Set {
                key: extract_bytes(&args[0])?,
                value: extract_bytes(&args[2])?,
                ex: Some(parse_u64(&args[1])?),
                px: None,
                nx: false,
                xx: false,
            })
        }
        b"PSETEX" => {
            check_arity("PSETEX", args, 3)?;
            Ok(Command::Set {
                key: extract_bytes(&args[0])?,
                value: extract_bytes(&args[2])?,
                ex: None,
                px: Some(parse_u64(&args[1])?),
                nx: false,
                xx: false,
            })
        }
        b"INCRBYFLOAT" => {
            check_arity("INCRBYFLOAT", args, 2)?;
            Ok(Command::IncrByFloat {
                key: extract_bytes(&args[0])?,
                delta: parse_f64(&args[1])?,
            })
        }
        b"GETRANGE" | b"SUBSTR" => {
            check_arity("GETRANGE", args, 3)?;
            Ok(Command::GetRange {
                key: extract_bytes(&args[0])?,
                start: parse_i64(&args[1])?,
                end: parse_i64(&args[2])?,
            })
        }
        b"SETRANGE" => {
            check_arity("SETRANGE", args, 3)?;
            Ok(Command::SetRange {
                key: extract_bytes(&args[0])?,
                offset: parse_u64(&args[1])? as usize,
                value: extract_bytes(&args[2])?,
            })
        }
        b"GETDEL" => {
            check_arity("GETDEL", args, 1)?;
            Ok(Command::GetDel {
                key: extract_bytes(&args[0])?,
            })
        }
        b"GETEX" => parse_getex(args),
        b"MSETNX" => {
            if args.len() < 2 || args.len() % 2 != 0 {
                return Err(ProtocolError::WrongArity("MSETNX".into()));
            }
            let entries = args
                .chunks(2)
                .map(|chunk| Ok((extract_bytes(&chunk[0])?, extract_bytes(&chunk[1])?)))
                .collect::<Result<_, ProtocolError>>()?;
            Ok(Command::MSetNx { entries })
        }

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
        b"FLUSHDB" => Ok(Command::FlushDb),
        b"EXPIREAT" => {
            check_arity("EXPIREAT", args, 2)?;
            Ok(Command::ExpireAt {
                key: extract_bytes(&args[0])?,
                timestamp: parse_u64(&args[1])?,
            })
        }
        b"PEXPIREAT" => {
            check_arity("PEXPIREAT", args, 2)?;
            Ok(Command::PExpireAt {
                key: extract_bytes(&args[0])?,
                timestamp_ms: parse_u64(&args[1])?,
            })
        }
        b"RENAME" => {
            check_arity("RENAME", args, 2)?;
            Ok(Command::Rename {
                key: extract_bytes(&args[0])?,
                newkey: extract_bytes(&args[1])?,
            })
        }
        b"RENAMENX" => {
            check_arity("RENAMENX", args, 2)?;
            Ok(Command::RenameNx {
                key: extract_bytes(&args[0])?,
                newkey: extract_bytes(&args[1])?,
            })
        }
        b"UNLINK" => {
            check_min_arity("UNLINK", args, 1)?;
            let keys = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
            Ok(Command::Unlink { keys })
        }
        b"COPY" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(ProtocolError::WrongArity("COPY".into()));
            }
            let source = extract_bytes(&args[0])?;
            let destination = extract_bytes(&args[1])?;
            let replace = if args.len() == 3 {
                let flag = extract_string(&args[2])?.to_ascii_uppercase();
                if flag != b"REPLACE" {
                    return Err(ProtocolError::InvalidData(
                        "ERR unsupported COPY flag".into(),
                    ));
                }
                true
            } else {
                false
            };
            Ok(Command::Copy {
                source,
                destination,
                replace,
            })
        }
        b"RANDOMKEY" => {
            check_arity("RANDOMKEY", args, 0)?;
            Ok(Command::RandomKey)
        }
        b"TOUCH" => {
            check_min_arity("TOUCH", args, 1)?;
            let keys = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
            Ok(Command::Touch { keys })
        }

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
        b"LSET" => {
            check_arity("LSET", args, 3)?;
            Ok(Command::LSet {
                key: extract_bytes(&args[0])?,
                index: parse_i64(&args[1])?,
                value: extract_bytes(&args[2])?,
            })
        }
        b"LINSERT" => {
            check_arity("LINSERT", args, 4)?;
            let direction = extract_string(&args[1])?.to_ascii_uppercase();
            let before = match direction.as_slice() {
                b"BEFORE" => true,
                b"AFTER" => false,
                _ => {
                    return Err(ProtocolError::InvalidData(
                        "ERR syntax error, BEFORE or AFTER required".into(),
                    ))
                }
            };
            Ok(Command::LInsert {
                key: extract_bytes(&args[0])?,
                before,
                pivot: extract_bytes(&args[2])?,
                value: extract_bytes(&args[3])?,
            })
        }
        b"LREM" => {
            check_arity("LREM", args, 3)?;
            Ok(Command::LRem {
                key: extract_bytes(&args[0])?,
                count: parse_i64(&args[1])?,
                value: extract_bytes(&args[2])?,
            })
        }
        b"LTRIM" => {
            check_arity("LTRIM", args, 3)?;
            Ok(Command::LTrim {
                key: extract_bytes(&args[0])?,
                start: parse_i64(&args[1])?,
                stop: parse_i64(&args[2])?,
            })
        }
        b"LPOS" => parse_lpos(args),
        b"RPOPLPUSH" => {
            check_arity("RPOPLPUSH", args, 2)?;
            Ok(Command::RPopLPush {
                source: extract_bytes(&args[0])?,
                destination: extract_bytes(&args[1])?,
            })
        }
        b"LMOVE" => {
            check_arity("LMOVE", args, 4)?;
            let from_dir = extract_string(&args[2])?.to_ascii_uppercase();
            let to_dir = extract_string(&args[3])?.to_ascii_uppercase();
            let from_left = match from_dir.as_slice() {
                b"LEFT" => true,
                b"RIGHT" => false,
                _ => {
                    return Err(ProtocolError::InvalidData(
                        "ERR syntax error, LEFT or RIGHT required".into(),
                    ))
                }
            };
            let to_left = match to_dir.as_slice() {
                b"LEFT" => true,
                b"RIGHT" => false,
                _ => {
                    return Err(ProtocolError::InvalidData(
                        "ERR syntax error, LEFT or RIGHT required".into(),
                    ))
                }
            };
            Ok(Command::LMove {
                source: extract_bytes(&args[0])?,
                destination: extract_bytes(&args[1])?,
                from_left,
                to_left,
            })
        }

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
        b"HMGET" => {
            check_min_arity("HMGET", args, 2)?;
            Ok(Command::HMGet {
                key: extract_bytes(&args[0])?,
                fields: args[1..]
                    .iter()
                    .map(extract_bytes)
                    .collect::<Result<_, _>>()?,
            })
        }
        b"HMSET" => {
            if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                return Err(ProtocolError::WrongArity("HMSET".into()));
            }
            let key = extract_bytes(&args[0])?;
            let fields = args[1..]
                .chunks(2)
                .map(|chunk| Ok((extract_bytes(&chunk[0])?, extract_bytes(&chunk[1])?)))
                .collect::<Result<_, ProtocolError>>()?;
            Ok(Command::HSet { key, fields })
        }
        b"HKEYS" => {
            check_arity("HKEYS", args, 1)?;
            Ok(Command::HKeys {
                key: extract_bytes(&args[0])?,
            })
        }
        b"HVALS" => {
            check_arity("HVALS", args, 1)?;
            Ok(Command::HVals {
                key: extract_bytes(&args[0])?,
            })
        }
        b"HSETNX" => {
            check_arity("HSETNX", args, 3)?;
            Ok(Command::HSetNx {
                key: extract_bytes(&args[0])?,
                field: extract_bytes(&args[1])?,
                value: extract_bytes(&args[2])?,
            })
        }
        b"HINCRBYFLOAT" => {
            check_arity("HINCRBYFLOAT", args, 3)?;
            Ok(Command::HIncrByFloat {
                key: extract_bytes(&args[0])?,
                field: extract_bytes(&args[1])?,
                delta: parse_f64(&args[2])?,
            })
        }
        b"HRANDFIELD" => parse_hrandfield(args),
        b"HSCAN" => parse_hscan(args),

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
        b"SPOP" => {
            check_min_arity("SPOP", args, 1)?;
            let key = extract_bytes(&args[0])?;
            let count = if args.len() >= 2 {
                Some(parse_u64(&args[1])? as usize)
            } else {
                None
            };
            Ok(Command::SPop { key, count })
        }
        b"SRANDMEMBER" => {
            check_min_arity("SRANDMEMBER", args, 1)?;
            let key = extract_bytes(&args[0])?;
            let count = if args.len() >= 2 {
                Some(parse_i64(&args[1])?)
            } else {
                None
            };
            Ok(Command::SRandMember { key, count })
        }
        b"SUNION" => {
            check_min_arity("SUNION", args, 1)?;
            let keys = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
            Ok(Command::SUnion { keys })
        }
        b"SUNIONSTORE" => {
            check_min_arity("SUNIONSTORE", args, 2)?;
            let destination = extract_bytes(&args[0])?;
            let keys = args[1..]
                .iter()
                .map(extract_bytes)
                .collect::<Result<_, _>>()?;
            Ok(Command::SUnionStore { destination, keys })
        }
        b"SINTER" => {
            check_min_arity("SINTER", args, 1)?;
            let keys = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
            Ok(Command::SInter { keys })
        }
        b"SINTERSTORE" => {
            check_min_arity("SINTERSTORE", args, 2)?;
            let destination = extract_bytes(&args[0])?;
            let keys = args[1..]
                .iter()
                .map(extract_bytes)
                .collect::<Result<_, _>>()?;
            Ok(Command::SInterStore { destination, keys })
        }
        b"SDIFF" => {
            check_min_arity("SDIFF", args, 1)?;
            let keys = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
            Ok(Command::SDiff { keys })
        }
        b"SDIFFSTORE" => {
            check_min_arity("SDIFFSTORE", args, 2)?;
            let destination = extract_bytes(&args[0])?;
            let keys = args[1..]
                .iter()
                .map(extract_bytes)
                .collect::<Result<_, _>>()?;
            Ok(Command::SDiffStore { destination, keys })
        }
        b"SINTERCARD" => parse_sintercard(args),
        b"SMOVE" => {
            check_arity("SMOVE", args, 3)?;
            Ok(Command::SMove {
                source: extract_bytes(&args[0])?,
                destination: extract_bytes(&args[1])?,
                member: extract_bytes(&args[2])?,
            })
        }
        b"SMISMEMBER" => {
            check_min_arity("SMISMEMBER", args, 2)?;
            Ok(Command::SMisMember {
                key: extract_bytes(&args[0])?,
                members: args[1..]
                    .iter()
                    .map(extract_bytes)
                    .collect::<Result<_, _>>()?,
            })
        }
        b"SSCAN" => parse_sscan(args),

        b"ZADD" => {
            check_min_arity("ZADD", args, 3)?;
            let key = extract_bytes(&args[0])?;
            let pair_args = &args[1..];
            if pair_args.len() % 2 != 0 {
                return Err(ProtocolError::WrongArity("ZADD".into()));
            }
            let members = pair_args
                .chunks(2)
                .map(|chunk| Ok((parse_f64(&chunk[0])?, extract_bytes(&chunk[1])?)))
                .collect::<Result<Vec<(f64, Vec<u8>)>, ProtocolError>>()?;
            Ok(Command::ZAdd { key, members })
        }
        b"ZREM" => {
            check_min_arity("ZREM", args, 2)?;
            Ok(Command::ZRem {
                key: extract_bytes(&args[0])?,
                members: args[1..]
                    .iter()
                    .map(extract_bytes)
                    .collect::<Result<_, _>>()?,
            })
        }
        b"ZSCORE" => {
            check_arity("ZSCORE", args, 2)?;
            Ok(Command::ZScore {
                key: extract_bytes(&args[0])?,
                member: extract_bytes(&args[1])?,
            })
        }
        b"ZRANK" => {
            check_arity("ZRANK", args, 2)?;
            Ok(Command::ZRank {
                key: extract_bytes(&args[0])?,
                member: extract_bytes(&args[1])?,
            })
        }
        b"ZREVRANK" => {
            check_arity("ZREVRANK", args, 2)?;
            Ok(Command::ZRevRank {
                key: extract_bytes(&args[0])?,
                member: extract_bytes(&args[1])?,
            })
        }
        b"ZCARD" => {
            check_arity("ZCARD", args, 1)?;
            Ok(Command::ZCard {
                key: extract_bytes(&args[0])?,
            })
        }
        b"ZRANGE" => {
            if args.len() < 3 || args.len() > 4 {
                return Err(ProtocolError::WrongArity("ZRANGE".into()));
            }
            let withscores = if args.len() == 4 {
                let flag = extract_string(&args[3])?.to_ascii_uppercase();
                if flag != b"WITHSCORES" {
                    return Err(ProtocolError::InvalidData("unsupported ZRANGE flag".into()));
                }
                true
            } else {
                false
            };
            Ok(Command::ZRange {
                key: extract_bytes(&args[0])?,
                start: parse_i64(&args[1])?,
                stop: parse_i64(&args[2])?,
                withscores,
            })
        }
        b"ZREVRANGE" => {
            if args.len() < 3 || args.len() > 4 {
                return Err(ProtocolError::WrongArity("ZREVRANGE".into()));
            }
            let withscores = if args.len() == 4 {
                let flag = extract_string(&args[3])?.to_ascii_uppercase();
                if flag != b"WITHSCORES" {
                    return Err(ProtocolError::InvalidData(
                        "unsupported ZREVRANGE flag".into(),
                    ));
                }
                true
            } else {
                false
            };
            Ok(Command::ZRevRange {
                key: extract_bytes(&args[0])?,
                start: parse_i64(&args[1])?,
                stop: parse_i64(&args[2])?,
                withscores,
            })
        }
        b"ZRANGEBYSCORE" => {
            check_min_arity("ZRANGEBYSCORE", args, 3)?;
            let key = extract_bytes(&args[0])?;
            let min = parse_score(&args[1])?;
            let max = parse_score(&args[2])?;
            let mut withscores = false;
            let mut offset = None;
            let mut count = None;
            let mut i = 3;
            while i < args.len() {
                let flag = extract_string(&args[i])?.to_ascii_uppercase();
                match flag.as_slice() {
                    b"WITHSCORES" => withscores = true,
                    b"LIMIT" => {
                        if i + 2 >= args.len() {
                            return Err(ProtocolError::WrongArity("ZRANGEBYSCORE".into()));
                        }
                        offset = Some(parse_u64(&args[i + 1])? as usize);
                        count = Some(parse_u64(&args[i + 2])? as usize);
                        i += 2;
                    }
                    _ => {
                        return Err(ProtocolError::InvalidData(
                            "unsupported ZRANGEBYSCORE flag".into(),
                        ))
                    }
                }
                i += 1;
            }
            Ok(Command::ZRangeByScore {
                key,
                min,
                max,
                withscores,
                offset,
                count,
            })
        }
        b"ZINCRBY" => {
            check_arity("ZINCRBY", args, 3)?;
            Ok(Command::ZIncrBy {
                key: extract_bytes(&args[0])?,
                delta: parse_f64(&args[1])?,
                member: extract_bytes(&args[2])?,
            })
        }
        b"ZCOUNT" => {
            check_arity("ZCOUNT", args, 3)?;
            Ok(Command::ZCount {
                key: extract_bytes(&args[0])?,
                min: parse_score(&args[1])?,
                max: parse_score(&args[2])?,
            })
        }
        b"ZREVRANGEBYSCORE" => {
            check_min_arity("ZREVRANGEBYSCORE", args, 3)?;
            let key = extract_bytes(&args[0])?;
            let max = parse_score(&args[1])?;
            let min = parse_score(&args[2])?;
            let mut withscores = false;
            let mut offset = None;
            let mut count = None;
            let mut i = 3;
            while i < args.len() {
                let flag = extract_string(&args[i])?.to_ascii_uppercase();
                match flag.as_slice() {
                    b"WITHSCORES" => withscores = true,
                    b"LIMIT" => {
                        if i + 2 >= args.len() {
                            return Err(ProtocolError::WrongArity("ZREVRANGEBYSCORE".into()));
                        }
                        offset = Some(parse_u64(&args[i + 1])? as usize);
                        count = Some(parse_u64(&args[i + 2])? as usize);
                        i += 2;
                    }
                    _ => {
                        return Err(ProtocolError::InvalidData(
                            "unsupported ZREVRANGEBYSCORE flag".into(),
                        ))
                    }
                }
                i += 1;
            }
            Ok(Command::ZRevRangeByScore {
                key,
                max,
                min,
                withscores,
                offset,
                count,
            })
        }
        b"ZPOPMIN" => {
            check_min_arity("ZPOPMIN", args, 1)?;
            let key = extract_bytes(&args[0])?;
            let count = if args.len() >= 2 {
                Some(parse_u64(&args[1])? as usize)
            } else {
                None
            };
            Ok(Command::ZPopMin { key, count })
        }
        b"ZPOPMAX" => {
            check_min_arity("ZPOPMAX", args, 1)?;
            let key = extract_bytes(&args[0])?;
            let count = if args.len() >= 2 {
                Some(parse_u64(&args[1])? as usize)
            } else {
                None
            };
            Ok(Command::ZPopMax { key, count })
        }
        b"ZRANGEBYLEX" => parse_zrangebylex(args),
        b"ZREVRANGEBYLEX" => parse_zrevrangebylex(args),
        b"ZLEXCOUNT" => {
            check_arity("ZLEXCOUNT", args, 3)?;
            Ok(Command::ZLexCount {
                key: extract_bytes(&args[0])?,
                min: extract_bytes(&args[1])?,
                max: extract_bytes(&args[2])?,
            })
        }
        b"ZMSCORE" => {
            check_min_arity("ZMSCORE", args, 2)?;
            Ok(Command::ZMScore {
                key: extract_bytes(&args[0])?,
                members: args[1..]
                    .iter()
                    .map(extract_bytes)
                    .collect::<Result<_, _>>()?,
            })
        }
        b"ZRANDMEMBER" => parse_zrandmember(args),
        b"ZSCAN" => parse_zscan(args),

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
            if args.is_empty() {
                return Ok(Command::CommandInfo { names: Vec::new() });
            }
            let subcmd = extract_string(&args[0])?.to_ascii_uppercase();
            match subcmd.as_slice() {
                b"COUNT" => {
                    check_arity("COMMAND COUNT", &args[1..], 0)?;
                    Ok(Command::CommandCount)
                }
                b"LIST" => {
                    check_arity("COMMAND LIST", &args[1..], 0)?;
                    Ok(Command::CommandList)
                }
                b"INFO" => Ok(Command::CommandInfo {
                    names: args[1..]
                        .iter()
                        .map(extract_bytes)
                        .collect::<Result<_, _>>()?,
                }),
                b"DOCS" => Ok(Command::CommandDocs {
                    names: args[1..]
                        .iter()
                        .map(extract_bytes)
                        .collect::<Result<_, _>>()?,
                }),
                b"HELP" => {
                    check_arity("COMMAND HELP", &args[1..], 0)?;
                    Ok(Command::CommandHelp)
                }
                _ => Err(ProtocolError::InvalidData(format!(
                    "unknown COMMAND subcommand '{}'. Try COMMAND HELP.",
                    String::from_utf8_lossy(&subcmd).to_ascii_lowercase()
                ))),
            }
        }

        b"BGSAVE" => {
            check_arity("BGSAVE", args, 0)?;
            Ok(Command::BgSave)
        }
        b"BGREWRITEAOF" => {
            check_arity("BGREWRITEAOF", args, 0)?;
            Ok(Command::BgRewriteAof)
        }
        b"FLUSHALL" => Ok(Command::FlushAll),
        b"DUMP" => {
            check_arity("DUMP", args, 0)?;
            Ok(Command::Dump)
        }
        b"HELLO" => {
            let version = if args.is_empty() {
                None
            } else {
                let v = parse_u64(&args[0])? as u8;
                Some(v)
            };
            Ok(Command::Hello { version })
        }
        b"AUTH" => {
            if args.is_empty() {
                return Err(ProtocolError::WrongArity("AUTH".into()));
            }
            let password = if args.len() == 1 {
                extract_bytes(&args[0])?
            } else {
                extract_bytes(&args[1])?
            };
            Ok(Command::Auth { password })
        }

        b"CDCPOLL" => {
            check_arity("CDCPOLL", args, 2)?;
            Ok(Command::CdcPoll {
                cursor: parse_u64(&args[0])?,
                count: parse_u64(&args[1])? as usize,
            })
        }
        b"CDC.GROUP" => parse_cdc_group(args),
        b"CDC.ACK" => {
            check_min_arity("CDC.ACK", args, 3)?;
            let key = extract_bytes(&args[0])?;
            let group = String::from_utf8_lossy(&extract_bytes(&args[1])?).into_owned();
            let seqs = args[2..].iter().map(parse_u64).collect::<Result<_, _>>()?;
            Ok(Command::CdcAck { key, group, seqs })
        }
        b"CDC.PENDING" => {
            check_arity("CDC.PENDING", args, 2)?;
            let key = extract_bytes(&args[0])?;
            let group = String::from_utf8_lossy(&extract_bytes(&args[1])?).into_owned();
            Ok(Command::CdcPending { key, group })
        }

        b"VECSET" => {
            check_min_arity("VECSET", args, 3)?;
            let key = extract_bytes(&args[0])?;
            let dimensions = parse_u64(&args[1])? as usize;
            if args.len() != 2 + dimensions {
                return Err(ProtocolError::InvalidData(format!(
                    "VECSET expected {} vector components, got {}",
                    dimensions,
                    args.len() - 2
                )));
            }
            let vector = args[2..].iter().map(parse_f32).collect::<Result<_, _>>()?;
            Ok(Command::VecSet {
                key,
                dimensions,
                vector,
            })
        }
        b"VECQUERY" => {
            check_min_arity("VECQUERY", args, 3)?;
            let key = extract_bytes(&args[0])?;
            let k = parse_u64(&args[1])? as usize;
            let vector = args[2..].iter().map(parse_f32).collect::<Result<_, _>>()?;
            Ok(Command::VecQuery { key, k, vector })
        }
        b"VECDEL" => {
            check_arity("VECDEL", args, 1)?;
            Ok(Command::VecDel {
                key: extract_bytes(&args[0])?,
            })
        }

        b"STATS.HOTKEYS" => {
            check_arity("STATS.HOTKEYS", args, 1)?;
            Ok(Command::StatsHotkeys {
                count: parse_u64(&args[0])? as usize,
            })
        }
        b"STATS.LATENCY" => {
            check_min_arity("STATS.LATENCY", args, 2)?;
            let command = extract_bytes(&args[0])?;
            let percentiles = args[1..].iter().map(parse_f64).collect::<Result<_, _>>()?;
            Ok(Command::StatsLatency {
                command,
                percentiles,
            })
        }
        b"STATS.MEMORY" => {
            check_min_arity("STATS.MEMORY", args, 1)?;
            let prefixes = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
            Ok(Command::StatsMemory { prefixes })
        }

        b"OBJECT" => {
            check_min_arity("OBJECT", args, 1)?;
            let subcmd = extract_string(&args[0])?.to_ascii_uppercase();
            match subcmd.as_slice() {
                b"FREQ" => {
                    check_arity("OBJECT FREQ", &args[1..], 1)?;
                    Ok(Command::ObjectFreq {
                        key: extract_bytes(&args[1])?,
                    })
                }
                b"ENCODING" => {
                    check_arity("OBJECT ENCODING", &args[1..], 1)?;
                    Ok(Command::ObjectEncoding {
                        key: extract_bytes(&args[1])?,
                    })
                }
                b"REFCOUNT" => {
                    check_arity("OBJECT REFCOUNT", &args[1..], 1)?;
                    Ok(Command::ObjectRefCount {
                        key: extract_bytes(&args[1])?,
                    })
                }
                b"IDLETIME" => {
                    check_arity("OBJECT IDLETIME", &args[1..], 1)?;
                    Ok(Command::ObjectIdleTime {
                        key: extract_bytes(&args[1])?,
                    })
                }
                b"HELP" => Ok(Command::ObjectHelp),
                _ => Err(ProtocolError::InvalidData(format!(
                    "unknown OBJECT subcommand: {}",
                    String::from_utf8_lossy(&subcmd)
                ))),
            }
        }

        b"XADD" => parse_xadd(args),
        b"XLEN" => {
            check_arity("XLEN", args, 1)?;
            Ok(Command::XLen {
                key: extract_bytes(&args[0])?,
            })
        }
        b"XRANGE" => parse_xrange(args),
        b"XREVRANGE" => parse_xrevrange(args),
        b"XREAD" => parse_xread(args),
        b"XTRIM" => {
            check_min_arity("XTRIM", args, 3)?;
            let key = extract_bytes(&args[0])?;
            let flag = extract_string(&args[1])?.to_ascii_uppercase();
            if flag != b"MAXLEN" {
                return Err(ProtocolError::InvalidData(
                    "ERR XTRIM requires MAXLEN strategy".into(),
                ));
            }
            let maxlen = parse_u64(&args[2])? as usize;
            Ok(Command::XTrim { key, maxlen })
        }

        b"SUBSCRIBE" => {
            check_min_arity("SUBSCRIBE", args, 1)?;
            let channels = args
                .iter()
                .map(extract_bytes)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Command::Subscribe { channels })
        }
        b"UNSUBSCRIBE" => {
            let channels = args
                .iter()
                .map(extract_bytes)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Command::Unsubscribe { channels })
        }
        b"PSUBSCRIBE" => {
            check_min_arity("PSUBSCRIBE", args, 1)?;
            let patterns = args
                .iter()
                .map(extract_bytes)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Command::PSubscribe { patterns })
        }
        b"PUNSUBSCRIBE" => {
            let patterns = args
                .iter()
                .map(extract_bytes)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Command::PUnsubscribe { patterns })
        }
        b"PUBLISH" => {
            check_arity("PUBLISH", args, 2)?;
            Ok(Command::Publish {
                channel: extract_bytes(&args[0])?,
                message: extract_bytes(&args[1])?,
            })
        }

        b"PFADD" => {
            check_min_arity("PFADD", args, 1)?;
            Ok(Command::PfAdd {
                key: extract_bytes(&args[0])?,
                elements: args[1..]
                    .iter()
                    .map(extract_bytes)
                    .collect::<Result<_, _>>()?,
            })
        }
        b"PFCOUNT" => {
            check_min_arity("PFCOUNT", args, 1)?;
            let keys = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
            Ok(Command::PfCount { keys })
        }
        b"PFMERGE" => {
            check_min_arity("PFMERGE", args, 2)?;
            let destkey = extract_bytes(&args[0])?;
            let sourcekeys = args[1..]
                .iter()
                .map(extract_bytes)
                .collect::<Result<_, _>>()?;
            Ok(Command::PfMerge {
                destkey,
                sourcekeys,
            })
        }

        b"SETBIT" => {
            check_arity("SETBIT", args, 3)?;
            let value = parse_u64(&args[2])?;
            if value > 1 {
                return Err(ProtocolError::InvalidData(
                    "ERR bit is not an integer or out of range".into(),
                ));
            }
            Ok(Command::SetBit {
                key: extract_bytes(&args[0])?,
                offset: parse_u64(&args[1])?,
                value: value as u8,
            })
        }
        b"GETBIT" => {
            check_arity("GETBIT", args, 2)?;
            Ok(Command::GetBit {
                key: extract_bytes(&args[0])?,
                offset: parse_u64(&args[1])?,
            })
        }
        b"BITCOUNT" => parse_bitcount(args),
        b"BITOP" => parse_bitop(args),
        b"BITPOS" => parse_bitpos(args),
        b"BITFIELD" => parse_bitfield(args),

        b"GEOADD" => parse_geoadd(args),
        b"GEODIST" => parse_geodist(args),
        b"GEOHASH" => {
            check_min_arity("GEOHASH", args, 2)?;
            Ok(Command::GeoHash {
                key: extract_bytes(&args[0])?,
                members: args[1..]
                    .iter()
                    .map(extract_bytes)
                    .collect::<Result<_, _>>()?,
            })
        }
        b"GEOPOS" => {
            check_min_arity("GEOPOS", args, 2)?;
            Ok(Command::GeoPos {
                key: extract_bytes(&args[0])?,
                members: args[1..]
                    .iter()
                    .map(extract_bytes)
                    .collect::<Result<_, _>>()?,
            })
        }
        b"GEOSEARCH" => parse_geosearch(args),

        b"MULTI" => {
            check_arity("MULTI", args, 0)?;
            Ok(Command::Multi)
        }
        b"EXEC" => {
            check_arity("EXEC", args, 0)?;
            Ok(Command::Exec)
        }
        b"DISCARD" => {
            check_arity("DISCARD", args, 0)?;
            Ok(Command::Discard)
        }
        b"WATCH" => {
            check_min_arity("WATCH", args, 1)?;
            let keys = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
            Ok(Command::Watch { keys })
        }
        b"UNWATCH" => {
            check_arity("UNWATCH", args, 0)?;
            Ok(Command::Unwatch)
        }

        b"CONFIG" => {
            if args.is_empty() {
                return Err(ProtocolError::WrongArity("CONFIG".into()));
            }
            let subcmd = extract_string(&args[0])?.to_ascii_uppercase();
            match subcmd.as_slice() {
                b"GET" => {
                    check_arity("CONFIG GET", &args[1..], 1)?;
                    let pattern = String::from_utf8_lossy(&extract_bytes(&args[1])?).into_owned();
                    Ok(Command::ConfigGet { pattern })
                }
                b"SET" => {
                    check_arity("CONFIG SET", &args[1..], 2)?;
                    let parameter = String::from_utf8_lossy(&extract_bytes(&args[1])?).into_owned();
                    let value = String::from_utf8_lossy(&extract_bytes(&args[2])?).into_owned();
                    Ok(Command::ConfigSet { parameter, value })
                }
                b"RESETSTAT" => {
                    check_arity("CONFIG RESETSTAT", &args[1..], 0)?;
                    Ok(Command::ConfigResetStat)
                }
                _ => Err(ProtocolError::InvalidData(format!(
                    "unknown CONFIG subcommand: {}",
                    String::from_utf8_lossy(&subcmd)
                ))),
            }
        }
        b"CLIENT" => {
            if args.is_empty() {
                return Err(ProtocolError::WrongArity("CLIENT".into()));
            }
            let subcmd = extract_string(&args[0])?.to_ascii_uppercase();
            match subcmd.as_slice() {
                b"ID" => {
                    check_arity("CLIENT ID", &args[1..], 0)?;
                    Ok(Command::ClientId)
                }
                b"GETNAME" => {
                    check_arity("CLIENT GETNAME", &args[1..], 0)?;
                    Ok(Command::ClientGetName)
                }
                b"SETNAME" => {
                    check_arity("CLIENT SETNAME", &args[1..], 1)?;
                    Ok(Command::ClientSetName {
                        name: extract_bytes(&args[1])?,
                    })
                }
                b"LIST" => Ok(Command::ClientList),
                b"INFO" => Ok(Command::ClientInfo),
                _ => Err(ProtocolError::InvalidData(format!(
                    "unknown CLIENT subcommand: {}",
                    String::from_utf8_lossy(&subcmd)
                ))),
            }
        }
        b"TIME" => {
            check_arity("TIME", args, 0)?;
            Ok(Command::Time)
        }
        b"SELECT" => {
            check_arity("SELECT", args, 1)?;
            Ok(Command::Select {
                db: parse_i64(&args[0])?,
            })
        }
        b"QUIT" => Ok(Command::Quit),
        b"WAIT" => {
            check_arity("WAIT", args, 2)?;
            Ok(Command::Wait {
                numreplicas: parse_i64(&args[0])?,
                timeout: parse_i64(&args[1])?,
            })
        }

        b"BLPOP" => parse_blpop(args),
        b"BRPOP" => parse_brpop(args),
        b"BLMOVE" => parse_blmove(args),
        b"BZPOPMIN" => parse_bzpopmin(args),
        b"BZPOPMAX" => parse_bzpopmax(args),

        b"XGROUP" => parse_xgroup(args),
        b"XREADGROUP" => parse_xreadgroup(args),
        b"XACK" => parse_xack(args),
        b"XPENDING" => parse_xpending(args),
        b"XCLAIM" => parse_xclaim(args),
        b"XAUTOCLAIM" => parse_xautoclaim(args),
        b"XINFO" => parse_xinfo(args),
        b"XDEL" => parse_xdel(args),

        _ => Err(ProtocolError::UnknownCommand(
            String::from_utf8_lossy(&cmd_name).into_owned(),
        )),
    }
}

fn parse_xgroup(args: &[RespValue]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(ProtocolError::WrongArity("XGROUP".into()));
    }
    let subcmd = extract_string(&args[0])?.to_ascii_uppercase();
    match subcmd.as_slice() {
        b"CREATE" => {
            check_min_arity("XGROUP CREATE", &args[1..], 3)?;
            let key = extract_bytes(&args[1])?;
            let group = String::from_utf8_lossy(&extract_bytes(&args[2])?).into_owned();
            let id = String::from_utf8_lossy(&extract_bytes(&args[3])?).into_owned();
            let mkstream = if args.len() > 4 {
                let flag = extract_string(&args[4])?.to_ascii_uppercase();
                flag == b"MKSTREAM"
            } else {
                false
            };
            Ok(Command::XGroupCreate {
                key,
                group,
                id,
                mkstream,
            })
        }
        b"DESTROY" => {
            check_arity("XGROUP DESTROY", &args[1..], 2)?;
            let key = extract_bytes(&args[1])?;
            let group = String::from_utf8_lossy(&extract_bytes(&args[2])?).into_owned();
            Ok(Command::XGroupDestroy { key, group })
        }
        b"DELCONSUMER" => {
            check_arity("XGROUP DELCONSUMER", &args[1..], 3)?;
            let key = extract_bytes(&args[1])?;
            let group = String::from_utf8_lossy(&extract_bytes(&args[2])?).into_owned();
            let consumer = String::from_utf8_lossy(&extract_bytes(&args[3])?).into_owned();
            Ok(Command::XGroupDelConsumer {
                key,
                group,
                consumer,
            })
        }
        _ => Err(ProtocolError::InvalidData(format!(
            "unknown XGROUP subcommand: {}",
            String::from_utf8_lossy(&subcmd)
        ))),
    }
}

fn parse_xreadgroup(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("XREADGROUP", args, 6)?;
    let first = extract_string(&args[0])?.to_ascii_uppercase();
    if first != b"GROUP" {
        return Err(ProtocolError::InvalidData(
            "ERR syntax error, GROUP expected".into(),
        ));
    }
    let group = String::from_utf8_lossy(&extract_bytes(&args[1])?).into_owned();
    let consumer = String::from_utf8_lossy(&extract_bytes(&args[2])?).into_owned();

    let mut idx = 3;
    let mut count = None;

    while idx < args.len() {
        let token = extract_string(&args[idx])?.to_ascii_uppercase();
        match token.as_slice() {
            b"COUNT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::WrongArity("XREADGROUP".into()));
                }
                count = Some(parse_u64(&args[idx])? as usize);
                idx += 1;
            }
            b"BLOCK" => {
                idx += 2;
            }
            b"STREAMS" => {
                idx += 1;
                break;
            }
            _ => {
                return Err(ProtocolError::InvalidData(format!(
                    "unsupported XREADGROUP option: {}",
                    String::from_utf8_lossy(&token)
                )));
            }
        }
    }

    let remaining = &args[idx..];
    if remaining.is_empty() || remaining.len() % 2 != 0 {
        return Err(ProtocolError::WrongArity("XREADGROUP".into()));
    }

    let half = remaining.len() / 2;
    let keys = remaining[..half]
        .iter()
        .map(extract_bytes)
        .collect::<Result<Vec<_>, _>>()?;
    let ids = remaining[half..]
        .iter()
        .map(extract_bytes)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Command::XReadGroup {
        group,
        consumer,
        count,
        keys,
        ids,
    })
}

fn parse_xack(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("XACK", args, 3)?;
    let key = extract_bytes(&args[0])?;
    let group = String::from_utf8_lossy(&extract_bytes(&args[1])?).into_owned();
    let ids = args[2..]
        .iter()
        .map(extract_bytes)
        .collect::<Result<_, _>>()?;
    Ok(Command::XAck { key, group, ids })
}

fn parse_xpending(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("XPENDING", args, 2)?;
    let key = extract_bytes(&args[0])?;
    let group = String::from_utf8_lossy(&extract_bytes(&args[1])?).into_owned();
    let (start, end, count) = if args.len() >= 5 {
        (
            Some(extract_bytes(&args[2])?),
            Some(extract_bytes(&args[3])?),
            Some(parse_u64(&args[4])? as usize),
        )
    } else {
        (None, None, None)
    };
    Ok(Command::XPending {
        key,
        group,
        start,
        end,
        count,
    })
}

fn parse_xclaim(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("XCLAIM", args, 5)?;
    let key = extract_bytes(&args[0])?;
    let group = String::from_utf8_lossy(&extract_bytes(&args[1])?).into_owned();
    let consumer = String::from_utf8_lossy(&extract_bytes(&args[2])?).into_owned();
    let min_idle_time = parse_u64(&args[3])?;
    let mut ids = Vec::new();
    for arg in &args[4..] {
        let val = extract_bytes(arg)?;
        let upper = extract_string(arg)?.to_ascii_uppercase();
        if upper == b"IDLE"
            || upper == b"TIME"
            || upper == b"RETRYCOUNT"
            || upper == b"FORCE"
            || upper == b"JUSTID"
        {
            break;
        }
        ids.push(val);
    }
    if ids.is_empty() {
        return Err(ProtocolError::WrongArity("XCLAIM".into()));
    }
    Ok(Command::XClaim {
        key,
        group,
        consumer,
        min_idle_time,
        ids,
    })
}

fn parse_xautoclaim(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("XAUTOCLAIM", args, 5)?;
    let key = extract_bytes(&args[0])?;
    let group = String::from_utf8_lossy(&extract_bytes(&args[1])?).into_owned();
    let consumer = String::from_utf8_lossy(&extract_bytes(&args[2])?).into_owned();
    let min_idle_time = parse_u64(&args[3])?;
    let start = extract_bytes(&args[4])?;
    let mut count = None;
    if args.len() >= 7 {
        let flag = extract_string(&args[5])?.to_ascii_uppercase();
        if flag == b"COUNT" {
            count = Some(parse_u64(&args[6])? as usize);
        }
    }
    Ok(Command::XAutoClaim {
        key,
        group,
        consumer,
        min_idle_time,
        start,
        count,
    })
}

fn parse_xinfo(args: &[RespValue]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(ProtocolError::WrongArity("XINFO".into()));
    }
    let subcmd = extract_string(&args[0])?.to_ascii_uppercase();
    match subcmd.as_slice() {
        b"STREAM" => {
            check_min_arity("XINFO STREAM", &args[1..], 1)?;
            let key = extract_bytes(&args[1])?;
            Ok(Command::XInfoStream { key })
        }
        b"GROUPS" => {
            check_min_arity("XINFO GROUPS", &args[1..], 1)?;
            let key = extract_bytes(&args[1])?;
            Ok(Command::XInfoGroups { key })
        }
        _ => Err(ProtocolError::InvalidData(format!(
            "unknown XINFO subcommand: {}",
            String::from_utf8_lossy(&subcmd)
        ))),
    }
}

fn parse_xdel(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("XDEL", args, 2)?;
    let key = extract_bytes(&args[0])?;
    let ids = args[1..]
        .iter()
        .map(extract_bytes)
        .collect::<Result<_, _>>()?;
    Ok(Command::XDel { key, ids })
}

fn parse_cdc_group(args: &[RespValue]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(ProtocolError::WrongArity("CDC.GROUP".into()));
    }
    let subcmd = extract_string(&args[0])?.to_ascii_uppercase();
    match subcmd.as_slice() {
        b"CREATE" => {
            check_arity("CDC.GROUP CREATE", &args[1..], 3)?;
            let key = extract_bytes(&args[1])?;
            let group = String::from_utf8_lossy(&extract_bytes(&args[2])?).into_owned();
            let start_seq = parse_u64(&args[3])?;
            Ok(Command::CdcGroupCreate {
                key,
                group,
                start_seq,
            })
        }
        b"READ" => {
            check_arity("CDC.GROUP READ", &args[1..], 4)?;
            let key = extract_bytes(&args[1])?;
            let group = String::from_utf8_lossy(&extract_bytes(&args[2])?).into_owned();
            let consumer = String::from_utf8_lossy(&extract_bytes(&args[3])?).into_owned();
            let count = parse_u64(&args[4])? as usize;
            Ok(Command::CdcGroupRead {
                key,
                group,
                consumer,
                count,
            })
        }
        _ => Err(ProtocolError::InvalidData(format!(
            "unknown CDC.GROUP subcommand: {}",
            String::from_utf8_lossy(&subcmd)
        ))),
    }
}

fn parse_getex(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("GETEX", args, 1)?;
    let key = extract_bytes(&args[0])?;
    let mut ex = None;
    let mut px = None;
    let mut exat = None;
    let mut pxat = None;
    let mut persist = false;

    let mut i = 1;
    while i < args.len() {
        let flag = extract_string(&args[i])?.to_ascii_uppercase();
        match flag.as_slice() {
            b"EX" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtocolError::WrongArity("GETEX".into()));
                }
                ex = Some(parse_u64(&args[i])?);
            }
            b"PX" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtocolError::WrongArity("GETEX".into()));
                }
                px = Some(parse_u64(&args[i])?);
            }
            b"EXAT" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtocolError::WrongArity("GETEX".into()));
                }
                exat = Some(parse_u64(&args[i])?);
            }
            b"PXAT" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtocolError::WrongArity("GETEX".into()));
                }
                pxat = Some(parse_u64(&args[i])?);
            }
            b"PERSIST" => {
                persist = true;
            }
            _ => {
                return Err(ProtocolError::InvalidData(format!(
                    "unsupported GETEX flag: {}",
                    String::from_utf8_lossy(&flag)
                )));
            }
        }
        i += 1;
    }

    Ok(Command::GetEx {
        key,
        ex,
        px,
        exat,
        pxat,
        persist,
    })
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

fn parse_lpos(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("LPOS", args, 2)?;
    let key = extract_bytes(&args[0])?;
    let value = extract_bytes(&args[1])?;
    let mut rank = None;
    let mut count = None;
    let mut maxlen = None;
    let mut i = 2;
    while i < args.len() {
        let flag = extract_string(&args[i])?.to_ascii_uppercase();
        match flag.as_slice() {
            b"RANK" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtocolError::WrongArity("LPOS".into()));
                }
                rank = Some(parse_i64(&args[i])?);
            }
            b"COUNT" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtocolError::WrongArity("LPOS".into()));
                }
                count = Some(parse_i64(&args[i])?);
            }
            b"MAXLEN" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtocolError::WrongArity("LPOS".into()));
                }
                maxlen = Some(parse_i64(&args[i])?);
            }
            _ => {
                return Err(ProtocolError::InvalidData(format!(
                    "unsupported LPOS option: {}",
                    String::from_utf8_lossy(&flag)
                )));
            }
        }
        i += 1;
    }
    Ok(Command::LPos {
        key,
        value,
        rank,
        count,
        maxlen,
    })
}

fn parse_hrandfield(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("HRANDFIELD", args, 1)?;
    let key = extract_bytes(&args[0])?;
    if args.len() == 1 {
        return Ok(Command::HRandField {
            key,
            count: None,
            withvalues: false,
        });
    }
    let count = Some(parse_i64(&args[1])?);
    let withvalues = if args.len() >= 3 {
        let flag = extract_string(&args[2])?.to_ascii_uppercase();
        if flag != b"WITHVALUES" {
            return Err(ProtocolError::InvalidData(
                "ERR syntax error, WITHVALUES expected".into(),
            ));
        }
        true
    } else {
        false
    };
    if args.len() > 3 {
        return Err(ProtocolError::WrongArity("HRANDFIELD".into()));
    }
    Ok(Command::HRandField {
        key,
        count,
        withvalues,
    })
}

fn parse_hscan(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("HSCAN", args, 2)?;
    let key = extract_bytes(&args[0])?;
    let cursor = parse_u64(&args[1])?;
    let mut pattern = None;
    let mut count = None;
    let mut i = 2;
    while i < args.len() {
        let flag = extract_string(&args[i])?.to_ascii_uppercase();
        match flag.as_slice() {
            b"MATCH" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtocolError::WrongArity("HSCAN".into()));
                }
                pattern = Some(String::from_utf8_lossy(&extract_bytes(&args[i])?).into_owned());
            }
            b"COUNT" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtocolError::WrongArity("HSCAN".into()));
                }
                count = Some(parse_u64(&args[i])? as usize);
            }
            _ => {
                return Err(ProtocolError::InvalidData(format!(
                    "unsupported HSCAN option: {}",
                    String::from_utf8_lossy(&flag)
                )));
            }
        }
        i += 1;
    }
    Ok(Command::HScan {
        key,
        cursor,
        pattern,
        count,
    })
}

fn parse_sintercard(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("SINTERCARD", args, 2)?;
    let numkeys = parse_u64(&args[0])? as usize;
    if numkeys == 0 {
        return Err(ProtocolError::InvalidData(
            "ERR numkeys can't be zero".into(),
        ));
    }
    if args.len() < 1 + numkeys {
        return Err(ProtocolError::WrongArity("SINTERCARD".into()));
    }
    let keys = args[1..1 + numkeys]
        .iter()
        .map(extract_bytes)
        .collect::<Result<_, _>>()?;
    let mut limit = None;
    let mut i = 1 + numkeys;
    while i < args.len() {
        let flag = extract_string(&args[i])?.to_ascii_uppercase();
        match flag.as_slice() {
            b"LIMIT" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtocolError::WrongArity("SINTERCARD".into()));
                }
                limit = Some(parse_u64(&args[i])? as usize);
            }
            _ => {
                return Err(ProtocolError::InvalidData(
                    "unsupported SINTERCARD option".into(),
                ))
            }
        }
        i += 1;
    }
    Ok(Command::SInterCard {
        numkeys,
        keys,
        limit,
    })
}

fn parse_sscan(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("SSCAN", args, 2)?;
    let key = extract_bytes(&args[0])?;
    let cursor = parse_u64(&args[1])?;
    let mut pattern = None;
    let mut count = None;
    let mut i = 2;
    while i < args.len() {
        let flag = extract_string(&args[i])?.to_ascii_uppercase();
        match flag.as_slice() {
            b"MATCH" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtocolError::WrongArity("SSCAN".into()));
                }
                pattern = Some(String::from_utf8_lossy(&extract_bytes(&args[i])?).into_owned());
            }
            b"COUNT" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtocolError::WrongArity("SSCAN".into()));
                }
                count = Some(parse_u64(&args[i])? as usize);
            }
            _ => {
                return Err(ProtocolError::InvalidData(format!(
                    "unsupported SSCAN option: {}",
                    String::from_utf8_lossy(&flag)
                )));
            }
        }
        i += 1;
    }
    Ok(Command::SScan {
        key,
        cursor,
        pattern,
        count,
    })
}

fn parse_zrangebylex(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("ZRANGEBYLEX", args, 3)?;
    let key = extract_bytes(&args[0])?;
    let min = extract_bytes(&args[1])?;
    let max = extract_bytes(&args[2])?;
    let mut offset = None;
    let mut count = None;
    let mut i = 3;
    while i < args.len() {
        let flag = extract_string(&args[i])?.to_ascii_uppercase();
        match flag.as_slice() {
            b"LIMIT" => {
                if i + 2 >= args.len() {
                    return Err(ProtocolError::WrongArity("ZRANGEBYLEX".into()));
                }
                offset = Some(parse_u64(&args[i + 1])? as usize);
                count = Some(parse_u64(&args[i + 2])? as usize);
                i += 2;
            }
            _ => {
                return Err(ProtocolError::InvalidData(
                    "unsupported ZRANGEBYLEX flag".into(),
                ))
            }
        }
        i += 1;
    }
    Ok(Command::ZRangeByLex {
        key,
        min,
        max,
        offset,
        count,
    })
}

fn parse_zrevrangebylex(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("ZREVRANGEBYLEX", args, 3)?;
    let key = extract_bytes(&args[0])?;
    let max = extract_bytes(&args[1])?;
    let min = extract_bytes(&args[2])?;
    let mut offset = None;
    let mut count = None;
    let mut i = 3;
    while i < args.len() {
        let flag = extract_string(&args[i])?.to_ascii_uppercase();
        match flag.as_slice() {
            b"LIMIT" => {
                if i + 2 >= args.len() {
                    return Err(ProtocolError::WrongArity("ZREVRANGEBYLEX".into()));
                }
                offset = Some(parse_u64(&args[i + 1])? as usize);
                count = Some(parse_u64(&args[i + 2])? as usize);
                i += 2;
            }
            _ => {
                return Err(ProtocolError::InvalidData(
                    "unsupported ZREVRANGEBYLEX flag".into(),
                ))
            }
        }
        i += 1;
    }
    Ok(Command::ZRevRangeByLex {
        key,
        max,
        min,
        offset,
        count,
    })
}

fn parse_zrandmember(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("ZRANDMEMBER", args, 1)?;
    let key = extract_bytes(&args[0])?;
    if args.len() == 1 {
        return Ok(Command::ZRandMember {
            key,
            count: None,
            withscores: false,
        });
    }
    let count = Some(parse_i64(&args[1])?);
    let withscores = if args.len() >= 3 {
        let flag = extract_string(&args[2])?.to_ascii_uppercase();
        if flag != b"WITHSCORES" {
            return Err(ProtocolError::InvalidData(
                "ERR syntax error, WITHSCORES expected".into(),
            ));
        }
        true
    } else {
        false
    };
    if args.len() > 3 {
        return Err(ProtocolError::WrongArity("ZRANDMEMBER".into()));
    }
    Ok(Command::ZRandMember {
        key,
        count,
        withscores,
    })
}

fn parse_zscan(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("ZSCAN", args, 2)?;
    let key = extract_bytes(&args[0])?;
    let cursor = parse_u64(&args[1])?;
    let mut pattern = None;
    let mut count = None;
    let mut i = 2;
    while i < args.len() {
        let flag = extract_string(&args[i])?.to_ascii_uppercase();
        match flag.as_slice() {
            b"MATCH" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtocolError::WrongArity("ZSCAN".into()));
                }
                pattern = Some(String::from_utf8_lossy(&extract_bytes(&args[i])?).into_owned());
            }
            b"COUNT" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtocolError::WrongArity("ZSCAN".into()));
                }
                count = Some(parse_u64(&args[i])? as usize);
            }
            _ => {
                return Err(ProtocolError::InvalidData(format!(
                    "unsupported ZSCAN option: {}",
                    String::from_utf8_lossy(&flag)
                )));
            }
        }
        i += 1;
    }
    Ok(Command::ZScan {
        key,
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

fn extract_bytes_owned(val: RespValue) -> Result<Vec<u8>, ProtocolError> {
    match val {
        RespValue::BulkString(Some(data)) => Ok(data),
        RespValue::SimpleString(data) => Ok(data),
        RespValue::Integer(n) => Ok(n.to_string().into_bytes()),
        _ => Err(ProtocolError::InvalidData("expected bulk string".into())),
    }
}

fn is_command_name(val: &RespValue, cmd: &[u8]) -> bool {
    match val {
        RespValue::BulkString(Some(data)) | RespValue::SimpleString(data) => {
            data.eq_ignore_ascii_case(cmd)
        }
        _ => false,
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

fn parse_f32(val: &RespValue) -> Result<f32, ProtocolError> {
    match val {
        RespValue::BulkString(Some(data)) | RespValue::SimpleString(data) => {
            std::str::from_utf8(data)
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| ProtocolError::InvalidData("expected float".into()))
        }
        RespValue::Integer(n) => Ok(*n as f32),
        RespValue::Double(d) => Ok(*d as f32),
        _ => Err(ProtocolError::InvalidData("expected float".into())),
    }
}

fn parse_f64(val: &RespValue) -> Result<f64, ProtocolError> {
    match val {
        RespValue::BulkString(Some(data)) | RespValue::SimpleString(data) => {
            std::str::from_utf8(data)
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| ProtocolError::InvalidData("expected float".into()))
        }
        RespValue::Integer(n) => Ok(*n as f64),
        RespValue::Double(d) => Ok(*d),
        _ => Err(ProtocolError::InvalidData("expected float".into())),
    }
}

/// Parse a score value, supporting "-inf" and "+inf" in addition to normal floats.
fn parse_score(val: &RespValue) -> Result<f64, ProtocolError> {
    let data = extract_bytes(val)?;
    let s = std::str::from_utf8(&data)
        .map_err(|_| ProtocolError::InvalidData("expected score".into()))?;
    match s.to_lowercase().as_str() {
        "-inf" => Ok(f64::NEG_INFINITY),
        "+inf" | "inf" => Ok(f64::INFINITY),
        _ => s
            .parse::<f64>()
            .map_err(|_| ProtocolError::InvalidData("expected score".into())),
    }
}

fn check_min_arity(cmd: &str, args: &[RespValue], min: usize) -> Result<(), ProtocolError> {
    if args.len() < min {
        Err(ProtocolError::WrongArity(cmd.into()))
    } else {
        Ok(())
    }
}

fn parse_xadd(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("XADD", args, 4)?;
    let key = extract_bytes(&args[0])?;
    let mut idx = 1;
    let mut maxlen = None;

    let flag = extract_string(&args[idx])?.to_ascii_uppercase();
    if flag == b"MAXLEN" {
        idx += 1;
        if idx >= args.len() {
            return Err(ProtocolError::WrongArity("XADD".into()));
        }
        maxlen = Some(parse_u64(&args[idx])? as usize);
        idx += 1;
    }

    if idx >= args.len() {
        return Err(ProtocolError::WrongArity("XADD".into()));
    }
    let id = extract_bytes(&args[idx])?;
    idx += 1;

    let field_args = &args[idx..];
    if field_args.is_empty() || field_args.len() % 2 != 0 {
        return Err(ProtocolError::WrongArity("XADD".into()));
    }
    let fields = field_args
        .chunks(2)
        .map(|chunk| Ok((extract_bytes(&chunk[0])?, extract_bytes(&chunk[1])?)))
        .collect::<Result<Vec<(Vec<u8>, Vec<u8>)>, ProtocolError>>()?;

    Ok(Command::XAdd {
        key,
        id,
        fields,
        maxlen,
    })
}

fn parse_xrange(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("XRANGE", args, 3)?;
    let key = extract_bytes(&args[0])?;
    let start = extract_bytes(&args[1])?;
    let end = extract_bytes(&args[2])?;
    let mut count = None;
    if args.len() >= 5 {
        let flag = extract_string(&args[3])?.to_ascii_uppercase();
        if flag == b"COUNT" {
            count = Some(parse_u64(&args[4])? as usize);
        }
    }
    Ok(Command::XRange {
        key,
        start,
        end,
        count,
    })
}

fn parse_xrevrange(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("XREVRANGE", args, 3)?;
    let key = extract_bytes(&args[0])?;
    let start = extract_bytes(&args[1])?;
    let end = extract_bytes(&args[2])?;
    let mut count = None;
    if args.len() >= 5 {
        let flag = extract_string(&args[3])?.to_ascii_uppercase();
        if flag == b"COUNT" {
            count = Some(parse_u64(&args[4])? as usize);
        }
    }
    Ok(Command::XRevRange {
        key,
        start,
        end,
        count,
    })
}

fn parse_xread(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("XREAD", args, 3)?;
    let mut idx = 0;
    let mut count = None;

    while idx < args.len() {
        let token = extract_string(&args[idx])?.to_ascii_uppercase();
        match token.as_slice() {
            b"COUNT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::WrongArity("XREAD".into()));
                }
                count = Some(parse_u64(&args[idx])? as usize);
                idx += 1;
            }
            b"STREAMS" => {
                idx += 1;
                break;
            }
            _ => {
                return Err(ProtocolError::InvalidData(format!(
                    "unsupported XREAD option: {}",
                    String::from_utf8_lossy(&token)
                )));
            }
        }
    }

    let remaining = &args[idx..];
    if remaining.is_empty() || remaining.len() % 2 != 0 {
        return Err(ProtocolError::WrongArity("XREAD".into()));
    }

    let half = remaining.len() / 2;
    let keys = remaining[..half]
        .iter()
        .map(extract_bytes)
        .collect::<Result<Vec<_>, _>>()?;
    let ids = remaining[half..]
        .iter()
        .map(extract_bytes)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Command::XRead { keys, ids, count })
}

fn parse_bitcount(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("BITCOUNT", args, 1)?;
    let key = extract_bytes(&args[0])?;
    if args.len() == 1 {
        return Ok(Command::BitCount {
            key,
            start: None,
            end: None,
            use_bit: false,
        });
    }
    if args.len() < 3 {
        return Err(ProtocolError::WrongArity("BITCOUNT".into()));
    }
    let start = Some(parse_i64(&args[1])?);
    let end = Some(parse_i64(&args[2])?);
    let use_bit = if args.len() >= 4 {
        let mode = extract_string(&args[3])?.to_ascii_uppercase();
        match mode.as_slice() {
            b"BIT" => true,
            b"BYTE" => false,
            _ => {
                return Err(ProtocolError::InvalidData(
                    "ERR syntax error, BIT or BYTE expected".into(),
                ))
            }
        }
    } else {
        false
    };
    Ok(Command::BitCount {
        key,
        start,
        end,
        use_bit,
    })
}

fn parse_bitop(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("BITOP", args, 3)?;
    let op_str = extract_string(&args[0])?.to_ascii_uppercase();
    let operation = match op_str.as_slice() {
        b"AND" => BitOperation::And,
        b"OR" => BitOperation::Or,
        b"XOR" => BitOperation::Xor,
        b"NOT" => {
            if args.len() != 3 {
                return Err(ProtocolError::InvalidData(
                    "ERR BITOP NOT requires one source key".into(),
                ));
            }
            BitOperation::Not
        }
        _ => {
            return Err(ProtocolError::InvalidData(
                "ERR syntax error, AND|OR|XOR|NOT expected".into(),
            ))
        }
    };
    let destkey = extract_bytes(&args[1])?;
    let keys = args[2..]
        .iter()
        .map(extract_bytes)
        .collect::<Result<_, _>>()?;
    Ok(Command::BitOp {
        operation,
        destkey,
        keys,
    })
}

fn parse_bitpos(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("BITPOS", args, 2)?;
    let key = extract_bytes(&args[0])?;
    let bit_val = parse_u64(&args[1])?;
    if bit_val > 1 {
        return Err(ProtocolError::InvalidData(
            "ERR bit is not an integer or out of range".into(),
        ));
    }
    let bit = bit_val as u8;
    let mut start = None;
    let mut end = None;
    let mut use_bit = false;
    if args.len() >= 3 {
        start = Some(parse_i64(&args[2])?);
    }
    if args.len() >= 4 {
        end = Some(parse_i64(&args[3])?);
    }
    if args.len() >= 5 {
        let mode = extract_string(&args[4])?.to_ascii_uppercase();
        match mode.as_slice() {
            b"BIT" => use_bit = true,
            b"BYTE" => use_bit = false,
            _ => {
                return Err(ProtocolError::InvalidData(
                    "ERR syntax error, BIT or BYTE expected".into(),
                ))
            }
        }
    }
    Ok(Command::BitPos {
        key,
        bit,
        start,
        end,
        use_bit,
    })
}

fn parse_bitfield(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("BITFIELD", args, 2)?;
    let key = extract_bytes(&args[0])?;
    let mut operations = Vec::new();
    let mut i = 1usize;

    while i < args.len() {
        let subcmd = extract_string(&args[i])?.to_ascii_uppercase();
        match subcmd.as_slice() {
            b"GET" => {
                if i + 2 >= args.len() {
                    return Err(ProtocolError::WrongArity("BITFIELD".into()));
                }
                let encoding = parse_bitfield_encoding(&args[i + 1])?;
                let offset = parse_bitfield_offset(&args[i + 2])?;
                operations.push(BitFieldOperation::Get { encoding, offset });
                i += 3;
            }
            b"SET" => {
                if i + 3 >= args.len() {
                    return Err(ProtocolError::WrongArity("BITFIELD".into()));
                }
                let encoding = parse_bitfield_encoding(&args[i + 1])?;
                let offset = parse_bitfield_offset(&args[i + 2])?;
                let value = parse_i64(&args[i + 3])?;
                operations.push(BitFieldOperation::Set {
                    encoding,
                    offset,
                    value,
                });
                i += 4;
            }
            b"INCRBY" => {
                if i + 3 >= args.len() {
                    return Err(ProtocolError::WrongArity("BITFIELD".into()));
                }
                let encoding = parse_bitfield_encoding(&args[i + 1])?;
                let offset = parse_bitfield_offset(&args[i + 2])?;
                let increment = parse_i64(&args[i + 3])?;
                operations.push(BitFieldOperation::IncrBy {
                    encoding,
                    offset,
                    increment,
                });
                i += 4;
            }
            b"OVERFLOW" => {
                if i + 1 >= args.len() {
                    return Err(ProtocolError::WrongArity("BITFIELD".into()));
                }
                let mode = extract_string(&args[i + 1])?.to_ascii_uppercase();
                let overflow = match mode.as_slice() {
                    b"WRAP" => BitFieldOverflow::Wrap,
                    b"SAT" => BitFieldOverflow::Sat,
                    b"FAIL" => BitFieldOverflow::Fail,
                    _ => {
                        return Err(ProtocolError::InvalidData(
                            "ERR syntax error, WRAP|SAT|FAIL expected".into(),
                        ))
                    }
                };
                operations.push(BitFieldOperation::Overflow(overflow));
                i += 2;
            }
            _ => return Err(ProtocolError::InvalidData("ERR syntax error".into())),
        }
    }

    Ok(Command::BitField { key, operations })
}

fn parse_bitfield_encoding(arg: &RespValue) -> Result<BitFieldEncoding, ProtocolError> {
    let spec = extract_string(arg)?;
    if spec.len() < 2 {
        return Err(ProtocolError::InvalidData(
            "ERR invalid bitfield type".into(),
        ));
    }

    let signed = match spec[0] {
        b'i' | b'I' => true,
        b'u' | b'U' => false,
        _ => {
            return Err(ProtocolError::InvalidData(
                "ERR invalid bitfield type".into(),
            ))
        }
    };

    let bits = std::str::from_utf8(&spec[1..])
        .ok()
        .and_then(|s| s.parse::<u8>().ok())
        .ok_or_else(|| ProtocolError::InvalidData("ERR invalid bitfield type".into()))?;

    if bits == 0 || bits > 64 || (!signed && bits > 63) {
        return Err(ProtocolError::InvalidData(
            "ERR invalid bitfield type".into(),
        ));
    }

    Ok(BitFieldEncoding { signed, bits })
}

fn parse_bitfield_offset(arg: &RespValue) -> Result<BitFieldOffset, ProtocolError> {
    let data = extract_string(arg)?;
    if data.is_empty() {
        return Err(ProtocolError::InvalidData(
            "ERR bit offset is not an integer or out of range".into(),
        ));
    }

    if data[0] == b'#' {
        if data.len() == 1 {
            return Err(ProtocolError::InvalidData(
                "ERR bit offset is not an integer or out of range".into(),
            ));
        }
        let n = std::str::from_utf8(&data[1..])
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| {
                ProtocolError::InvalidData(
                    "ERR bit offset is not an integer or out of range".into(),
                )
            })?;
        if n < 0 {
            return Err(ProtocolError::InvalidData(
                "ERR bit offset is not an integer or out of range".into(),
            ));
        }
        Ok(BitFieldOffset::Multiplied(n))
    } else {
        let n = std::str::from_utf8(&data)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| {
                ProtocolError::InvalidData(
                    "ERR bit offset is not an integer or out of range".into(),
                )
            })?;
        if n < 0 {
            return Err(ProtocolError::InvalidData(
                "ERR bit offset is not an integer or out of range".into(),
            ));
        }
        Ok(BitFieldOffset::Absolute(n))
    }
}

fn parse_geo_unit(val: &RespValue) -> Result<GeoUnit, ProtocolError> {
    let unit_str = extract_string(val)?.to_ascii_lowercase();
    match unit_str.as_slice() {
        b"m" => Ok(GeoUnit::Meters),
        b"km" => Ok(GeoUnit::Kilometers),
        b"ft" => Ok(GeoUnit::Feet),
        b"mi" => Ok(GeoUnit::Miles),
        _ => Err(ProtocolError::InvalidData(
            "ERR unsupported unit, m|km|ft|mi expected".into(),
        )),
    }
}

fn parse_geoadd(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("GEOADD", args, 4)?;
    let key = extract_bytes(&args[0])?;
    let mut nx = false;
    let mut xx = false;
    let mut ch = false;
    let mut i = 1;

    while i < args.len() {
        let flag = extract_string(&args[i])?.to_ascii_uppercase();
        match flag.as_slice() {
            b"NX" => {
                nx = true;
                i += 1;
            }
            b"XX" => {
                xx = true;
                i += 1;
            }
            b"CH" => {
                ch = true;
                i += 1;
            }
            _ => break,
        }
    }

    let remaining = &args[i..];
    if remaining.len() < 3 || remaining.len() % 3 != 0 {
        return Err(ProtocolError::WrongArity("GEOADD".into()));
    }

    let members = remaining
        .chunks(3)
        .map(|chunk| {
            let lon = parse_f64(&chunk[0])?;
            let lat = parse_f64(&chunk[1])?;
            let member = extract_bytes(&chunk[2])?;
            Ok((lon, lat, member))
        })
        .collect::<Result<Vec<_>, ProtocolError>>()?;

    Ok(Command::GeoAdd {
        key,
        nx,
        xx,
        ch,
        members,
    })
}

fn parse_geodist(args: &[RespValue]) -> Result<Command, ProtocolError> {
    if args.len() < 3 || args.len() > 4 {
        return Err(ProtocolError::WrongArity("GEODIST".into()));
    }
    let unit = if args.len() == 4 {
        parse_geo_unit(&args[3])?
    } else {
        GeoUnit::Meters
    };
    Ok(Command::GeoDist {
        key: extract_bytes(&args[0])?,
        member1: extract_bytes(&args[1])?,
        member2: extract_bytes(&args[2])?,
        unit,
    })
}

fn parse_geosearch(args: &[RespValue]) -> Result<Command, ProtocolError> {
    check_min_arity("GEOSEARCH", args, 4)?;
    let key = extract_bytes(&args[0])?;

    let mut from_member = None;
    let mut from_lonlat = None;
    let mut radius = None;
    let mut unit = GeoUnit::Meters;
    let mut asc = None;
    let mut count = None;
    let mut withcoord = false;
    let mut withdist = false;
    let mut withhash = false;

    let mut i = 1;
    while i < args.len() {
        let flag = extract_string(&args[i])?.to_ascii_uppercase();
        match flag.as_slice() {
            b"FROMMEMBER" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtocolError::WrongArity("GEOSEARCH".into()));
                }
                from_member = Some(extract_bytes(&args[i])?);
            }
            b"FROMLONLAT" => {
                if i + 2 >= args.len() {
                    return Err(ProtocolError::WrongArity("GEOSEARCH".into()));
                }
                let lon = parse_f64(&args[i + 1])?;
                let lat = parse_f64(&args[i + 2])?;
                from_lonlat = Some((lon, lat));
                i += 2;
            }
            b"BYRADIUS" => {
                if i + 2 >= args.len() {
                    return Err(ProtocolError::WrongArity("GEOSEARCH".into()));
                }
                radius = Some(parse_f64(&args[i + 1])?);
                unit = parse_geo_unit(&args[i + 2])?;
                i += 2;
            }
            b"ASC" => asc = Some(true),
            b"DESC" => asc = Some(false),
            b"COUNT" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtocolError::WrongArity("GEOSEARCH".into()));
                }
                count = Some(parse_u64(&args[i])? as usize);
                if i + 1 < args.len() {
                    if let Ok(next_flag) = extract_string(&args[i + 1]) {
                        if next_flag.eq_ignore_ascii_case(b"ANY") {
                            i += 1;
                        }
                    }
                }
            }
            b"WITHCOORD" => withcoord = true,
            b"WITHDIST" => withdist = true,
            b"WITHHASH" => withhash = true,
            _ => {
                return Err(ProtocolError::InvalidData(format!(
                    "ERR unsupported GEOSEARCH option: {}",
                    String::from_utf8_lossy(&flag)
                )))
            }
        }
        i += 1;
    }

    if from_member.is_none() && from_lonlat.is_none() {
        return Err(ProtocolError::InvalidData(
            "ERR FROMMEMBER or FROMLONLAT required".into(),
        ));
    }
    let radius =
        radius.ok_or_else(|| ProtocolError::InvalidData("ERR BYRADIUS required".into()))?;

    Ok(Command::GeoSearch {
        key,
        from_member,
        from_lonlat,
        radius,
        unit,
        asc,
        count,
        withcoord,
        withdist,
        withhash,
    })
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

    #[test]
    fn test_parse_bgsave() {
        let cmd = parse_command(make_cmd(&[b"BGSAVE"])).unwrap();
        assert!(matches!(cmd, Command::BgSave));
    }

    #[test]
    fn test_parse_bgrewriteaof() {
        let cmd = parse_command(make_cmd(&[b"BGREWRITEAOF"])).unwrap();
        assert!(matches!(cmd, Command::BgRewriteAof));
    }

    #[test]
    fn test_parse_flushall() {
        let cmd = parse_command(make_cmd(&[b"FLUSHALL"])).unwrap();
        assert!(matches!(cmd, Command::FlushAll));
    }

    #[test]
    fn test_parse_hello() {
        let cmd = parse_command(make_cmd(&[b"HELLO", b"3"])).unwrap();
        assert!(matches!(cmd, Command::Hello { version: Some(3) }));
    }

    #[test]
    fn test_parse_hello_no_version() {
        let cmd = parse_command(make_cmd(&[b"HELLO"])).unwrap();
        assert!(matches!(cmd, Command::Hello { version: None }));
    }

    #[test]
    fn test_parse_auth_password_only() {
        let cmd = parse_command(make_cmd(&[b"AUTH", b"secret"])).unwrap();
        match cmd {
            Command::Auth { password } => {
                assert_eq!(password, b"secret");
            }
            other => panic!("Expected Auth, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_auth_with_username() {
        let cmd = parse_command(make_cmd(&[b"AUTH", b"default", b"secret"])).unwrap();
        match cmd {
            Command::Auth { password } => {
                assert_eq!(password, b"secret");
            }
            other => panic!("Expected Auth, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_cdcpoll() {
        let cmd = parse_command(make_cmd(&[b"CDCPOLL", b"0", b"100"])).unwrap();
        match cmd {
            Command::CdcPoll { cursor, count } => {
                assert_eq!(cursor, 0);
                assert_eq!(count, 100);
            }
            other => panic!("Expected CdcPoll, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_vecset() {
        let cmd = parse_command(make_cmd(&[
            b"VECSET", b"mykey", b"3", b"1.0", b"2.0", b"3.0",
        ]))
        .unwrap();
        match cmd {
            Command::VecSet {
                key,
                dimensions,
                vector,
            } => {
                assert_eq!(key, b"mykey");
                assert_eq!(dimensions, 3);
                assert_eq!(vector, vec![1.0, 2.0, 3.0]);
            }
            other => panic!("Expected VecSet, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_vecquery() {
        let cmd = parse_command(make_cmd(&[b"VECQUERY", b"mykey", b"5", b"1.0", b"2.0"])).unwrap();
        match cmd {
            Command::VecQuery { key, k, vector } => {
                assert_eq!(key, b"mykey");
                assert_eq!(k, 5);
                assert_eq!(vector, vec![1.0, 2.0]);
            }
            other => panic!("Expected VecQuery, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_vecdel() {
        let cmd = parse_command(make_cmd(&[b"VECDEL", b"mykey"])).unwrap();
        assert!(matches!(cmd, Command::VecDel { key } if key == b"mykey"));
    }

    #[test]
    fn test_parse_doc_create() {
        let cmd = parse_command(make_cmd(&[b"DOC.CREATE", b"users"])).unwrap();
        assert!(matches!(
            cmd,
            Command::DocCreate {
                collection,
                compression: None
            } if collection == b"users"
        ));

        let dictinfo = parse_command(make_cmd(&[b"DOC.DICTINFO", b"users"])).unwrap();
        assert!(matches!(
            dictinfo,
            Command::DocDictInfo { collection } if collection == b"users"
        ));

        let storage = parse_command(make_cmd(&[b"DOC.STORAGE", b"users"])).unwrap();
        assert!(matches!(
            storage,
            Command::DocStorage { collection } if collection == b"users"
        ));

        let cmd = parse_command(make_cmd(&[
            b"DOC.CREATE",
            b"users",
            b"COMPRESSION",
            b"dictionary",
        ]))
        .unwrap();
        assert!(matches!(
            cmd,
            Command::DocCreate {
                collection,
                compression: Some(profile)
            } if collection == b"users" && profile == b"dictionary"
        ));
    }

    #[test]
    fn test_parse_doc_set_get_del_exists() {
        let set = parse_command(make_cmd(&[
            b"DOC.SET",
            b"users",
            b"doc:1",
            br#"{"name":"A"}"#,
        ]))
        .unwrap();
        assert!(matches!(
            set,
            Command::DocSet {
                collection,
                doc_id,
                json
            } if collection == b"users" && doc_id == b"doc:1" && json == br#"{"name":"A"}"#
        ));

        let mset = parse_command(make_cmd(&[
            b"DOC.MSET",
            b"users",
            b"doc:1",
            br#"{"name":"A"}"#,
            b"doc:2",
            br#"{"name":"B"}"#,
        ]))
        .unwrap();
        assert!(matches!(
            mset,
            Command::DocMSet { collection, entries }
                if collection == b"users"
                    && entries == vec![
                        (b"doc:1".to_vec(), br#"{"name":"A"}"#.to_vec()),
                        (b"doc:2".to_vec(), br#"{"name":"B"}"#.to_vec())
                    ]
        ));

        let get = parse_command(make_cmd(&[b"DOC.GET", b"users", b"doc:1"])).unwrap();
        assert!(matches!(
            get,
            Command::DocGet {
                collection,
                doc_id,
                fields
            } if collection == b"users" && doc_id == b"doc:1" && fields.is_empty()
        ));

        let get_fields = parse_command(make_cmd(&[
            b"DOC.GET",
            b"users",
            b"doc:1",
            b"FIELDS",
            b"name",
            b"address.city",
        ]))
        .unwrap();
        assert!(matches!(
            get_fields,
            Command::DocGet {
                collection,
                doc_id,
                fields
            } if collection == b"users"
                && doc_id == b"doc:1"
                && fields == vec![b"name".to_vec(), b"address.city".to_vec()]
        ));

        let mget = parse_command(make_cmd(&[b"DOC.MGET", b"users", b"doc:1", b"doc:2"])).unwrap();
        assert!(matches!(
            mget,
            Command::DocMGet {
                collection,
                doc_ids
            } if collection == b"users" && doc_ids == vec![b"doc:1".to_vec(), b"doc:2".to_vec()]
        ));

        let update = parse_command(make_cmd(&[
            b"DOC.UPDATE",
            b"users",
            b"doc:1",
            b"SET",
            b"address.city",
            br#""London""#,
            b"INCR",
            b"score",
            b"1.5",
            b"PUSH",
            b"tags",
            br#""cache""#,
            b"PULL",
            b"tags",
            br#""rust""#,
            b"DEL",
            b"active",
        ]))
        .unwrap();
        assert!(matches!(
            update,
            Command::DocUpdate {
                collection,
                doc_id,
                mutations
            } if collection == b"users"
                && doc_id == b"doc:1"
                && mutations == vec![
                    DocUpdateMutation::Set {
                        path: b"address.city".to_vec(),
                        value: br#""London""#.to_vec()
                    },
                    DocUpdateMutation::Incr {
                        path: b"score".to_vec(),
                        delta: 1.5
                    },
                    DocUpdateMutation::Push {
                        path: b"tags".to_vec(),
                        value: br#""cache""#.to_vec()
                    },
                    DocUpdateMutation::Pull {
                        path: b"tags".to_vec(),
                        value: br#""rust""#.to_vec()
                    },
                    DocUpdateMutation::Del {
                        path: b"active".to_vec()
                    }
                ]
        ));

        let del = parse_command(make_cmd(&[b"DOC.DEL", b"users", b"doc:1"])).unwrap();
        assert!(matches!(
            del,
            Command::DocDel { collection, doc_id } if collection == b"users" && doc_id == b"doc:1"
        ));

        let exists = parse_command(make_cmd(&[b"DOC.EXISTS", b"users", b"doc:1"])).unwrap();
        assert!(matches!(
            exists,
            Command::DocExists { collection, doc_id } if collection == b"users" && doc_id == b"doc:1"
        ));
    }

    #[test]
    fn test_parse_doc_get_invalid_optional_clause() {
        let err =
            parse_command(make_cmd(&[b"DOC.GET", b"users", b"doc:1", b"LIMIT", b"1"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidData(_)));
    }

    #[test]
    fn test_parse_doc_mset_wrong_arity() {
        let err = parse_command(make_cmd(&[b"DOC.MSET", b"users", b"doc:1"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn test_parse_doc_update_invalid_clause() {
        let err = parse_command(make_cmd(&[
            b"DOC.UPDATE",
            b"users",
            b"doc:1",
            b"RENAME",
            b"name",
            b"full_name",
        ]))
        .unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidData(_)));
    }

    #[test]
    fn test_parse_doc_find_project_requires_fields() {
        let err = parse_command(make_cmd(&[
            b"DOC.FIND",
            b"users",
            b"WHERE",
            b"city",
            b"=",
            br#""Accra""#,
            b"PROJECT",
        ]))
        .unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn test_parse_command_info() {
        let cmd = parse_command(make_cmd(&[b"COMMAND"])).unwrap();
        assert!(matches!(cmd, Command::CommandInfo { names } if names.is_empty()));
    }

    #[test]
    fn test_parse_command_subcommands() {
        let cmd = parse_command(make_cmd(&[b"COMMAND", b"LIST"])).unwrap();
        assert!(matches!(cmd, Command::CommandList));

        let cmd = parse_command(make_cmd(&[b"COMMAND", b"INFO", b"GET", b"SET"])).unwrap();
        assert!(
            matches!(cmd, Command::CommandInfo { names } if names == vec![b"GET".to_vec(), b"SET".to_vec()])
        );

        let cmd = parse_command(make_cmd(&[b"COMMAND", b"DOCS", b"GET"])).unwrap();
        assert!(matches!(cmd, Command::CommandDocs { names } if names == vec![b"GET".to_vec()]));

        let cmd = parse_command(make_cmd(&[b"COMMAND", b"HELP"])).unwrap();
        assert!(matches!(cmd, Command::CommandHelp));
    }

    #[test]
    fn test_parse_command_unknown_subcommand() {
        let err = parse_command(make_cmd(&[b"COMMAND", b"BOGUS"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidData(_)));
    }

    #[test]
    fn test_parse_dump() {
        let cmd = parse_command(make_cmd(&[b"DUMP"])).unwrap();
        assert!(matches!(cmd, Command::Dump));
    }

    #[test]
    fn test_parse_object_freq() {
        let cmd = parse_command(make_cmd(&[b"OBJECT", b"FREQ", b"mykey"])).unwrap();
        assert!(matches!(cmd, Command::ObjectFreq { key } if key == b"mykey"));
    }

    #[test]
    fn test_parse_object_encoding() {
        let cmd = parse_command(make_cmd(&[b"OBJECT", b"ENCODING", b"mykey"])).unwrap();
        assert!(matches!(cmd, Command::ObjectEncoding { key } if key == b"mykey"));
    }

    #[test]
    fn test_parse_object_refcount() {
        let cmd = parse_command(make_cmd(&[b"OBJECT", b"REFCOUNT", b"mykey"])).unwrap();
        assert!(matches!(cmd, Command::ObjectRefCount { key } if key == b"mykey"));
    }

    #[test]
    fn test_parse_object_idletime() {
        let cmd = parse_command(make_cmd(&[b"OBJECT", b"IDLETIME", b"mykey"])).unwrap();
        assert!(matches!(cmd, Command::ObjectIdleTime { key } if key == b"mykey"));
    }

    #[test]
    fn test_parse_object_help() {
        let cmd = parse_command(make_cmd(&[b"OBJECT", b"HELP"])).unwrap();
        assert!(matches!(cmd, Command::ObjectHelp));
    }

    #[test]
    fn test_parse_object_unknown_subcmd() {
        let result = parse_command(make_cmd(&[b"OBJECT", b"BADCMD", b"mykey"]));
        assert!(matches!(result, Err(ProtocolError::InvalidData(_))));
    }

    #[test]
    fn test_parse_zadd() {
        let cmd =
            parse_command(make_cmd(&[b"ZADD", b"myset", b"1.5", b"a", b"2.5", b"b"])).unwrap();
        match cmd {
            Command::ZAdd { key, members } => {
                assert_eq!(key, b"myset");
                assert_eq!(members.len(), 2);
                assert!((members[0].0 - 1.5).abs() < f64::EPSILON);
                assert_eq!(members[0].1, b"a");
                assert!((members[1].0 - 2.5).abs() < f64::EPSILON);
                assert_eq!(members[1].1, b"b");
            }
            other => panic!("Expected ZAdd, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_zscore() {
        let cmd = parse_command(make_cmd(&[b"ZSCORE", b"myset", b"member1"])).unwrap();
        assert!(
            matches!(cmd, Command::ZScore { key, member } if key == b"myset" && member == b"member1")
        );
    }

    #[test]
    fn test_parse_zrange_withscores() {
        let cmd =
            parse_command(make_cmd(&[b"ZRANGE", b"myset", b"0", b"-1", b"WITHSCORES"])).unwrap();
        match cmd {
            Command::ZRange {
                key,
                start,
                stop,
                withscores,
            } => {
                assert_eq!(key, b"myset");
                assert_eq!(start, 0);
                assert_eq!(stop, -1);
                assert!(withscores);
            }
            other => panic!("Expected ZRange, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_zcount_inf() {
        let cmd = parse_command(make_cmd(&[b"ZCOUNT", b"myset", b"-inf", b"+inf"])).unwrap();
        match cmd {
            Command::ZCount { key, min, max } => {
                assert_eq!(key, b"myset");
                assert!(min.is_infinite() && min.is_sign_negative());
                assert!(max.is_infinite() && max.is_sign_positive());
            }
            other => panic!("Expected ZCount, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_bitfield_get_set_incrby() {
        let cmd = parse_command(make_cmd(&[
            b"BITFIELD",
            b"bits",
            b"GET",
            b"u8",
            b"0",
            b"SET",
            b"i5",
            b"#1",
            b"3",
            b"INCRBY",
            b"u4",
            b"8",
            b"2",
            b"OVERFLOW",
            b"SAT",
            b"INCRBY",
            b"u4",
            b"8",
            b"100",
        ]))
        .unwrap();

        match cmd {
            Command::BitField { key, operations } => {
                assert_eq!(key, b"bits");
                assert_eq!(operations.len(), 5);
                assert!(matches!(
                    operations[0],
                    BitFieldOperation::Get {
                        encoding: BitFieldEncoding {
                            signed: false,
                            bits: 8
                        },
                        offset: BitFieldOffset::Absolute(0)
                    }
                ));
                assert!(matches!(
                    operations[1],
                    BitFieldOperation::Set {
                        encoding: BitFieldEncoding {
                            signed: true,
                            bits: 5
                        },
                        offset: BitFieldOffset::Multiplied(1),
                        value: 3
                    }
                ));
                assert!(matches!(
                    operations[2],
                    BitFieldOperation::IncrBy {
                        encoding: BitFieldEncoding {
                            signed: false,
                            bits: 4
                        },
                        offset: BitFieldOffset::Absolute(8),
                        increment: 2
                    }
                ));
                assert!(matches!(
                    operations[3],
                    BitFieldOperation::Overflow(BitFieldOverflow::Sat)
                ));
            }
            other => panic!("Expected BitField, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_bitfield_invalid_type() {
        let result = parse_command(make_cmd(&[b"BITFIELD", b"bits", b"GET", b"u0", b"0"]));
        assert!(matches!(result, Err(ProtocolError::InvalidData(_))));
    }

    #[test]
    fn test_parse_bitfield_negative_offset() {
        let result = parse_command(make_cmd(&[b"BITFIELD", b"bits", b"GET", b"u8", b"-1"]));
        assert!(matches!(result, Err(ProtocolError::InvalidData(_))));
    }

    #[test]
    fn test_parse_xadd() {
        let cmd = parse_command(make_cmd(&[
            b"XADD",
            b"mystream",
            b"*",
            b"field1",
            b"value1",
            b"field2",
            b"value2",
        ]))
        .unwrap();
        match cmd {
            Command::XAdd {
                key,
                id,
                fields,
                maxlen,
            } => {
                assert_eq!(key, b"mystream");
                assert_eq!(id, b"*");
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0], (b"field1".to_vec(), b"value1".to_vec()));
                assert!(maxlen.is_none());
            }
            other => panic!("Expected XAdd, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_xadd_with_maxlen() {
        let cmd = parse_command(make_cmd(&[
            b"XADD",
            b"mystream",
            b"MAXLEN",
            b"1000",
            b"*",
            b"k",
            b"v",
        ]))
        .unwrap();
        match cmd {
            Command::XAdd {
                maxlen: Some(1000),
                id,
                fields,
                ..
            } => {
                assert_eq!(id, b"*");
                assert_eq!(fields.len(), 1);
            }
            other => panic!("Expected XAdd with MAXLEN, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_xlen() {
        let cmd = parse_command(make_cmd(&[b"XLEN", b"mystream"])).unwrap();
        assert!(matches!(cmd, Command::XLen { key } if key == b"mystream"));
    }

    #[test]
    fn test_parse_xrange() {
        let cmd = parse_command(make_cmd(&[b"XRANGE", b"mystream", b"-", b"+"])).unwrap();
        match cmd {
            Command::XRange {
                key,
                start,
                end,
                count,
            } => {
                assert_eq!(key, b"mystream");
                assert_eq!(start, b"-");
                assert_eq!(end, b"+");
                assert!(count.is_none());
            }
            other => panic!("Expected XRange, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_xrange_with_count() {
        let cmd = parse_command(make_cmd(&[
            b"XRANGE",
            b"mystream",
            b"1000-0",
            b"2000-0",
            b"COUNT",
            b"10",
        ]))
        .unwrap();
        match cmd {
            Command::XRange {
                count: Some(10), ..
            } => {}
            other => panic!("Expected XRange with COUNT, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_xrevrange() {
        let cmd = parse_command(make_cmd(&[b"XREVRANGE", b"mystream", b"+", b"-"])).unwrap();
        match cmd {
            Command::XRevRange {
                key,
                start,
                end,
                count,
            } => {
                assert_eq!(key, b"mystream");
                assert_eq!(start, b"+");
                assert_eq!(end, b"-");
                assert!(count.is_none());
            }
            other => panic!("Expected XRevRange, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_xread() {
        let cmd = parse_command(make_cmd(&[
            b"XREAD", b"COUNT", b"5", b"STREAMS", b"stream1", b"stream2", b"0-0", b"0-0",
        ]))
        .unwrap();
        match cmd {
            Command::XRead { keys, ids, count } => {
                assert_eq!(keys.len(), 2);
                assert_eq!(keys[0], b"stream1");
                assert_eq!(keys[1], b"stream2");
                assert_eq!(ids.len(), 2);
                assert_eq!(count, Some(5));
            }
            other => panic!("Expected XRead, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_xread_no_count() {
        let cmd = parse_command(make_cmd(&[b"XREAD", b"STREAMS", b"mystream", b"0-0"])).unwrap();
        match cmd {
            Command::XRead { keys, ids, count } => {
                assert_eq!(keys.len(), 1);
                assert_eq!(ids.len(), 1);
                assert!(count.is_none());
            }
            other => panic!("Expected XRead, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_xtrim() {
        let cmd = parse_command(make_cmd(&[b"XTRIM", b"mystream", b"MAXLEN", b"100"])).unwrap();
        match cmd {
            Command::XTrim { key, maxlen } => {
                assert_eq!(key, b"mystream");
                assert_eq!(maxlen, 100);
            }
            other => panic!("Expected XTrim, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_subscribe() {
        let cmd = parse_command(make_cmd(&[b"SUBSCRIBE", b"ch1", b"ch2"])).unwrap();
        match cmd {
            Command::Subscribe { channels } => {
                assert_eq!(channels.len(), 2);
                assert_eq!(channels[0], b"ch1");
                assert_eq!(channels[1], b"ch2");
            }
            other => panic!("Expected Subscribe, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_subscribe_requires_channel() {
        let result = parse_command(make_cmd(&[b"SUBSCRIBE"]));
        assert!(matches!(result, Err(ProtocolError::WrongArity(_))));
    }

    #[test]
    fn test_parse_unsubscribe_with_channels() {
        let cmd = parse_command(make_cmd(&[b"UNSUBSCRIBE", b"ch1", b"ch2"])).unwrap();
        match cmd {
            Command::Unsubscribe { channels } => {
                assert_eq!(channels.len(), 2);
                assert_eq!(channels[0], b"ch1");
                assert_eq!(channels[1], b"ch2");
            }
            other => panic!("Expected Unsubscribe, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_unsubscribe_no_args() {
        let cmd = parse_command(make_cmd(&[b"UNSUBSCRIBE"])).unwrap();
        match cmd {
            Command::Unsubscribe { channels } => {
                assert!(channels.is_empty());
            }
            other => panic!("Expected Unsubscribe, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_psubscribe() {
        let cmd = parse_command(make_cmd(&[b"PSUBSCRIBE", b"ch.*", b"news.*"])).unwrap();
        match cmd {
            Command::PSubscribe { patterns } => {
                assert_eq!(patterns.len(), 2);
                assert_eq!(patterns[0], b"ch.*");
                assert_eq!(patterns[1], b"news.*");
            }
            other => panic!("Expected PSubscribe, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_psubscribe_requires_pattern() {
        let result = parse_command(make_cmd(&[b"PSUBSCRIBE"]));
        assert!(matches!(result, Err(ProtocolError::WrongArity(_))));
    }

    #[test]
    fn test_parse_punsubscribe() {
        let cmd = parse_command(make_cmd(&[b"PUNSUBSCRIBE", b"ch.*"])).unwrap();
        match cmd {
            Command::PUnsubscribe { patterns } => {
                assert_eq!(patterns.len(), 1);
                assert_eq!(patterns[0], b"ch.*");
            }
            other => panic!("Expected PUnsubscribe, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_punsubscribe_no_args() {
        let cmd = parse_command(make_cmd(&[b"PUNSUBSCRIBE"])).unwrap();
        match cmd {
            Command::PUnsubscribe { patterns } => {
                assert!(patterns.is_empty());
            }
            other => panic!("Expected PUnsubscribe, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_publish() {
        let cmd = parse_command(make_cmd(&[b"PUBLISH", b"mychannel", b"hello world"])).unwrap();
        match cmd {
            Command::Publish { channel, message } => {
                assert_eq!(channel, b"mychannel");
                assert_eq!(message, b"hello world");
            }
            other => panic!("Expected Publish, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_publish_wrong_arity() {
        let result = parse_command(make_cmd(&[b"PUBLISH", b"mychannel"]));
        assert!(matches!(result, Err(ProtocolError::WrongArity(_))));
    }
}
