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
        b"FLUSHDB" => Ok(Command::FlushDb),

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

        // Sorted set commands
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
        b"COMMAND" => Ok(Command::CommandInfo),

        // Server / admin commands
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
            if args.len() == 1 {
                Ok(Command::Auth {
                    tenant: None,
                    password: extract_bytes(&args[0])?,
                })
            } else {
                Ok(Command::Auth {
                    tenant: Some(extract_bytes(&args[0])?),
                    password: extract_bytes(&args[1])?,
                })
            }
        }

        // CDC commands
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

        // Vector commands
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

        // Scripting commands
        b"SCRIPTLOAD" => {
            check_arity("SCRIPTLOAD", args, 2)?;
            Ok(Command::ScriptLoad {
                name: extract_bytes(&args[0])?,
                wasm_bytes: extract_bytes(&args[1])?,
            })
        }
        b"SCRIPTCALL" => {
            check_min_arity("SCRIPTCALL", args, 1)?;
            let name = extract_bytes(&args[0])?;
            let byte_args: Vec<Vec<u8>> = args[1..]
                .iter()
                .map(extract_bytes)
                .collect::<Result<_, _>>()?;
            let int_args: Vec<i64> = byte_args
                .iter()
                .filter_map(|b| std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()))
                .collect();
            Ok(Command::ScriptCall {
                name,
                args: int_args,
                byte_args,
            })
        }
        b"SCRIPTDEL" => {
            check_arity("SCRIPTDEL", args, 1)?;
            Ok(Command::ScriptDel {
                name: extract_bytes(&args[0])?,
            })
        }

        // Stats commands
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

        // Object commands
        b"OBJECT" => {
            check_min_arity("OBJECT", args, 2)?;
            let subcmd = extract_string(&args[0])?.to_ascii_uppercase();
            match subcmd.as_slice() {
                b"FREQ" => Ok(Command::ObjectFreq {
                    key: extract_bytes(&args[1])?,
                }),
                b"ENCODING" => Ok(Command::ObjectEncoding {
                    key: extract_bytes(&args[1])?,
                }),
                _ => Err(ProtocolError::InvalidData(format!(
                    "unknown OBJECT subcommand: {}",
                    String::from_utf8_lossy(&subcmd)
                ))),
            }
        }

        _ => Err(ProtocolError::UnknownCommand(
            String::from_utf8_lossy(&cmd_name).into_owned(),
        )),
    }
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
            Command::Auth { tenant, password } => {
                assert!(tenant.is_none());
                assert_eq!(password, b"secret");
            }
            other => panic!("Expected Auth, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_auth_with_tenant() {
        let cmd = parse_command(make_cmd(&[b"AUTH", b"tenant1", b"secret"])).unwrap();
        match cmd {
            Command::Auth { tenant, password } => {
                assert_eq!(tenant.unwrap(), b"tenant1");
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
    fn test_parse_scriptload() {
        let cmd = parse_command(make_cmd(&[b"SCRIPTLOAD", b"myfn", b"wasmdata"])).unwrap();
        match cmd {
            Command::ScriptLoad { name, wasm_bytes } => {
                assert_eq!(name, b"myfn");
                assert_eq!(wasm_bytes, b"wasmdata");
            }
            other => panic!("Expected ScriptLoad, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_scriptcall() {
        let cmd = parse_command(make_cmd(&[b"SCRIPTCALL", b"myfn", b"42", b"7"])).unwrap();
        match cmd {
            Command::ScriptCall {
                name,
                args,
                byte_args,
            } => {
                assert_eq!(name, b"myfn");
                assert_eq!(args, vec![42, 7]);
                assert_eq!(byte_args, vec![b"42".to_vec(), b"7".to_vec()]);
            }
            other => panic!("Expected ScriptCall, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_scriptdel() {
        let cmd = parse_command(make_cmd(&[b"SCRIPTDEL", b"myfn"])).unwrap();
        assert!(matches!(cmd, Command::ScriptDel { name } if name == b"myfn"));
    }

    #[test]
    fn test_parse_command_info() {
        let cmd = parse_command(make_cmd(&[b"COMMAND"])).unwrap();
        assert!(matches!(cmd, Command::CommandInfo));
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
    fn test_parse_object_unknown_subcmd() {
        let result = parse_command(make_cmd(&[b"OBJECT", b"HELP", b"mykey"]));
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
}
