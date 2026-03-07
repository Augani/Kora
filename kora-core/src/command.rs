//! Command and response types for the Kōra engine.
//!
//! These types define the interface between the protocol layer and the shard engine.

use std::time::Duration;

/// A parsed command ready for execution by a shard.
#[derive(Debug, Clone)]
pub enum Command {
    // -- String commands --
    /// GET key
    Get { key: Vec<u8> },
    /// SET key value [EX seconds] [PX millis] [NX|XX]
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        ex: Option<u64>,
        px: Option<u64>,
        nx: bool,
        xx: bool,
    },
    /// GETSET key value (deprecated but supported)
    GetSet { key: Vec<u8>, value: Vec<u8> },
    /// APPEND key value
    Append { key: Vec<u8>, value: Vec<u8> },
    /// STRLEN key
    Strlen { key: Vec<u8> },
    /// INCR key
    Incr { key: Vec<u8> },
    /// DECR key
    Decr { key: Vec<u8> },
    /// INCRBY key delta
    IncrBy { key: Vec<u8>, delta: i64 },
    /// DECRBY key delta
    DecrBy { key: Vec<u8>, delta: i64 },
    /// MGET key [key ...]
    MGet { keys: Vec<Vec<u8>> },
    /// MSET key value [key value ...]
    MSet { entries: Vec<(Vec<u8>, Vec<u8>)> },
    /// SETNX key value
    SetNx { key: Vec<u8>, value: Vec<u8> },

    // -- Key commands --
    /// DEL key [key ...]
    Del { keys: Vec<Vec<u8>> },
    /// EXISTS key [key ...]
    Exists { keys: Vec<Vec<u8>> },
    /// EXPIRE key seconds
    Expire { key: Vec<u8>, seconds: u64 },
    /// PEXPIRE key milliseconds
    PExpire { key: Vec<u8>, millis: u64 },
    /// PERSIST key
    Persist { key: Vec<u8> },
    /// TTL key
    Ttl { key: Vec<u8> },
    /// PTTL key
    PTtl { key: Vec<u8> },
    /// TYPE key
    Type { key: Vec<u8> },
    /// KEYS pattern
    Keys { pattern: String },
    /// SCAN cursor [MATCH pattern] [COUNT count]
    Scan {
        cursor: u64,
        pattern: Option<String>,
        count: Option<usize>,
    },
    /// DBSIZE
    DbSize,
    /// FLUSHDB
    FlushDb,

    // -- List commands --
    /// LPUSH key value [value ...]
    LPush { key: Vec<u8>, values: Vec<Vec<u8>> },
    /// RPUSH key value [value ...]
    RPush { key: Vec<u8>, values: Vec<Vec<u8>> },
    /// LPOP key
    LPop { key: Vec<u8> },
    /// RPOP key
    RPop { key: Vec<u8> },
    /// LLEN key
    LLen { key: Vec<u8> },
    /// LRANGE key start stop
    LRange { key: Vec<u8>, start: i64, stop: i64 },
    /// LINDEX key index
    LIndex { key: Vec<u8>, index: i64 },

    // -- Hash commands --
    /// HSET key field value [field value ...]
    HSet {
        key: Vec<u8>,
        fields: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /// HGET key field
    HGet { key: Vec<u8>, field: Vec<u8> },
    /// HDEL key field [field ...]
    HDel { key: Vec<u8>, fields: Vec<Vec<u8>> },
    /// HGETALL key
    HGetAll { key: Vec<u8> },
    /// HLEN key
    HLen { key: Vec<u8> },
    /// HEXISTS key field
    HExists { key: Vec<u8>, field: Vec<u8> },
    /// HINCRBY key field increment
    HIncrBy {
        key: Vec<u8>,
        field: Vec<u8>,
        delta: i64,
    },

    // -- Set commands --
    /// SADD key member [member ...]
    SAdd { key: Vec<u8>, members: Vec<Vec<u8>> },
    /// SREM key member [member ...]
    SRem { key: Vec<u8>, members: Vec<Vec<u8>> },
    /// SMEMBERS key
    SMembers { key: Vec<u8> },
    /// SISMEMBER key member
    SIsMember { key: Vec<u8>, member: Vec<u8> },
    /// SCARD key
    SCard { key: Vec<u8> },

    // -- Server commands --
    /// PING [message]
    Ping { message: Option<Vec<u8>> },
    /// ECHO message
    Echo { message: Vec<u8> },
    /// INFO [section]
    Info { section: Option<String> },
}

impl Command {
    /// Extract the primary key for routing, if this is a single-key command.
    pub fn key(&self) -> Option<&[u8]> {
        match self {
            Command::Get { key }
            | Command::Set { key, .. }
            | Command::GetSet { key, .. }
            | Command::Append { key, .. }
            | Command::Strlen { key }
            | Command::Incr { key }
            | Command::Decr { key }
            | Command::IncrBy { key, .. }
            | Command::DecrBy { key, .. }
            | Command::SetNx { key, .. }
            | Command::Expire { key, .. }
            | Command::PExpire { key, .. }
            | Command::Persist { key }
            | Command::Ttl { key }
            | Command::PTtl { key }
            | Command::Type { key }
            | Command::LPush { key, .. }
            | Command::RPush { key, .. }
            | Command::LPop { key }
            | Command::RPop { key }
            | Command::LLen { key }
            | Command::LRange { key, .. }
            | Command::LIndex { key, .. }
            | Command::HSet { key, .. }
            | Command::HGet { key, .. }
            | Command::HDel { key, .. }
            | Command::HGetAll { key }
            | Command::HLen { key }
            | Command::HExists { key, .. }
            | Command::HIncrBy { key, .. }
            | Command::SAdd { key, .. }
            | Command::SRem { key, .. }
            | Command::SMembers { key }
            | Command::SIsMember { key, .. }
            | Command::SCard { key } => Some(key),
            _ => None,
        }
    }

    /// Returns true if this command operates on multiple keys that may span shards.
    pub fn is_multi_key(&self) -> bool {
        matches!(
            self,
            Command::MGet { .. }
                | Command::MSet { .. }
                | Command::Del { .. }
                | Command::Exists { .. }
        )
    }

    /// Returns true if this is a keyless/server command.
    pub fn is_keyless(&self) -> bool {
        matches!(
            self,
            Command::Ping { .. }
                | Command::Echo { .. }
                | Command::Info { .. }
                | Command::DbSize
                | Command::FlushDb
                | Command::Keys { .. }
                | Command::Scan { .. }
        )
    }

    /// Get the TTL duration from EX/PX options.
    pub fn ttl_duration(ex: Option<u64>, px: Option<u64>) -> Option<Duration> {
        if let Some(s) = ex {
            Some(Duration::from_secs(s))
        } else {
            px.map(Duration::from_millis)
        }
    }
}

/// Response from executing a command on a shard.
#[derive(Debug, Clone)]
pub enum CommandResponse {
    /// +OK
    Ok,
    /// Null bulk string ($-1)
    Nil,
    /// Integer reply
    Integer(i64),
    /// Bulk string
    BulkString(Vec<u8>),
    /// Simple string (without the +)
    SimpleString(String),
    /// Array of responses
    Array(Vec<CommandResponse>),
    /// Error message
    Error(String),
}
