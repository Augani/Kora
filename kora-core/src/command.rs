//! Command and response types for the Kōra engine.
//!
//! These types define the interface between the protocol layer and the shard engine.

use std::time::Duration;

/// A parsed command ready for execution by a shard.
#[derive(Debug, Clone)]
pub enum Command {
    // -- String commands --
    /// GET key
    Get {
        /// The key to retrieve.
        key: Vec<u8>,
    },
    /// SET key value \[EX seconds\] \[PX millis\] \[NX|XX\]
    Set {
        /// The key.
        key: Vec<u8>,
        /// The value.
        value: Vec<u8>,
        /// Optional TTL in seconds.
        ex: Option<u64>,
        /// Optional TTL in milliseconds.
        px: Option<u64>,
        /// Only set if key does not exist.
        nx: bool,
        /// Only set if key already exists.
        xx: bool,
    },
    /// GETSET key value (deprecated but supported)
    GetSet {
        /// The key.
        key: Vec<u8>,
        /// The new value.
        value: Vec<u8>,
    },
    /// APPEND key value
    Append {
        /// The key.
        key: Vec<u8>,
        /// The value to append.
        value: Vec<u8>,
    },
    /// STRLEN key
    Strlen {
        /// The key.
        key: Vec<u8>,
    },
    /// INCR key
    Incr {
        /// The key.
        key: Vec<u8>,
    },
    /// DECR key
    Decr {
        /// The key.
        key: Vec<u8>,
    },
    /// INCRBY key delta
    IncrBy {
        /// The key.
        key: Vec<u8>,
        /// The increment amount.
        delta: i64,
    },
    /// DECRBY key delta
    DecrBy {
        /// The key.
        key: Vec<u8>,
        /// The decrement amount.
        delta: i64,
    },
    /// MGET key \[key ...\]
    MGet {
        /// The keys to retrieve.
        keys: Vec<Vec<u8>>,
    },
    /// MSET key value \[key value ...\]
    MSet {
        /// Key-value pairs to set.
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /// SETNX key value
    SetNx {
        /// The key.
        key: Vec<u8>,
        /// The value.
        value: Vec<u8>,
    },

    // -- Key commands --
    /// DEL key \[key ...\]
    Del {
        /// The keys to delete.
        keys: Vec<Vec<u8>>,
    },
    /// EXISTS key \[key ...\]
    Exists {
        /// The keys to check.
        keys: Vec<Vec<u8>>,
    },
    /// EXPIRE key seconds
    Expire {
        /// The key.
        key: Vec<u8>,
        /// TTL in seconds.
        seconds: u64,
    },
    /// PEXPIRE key milliseconds
    PExpire {
        /// The key.
        key: Vec<u8>,
        /// TTL in milliseconds.
        millis: u64,
    },
    /// PERSIST key
    Persist {
        /// The key.
        key: Vec<u8>,
    },
    /// TTL key
    Ttl {
        /// The key.
        key: Vec<u8>,
    },
    /// PTTL key
    PTtl {
        /// The key.
        key: Vec<u8>,
    },
    /// TYPE key
    Type {
        /// The key.
        key: Vec<u8>,
    },
    /// KEYS pattern
    Keys {
        /// Glob pattern to match.
        pattern: String,
    },
    /// SCAN cursor \[MATCH pattern\] \[COUNT count\]
    Scan {
        /// The cursor position.
        cursor: u64,
        /// Optional glob pattern filter.
        pattern: Option<String>,
        /// Optional count hint.
        count: Option<usize>,
    },
    /// DBSIZE
    DbSize,
    /// FLUSHDB
    FlushDb,

    // -- List commands --
    /// LPUSH key value \[value ...\]
    LPush {
        /// The key.
        key: Vec<u8>,
        /// Values to push.
        values: Vec<Vec<u8>>,
    },
    /// RPUSH key value \[value ...\]
    RPush {
        /// The key.
        key: Vec<u8>,
        /// Values to push.
        values: Vec<Vec<u8>>,
    },
    /// LPOP key
    LPop {
        /// The key.
        key: Vec<u8>,
    },
    /// RPOP key
    RPop {
        /// The key.
        key: Vec<u8>,
    },
    /// LLEN key
    LLen {
        /// The key.
        key: Vec<u8>,
    },
    /// LRANGE key start stop
    LRange {
        /// The key.
        key: Vec<u8>,
        /// Start index (0-based, negative from end).
        start: i64,
        /// Stop index (inclusive, negative from end).
        stop: i64,
    },
    /// LINDEX key index
    LIndex {
        /// The key.
        key: Vec<u8>,
        /// The index (negative from end).
        index: i64,
    },

    // -- Hash commands --
    /// HSET key field value \[field value ...\]
    HSet {
        /// The key.
        key: Vec<u8>,
        /// Field-value pairs.
        fields: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /// HGET key field
    HGet {
        /// The key.
        key: Vec<u8>,
        /// The field name.
        field: Vec<u8>,
    },
    /// HDEL key field \[field ...\]
    HDel {
        /// The key.
        key: Vec<u8>,
        /// Fields to delete.
        fields: Vec<Vec<u8>>,
    },
    /// HGETALL key
    HGetAll {
        /// The key.
        key: Vec<u8>,
    },
    /// HLEN key
    HLen {
        /// The key.
        key: Vec<u8>,
    },
    /// HEXISTS key field
    HExists {
        /// The key.
        key: Vec<u8>,
        /// The field name.
        field: Vec<u8>,
    },
    /// HINCRBY key field increment
    HIncrBy {
        /// The key.
        key: Vec<u8>,
        /// The field name.
        field: Vec<u8>,
        /// The increment amount.
        delta: i64,
    },

    // -- Set commands --
    /// SADD key member \[member ...\]
    SAdd {
        /// The key.
        key: Vec<u8>,
        /// Members to add.
        members: Vec<Vec<u8>>,
    },
    /// SREM key member \[member ...\]
    SRem {
        /// The key.
        key: Vec<u8>,
        /// Members to remove.
        members: Vec<Vec<u8>>,
    },
    /// SMEMBERS key
    SMembers {
        /// The key.
        key: Vec<u8>,
    },
    /// SISMEMBER key member
    SIsMember {
        /// The key.
        key: Vec<u8>,
        /// The member to check.
        member: Vec<u8>,
    },
    /// SCARD key
    SCard {
        /// The key.
        key: Vec<u8>,
    },

    // -- Server commands --
    /// PING \[message\]
    Ping {
        /// Optional message to echo back.
        message: Option<Vec<u8>>,
    },
    /// ECHO message
    Echo {
        /// The message to echo.
        message: Vec<u8>,
    },
    /// INFO \[section\]
    Info {
        /// Optional section filter.
        section: Option<String>,
    },
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
#[derive(Debug, Clone, PartialEq)]
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
