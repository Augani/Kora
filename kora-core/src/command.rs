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
    /// BGSAVE — trigger a background RDB snapshot.
    BgSave,
    /// BGREWRITEAOF — trigger a WAL rewrite.
    BgRewriteAof,
    /// FLUSHALL — flush all databases.
    FlushAll,
    /// COMMAND — returns info about commands (stub).
    CommandInfo,
    /// HELLO \[protover\] — protocol version negotiation (RESP3).
    Hello {
        /// Requested protocol version (2 or 3).
        version: Option<u8>,
    },
    /// AUTH \[tenant\] password — authenticate / set tenant.
    Auth {
        /// Optional tenant identifier.
        tenant: Option<Vec<u8>>,
        /// Password.
        password: Vec<u8>,
    },
    /// DUMP — extract all key-value entries from a shard (internal).
    Dump,

    // -- CDC commands --
    /// CDCPOLL cursor count — poll CDC events from a shard.
    CdcPoll {
        /// Cursor position (sequence number).
        cursor: u64,
        /// Maximum events to return.
        count: usize,
    },
    /// CDC.GROUP CREATE key group start\_seq — create a consumer group.
    CdcGroupCreate {
        /// The CDC stream key.
        key: Vec<u8>,
        /// Consumer group name.
        group: String,
        /// Starting sequence number.
        start_seq: u64,
    },
    /// CDC.GROUP READ key group consumer count — read from a consumer group.
    CdcGroupRead {
        /// The CDC stream key.
        key: Vec<u8>,
        /// Consumer group name.
        group: String,
        /// Consumer name within the group.
        consumer: String,
        /// Maximum events to return.
        count: usize,
    },
    /// CDC.ACK key group seq \[seq ...\] — acknowledge events.
    CdcAck {
        /// The CDC stream key.
        key: Vec<u8>,
        /// Consumer group name.
        group: String,
        /// Sequence numbers to acknowledge.
        seqs: Vec<u64>,
    },
    /// CDC.PENDING key group — list pending entries.
    CdcPending {
        /// The CDC stream key.
        key: Vec<u8>,
        /// Consumer group name.
        group: String,
    },

    // -- Vector commands --
    /// VECSET key dim v1 v2 ... — store a vector.
    VecSet {
        /// The key.
        key: Vec<u8>,
        /// Vector dimensions.
        dimensions: usize,
        /// The vector components.
        vector: Vec<f32>,
    },
    /// VECQUERY key k v1 v2 ... — query nearest neighbors.
    VecQuery {
        /// The index key.
        key: Vec<u8>,
        /// Number of neighbors.
        k: usize,
        /// Query vector.
        vector: Vec<f32>,
    },
    /// VECDEL key — delete a vector.
    VecDel {
        /// The key.
        key: Vec<u8>,
    },

    // -- Scripting commands --
    /// SCRIPTLOAD name wasm\_bytes — load a WASM module.
    ScriptLoad {
        /// Function name.
        name: Vec<u8>,
        /// WASM module bytes.
        wasm_bytes: Vec<u8>,
    },
    /// SCRIPTCALL name \[args...\] — call a loaded WASM function.
    ScriptCall {
        /// Function name.
        name: Vec<u8>,
        /// Integer arguments (for numeric-only calls).
        args: Vec<i64>,
        /// Byte arguments (for calls that pass binary data via WASM linear memory).
        byte_args: Vec<Vec<u8>>,
    },
    /// SCRIPTDEL name — unload a WASM module.
    ScriptDel {
        /// Function name.
        name: Vec<u8>,
    },

    // -- Sorted Set commands --
    /// ZADD key score member \[score member ...\]
    ZAdd {
        /// The key.
        key: Vec<u8>,
        /// Score-member pairs.
        members: Vec<(f64, Vec<u8>)>,
    },
    /// ZREM key member \[member ...\]
    ZRem {
        /// The key.
        key: Vec<u8>,
        /// Members to remove.
        members: Vec<Vec<u8>>,
    },
    /// ZSCORE key member
    ZScore {
        /// The key.
        key: Vec<u8>,
        /// The member.
        member: Vec<u8>,
    },
    /// ZRANK key member
    ZRank {
        /// The key.
        key: Vec<u8>,
        /// The member.
        member: Vec<u8>,
    },
    /// ZREVRANK key member
    ZRevRank {
        /// The key.
        key: Vec<u8>,
        /// The member.
        member: Vec<u8>,
    },
    /// ZCARD key
    ZCard {
        /// The key.
        key: Vec<u8>,
    },
    /// ZRANGE key start stop \[WITHSCORES\]
    ZRange {
        /// The key.
        key: Vec<u8>,
        /// Start index (0-based, negative from end).
        start: i64,
        /// Stop index (inclusive, negative from end).
        stop: i64,
        /// Whether to include scores in output.
        withscores: bool,
    },
    /// ZREVRANGE key start stop \[WITHSCORES\]
    ZRevRange {
        /// The key.
        key: Vec<u8>,
        /// Start index (0-based, negative from end).
        start: i64,
        /// Stop index (inclusive, negative from end).
        stop: i64,
        /// Whether to include scores in output.
        withscores: bool,
    },
    /// ZRANGEBYSCORE key min max \[WITHSCORES\] \[LIMIT offset count\]
    ZRangeByScore {
        /// The key.
        key: Vec<u8>,
        /// Minimum score (inclusive).
        min: f64,
        /// Maximum score (inclusive).
        max: f64,
        /// Whether to include scores in output.
        withscores: bool,
        /// Optional offset for LIMIT.
        offset: Option<usize>,
        /// Optional count for LIMIT.
        count: Option<usize>,
    },
    /// ZINCRBY key increment member
    ZIncrBy {
        /// The key.
        key: Vec<u8>,
        /// The increment amount.
        delta: f64,
        /// The member.
        member: Vec<u8>,
    },
    /// ZCOUNT key min max
    ZCount {
        /// The key.
        key: Vec<u8>,
        /// Minimum score (inclusive).
        min: f64,
        /// Maximum score (inclusive).
        max: f64,
    },

    // -- Object commands --
    /// OBJECT FREQ key — return the LFU frequency counter.
    ObjectFreq {
        /// The key.
        key: Vec<u8>,
    },
    /// OBJECT ENCODING key — return the encoding type.
    ObjectEncoding {
        /// The key.
        key: Vec<u8>,
    },

    // -- Stats commands --
    /// STATS.HOTKEYS count — return top hot keys.
    StatsHotkeys {
        /// Number of hot keys to return.
        count: usize,
    },
    /// STATS.LATENCY command p1 p2 ... — return latency percentiles.
    StatsLatency {
        /// Command name (e.g. "GET").
        command: Vec<u8>,
        /// Percentile values to return (e.g. \[50.0, 99.0, 99.9\]).
        percentiles: Vec<f64>,
    },
    /// STATS.MEMORY prefix ... — return memory usage for key prefixes.
    StatsMemory {
        /// Key prefixes to query.
        prefixes: Vec<Vec<u8>>,
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
            | Command::SCard { key }
            | Command::ZAdd { key, .. }
            | Command::ZRem { key, .. }
            | Command::ZScore { key, .. }
            | Command::ZRank { key, .. }
            | Command::ZRevRank { key, .. }
            | Command::ZCard { key }
            | Command::ZRange { key, .. }
            | Command::ZRevRange { key, .. }
            | Command::ZRangeByScore { key, .. }
            | Command::ZIncrBy { key, .. }
            | Command::ZCount { key, .. }
            | Command::VecSet { key, .. }
            | Command::VecDel { key }
            | Command::ObjectFreq { key }
            | Command::ObjectEncoding { key } => Some(key),
            Command::VecQuery { .. } => None,
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
                | Command::FlushAll
                | Command::Keys { .. }
                | Command::Scan { .. }
                | Command::BgSave
                | Command::BgRewriteAof
                | Command::CommandInfo
                | Command::Hello { .. }
                | Command::Auth { .. }
                | Command::Dump
                | Command::CdcPoll { .. }
                | Command::CdcGroupCreate { .. }
                | Command::CdcGroupRead { .. }
                | Command::CdcAck { .. }
                | Command::CdcPending { .. }
                | Command::ScriptLoad { .. }
                | Command::ScriptCall { .. }
                | Command::ScriptDel { .. }
                | Command::StatsHotkeys { .. }
                | Command::StatsLatency { .. }
                | Command::StatsMemory { .. }
        )
    }

    /// Returns true if this command mutates data (for WAL logging).
    pub fn is_mutation(&self) -> bool {
        matches!(
            self,
            Command::Set { .. }
                | Command::GetSet { .. }
                | Command::Append { .. }
                | Command::Incr { .. }
                | Command::Decr { .. }
                | Command::IncrBy { .. }
                | Command::DecrBy { .. }
                | Command::MSet { .. }
                | Command::SetNx { .. }
                | Command::Del { .. }
                | Command::Expire { .. }
                | Command::PExpire { .. }
                | Command::Persist { .. }
                | Command::FlushDb
                | Command::FlushAll
                | Command::LPush { .. }
                | Command::RPush { .. }
                | Command::LPop { .. }
                | Command::RPop { .. }
                | Command::HSet { .. }
                | Command::HDel { .. }
                | Command::HIncrBy { .. }
                | Command::SAdd { .. }
                | Command::SRem { .. }
                | Command::ZAdd { .. }
                | Command::ZRem { .. }
                | Command::ZIncrBy { .. }
                | Command::VecSet { .. }
                | Command::VecDel { .. }
        )
    }

    /// Return a numeric type index for stats tracking.
    pub fn cmd_type(&self) -> u8 {
        match self {
            Command::Get { .. } => 0,
            Command::Set { .. } => 1,
            Command::Del { .. } => 2,
            Command::Incr { .. } | Command::IncrBy { .. } => 3,
            Command::Decr { .. } | Command::DecrBy { .. } => 4,
            Command::MGet { .. } => 5,
            Command::MSet { .. } => 6,
            Command::Exists { .. } => 7,
            Command::Expire { .. } | Command::PExpire { .. } => 8,
            Command::Ttl { .. } | Command::PTtl { .. } => 9,
            Command::LPush { .. } => 10,
            Command::RPush { .. } => 11,
            Command::LPop { .. } => 12,
            Command::RPop { .. } => 13,
            Command::HSet { .. } => 14,
            Command::HGet { .. } => 15,
            Command::HGetAll { .. } => 16,
            Command::SAdd { .. } => 17,
            Command::SRem { .. } => 18,
            Command::Ping { .. } => 19,
            Command::Info { .. } => 20,
            Command::DbSize => 21,
            Command::FlushDb | Command::FlushAll => 22,
            Command::Keys { .. } | Command::Scan { .. } => 23,
            Command::VecSet { .. } => 24,
            Command::VecQuery { .. } => 25,
            Command::ScriptCall { .. } => 26,
            Command::ZAdd { .. } => 27,
            Command::ZRange { .. } | Command::ZRevRange { .. } | Command::ZRangeByScore { .. } => {
                28
            }
            Command::ZScore { .. }
            | Command::ZRank { .. }
            | Command::ZRevRank { .. }
            | Command::ZCard { .. }
            | Command::ZCount { .. } => 29,
            Command::ZRem { .. } | Command::ZIncrBy { .. } => 30,
            Command::ObjectFreq { .. } | Command::ObjectEncoding { .. } => 31,
            _ => 32,
        }
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
    /// RESP3 Map (key-value pairs)
    Map(Vec<(CommandResponse, CommandResponse)>),
    /// RESP3 Set (unique values)
    Set(Vec<CommandResponse>),
    /// RESP3 Double (floating-point)
    Double(f64),
    /// RESP3 Boolean
    Boolean(bool),
}
