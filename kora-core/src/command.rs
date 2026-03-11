//! Command and response types for the Kōra engine.
//!
//! `Command` is the parsed, type-safe representation of every operation the
//! engine supports — strings, lists, sets, hashes, sorted sets, streams,
//! HyperLogLog, bitmaps, geo, vectors, pub/sub, transactions, and server
//! management. The protocol crate parses raw RESP frames into `Command`
//! variants, and the shard engine pattern-matches on them for execution.
//!
//! `CommandResponse` is the corresponding output type. Variants map directly
//! to RESP wire types (simple string, error, integer, bulk string, array, nil)
//! plus a `BulkStringShared` variant that allows zero-copy responses via
//! `Arc<[u8]>`.
//!
//! Helper methods on `Command` (`key()`, `is_multi_key()`, `is_mutation()`,
//! `cmd_type()`) drive the engine's routing and WAL logic.

use std::sync::Arc;
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
    /// INCRBYFLOAT key delta
    IncrByFloat {
        /// The key.
        key: Vec<u8>,
        /// The float increment amount.
        delta: f64,
    },
    /// GETRANGE key start end
    GetRange {
        /// The key.
        key: Vec<u8>,
        /// Start offset.
        start: i64,
        /// End offset (inclusive).
        end: i64,
    },
    /// SETRANGE key offset value
    SetRange {
        /// The key.
        key: Vec<u8>,
        /// Byte offset.
        offset: usize,
        /// Value to write at offset.
        value: Vec<u8>,
    },
    /// GETDEL key
    GetDel {
        /// The key.
        key: Vec<u8>,
    },
    /// GETEX key \[EX seconds | PX millis | EXAT timestamp | PXAT timestamp\_ms | PERSIST\]
    GetEx {
        /// The key.
        key: Vec<u8>,
        /// Optional TTL in seconds.
        ex: Option<u64>,
        /// Optional TTL in milliseconds.
        px: Option<u64>,
        /// Optional absolute Unix timestamp (seconds).
        exat: Option<u64>,
        /// Optional absolute Unix timestamp (milliseconds).
        pxat: Option<u64>,
        /// Whether to remove TTL.
        persist: bool,
    },
    /// MSETNX key value \[key value ...\]
    MSetNx {
        /// Key-value pairs to set.
        entries: Vec<(Vec<u8>, Vec<u8>)>,
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
    /// EXPIREAT key timestamp
    ExpireAt {
        /// The key.
        key: Vec<u8>,
        /// Absolute Unix timestamp in seconds.
        timestamp: u64,
    },
    /// PEXPIREAT key timestamp\_ms
    PExpireAt {
        /// The key.
        key: Vec<u8>,
        /// Absolute Unix timestamp in milliseconds.
        timestamp_ms: u64,
    },
    /// RENAME key newkey
    Rename {
        /// The source key.
        key: Vec<u8>,
        /// The destination key.
        newkey: Vec<u8>,
    },
    /// RENAMENX key newkey
    RenameNx {
        /// The source key.
        key: Vec<u8>,
        /// The destination key.
        newkey: Vec<u8>,
    },
    /// UNLINK key \[key ...\]
    Unlink {
        /// The keys to unlink.
        keys: Vec<Vec<u8>>,
    },
    /// COPY source destination \[REPLACE\]
    Copy {
        /// The source key.
        source: Vec<u8>,
        /// The destination key.
        destination: Vec<u8>,
        /// Whether to overwrite the destination.
        replace: bool,
    },
    /// RANDOMKEY
    RandomKey,
    /// TOUCH key \[key ...\]
    Touch {
        /// The keys to touch.
        keys: Vec<Vec<u8>>,
    },
    /// OBJECT REFCOUNT key
    ObjectRefCount {
        /// The key.
        key: Vec<u8>,
    },
    /// OBJECT IDLETIME key
    ObjectIdleTime {
        /// The key.
        key: Vec<u8>,
    },
    /// OBJECT HELP
    ObjectHelp,
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
    /// LSET key index value
    LSet {
        /// The key.
        key: Vec<u8>,
        /// The index.
        index: i64,
        /// The value to set.
        value: Vec<u8>,
    },
    /// LINSERT key BEFORE|AFTER pivot value
    LInsert {
        /// The key.
        key: Vec<u8>,
        /// Insert before pivot if true, after if false.
        before: bool,
        /// The pivot element.
        pivot: Vec<u8>,
        /// The value to insert.
        value: Vec<u8>,
    },
    /// LREM key count value
    LRem {
        /// The key.
        key: Vec<u8>,
        /// Count: >0 from head, <0 from tail, 0 = all.
        count: i64,
        /// The value to remove.
        value: Vec<u8>,
    },
    /// LTRIM key start stop
    LTrim {
        /// The key.
        key: Vec<u8>,
        /// Start index.
        start: i64,
        /// Stop index (inclusive).
        stop: i64,
    },
    /// LPOS key value \[RANK rank\] \[COUNT count\] \[MAXLEN len\]
    LPos {
        /// The key.
        key: Vec<u8>,
        /// The value to search for.
        value: Vec<u8>,
        /// Rank parameter (skip N matches).
        rank: Option<i64>,
        /// Count parameter (return N positions, 0 = all).
        count: Option<i64>,
        /// Maximum elements to scan.
        maxlen: Option<i64>,
    },
    /// RPOPLPUSH source destination
    RPopLPush {
        /// The source key.
        source: Vec<u8>,
        /// The destination key.
        destination: Vec<u8>,
    },
    /// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
    LMove {
        /// The source key.
        source: Vec<u8>,
        /// The destination key.
        destination: Vec<u8>,
        /// Pop from left side of source if true, right if false.
        from_left: bool,
        /// Push to left side of destination if true, right if false.
        to_left: bool,
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
    /// HMGET key field \[field ...\]
    HMGet {
        /// The key.
        key: Vec<u8>,
        /// Fields to retrieve.
        fields: Vec<Vec<u8>>,
    },
    /// HKEYS key
    HKeys {
        /// The key.
        key: Vec<u8>,
    },
    /// HVALS key
    HVals {
        /// The key.
        key: Vec<u8>,
    },
    /// HSETNX key field value
    HSetNx {
        /// The key.
        key: Vec<u8>,
        /// The field name.
        field: Vec<u8>,
        /// The value.
        value: Vec<u8>,
    },
    /// HINCRBYFLOAT key field delta
    HIncrByFloat {
        /// The key.
        key: Vec<u8>,
        /// The field name.
        field: Vec<u8>,
        /// The float increment amount.
        delta: f64,
    },
    /// HRANDFIELD key \[count \[WITHVALUES\]\]
    HRandField {
        /// The key.
        key: Vec<u8>,
        /// Optional count (negative = allow duplicates).
        count: Option<i64>,
        /// Whether to include values.
        withvalues: bool,
    },
    /// HSCAN key cursor \[MATCH pattern\] \[COUNT count\]
    HScan {
        /// The key.
        key: Vec<u8>,
        /// The cursor position.
        cursor: u64,
        /// Optional glob pattern filter.
        pattern: Option<String>,
        /// Optional count hint.
        count: Option<usize>,
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
    /// SPOP key \[count\]
    SPop {
        /// The key.
        key: Vec<u8>,
        /// Optional count of members to pop.
        count: Option<usize>,
    },
    /// SRANDMEMBER key \[count\]
    SRandMember {
        /// The key.
        key: Vec<u8>,
        /// Optional count (negative = allow duplicates).
        count: Option<i64>,
    },
    /// SUNION key \[key ...\]
    SUnion {
        /// The keys of the sets.
        keys: Vec<Vec<u8>>,
    },
    /// SUNIONSTORE destination key \[key ...\]
    SUnionStore {
        /// The destination key.
        destination: Vec<u8>,
        /// The source set keys.
        keys: Vec<Vec<u8>>,
    },
    /// SINTER key \[key ...\]
    SInter {
        /// The keys of the sets.
        keys: Vec<Vec<u8>>,
    },
    /// SINTERSTORE destination key \[key ...\]
    SInterStore {
        /// The destination key.
        destination: Vec<u8>,
        /// The source set keys.
        keys: Vec<Vec<u8>>,
    },
    /// SDIFF key \[key ...\]
    SDiff {
        /// The keys of the sets.
        keys: Vec<Vec<u8>>,
    },
    /// SDIFFSTORE destination key \[key ...\]
    SDiffStore {
        /// The destination key.
        destination: Vec<u8>,
        /// The source set keys.
        keys: Vec<Vec<u8>>,
    },
    /// SINTERCARD numkeys key \[key ...\] \[LIMIT limit\]
    SInterCard {
        /// Number of keys.
        numkeys: usize,
        /// The keys of the sets.
        keys: Vec<Vec<u8>>,
        /// Optional limit on the count.
        limit: Option<usize>,
    },
    /// SMOVE source destination member
    SMove {
        /// The source key.
        source: Vec<u8>,
        /// The destination key.
        destination: Vec<u8>,
        /// The member to move.
        member: Vec<u8>,
    },
    /// SMISMEMBER key member \[member ...\]
    SMisMember {
        /// The key.
        key: Vec<u8>,
        /// Members to check.
        members: Vec<Vec<u8>>,
    },
    /// SSCAN key cursor \[MATCH pattern\] \[COUNT count\]
    SScan {
        /// The key.
        key: Vec<u8>,
        /// The cursor position.
        cursor: u64,
        /// Optional glob pattern filter.
        pattern: Option<String>,
        /// Optional count hint.
        count: Option<usize>,
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
    /// COMMAND \[INFO \[command ...\]\] — returns command metadata.
    CommandInfo {
        /// Optional command names to fetch metadata for. Empty means all commands.
        names: Vec<Vec<u8>>,
    },
    /// HELLO \[protover\] — protocol version negotiation (RESP3).
    Hello {
        /// Requested protocol version (2 or 3).
        version: Option<u8>,
    },
    /// AUTH password — authenticate the connection.
    Auth {
        /// Password.
        password: Vec<u8>,
    },
    /// DUMP — extract all key-value entries from a shard (internal).
    Dump,

    // -- Transaction commands --
    /// MULTI — start a transaction block.
    Multi,
    /// EXEC — execute all queued commands in a transaction.
    Exec,
    /// DISCARD — discard all queued commands in a transaction.
    Discard,
    /// WATCH key \[key ...\] — optimistic locking (accepted, not enforced).
    Watch {
        /// Keys to watch.
        keys: Vec<Vec<u8>>,
    },
    /// UNWATCH — clear all watched keys.
    Unwatch,

    // -- Server commands (Phase 5) --
    /// CONFIG GET pattern
    ConfigGet {
        /// Glob pattern for config keys.
        pattern: String,
    },
    /// CONFIG SET parameter value
    ConfigSet {
        /// Config parameter name.
        parameter: String,
        /// Config value.
        value: String,
    },
    /// CONFIG RESETSTAT
    ConfigResetStat,
    /// CLIENT ID — return connection unique ID.
    ClientId,
    /// CLIENT GETNAME — return connection name.
    ClientGetName,
    /// CLIENT SETNAME name — set connection name.
    ClientSetName {
        /// The connection name.
        name: Vec<u8>,
    },
    /// CLIENT LIST — list all connections.
    ClientList,
    /// CLIENT INFO — current connection info.
    ClientInfo,
    /// TIME — return server time.
    Time,
    /// SELECT db — select database (only db 0 supported).
    Select {
        /// Database index.
        db: i64,
    },
    /// QUIT — close connection gracefully.
    Quit,
    /// WAIT numreplicas timeout — wait for replication acknowledgements.
    Wait {
        /// Number of replicas to wait for.
        numreplicas: i64,
        /// Timeout in milliseconds.
        timeout: i64,
    },
    /// COMMAND COUNT — return number of supported commands.
    CommandCount,
    /// COMMAND LIST — return the names of supported commands.
    CommandList,
    /// COMMAND HELP — return command help text.
    CommandHelp,
    /// COMMAND DOCS \[command ...\] — return command documentation metadata.
    CommandDocs {
        /// Optional command names to fetch docs for. Empty means all commands.
        names: Vec<Vec<u8>>,
    },

    // -- Document commands --
    /// DOC.CREATE collection \[COMPRESSION profile\] — create a collection.
    DocCreate {
        /// Collection name.
        collection: Vec<u8>,
        /// Optional compression profile name.
        compression: Option<Vec<u8>>,
    },
    /// DOC.DROP collection — drop a collection.
    DocDrop {
        /// Collection name.
        collection: Vec<u8>,
    },
    /// DOC.INFO collection — collection metadata.
    DocInfo {
        /// Collection name.
        collection: Vec<u8>,
    },
    /// DOC.DICTINFO collection — dictionary statistics for a collection.
    DocDictInfo {
        /// Collection name.
        collection: Vec<u8>,
    },
    /// DOC.STORAGE collection — storage statistics for a collection.
    DocStorage {
        /// Collection name.
        collection: Vec<u8>,
    },
    /// DOC.SET collection doc_id json — insert or replace a document.
    DocSet {
        /// Collection name.
        collection: Vec<u8>,
        /// External document ID.
        doc_id: Vec<u8>,
        /// JSON payload bytes.
        json: Vec<u8>,
    },
    /// DOC.INSERT collection json — insert with auto-generated ID.
    DocInsert {
        /// Collection name.
        collection: Vec<u8>,
        /// JSON payload bytes.
        json: Vec<u8>,
    },
    /// DOC.MSET collection doc_id json \[doc_id json ...\] — batch insert/replace.
    DocMSet {
        /// Collection name.
        collection: Vec<u8>,
        /// Batch entries as `(doc_id, json_payload)`.
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /// DOC.GET collection doc_id \[FIELDS path ...\] — fetch a document.
    DocGet {
        /// Collection name.
        collection: Vec<u8>,
        /// External document ID.
        doc_id: Vec<u8>,
        /// Optional projection paths. Empty means full document.
        fields: Vec<Vec<u8>>,
    },
    /// DOC.MGET collection doc_id \[doc_id ...\] — batch fetch.
    DocMGet {
        /// Collection name.
        collection: Vec<u8>,
        /// External document IDs.
        doc_ids: Vec<Vec<u8>>,
    },
    /// DOC.UPDATE collection doc_id <mutation...> — apply field-level mutations.
    DocUpdate {
        /// Collection name.
        collection: Vec<u8>,
        /// External document ID.
        doc_id: Vec<u8>,
        /// Mutation operations to apply in order.
        mutations: Vec<DocUpdateMutation>,
    },
    /// DOC.DEL collection doc_id — delete a document.
    DocDel {
        /// Collection name.
        collection: Vec<u8>,
        /// External document ID.
        doc_id: Vec<u8>,
    },
    /// DOC.EXISTS collection doc_id — check document existence.
    DocExists {
        /// Collection name.
        collection: Vec<u8>,
        /// External document ID.
        doc_id: Vec<u8>,
    },
    /// DOC.CREATEINDEX collection field type — create a secondary index.
    DocCreateIndex {
        /// Collection name.
        collection: Vec<u8>,
        /// Dotted field path.
        field: Vec<u8>,
        /// Index type string (hash, sorted, array, unique).
        index_type: Vec<u8>,
    },
    /// DOC.DROPINDEX collection field — drop a secondary index.
    DocDropIndex {
        /// Collection name.
        collection: Vec<u8>,
        /// Dotted field path.
        field: Vec<u8>,
    },
    /// DOC.INDEXES collection — list indexes.
    DocIndexes {
        /// Collection name.
        collection: Vec<u8>,
    },
    /// DOC.FIND collection WHERE expr \[ORDER BY field \[ASC|DESC\]\] \[PROJECT f1 f2 ...\] \[LIMIT n\] \[OFFSET n\]
    DocFind {
        /// Collection name.
        collection: Vec<u8>,
        /// WHERE expression string.
        where_clause: Vec<u8>,
        /// Optional projection field paths.
        fields: Vec<Vec<u8>>,
        /// Optional result limit.
        limit: Option<usize>,
        /// Result offset (default 0).
        offset: usize,
        /// Optional field path to sort results by.
        order_by: Option<Vec<u8>>,
        /// True if sort order is descending (default ascending).
        order_desc: bool,
    },
    /// DOC.COUNT collection WHERE expr
    DocCount {
        /// Collection name.
        collection: Vec<u8>,
        /// WHERE expression string.
        where_clause: Vec<u8>,
    },

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
    /// ZREVRANGEBYSCORE key max min \[WITHSCORES\] \[LIMIT offset count\]
    ZRevRangeByScore {
        /// The key.
        key: Vec<u8>,
        /// Maximum score.
        max: f64,
        /// Minimum score.
        min: f64,
        /// Whether to include scores in output.
        withscores: bool,
        /// Optional offset for LIMIT.
        offset: Option<usize>,
        /// Optional count for LIMIT.
        count: Option<usize>,
    },
    /// ZPOPMIN key \[count\]
    ZPopMin {
        /// The key.
        key: Vec<u8>,
        /// Optional count of members to pop.
        count: Option<usize>,
    },
    /// ZPOPMAX key \[count\]
    ZPopMax {
        /// The key.
        key: Vec<u8>,
        /// Optional count of members to pop.
        count: Option<usize>,
    },
    /// ZRANGEBYLEX key min max \[LIMIT offset count\]
    ZRangeByLex {
        /// The key.
        key: Vec<u8>,
        /// Lexicographic minimum (e.g. "-", "\[a", "(a").
        min: Vec<u8>,
        /// Lexicographic maximum (e.g. "+", "\[z", "(z").
        max: Vec<u8>,
        /// Optional offset for LIMIT.
        offset: Option<usize>,
        /// Optional count for LIMIT.
        count: Option<usize>,
    },
    /// ZREVRANGEBYLEX key max min \[LIMIT offset count\]
    ZRevRangeByLex {
        /// The key.
        key: Vec<u8>,
        /// Lexicographic maximum.
        max: Vec<u8>,
        /// Lexicographic minimum.
        min: Vec<u8>,
        /// Optional offset for LIMIT.
        offset: Option<usize>,
        /// Optional count for LIMIT.
        count: Option<usize>,
    },
    /// ZLEXCOUNT key min max
    ZLexCount {
        /// The key.
        key: Vec<u8>,
        /// Lexicographic minimum.
        min: Vec<u8>,
        /// Lexicographic maximum.
        max: Vec<u8>,
    },
    /// ZMSCORE key member \[member ...\]
    ZMScore {
        /// The key.
        key: Vec<u8>,
        /// Members to query.
        members: Vec<Vec<u8>>,
    },
    /// ZRANDMEMBER key \[count \[WITHSCORES\]\]
    ZRandMember {
        /// The key.
        key: Vec<u8>,
        /// Optional count (negative = allow duplicates).
        count: Option<i64>,
        /// Whether to include scores.
        withscores: bool,
    },
    /// ZSCAN key cursor \[MATCH pattern\] \[COUNT count\]
    ZScan {
        /// The key.
        key: Vec<u8>,
        /// The cursor position.
        cursor: u64,
        /// Optional glob pattern filter.
        pattern: Option<String>,
        /// Optional count hint.
        count: Option<usize>,
    },

    // -- Stream commands --
    /// XADD key \[MAXLEN count\] id field value \[field value ...\]
    XAdd {
        /// The key.
        key: Vec<u8>,
        /// The entry ID (usually "*" for auto-generate).
        id: Vec<u8>,
        /// Field-value pairs for the entry.
        fields: Vec<(Vec<u8>, Vec<u8>)>,
        /// Optional MAXLEN trimming threshold.
        maxlen: Option<usize>,
    },
    /// XLEN key
    XLen {
        /// The key.
        key: Vec<u8>,
    },
    /// XRANGE key start end \[COUNT count\]
    XRange {
        /// The key.
        key: Vec<u8>,
        /// Start ID (or "-" for minimum).
        start: Vec<u8>,
        /// End ID (or "+" for maximum).
        end: Vec<u8>,
        /// Optional maximum number of entries to return.
        count: Option<usize>,
    },
    /// XREVRANGE key end start \[COUNT count\]
    XRevRange {
        /// The key.
        key: Vec<u8>,
        /// Start ID (or "+" for maximum, reversed semantics).
        start: Vec<u8>,
        /// End ID (or "-" for minimum, reversed semantics).
        end: Vec<u8>,
        /// Optional maximum number of entries to return.
        count: Option<usize>,
    },
    /// XREAD \[COUNT count\] STREAMS key \[key ...\] id \[id ...\]
    XRead {
        /// Keys to read from.
        keys: Vec<Vec<u8>>,
        /// IDs to read after (one per key).
        ids: Vec<Vec<u8>>,
        /// Optional maximum number of entries per key.
        count: Option<usize>,
    },
    /// XTRIM key MAXLEN count
    XTrim {
        /// The key.
        key: Vec<u8>,
        /// Maximum number of entries to keep.
        maxlen: usize,
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

    // -- Pub/Sub commands --
    /// SUBSCRIBE channel \[channel ...\]
    Subscribe {
        /// Channels to subscribe to.
        channels: Vec<Vec<u8>>,
    },
    /// UNSUBSCRIBE \[channel ...\]
    Unsubscribe {
        /// Channels to unsubscribe from (empty = all).
        channels: Vec<Vec<u8>>,
    },
    /// PSUBSCRIBE pattern \[pattern ...\]
    PSubscribe {
        /// Glob patterns to subscribe to.
        patterns: Vec<Vec<u8>>,
    },
    /// PUNSUBSCRIBE \[pattern ...\]
    PUnsubscribe {
        /// Glob patterns to unsubscribe from (empty = all).
        patterns: Vec<Vec<u8>>,
    },
    /// PUBLISH channel message
    Publish {
        /// The target channel.
        channel: Vec<u8>,
        /// The message payload.
        message: Vec<u8>,
    },

    // -- HyperLogLog commands --
    /// PFADD key element \[element ...\]
    PfAdd {
        /// The key.
        key: Vec<u8>,
        /// Elements to add to the HyperLogLog.
        elements: Vec<Vec<u8>>,
    },
    /// PFCOUNT key \[key ...\]
    PfCount {
        /// The keys to count (union cardinality when multiple).
        keys: Vec<Vec<u8>>,
    },
    /// PFMERGE destkey sourcekey \[sourcekey ...\]
    PfMerge {
        /// The destination key.
        destkey: Vec<u8>,
        /// The source keys to merge from.
        sourcekeys: Vec<Vec<u8>>,
    },

    // -- Bitmap commands --
    /// SETBIT key offset value
    SetBit {
        /// The key.
        key: Vec<u8>,
        /// The bit offset.
        offset: u64,
        /// The bit value (0 or 1).
        value: u8,
    },
    /// GETBIT key offset
    GetBit {
        /// The key.
        key: Vec<u8>,
        /// The bit offset.
        offset: u64,
    },
    /// BITCOUNT key \[start end \[BYTE|BIT\]\]
    BitCount {
        /// The key.
        key: Vec<u8>,
        /// Optional start offset.
        start: Option<i64>,
        /// Optional end offset.
        end: Option<i64>,
        /// True if offsets are in bits, false (default) for bytes.
        use_bit: bool,
    },
    /// BITOP AND|OR|XOR|NOT destkey key \[key ...\]
    BitOp {
        /// The bitwise operation.
        operation: BitOperation,
        /// The destination key.
        destkey: Vec<u8>,
        /// The source keys.
        keys: Vec<Vec<u8>>,
    },
    /// BITPOS key bit \[start \[end \[BYTE|BIT\]\]\]
    BitPos {
        /// The key.
        key: Vec<u8>,
        /// The bit value to search for (0 or 1).
        bit: u8,
        /// Optional start offset.
        start: Option<i64>,
        /// Optional end offset.
        end: Option<i64>,
        /// True if offsets are in bits, false (default) for bytes.
        use_bit: bool,
    },
    /// BITFIELD key \[GET|SET|INCRBY|OVERFLOW ...\]
    BitField {
        /// The key.
        key: Vec<u8>,
        /// Ordered BITFIELD operations to execute.
        operations: Vec<BitFieldOperation>,
    },

    // -- Geo commands --
    /// GEOADD key \[NX|XX\] \[CH\] longitude latitude member \[...\]
    GeoAdd {
        /// The key.
        key: Vec<u8>,
        /// Only add new members, don't update existing.
        nx: bool,
        /// Only update existing members, don't add new.
        xx: bool,
        /// Return number of changed elements instead of added.
        ch: bool,
        /// Longitude, latitude, member triples.
        members: Vec<(f64, f64, Vec<u8>)>,
    },
    /// GEODIST key member1 member2 \[m|km|ft|mi\]
    GeoDist {
        /// The key.
        key: Vec<u8>,
        /// First member.
        member1: Vec<u8>,
        /// Second member.
        member2: Vec<u8>,
        /// Distance unit (default: meters).
        unit: GeoUnit,
    },
    /// GEOHASH key member \[member ...\]
    GeoHash {
        /// The key.
        key: Vec<u8>,
        /// Members to get geohash strings for.
        members: Vec<Vec<u8>>,
    },
    /// GEOPOS key member \[member ...\]
    GeoPos {
        /// The key.
        key: Vec<u8>,
        /// Members to get positions for.
        members: Vec<Vec<u8>>,
    },
    /// GEOSEARCH key FROMMEMBER member|FROMLONLAT lon lat BYRADIUS radius unit \[ASC|DESC\] \[COUNT count\] \[WITHCOORD\] \[WITHDIST\] \[WITHHASH\]
    GeoSearch {
        /// The key.
        key: Vec<u8>,
        /// Member to search from (mutually exclusive with from\_lonlat).
        from_member: Option<Vec<u8>>,
        /// Longitude/latitude to search from (mutually exclusive with from\_member).
        from_lonlat: Option<(f64, f64)>,
        /// Search radius.
        radius: f64,
        /// Radius unit.
        unit: GeoUnit,
        /// Sort ascending if true, descending if false, unsorted if None.
        asc: Option<bool>,
        /// Maximum number of results.
        count: Option<usize>,
        /// Include coordinates in response.
        withcoord: bool,
        /// Include distance in response.
        withdist: bool,
        /// Include geohash in response.
        withhash: bool,
    },

    // -- Blocking operations --
    /// BLPOP key \[key ...\] timeout
    BLPop {
        /// Keys to pop from (tried in order).
        keys: Vec<Vec<u8>>,
        /// Timeout in seconds (0 = try once).
        timeout: f64,
    },
    /// BRPOP key \[key ...\] timeout
    BRPop {
        /// Keys to pop from (tried in order).
        keys: Vec<Vec<u8>>,
        /// Timeout in seconds (0 = try once).
        timeout: f64,
    },
    /// BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
    BLMove {
        /// The source key.
        source: Vec<u8>,
        /// The destination key.
        destination: Vec<u8>,
        /// Pop from left side of source if true, right if false.
        from_left: bool,
        /// Push to left side of destination if true, right if false.
        to_left: bool,
        /// Timeout in seconds (0 = try once).
        timeout: f64,
    },
    /// BZPOPMIN key \[key ...\] timeout
    BZPopMin {
        /// Keys to pop from (tried in order).
        keys: Vec<Vec<u8>>,
        /// Timeout in seconds (0 = try once).
        timeout: f64,
    },
    /// BZPOPMAX key \[key ...\] timeout
    BZPopMax {
        /// Keys to pop from (tried in order).
        keys: Vec<Vec<u8>>,
        /// Timeout in seconds (0 = try once).
        timeout: f64,
    },

    // -- Stream consumer group commands --
    /// XGROUP CREATE key group id \[MKSTREAM\]
    XGroupCreate {
        /// The stream key.
        key: Vec<u8>,
        /// Consumer group name.
        group: String,
        /// Starting ID ("$" or "0" or specific ID).
        id: String,
        /// Create stream if it does not exist.
        mkstream: bool,
    },
    /// XGROUP DESTROY key group
    XGroupDestroy {
        /// The stream key.
        key: Vec<u8>,
        /// Consumer group name.
        group: String,
    },
    /// XGROUP DELCONSUMER key group consumer
    XGroupDelConsumer {
        /// The stream key.
        key: Vec<u8>,
        /// Consumer group name.
        group: String,
        /// Consumer name.
        consumer: String,
    },
    /// XREADGROUP GROUP group consumer \[COUNT count\] STREAMS key \[key ...\] id \[id ...\]
    XReadGroup {
        /// Consumer group name.
        group: String,
        /// Consumer name.
        consumer: String,
        /// Optional max entries per key.
        count: Option<usize>,
        /// Keys to read from.
        keys: Vec<Vec<u8>>,
        /// IDs to read after (one per key, ">" for new).
        ids: Vec<Vec<u8>>,
    },
    /// XACK key group id \[id ...\]
    XAck {
        /// The stream key.
        key: Vec<u8>,
        /// Consumer group name.
        group: String,
        /// IDs to acknowledge.
        ids: Vec<Vec<u8>>,
    },
    /// XPENDING key group \[start end count\]
    XPending {
        /// The stream key.
        key: Vec<u8>,
        /// Consumer group name.
        group: String,
        /// Optional start ID filter.
        start: Option<Vec<u8>>,
        /// Optional end ID filter.
        end: Option<Vec<u8>>,
        /// Optional count limit.
        count: Option<usize>,
    },
    /// XCLAIM key group consumer min-idle-time id \[id ...\]
    XClaim {
        /// The stream key.
        key: Vec<u8>,
        /// Consumer group name.
        group: String,
        /// Consumer claiming the messages.
        consumer: String,
        /// Minimum idle time in milliseconds.
        min_idle_time: u64,
        /// IDs to claim.
        ids: Vec<Vec<u8>>,
    },
    /// XAUTOCLAIM key group consumer min-idle-time start \[COUNT count\]
    XAutoClaim {
        /// The stream key.
        key: Vec<u8>,
        /// Consumer group name.
        group: String,
        /// Consumer claiming the messages.
        consumer: String,
        /// Minimum idle time in milliseconds.
        min_idle_time: u64,
        /// Start ID for scanning.
        start: Vec<u8>,
        /// Optional count of entries to claim.
        count: Option<usize>,
    },
    /// XINFO STREAM key
    XInfoStream {
        /// The stream key.
        key: Vec<u8>,
    },
    /// XINFO GROUPS key
    XInfoGroups {
        /// The stream key.
        key: Vec<u8>,
    },
    /// XDEL key id \[id ...\]
    XDel {
        /// The stream key.
        key: Vec<u8>,
        /// IDs to delete.
        ids: Vec<Vec<u8>>,
    },
}

/// One `DOC.UPDATE` mutation operation.
#[derive(Debug, Clone, PartialEq)]
pub enum DocUpdateMutation {
    /// `SET path value` — set path to JSON value.
    Set {
        /// Dotted path bytes.
        path: Vec<u8>,
        /// JSON value bytes.
        value: Vec<u8>,
    },
    /// `DEL path` — remove path.
    Del {
        /// Dotted path bytes.
        path: Vec<u8>,
    },
    /// `INCR path delta` — increment numeric value at path.
    Incr {
        /// Dotted path bytes.
        path: Vec<u8>,
        /// Increment amount.
        delta: f64,
    },
    /// `PUSH path value` — append JSON value to array at path.
    Push {
        /// Dotted path bytes.
        path: Vec<u8>,
        /// JSON value bytes.
        value: Vec<u8>,
    },
    /// `PULL path value` — remove matching JSON value(s) from array at path.
    Pull {
        /// Dotted path bytes.
        path: Vec<u8>,
        /// JSON value bytes.
        value: Vec<u8>,
    },
}

/// Bitwise operation for BITOP command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitOperation {
    /// Bitwise AND.
    And,
    /// Bitwise OR.
    Or,
    /// Bitwise XOR.
    Xor,
    /// Bitwise NOT (single source key).
    Not,
}

/// Integer encoding used by BITFIELD subcommands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitFieldEncoding {
    /// Signed (`i`) when true, unsigned (`u`) when false.
    pub signed: bool,
    /// Bit width of the encoded integer.
    pub bits: u8,
}

/// Offset format used by BITFIELD subcommands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitFieldOffset {
    /// Absolute bit offset (e.g. `0`).
    Absolute(i64),
    /// Type-scaled offset (e.g. `#2` multiplies by type width).
    Multiplied(i64),
}

/// Overflow behavior for BITFIELD mutation operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitFieldOverflow {
    /// Wrap around the representable range (default).
    Wrap,
    /// Clamp to minimum/maximum representable value.
    Sat,
    /// Do not apply the operation and return nil for that subcommand.
    Fail,
}

/// A parsed BITFIELD subcommand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitFieldOperation {
    /// GET type offset
    Get {
        /// Integer encoding for the field.
        encoding: BitFieldEncoding,
        /// Bit offset to read from.
        offset: BitFieldOffset,
    },
    /// SET type offset value
    Set {
        /// Integer encoding for the field.
        encoding: BitFieldEncoding,
        /// Bit offset to write to.
        offset: BitFieldOffset,
        /// New value to assign.
        value: i64,
    },
    /// INCRBY type offset increment
    IncrBy {
        /// Integer encoding for the field.
        encoding: BitFieldEncoding,
        /// Bit offset to mutate.
        offset: BitFieldOffset,
        /// Amount to increment/decrement by.
        increment: i64,
    },
    /// OVERFLOW WRAP|SAT|FAIL
    Overflow(BitFieldOverflow),
}

/// Distance unit for geo commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoUnit {
    /// Meters (default).
    Meters,
    /// Kilometers.
    Kilometers,
    /// Feet.
    Feet,
    /// Miles.
    Miles,
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
            | Command::LSet { key, .. }
            | Command::LInsert { key, .. }
            | Command::LRem { key, .. }
            | Command::LTrim { key, .. }
            | Command::LPos { key, .. }
            | Command::RPopLPush { source: key, .. }
            | Command::LMove { source: key, .. }
            | Command::HSet { key, .. }
            | Command::HGet { key, .. }
            | Command::HDel { key, .. }
            | Command::HGetAll { key }
            | Command::HLen { key }
            | Command::HExists { key, .. }
            | Command::HIncrBy { key, .. }
            | Command::HMGet { key, .. }
            | Command::HKeys { key }
            | Command::HVals { key }
            | Command::HSetNx { key, .. }
            | Command::HIncrByFloat { key, .. }
            | Command::HRandField { key, .. }
            | Command::HScan { key, .. }
            | Command::SAdd { key, .. }
            | Command::SRem { key, .. }
            | Command::SMembers { key }
            | Command::SIsMember { key, .. }
            | Command::SCard { key }
            | Command::SPop { key, .. }
            | Command::SRandMember { key, .. }
            | Command::SMove { source: key, .. }
            | Command::SMisMember { key, .. }
            | Command::SScan { key, .. }
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
            | Command::ZRevRangeByScore { key, .. }
            | Command::ZPopMin { key, .. }
            | Command::ZPopMax { key, .. }
            | Command::ZRangeByLex { key, .. }
            | Command::ZRevRangeByLex { key, .. }
            | Command::ZLexCount { key, .. }
            | Command::ZMScore { key, .. }
            | Command::ZRandMember { key, .. }
            | Command::ZScan { key, .. }
            | Command::VecSet { key, .. }
            | Command::VecDel { key }
            | Command::XAdd { key, .. }
            | Command::XLen { key }
            | Command::XRange { key, .. }
            | Command::XRevRange { key, .. }
            | Command::XTrim { key, .. }
            | Command::ObjectFreq { key }
            | Command::ObjectEncoding { key }
            | Command::ObjectRefCount { key }
            | Command::ObjectIdleTime { key }
            | Command::IncrByFloat { key, .. }
            | Command::GetRange { key, .. }
            | Command::SetRange { key, .. }
            | Command::GetDel { key }
            | Command::GetEx { key, .. }
            | Command::ExpireAt { key, .. }
            | Command::PExpireAt { key, .. }
            | Command::Rename { key, .. }
            | Command::RenameNx { key, .. }
            | Command::Copy { source: key, .. }
            | Command::PfAdd { key, .. }
            | Command::SetBit { key, .. }
            | Command::GetBit { key, .. }
            | Command::BitCount { key, .. }
            | Command::BitPos { key, .. }
            | Command::BitField { key, .. }
            | Command::GeoAdd { key, .. }
            | Command::GeoDist { key, .. }
            | Command::GeoHash { key, .. }
            | Command::GeoPos { key, .. }
            | Command::GeoSearch { key, .. }
            | Command::BLMove { source: key, .. }
            | Command::XGroupCreate { key, .. }
            | Command::XGroupDestroy { key, .. }
            | Command::XGroupDelConsumer { key, .. }
            | Command::XAck { key, .. }
            | Command::XPending { key, .. }
            | Command::XClaim { key, .. }
            | Command::XAutoClaim { key, .. }
            | Command::XInfoStream { key }
            | Command::XInfoGroups { key }
            | Command::XDel { key, .. } => Some(key),
            Command::PfCount { keys } => {
                if keys.len() == 1 {
                    Some(&keys[0])
                } else {
                    None
                }
            }
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
                | Command::XRead { .. }
                | Command::MSetNx { .. }
                | Command::Unlink { .. }
                | Command::Touch { .. }
                | Command::SUnion { .. }
                | Command::SUnionStore { .. }
                | Command::SInter { .. }
                | Command::SInterStore { .. }
                | Command::SDiff { .. }
                | Command::SDiffStore { .. }
                | Command::SInterCard { .. }
                | Command::PfMerge { .. }
                | Command::BitOp { .. }
                | Command::BLPop { .. }
                | Command::BRPop { .. }
                | Command::BZPopMin { .. }
                | Command::BZPopMax { .. }
                | Command::XReadGroup { .. }
        ) || matches!(self, Command::PfCount { keys } if keys.len() > 1)
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
                | Command::CommandInfo { .. }
                | Command::Hello { .. }
                | Command::Auth { .. }
                | Command::Dump
                | Command::CdcPoll { .. }
                | Command::CdcGroupCreate { .. }
                | Command::CdcGroupRead { .. }
                | Command::CdcAck { .. }
                | Command::CdcPending { .. }
                | Command::StatsHotkeys { .. }
                | Command::StatsLatency { .. }
                | Command::StatsMemory { .. }
                | Command::Subscribe { .. }
                | Command::Unsubscribe { .. }
                | Command::PSubscribe { .. }
                | Command::PUnsubscribe { .. }
                | Command::Publish { .. }
                | Command::RandomKey
                | Command::ObjectHelp
                | Command::Multi
                | Command::Exec
                | Command::Discard
                | Command::Watch { .. }
                | Command::Unwatch
                | Command::ConfigGet { .. }
                | Command::ConfigSet { .. }
                | Command::ConfigResetStat
                | Command::ClientId
                | Command::ClientGetName
                | Command::ClientSetName { .. }
                | Command::ClientList
                | Command::ClientInfo
                | Command::Time
                | Command::Select { .. }
                | Command::Quit
                | Command::Wait { .. }
                | Command::CommandCount
                | Command::CommandList
                | Command::CommandHelp
                | Command::CommandDocs { .. }
                | Command::DocCreate { .. }
                | Command::DocDrop { .. }
                | Command::DocInfo { .. }
                | Command::DocDictInfo { .. }
                | Command::DocStorage { .. }
                | Command::DocSet { .. }
                | Command::DocInsert { .. }
                | Command::DocMSet { .. }
                | Command::DocGet { .. }
                | Command::DocMGet { .. }
                | Command::DocUpdate { .. }
                | Command::DocDel { .. }
                | Command::DocExists { .. }
                | Command::DocCreateIndex { .. }
                | Command::DocDropIndex { .. }
                | Command::DocIndexes { .. }
                | Command::DocFind { .. }
                | Command::DocCount { .. }
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
                | Command::LSet { .. }
                | Command::LInsert { .. }
                | Command::LRem { .. }
                | Command::LTrim { .. }
                | Command::RPopLPush { .. }
                | Command::LMove { .. }
                | Command::HSet { .. }
                | Command::HDel { .. }
                | Command::HIncrBy { .. }
                | Command::HSetNx { .. }
                | Command::HIncrByFloat { .. }
                | Command::SAdd { .. }
                | Command::SRem { .. }
                | Command::SPop { .. }
                | Command::SUnionStore { .. }
                | Command::SInterStore { .. }
                | Command::SDiffStore { .. }
                | Command::SMove { .. }
                | Command::ZAdd { .. }
                | Command::ZRem { .. }
                | Command::ZIncrBy { .. }
                | Command::ZPopMin { .. }
                | Command::ZPopMax { .. }
                | Command::VecSet { .. }
                | Command::VecDel { .. }
                | Command::XAdd { .. }
                | Command::XTrim { .. }
                | Command::IncrByFloat { .. }
                | Command::SetRange { .. }
                | Command::GetDel { .. }
                | Command::GetEx { .. }
                | Command::MSetNx { .. }
                | Command::ExpireAt { .. }
                | Command::PExpireAt { .. }
                | Command::Rename { .. }
                | Command::RenameNx { .. }
                | Command::Unlink { .. }
                | Command::Copy { .. }
                | Command::PfAdd { .. }
                | Command::PfMerge { .. }
                | Command::SetBit { .. }
                | Command::BitOp { .. }
                | Command::GeoAdd { .. }
                | Command::BLPop { .. }
                | Command::BRPop { .. }
                | Command::BLMove { .. }
                | Command::BZPopMin { .. }
                | Command::BZPopMax { .. }
                | Command::XGroupCreate { .. }
                | Command::XGroupDestroy { .. }
                | Command::XGroupDelConsumer { .. }
                | Command::XAck { .. }
                | Command::XClaim { .. }
                | Command::XAutoClaim { .. }
                | Command::XDel { .. }
                | Command::DocCreate { .. }
                | Command::DocDrop { .. }
                | Command::DocSet { .. }
                | Command::DocInsert { .. }
                | Command::DocMSet { .. }
                | Command::DocUpdate { .. }
                | Command::DocDel { .. }
                | Command::DocCreateIndex { .. }
                | Command::DocDropIndex { .. }
        )
    }

    /// Return a numeric type index for stats tracking.
    pub fn cmd_type(&self) -> u8 {
        match self {
            Command::Get { .. } => 0,
            Command::Set { .. } => 1,
            Command::Del { .. } | Command::Unlink { .. } => 2,
            Command::Incr { .. } | Command::IncrBy { .. } | Command::IncrByFloat { .. } => 3,
            Command::Decr { .. } | Command::DecrBy { .. } => 4,
            Command::MGet { .. } => 5,
            Command::MSet { .. } | Command::MSetNx { .. } => 6,
            Command::Exists { .. } => 7,
            Command::Expire { .. }
            | Command::PExpire { .. }
            | Command::ExpireAt { .. }
            | Command::PExpireAt { .. } => 8,
            Command::Ttl { .. } | Command::PTtl { .. } => 9,
            Command::LPush { .. } => 10,
            Command::RPush { .. } => 11,
            Command::LPop { .. } => 12,
            Command::RPop { .. } => 13,
            Command::HSet { .. } | Command::HSetNx { .. } => 14,
            Command::HGet { .. } | Command::HMGet { .. } => 15,
            Command::HGetAll { .. } | Command::HKeys { .. } | Command::HVals { .. } => 16,
            Command::SAdd { .. } => 17,
            Command::SRem { .. } | Command::SPop { .. } => 18,
            Command::SRandMember { .. } | Command::SMisMember { .. } | Command::SScan { .. } => 40,
            Command::SUnion { .. }
            | Command::SUnionStore { .. }
            | Command::SInter { .. }
            | Command::SInterStore { .. }
            | Command::SDiff { .. }
            | Command::SDiffStore { .. }
            | Command::SInterCard { .. }
            | Command::SMove { .. } => 41,
            Command::Ping { .. } => 19,
            Command::Info { .. } => 20,
            Command::DbSize => 21,
            Command::FlushDb | Command::FlushAll => 22,
            Command::Keys { .. } | Command::Scan { .. } => 23,
            Command::VecSet { .. } => 24,
            Command::VecQuery { .. } => 25,
            Command::ZAdd { .. } => 27,
            Command::ZRange { .. }
            | Command::ZRevRange { .. }
            | Command::ZRangeByScore { .. }
            | Command::ZRevRangeByScore { .. }
            | Command::ZRangeByLex { .. }
            | Command::ZRevRangeByLex { .. } => 28,
            Command::ZScore { .. }
            | Command::ZRank { .. }
            | Command::ZRevRank { .. }
            | Command::ZCard { .. }
            | Command::ZCount { .. }
            | Command::ZLexCount { .. }
            | Command::ZMScore { .. } => 29,
            Command::ZRem { .. }
            | Command::ZIncrBy { .. }
            | Command::ZPopMin { .. }
            | Command::ZPopMax { .. } => 30,
            Command::ZRandMember { .. } | Command::ZScan { .. } => 42,
            Command::ObjectFreq { .. }
            | Command::ObjectEncoding { .. }
            | Command::ObjectRefCount { .. }
            | Command::ObjectIdleTime { .. }
            | Command::ObjectHelp => 31,
            Command::XAdd { .. } | Command::XTrim { .. } => 33,
            Command::XLen { .. }
            | Command::XRange { .. }
            | Command::XRevRange { .. }
            | Command::XRead { .. } => 34,
            Command::Subscribe { .. }
            | Command::Unsubscribe { .. }
            | Command::PSubscribe { .. }
            | Command::PUnsubscribe { .. }
            | Command::Publish { .. } => 35,
            Command::LSet { .. }
            | Command::LInsert { .. }
            | Command::LRem { .. }
            | Command::LTrim { .. } => 36,
            Command::LPos { .. } => 37,
            Command::RPopLPush { .. } | Command::LMove { .. } => 38,
            Command::HRandField { .. } | Command::HScan { .. } => 39,
            Command::PfAdd { .. } | Command::PfCount { .. } | Command::PfMerge { .. } => 43,
            Command::SetBit { .. }
            | Command::GetBit { .. }
            | Command::BitCount { .. }
            | Command::BitOp { .. }
            | Command::BitPos { .. }
            | Command::BitField { .. } => 44,
            Command::GeoAdd { .. }
            | Command::GeoDist { .. }
            | Command::GeoHash { .. }
            | Command::GeoPos { .. }
            | Command::GeoSearch { .. } => 45,
            Command::Multi
            | Command::Exec
            | Command::Discard
            | Command::Watch { .. }
            | Command::Unwatch => 46,
            Command::ConfigGet { .. }
            | Command::ConfigSet { .. }
            | Command::ConfigResetStat
            | Command::ClientId
            | Command::ClientGetName
            | Command::ClientSetName { .. }
            | Command::ClientList
            | Command::ClientInfo
            | Command::Time
            | Command::Select { .. }
            | Command::Quit
            | Command::Wait { .. }
            | Command::CommandCount
            | Command::CommandList
            | Command::CommandHelp
            | Command::CommandDocs { .. } => 47,
            Command::BLPop { .. }
            | Command::BRPop { .. }
            | Command::BLMove { .. }
            | Command::BZPopMin { .. }
            | Command::BZPopMax { .. } => 48,
            Command::XGroupCreate { .. }
            | Command::XGroupDestroy { .. }
            | Command::XGroupDelConsumer { .. }
            | Command::XReadGroup { .. }
            | Command::XAck { .. }
            | Command::XPending { .. }
            | Command::XClaim { .. }
            | Command::XAutoClaim { .. }
            | Command::XInfoStream { .. }
            | Command::XInfoGroups { .. }
            | Command::XDel { .. } => 49,
            Command::DocCreate { .. }
            | Command::DocDrop { .. }
            | Command::DocInfo { .. }
            | Command::DocDictInfo { .. }
            | Command::DocStorage { .. }
            | Command::DocSet { .. }
            | Command::DocInsert { .. }
            | Command::DocMSet { .. }
            | Command::DocGet { .. }
            | Command::DocMGet { .. }
            | Command::DocUpdate { .. }
            | Command::DocDel { .. }
            | Command::DocExists { .. }
            | Command::DocCreateIndex { .. }
            | Command::DocDropIndex { .. }
            | Command::DocIndexes { .. }
            | Command::DocFind { .. }
            | Command::DocCount { .. } => 50,
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

/// Canonical supported command names exposed by `COMMAND LIST`.
///
/// Names are lowercase and sorted for binary search.
pub const SUPPORTED_COMMAND_NAMES: &[&str] = &[
    "append",
    "auth",
    "bgrewriteaof",
    "bgsave",
    "bitcount",
    "bitfield",
    "bitop",
    "bitpos",
    "blmove",
    "blpop",
    "brpop",
    "bzpopmax",
    "bzpopmin",
    "cdc.ack",
    "cdc.group",
    "cdc.pending",
    "cdcpoll",
    "client",
    "command",
    "config",
    "copy",
    "dbsize",
    "decr",
    "decrby",
    "del",
    "discard",
    "doc.count",
    "doc.create",
    "doc.createindex",
    "doc.del",
    "doc.dictinfo",
    "doc.drop",
    "doc.dropindex",
    "doc.exists",
    "doc.find",
    "doc.get",
    "doc.indexes",
    "doc.info",
    "doc.mget",
    "doc.mset",
    "doc.set",
    "doc.storage",
    "doc.update",
    "dump",
    "echo",
    "exec",
    "exists",
    "expire",
    "expireat",
    "flushall",
    "flushdb",
    "geoadd",
    "geodist",
    "geohash",
    "geopos",
    "geosearch",
    "get",
    "getbit",
    "getdel",
    "getex",
    "getrange",
    "getset",
    "hdel",
    "hello",
    "hexists",
    "hget",
    "hgetall",
    "hincrby",
    "hincrbyfloat",
    "hkeys",
    "hlen",
    "hmget",
    "hmset",
    "hrandfield",
    "hscan",
    "hset",
    "hsetnx",
    "hvals",
    "incr",
    "incrby",
    "incrbyfloat",
    "info",
    "keys",
    "lindex",
    "linsert",
    "llen",
    "lmove",
    "lpop",
    "lpos",
    "lpush",
    "lrange",
    "lrem",
    "lset",
    "ltrim",
    "mget",
    "mset",
    "msetnx",
    "multi",
    "object",
    "persist",
    "pexpire",
    "pexpireat",
    "pfadd",
    "pfcount",
    "pfmerge",
    "ping",
    "psetex",
    "psubscribe",
    "pttl",
    "publish",
    "punsubscribe",
    "quit",
    "randomkey",
    "rename",
    "renamenx",
    "rpop",
    "rpoplpush",
    "rpush",
    "sadd",
    "scan",
    "scard",
    "sdiff",
    "sdiffstore",
    "select",
    "set",
    "setbit",
    "setex",
    "setnx",
    "setrange",
    "sinter",
    "sintercard",
    "sinterstore",
    "sismember",
    "smembers",
    "smismember",
    "smove",
    "spop",
    "srandmember",
    "srem",
    "sscan",
    "stats.hotkeys",
    "stats.latency",
    "stats.memory",
    "strlen",
    "subscribe",
    "substr",
    "sunion",
    "sunionstore",
    "time",
    "touch",
    "ttl",
    "type",
    "unlink",
    "unsubscribe",
    "unwatch",
    "vecdel",
    "vecquery",
    "vecset",
    "wait",
    "watch",
    "xack",
    "xadd",
    "xautoclaim",
    "xclaim",
    "xdel",
    "xgroup",
    "xinfo",
    "xlen",
    "xpending",
    "xrange",
    "xread",
    "xreadgroup",
    "xrevrange",
    "xtrim",
    "zadd",
    "zcard",
    "zcount",
    "zincrby",
    "zlexcount",
    "zmscore",
    "zpopmax",
    "zpopmin",
    "zrandmember",
    "zrange",
    "zrangebylex",
    "zrangebyscore",
    "zrank",
    "zrem",
    "zrevrange",
    "zrevrangebylex",
    "zrevrangebyscore",
    "zrevrank",
    "zscan",
    "zscore",
];

/// Human-readable help text for the `COMMAND HELP` reply.
pub const COMMAND_HELP_LINES: &[&str] = &[
    "COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "(no subcommand)",
    "    Return details about all Kora commands.",
    "COUNT",
    "    Return the total number of commands in this Kora server.",
    "LIST",
    "    Return a list of all commands in this Kora server.",
    "INFO [<command-name> ...]",
    "    Return details about multiple Kora commands.",
    "DOCS [<command-name> ...]",
    "    Return documentation details about multiple Kora commands.",
    "HELP",
    "    Print this help.",
];

/// Returns the number of supported command names.
pub fn supported_command_count() -> i64 {
    SUPPORTED_COMMAND_NAMES.len() as i64
}

fn normalize_command_name(name: &[u8]) -> String {
    String::from_utf8_lossy(name).to_ascii_lowercase()
}

fn command_supported(name: &str) -> bool {
    SUPPORTED_COMMAND_NAMES.binary_search(&name).is_ok()
}

fn command_info_entry(name: &str) -> CommandResponse {
    CommandResponse::Array(vec![
        CommandResponse::BulkString(name.as_bytes().to_vec()),
        CommandResponse::Integer(-1),
        CommandResponse::Array(vec![CommandResponse::BulkString(b"fast".to_vec())]),
        CommandResponse::Integer(0),
        CommandResponse::Integer(0),
        CommandResponse::Integer(0),
        CommandResponse::Array(vec![CommandResponse::BulkString(b"@kora".to_vec())]),
        CommandResponse::Array(vec![]),
        CommandResponse::Array(vec![]),
        CommandResponse::Array(vec![]),
    ])
}

/// Build a `COMMAND INFO`/`COMMAND` response.
pub fn command_info_response(names: &[Vec<u8>]) -> CommandResponse {
    if names.is_empty() {
        return CommandResponse::Array(
            SUPPORTED_COMMAND_NAMES
                .iter()
                .map(|name| command_info_entry(name))
                .collect(),
        );
    }

    let mut entries = Vec::with_capacity(names.len());
    for name in names {
        let normalized = normalize_command_name(name);
        if command_supported(&normalized) {
            entries.push(command_info_entry(&normalized));
        } else {
            entries.push(CommandResponse::Nil);
        }
    }
    CommandResponse::Array(entries)
}

/// Build a `COMMAND LIST` response.
pub fn command_list_response() -> CommandResponse {
    CommandResponse::Array(
        SUPPORTED_COMMAND_NAMES
            .iter()
            .map(|name| CommandResponse::BulkString(name.as_bytes().to_vec()))
            .collect(),
    )
}

fn command_docs_entry(name: &str) -> CommandResponse {
    CommandResponse::Map(vec![
        (
            CommandResponse::BulkString(b"summary".to_vec()),
            CommandResponse::BulkString(format!("Kora implementation of `{}`.", name).into_bytes()),
        ),
        (
            CommandResponse::BulkString(b"since".to_vec()),
            CommandResponse::BulkString(b"0.1.0".to_vec()),
        ),
        (
            CommandResponse::BulkString(b"group".to_vec()),
            CommandResponse::BulkString(b"generic".to_vec()),
        ),
    ])
}

/// Build a `COMMAND DOCS` response.
pub fn command_docs_response(names: &[Vec<u8>]) -> CommandResponse {
    let mut docs = Vec::new();

    if names.is_empty() {
        docs.reserve(SUPPORTED_COMMAND_NAMES.len());
        for name in SUPPORTED_COMMAND_NAMES {
            docs.push((
                CommandResponse::BulkString(name.as_bytes().to_vec()),
                command_docs_entry(name),
            ));
        }
        return CommandResponse::Map(docs);
    }

    for name in names {
        let normalized = normalize_command_name(name);
        if command_supported(&normalized) {
            docs.push((
                CommandResponse::BulkString(normalized.as_bytes().to_vec()),
                command_docs_entry(&normalized),
            ));
        }
    }
    CommandResponse::Map(docs)
}

/// Build a `COMMAND HELP` response.
pub fn command_help_response() -> CommandResponse {
    CommandResponse::Array(
        COMMAND_HELP_LINES
            .iter()
            .map(|line| CommandResponse::BulkString(line.as_bytes().to_vec()))
            .collect(),
    )
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
    /// Bulk string backed by shared bytes to avoid copying large values on reads.
    BulkStringShared(Arc<[u8]>),
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

impl CommandResponse {
    /// Returns the bulk-string payload if this response contains one.
    pub fn bulk_string_bytes(&self) -> Option<&[u8]> {
        match self {
            CommandResponse::BulkString(data) => Some(data),
            CommandResponse::BulkStringShared(data) => Some(data),
            _ => None,
        }
    }

    /// Consumes the response and returns an owned bulk-string payload when present.
    pub fn into_bulk_string_bytes(self) -> Option<Vec<u8>> {
        match self {
            CommandResponse::BulkString(data) => Some(data),
            CommandResponse::BulkStringShared(data) => Some(data.as_ref().to_vec()),
            _ => None,
        }
    }
}
