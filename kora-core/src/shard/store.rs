//! Per-shard key-value store with full Redis command execution.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Duration;

use crate::command::{Command, CommandResponse};
use crate::types::{CompactKey, KeyEntry, Value};

#[cfg(feature = "observability")]
use kora_observability::stats::ShardStats;

#[cfg(feature = "vector")]
use kora_vector::distance::DistanceMetric;
#[cfg(feature = "vector")]
use kora_vector::hnsw::HnswIndex;

/// A single shard's key-value store.
///
/// Each worker thread owns exactly one `ShardStore`. All operations on it
/// are single-threaded — no locking required.
pub struct ShardStore {
    entries: HashMap<CompactKey, KeyEntry>,
    shard_id: u16,
    max_memory: usize,
    memory_used: usize,
    eviction_counter: u64,
    #[cfg(feature = "observability")]
    stats: ShardStats,
    #[cfg(feature = "vector")]
    vector_indexes: HashMap<CompactKey, HnswIndex>,
}

impl ShardStore {
    /// Create a new empty shard store.
    pub fn new(shard_id: u16) -> Self {
        Self {
            entries: HashMap::new(),
            shard_id,
            max_memory: 0,
            memory_used: 0,
            eviction_counter: 0,
            #[cfg(feature = "observability")]
            stats: ShardStats::new(),
            #[cfg(feature = "vector")]
            vector_indexes: HashMap::new(),
        }
    }

    /// Get a reference to the shard stats (observability feature).
    #[cfg(feature = "observability")]
    pub fn stats(&self) -> &ShardStats {
        &self.stats
    }

    /// Get the shard ID.
    pub fn shard_id(&self) -> u16 {
        self.shard_id
    }

    /// Get the number of keys in this shard.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the shard is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Set the maximum memory limit for this shard (0 = unlimited).
    pub fn set_max_memory(&mut self, bytes: usize) {
        self.max_memory = bytes;
    }

    /// Get the maximum memory limit for this shard.
    pub fn max_memory(&self) -> usize {
        self.max_memory
    }

    /// Remove all expired keys, returning the count removed.
    pub fn evict_expired(&mut self) -> usize {
        let before = self.entries.len();
        self.entries.retain(|_, entry| !entry.is_expired());
        before - self.entries.len()
    }

    /// Remove all keys from this shard.
    pub fn flush(&mut self) {
        self.entries.clear();
    }

    /// Execute a command on this shard and return the response.
    pub fn execute(&mut self, cmd: Command) -> CommandResponse {
        #[cfg(feature = "observability")]
        let start = std::time::Instant::now();

        #[cfg(feature = "observability")]
        let cmd_type = cmd.cmd_type() as usize;

        #[cfg(feature = "observability")]
        if let Some(key) = cmd.key() {
            self.stats.record_key_access(key);
        }

        if cmd.is_mutation() {
            self.maybe_evict();
        }

        let is_read = !cmd.is_mutation();
        let cmd_key = if is_read {
            cmd.key().map(|k| k.to_vec())
        } else {
            None
        };

        let response = self.execute_inner(cmd);

        if let Some(key) = cmd_key {
            self.touch_key(&key);
        }

        #[cfg(feature = "observability")]
        {
            let duration_ns = start.elapsed().as_nanos() as u64;
            self.stats.record_command(cmd_type, duration_ns);
            self.stats.set_key_count(self.entries.len() as u64);
        }

        response
    }

    fn execute_inner(&mut self, cmd: Command) -> CommandResponse {
        match cmd {
            // String commands
            Command::Get { key } => self.cmd_get(&key),
            Command::Set {
                key,
                value,
                ex,
                px,
                nx,
                xx,
            } => self.cmd_set(&key, &value, ex, px, nx, xx),
            Command::GetSet { key, value } => self.cmd_getset(&key, &value),
            Command::Append { key, value } => self.cmd_append(&key, &value),
            Command::Strlen { key } => self.cmd_strlen(&key),
            Command::Incr { key } => self.cmd_incrby(&key, 1),
            Command::Decr { key } => self.cmd_incrby(&key, -1),
            Command::IncrBy { key, delta } => self.cmd_incrby(&key, delta),
            Command::DecrBy { key, delta } => self.cmd_incrby(&key, -delta),
            Command::SetNx { key, value } => self.cmd_set(&key, &value, None, None, true, false),

            // Key commands
            Command::Del { keys } => {
                let count = keys.iter().filter(|k| self.del(k)).count();
                CommandResponse::Integer(count as i64)
            }
            Command::Exists { keys } => {
                let count = keys.iter().filter(|k| self.exists(k)).count();
                CommandResponse::Integer(count as i64)
            }
            Command::Expire { key, seconds } => self.cmd_expire(&key, Duration::from_secs(seconds)),
            Command::PExpire { key, millis } => {
                self.cmd_expire(&key, Duration::from_millis(millis))
            }
            Command::Persist { key } => self.cmd_persist(&key),
            Command::Ttl { key } => self.cmd_ttl(&key, false),
            Command::PTtl { key } => self.cmd_ttl(&key, true),
            Command::Type { key } => self.cmd_type(&key),
            Command::Keys { pattern } => self.cmd_keys(&pattern),
            Command::Scan {
                cursor,
                pattern,
                count,
            } => self.cmd_scan(cursor, pattern.as_deref(), count.unwrap_or(10)),
            Command::DbSize => CommandResponse::Integer(self.entries.len() as i64),
            Command::FlushDb | Command::FlushAll => {
                self.flush();
                CommandResponse::Ok
            }

            // List commands
            Command::LPush { key, values } => self.cmd_lpush(&key, &values),
            Command::RPush { key, values } => self.cmd_rpush(&key, &values),
            Command::LPop { key } => self.cmd_lpop(&key),
            Command::RPop { key } => self.cmd_rpop(&key),
            Command::LLen { key } => self.cmd_llen(&key),
            Command::LRange { key, start, stop } => self.cmd_lrange(&key, start, stop),
            Command::LIndex { key, index } => self.cmd_lindex(&key, index),

            // Hash commands
            Command::HSet { key, fields } => self.cmd_hset(&key, &fields),
            Command::HGet { key, field } => self.cmd_hget(&key, &field),
            Command::HDel { key, fields } => self.cmd_hdel(&key, &fields),
            Command::HGetAll { key } => self.cmd_hgetall(&key),
            Command::HLen { key } => self.cmd_hlen(&key),
            Command::HExists { key, field } => self.cmd_hexists(&key, &field),
            Command::HIncrBy { key, field, delta } => self.cmd_hincrby(&key, &field, delta),

            // Set commands
            Command::SAdd { key, members } => self.cmd_sadd(&key, &members),
            Command::SRem { key, members } => self.cmd_srem(&key, &members),
            Command::SMembers { key } => self.cmd_smembers(&key),
            Command::SIsMember { key, member } => self.cmd_sismember(&key, &member),
            Command::SCard { key } => self.cmd_scard(&key),

            // Sorted set commands
            Command::ZAdd { key, members } => self.cmd_zadd(&key, &members),
            Command::ZRem { key, members } => self.cmd_zrem(&key, &members),
            Command::ZScore { key, member } => self.cmd_zscore(&key, &member),
            Command::ZRank { key, member } => self.cmd_zrank(&key, &member, false),
            Command::ZRevRank { key, member } => self.cmd_zrank(&key, &member, true),
            Command::ZCard { key } => self.cmd_zcard(&key),
            Command::ZRange {
                key,
                start,
                stop,
                withscores,
            } => self.cmd_zrange(&key, start, stop, withscores, false),
            Command::ZRevRange {
                key,
                start,
                stop,
                withscores,
            } => self.cmd_zrange(&key, start, stop, withscores, true),
            Command::ZRangeByScore {
                key,
                min,
                max,
                withscores,
                offset,
                count,
            } => self.cmd_zrangebyscore(&key, min, max, withscores, offset, count),
            Command::ZIncrBy { key, delta, member } => self.cmd_zincrby(&key, delta, &member),
            Command::ZCount { key, min, max } => self.cmd_zcount(&key, min, max),

            // Server commands
            Command::Ping { message } => match message {
                Some(msg) => CommandResponse::BulkString(msg),
                None => CommandResponse::SimpleString("PONG".to_string()),
            },
            Command::Echo { message } => CommandResponse::BulkString(message),
            Command::Info { .. } => CommandResponse::BulkString(
                format!(
                    "# Server\r\nkora_version:0.1.0\r\n# Keyspace\r\ndb0:keys={}\r\n",
                    self.entries.len()
                )
                .into_bytes(),
            ),
            Command::CommandInfo => CommandResponse::Array(vec![]),

            // Dump: return all entries for RDB snapshot
            Command::Dump => {
                let entries: Vec<CommandResponse> = self
                    .entries
                    .iter()
                    .filter(|(_, entry)| !entry.is_expired())
                    .flat_map(|(key, entry)| {
                        vec![
                            CommandResponse::BulkString(key.as_bytes().to_vec()),
                            CommandResponse::BulkString(entry.value.to_bytes()),
                        ]
                    })
                    .collect();
                CommandResponse::Array(entries)
            }

            // Multi-key commands handled at engine level, but provide per-shard fallback
            Command::MGet { keys } => {
                let results: Vec<CommandResponse> = keys.iter().map(|k| self.cmd_get(k)).collect();
                CommandResponse::Array(results)
            }
            Command::MSet { entries } => {
                for (k, v) in &entries {
                    self.cmd_set(k, v, None, None, false, false);
                }
                CommandResponse::Ok
            }

            // Vector commands — handled per-shard when vector feature is enabled
            #[cfg(feature = "vector")]
            Command::VecSet {
                key,
                dimensions,
                vector,
            } => self.cmd_vec_set(&key, dimensions, &vector),
            #[cfg(feature = "vector")]
            Command::VecQuery { key, k, vector } => self.cmd_vec_query(&key, k, &vector),
            #[cfg(feature = "vector")]
            Command::VecDel { key } => self.cmd_vec_del(&key),

            // Object commands
            Command::ObjectFreq { ref key } => {
                let compact = CompactKey::new(key);
                match self.entries.get(&compact) {
                    Some(entry) if !entry.is_expired() => {
                        CommandResponse::Integer(entry.lfu_counter as i64)
                    }
                    _ => CommandResponse::Nil,
                }
            }
            Command::ObjectEncoding { ref key } => {
                let compact = CompactKey::new(key);
                match self.entries.get(&compact) {
                    Some(entry) if !entry.is_expired() => {
                        let encoding = match &entry.value {
                            Value::InlineStr { .. } => "embstr",
                            Value::HeapStr(_) => "raw",
                            Value::Int(_) => "int",
                            Value::List(_) => "linkedlist",
                            Value::Hash(_) => "hashtable",
                            Value::Set(_) => "hashtable",
                            _ => "unknown",
                        };
                        CommandResponse::BulkString(encoding.as_bytes().to_vec())
                    }
                    _ => CommandResponse::Nil,
                }
            }

            // Commands handled at server/engine level — should not reach here
            Command::BgSave
            | Command::BgRewriteAof
            | Command::Hello { .. }
            | Command::Auth { .. }
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
            | Command::StatsMemory { .. } => {
                CommandResponse::Error("ERR command handled at server level".into())
            }

            #[cfg(not(feature = "vector"))]
            Command::VecSet { .. } | Command::VecQuery { .. } | Command::VecDel { .. } => {
                CommandResponse::Error("ERR vector feature not enabled".into())
            }
        }
    }

    // ─── String operations ───────────────────────────────────────────

    fn cmd_get(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match entry.value.as_bytes() {
                Some(bytes) => CommandResponse::BulkString(bytes),
                None => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Nil,
        }
    }

    fn cmd_set(
        &mut self,
        key: &[u8],
        value: &[u8],
        ex: Option<u64>,
        px: Option<u64>,
        nx: bool,
        xx: bool,
    ) -> CommandResponse {
        let compact = CompactKey::new(key);
        let key_exists = self.entries.contains_key(&compact) && !self.is_expired(&compact);

        if nx && key_exists {
            return CommandResponse::Nil;
        }
        if xx && !key_exists {
            return CommandResponse::Nil;
        }

        let mut entry = KeyEntry::new(compact.clone(), Value::from_bytes(value));
        if let Some(dur) = Command::ttl_duration(ex, px) {
            entry.set_ttl(dur);
        }
        self.entries.insert(compact, entry);
        CommandResponse::Ok
    }

    fn cmd_getset(&mut self, key: &[u8], value: &[u8]) -> CommandResponse {
        let old = self.cmd_get(key);
        self.cmd_set(key, value, None, None, false, false);
        old
    }

    fn cmd_append(&mut self, key: &[u8], value: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get_mut(&compact) {
            Some(entry) => match &entry.value {
                Value::InlineStr { data, len } => {
                    let mut existing = data[..*len as usize].to_vec();
                    existing.extend_from_slice(value);
                    let new_len = existing.len();
                    entry.value = Value::from_bytes(&existing);
                    CommandResponse::Integer(new_len as i64)
                }
                Value::HeapStr(arc) => {
                    let mut existing = arc.to_vec();
                    existing.extend_from_slice(value);
                    let new_len = existing.len();
                    entry.value = Value::from_bytes(&existing);
                    CommandResponse::Integer(new_len as i64)
                }
                Value::Int(i) => {
                    let mut existing = i.to_string().into_bytes();
                    existing.extend_from_slice(value);
                    let new_len = existing.len();
                    entry.value = Value::from_bytes(&existing);
                    CommandResponse::Integer(new_len as i64)
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => {
                let new_entry = KeyEntry::new(compact.clone(), Value::from_bytes(value));
                self.entries.insert(compact, new_entry);
                CommandResponse::Integer(value.len() as i64)
            }
        }
    }

    fn cmd_strlen(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match entry.value.as_bytes() {
                Some(bytes) => CommandResponse::Integer(bytes.len() as i64),
                None => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Integer(0),
        }
    }

    fn cmd_incrby(&mut self, key: &[u8], delta: i64) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get_mut(&compact) {
            Some(entry) => {
                let current = match &entry.value {
                    Value::Int(i) => *i,
                    Value::InlineStr { data, len } => {
                        match std::str::from_utf8(&data[..*len as usize])
                            .ok()
                            .and_then(|s| s.parse::<i64>().ok())
                        {
                            Some(i) => i,
                            None => {
                                return CommandResponse::Error(
                                    "ERR value is not an integer or out of range".into(),
                                )
                            }
                        }
                    }
                    Value::HeapStr(arc) => {
                        match std::str::from_utf8(arc)
                            .ok()
                            .and_then(|s| s.parse::<i64>().ok())
                        {
                            Some(i) => i,
                            None => {
                                return CommandResponse::Error(
                                    "ERR value is not an integer or out of range".into(),
                                )
                            }
                        }
                    }
                    _ => {
                        return CommandResponse::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .into(),
                        )
                    }
                };
                match current.checked_add(delta) {
                    Some(result) => {
                        entry.value = Value::Int(result);
                        CommandResponse::Integer(result)
                    }
                    None => {
                        CommandResponse::Error("ERR increment or decrement would overflow".into())
                    }
                }
            }
            None => {
                let entry = KeyEntry::new(compact.clone(), Value::Int(delta));
                self.entries.insert(compact, entry);
                CommandResponse::Integer(delta)
            }
        }
    }

    // ─── Key operations ──────────────────────────────────────────────

    fn del(&mut self, key: &[u8]) -> bool {
        let compact = CompactKey::new(key);
        self.entries.remove(&compact).is_some()
    }

    fn exists(&mut self, key: &[u8]) -> bool {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        self.entries.contains_key(&compact)
    }

    fn cmd_expire(&mut self, key: &[u8], duration: Duration) -> CommandResponse {
        let compact = CompactKey::new(key);
        match self.entries.get_mut(&compact) {
            Some(entry) if !entry.is_expired() => {
                entry.set_ttl(duration);
                CommandResponse::Integer(1)
            }
            _ => CommandResponse::Integer(0),
        }
    }

    fn cmd_persist(&mut self, key: &[u8]) -> CommandResponse {
        let compact = CompactKey::new(key);
        match self.entries.get_mut(&compact) {
            Some(entry) if !entry.is_expired() && entry.ttl.is_some() => {
                entry.clear_ttl();
                CommandResponse::Integer(1)
            }
            _ => CommandResponse::Integer(0),
        }
    }

    fn cmd_ttl(&mut self, key: &[u8], millis: bool) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            None => CommandResponse::Integer(-2),
            Some(entry) => match entry.remaining_ttl() {
                None => CommandResponse::Integer(-1),
                Some(dur) => {
                    if millis {
                        CommandResponse::Integer(dur.as_millis() as i64)
                    } else {
                        CommandResponse::Integer(dur.as_secs() as i64)
                    }
                }
            },
        }
    }

    fn cmd_type(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => CommandResponse::SimpleString(entry.value.type_name().to_string()),
            None => CommandResponse::SimpleString("none".to_string()),
        }
    }

    fn cmd_keys(&self, pattern: &str) -> CommandResponse {
        let results: Vec<CommandResponse> = self
            .entries
            .iter()
            .filter(|(_, entry)| !entry.is_expired())
            .filter(|(key, _)| glob_match(pattern, key.as_bytes()))
            .map(|(key, _)| CommandResponse::BulkString(key.as_bytes().to_vec()))
            .collect();
        CommandResponse::Array(results)
    }

    fn cmd_scan(&self, cursor: u64, pattern: Option<&str>, count: usize) -> CommandResponse {
        // Collect and sort keys for deterministic cursor iteration.
        // Sorting ensures the same key set always yields the same order,
        // so cursor-based pagination works correctly across calls.
        let mut keys: Vec<&CompactKey> = self
            .entries
            .iter()
            .filter(|(_, entry)| !entry.is_expired())
            .filter(|(key, _)| pattern.map_or(true, |p| glob_match(p, key.as_bytes())))
            .map(|(key, _)| key)
            .collect();
        keys.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));

        let start = cursor as usize;
        let end = (start + count).min(keys.len());

        if start >= keys.len() {
            return CommandResponse::Array(vec![
                CommandResponse::BulkString(b"0".to_vec()),
                CommandResponse::Array(vec![]),
            ]);
        }

        let result_keys: Vec<CommandResponse> = keys[start..end]
            .iter()
            .map(|k| CommandResponse::BulkString(k.as_bytes().to_vec()))
            .collect();

        let next_cursor = if end >= keys.len() { 0 } else { end as u64 };

        CommandResponse::Array(vec![
            CommandResponse::BulkString(next_cursor.to_string().into_bytes()),
            CommandResponse::Array(result_keys),
        ])
    }

    // ─── List operations ─────────────────────────────────────────────

    fn cmd_lpush(&mut self, key: &[u8], values: &[Vec<u8>]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let list = self.get_or_create_list(&compact);
        match list {
            Ok(deque) => {
                for v in values {
                    deque.push_front(Value::from_bytes(v));
                }
                CommandResponse::Integer(deque.len() as i64)
            }
            Err(e) => e,
        }
    }

    fn cmd_rpush(&mut self, key: &[u8], values: &[Vec<u8>]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let list = self.get_or_create_list(&compact);
        match list {
            Ok(deque) => {
                for v in values {
                    deque.push_back(Value::from_bytes(v));
                }
                CommandResponse::Integer(deque.len() as i64)
            }
            Err(e) => e,
        }
    }

    fn cmd_lpop(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let (result, is_empty) = match self.entries.get_mut(&compact) {
            Some(entry) => match &mut entry.value {
                Value::List(deque) => {
                    let val = deque.pop_front();
                    let empty = deque.is_empty();
                    match val {
                        Some(v) => match v.as_bytes() {
                            Some(b) => (CommandResponse::BulkString(b), empty),
                            None => (CommandResponse::Nil, empty),
                        },
                        None => (CommandResponse::Nil, false),
                    }
                }
                _ => (
                    CommandResponse::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    ),
                    false,
                ),
            },
            None => (CommandResponse::Nil, false),
        };
        if is_empty {
            self.entries.remove(&compact);
        }
        result
    }

    fn cmd_rpop(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let (result, is_empty) = match self.entries.get_mut(&compact) {
            Some(entry) => match &mut entry.value {
                Value::List(deque) => {
                    let val = deque.pop_back();
                    let empty = deque.is_empty();
                    match val {
                        Some(v) => match v.as_bytes() {
                            Some(b) => (CommandResponse::BulkString(b), empty),
                            None => (CommandResponse::Nil, empty),
                        },
                        None => (CommandResponse::Nil, false),
                    }
                }
                _ => (
                    CommandResponse::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    ),
                    false,
                ),
            },
            None => (CommandResponse::Nil, false),
        };
        if is_empty {
            self.entries.remove(&compact);
        }
        result
    }

    fn cmd_llen(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::List(deque) => CommandResponse::Integer(deque.len() as i64),
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Integer(0),
        }
    }

    fn cmd_lrange(&mut self, key: &[u8], start: i64, stop: i64) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::List(deque) => {
                    let len = deque.len() as i64;
                    let s = normalize_index(start, len);
                    let e = normalize_index(stop, len);
                    if s > e || s >= len as usize {
                        return CommandResponse::Array(vec![]);
                    }
                    let e = e.min(len as usize - 1);
                    let results: Vec<CommandResponse> = deque
                        .iter()
                        .skip(s)
                        .take(e - s + 1)
                        .map(|v| match v.as_bytes() {
                            Some(b) => CommandResponse::BulkString(b),
                            None => CommandResponse::Nil,
                        })
                        .collect();
                    CommandResponse::Array(results)
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Array(vec![]),
        }
    }

    fn cmd_lindex(&mut self, key: &[u8], index: i64) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::List(deque) => {
                    let idx = normalize_index(index, deque.len() as i64);
                    match deque.get(idx) {
                        Some(val) => match val.as_bytes() {
                            Some(b) => CommandResponse::BulkString(b),
                            None => CommandResponse::Nil,
                        },
                        None => CommandResponse::Nil,
                    }
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Nil,
        }
    }

    // ─── Hash operations ─────────────────────────────────────────────

    fn cmd_hset(&mut self, key: &[u8], fields: &[(Vec<u8>, Vec<u8>)]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let hash = self.get_or_create_hash(&compact);
        match hash {
            Ok(map) => {
                let mut count = 0i64;
                for (f, v) in fields {
                    let fk = CompactKey::new(f);
                    if map.insert(fk, Value::from_bytes(v)).is_none() {
                        count += 1;
                    }
                }
                CommandResponse::Integer(count)
            }
            Err(e) => e,
        }
    }

    fn cmd_hget(&mut self, key: &[u8], field: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Hash(map) => {
                    let fk = CompactKey::new(field);
                    match map.get(&fk) {
                        Some(val) => match val.as_bytes() {
                            Some(b) => CommandResponse::BulkString(b),
                            None => CommandResponse::Nil,
                        },
                        None => CommandResponse::Nil,
                    }
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Nil,
        }
    }

    fn cmd_hdel(&mut self, key: &[u8], fields: &[Vec<u8>]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let (count, is_empty) = match self.entries.get_mut(&compact) {
            Some(entry) => match &mut entry.value {
                Value::Hash(map) => {
                    let c = fields
                        .iter()
                        .filter(|f| map.remove(&CompactKey::new(f)).is_some())
                        .count();
                    (c as i64, map.is_empty())
                }
                _ => {
                    return CommandResponse::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            },
            None => return CommandResponse::Integer(0),
        };
        if is_empty {
            self.entries.remove(&compact);
        }
        CommandResponse::Integer(count)
    }

    fn cmd_hgetall(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Hash(map) => {
                    let mut results = Vec::with_capacity(map.len() * 2);
                    for (k, v) in map {
                        results.push(CommandResponse::BulkString(k.as_bytes().to_vec()));
                        results.push(match v.as_bytes() {
                            Some(b) => CommandResponse::BulkString(b),
                            None => CommandResponse::Nil,
                        });
                    }
                    CommandResponse::Array(results)
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Array(vec![]),
        }
    }

    fn cmd_hlen(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Hash(map) => CommandResponse::Integer(map.len() as i64),
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Integer(0),
        }
    }

    fn cmd_hexists(&mut self, key: &[u8], field: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Hash(map) => {
                    let fk = CompactKey::new(field);
                    CommandResponse::Integer(if map.contains_key(&fk) { 1 } else { 0 })
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Integer(0),
        }
    }

    fn cmd_hincrby(&mut self, key: &[u8], field: &[u8], delta: i64) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let hash = self.get_or_create_hash(&compact);
        match hash {
            Ok(map) => {
                let fk = CompactKey::new(field);
                let current = match map.get(&fk) {
                    Some(val) => match val.as_bytes().and_then(|b| {
                        std::str::from_utf8(&b)
                            .ok()
                            .and_then(|s| s.parse::<i64>().ok())
                    }) {
                        Some(i) => i,
                        None => {
                            return CommandResponse::Error(
                                "ERR hash value is not an integer".into(),
                            )
                        }
                    },
                    None => 0,
                };
                match current.checked_add(delta) {
                    Some(result) => {
                        map.insert(fk, Value::Int(result));
                        CommandResponse::Integer(result)
                    }
                    None => {
                        CommandResponse::Error("ERR increment or decrement would overflow".into())
                    }
                }
            }
            Err(e) => e,
        }
    }

    // ─── Set operations ──────────────────────────────────────────────

    fn cmd_sadd(&mut self, key: &[u8], members: &[Vec<u8>]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let set = self.get_or_create_set(&compact);
        match set {
            Ok(set) => {
                let count = members
                    .iter()
                    .filter(|m| set.insert(Value::from_bytes(m)))
                    .count();
                CommandResponse::Integer(count as i64)
            }
            Err(e) => e,
        }
    }

    fn cmd_srem(&mut self, key: &[u8], members: &[Vec<u8>]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let (count, is_empty) = match self.entries.get_mut(&compact) {
            Some(entry) => match &mut entry.value {
                Value::Set(set) => {
                    let c = members
                        .iter()
                        .filter(|m| set.remove(&Value::from_bytes(m)))
                        .count();
                    (c as i64, set.is_empty())
                }
                _ => {
                    return CommandResponse::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            },
            None => return CommandResponse::Integer(0),
        };
        if is_empty {
            self.entries.remove(&compact);
        }
        CommandResponse::Integer(count)
    }

    fn cmd_smembers(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Set(set) => {
                    let results: Vec<CommandResponse> = set
                        .iter()
                        .map(|v| match v.as_bytes() {
                            Some(b) => CommandResponse::BulkString(b),
                            None => CommandResponse::Nil,
                        })
                        .collect();
                    CommandResponse::Array(results)
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Array(vec![]),
        }
    }

    fn cmd_sismember(&mut self, key: &[u8], member: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Set(set) => {
                    let val = Value::from_bytes(member);
                    CommandResponse::Integer(if set.contains(&val) { 1 } else { 0 })
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Integer(0),
        }
    }

    fn cmd_scard(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Set(set) => CommandResponse::Integer(set.len() as i64),
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Integer(0),
        }
    }

    // ─── Sorted Set operations ────────────────────────────────────────

    fn cmd_zadd(&mut self, key: &[u8], members: &[(f64, Vec<u8>)]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let zset = self.get_or_create_sorted_set(&compact);
        match zset {
            Ok(map) => {
                let mut added = 0i64;
                for (score, member) in members {
                    if map.insert(member.clone(), *score).is_none() {
                        added += 1;
                    }
                }
                CommandResponse::Integer(added)
            }
            Err(e) => e,
        }
    }

    fn cmd_zrem(&mut self, key: &[u8], members: &[Vec<u8>]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let (count, is_empty) = match self.entries.get_mut(&compact) {
            Some(entry) => match &mut entry.value {
                Value::SortedSet(map) => {
                    let c = members.iter().filter(|m| map.remove(*m).is_some()).count();
                    (c as i64, map.is_empty())
                }
                _ => {
                    return CommandResponse::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            },
            None => return CommandResponse::Integer(0),
        };
        if is_empty {
            self.entries.remove(&compact);
        }
        CommandResponse::Integer(count)
    }

    fn cmd_zscore(&mut self, key: &[u8], member: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::SortedSet(map) => match map.get(member) {
                    Some(score) => CommandResponse::BulkString(format!("{}", score).into_bytes()),
                    None => CommandResponse::Nil,
                },
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Nil,
        }
    }

    fn cmd_zrank(&mut self, key: &[u8], member: &[u8], reverse: bool) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::SortedSet(map) => {
                    if !map.contains_key(member) {
                        return CommandResponse::Nil;
                    }
                    let mut sorted: Vec<(&Vec<u8>, &f64)> = map.iter().collect();
                    sorted.sort_by(|a, b| {
                        a.1.partial_cmp(b.1)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| a.0.cmp(b.0))
                    });
                    if reverse {
                        sorted.reverse();
                    }
                    match sorted.iter().position(|(m, _)| m.as_slice() == member) {
                        Some(pos) => CommandResponse::Integer(pos as i64),
                        None => CommandResponse::Nil,
                    }
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Nil,
        }
    }

    fn cmd_zcard(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::SortedSet(map) => CommandResponse::Integer(map.len() as i64),
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Integer(0),
        }
    }

    fn cmd_zrange(
        &mut self,
        key: &[u8],
        start: i64,
        stop: i64,
        withscores: bool,
        reverse: bool,
    ) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::SortedSet(map) => {
                    let mut sorted: Vec<(&Vec<u8>, &f64)> = map.iter().collect();
                    sorted.sort_by(|a, b| {
                        a.1.partial_cmp(b.1)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| a.0.cmp(b.0))
                    });
                    if reverse {
                        sorted.reverse();
                    }
                    let len = sorted.len() as i64;
                    let s = normalize_index(start, len);
                    let e = normalize_index(stop, len);
                    if s > e || s >= len as usize {
                        return CommandResponse::Array(vec![]);
                    }
                    let e = e.min(len as usize - 1);
                    let mut results = Vec::new();
                    for (member, score) in &sorted[s..=e] {
                        results.push(CommandResponse::BulkString(member.to_vec()));
                        if withscores {
                            results.push(CommandResponse::BulkString(
                                format!("{}", score).into_bytes(),
                            ));
                        }
                    }
                    CommandResponse::Array(results)
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Array(vec![]),
        }
    }

    fn cmd_zrangebyscore(
        &mut self,
        key: &[u8],
        min: f64,
        max: f64,
        withscores: bool,
        offset: Option<usize>,
        count: Option<usize>,
    ) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::SortedSet(map) => {
                    let mut sorted: Vec<(&Vec<u8>, &f64)> = map
                        .iter()
                        .filter(|(_, s)| **s >= min && **s <= max)
                        .collect();
                    sorted.sort_by(|a, b| {
                        a.1.partial_cmp(b.1)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| a.0.cmp(b.0))
                    });
                    let off = offset.unwrap_or(0);
                    let iter: Box<dyn Iterator<Item = &(&Vec<u8>, &f64)>> = if let Some(c) = count {
                        Box::new(sorted.iter().skip(off).take(c))
                    } else {
                        Box::new(sorted.iter().skip(off))
                    };
                    let mut results = Vec::new();
                    for (member, score) in iter {
                        results.push(CommandResponse::BulkString(member.to_vec()));
                        if withscores {
                            results.push(CommandResponse::BulkString(
                                format!("{}", score).into_bytes(),
                            ));
                        }
                    }
                    CommandResponse::Array(results)
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Array(vec![]),
        }
    }

    fn cmd_zincrby(&mut self, key: &[u8], delta: f64, member: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let zset = self.get_or_create_sorted_set(&compact);
        match zset {
            Ok(map) => {
                let new_score = map.get(member).copied().unwrap_or(0.0) + delta;
                map.insert(member.to_vec(), new_score);
                CommandResponse::BulkString(format!("{}", new_score).into_bytes())
            }
            Err(e) => e,
        }
    }

    fn cmd_zcount(&mut self, key: &[u8], min: f64, max: f64) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::SortedSet(map) => {
                    let count = map.values().filter(|s| **s >= min && **s <= max).count();
                    CommandResponse::Integer(count as i64)
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Integer(0),
        }
    }

    // ─── Vector operations ────────────────────────────────────────────

    #[cfg(feature = "vector")]
    fn cmd_vec_set(&mut self, key: &[u8], dimensions: usize, vector: &[f32]) -> CommandResponse {
        let compact = CompactKey::new(key);
        let index = self
            .vector_indexes
            .entry(compact)
            .or_insert_with(|| HnswIndex::new(dimensions, DistanceMetric::L2, 16, 200));

        if index.dim() != dimensions {
            return CommandResponse::Error(format!(
                "ERR dimension mismatch: index has {}, got {}",
                index.dim(),
                dimensions
            ));
        }

        let id = {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            for &v in vector {
                std::hash::Hasher::write(&mut hasher, &v.to_le_bytes());
            }
            std::hash::Hasher::finish(&hasher)
        };

        index.insert(id, vector);
        CommandResponse::Integer(id as i64)
    }

    #[cfg(feature = "vector")]
    fn cmd_vec_query(&self, key: &[u8], k: usize, vector: &[f32]) -> CommandResponse {
        let compact = CompactKey::new(key);
        let index = match self.vector_indexes.get(&compact) {
            Some(idx) => idx,
            None => return CommandResponse::Array(vec![]),
        };

        let results = index.search(vector, k, k.max(50));
        let items: Vec<CommandResponse> = results
            .into_iter()
            .map(|r| {
                CommandResponse::Array(vec![
                    CommandResponse::Integer(r.id as i64),
                    CommandResponse::BulkString(format!("{}", r.distance).into_bytes()),
                ])
            })
            .collect();
        CommandResponse::Array(items)
    }

    #[cfg(feature = "vector")]
    fn cmd_vec_del(&mut self, key: &[u8]) -> CommandResponse {
        let compact = CompactKey::new(key);
        if self.vector_indexes.remove(&compact).is_some() {
            CommandResponse::Integer(1)
        } else {
            CommandResponse::Integer(0)
        }
    }

    // ─── Helpers ─────────────────────────────────────────────────────

    fn lazy_expire(&mut self, key: &[u8]) {
        let compact = CompactKey::new(key);
        if self.is_expired(&compact) {
            self.entries.remove(&compact);
        }
    }

    fn is_expired(&self, key: &CompactKey) -> bool {
        self.entries
            .get(key)
            .is_some_and(|entry| entry.is_expired())
    }

    /// Update the LFU counter for a key on read access.
    fn touch_key(&mut self, key: &[u8]) {
        if let Some(entry) = self.entries.get_mut(&CompactKey::new(key)) {
            entry.touch_lfu();
        }
    }

    /// Evict keys using LFU sampling when memory pressure is detected.
    ///
    /// Samples 5 random keys and evicts the one with the lowest LFU counter.
    fn maybe_evict(&mut self) {
        if self.max_memory == 0 || self.memory_used < self.max_memory {
            return;
        }

        let sample_size = 5usize;
        let entry_count = self.entries.len();
        if entry_count == 0 {
            return;
        }

        self.eviction_counter = self.eviction_counter.wrapping_add(1);
        let start = (self.eviction_counter as usize).wrapping_mul(self.shard_id as usize + 1)
            % entry_count.max(1);

        let mut lowest_counter = u8::MAX;
        let mut lowest_key: Option<CompactKey> = None;

        for (i, (key, entry)) in self.entries.iter().enumerate() {
            if i < start {
                continue;
            }
            if i >= start + sample_size {
                break;
            }
            if entry.lfu_counter < lowest_counter {
                lowest_counter = entry.lfu_counter;
                lowest_key = Some(key.clone());
            }
        }

        if lowest_key.is_none() {
            for (key, entry) in self.entries.iter().take(sample_size) {
                if entry.lfu_counter < lowest_counter {
                    lowest_counter = entry.lfu_counter;
                    lowest_key = Some(key.clone());
                }
            }
        }

        if let Some(key) = lowest_key {
            let removed_size = self.estimate_entry_size(&key);
            self.entries.remove(&key);
            self.memory_used = self.memory_used.saturating_sub(removed_size);
        }
    }

    /// Estimate the memory footprint of a single entry.
    fn estimate_entry_size(&self, key: &CompactKey) -> usize {
        let key_size = key.as_bytes().len() + std::mem::size_of::<CompactKey>();
        let value_size = self
            .entries
            .get(key)
            .map(|e| Self::estimate_value_size(&e.value))
            .unwrap_or(0);
        key_size + value_size + std::mem::size_of::<KeyEntry>()
    }

    /// Rough estimate of value memory usage.
    fn estimate_value_size(value: &Value) -> usize {
        match value {
            Value::InlineStr { .. } | Value::Int(_) => std::mem::size_of::<Value>(),
            Value::HeapStr(arc) => std::mem::size_of::<Value>() + arc.len(),
            Value::List(deque) => {
                std::mem::size_of::<VecDeque<Value>>() + deque.len() * std::mem::size_of::<Value>()
            }
            Value::Hash(map) => {
                std::mem::size_of::<HashMap<CompactKey, Value>>()
                    + map.len() * (std::mem::size_of::<CompactKey>() + std::mem::size_of::<Value>())
            }
            Value::Set(set) => {
                std::mem::size_of::<HashSet<Value>>() + set.len() * std::mem::size_of::<Value>()
            }
            _ => std::mem::size_of::<Value>(),
        }
    }

    fn get_or_create_list(
        &mut self,
        key: &CompactKey,
    ) -> Result<&mut VecDeque<Value>, CommandResponse> {
        if !self.entries.contains_key(key) {
            let entry = KeyEntry::new(key.clone(), Value::List(VecDeque::new()));
            self.entries.insert(key.clone(), entry);
        }
        let entry = self.entries.get_mut(key).ok_or_else(|| {
            CommandResponse::Error("ERR internal: key not found after insert".into())
        })?;
        match &mut entry.value {
            Value::List(deque) => Ok(deque),
            _ => Err(CommandResponse::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            )),
        }
    }

    fn get_or_create_hash(
        &mut self,
        key: &CompactKey,
    ) -> Result<&mut HashMap<CompactKey, Value>, CommandResponse> {
        if !self.entries.contains_key(key) {
            let entry = KeyEntry::new(key.clone(), Value::Hash(HashMap::new()));
            self.entries.insert(key.clone(), entry);
        }
        let entry = self.entries.get_mut(key).ok_or_else(|| {
            CommandResponse::Error("ERR internal: key not found after insert".into())
        })?;
        match &mut entry.value {
            Value::Hash(map) => Ok(map),
            _ => Err(CommandResponse::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            )),
        }
    }

    fn get_or_create_sorted_set(
        &mut self,
        key: &CompactKey,
    ) -> Result<&mut BTreeMap<Vec<u8>, f64>, CommandResponse> {
        if !self.entries.contains_key(key) {
            let entry = KeyEntry::new(key.clone(), Value::SortedSet(BTreeMap::new()));
            self.entries.insert(key.clone(), entry);
        }
        let entry = self.entries.get_mut(key).ok_or_else(|| {
            CommandResponse::Error("ERR internal: key not found after insert".into())
        })?;
        match &mut entry.value {
            Value::SortedSet(map) => Ok(map),
            _ => Err(CommandResponse::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            )),
        }
    }

    fn get_or_create_set(
        &mut self,
        key: &CompactKey,
    ) -> Result<&mut HashSet<Value>, CommandResponse> {
        if !self.entries.contains_key(key) {
            let entry = KeyEntry::new(key.clone(), Value::Set(HashSet::new()));
            self.entries.insert(key.clone(), entry);
        }
        let entry = self.entries.get_mut(key).ok_or_else(|| {
            CommandResponse::Error("ERR internal: key not found after insert".into())
        })?;
        match &mut entry.value {
            Value::Set(set) => Ok(set),
            _ => Err(CommandResponse::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            )),
        }
    }
}

/// Normalize a Redis-style index (supports negative indexing).
fn normalize_index(index: i64, len: i64) -> usize {
    if index < 0 {
        let normalized = len + index;
        if normalized < 0 {
            0
        } else {
            normalized as usize
        }
    } else {
        index as usize
    }
}

/// Simple glob pattern matching (supports * and ?).
fn glob_match(pattern: &str, key: &[u8]) -> bool {
    let key_str = match std::str::from_utf8(key) {
        Ok(s) => s,
        Err(_) => return false,
    };
    if pattern == "*" {
        return true;
    }
    glob_match_iterative(pattern.as_bytes(), key_str.as_bytes())
}

/// Iterative glob matching — O(n*m) worst case, no exponential blowup.
fn glob_match_iterative(pattern: &[u8], text: &[u8]) -> bool {
    let mut pi = 0; // pattern index
    let mut ti = 0; // text index
    let mut star_pi = usize::MAX; // pattern index after last *
    let mut star_ti = 0; // text index when last * was matched

    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = pi;
            star_ti = ti;
            pi += 1; // try matching * with empty string first
        } else if star_pi != usize::MAX {
            // backtrack: let * match one more character
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    // Consume trailing *'s in pattern
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get() {
        let mut store = ShardStore::new(0);
        store.execute(Command::Set {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });
        match store.execute(Command::Get {
            key: b"key1".to_vec(),
        }) {
            CommandResponse::BulkString(v) => assert_eq!(v, b"value1"),
            other => panic!("Expected BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_get_nonexistent() {
        let mut store = ShardStore::new(0);
        assert!(matches!(
            store.execute(Command::Get {
                key: b"nope".to_vec()
            }),
            CommandResponse::Nil
        ));
    }

    #[test]
    fn test_set_nx() {
        let mut store = ShardStore::new(0);
        store.execute(Command::Set {
            key: b"k".to_vec(),
            value: b"v1".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });
        // NX should fail if key exists
        let resp = store.execute(Command::SetNx {
            key: b"k".to_vec(),
            value: b"v2".to_vec(),
        });
        assert!(matches!(resp, CommandResponse::Nil));
        // Original value unchanged
        match store.execute(Command::Get { key: b"k".to_vec() }) {
            CommandResponse::BulkString(v) => assert_eq!(v, b"v1"),
            other => panic!("Expected v1, got {:?}", other),
        }
    }

    #[test]
    fn test_incr_decr() {
        let mut store = ShardStore::new(0);
        // INCR on nonexistent key starts at 0
        match store.execute(Command::Incr {
            key: b"counter".to_vec(),
        }) {
            CommandResponse::Integer(1) => {}
            other => panic!("Expected 1, got {:?}", other),
        }
        store.execute(Command::IncrBy {
            key: b"counter".to_vec(),
            delta: 10,
        });
        match store.execute(Command::Get {
            key: b"counter".to_vec(),
        }) {
            CommandResponse::BulkString(v) => assert_eq!(v, b"11"),
            other => panic!("Expected 11, got {:?}", other),
        }
        store.execute(Command::Decr {
            key: b"counter".to_vec(),
        });
        match store.execute(Command::Get {
            key: b"counter".to_vec(),
        }) {
            CommandResponse::BulkString(v) => assert_eq!(v, b"10"),
            other => panic!("Expected 10, got {:?}", other),
        }
    }

    #[test]
    fn test_del_multiple() {
        let mut store = ShardStore::new(0);
        store.execute(Command::Set {
            key: b"a".to_vec(),
            value: b"1".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });
        store.execute(Command::Set {
            key: b"b".to_vec(),
            value: b"2".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });
        match store.execute(Command::Del {
            keys: vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        }) {
            CommandResponse::Integer(2) => {}
            other => panic!("Expected 2, got {:?}", other),
        }
    }

    #[test]
    fn test_expire_and_ttl() {
        let mut store = ShardStore::new(0);
        store.execute(Command::Set {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ex: Some(100),
            px: None,
            nx: false,
            xx: false,
        });
        match store.execute(Command::Ttl { key: b"k".to_vec() }) {
            CommandResponse::Integer(n) => assert!(n > 0 && n <= 100),
            other => panic!("Expected positive TTL, got {:?}", other),
        }
    }

    #[test]
    fn test_type_command() {
        let mut store = ShardStore::new(0);
        store.execute(Command::Set {
            key: b"s".to_vec(),
            value: b"v".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });
        store.execute(Command::LPush {
            key: b"l".to_vec(),
            values: vec![b"a".to_vec()],
        });
        store.execute(Command::HSet {
            key: b"h".to_vec(),
            fields: vec![(b"f".to_vec(), b"v".to_vec())],
        });
        store.execute(Command::SAdd {
            key: b"set".to_vec(),
            members: vec![b"m".to_vec()],
        });

        assert!(
            matches!(store.execute(Command::Type { key: b"s".to_vec() }), CommandResponse::SimpleString(s) if s == "string")
        );
        assert!(
            matches!(store.execute(Command::Type { key: b"l".to_vec() }), CommandResponse::SimpleString(s) if s == "list")
        );
        assert!(
            matches!(store.execute(Command::Type { key: b"h".to_vec() }), CommandResponse::SimpleString(s) if s == "hash")
        );
        assert!(
            matches!(store.execute(Command::Type { key: b"set".to_vec() }), CommandResponse::SimpleString(s) if s == "set")
        );
        assert!(
            matches!(store.execute(Command::Type { key: b"none".to_vec() }), CommandResponse::SimpleString(s) if s == "none")
        );
    }

    #[test]
    fn test_list_operations() {
        let mut store = ShardStore::new(0);
        store.execute(Command::RPush {
            key: b"list".to_vec(),
            values: vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        });
        assert!(matches!(
            store.execute(Command::LLen {
                key: b"list".to_vec()
            }),
            CommandResponse::Integer(3)
        ));
        match store.execute(Command::LRange {
            key: b"list".to_vec(),
            start: 0,
            stop: -1,
        }) {
            CommandResponse::Array(items) => assert_eq!(items.len(), 3),
            other => panic!("Expected array of 3, got {:?}", other),
        }
        match store.execute(Command::LPop {
            key: b"list".to_vec(),
        }) {
            CommandResponse::BulkString(v) => assert_eq!(v, b"a"),
            other => panic!("Expected 'a', got {:?}", other),
        }
        match store.execute(Command::RPop {
            key: b"list".to_vec(),
        }) {
            CommandResponse::BulkString(v) => assert_eq!(v, b"c"),
            other => panic!("Expected 'c', got {:?}", other),
        }
    }

    #[test]
    fn test_hash_operations() {
        let mut store = ShardStore::new(0);
        store.execute(Command::HSet {
            key: b"hash".to_vec(),
            fields: vec![
                (b"name".to_vec(), b"Alice".to_vec()),
                (b"age".to_vec(), b"30".to_vec()),
            ],
        });
        match store.execute(Command::HGet {
            key: b"hash".to_vec(),
            field: b"name".to_vec(),
        }) {
            CommandResponse::BulkString(v) => assert_eq!(v, b"Alice"),
            other => panic!("Expected Alice, got {:?}", other),
        }
        assert!(matches!(
            store.execute(Command::HLen {
                key: b"hash".to_vec()
            }),
            CommandResponse::Integer(2)
        ));
        assert!(matches!(
            store.execute(Command::HExists {
                key: b"hash".to_vec(),
                field: b"name".to_vec()
            }),
            CommandResponse::Integer(1)
        ));
        store.execute(Command::HIncrBy {
            key: b"hash".to_vec(),
            field: b"age".to_vec(),
            delta: 5,
        });
        match store.execute(Command::HGet {
            key: b"hash".to_vec(),
            field: b"age".to_vec(),
        }) {
            CommandResponse::BulkString(v) => assert_eq!(v, b"35"),
            other => panic!("Expected 35, got {:?}", other),
        }
    }

    #[test]
    fn test_set_operations() {
        let mut store = ShardStore::new(0);
        store.execute(Command::SAdd {
            key: b"myset".to_vec(),
            members: vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        });
        assert!(matches!(
            store.execute(Command::SCard {
                key: b"myset".to_vec()
            }),
            CommandResponse::Integer(3)
        ));
        assert!(matches!(
            store.execute(Command::SIsMember {
                key: b"myset".to_vec(),
                member: b"a".to_vec()
            }),
            CommandResponse::Integer(1)
        ));
        assert!(matches!(
            store.execute(Command::SIsMember {
                key: b"myset".to_vec(),
                member: b"z".to_vec()
            }),
            CommandResponse::Integer(0)
        ));
        store.execute(Command::SRem {
            key: b"myset".to_vec(),
            members: vec![b"b".to_vec()],
        });
        assert!(matches!(
            store.execute(Command::SCard {
                key: b"myset".to_vec()
            }),
            CommandResponse::Integer(2)
        ));
    }

    #[test]
    fn test_wrongtype_error() {
        let mut store = ShardStore::new(0);
        store.execute(Command::Set {
            key: b"str".to_vec(),
            value: b"v".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });
        let resp = store.execute(Command::LPush {
            key: b"str".to_vec(),
            values: vec![b"x".to_vec()],
        });
        assert!(matches!(resp, CommandResponse::Error(s) if s.contains("WRONGTYPE")));
    }

    #[test]
    fn test_keys_pattern() {
        let mut store = ShardStore::new(0);
        store.execute(Command::Set {
            key: b"user:1".to_vec(),
            value: b"a".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });
        store.execute(Command::Set {
            key: b"user:2".to_vec(),
            value: b"b".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });
        store.execute(Command::Set {
            key: b"session:1".to_vec(),
            value: b"c".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });
        match store.execute(Command::Keys {
            pattern: "user:*".into(),
        }) {
            CommandResponse::Array(items) => assert_eq!(items.len(), 2),
            other => panic!("Expected 2 keys, got {:?}", other),
        }
    }

    #[test]
    fn test_ping_echo() {
        let mut store = ShardStore::new(0);
        assert!(
            matches!(store.execute(Command::Ping { message: None }), CommandResponse::SimpleString(s) if s == "PONG")
        );
        match store.execute(Command::Echo {
            message: b"hello".to_vec(),
        }) {
            CommandResponse::BulkString(v) => assert_eq!(v, b"hello"),
            other => panic!("Expected hello, got {:?}", other),
        }
    }

    #[test]
    fn test_glob_matching() {
        assert!(glob_match("*", b"anything"));
        assert!(glob_match("user:*", b"user:123"));
        assert!(!glob_match("user:*", b"session:123"));
        assert!(glob_match("h?llo", b"hello"));
        assert!(glob_match("h?llo", b"hallo"));
        assert!(!glob_match("h?llo", b"hlo"));
    }

    #[test]
    fn test_append() {
        let mut store = ShardStore::new(0);
        store.execute(Command::Append {
            key: b"k".to_vec(),
            value: b"Hello".to_vec(),
        });
        store.execute(Command::Append {
            key: b"k".to_vec(),
            value: b" World".to_vec(),
        });
        match store.execute(Command::Get { key: b"k".to_vec() }) {
            CommandResponse::BulkString(v) => assert_eq!(v, b"Hello World"),
            other => panic!("Expected 'Hello World', got {:?}", other),
        }
    }

    #[test]
    fn test_dbsize_and_flush() {
        let mut store = ShardStore::new(0);
        store.execute(Command::Set {
            key: b"a".to_vec(),
            value: b"1".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });
        store.execute(Command::Set {
            key: b"b".to_vec(),
            value: b"2".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });
        assert!(matches!(
            store.execute(Command::DbSize),
            CommandResponse::Integer(2)
        ));
        store.execute(Command::FlushDb);
        assert!(matches!(
            store.execute(Command::DbSize),
            CommandResponse::Integer(0)
        ));
    }

    #[test]
    fn test_lfu_counter_starts_at_5() {
        let mut store = ShardStore::new(0);
        store.execute(Command::Set {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });
        match store.execute(Command::ObjectFreq { key: b"k".to_vec() }) {
            CommandResponse::Integer(n) => assert_eq!(n, 5),
            other => panic!("Expected Integer(5), got {:?}", other),
        }
    }

    #[test]
    fn test_lfu_counter_increments_on_access() {
        let mut store = ShardStore::new(0);
        store.execute(Command::Set {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });
        for _ in 0..100 {
            store.execute(Command::Get { key: b"k".to_vec() });
        }
        match store.execute(Command::ObjectFreq { key: b"k".to_vec() }) {
            CommandResponse::Integer(n) => assert!(n >= 5, "LFU counter should be >= 5, got {}", n),
            other => panic!("Expected Integer, got {:?}", other),
        }
    }

    #[test]
    fn test_eviction_removes_least_frequent_key() {
        let mut store = ShardStore::new(0);
        store.set_max_memory(1);

        store.execute(Command::Set {
            key: b"cold".to_vec(),
            value: b"val".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });

        for _ in 0..50 {
            store.execute(Command::Get {
                key: b"cold".to_vec(),
            });
        }

        store.execute(Command::Set {
            key: b"hot".to_vec(),
            value: b"val".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });

        for _ in 0..200 {
            store.execute(Command::Get {
                key: b"hot".to_vec(),
            });
        }

        store.execute(Command::Set {
            key: b"trigger".to_vec(),
            value: b"val".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });

        assert!(
            store.len() < 4,
            "Eviction should have removed at least one key"
        );
    }

    #[test]
    fn test_object_freq_command() {
        let mut store = ShardStore::new(0);
        assert!(matches!(
            store.execute(Command::ObjectFreq {
                key: b"missing".to_vec()
            }),
            CommandResponse::Nil
        ));
        store.execute(Command::Set {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });
        match store.execute(Command::ObjectFreq { key: b"k".to_vec() }) {
            CommandResponse::Integer(n) => assert!(n >= 0),
            other => panic!("Expected Integer, got {:?}", other),
        }
    }

    #[test]
    fn test_object_encoding_command() {
        let mut store = ShardStore::new(0);
        store.execute(Command::Set {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });
        match store.execute(Command::ObjectEncoding { key: b"k".to_vec() }) {
            CommandResponse::BulkString(v) => {
                let encoding = String::from_utf8(v).unwrap_or_default();
                assert!(
                    encoding == "embstr" || encoding == "raw" || encoding == "int",
                    "Unexpected encoding: {}",
                    encoding
                );
            }
            other => panic!("Expected BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_zadd_and_zscore() {
        let mut store = ShardStore::new(0);
        let resp = store.execute(Command::ZAdd {
            key: b"zs".to_vec(),
            members: vec![
                (1.0, b"alice".to_vec()),
                (2.0, b"bob".to_vec()),
                (3.0, b"charlie".to_vec()),
            ],
        });
        assert_eq!(resp, CommandResponse::Integer(3));
        match store.execute(Command::ZScore {
            key: b"zs".to_vec(),
            member: b"bob".to_vec(),
        }) {
            CommandResponse::BulkString(v) => assert_eq!(v, b"2"),
            other => panic!("Expected BulkString, got {:?}", other),
        }
        assert!(matches!(
            store.execute(Command::ZScore {
                key: b"zs".to_vec(),
                member: b"unknown".to_vec(),
            }),
            CommandResponse::Nil
        ));
    }

    #[test]
    fn test_zadd_update_score() {
        let mut store = ShardStore::new(0);
        store.execute(Command::ZAdd {
            key: b"zs".to_vec(),
            members: vec![(1.0, b"alice".to_vec())],
        });
        let resp = store.execute(Command::ZAdd {
            key: b"zs".to_vec(),
            members: vec![(5.0, b"alice".to_vec())],
        });
        assert_eq!(resp, CommandResponse::Integer(0));
        match store.execute(Command::ZScore {
            key: b"zs".to_vec(),
            member: b"alice".to_vec(),
        }) {
            CommandResponse::BulkString(v) => assert_eq!(v, b"5"),
            other => panic!("Expected BulkString(5), got {:?}", other),
        }
    }

    #[test]
    fn test_zrem() {
        let mut store = ShardStore::new(0);
        store.execute(Command::ZAdd {
            key: b"zs".to_vec(),
            members: vec![
                (1.0, b"a".to_vec()),
                (2.0, b"b".to_vec()),
                (3.0, b"c".to_vec()),
            ],
        });
        let resp = store.execute(Command::ZRem {
            key: b"zs".to_vec(),
            members: vec![b"a".to_vec(), b"c".to_vec(), b"nonexistent".to_vec()],
        });
        assert_eq!(resp, CommandResponse::Integer(2));
        assert_eq!(
            store.execute(Command::ZCard {
                key: b"zs".to_vec()
            }),
            CommandResponse::Integer(1)
        );
    }

    #[test]
    fn test_zrank_and_zrevrank() {
        let mut store = ShardStore::new(0);
        store.execute(Command::ZAdd {
            key: b"zs".to_vec(),
            members: vec![
                (10.0, b"a".to_vec()),
                (20.0, b"b".to_vec()),
                (30.0, b"c".to_vec()),
            ],
        });
        assert_eq!(
            store.execute(Command::ZRank {
                key: b"zs".to_vec(),
                member: b"a".to_vec(),
            }),
            CommandResponse::Integer(0)
        );
        assert_eq!(
            store.execute(Command::ZRank {
                key: b"zs".to_vec(),
                member: b"c".to_vec(),
            }),
            CommandResponse::Integer(2)
        );
        assert_eq!(
            store.execute(Command::ZRevRank {
                key: b"zs".to_vec(),
                member: b"c".to_vec(),
            }),
            CommandResponse::Integer(0)
        );
        assert!(matches!(
            store.execute(Command::ZRank {
                key: b"zs".to_vec(),
                member: b"missing".to_vec(),
            }),
            CommandResponse::Nil
        ));
    }

    #[test]
    fn test_zrange_and_zrevrange() {
        let mut store = ShardStore::new(0);
        store.execute(Command::ZAdd {
            key: b"zs".to_vec(),
            members: vec![
                (1.0, b"a".to_vec()),
                (2.0, b"b".to_vec()),
                (3.0, b"c".to_vec()),
            ],
        });
        match store.execute(Command::ZRange {
            key: b"zs".to_vec(),
            start: 0,
            stop: -1,
            withscores: false,
        }) {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0], CommandResponse::BulkString(b"a".to_vec()));
                assert_eq!(items[2], CommandResponse::BulkString(b"c".to_vec()));
            }
            other => panic!("Expected Array, got {:?}", other),
        }
        match store.execute(Command::ZRevRange {
            key: b"zs".to_vec(),
            start: 0,
            stop: 1,
            withscores: true,
        }) {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 4);
                assert_eq!(items[0], CommandResponse::BulkString(b"c".to_vec()));
                assert_eq!(items[1], CommandResponse::BulkString(b"3".to_vec()));
                assert_eq!(items[2], CommandResponse::BulkString(b"b".to_vec()));
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_zrangebyscore() {
        let mut store = ShardStore::new(0);
        store.execute(Command::ZAdd {
            key: b"zs".to_vec(),
            members: vec![
                (1.0, b"a".to_vec()),
                (2.0, b"b".to_vec()),
                (3.0, b"c".to_vec()),
                (4.0, b"d".to_vec()),
            ],
        });
        match store.execute(Command::ZRangeByScore {
            key: b"zs".to_vec(),
            min: 2.0,
            max: 3.0,
            withscores: false,
            offset: None,
            count: None,
        }) {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], CommandResponse::BulkString(b"b".to_vec()));
                assert_eq!(items[1], CommandResponse::BulkString(b"c".to_vec()));
            }
            other => panic!("Expected Array, got {:?}", other),
        }
        match store.execute(Command::ZRangeByScore {
            key: b"zs".to_vec(),
            min: 1.0,
            max: 4.0,
            withscores: false,
            offset: Some(1),
            count: Some(2),
        }) {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], CommandResponse::BulkString(b"b".to_vec()));
                assert_eq!(items[1], CommandResponse::BulkString(b"c".to_vec()));
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_zincrby() {
        let mut store = ShardStore::new(0);
        store.execute(Command::ZAdd {
            key: b"zs".to_vec(),
            members: vec![(10.0, b"a".to_vec())],
        });
        match store.execute(Command::ZIncrBy {
            key: b"zs".to_vec(),
            delta: 5.0,
            member: b"a".to_vec(),
        }) {
            CommandResponse::BulkString(v) => assert_eq!(v, b"15"),
            other => panic!("Expected BulkString, got {:?}", other),
        }
        match store.execute(Command::ZIncrBy {
            key: b"zs".to_vec(),
            delta: 3.0,
            member: b"newmember".to_vec(),
        }) {
            CommandResponse::BulkString(v) => assert_eq!(v, b"3"),
            other => panic!("Expected BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_zcount() {
        let mut store = ShardStore::new(0);
        store.execute(Command::ZAdd {
            key: b"zs".to_vec(),
            members: vec![
                (1.0, b"a".to_vec()),
                (2.0, b"b".to_vec()),
                (3.0, b"c".to_vec()),
                (4.0, b"d".to_vec()),
            ],
        });
        assert_eq!(
            store.execute(Command::ZCount {
                key: b"zs".to_vec(),
                min: 2.0,
                max: 3.0,
            }),
            CommandResponse::Integer(2)
        );
        assert_eq!(
            store.execute(Command::ZCount {
                key: b"zs".to_vec(),
                min: f64::NEG_INFINITY,
                max: f64::INFINITY,
            }),
            CommandResponse::Integer(4)
        );
    }

    #[test]
    fn test_zcard() {
        let mut store = ShardStore::new(0);
        assert_eq!(
            store.execute(Command::ZCard {
                key: b"zs".to_vec()
            }),
            CommandResponse::Integer(0)
        );
        store.execute(Command::ZAdd {
            key: b"zs".to_vec(),
            members: vec![(1.0, b"a".to_vec()), (2.0, b"b".to_vec())],
        });
        assert_eq!(
            store.execute(Command::ZCard {
                key: b"zs".to_vec()
            }),
            CommandResponse::Integer(2)
        );
    }
}
