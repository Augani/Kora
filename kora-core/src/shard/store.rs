//! Per-shard key-value store with full command execution.
//!
//! `ShardStore` is the workhorse of the Kōra engine. Each shard worker thread
//! owns exactly one instance, so every operation runs single-threaded with no
//! locking. The store handles:
//!
//! - String, list, hash, set, sorted set, stream, HyperLogLog, bitmap, and
//!   geo commands.
//! - Lazy TTL expiration on access plus periodic bounded-sample sweeps.
//! - LFU-based eviction when a memory limit is configured.
//! - Optional per-command statistics recording (behind the `observability`
//!   feature gate).
//! - Optional HNSW vector index operations (behind the `vector` feature gate).

use std::collections::{hash_map::Entry, BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Duration;

use ahash::AHashMap;

use crate::command::{
    command_docs_response, command_help_response, command_info_response, command_list_response,
    supported_command_count, BitFieldEncoding, BitFieldOffset, BitFieldOperation, BitFieldOverflow,
    BitOperation, Command, CommandResponse, GeoUnit,
};
use crate::types::{
    CompactKey, KeyEntry, PendingEntry, StreamConsumerGroup, StreamEntry, StreamId, StreamLog,
    Value,
};

mod hash_commands;
mod list_commands;

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
    entries: AHashMap<CompactKey, KeyEntry>,
    shard_id: u16,
    max_memory: usize,
    memory_used: usize,
    eviction_counter: u64,
    expire_scan_cursor: usize,
    #[cfg(feature = "observability")]
    stats: ShardStats,
    #[cfg(feature = "observability")]
    stats_enabled: bool,
    #[cfg(feature = "vector")]
    vector_indexes: AHashMap<CompactKey, HnswIndex>,
    stream_groups: AHashMap<CompactKey, AHashMap<String, StreamConsumerGroup>>,
}

impl ShardStore {
    /// Create a new empty shard store.
    pub fn new(shard_id: u16) -> Self {
        Self {
            entries: AHashMap::new(),
            shard_id,
            max_memory: 0,
            memory_used: 0,
            eviction_counter: 0,
            expire_scan_cursor: 0,
            #[cfg(feature = "observability")]
            stats: ShardStats::new(),
            #[cfg(feature = "observability")]
            stats_enabled: false,
            #[cfg(feature = "vector")]
            vector_indexes: AHashMap::new(),
            stream_groups: AHashMap::new(),
        }
    }

    /// Get a reference to the shard stats (observability feature).
    #[cfg(feature = "observability")]
    pub fn stats(&self) -> &ShardStats {
        &self.stats
    }

    /// Enable or disable per-command statistics recording.
    #[cfg(feature = "observability")]
    pub fn set_stats_enabled(&mut self, enabled: bool) {
        self.stats_enabled = enabled;
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

    /// Iterate over all entries in this shard.
    pub fn entries_iter(&self) -> impl Iterator<Item = (&CompactKey, &KeyEntry)> {
        self.entries.iter()
    }

    /// Get a reference to a key entry.
    pub fn get_entry(&self, key: &CompactKey) -> Option<&KeyEntry> {
        self.entries.get(key)
    }

    /// Execute a GET operation against a borrowed key.
    pub fn get_bytes(&mut self, key: &[u8]) -> CommandResponse {
        self.cmd_get(key)
    }

    /// Execute a SET-like operation against borrowed key and value bytes.
    pub fn set_bytes(
        &mut self,
        key: &[u8],
        value: &[u8],
        ex: Option<u64>,
        px: Option<u64>,
        nx: bool,
        xx: bool,
    ) -> CommandResponse {
        self.cmd_set(key, value, ex, px, nx, xx)
    }

    /// Execute INCRBY/DECRBY style mutation against a borrowed key.
    pub fn incr_by_bytes(&mut self, key: &[u8], delta: i64) -> CommandResponse {
        self.cmd_incrby(key, delta)
    }

    /// Get a mutable reference to a key entry.
    pub fn get_entry_mut(&mut self, key: &CompactKey) -> Option<&mut KeyEntry> {
        self.entries.get_mut(key)
    }

    /// Insert a key entry directly (used by migration and restore paths).
    pub fn insert_entry(&mut self, key: CompactKey, entry: KeyEntry) {
        let size = Self::estimate_key_entry_size(key.as_bytes(), &entry.value);
        self.memory_used += size;
        self.entries.insert(key, entry);
    }

    /// Replace a key's value with a tier reference after demotion.
    pub fn mark_demoted(&mut self, key: &CompactKey, tier: u8, ref_hash: u64) {
        if let Some(entry) = self.entries.get_mut(key) {
            let old_size = entry.value.estimated_size();
            entry.value = match tier {
                crate::types::TIER_WARM => Value::WarmRef(ref_hash),
                _ => Value::ColdRef(ref_hash),
            };
            let new_size = entry.value.estimated_size();
            if old_size > new_size {
                self.memory_used = self.memory_used.saturating_sub(old_size - new_size);
            }
            entry.set_tier(tier);
        }
    }

    /// Promote a key back to the hot tier with the given value.
    pub fn promote(&mut self, key: &CompactKey, value: Value) {
        if let Some(entry) = self.entries.get_mut(key) {
            let old_size = entry.value.estimated_size();
            let new_size = value.estimated_size();
            entry.value = value;
            entry.set_tier(crate::types::TIER_HOT);
            entry.lfu_counter = 5;
            if new_size > old_size {
                let delta = new_size - old_size;
                self.memory_used += delta;
            } else {
                let delta = old_size - new_size;
                self.memory_used = self.memory_used.saturating_sub(delta);
            }
        }
    }

    /// Remove all expired keys, returning the count removed.
    pub fn evict_expired(&mut self) -> usize {
        let expired_keys: Vec<CompactKey> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.is_expired())
            .map(|(key, _)| key.clone())
            .collect();
        for key in &expired_keys {
            let _ = self.remove_compact_entry(key);
        }
        expired_keys.len()
    }

    /// Remove expired keys by scanning a bounded sample of entries.
    ///
    /// This avoids latency spikes from full-map sweeps on the hot path.
    pub fn evict_expired_sample(&mut self, sample_size: usize) -> usize {
        if sample_size == 0 || self.entries.is_empty() {
            return 0;
        }

        let len = self.entries.len();
        let start = self.expire_scan_cursor % len;
        let mut examined = 0usize;
        let mut expired_keys = Vec::new();

        for (key, entry) in self.entries.iter().skip(start) {
            if examined >= sample_size {
                break;
            }
            if entry.is_expired() {
                expired_keys.push(key.clone());
            }
            examined += 1;
        }

        if examined < sample_size {
            for (key, entry) in self.entries.iter().take(start) {
                if examined >= sample_size {
                    break;
                }
                if entry.is_expired() {
                    expired_keys.push(key.clone());
                }
                examined += 1;
            }
        }

        for key in &expired_keys {
            let _ = self.remove_compact_entry(key);
        }
        self.expire_scan_cursor = self.expire_scan_cursor.wrapping_add(examined);

        expired_keys.len()
    }

    /// Remove all keys from this shard.
    pub fn flush(&mut self) {
        self.entries.clear();
        self.memory_used = 0;
        self.expire_scan_cursor = 0;
        self.stream_groups.clear();
    }

    /// Execute a command on this shard and return the response.
    pub fn execute(&mut self, cmd: Command) -> CommandResponse {
        #[cfg(feature = "observability")]
        let track_stats = self.stats_enabled;
        #[cfg(feature = "observability")]
        let start = if track_stats {
            Some(std::time::Instant::now())
        } else {
            None
        };
        #[cfg(feature = "observability")]
        let cmd_type = if track_stats {
            Some(cmd.cmd_type() as usize)
        } else {
            None
        };

        if cmd.is_mutation() {
            self.maybe_evict();
        }

        // LFU touches are only needed when eviction is active (max_memory > 0).
        let should_touch_lfu = !cmd.is_mutation() && self.max_memory > 0;
        let cmd_key = if should_touch_lfu {
            cmd.key().map(CompactKey::new)
        } else {
            None
        };

        let response = self.execute_inner(cmd);

        if let Some(key) = cmd_key {
            self.touch_key(&key);
        }

        #[cfg(feature = "observability")]
        {
            if let (Some(start), Some(cmd_type)) = (start, cmd_type) {
                let duration_ns = start.elapsed().as_nanos() as u64;
                self.stats.record_command(cmd_type, duration_ns);
                self.stats.set_key_count(self.entries.len() as u64);
            }
        }

        response
    }

    fn execute_inner(&mut self, cmd: Command) -> CommandResponse {
        match cmd {
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
            Command::SetNx { key, value } => {
                match self.cmd_set(&key, &value, None, None, true, false) {
                    CommandResponse::Ok => CommandResponse::Integer(1),
                    CommandResponse::Nil => CommandResponse::Integer(0),
                    other => other,
                }
            }
            Command::IncrByFloat { key, delta } => self.cmd_incrbyfloat(&key, delta),
            Command::GetRange { key, start, end } => self.cmd_getrange(&key, start, end),
            Command::SetRange { key, offset, value } => self.cmd_setrange(&key, offset, &value),
            Command::GetDel { key } => self.cmd_getdel(&key),
            Command::GetEx {
                key,
                ex,
                px,
                exat,
                pxat,
                persist,
            } => self.cmd_getex(&key, ex, px, exat, pxat, persist),
            Command::MSetNx { entries } => self.cmd_msetnx(&entries),

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
            Command::ExpireAt { key, timestamp } => self.cmd_expireat(&key, timestamp, false),
            Command::PExpireAt { key, timestamp_ms } => self.cmd_expireat(&key, timestamp_ms, true),
            Command::Rename { key, newkey } => self.cmd_rename(&key, &newkey, false),
            Command::RenameNx { key, newkey } => self.cmd_rename(&key, &newkey, true),
            Command::Unlink { keys } => {
                let count = keys.iter().filter(|k| self.del(k)).count();
                CommandResponse::Integer(count as i64)
            }
            Command::Copy {
                source,
                destination,
                replace,
            } => self.cmd_copy(&source, &destination, replace),
            Command::RandomKey => self.cmd_randomkey(),
            Command::Touch { keys } => {
                let count = keys.iter().filter(|k| self.exists(k)).count();
                CommandResponse::Integer(count as i64)
            }
            Command::ObjectRefCount { ref key } => {
                let compact = CompactKey::new(key);
                match self.entries.get(&compact) {
                    Some(entry) if !entry.is_expired() => CommandResponse::Integer(1),
                    _ => CommandResponse::Nil,
                }
            }
            Command::ObjectIdleTime { ref key } => {
                let compact = CompactKey::new(key);
                match self.entries.get(&compact) {
                    Some(entry) if !entry.is_expired() => CommandResponse::Integer(0),
                    _ => CommandResponse::Nil,
                }
            }
            Command::ObjectHelp => CommandResponse::Array(vec![
                CommandResponse::BulkString(b"OBJECT subcommand [arguments]".to_vec()),
                CommandResponse::BulkString(
                    b"ENCODING <key> - Return the encoding of a key.".to_vec(),
                ),
                CommandResponse::BulkString(
                    b"FREQ <key> - Return the access frequency of a key.".to_vec(),
                ),
                CommandResponse::BulkString(b"HELP - Return this help message.".to_vec()),
                CommandResponse::BulkString(
                    b"IDLETIME <key> - Return the idle time of a key.".to_vec(),
                ),
                CommandResponse::BulkString(
                    b"REFCOUNT <key> - Return the reference count of a key.".to_vec(),
                ),
            ]),
            Command::DbSize => CommandResponse::Integer(self.entries.len() as i64),
            Command::FlushDb | Command::FlushAll => {
                self.flush();
                CommandResponse::Ok
            }

            Command::LPush { key, values } => self.cmd_lpush(&key, &values),
            Command::RPush { key, values } => self.cmd_rpush(&key, &values),
            Command::LPop { key } => self.cmd_lpop(&key),
            Command::RPop { key } => self.cmd_rpop(&key),
            Command::LLen { key } => self.cmd_llen(&key),
            Command::LRange { key, start, stop } => self.cmd_lrange(&key, start, stop),
            Command::LIndex { key, index } => self.cmd_lindex(&key, index),
            Command::LSet { key, index, value } => self.cmd_lset(&key, index, &value),
            Command::LInsert {
                key,
                before,
                pivot,
                value,
            } => self.cmd_linsert(&key, before, &pivot, &value),
            Command::LRem { key, count, value } => self.cmd_lrem(&key, count, &value),
            Command::LTrim { key, start, stop } => self.cmd_ltrim(&key, start, stop),
            Command::LPos {
                key,
                value,
                rank,
                count,
                maxlen,
            } => self.cmd_lpos(&key, &value, rank, count, maxlen),
            Command::RPopLPush {
                source,
                destination,
            } => self.cmd_rpoplpush(&source, &destination),
            Command::LMove {
                source,
                destination,
                from_left,
                to_left,
            } => self.cmd_lmove(&source, &destination, from_left, to_left),

            Command::HSet { key, fields } => self.cmd_hset(&key, &fields),
            Command::HGet { key, field } => self.cmd_hget(&key, &field),
            Command::HDel { key, fields } => self.cmd_hdel(&key, &fields),
            Command::HGetAll { key } => self.cmd_hgetall(&key),
            Command::HLen { key } => self.cmd_hlen(&key),
            Command::HExists { key, field } => self.cmd_hexists(&key, &field),
            Command::HIncrBy { key, field, delta } => self.cmd_hincrby(&key, &field, delta),
            Command::HMGet { key, fields } => self.cmd_hmget(&key, &fields),
            Command::HKeys { key } => self.cmd_hkeys(&key),
            Command::HVals { key } => self.cmd_hvals(&key),
            Command::HSetNx { key, field, value } => self.cmd_hsetnx(&key, &field, &value),
            Command::HIncrByFloat { key, field, delta } => {
                self.cmd_hincrbyfloat(&key, &field, delta)
            }
            Command::HRandField {
                key,
                count,
                withvalues,
            } => self.cmd_hrandfield(&key, count, withvalues),
            Command::HScan {
                key,
                cursor,
                pattern,
                count,
            } => self.cmd_hscan(&key, cursor, pattern.as_deref(), count.unwrap_or(10)),

            Command::SAdd { key, members } => self.cmd_sadd(&key, &members),
            Command::SRem { key, members } => self.cmd_srem(&key, &members),
            Command::SMembers { key } => self.cmd_smembers(&key),
            Command::SIsMember { key, member } => self.cmd_sismember(&key, &member),
            Command::SCard { key } => self.cmd_scard(&key),
            Command::SPop { key, count } => self.cmd_spop(&key, count),
            Command::SRandMember { key, count } => self.cmd_srandmember(&key, count),
            Command::SUnion { keys } => self.cmd_sunion(&keys),
            Command::SUnionStore { destination, keys } => self.cmd_sunionstore(&destination, &keys),
            Command::SInter { keys } => self.cmd_sinter(&keys),
            Command::SInterStore { destination, keys } => self.cmd_sinterstore(&destination, &keys),
            Command::SDiff { keys } => self.cmd_sdiff(&keys),
            Command::SDiffStore { destination, keys } => self.cmd_sdiffstore(&destination, &keys),
            Command::SInterCard {
                numkeys: _,
                keys,
                limit,
            } => self.cmd_sintercard(&keys, limit),
            Command::SMove {
                source,
                destination,
                member,
            } => self.cmd_smove(&source, &destination, &member),
            Command::SMisMember { key, members } => self.cmd_smismember(&key, &members),
            Command::SScan {
                key,
                cursor,
                pattern,
                count,
            } => self.cmd_sscan(&key, cursor, pattern.as_deref(), count.unwrap_or(10)),

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
            Command::ZRevRangeByScore {
                key,
                max,
                min,
                withscores,
                offset,
                count,
            } => self.cmd_zrevrangebyscore(&key, max, min, withscores, offset, count),
            Command::ZPopMin { key, count } => self.cmd_zpopmin(&key, count),
            Command::ZPopMax { key, count } => self.cmd_zpopmax(&key, count),
            Command::ZRangeByLex {
                key,
                min,
                max,
                offset,
                count,
            } => self.cmd_zrangebylex(&key, &min, &max, offset, count, false),
            Command::ZRevRangeByLex {
                key,
                max,
                min,
                offset,
                count,
            } => self.cmd_zrangebylex(&key, &min, &max, offset, count, true),
            Command::ZLexCount { key, min, max } => self.cmd_zlexcount(&key, &min, &max),
            Command::ZMScore { key, members } => self.cmd_zmscore(&key, &members),
            Command::ZRandMember {
                key,
                count,
                withscores,
            } => self.cmd_zrandmember(&key, count, withscores),
            Command::ZScan {
                key,
                cursor,
                pattern,
                count,
            } => self.cmd_zscan(&key, cursor, pattern.as_deref(), count.unwrap_or(10)),

            Command::XAdd {
                key,
                id,
                fields,
                maxlen,
            } => self.cmd_xadd(&key, &id, &fields, maxlen),
            Command::XLen { key } => self.cmd_xlen(&key),
            Command::XRange {
                key,
                start,
                end,
                count,
            } => self.cmd_xrange(&key, &start, &end, count),
            Command::XRevRange {
                key,
                start,
                end,
                count,
            } => self.cmd_xrevrange(&key, &start, &end, count),
            Command::XRead { keys, ids, count } => self.cmd_xread(&keys, &ids, count),
            Command::XTrim { key, maxlen } => self.cmd_xtrim(&key, maxlen),
            Command::XDel { key, ids } => self.cmd_xdel(&key, &ids),
            Command::XGroupCreate {
                key,
                group,
                id,
                mkstream,
            } => self.cmd_xgroup_create(&key, &group, &id, mkstream),
            Command::XGroupDestroy { key, group } => self.cmd_xgroup_destroy(&key, &group),
            Command::XGroupDelConsumer {
                key,
                group,
                consumer,
            } => self.cmd_xgroup_delconsumer(&key, &group, &consumer),
            Command::XReadGroup {
                group,
                consumer,
                count,
                keys,
                ids,
            } => self.cmd_xreadgroup(&group, &consumer, count, &keys, &ids),
            Command::XAck { key, group, ids } => self.cmd_xack(&key, &group, &ids),
            Command::XPending {
                key,
                group,
                start,
                end,
                count,
            } => self.cmd_xpending(&key, &group, start.as_deref(), end.as_deref(), count),
            Command::XClaim {
                key,
                group,
                consumer,
                min_idle_time,
                ids,
            } => self.cmd_xclaim(&key, &group, &consumer, min_idle_time, &ids),
            Command::XAutoClaim {
                key,
                group,
                consumer,
                min_idle_time,
                start,
                count,
            } => self.cmd_xautoclaim(&key, &group, &consumer, min_idle_time, &start, count),
            Command::XInfoStream { key } => self.cmd_xinfo_stream(&key),
            Command::XInfoGroups { key } => self.cmd_xinfo_groups(&key),

            Command::BLPop { keys, .. } => {
                for key in &keys {
                    let resp = self.cmd_lpop(key);
                    if !matches!(resp, CommandResponse::Nil) {
                        return CommandResponse::Array(vec![
                            CommandResponse::BulkString(key.clone()),
                            resp,
                        ]);
                    }
                }
                CommandResponse::Nil
            }
            Command::BRPop { keys, .. } => {
                for key in &keys {
                    let resp = self.cmd_rpop(key);
                    if !matches!(resp, CommandResponse::Nil) {
                        return CommandResponse::Array(vec![
                            CommandResponse::BulkString(key.clone()),
                            resp,
                        ]);
                    }
                }
                CommandResponse::Nil
            }
            Command::BLMove {
                source,
                destination,
                from_left,
                to_left,
                ..
            } => self.cmd_lmove(&source, &destination, from_left, to_left),
            Command::BZPopMin { keys, .. } => {
                for key in &keys {
                    let resp = self.cmd_zpopmin(key, Some(1));
                    if let CommandResponse::Array(ref items) = resp {
                        if !items.is_empty() {
                            return CommandResponse::Array(vec![
                                CommandResponse::BulkString(key.clone()),
                                resp,
                            ]);
                        }
                    }
                }
                CommandResponse::Nil
            }
            Command::BZPopMax { keys, .. } => {
                for key in &keys {
                    let resp = self.cmd_zpopmax(key, Some(1));
                    if let CommandResponse::Array(ref items) = resp {
                        if !items.is_empty() {
                            return CommandResponse::Array(vec![
                                CommandResponse::BulkString(key.clone()),
                                resp,
                            ]);
                        }
                    }
                }
                CommandResponse::Nil
            }

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
            Command::CommandInfo { names } => command_info_response(&names),

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
            Command::StatsHotkeys { count } => self.cmd_stats_hotkeys(count),
            Command::StatsMemory { prefixes } => self.cmd_stats_memory(&prefixes),

            Command::PfAdd { key, elements } => self.cmd_pfadd(&key, &elements),
            Command::PfCount { keys } => {
                if keys.len() == 1 {
                    self.cmd_pfcount_single(&keys[0])
                } else {
                    self.cmd_pfcount_multi(&keys)
                }
            }
            Command::PfMerge {
                destkey,
                sourcekeys,
            } => self.cmd_pfmerge(&destkey, &sourcekeys),

            Command::SetBit { key, offset, value } => self.cmd_setbit(&key, offset, value),
            Command::GetBit { key, offset } => self.cmd_getbit(&key, offset),
            Command::BitCount {
                key,
                start,
                end,
                use_bit,
            } => self.cmd_bitcount(&key, start, end, use_bit),
            Command::BitOp {
                operation,
                destkey,
                keys,
            } => self.cmd_bitop(operation, &destkey, &keys),
            Command::BitPos {
                key,
                bit,
                start,
                end,
                use_bit,
            } => self.cmd_bitpos(&key, bit, start, end, use_bit),
            Command::BitField { key, operations } => self.cmd_bitfield(&key, &operations),

            Command::GeoAdd {
                key,
                nx,
                xx,
                ch,
                members,
            } => self.cmd_geoadd(&key, nx, xx, ch, &members),
            Command::GeoDist {
                key,
                member1,
                member2,
                unit,
            } => self.cmd_geodist(&key, &member1, &member2, unit),
            Command::GeoHash { key, members } => self.cmd_geohash(&key, &members),
            Command::GeoPos { key, members } => self.cmd_geopos(&key, &members),
            Command::GeoSearch {
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
            } => self.cmd_geosearch(
                &key,
                from_member.as_deref(),
                from_lonlat,
                radius,
                unit,
                asc,
                count,
                withcoord,
                withdist,
                withhash,
            ),

            Command::Multi
            | Command::Exec
            | Command::Discard
            | Command::Watch { .. }
            | Command::Unwatch => {
                CommandResponse::Error("ERR command handled at connection level".into())
            }

            Command::ClientId
            | Command::ClientGetName
            | Command::ClientSetName { .. }
            | Command::ClientList
            | Command::ClientInfo => {
                CommandResponse::Error("ERR command handled at connection level".into())
            }

            Command::ConfigGet { .. } => CommandResponse::Array(vec![]),
            Command::ConfigSet { .. } => {
                CommandResponse::Error("ERR unsupported CONFIG SET parameter".into())
            }
            Command::ConfigResetStat => CommandResponse::Ok,

            Command::Time => {
                use std::time::SystemTime;
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default();
                let secs = now.as_secs();
                let micros = now.subsec_micros() as u64;
                CommandResponse::Array(vec![
                    CommandResponse::BulkString(secs.to_string().into_bytes()),
                    CommandResponse::BulkString(micros.to_string().into_bytes()),
                ])
            }
            Command::Select { db } => {
                if db < 0 {
                    CommandResponse::Error("ERR DB index is out of range".into())
                } else if db == 0 {
                    CommandResponse::Ok
                } else {
                    CommandResponse::Error("ERR SELECT is not allowed in cluster mode".into())
                }
            }
            Command::Quit => CommandResponse::Ok,
            Command::Wait { timeout, .. } => {
                if timeout < 0 {
                    CommandResponse::Error("ERR timeout is negative".into())
                } else {
                    CommandResponse::Integer(0)
                }
            }
            Command::CommandCount => CommandResponse::Integer(supported_command_count()),
            Command::CommandList => command_list_response(),
            Command::CommandHelp => command_help_response(),
            Command::CommandDocs { names } => command_docs_response(&names),

            Command::BgSave
            | Command::BgRewriteAof
            | Command::Hello { .. }
            | Command::Auth { .. }
            | Command::CdcPoll { .. }
            | Command::CdcGroupCreate { .. }
            | Command::CdcGroupRead { .. }
            | Command::CdcAck { .. }
            | Command::CdcPending { .. }
            | Command::StatsLatency { .. }
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
            | Command::Subscribe { .. }
            | Command::Unsubscribe { .. }
            | Command::PSubscribe { .. }
            | Command::PUnsubscribe { .. }
            | Command::Publish { .. } => {
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
        match self.entries.get(key) {
            Some(entry) => match entry.value.bulk_response() {
                Some(resp) => resp,
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
        let ttl = Command::ttl_duration(ex, px);
        let compact = CompactKey::new(key);
        match self.entries.entry(compact) {
            Entry::Occupied(mut occupied) => {
                if occupied.get().is_expired() {
                    let (map_key, old_entry) = occupied.remove_entry();
                    let old_size =
                        Self::estimate_key_entry_size(map_key.as_bytes(), &old_entry.value);
                    self.memory_used = self.memory_used.saturating_sub(old_size);

                    if xx {
                        return CommandResponse::Nil;
                    }

                    let (entry, entry_size) = Self::build_string_entry(&map_key, value, ttl);
                    self.memory_used += entry_size;
                    self.entries.insert(map_key, entry);
                    CommandResponse::Ok
                } else {
                    if nx {
                        return CommandResponse::Nil;
                    }

                    let new_value = Value::from_raw_bytes(value);
                    let old_value_size = occupied.get().value.estimated_size();
                    let new_value_size = new_value.estimated_size();
                    let size_delta = new_value_size as isize - old_value_size as isize;

                    if size_delta >= 0 {
                        self.memory_used += size_delta as usize;
                    } else {
                        self.memory_used = self.memory_used.saturating_sub((-size_delta) as usize);
                    }

                    let entry = occupied.get_mut();
                    entry.value = new_value;
                    entry.client_flags = 0;
                    match ttl {
                        Some(dur) => entry.set_ttl(dur),
                        None => entry.ttl = None,
                    }
                    CommandResponse::Ok
                }
            }
            Entry::Vacant(vacant) => {
                if xx {
                    return CommandResponse::Nil;
                }

                let new_value = Value::from_raw_bytes(value);
                let entry_size = Self::estimate_key_entry_size(vacant.key().as_bytes(), &new_value);
                let mut entry = KeyEntry::new(vacant.key().clone(), new_value);
                if let Some(dur) = ttl {
                    entry.set_ttl(dur);
                }
                self.memory_used += entry_size;
                vacant.insert(entry);
                CommandResponse::Ok
            }
        }
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
                    entry.value = Value::from_raw_bytes(&existing);
                    CommandResponse::Integer(new_len as i64)
                }
                Value::HeapStr(arc) => {
                    let mut existing = arc.to_vec();
                    existing.extend_from_slice(value);
                    let new_len = existing.len();
                    entry.value = Value::from_raw_bytes(&existing);
                    CommandResponse::Integer(new_len as i64)
                }
                Value::Int(i) => {
                    let mut existing = i.to_string().into_bytes();
                    existing.extend_from_slice(value);
                    let new_len = existing.len();
                    entry.value = Value::from_raw_bytes(&existing);
                    CommandResponse::Integer(new_len as i64)
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => {
                let new_entry = KeyEntry::new(compact.clone(), Value::from_raw_bytes(value));
                self.entries.insert(compact, new_entry);
                CommandResponse::Integer(value.len() as i64)
            }
        }
    }

    fn cmd_strlen(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        match self.entries.get(key) {
            Some(entry) => match entry.value.string_len() {
                Some(len) => CommandResponse::Integer(len as i64),
                None => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Integer(0),
        }
    }

    fn cmd_incrby(&mut self, key: &[u8], delta: i64) -> CommandResponse {
        self.lazy_expire(key);
        match self.entries.get_mut(key) {
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
                let compact = CompactKey::new(key);
                let entry = KeyEntry::new(compact.clone(), Value::Int(delta));
                self.entries.insert(compact, entry);
                CommandResponse::Integer(delta)
            }
        }
    }

    fn cmd_incrbyfloat(&mut self, key: &[u8], delta: f64) -> CommandResponse {
        self.lazy_expire(key);
        match self.entries.get_mut(key) {
            Some(entry) => {
                let current = match &entry.value {
                    Value::Int(i) => *i as f64,
                    Value::InlineStr { data, len } => {
                        match std::str::from_utf8(&data[..*len as usize])
                            .ok()
                            .and_then(|s| s.parse::<f64>().ok())
                        {
                            Some(f) => f,
                            None => {
                                return CommandResponse::Error(
                                    "ERR value is not a valid float".into(),
                                )
                            }
                        }
                    }
                    Value::HeapStr(arc) => {
                        match std::str::from_utf8(arc)
                            .ok()
                            .and_then(|s| s.parse::<f64>().ok())
                        {
                            Some(f) => f,
                            None => {
                                return CommandResponse::Error(
                                    "ERR value is not a valid float".into(),
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
                let result = current + delta;
                if result.is_infinite() || result.is_nan() {
                    return CommandResponse::Error(
                        "ERR increment would produce NaN or Infinity".into(),
                    );
                }
                let result_str = format_float(result);
                entry.value = Value::from_raw_bytes(result_str.as_bytes());
                CommandResponse::BulkString(result_str.into_bytes())
            }
            None => {
                if delta.is_infinite() || delta.is_nan() {
                    return CommandResponse::Error(
                        "ERR increment would produce NaN or Infinity".into(),
                    );
                }
                let result_str = format_float(delta);
                let compact = CompactKey::new(key);
                let entry = KeyEntry::new(
                    compact.clone(),
                    Value::from_raw_bytes(result_str.as_bytes()),
                );
                self.entries.insert(compact, entry);
                CommandResponse::BulkString(result_str.into_bytes())
            }
        }
    }

    fn cmd_getrange(&mut self, key: &[u8], start: i64, end: i64) -> CommandResponse {
        self.lazy_expire(key);
        match self.entries.get(key) {
            Some(entry) => {
                let bytes = match entry.value.as_bytes() {
                    Some(b) => b,
                    None => {
                        return CommandResponse::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .into(),
                        )
                    }
                };
                let len = bytes.len() as i64;
                if len == 0 {
                    return CommandResponse::BulkString(vec![]);
                }
                let s = if start < 0 {
                    (len + start).max(0) as usize
                } else {
                    start.min(len) as usize
                };
                let e = if end < 0 {
                    (len + end).max(0) as usize
                } else {
                    end.min(len - 1) as usize
                };
                if s > e || s >= bytes.len() {
                    CommandResponse::BulkString(vec![])
                } else {
                    CommandResponse::BulkString(bytes[s..=e].to_vec())
                }
            }
            None => CommandResponse::BulkString(vec![]),
        }
    }

    fn cmd_setrange(&mut self, key: &[u8], offset: usize, value: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let mut current = match self.entries.get(key) {
            Some(entry) => match entry.value.as_bytes() {
                Some(b) => b,
                None => {
                    return CommandResponse::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            },
            None => vec![],
        };
        let needed = offset + value.len();
        if current.len() < needed {
            current.resize(needed, 0u8);
        }
        current[offset..offset + value.len()].copy_from_slice(value);
        let new_len = current.len() as i64;
        let old_size = self
            .entries
            .get(key)
            .map(|e| Self::estimate_key_entry_size(key, &e.value))
            .unwrap_or(0);
        let new_value = Value::from_raw_bytes(&current);
        let new_size = Self::estimate_key_entry_size(key, &new_value);
        match self.entries.get_mut(key) {
            Some(entry) => {
                entry.value = new_value;
                if new_size > old_size {
                    self.memory_used += new_size - old_size;
                } else {
                    self.memory_used = self.memory_used.saturating_sub(old_size - new_size);
                }
            }
            None => {
                let entry = KeyEntry::new(compact.clone(), new_value);
                self.memory_used += new_size;
                self.entries.insert(compact, entry);
            }
        }
        CommandResponse::Integer(new_len)
    }

    fn cmd_getdel(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) if !entry.is_expired() => {
                let resp = match entry.value.bulk_response() {
                    Some(r) => r,
                    None => {
                        return CommandResponse::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .into(),
                        )
                    }
                };
                let _ = self.remove_compact_entry(&compact);
                resp
            }
            _ => CommandResponse::Nil,
        }
    }

    fn cmd_getex(
        &mut self,
        key: &[u8],
        ex: Option<u64>,
        px: Option<u64>,
        exat: Option<u64>,
        pxat: Option<u64>,
        persist: bool,
    ) -> CommandResponse {
        self.lazy_expire(key);
        match self.entries.get_mut(key) {
            Some(entry) if !entry.is_expired() => {
                let resp = match entry.value.bulk_response() {
                    Some(r) => r,
                    None => {
                        return CommandResponse::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .into(),
                        )
                    }
                };
                if let Some(s) = ex {
                    entry.set_ttl(Duration::from_secs(s));
                } else if let Some(ms) = px {
                    entry.set_ttl(Duration::from_millis(ms));
                } else if let Some(ts) = exat {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .unwrap_or_default();
                    let target = Duration::from_secs(ts);
                    if target > now {
                        entry.set_ttl(target - now);
                    } else {
                        let compact = CompactKey::new(key);
                        let _ = self.remove_compact_entry(&compact);
                        return resp;
                    }
                } else if let Some(ts_ms) = pxat {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .unwrap_or_default();
                    let target = Duration::from_millis(ts_ms);
                    if target > now {
                        entry.set_ttl(target - now);
                    } else {
                        let compact = CompactKey::new(key);
                        let _ = self.remove_compact_entry(&compact);
                        return resp;
                    }
                } else if persist {
                    entry.clear_ttl();
                }
                resp
            }
            _ => CommandResponse::Nil,
        }
    }

    fn cmd_msetnx(&mut self, entries: &[(Vec<u8>, Vec<u8>)]) -> CommandResponse {
        for (k, _) in entries {
            self.lazy_expire(k);
            if self.entries.contains_key(k.as_slice()) {
                return CommandResponse::Integer(0);
            }
        }
        for (k, v) in entries {
            self.cmd_set(k, v, None, None, false, false);
        }
        CommandResponse::Integer(1)
    }

    // ─── Key operations ──────────────────────────────────────────────

    fn del(&mut self, key: &[u8]) -> bool {
        let compact = CompactKey::new(key);
        self.remove_compact_entry(&compact).is_some()
    }

    fn exists(&mut self, key: &[u8]) -> bool {
        self.lazy_expire(key);
        self.entries.contains_key(key)
    }

    fn cmd_expire(&mut self, key: &[u8], duration: Duration) -> CommandResponse {
        match self.entries.get_mut(key) {
            Some(entry) if !entry.is_expired() => {
                entry.set_ttl(duration);
                CommandResponse::Integer(1)
            }
            _ => CommandResponse::Integer(0),
        }
    }

    fn cmd_persist(&mut self, key: &[u8]) -> CommandResponse {
        match self.entries.get_mut(key) {
            Some(entry) if !entry.is_expired() && entry.ttl.is_some() => {
                entry.clear_ttl();
                CommandResponse::Integer(1)
            }
            _ => CommandResponse::Integer(0),
        }
    }

    fn cmd_ttl(&mut self, key: &[u8], millis: bool) -> CommandResponse {
        self.lazy_expire(key);
        match self.entries.get(key) {
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
        match self.entries.get(key) {
            Some(entry) => CommandResponse::SimpleString(entry.value.type_name().to_string()),
            None => CommandResponse::SimpleString("none".to_string()),
        }
    }

    fn cmd_expireat(&mut self, key: &[u8], timestamp: u64, millis: bool) -> CommandResponse {
        match self.entries.get_mut(key) {
            Some(entry) if !entry.is_expired() => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .unwrap_or_default();
                let target = if millis {
                    Duration::from_millis(timestamp)
                } else {
                    Duration::from_secs(timestamp)
                };
                if target > now {
                    entry.set_ttl(target - now);
                    CommandResponse::Integer(1)
                } else {
                    let compact = CompactKey::new(key);
                    let _ = self.remove_compact_entry(&compact);
                    CommandResponse::Integer(1)
                }
            }
            _ => CommandResponse::Integer(0),
        }
    }

    fn cmd_rename(&mut self, key: &[u8], newkey: &[u8], nx: bool) -> CommandResponse {
        self.lazy_expire(key);
        let source_compact = CompactKey::new(key);
        let source_entry = match self.remove_compact_entry(&source_compact) {
            Some(e) => e,
            None => {
                return CommandResponse::Error("ERR no such key".into());
            }
        };
        self.lazy_expire(newkey);
        let dest_compact = CompactKey::new(newkey);
        if nx && self.entries.contains_key(&dest_compact) {
            let size = Self::estimate_key_entry_size(key, &source_entry.value);
            self.memory_used += size;
            self.entries.insert(source_compact, source_entry);
            return CommandResponse::Integer(0);
        }
        if let Some(_old) = self.remove_compact_entry(&dest_compact) {}
        let mut new_entry = KeyEntry::new(dest_compact.clone(), source_entry.value.clone());
        new_entry.ttl = source_entry.ttl;
        new_entry.lfu_counter = source_entry.lfu_counter;
        let size = Self::estimate_key_entry_size(newkey, &new_entry.value);
        self.memory_used += size;
        self.entries.insert(dest_compact, new_entry);
        if nx {
            CommandResponse::Integer(1)
        } else {
            CommandResponse::Ok
        }
    }

    fn cmd_copy(&mut self, source: &[u8], destination: &[u8], replace: bool) -> CommandResponse {
        self.lazy_expire(source);
        let src_compact = CompactKey::new(source);
        let (value, ttl) = match self.entries.get(&src_compact) {
            Some(entry) if !entry.is_expired() => (entry.value.clone(), entry.ttl),
            _ => return CommandResponse::Integer(0),
        };
        self.lazy_expire(destination);
        let dest_compact = CompactKey::new(destination);
        if self.entries.contains_key(&dest_compact) && !replace {
            return CommandResponse::Integer(0);
        }
        if replace {
            let _ = self.remove_compact_entry(&dest_compact);
        }
        let mut new_entry = KeyEntry::new(dest_compact.clone(), value);
        new_entry.ttl = ttl;
        let size = Self::estimate_key_entry_size(destination, &new_entry.value);
        self.memory_used += size;
        self.entries.insert(dest_compact, new_entry);
        CommandResponse::Integer(1)
    }

    fn cmd_randomkey(&mut self) -> CommandResponse {
        for (key, entry) in &self.entries {
            if !entry.is_expired() {
                return CommandResponse::BulkString(key.as_bytes().to_vec());
            }
        }
        CommandResponse::Nil
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
            .filter(|(key, _)| pattern.is_none_or(|p| glob_match(p, key.as_bytes())))
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

    fn cmd_stats_hotkeys(&self, count: usize) -> CommandResponse {
        let mut hot: Vec<(&CompactKey, u8)> = self
            .entries
            .iter()
            .filter(|(_, entry)| !entry.is_expired())
            .map(|(key, entry)| (key, entry.lfu_counter))
            .collect();
        hot.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| a.0.as_bytes().cmp(b.0.as_bytes()))
        });

        let items = hot
            .into_iter()
            .take(count)
            .map(|(key, freq)| {
                CommandResponse::Array(vec![
                    CommandResponse::BulkString(key.as_bytes().to_vec()),
                    CommandResponse::Integer(i64::from(freq)),
                ])
            })
            .collect();
        CommandResponse::Array(items)
    }

    fn cmd_stats_memory(&self, prefixes: &[Vec<u8>]) -> CommandResponse {
        let items: Vec<CommandResponse> = prefixes
            .iter()
            .map(|prefix| {
                let total: usize = self
                    .entries
                    .iter()
                    .filter(|(key, entry)| {
                        !entry.is_expired() && key.as_bytes().starts_with(prefix.as_slice())
                    })
                    .map(|(key, entry)| Self::estimate_key_entry_size(key.as_bytes(), &entry.value))
                    .sum();
                CommandResponse::Integer(total as i64)
            })
            .collect();
        CommandResponse::Array(items)
    }

    // List and hash command implementations live in dedicated modules.

    fn cmd_sadd(&mut self, key: &[u8], members: &[Vec<u8>]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let is_new = !self.entries.contains_key(&compact);
        let set = self.get_or_create_set(&compact);
        match set {
            Ok(set) => {
                let mut memory_delta: usize = 0;
                if is_new {
                    memory_delta += key.len()
                        + std::mem::size_of::<CompactKey>()
                        + std::mem::size_of::<KeyEntry>();
                }
                let mut count = 0usize;
                for m in members {
                    let val = Value::from_raw_bytes(m);
                    let size = val.estimated_size();
                    if set.insert(val) {
                        memory_delta += size;
                        count += 1;
                    }
                }
                self.memory_used += memory_delta;
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
                        .filter(|m| set.remove(&Value::from_raw_bytes(m)))
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
                        .map(|v| match v.bulk_response() {
                            Some(resp) => resp,
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
                    let val = Value::from_raw_bytes(member);
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

    fn cmd_spop(&mut self, key: &[u8], count: Option<usize>) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let result = match self.entries.get_mut(&compact) {
            Some(entry) => match &mut entry.value {
                Value::Set(set) => {
                    if set.is_empty() {
                        return if count.is_some() {
                            CommandResponse::Array(vec![])
                        } else {
                            CommandResponse::Nil
                        };
                    }
                    let n = count.unwrap_or(1);
                    let mut popped = Vec::with_capacity(n.min(set.len()));
                    for _ in 0..n {
                        if set.is_empty() {
                            break;
                        }
                        let idx = (self.eviction_counter as usize) % set.len();
                        self.eviction_counter = self.eviction_counter.wrapping_add(1);
                        let val = set.iter().nth(idx).cloned();
                        if let Some(v) = val {
                            set.remove(&v);
                            popped.push(v);
                        }
                    }
                    let is_empty = set.is_empty();
                    let resp = if count.is_some() {
                        CommandResponse::Array(
                            popped
                                .iter()
                                .map(|v| v.bulk_response().unwrap_or(CommandResponse::Nil))
                                .collect(),
                        )
                    } else {
                        popped
                            .first()
                            .and_then(|v| v.bulk_response())
                            .unwrap_or(CommandResponse::Nil)
                    };
                    (resp, is_empty)
                }
                _ => {
                    return CommandResponse::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            },
            None => {
                return if count.is_some() {
                    CommandResponse::Array(vec![])
                } else {
                    CommandResponse::Nil
                }
            }
        };
        if result.1 {
            self.entries.remove(&compact);
        }
        result.0
    }

    fn cmd_srandmember(&mut self, key: &[u8], count: Option<i64>) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Set(set) => {
                    if set.is_empty() {
                        return if count.is_some() {
                            CommandResponse::Array(vec![])
                        } else {
                            CommandResponse::Nil
                        };
                    }
                    let members: Vec<&Value> = set.iter().collect();
                    match count {
                        None => {
                            let idx = (self.eviction_counter as usize) % members.len();
                            self.eviction_counter = self.eviction_counter.wrapping_add(1);
                            members[idx].bulk_response().unwrap_or(CommandResponse::Nil)
                        }
                        Some(c) if c >= 0 => {
                            let take = (c as usize).min(members.len());
                            let start = (self.eviction_counter as usize) % members.len();
                            self.eviction_counter = self.eviction_counter.wrapping_add(take as u64);
                            let mut result = Vec::with_capacity(take);
                            for i in 0..take {
                                let idx = (start + i) % members.len();
                                result.push(
                                    members[idx].bulk_response().unwrap_or(CommandResponse::Nil),
                                );
                            }
                            CommandResponse::Array(result)
                        }
                        Some(c) => {
                            let abs_count = c.unsigned_abs() as usize;
                            let mut result = Vec::with_capacity(abs_count);
                            for i in 0..abs_count {
                                let idx = ((self.eviction_counter as usize) + i) % members.len();
                                result.push(
                                    members[idx].bulk_response().unwrap_or(CommandResponse::Nil),
                                );
                            }
                            self.eviction_counter =
                                self.eviction_counter.wrapping_add(abs_count as u64);
                            CommandResponse::Array(result)
                        }
                    }
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => {
                if count.is_some() {
                    CommandResponse::Array(vec![])
                } else {
                    CommandResponse::Nil
                }
            }
        }
    }

    fn collect_set_members(&mut self, key: &[u8]) -> Result<HashSet<Value>, CommandResponse> {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Set(set) => Ok(set.clone()),
                _ => Err(CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                )),
            },
            None => Ok(HashSet::new()),
        }
    }

    fn cmd_sunion(&mut self, keys: &[Vec<u8>]) -> CommandResponse {
        let mut result: HashSet<Value> = HashSet::new();
        for key in keys {
            match self.collect_set_members(key) {
                Ok(set) => {
                    for v in set {
                        result.insert(v);
                    }
                }
                Err(e) => return e,
            }
        }
        CommandResponse::Array(
            result
                .iter()
                .map(|v| v.bulk_response().unwrap_or(CommandResponse::Nil))
                .collect(),
        )
    }

    fn cmd_sunionstore(&mut self, destination: &[u8], keys: &[Vec<u8>]) -> CommandResponse {
        let mut result: HashSet<Value> = HashSet::new();
        for key in keys {
            match self.collect_set_members(key) {
                Ok(set) => {
                    for v in set {
                        result.insert(v);
                    }
                }
                Err(e) => return e,
            }
        }
        let count = result.len() as i64;
        self.del(destination);
        if !result.is_empty() {
            let compact = CompactKey::new(destination);
            let mut memory_delta = destination.len()
                + std::mem::size_of::<CompactKey>()
                + std::mem::size_of::<KeyEntry>();
            for v in &result {
                memory_delta += v.estimated_size();
            }
            let entry = KeyEntry::new(compact.clone(), Value::Set(result));
            self.entries.insert(compact, entry);
            self.memory_used += memory_delta;
        }
        CommandResponse::Integer(count)
    }

    fn cmd_sinter(&mut self, keys: &[Vec<u8>]) -> CommandResponse {
        if keys.is_empty() {
            return CommandResponse::Array(vec![]);
        }
        let first = match self.collect_set_members(&keys[0]) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let mut result = first;
        for key in &keys[1..] {
            match self.collect_set_members(key) {
                Ok(set) => {
                    result = result.intersection(&set).cloned().collect();
                }
                Err(e) => return e,
            }
        }
        CommandResponse::Array(
            result
                .iter()
                .map(|v| v.bulk_response().unwrap_or(CommandResponse::Nil))
                .collect(),
        )
    }

    fn cmd_sinterstore(&mut self, destination: &[u8], keys: &[Vec<u8>]) -> CommandResponse {
        if keys.is_empty() {
            self.del(destination);
            return CommandResponse::Integer(0);
        }
        let first = match self.collect_set_members(&keys[0]) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let mut result = first;
        for key in &keys[1..] {
            match self.collect_set_members(key) {
                Ok(set) => {
                    result = result.intersection(&set).cloned().collect();
                }
                Err(e) => return e,
            }
        }
        let count = result.len() as i64;
        self.del(destination);
        if !result.is_empty() {
            let compact = CompactKey::new(destination);
            let mut memory_delta = destination.len()
                + std::mem::size_of::<CompactKey>()
                + std::mem::size_of::<KeyEntry>();
            for v in &result {
                memory_delta += v.estimated_size();
            }
            let entry = KeyEntry::new(compact.clone(), Value::Set(result));
            self.entries.insert(compact, entry);
            self.memory_used += memory_delta;
        }
        CommandResponse::Integer(count)
    }

    fn cmd_sdiff(&mut self, keys: &[Vec<u8>]) -> CommandResponse {
        if keys.is_empty() {
            return CommandResponse::Array(vec![]);
        }
        let first = match self.collect_set_members(&keys[0]) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let mut result = first;
        for key in &keys[1..] {
            match self.collect_set_members(key) {
                Ok(set) => {
                    result = result.difference(&set).cloned().collect();
                }
                Err(e) => return e,
            }
        }
        CommandResponse::Array(
            result
                .iter()
                .map(|v| v.bulk_response().unwrap_or(CommandResponse::Nil))
                .collect(),
        )
    }

    fn cmd_sdiffstore(&mut self, destination: &[u8], keys: &[Vec<u8>]) -> CommandResponse {
        if keys.is_empty() {
            self.del(destination);
            return CommandResponse::Integer(0);
        }
        let first = match self.collect_set_members(&keys[0]) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let mut result = first;
        for key in &keys[1..] {
            match self.collect_set_members(key) {
                Ok(set) => {
                    result = result.difference(&set).cloned().collect();
                }
                Err(e) => return e,
            }
        }
        let count = result.len() as i64;
        self.del(destination);
        if !result.is_empty() {
            let compact = CompactKey::new(destination);
            let mut memory_delta = destination.len()
                + std::mem::size_of::<CompactKey>()
                + std::mem::size_of::<KeyEntry>();
            for v in &result {
                memory_delta += v.estimated_size();
            }
            let entry = KeyEntry::new(compact.clone(), Value::Set(result));
            self.entries.insert(compact, entry);
            self.memory_used += memory_delta;
        }
        CommandResponse::Integer(count)
    }

    fn cmd_sintercard(&mut self, keys: &[Vec<u8>], limit: Option<usize>) -> CommandResponse {
        if keys.is_empty() {
            return CommandResponse::Integer(0);
        }
        let first = match self.collect_set_members(&keys[0]) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let mut result = first;
        for key in &keys[1..] {
            match self.collect_set_members(key) {
                Ok(set) => {
                    result = result.intersection(&set).cloned().collect();
                }
                Err(e) => return e,
            }
        }
        let count = match limit {
            Some(lim) if lim > 0 => result.len().min(lim),
            _ => result.len(),
        };
        CommandResponse::Integer(count as i64)
    }

    fn cmd_smove(&mut self, source: &[u8], destination: &[u8], member: &[u8]) -> CommandResponse {
        self.lazy_expire(source);
        self.lazy_expire(destination);
        let val = Value::from_raw_bytes(member);
        let src_compact = CompactKey::new(source);
        let removed = match self.entries.get_mut(&src_compact) {
            Some(entry) => match &mut entry.value {
                Value::Set(set) => set.remove(&val),
                _ => {
                    return CommandResponse::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            },
            None => return CommandResponse::Integer(0),
        };
        if !removed {
            return CommandResponse::Integer(0);
        }
        let src_empty = self
            .entries
            .get(&src_compact)
            .map(|e| matches!(&e.value, Value::Set(s) if s.is_empty()))
            .unwrap_or(false);
        if src_empty {
            self.entries.remove(&src_compact);
        }
        let dst_compact = CompactKey::new(destination);
        let dst_set = self.get_or_create_set(&dst_compact);
        match dst_set {
            Ok(set) => {
                set.insert(val);
            }
            Err(e) => return e,
        }
        CommandResponse::Integer(1)
    }

    fn cmd_smismember(&mut self, key: &[u8], members: &[Vec<u8>]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Set(set) => {
                    let results: Vec<CommandResponse> = members
                        .iter()
                        .map(|m| {
                            let val = Value::from_raw_bytes(m);
                            CommandResponse::Integer(if set.contains(&val) { 1 } else { 0 })
                        })
                        .collect();
                    CommandResponse::Array(results)
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Array(vec![CommandResponse::Integer(0); members.len()]),
        }
    }

    fn cmd_sscan(
        &mut self,
        key: &[u8],
        cursor: u64,
        pattern: Option<&str>,
        count: usize,
    ) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Set(set) => {
                    let mut members: Vec<&Value> = set
                        .iter()
                        .filter(|v| {
                            pattern.is_none_or(|p| {
                                if let Some(CommandResponse::BulkString(b)) = v.bulk_response() {
                                    glob_match(p, &b)
                                } else {
                                    false
                                }
                            })
                        })
                        .collect();
                    members.sort_by(|a, b| {
                        let ab = a.to_bytes();
                        let bb = b.to_bytes();
                        ab.cmp(&bb)
                    });

                    let start = cursor as usize;
                    let end = (start + count).min(members.len());

                    if start >= members.len() {
                        return CommandResponse::Array(vec![
                            CommandResponse::BulkString(b"0".to_vec()),
                            CommandResponse::Array(vec![]),
                        ]);
                    }

                    let mut results = Vec::with_capacity(end - start);
                    for v in &members[start..end] {
                        results.push(v.bulk_response().unwrap_or(CommandResponse::Nil));
                    }

                    let next_cursor = if end >= members.len() { 0 } else { end as u64 };

                    CommandResponse::Array(vec![
                        CommandResponse::BulkString(next_cursor.to_string().into_bytes()),
                        CommandResponse::Array(results),
                    ])
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Array(vec![
                CommandResponse::BulkString(b"0".to_vec()),
                CommandResponse::Array(vec![]),
            ]),
        }
    }

    // ─── Sorted Set operations ────────────────────────────────────────

    fn cmd_zadd(&mut self, key: &[u8], members: &[(f64, Vec<u8>)]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let is_new = !self.entries.contains_key(&compact);
        let zset = self.get_or_create_sorted_set(&compact);
        match zset {
            Ok(map) => {
                let mut memory_delta: usize = 0;
                if is_new {
                    memory_delta += key.len()
                        + std::mem::size_of::<CompactKey>()
                        + std::mem::size_of::<KeyEntry>();
                }
                let mut added = 0i64;
                let member_overhead = std::mem::size_of::<f64>();
                for (score, member) in members {
                    if map.insert(member.clone(), *score).is_none() {
                        memory_delta += member.len() + member_overhead;
                        added += 1;
                    }
                }
                self.memory_used += memory_delta;
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

    fn cmd_zrevrangebyscore(
        &mut self,
        key: &[u8],
        max: f64,
        min: f64,
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
                        b.1.partial_cmp(a.1)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| b.0.cmp(a.0))
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

    fn cmd_zpopmin(&mut self, key: &[u8], count: Option<usize>) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let result = match self.entries.get_mut(&compact) {
            Some(entry) => match &mut entry.value {
                Value::SortedSet(map) => {
                    if map.is_empty() {
                        return CommandResponse::Array(vec![]);
                    }
                    let n = count.unwrap_or(1);
                    let mut sorted: Vec<(Vec<u8>, f64)> =
                        map.iter().map(|(m, s)| (m.clone(), *s)).collect();
                    sorted.sort_by(|a, b| {
                        a.1.partial_cmp(&b.1)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| a.0.cmp(&b.0))
                    });
                    let take = n.min(sorted.len());
                    let mut results = Vec::with_capacity(take * 2);
                    for (member, score) in sorted.iter().take(take) {
                        map.remove(member);
                        results.push(CommandResponse::BulkString(member.clone()));
                        results.push(CommandResponse::BulkString(
                            format!("{}", score).into_bytes(),
                        ));
                    }
                    let is_empty = map.is_empty();
                    (CommandResponse::Array(results), is_empty)
                }
                _ => {
                    return CommandResponse::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            },
            None => return CommandResponse::Array(vec![]),
        };
        if result.1 {
            self.entries.remove(&compact);
        }
        result.0
    }

    fn cmd_zpopmax(&mut self, key: &[u8], count: Option<usize>) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let result = match self.entries.get_mut(&compact) {
            Some(entry) => match &mut entry.value {
                Value::SortedSet(map) => {
                    if map.is_empty() {
                        return CommandResponse::Array(vec![]);
                    }
                    let n = count.unwrap_or(1);
                    let mut sorted: Vec<(Vec<u8>, f64)> =
                        map.iter().map(|(m, s)| (m.clone(), *s)).collect();
                    sorted.sort_by(|a, b| {
                        b.1.partial_cmp(&a.1)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| b.0.cmp(&a.0))
                    });
                    let take = n.min(sorted.len());
                    let mut results = Vec::with_capacity(take * 2);
                    for (member, score) in sorted.iter().take(take) {
                        map.remove(member);
                        results.push(CommandResponse::BulkString(member.clone()));
                        results.push(CommandResponse::BulkString(
                            format!("{}", score).into_bytes(),
                        ));
                    }
                    let is_empty = map.is_empty();
                    (CommandResponse::Array(results), is_empty)
                }
                _ => {
                    return CommandResponse::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            },
            None => return CommandResponse::Array(vec![]),
        };
        if result.1 {
            self.entries.remove(&compact);
        }
        result.0
    }

    fn cmd_zrangebylex(
        &mut self,
        key: &[u8],
        min: &[u8],
        max: &[u8],
        offset: Option<usize>,
        count: Option<usize>,
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
                    let filtered: Vec<&Vec<u8>> = sorted
                        .iter()
                        .map(|(m, _)| *m)
                        .filter(|m| lex_in_range(m, min, max))
                        .collect();
                    let off = offset.unwrap_or(0);
                    let iter: Box<dyn Iterator<Item = &&Vec<u8>>> = if let Some(c) = count {
                        Box::new(filtered.iter().skip(off).take(c))
                    } else {
                        Box::new(filtered.iter().skip(off))
                    };
                    let results: Vec<CommandResponse> = iter
                        .map(|m| CommandResponse::BulkString((*m).clone()))
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

    fn cmd_zlexcount(&mut self, key: &[u8], min: &[u8], max: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::SortedSet(map) => {
                    let count = map.keys().filter(|m| lex_in_range(m, min, max)).count();
                    CommandResponse::Integer(count as i64)
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Integer(0),
        }
    }

    fn cmd_zmscore(&mut self, key: &[u8], members: &[Vec<u8>]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::SortedSet(map) => {
                    let results: Vec<CommandResponse> = members
                        .iter()
                        .map(|m| match map.get(m.as_slice()) {
                            Some(score) => {
                                CommandResponse::BulkString(format!("{}", score).into_bytes())
                            }
                            None => CommandResponse::Nil,
                        })
                        .collect();
                    CommandResponse::Array(results)
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Array(vec![CommandResponse::Nil; members.len()]),
        }
    }

    fn cmd_zrandmember(
        &mut self,
        key: &[u8],
        count: Option<i64>,
        withscores: bool,
    ) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::SortedSet(map) => {
                    if map.is_empty() {
                        return if count.is_some() {
                            CommandResponse::Array(vec![])
                        } else {
                            CommandResponse::Nil
                        };
                    }
                    let members: Vec<(&Vec<u8>, &f64)> = map.iter().collect();
                    match count {
                        None => {
                            let idx = (self.eviction_counter as usize) % members.len();
                            self.eviction_counter = self.eviction_counter.wrapping_add(1);
                            CommandResponse::BulkString(members[idx].0.clone())
                        }
                        Some(c) if c >= 0 => {
                            let take = (c as usize).min(members.len());
                            let start = (self.eviction_counter as usize) % members.len();
                            self.eviction_counter = self.eviction_counter.wrapping_add(take as u64);
                            let mut result =
                                Vec::with_capacity(if withscores { take * 2 } else { take });
                            for i in 0..take {
                                let idx = (start + i) % members.len();
                                result.push(CommandResponse::BulkString(members[idx].0.clone()));
                                if withscores {
                                    result.push(CommandResponse::BulkString(
                                        format!("{}", members[idx].1).into_bytes(),
                                    ));
                                }
                            }
                            CommandResponse::Array(result)
                        }
                        Some(c) => {
                            let abs_count = c.unsigned_abs() as usize;
                            let mut result = Vec::with_capacity(if withscores {
                                abs_count * 2
                            } else {
                                abs_count
                            });
                            for i in 0..abs_count {
                                let idx = ((self.eviction_counter as usize) + i) % members.len();
                                result.push(CommandResponse::BulkString(members[idx].0.clone()));
                                if withscores {
                                    result.push(CommandResponse::BulkString(
                                        format!("{}", members[idx].1).into_bytes(),
                                    ));
                                }
                            }
                            self.eviction_counter =
                                self.eviction_counter.wrapping_add(abs_count as u64);
                            CommandResponse::Array(result)
                        }
                    }
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => {
                if count.is_some() {
                    CommandResponse::Array(vec![])
                } else {
                    CommandResponse::Nil
                }
            }
        }
    }

    fn cmd_zscan(
        &mut self,
        key: &[u8],
        cursor: u64,
        pattern: Option<&str>,
        count: usize,
    ) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::SortedSet(map) => {
                    let mut members: Vec<(&Vec<u8>, &f64)> = map
                        .iter()
                        .filter(|(m, _)| pattern.is_none_or(|p| glob_match(p, m)))
                        .collect();
                    members.sort_by(|a, b| a.0.cmp(b.0));

                    let start = cursor as usize;
                    let end = (start + count).min(members.len());

                    if start >= members.len() {
                        return CommandResponse::Array(vec![
                            CommandResponse::BulkString(b"0".to_vec()),
                            CommandResponse::Array(vec![]),
                        ]);
                    }

                    let mut results = Vec::with_capacity((end - start) * 2);
                    for (m, s) in &members[start..end] {
                        results.push(CommandResponse::BulkString((*m).clone()));
                        results.push(CommandResponse::BulkString(format!("{}", s).into_bytes()));
                    }

                    let next_cursor = if end >= members.len() { 0 } else { end as u64 };

                    CommandResponse::Array(vec![
                        CommandResponse::BulkString(next_cursor.to_string().into_bytes()),
                        CommandResponse::Array(results),
                    ])
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Array(vec![
                CommandResponse::BulkString(b"0".to_vec()),
                CommandResponse::Array(vec![]),
            ]),
        }
    }

    // ─── HyperLogLog operations ─────────────────────────────────────────

    fn cmd_pfadd(&mut self, key: &[u8], elements: &[Vec<u8>]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let is_new = !self.entries.contains_key(&compact);
        let mut registers = if is_new {
            vec![0u8; HLL_REGISTER_BYTES]
        } else {
            match self.entries.get(&compact) {
                Some(entry) => match entry.value.as_bytes() {
                    Some(bytes) if bytes.len() == HLL_REGISTER_BYTES => bytes,
                    Some(_) | None => {
                        return CommandResponse::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .into(),
                        )
                    }
                },
                None => vec![0u8; HLL_REGISTER_BYTES],
            }
        };

        let mut changed = false;
        for elem in elements {
            if hll_add(&mut registers, elem) {
                changed = true;
            }
        }

        if changed || is_new {
            let mut memory_delta = 0usize;
            if is_new {
                memory_delta += key.len()
                    + std::mem::size_of::<CompactKey>()
                    + std::mem::size_of::<KeyEntry>()
                    + HLL_REGISTER_BYTES;
            }
            let entry = KeyEntry::new(compact.clone(), Value::from_raw_bytes(&registers));
            self.entries.insert(compact, entry);
            self.memory_used += memory_delta;
        }

        CommandResponse::Integer(if changed { 1 } else { 0 })
    }

    fn cmd_pfcount_single(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match entry.value.as_bytes() {
                Some(bytes) if bytes.len() == HLL_REGISTER_BYTES => {
                    CommandResponse::Integer(hll_count(&bytes) as i64)
                }
                Some(_) => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
                None => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Integer(0),
        }
    }

    fn cmd_pfcount_multi(&mut self, keys: &[Vec<u8>]) -> CommandResponse {
        let mut merged = vec![0u8; HLL_REGISTER_BYTES];
        for key in keys {
            self.lazy_expire(key);
            let compact = CompactKey::new(key);
            if let Some(entry) = self.entries.get(&compact) {
                match entry.value.as_bytes() {
                    Some(bytes) if bytes.len() == HLL_REGISTER_BYTES => {
                        hll_merge(&mut merged, &bytes);
                    }
                    Some(_) => {
                        return CommandResponse::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .into(),
                        )
                    }
                    None => {
                        return CommandResponse::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .into(),
                        )
                    }
                }
            }
        }
        CommandResponse::Integer(hll_count(&merged) as i64)
    }

    fn cmd_pfmerge(&mut self, destkey: &[u8], sourcekeys: &[Vec<u8>]) -> CommandResponse {
        let mut merged = vec![0u8; HLL_REGISTER_BYTES];

        let dest_compact = CompactKey::new(destkey);
        self.lazy_expire(destkey);
        if let Some(entry) = self.entries.get(&dest_compact) {
            match entry.value.as_bytes() {
                Some(bytes) if bytes.len() == HLL_REGISTER_BYTES => {
                    hll_merge(&mut merged, &bytes);
                }
                Some(_) => {
                    return CommandResponse::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
                None => {
                    return CommandResponse::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            }
        }

        for key in sourcekeys {
            self.lazy_expire(key);
            let compact = CompactKey::new(key);
            if let Some(entry) = self.entries.get(&compact) {
                match entry.value.as_bytes() {
                    Some(bytes) if bytes.len() == HLL_REGISTER_BYTES => {
                        hll_merge(&mut merged, &bytes);
                    }
                    Some(_) => {
                        return CommandResponse::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .into(),
                        )
                    }
                    None => {
                        return CommandResponse::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .into(),
                        )
                    }
                }
            }
        }

        let is_new = !self.entries.contains_key(&dest_compact);
        let mut memory_delta = 0usize;
        if is_new {
            memory_delta += destkey.len()
                + std::mem::size_of::<CompactKey>()
                + std::mem::size_of::<KeyEntry>()
                + HLL_REGISTER_BYTES;
        }
        let entry = KeyEntry::new(dest_compact.clone(), Value::from_raw_bytes(&merged));
        self.entries.insert(dest_compact, entry);
        self.memory_used += memory_delta;

        CommandResponse::Ok
    }

    // ─── Bitmap operations ──────────────────────────────────────────

    fn get_string_bytes(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let compact = CompactKey::new(key);
        self.entries.get(&compact).and_then(|e| e.value.as_bytes())
    }

    fn set_string_value(&mut self, key: &[u8], data: Vec<u8>) {
        let compact = CompactKey::new(key);
        let is_new = !self.entries.contains_key(&compact);
        let new_value = Value::from_raw_bytes(&data);
        if is_new {
            let size = Self::estimate_key_entry_size(key, &new_value);
            let entry = KeyEntry::new(compact.clone(), new_value);
            self.entries.insert(compact, entry);
            self.memory_used += size;
        } else {
            let entry = self
                .entries
                .get_mut(&compact)
                .expect("checked contains_key");
            let old_size = entry.value.estimated_size();
            entry.value = new_value;
            let new_size = entry.value.estimated_size();
            if new_size > old_size {
                self.memory_used += new_size - old_size;
            } else {
                self.memory_used = self.memory_used.saturating_sub(old_size - new_size);
            }
        }
    }

    fn cmd_setbit(&mut self, key: &[u8], offset: u64, value: u8) -> CommandResponse {
        self.lazy_expire(key);
        let byte_idx = (offset / 8) as usize;
        let bit_idx = 7 - (offset % 8) as usize;

        let mut data = self.get_string_bytes(key).unwrap_or_default();

        if byte_idx >= data.len() {
            data.resize(byte_idx + 1, 0);
        }

        let old_bit = (data[byte_idx] >> bit_idx) & 1;

        if value == 1 {
            data[byte_idx] |= 1 << bit_idx;
        } else {
            data[byte_idx] &= !(1 << bit_idx);
        }

        self.set_string_value(key, data);
        CommandResponse::Integer(old_bit as i64)
    }

    fn cmd_getbit(&mut self, key: &[u8], offset: u64) -> CommandResponse {
        self.lazy_expire(key);
        let byte_idx = (offset / 8) as usize;
        let bit_idx = 7 - (offset % 8) as usize;

        match self.get_string_bytes(key) {
            Some(data) => {
                if byte_idx >= data.len() {
                    CommandResponse::Integer(0)
                } else {
                    CommandResponse::Integer(((data[byte_idx] >> bit_idx) & 1) as i64)
                }
            }
            None => CommandResponse::Integer(0),
        }
    }

    fn cmd_bitcount(
        &mut self,
        key: &[u8],
        start: Option<i64>,
        end: Option<i64>,
        use_bit: bool,
    ) -> CommandResponse {
        self.lazy_expire(key);
        let data = match self.get_string_bytes(key) {
            Some(d) => d,
            None => return CommandResponse::Integer(0),
        };

        if data.is_empty() {
            return CommandResponse::Integer(0);
        }

        if use_bit {
            let total_bits = (data.len() * 8) as i64;
            let s = normalize_index(start.unwrap_or(0), total_bits);
            let e = normalize_index(end.unwrap_or(total_bits - 1), total_bits);
            if s > e {
                return CommandResponse::Integer(0);
            }
            let mut count = 0i64;
            for bit_pos in s..=e {
                let byte_idx = bit_pos / 8;
                let bit_idx = 7 - (bit_pos % 8);
                if byte_idx < data.len() && (data[byte_idx] >> bit_idx) & 1 == 1 {
                    count += 1;
                }
            }
            CommandResponse::Integer(count)
        } else {
            let len = data.len() as i64;
            let s = normalize_index(start.unwrap_or(0), len);
            let e = normalize_index(end.unwrap_or(len - 1), len);
            if s > e || s >= data.len() {
                return CommandResponse::Integer(0);
            }
            let e = e.min(data.len() - 1);
            let count: u32 = data[s..=e].iter().map(|b| b.count_ones()).sum();
            CommandResponse::Integer(count as i64)
        }
    }

    fn cmd_bitop(
        &mut self,
        operation: BitOperation,
        destkey: &[u8],
        keys: &[Vec<u8>],
    ) -> CommandResponse {
        let mut buffers: Vec<Vec<u8>> = Vec::with_capacity(keys.len());
        let mut max_len = 0usize;

        for key in keys {
            self.lazy_expire(key);
            let data = self.get_string_bytes(key).unwrap_or_default();
            max_len = max_len.max(data.len());
            buffers.push(data);
        }

        if buffers.is_empty() {
            self.set_string_value(destkey, Vec::new());
            return CommandResponse::Integer(0);
        }

        let mut result = vec![0u8; max_len];

        match operation {
            BitOperation::And => {
                result = vec![0xFF; max_len];
                for buf in &buffers {
                    for (i, byte) in result.iter_mut().enumerate() {
                        let src = if i < buf.len() { buf[i] } else { 0 };
                        *byte &= src;
                    }
                }
            }
            BitOperation::Or => {
                for buf in &buffers {
                    for (i, &src) in buf.iter().enumerate() {
                        result[i] |= src;
                    }
                }
            }
            BitOperation::Xor => {
                for buf in &buffers {
                    for (i, &src) in buf.iter().enumerate() {
                        result[i] ^= src;
                    }
                }
            }
            BitOperation::Not => {
                let buf = &buffers[0];
                result = vec![0u8; buf.len()];
                for (i, &src) in buf.iter().enumerate() {
                    result[i] = !src;
                }
            }
        }

        let len = result.len() as i64;
        self.set_string_value(destkey, result);
        CommandResponse::Integer(len)
    }

    fn cmd_bitpos(
        &mut self,
        key: &[u8],
        bit: u8,
        start: Option<i64>,
        end: Option<i64>,
        use_bit: bool,
    ) -> CommandResponse {
        self.lazy_expire(key);
        let data = match self.get_string_bytes(key) {
            Some(d) if !d.is_empty() => d,
            _ => {
                return if bit == 0 {
                    CommandResponse::Integer(0)
                } else {
                    CommandResponse::Integer(-1)
                };
            }
        };

        let has_explicit_end = end.is_some();

        if use_bit {
            let total_bits = (data.len() * 8) as i64;
            let s = normalize_index(start.unwrap_or(0), total_bits);
            let e = normalize_index(end.unwrap_or(total_bits - 1), total_bits);
            if s > e {
                return CommandResponse::Integer(-1);
            }
            for bit_pos in s..=e.min(data.len() * 8 - 1) {
                let byte_idx = bit_pos / 8;
                let bit_idx = 7 - (bit_pos % 8);
                let current = (data[byte_idx] >> bit_idx) & 1;
                if current == bit {
                    return CommandResponse::Integer(bit_pos as i64);
                }
            }
            CommandResponse::Integer(-1)
        } else {
            let len = data.len() as i64;
            let s = normalize_index(start.unwrap_or(0), len);
            let e = normalize_index(end.unwrap_or(len - 1), len);
            if s > e || s >= data.len() {
                return CommandResponse::Integer(-1);
            }
            let e = e.min(data.len() - 1);
            for (byte_idx, &byte_val) in data.iter().enumerate().take(e + 1).skip(s) {
                for bit_idx in (0..8).rev() {
                    let current = (byte_val >> bit_idx) & 1;
                    if current == bit {
                        return CommandResponse::Integer((byte_idx * 8 + (7 - bit_idx)) as i64);
                    }
                }
            }
            if bit == 0 && !has_explicit_end {
                CommandResponse::Integer((data.len() * 8) as i64)
            } else {
                CommandResponse::Integer(-1)
            }
        }
    }

    fn cmd_bitfield(&mut self, key: &[u8], operations: &[BitFieldOperation]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);

        let mut data = match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::InlineStr { .. } | Value::HeapStr(_) | Value::Int(_) => {
                    entry.value.as_bytes().unwrap_or_default()
                }
                _ => {
                    return CommandResponse::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            },
            None => Vec::new(),
        };

        let mut overflow = BitFieldOverflow::Wrap;
        let mut responses = Vec::new();
        let mut dirty = false;

        for op in operations {
            match *op {
                BitFieldOperation::Overflow(mode) => overflow = mode,
                BitFieldOperation::Get { encoding, offset } => {
                    let Some(bit_offset) = Self::resolve_bitfield_offset(encoding, offset) else {
                        return CommandResponse::Error(
                            "ERR bit offset is not an integer or out of range".into(),
                        );
                    };
                    let value = Self::bitfield_read(&data, bit_offset, encoding);
                    responses.push(CommandResponse::Integer(value));
                }
                BitFieldOperation::Set {
                    encoding,
                    offset,
                    value,
                } => {
                    let Some(bit_offset) = Self::resolve_bitfield_offset(encoding, offset) else {
                        return CommandResponse::Error(
                            "ERR bit offset is not an integer or out of range".into(),
                        );
                    };
                    let old_value = Self::bitfield_read(&data, bit_offset, encoding);
                    let Some(applied) =
                        Self::apply_bitfield_overflow(value as i128, encoding, overflow)
                    else {
                        responses.push(CommandResponse::Nil);
                        continue;
                    };
                    Self::bitfield_write(&mut data, bit_offset, encoding, applied);
                    responses.push(CommandResponse::Integer(old_value));
                    dirty = true;
                }
                BitFieldOperation::IncrBy {
                    encoding,
                    offset,
                    increment,
                } => {
                    let Some(bit_offset) = Self::resolve_bitfield_offset(encoding, offset) else {
                        return CommandResponse::Error(
                            "ERR bit offset is not an integer or out of range".into(),
                        );
                    };
                    let current = Self::bitfield_read(&data, bit_offset, encoding) as i128;
                    let target = current + increment as i128;
                    let Some(applied) = Self::apply_bitfield_overflow(target, encoding, overflow)
                    else {
                        responses.push(CommandResponse::Nil);
                        continue;
                    };
                    Self::bitfield_write(&mut data, bit_offset, encoding, applied);
                    responses.push(CommandResponse::Integer(applied as i64));
                    dirty = true;
                }
            }
        }

        if dirty {
            self.set_string_value(key, data);
        }

        CommandResponse::Array(responses)
    }

    fn resolve_bitfield_offset(encoding: BitFieldEncoding, offset: BitFieldOffset) -> Option<u64> {
        let base = match offset {
            BitFieldOffset::Absolute(v) => v,
            BitFieldOffset::Multiplied(v) => v.checked_mul(encoding.bits as i64)?,
        };
        if base < 0 {
            return None;
        }
        let start = base as u64;
        start.checked_add(encoding.bits as u64)?;
        Some(start)
    }

    fn bitfield_read(data: &[u8], bit_offset: u64, encoding: BitFieldEncoding) -> i64 {
        let width = encoding.bits as u32;
        let mut raw: u128 = 0;
        for i in 0..width {
            let pos = bit_offset + i as u64;
            let byte_idx = (pos / 8) as usize;
            let bit_idx = 7 - (pos % 8) as usize;
            let bit = if byte_idx < data.len() {
                (data[byte_idx] >> bit_idx) & 1
            } else {
                0
            };
            raw = (raw << 1) | bit as u128;
        }

        if encoding.signed {
            let shift = 128 - width;
            (((raw << shift) as i128) >> shift) as i64
        } else {
            raw as i64
        }
    }

    fn bitfield_write(
        data: &mut Vec<u8>,
        bit_offset: u64,
        encoding: BitFieldEncoding,
        value: i128,
    ) {
        let width = encoding.bits as u32;
        let encoded = Self::encode_bitfield_value(value, encoding);
        let end_bit = bit_offset + width as u64;
        let needed_bytes = end_bit.div_ceil(8) as usize;
        if needed_bytes > data.len() {
            data.resize(needed_bytes, 0);
        }

        for i in 0..width {
            let src_shift = width - 1 - i;
            let bit = ((encoded >> src_shift) & 1) as u8;
            let pos = bit_offset + i as u64;
            let byte_idx = (pos / 8) as usize;
            let bit_idx = 7 - (pos % 8) as usize;
            if bit == 1 {
                data[byte_idx] |= 1 << bit_idx;
            } else {
                data[byte_idx] &= !(1 << bit_idx);
            }
        }
    }

    fn bitfield_bounds(encoding: BitFieldEncoding) -> (i128, i128) {
        if encoding.signed {
            let min = -(1i128 << (encoding.bits as u32 - 1));
            let max = (1i128 << (encoding.bits as u32 - 1)) - 1;
            (min, max)
        } else {
            (0, (1i128 << encoding.bits as u32) - 1)
        }
    }

    fn apply_bitfield_overflow(
        value: i128,
        encoding: BitFieldEncoding,
        mode: BitFieldOverflow,
    ) -> Option<i128> {
        let (min, max) = Self::bitfield_bounds(encoding);
        if (min..=max).contains(&value) {
            return Some(value);
        }

        match mode {
            BitFieldOverflow::Fail => None,
            BitFieldOverflow::Sat => Some(value.clamp(min, max)),
            BitFieldOverflow::Wrap => {
                let modulo = 1i128 << encoding.bits as u32;
                let wrapped = ((value % modulo) + modulo) % modulo;
                if encoding.signed {
                    let sign_boundary = 1i128 << (encoding.bits as u32 - 1);
                    if wrapped >= sign_boundary {
                        Some(wrapped - modulo)
                    } else {
                        Some(wrapped)
                    }
                } else {
                    Some(wrapped)
                }
            }
        }
    }

    fn encode_bitfield_value(value: i128, encoding: BitFieldEncoding) -> u128 {
        if encoding.signed && value < 0 {
            (value + (1i128 << encoding.bits as u32)) as u128
        } else {
            value as u128
        }
    }

    // ─── Geo operations ─────────────────────────────────────────────

    fn cmd_geoadd(
        &mut self,
        key: &[u8],
        nx: bool,
        xx: bool,
        ch: bool,
        members: &[(f64, f64, Vec<u8>)],
    ) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let is_new = !self.entries.contains_key(&compact);
        let zset = self.get_or_create_sorted_set(&compact);
        match zset {
            Ok(map) => {
                let mut memory_delta: usize = 0;
                if is_new {
                    memory_delta += key.len()
                        + std::mem::size_of::<CompactKey>()
                        + std::mem::size_of::<KeyEntry>();
                }
                let mut count = 0i64;
                let member_overhead = std::mem::size_of::<f64>();
                for (lon, lat, member) in members {
                    let score = geo_encode(*lon, *lat);
                    let existing = map.get(member).copied();
                    match existing {
                        Some(old_score) => {
                            if nx {
                                continue;
                            }
                            if (old_score - score).abs() > f64::EPSILON {
                                map.insert(member.clone(), score);
                                if ch {
                                    count += 1;
                                }
                            }
                        }
                        None => {
                            if xx {
                                continue;
                            }
                            map.insert(member.clone(), score);
                            memory_delta += member.len() + member_overhead;
                            count += 1;
                        }
                    }
                }
                self.memory_used += memory_delta;
                CommandResponse::Integer(count)
            }
            Err(e) => e,
        }
    }

    fn cmd_geodist(
        &mut self,
        key: &[u8],
        member1: &[u8],
        member2: &[u8],
        unit: GeoUnit,
    ) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::SortedSet(map) => {
                    let s1 = match map.get(member1) {
                        Some(s) => *s,
                        None => return CommandResponse::Nil,
                    };
                    let s2 = match map.get(member2) {
                        Some(s) => *s,
                        None => return CommandResponse::Nil,
                    };
                    let (lon1, lat1) = geo_decode(s1);
                    let (lon2, lat2) = geo_decode(s2);
                    let dist = haversine_distance(lat1, lon1, lat2, lon2);
                    let converted = geo_convert_distance(dist, unit);
                    CommandResponse::BulkString(format!("{:.4}", converted).into_bytes())
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Nil,
        }
    }

    fn cmd_geohash(&mut self, key: &[u8], members: &[Vec<u8>]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::SortedSet(map) => {
                    let results: Vec<CommandResponse> = members
                        .iter()
                        .map(|member| match map.get(member.as_slice()) {
                            Some(&score) => {
                                let (lon, lat) = geo_decode(score);
                                let hash = geo_encode_base32(lon, lat);
                                CommandResponse::BulkString(hash.into_bytes())
                            }
                            None => CommandResponse::Nil,
                        })
                        .collect();
                    CommandResponse::Array(results)
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => {
                let results = vec![CommandResponse::Nil; members.len()];
                CommandResponse::Array(results)
            }
        }
    }

    fn cmd_geopos(&mut self, key: &[u8], members: &[Vec<u8>]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::SortedSet(map) => {
                    let results: Vec<CommandResponse> = members
                        .iter()
                        .map(|member| match map.get(member.as_slice()) {
                            Some(&score) => {
                                let (lon, lat) = geo_decode(score);
                                CommandResponse::Array(vec![
                                    CommandResponse::BulkString(format!("{:.6}", lon).into_bytes()),
                                    CommandResponse::BulkString(format!("{:.6}", lat).into_bytes()),
                                ])
                            }
                            None => CommandResponse::Nil,
                        })
                        .collect();
                    CommandResponse::Array(results)
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => {
                let results = vec![CommandResponse::Nil; members.len()];
                CommandResponse::Array(results)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn cmd_geosearch(
        &mut self,
        key: &[u8],
        from_member: Option<&[u8]>,
        from_lonlat: Option<(f64, f64)>,
        radius: f64,
        unit: GeoUnit,
        asc: Option<bool>,
        count: Option<usize>,
        withcoord: bool,
        withdist: bool,
        withhash: bool,
    ) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let map = match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::SortedSet(map) => map,
                _ => {
                    return CommandResponse::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    )
                }
            },
            None => return CommandResponse::Array(vec![]),
        };

        let (center_lon, center_lat) = if let Some(member) = from_member {
            match map.get(member) {
                Some(&score) => geo_decode(score),
                None => return CommandResponse::Array(vec![]),
            }
        } else if let Some((lon, lat)) = from_lonlat {
            (lon, lat)
        } else {
            return CommandResponse::Error("ERR FROMMEMBER or FROMLONLAT required".into());
        };

        let radius_meters = geo_to_meters(radius, unit);

        let mut results: Vec<(Vec<u8>, f64, f64, f64, f64)> = Vec::new();
        for (member, &score) in map.iter() {
            let (lon, lat) = geo_decode(score);
            let dist = haversine_distance(center_lat, center_lon, lat, lon);
            if dist <= radius_meters {
                results.push((member.clone(), dist, lon, lat, score));
            }
        }

        if let Some(true) = asc {
            results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        } else if let Some(false) = asc {
            results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        }

        if let Some(c) = count {
            results.truncate(c);
        }

        let items: Vec<CommandResponse> = results
            .into_iter()
            .map(|(member, dist, lon, lat, score)| {
                if !withcoord && !withdist && !withhash {
                    return CommandResponse::BulkString(member);
                }
                let mut arr = vec![CommandResponse::BulkString(member)];
                if withdist {
                    let converted = geo_convert_distance(dist, unit);
                    arr.push(CommandResponse::BulkString(
                        format!("{:.4}", converted).into_bytes(),
                    ));
                }
                if withhash {
                    arr.push(CommandResponse::Integer(score.to_bits() as i64));
                }
                if withcoord {
                    arr.push(CommandResponse::Array(vec![
                        CommandResponse::BulkString(format!("{:.6}", lon).into_bytes()),
                        CommandResponse::BulkString(format!("{:.6}", lat).into_bytes()),
                    ]));
                }
                CommandResponse::Array(arr)
            })
            .collect();

        CommandResponse::Array(items)
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

    fn build_string_entry(
        key: &CompactKey,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> (KeyEntry, usize) {
        let new_value = Value::from_raw_bytes(value);
        let entry_size = Self::estimate_key_entry_size(key.as_bytes(), &new_value);
        let mut entry = KeyEntry::new(key.clone(), new_value);
        if let Some(dur) = ttl {
            entry.set_ttl(dur);
        }
        (entry, entry_size)
    }

    fn lazy_expire(&mut self, key: &[u8]) {
        if self.is_expired(key) {
            let compact = CompactKey::new(key);
            let _ = self.remove_compact_entry(&compact);
        }
    }

    fn is_expired(&self, key: &[u8]) -> bool {
        self.entries
            .get(key)
            .is_some_and(|entry| entry.is_expired())
    }

    /// Update the LFU counter for a key on read access.
    fn touch_key(&mut self, key: &CompactKey) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.touch_lfu();
        }
    }

    /// Evict keys using LFU sampling when memory pressure is detected.
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
        let mut sampled = 0usize;

        for (i, (key, entry)) in self.entries.iter().enumerate() {
            if i < start {
                continue;
            }
            if sampled >= sample_size {
                break;
            }
            sampled += 1;
            if entry.lfu_counter < lowest_counter {
                lowest_counter = entry.lfu_counter;
                lowest_key = Some(key.clone());
            }
        }

        if lowest_key.is_none() {
            for (key, entry) in self.entries.iter().take(sample_size * 3) {
                if entry.lfu_counter < lowest_counter {
                    lowest_counter = entry.lfu_counter;
                    lowest_key = Some(key.clone());
                }
            }
        }

        if let Some(key) = lowest_key {
            let _ = self.remove_compact_entry(&key);
        }
    }

    fn remove_compact_entry(&mut self, key: &CompactKey) -> Option<KeyEntry> {
        let removed = self.entries.remove(key)?;
        let removed_size = Self::estimate_key_entry_size(key.as_bytes(), &removed.value);
        self.memory_used = self.memory_used.saturating_sub(removed_size);
        Some(removed)
    }

    fn estimate_key_entry_size(key: &[u8], value: &Value) -> usize {
        key.len()
            + std::mem::size_of::<CompactKey>()
            + std::mem::size_of::<KeyEntry>()
            + value.estimated_size()
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

    fn get_or_create_stream(
        &mut self,
        key: &CompactKey,
    ) -> Result<&mut StreamLog, CommandResponse> {
        if !self.entries.contains_key(key) {
            let log = StreamLog {
                entries: VecDeque::new(),
                last_id: StreamId { ms: 0, seq: 0 },
            };
            let entry = KeyEntry::new(key.clone(), Value::Stream(Box::new(log)));
            self.entries.insert(key.clone(), entry);
        }
        let entry = self.entries.get_mut(key).ok_or_else(|| {
            CommandResponse::Error("ERR internal: key not found after insert".into())
        })?;
        match &mut entry.value {
            Value::Stream(log) => Ok(log),
            _ => Err(CommandResponse::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            )),
        }
    }

    fn cmd_xadd(
        &mut self,
        key: &[u8],
        id_arg: &[u8],
        fields: &[(Vec<u8>, Vec<u8>)],
        maxlen: Option<usize>,
    ) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        let is_new = !self.entries.contains_key(&compact);

        let log = match self.get_or_create_stream(&compact) {
            Ok(log) => log,
            Err(e) => return e,
        };

        let new_id = if id_arg == b"*" {
            let ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            if ms > log.last_id.ms {
                StreamId { ms, seq: 0 }
            } else {
                StreamId {
                    ms: log.last_id.ms,
                    seq: log.last_id.seq + 1,
                }
            }
        } else {
            match StreamId::parse(id_arg) {
                Some(parsed) => {
                    if parsed <= log.last_id {
                        return CommandResponse::Error(
                            "ERR The ID specified in XADD is equal or smaller than the target stream top item".into(),
                        );
                    }
                    parsed
                }
                None => {
                    return CommandResponse::Error(
                        "ERR Invalid stream ID specified as stream command argument".into(),
                    );
                }
            }
        };

        let id_str = format!("{}", new_id).into_bytes();
        log.last_id = new_id.clone();

        let mut memory_delta: usize = 0;
        if is_new {
            memory_delta +=
                key.len() + std::mem::size_of::<CompactKey>() + std::mem::size_of::<KeyEntry>();
        }
        memory_delta += std::mem::size_of::<StreamEntry>();
        for (k, v) in fields {
            memory_delta += k.len() + v.len();
        }

        log.entries.push_back(StreamEntry {
            id: new_id,
            fields: fields.to_vec(),
        });

        if let Some(max) = maxlen {
            while log.entries.len() > max {
                log.entries.pop_front();
            }
        }

        self.memory_used += memory_delta;
        CommandResponse::BulkString(id_str)
    }

    fn cmd_xlen(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Stream(log) => CommandResponse::Integer(log.entries.len() as i64),
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Integer(0),
        }
    }

    fn parse_range_id(id_bytes: &[u8], is_start: bool) -> StreamId {
        if id_bytes == b"-" {
            StreamId::min_id()
        } else if id_bytes == b"+" {
            StreamId::max_id()
        } else {
            StreamId::parse(id_bytes).unwrap_or(if is_start {
                StreamId::min_id()
            } else {
                StreamId::max_id()
            })
        }
    }

    fn format_stream_entry(entry: &StreamEntry) -> CommandResponse {
        let mut fields_resp: Vec<CommandResponse> = Vec::with_capacity(entry.fields.len() * 2);
        for (k, v) in &entry.fields {
            fields_resp.push(CommandResponse::BulkString(k.clone()));
            fields_resp.push(CommandResponse::BulkString(v.clone()));
        }
        CommandResponse::Array(vec![
            CommandResponse::BulkString(format!("{}", entry.id).into_bytes()),
            CommandResponse::Array(fields_resp),
        ])
    }

    fn cmd_xrange(
        &mut self,
        key: &[u8],
        start: &[u8],
        end: &[u8],
        count: Option<usize>,
    ) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Stream(log) => {
                    let start_id = Self::parse_range_id(start, true);
                    let end_id = Self::parse_range_id(end, false);
                    let limit = count.unwrap_or(usize::MAX);

                    let results: Vec<CommandResponse> = log
                        .entries
                        .iter()
                        .filter(|e| e.id >= start_id && e.id <= end_id)
                        .take(limit)
                        .map(Self::format_stream_entry)
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

    fn cmd_xrevrange(
        &mut self,
        key: &[u8],
        start: &[u8],
        end: &[u8],
        count: Option<usize>,
    ) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Stream(log) => {
                    let start_id = Self::parse_range_id(start, false);
                    let end_id = Self::parse_range_id(end, true);
                    let limit = count.unwrap_or(usize::MAX);

                    let results: Vec<CommandResponse> = log
                        .entries
                        .iter()
                        .rev()
                        .filter(|e| e.id >= end_id && e.id <= start_id)
                        .take(limit)
                        .map(Self::format_stream_entry)
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

    fn cmd_xread(
        &mut self,
        keys: &[Vec<u8>],
        ids: &[Vec<u8>],
        count: Option<usize>,
    ) -> CommandResponse {
        if keys.len() != ids.len() {
            return CommandResponse::Error(
                "ERR Unbalanced XREAD list of streams: for each stream key an ID must be specified"
                    .into(),
            );
        }

        let limit = count.unwrap_or(usize::MAX);
        let mut results: Vec<CommandResponse> = Vec::new();

        for (key, id_bytes) in keys.iter().zip(ids.iter()) {
            self.lazy_expire(key);
            let compact = CompactKey::new(key);
            if let Some(entry) = self.entries.get(&compact) {
                if let Value::Stream(log) = &entry.value {
                    let after_id = if id_bytes == b"$" {
                        log.last_id.clone()
                    } else {
                        match StreamId::parse(id_bytes) {
                            Some(id) => id,
                            None => continue,
                        }
                    };

                    let entries: Vec<CommandResponse> = log
                        .entries
                        .iter()
                        .filter(|e| e.id > after_id)
                        .take(limit)
                        .map(Self::format_stream_entry)
                        .collect();

                    if !entries.is_empty() {
                        results.push(CommandResponse::Array(vec![
                            CommandResponse::BulkString(key.clone()),
                            CommandResponse::Array(entries),
                        ]));
                    }
                }
            }
        }

        if results.is_empty() {
            CommandResponse::Nil
        } else {
            CommandResponse::Array(results)
        }
    }

    fn cmd_xtrim(&mut self, key: &[u8], maxlen: usize) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get_mut(&compact) {
            Some(entry) => match &mut entry.value {
                Value::Stream(log) => {
                    let mut trimmed = 0i64;
                    while log.entries.len() > maxlen {
                        log.entries.pop_front();
                        trimmed += 1;
                    }
                    CommandResponse::Integer(trimmed)
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Integer(0),
        }
    }

    fn cmd_xdel(&mut self, key: &[u8], ids: &[Vec<u8>]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get_mut(&compact) {
            Some(entry) => match &mut entry.value {
                Value::Stream(log) => {
                    let mut deleted = 0i64;
                    for id_bytes in ids {
                        if let Some(sid) = StreamId::parse(id_bytes) {
                            let before = log.entries.len();
                            log.entries.retain(|e| e.id != sid);
                            if log.entries.len() < before {
                                deleted += 1;
                            }
                        }
                    }
                    CommandResponse::Integer(deleted)
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Integer(0),
        }
    }

    fn cmd_xgroup_create(
        &mut self,
        key: &[u8],
        group: &str,
        id: &str,
        mkstream: bool,
    ) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);

        if !self.entries.contains_key(&compact) {
            if !mkstream {
                return CommandResponse::Error(
                    "ERR The XGROUP subcommand requires the key to exist".into(),
                );
            }
            let log = StreamLog {
                entries: VecDeque::new(),
                last_id: StreamId { ms: 0, seq: 0 },
            };
            let entry = KeyEntry::new(compact.clone(), Value::Stream(Box::new(log)));
            self.entries.insert(compact.clone(), entry);
        }

        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Stream(log) => {
                    let last_delivered_id = if id == "$" {
                        log.last_id.clone()
                    } else if id == "0" || id == "0-0" {
                        StreamId::min_id()
                    } else {
                        StreamId::parse(id.as_bytes()).unwrap_or(StreamId::min_id())
                    };
                    let groups = self.stream_groups.entry(compact).or_default();
                    if groups.contains_key(group) {
                        return CommandResponse::Error(
                            "BUSYGROUP Consumer Group name already exists".into(),
                        );
                    }
                    groups.insert(
                        group.to_string(),
                        StreamConsumerGroup {
                            last_delivered_id,
                            pel: HashMap::new(),
                            consumers: HashMap::new(),
                        },
                    );
                    CommandResponse::Ok
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Error("ERR internal error".into()),
        }
    }

    fn cmd_xgroup_destroy(&mut self, key: &[u8], group: &str) -> CommandResponse {
        let compact = CompactKey::new(key);
        if let Some(groups) = self.stream_groups.get_mut(&compact) {
            if groups.remove(group).is_some() {
                return CommandResponse::Integer(1);
            }
        }
        CommandResponse::Integer(0)
    }

    fn cmd_xgroup_delconsumer(
        &mut self,
        key: &[u8],
        group: &str,
        consumer: &str,
    ) -> CommandResponse {
        let compact = CompactKey::new(key);
        if let Some(groups) = self.stream_groups.get_mut(&compact) {
            if let Some(grp) = groups.get_mut(group) {
                if let Some(state) = grp.consumers.remove(consumer) {
                    let pending_count = state.pending.len() as i64;
                    for sid in &state.pending {
                        grp.pel.remove(sid);
                    }
                    return CommandResponse::Integer(pending_count);
                }
            }
        }
        CommandResponse::Integer(0)
    }

    fn cmd_xreadgroup(
        &mut self,
        group: &str,
        consumer: &str,
        count: Option<usize>,
        keys: &[Vec<u8>],
        ids: &[Vec<u8>],
    ) -> CommandResponse {
        if keys.len() != ids.len() {
            return CommandResponse::Error(
                "ERR Unbalanced XREADGROUP list of streams: for each stream key an ID must be specified"
                    .into(),
            );
        }

        let limit = count.unwrap_or(usize::MAX);
        let mut results: Vec<CommandResponse> = Vec::new();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        for (key, id_bytes) in keys.iter().zip(ids.iter()) {
            self.lazy_expire(key);
            let compact = CompactKey::new(key);

            let is_new_msgs = id_bytes == b">";

            let stream_entries: Vec<StreamEntry> = if let Some(entry) = self.entries.get(&compact) {
                if let Value::Stream(log) = &entry.value {
                    if is_new_msgs {
                        let last_delivered = self
                            .stream_groups
                            .get(&compact)
                            .and_then(|gs| gs.get(group))
                            .map(|g| g.last_delivered_id.clone())
                            .unwrap_or(StreamId::min_id());
                        log.entries
                            .iter()
                            .filter(|e| e.id > last_delivered)
                            .take(limit)
                            .cloned()
                            .collect()
                    } else {
                        let groups = self.stream_groups.get(&compact);
                        let grp = groups.and_then(|gs| gs.get(group));
                        if let Some(grp) = grp {
                            let consumer_state = grp.consumers.get(consumer);
                            let pending_ids: Vec<StreamId> = consumer_state
                                .map(|cs| {
                                    let mut ids: Vec<StreamId> =
                                        cs.pending.iter().cloned().collect();
                                    ids.sort();
                                    ids
                                })
                                .unwrap_or_default();
                            pending_ids
                                .into_iter()
                                .take(limit)
                                .filter_map(|sid| log.entries.iter().find(|e| e.id == sid).cloned())
                                .collect()
                        } else {
                            Vec::new()
                        }
                    }
                } else {
                    continue;
                }
            } else {
                continue;
            };

            if is_new_msgs && !stream_entries.is_empty() {
                let groups = self.stream_groups.entry(compact.clone()).or_default();
                let grp = groups
                    .entry(group.to_string())
                    .or_insert_with(|| StreamConsumerGroup {
                        last_delivered_id: StreamId::min_id(),
                        pel: HashMap::new(),
                        consumers: HashMap::new(),
                    });
                let cs = grp.consumers.entry(consumer.to_string()).or_default();
                for se in &stream_entries {
                    if se.id > grp.last_delivered_id {
                        grp.last_delivered_id = se.id.clone();
                    }
                    grp.pel.insert(
                        se.id.clone(),
                        PendingEntry {
                            consumer: consumer.to_string(),
                            delivery_time: now_ms,
                            delivery_count: 1,
                        },
                    );
                    cs.pending.insert(se.id.clone());
                }
            }

            if !stream_entries.is_empty() {
                let entries_resp: Vec<CommandResponse> = stream_entries
                    .iter()
                    .map(Self::format_stream_entry)
                    .collect();
                results.push(CommandResponse::Array(vec![
                    CommandResponse::BulkString(key.clone()),
                    CommandResponse::Array(entries_resp),
                ]));
            }
        }

        if results.is_empty() {
            CommandResponse::Nil
        } else {
            CommandResponse::Array(results)
        }
    }

    fn cmd_xack(&mut self, key: &[u8], group: &str, ids: &[Vec<u8>]) -> CommandResponse {
        let compact = CompactKey::new(key);
        let mut acked = 0i64;
        if let Some(groups) = self.stream_groups.get_mut(&compact) {
            if let Some(grp) = groups.get_mut(group) {
                for id_bytes in ids {
                    if let Some(sid) = StreamId::parse(id_bytes) {
                        if let Some(pe) = grp.pel.remove(&sid) {
                            if let Some(cs) = grp.consumers.get_mut(&pe.consumer) {
                                cs.pending.remove(&sid);
                            }
                            acked += 1;
                        }
                    }
                }
            }
        }
        CommandResponse::Integer(acked)
    }

    fn cmd_xpending(
        &mut self,
        key: &[u8],
        group: &str,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        count: Option<usize>,
    ) -> CommandResponse {
        let compact = CompactKey::new(key);
        let groups = self.stream_groups.get(&compact);
        let grp = match groups.and_then(|gs| gs.get(group)) {
            Some(g) => g,
            None => {
                return CommandResponse::Error("NOGROUP No such consumer group for key".into());
            }
        };

        if let (Some(s), Some(e), Some(c)) = (start, end, count) {
            let start_id = Self::parse_range_id(s, true);
            let end_id = Self::parse_range_id(e, false);
            let limit = c;

            let mut entries: Vec<(&StreamId, &PendingEntry)> = grp
                .pel
                .iter()
                .filter(|(sid, _)| **sid >= start_id && **sid <= end_id)
                .collect();
            entries.sort_by_key(|(sid, _)| (*sid).clone());
            entries.truncate(limit);

            let results: Vec<CommandResponse> = entries
                .into_iter()
                .map(|(sid, pe)| {
                    CommandResponse::Array(vec![
                        CommandResponse::BulkString(format!("{}", sid).into_bytes()),
                        CommandResponse::BulkString(pe.consumer.as_bytes().to_vec()),
                        CommandResponse::Integer(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_millis() as u64)
                                .unwrap_or(0)
                                .saturating_sub(pe.delivery_time)
                                as i64,
                        ),
                        CommandResponse::Integer(pe.delivery_count as i64),
                    ])
                })
                .collect();

            return CommandResponse::Array(results);
        }

        let total_pending = grp.pel.len() as i64;
        let min_id = grp
            .pel
            .keys()
            .min()
            .map(|id| format!("{}", id))
            .unwrap_or_default();
        let max_id = grp
            .pel
            .keys()
            .max()
            .map(|id| format!("{}", id))
            .unwrap_or_default();

        let mut consumer_counts: HashMap<&str, i64> = HashMap::new();
        for pe in grp.pel.values() {
            *consumer_counts.entry(&pe.consumer).or_insert(0) += 1;
        }
        let consumers_resp: Vec<CommandResponse> = consumer_counts
            .into_iter()
            .map(|(name, count)| {
                CommandResponse::Array(vec![
                    CommandResponse::BulkString(name.as_bytes().to_vec()),
                    CommandResponse::BulkString(count.to_string().into_bytes()),
                ])
            })
            .collect();

        CommandResponse::Array(vec![
            CommandResponse::Integer(total_pending),
            CommandResponse::BulkString(min_id.into_bytes()),
            CommandResponse::BulkString(max_id.into_bytes()),
            CommandResponse::Array(consumers_resp),
        ])
    }

    fn cmd_xclaim(
        &mut self,
        key: &[u8],
        group: &str,
        consumer: &str,
        min_idle_time: u64,
        ids: &[Vec<u8>],
    ) -> CommandResponse {
        let compact = CompactKey::new(key);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let stream_ok = self
            .entries
            .get(&compact)
            .map(|e| matches!(&e.value, Value::Stream(_)))
            .unwrap_or(false);

        if !stream_ok {
            return CommandResponse::Array(vec![]);
        }

        let groups = match self.stream_groups.get_mut(&compact) {
            Some(g) => g,
            None => return CommandResponse::Array(vec![]),
        };
        let grp = match groups.get_mut(group) {
            Some(g) => g,
            None => return CommandResponse::Array(vec![]),
        };

        let mut claimed_ids: Vec<StreamId> = Vec::new();

        for id_bytes in ids {
            if let Some(sid) = StreamId::parse(id_bytes) {
                if let Some(pe) = grp.pel.get_mut(&sid) {
                    let idle = now_ms.saturating_sub(pe.delivery_time);
                    if idle >= min_idle_time {
                        let old_consumer = pe.consumer.clone();
                        pe.consumer = consumer.to_string();
                        pe.delivery_time = now_ms;
                        pe.delivery_count += 1;

                        if old_consumer != consumer {
                            if let Some(cs) = grp.consumers.get_mut(&old_consumer) {
                                cs.pending.remove(&sid);
                            }
                        }
                        grp.consumers
                            .entry(consumer.to_string())
                            .or_default()
                            .pending
                            .insert(sid.clone());
                        claimed_ids.push(sid);
                    }
                }
            }
        }

        let entry_map = self.entries.get(&compact);
        let results: Vec<CommandResponse> = if let Some(entry) = entry_map {
            if let Value::Stream(log) = &entry.value {
                claimed_ids
                    .iter()
                    .filter_map(|sid| {
                        log.entries
                            .iter()
                            .find(|e| e.id == *sid)
                            .map(Self::format_stream_entry)
                    })
                    .collect()
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        CommandResponse::Array(results)
    }

    fn cmd_xautoclaim(
        &mut self,
        key: &[u8],
        group: &str,
        consumer: &str,
        min_idle_time: u64,
        start: &[u8],
        count: Option<usize>,
    ) -> CommandResponse {
        let compact = CompactKey::new(key);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let start_id = Self::parse_range_id(start, true);
        let limit = count.unwrap_or(100);

        let stream_ok = self
            .entries
            .get(&compact)
            .map(|e| matches!(&e.value, Value::Stream(_)))
            .unwrap_or(false);

        if !stream_ok {
            return CommandResponse::Array(vec![
                CommandResponse::BulkString(b"0-0".to_vec()),
                CommandResponse::Array(vec![]),
                CommandResponse::Array(vec![]),
            ]);
        }

        let groups = match self.stream_groups.get_mut(&compact) {
            Some(g) => g,
            None => {
                return CommandResponse::Array(vec![
                    CommandResponse::BulkString(b"0-0".to_vec()),
                    CommandResponse::Array(vec![]),
                    CommandResponse::Array(vec![]),
                ])
            }
        };
        let grp = match groups.get_mut(group) {
            Some(g) => g,
            None => {
                return CommandResponse::Array(vec![
                    CommandResponse::BulkString(b"0-0".to_vec()),
                    CommandResponse::Array(vec![]),
                    CommandResponse::Array(vec![]),
                ])
            }
        };

        let mut eligible: Vec<StreamId> = grp
            .pel
            .iter()
            .filter(|(sid, pe)| {
                **sid >= start_id && now_ms.saturating_sub(pe.delivery_time) >= min_idle_time
            })
            .map(|(sid, _)| sid.clone())
            .collect();
        eligible.sort();
        eligible.truncate(limit);

        let next_start = eligible
            .last()
            .map(|sid| StreamId {
                ms: sid.ms,
                seq: sid.seq + 1,
            })
            .unwrap_or(StreamId::min_id());

        let mut claimed_ids: Vec<StreamId> = Vec::new();
        for sid in &eligible {
            if let Some(pe) = grp.pel.get_mut(sid) {
                let old_consumer = pe.consumer.clone();
                pe.consumer = consumer.to_string();
                pe.delivery_time = now_ms;
                pe.delivery_count += 1;
                if old_consumer != consumer {
                    if let Some(cs) = grp.consumers.get_mut(&old_consumer) {
                        cs.pending.remove(sid);
                    }
                }
                grp.consumers
                    .entry(consumer.to_string())
                    .or_default()
                    .pending
                    .insert(sid.clone());
                claimed_ids.push(sid.clone());
            }
        }

        let entry_map = self.entries.get(&compact);
        let entries_resp: Vec<CommandResponse> = if let Some(entry) = entry_map {
            if let Value::Stream(log) = &entry.value {
                claimed_ids
                    .iter()
                    .filter_map(|sid| {
                        log.entries
                            .iter()
                            .find(|e| e.id == *sid)
                            .map(Self::format_stream_entry)
                    })
                    .collect()
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        CommandResponse::Array(vec![
            CommandResponse::BulkString(format!("{}", next_start).into_bytes()),
            CommandResponse::Array(entries_resp),
            CommandResponse::Array(vec![]),
        ])
    }

    fn cmd_xinfo_stream(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Stream(log) => {
                    let length = log.entries.len() as i64;
                    let groups_count = self
                        .stream_groups
                        .get(&compact)
                        .map(|gs| gs.len() as i64)
                        .unwrap_or(0);

                    CommandResponse::Array(vec![
                        CommandResponse::BulkString(b"length".to_vec()),
                        CommandResponse::Integer(length),
                        CommandResponse::BulkString(b"first-entry".to_vec()),
                        if let Some(first) = log.entries.front() {
                            Self::format_stream_entry(first)
                        } else {
                            CommandResponse::Nil
                        },
                        CommandResponse::BulkString(b"last-entry".to_vec()),
                        if let Some(last) = log.entries.back() {
                            Self::format_stream_entry(last)
                        } else {
                            CommandResponse::Nil
                        },
                        CommandResponse::BulkString(b"last-generated-id".to_vec()),
                        CommandResponse::BulkString(format!("{}", log.last_id).into_bytes()),
                        CommandResponse::BulkString(b"groups".to_vec()),
                        CommandResponse::Integer(groups_count),
                    ])
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Error("ERR no such key".into()),
        }
    }

    fn cmd_xinfo_groups(&mut self, key: &[u8]) -> CommandResponse {
        self.lazy_expire(key);
        let compact = CompactKey::new(key);
        match self.entries.get(&compact) {
            Some(entry) => match &entry.value {
                Value::Stream(_) => {
                    let groups = self.stream_groups.get(&compact);
                    let results: Vec<CommandResponse> = groups
                        .map(|gs| {
                            gs.iter()
                                .map(|(name, grp)| {
                                    CommandResponse::Array(vec![
                                        CommandResponse::BulkString(b"name".to_vec()),
                                        CommandResponse::BulkString(name.as_bytes().to_vec()),
                                        CommandResponse::BulkString(b"consumers".to_vec()),
                                        CommandResponse::Integer(grp.consumers.len() as i64),
                                        CommandResponse::BulkString(b"pending".to_vec()),
                                        CommandResponse::Integer(grp.pel.len() as i64),
                                        CommandResponse::BulkString(b"last-delivered-id".to_vec()),
                                        CommandResponse::BulkString(
                                            format!("{}", grp.last_delivered_id).into_bytes(),
                                        ),
                                    ])
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    CommandResponse::Array(results)
                }
                _ => CommandResponse::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            },
            None => CommandResponse::Error("ERR no such key".into()),
        }
    }
}

/// Format a float with the minimum precision needed for round-trip fidelity.
fn format_float(f: f64) -> String {
    if f.fract() == 0.0 && f.abs() < 1e17 {
        let i = f as i64;
        if (i as f64) == f {
            return format!("{i}");
        }
    }
    for precision in 0..=17 {
        let s = format!("{f:.precision$}");
        if s.parse::<f64>().ok() == Some(f) {
            return s;
        }
    }
    format!("{f:.17}")
}

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

fn lex_gte_min(member: &[u8], min: &[u8]) -> bool {
    if min.is_empty() {
        return true;
    }
    match min[0] {
        b'-' => true,
        b'+' => false,
        b'[' => member >= &min[1..],
        b'(' => member > &min[1..],
        _ => true,
    }
}

fn lex_lte_max(member: &[u8], max: &[u8]) -> bool {
    if max.is_empty() {
        return true;
    }
    match max[0] {
        b'+' => true,
        b'-' => false,
        b'[' => member <= &max[1..],
        b'(' => member < &max[1..],
        _ => true,
    }
}

fn lex_in_range(member: &[u8], min: &[u8], max: &[u8]) -> bool {
    lex_gte_min(member, min) && lex_lte_max(member, max)
}

// ─── HyperLogLog algorithm ──────────────────────────────────────────

const HLL_P: usize = 14;
const HLL_REGISTERS: usize = 1 << HLL_P;
const HLL_REGISTER_BYTES: usize = HLL_REGISTERS * 6 / 8;

fn hll_hash(element: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = ahash::AHasher::default();
    element.hash(&mut hasher);
    hasher.finish()
}

fn hll_register_get(registers: &[u8], index: usize) -> u8 {
    let bit_offset = index * 6;
    let byte_offset = bit_offset / 8;
    let bit_shift = bit_offset % 8;

    if bit_shift <= 2 {
        (registers[byte_offset] >> bit_shift) & 0x3F
    } else {
        let lo = registers[byte_offset] >> bit_shift;
        let hi = if byte_offset + 1 < registers.len() {
            registers[byte_offset + 1] << (8 - bit_shift)
        } else {
            0
        };
        (lo | hi) & 0x3F
    }
}

fn hll_register_set(registers: &mut [u8], index: usize, value: u8) {
    let bit_offset = index * 6;
    let byte_offset = bit_offset / 8;
    let bit_shift = bit_offset % 8;

    let mask_lo = !(0x3F_u8 << bit_shift);
    registers[byte_offset] = (registers[byte_offset] & mask_lo) | ((value & 0x3F) << bit_shift);

    if bit_shift > 2 && byte_offset + 1 < registers.len() {
        let bits_in_first = 8 - bit_shift;
        let mask_hi = !((0x3F >> bits_in_first) as u8);
        registers[byte_offset + 1] =
            (registers[byte_offset + 1] & mask_hi) | ((value & 0x3F) >> bits_in_first);
    }
}

fn hll_add(registers: &mut [u8], element: &[u8]) -> bool {
    let hash = hll_hash(element);
    let index = (hash & ((1 << HLL_P) - 1)) as usize;
    let remaining = hash >> HLL_P;
    let rank = if remaining == 0 {
        (64 - HLL_P) as u8 + 1
    } else {
        (remaining.trailing_zeros() + 1) as u8
    };

    let current = hll_register_get(registers, index);
    if rank > current {
        hll_register_set(registers, index, rank);
        true
    } else {
        false
    }
}

fn hll_count(registers: &[u8]) -> u64 {
    let m = HLL_REGISTERS as f64;
    let alpha = match HLL_REGISTERS {
        16 => 0.673,
        32 => 0.697,
        64 => 0.709,
        _ => 0.7213 / (1.0 + 1.079 / m),
    };

    let mut sum = 0.0f64;
    let mut zeros = 0u32;

    for i in 0..HLL_REGISTERS {
        let val = hll_register_get(registers, i);
        sum += 1.0 / (1u64 << val) as f64;
        if val == 0 {
            zeros += 1;
        }
    }

    let mut estimate = alpha * m * m / sum;

    if estimate <= 2.5 * m && zeros > 0 {
        estimate = m * (m / zeros as f64).ln();
    }

    estimate.round() as u64
}

fn hll_merge(dest: &mut [u8], src: &[u8]) {
    for i in 0..HLL_REGISTERS {
        let src_val = hll_register_get(src, i);
        let dst_val = hll_register_get(dest, i);
        if src_val > dst_val {
            hll_register_set(dest, i, src_val);
        }
    }
}

// ─── Geospatial helpers ──────────────────────────────────────────────

const GEO_HASH_BITS: u32 = 52;

fn geo_encode(lon: f64, lat: f64) -> f64 {
    let lon_norm = (lon + 180.0) / 360.0;
    let lat_norm = (lat + 90.0) / 180.0;

    let mut hash: u64 = 0;
    let mut lon_min = 0.0f64;
    let mut lon_max = 1.0f64;
    let mut lat_min = 0.0f64;
    let mut lat_max = 1.0f64;

    for i in 0..GEO_HASH_BITS {
        if i % 2 == 0 {
            let mid = (lon_min + lon_max) / 2.0;
            if lon_norm >= mid {
                hash |= 1 << (GEO_HASH_BITS - 1 - i);
                lon_min = mid;
            } else {
                lon_max = mid;
            }
        } else {
            let mid = (lat_min + lat_max) / 2.0;
            if lat_norm >= mid {
                hash |= 1 << (GEO_HASH_BITS - 1 - i);
                lat_min = mid;
            } else {
                lat_max = mid;
            }
        }
    }

    f64::from_bits(hash)
}

fn geo_decode(score: f64) -> (f64, f64) {
    let hash = score.to_bits();

    let mut lon_min = 0.0f64;
    let mut lon_max = 1.0f64;
    let mut lat_min = 0.0f64;
    let mut lat_max = 1.0f64;

    for i in 0..GEO_HASH_BITS {
        let bit = (hash >> (GEO_HASH_BITS - 1 - i)) & 1;
        if i % 2 == 0 {
            let mid = (lon_min + lon_max) / 2.0;
            if bit == 1 {
                lon_min = mid;
            } else {
                lon_max = mid;
            }
        } else {
            let mid = (lat_min + lat_max) / 2.0;
            if bit == 1 {
                lat_min = mid;
            } else {
                lat_max = mid;
            }
        }
    }

    let lon = (lon_min + lon_max) / 2.0 * 360.0 - 180.0;
    let lat = (lat_min + lat_max) / 2.0 * 180.0 - 90.0;
    (lon, lat)
}

const BASE32_CHARS: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

fn geo_encode_base32(lon: f64, lat: f64) -> String {
    let lon_norm = (lon + 180.0) / 360.0;
    let lat_norm = (lat + 90.0) / 180.0;

    let mut bits: Vec<bool> = Vec::with_capacity(55);
    let mut lon_min = 0.0f64;
    let mut lon_max = 1.0f64;
    let mut lat_min = 0.0f64;
    let mut lat_max = 1.0f64;

    for i in 0..55 {
        if i % 2 == 0 {
            let mid = (lon_min + lon_max) / 2.0;
            if lon_norm >= mid {
                bits.push(true);
                lon_min = mid;
            } else {
                bits.push(false);
                lon_max = mid;
            }
        } else {
            let mid = (lat_min + lat_max) / 2.0;
            if lat_norm >= mid {
                bits.push(true);
                lat_min = mid;
            } else {
                bits.push(false);
                lat_max = mid;
            }
        }
    }

    let mut result = String::with_capacity(11);
    for chunk in bits.chunks(5) {
        let mut idx = 0u8;
        for (j, &bit) in chunk.iter().enumerate() {
            if bit {
                idx |= 1 << (4 - j);
            }
        }
        result.push(BASE32_CHARS[idx as usize] as char);
    }
    result
}

fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS: f64 = 6372797.560856;

    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();

    let a =
        (dlat / 2.0).sin().powi(2) + lat1_rad.cos() * lat2_rad.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();

    EARTH_RADIUS * c
}

fn geo_convert_distance(meters: f64, unit: GeoUnit) -> f64 {
    match unit {
        GeoUnit::Meters => meters,
        GeoUnit::Kilometers => meters / 1000.0,
        GeoUnit::Feet => meters * 3.28084,
        GeoUnit::Miles => meters / 1609.344,
    }
}

fn geo_to_meters(value: f64, unit: GeoUnit) -> f64 {
    match unit {
        GeoUnit::Meters => value,
        GeoUnit::Kilometers => value * 1000.0,
        GeoUnit::Feet => value / 3.28084,
        GeoUnit::Miles => value * 1609.344,
    }
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
    fn test_get_large_value_uses_shared_bulk_string() {
        let mut store = ShardStore::new(0);
        let value = vec![b'x'; 256];
        store.execute(Command::Set {
            key: b"large".to_vec(),
            value: value.clone(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });

        match store.execute(Command::Get {
            key: b"large".to_vec(),
        }) {
            CommandResponse::BulkStringShared(bytes) => assert_eq!(bytes.as_ref(), value),
            other => panic!("Expected BulkStringShared, got {:?}", other),
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
        assert!(matches!(resp, CommandResponse::Integer(0)));
        // Original value unchanged
        match store.execute(Command::Get { key: b"k".to_vec() }) {
            CommandResponse::BulkString(v) => assert_eq!(v, b"v1"),
            other => panic!("Expected v1, got {:?}", other),
        }
    }

    #[test]
    fn test_set_xx_treats_expired_key_as_missing() {
        let mut store = ShardStore::new(0);
        store.execute(Command::Set {
            key: b"k".to_vec(),
            value: b"stale".to_vec(),
            ex: None,
            px: Some(1),
            nx: false,
            xx: false,
        });
        std::thread::sleep(Duration::from_millis(5));

        let resp = store.execute(Command::Set {
            key: b"k".to_vec(),
            value: b"fresh".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: true,
        });
        assert!(matches!(resp, CommandResponse::Nil));
        assert!(matches!(
            store.execute(Command::Get { key: b"k".to_vec() }),
            CommandResponse::Nil
        ));
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
    fn test_set_numeric_payload_keeps_string_encoding() {
        let mut store = ShardStore::new(0);
        store.execute(Command::Set {
            key: b"num".to_vec(),
            value: b"42".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });

        let get_resp = store.execute(Command::Get {
            key: b"num".to_vec(),
        });
        assert_eq!(get_resp.bulk_string_bytes(), Some(b"42".as_slice()));

        match store.execute(Command::ObjectEncoding {
            key: b"num".to_vec(),
        }) {
            CommandResponse::BulkString(encoding) => assert_eq!(encoding, b"embstr"),
            other => panic!("Expected embstr encoding, got {:?}", other),
        }
    }

    #[test]
    fn test_set_members_are_binary_safe() {
        let mut store = ShardStore::new(0);
        store.execute(Command::SAdd {
            key: b"codes".to_vec(),
            members: vec![b"042".to_vec()],
        });

        assert_eq!(
            store.execute(Command::SIsMember {
                key: b"codes".to_vec(),
                member: b"042".to_vec(),
            }),
            CommandResponse::Integer(1)
        );
        assert_eq!(
            store.execute(Command::SIsMember {
                key: b"codes".to_vec(),
                member: b"42".to_vec(),
            }),
            CommandResponse::Integer(0)
        );
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
