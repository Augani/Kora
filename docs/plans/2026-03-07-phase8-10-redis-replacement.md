# Phases 8–10: Full Redis Drop-in Replacement

## Performance Baseline (redis-benchmark, 100K ops)

| Command | Redis 7 (req/s) | Kōra (req/s) | Delta |
|---------|-----------------|--------------|-------|
| SET     | 100,705         | 106,045      | +5%   |
| GET     | 101,420         | 107,066      | +6%   |
| INCR    | 95,785          | 105,708      | +10%  |
| LPUSH   | 96,805          | 105,932      | +9%   |
| SADD    | 83,893          | 105,932      | +26%  |
| Pipeline SET (P16) | 1,225,490 | 150,602 | -88% |
| Pipeline GET (P16) | 941,620   | 145,307 | -85% |

Single-command: Kōra wins. Pipeline: 85% slower due to per-command channel dispatch.

---

## Phase 8 — Redis Core Compatibility

### 8.1 Pipeline Batch Dispatch (Critical Performance)

**Problem:** Each pipelined command does an individual channel round-trip:
parse → create channel → send to shard → spawn_blocking(recv) → serialize.

**Fix:** Batch dispatch — parse all commands from one read, group by shard, send batches, collect responses in order.

**Design:**
```
// New engine method
pub fn dispatch_batch(&self, commands: Vec<Command>) -> Vec<ResponseReceiver> {
    // Group commands by shard, preserving original order
    // Send batch messages to each shard
    // Return receivers in original command order
}

// New shard message type
pub struct ShardBatchMessage {
    pub commands: Vec<(Command, ResponseSender)>,
}

// Server: parse all frames, dispatch_batch, serialize all responses
```

**Files:** engine.rs, lib.rs (server)

### 8.2 Missing String Commands

GETRANGE, SETRANGE, GETDEL, GETEX, INCRBYFLOAT, MSETNX

### 8.3 Missing Key Commands

EXPIREAT, PEXPIREAT, EXPIRETIME, PEXPIRETIME, RANDOMKEY, RENAME, RENAMENX, COPY, UNLINK, TOUCH, OBJECT HELP/IDLETIME/REFCOUNT

### 8.4 Missing List Commands

LSET, LINSERT, LPOS, LREM, LMPOP

### 8.5 Missing Hash Commands

HMGET, HMSET, HKEYS, HVALS, HSETNX, HRANDFIELD, HINCRBYFLOAT

### 8.6 Missing Set Commands

SPOP, SRANDMEMBER, SMOVE, SUNION, SINTER, SDIFF, SUNIONSTORE, SINTERSTORE, SDIFFSTORE

### 8.7 Missing Sorted Set Commands

ZRANGEBYLEX, ZRANGESTORE, ZPOPMIN, ZPOPMAX, ZRANDMEMBER, ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE

### 8.8 Server/Connection Commands

CONFIG GET/SET/RESETSTAT, CLIENT SETNAME/GETNAME/LIST/ID, SELECT (multi-db), COMMAND COUNT/INFO, TIME, DBSIZE (already exists), DEBUG SLEEP, WAIT, RESET, QUIT

### 8.9 Pub/Sub

SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH

**Design:** Global `PubSubBroker` with `Arc<RwLock<HashMap<channel, Vec<Sender>>>>`. Subscribed connections enter push mode (no request-response, server pushes messages). Pattern subscriptions use glob matching.

### 8.10 Transactions (MULTI/EXEC)

MULTI, EXEC, DISCARD, WATCH

**Design:** Per-connection transaction state. MULTI starts queuing commands. EXEC dispatches all queued commands atomically to the shard. WATCH tracks key versions for optimistic locking. All keys in a transaction MUST hash to the same shard (error otherwise, same as Redis Cluster).

---

## Phase 9 — Extended Data Types & Blocking

### 9.1 Blocking Commands

BLPOP, BRPOP, BLMPOP, BZPOPMIN, BZPOPMAX, BRPOPLPUSH, BLMOVE

**Design:** Per-shard wait queues. When a blocking pop finds an empty list, the connection's response channel is parked in a wait queue keyed by the list key. When a push arrives, wake the oldest waiter.

### 9.2 HyperLogLog

PFADD, PFCOUNT, PFMERGE

### 9.3 Bitmaps

SETBIT, GETBIT, BITCOUNT, BITOP, BITPOS, BITFIELD

### 9.4 Geo

GEOADD, GEODIST, GEOHASH, GEOPOS, GEOSEARCH, GEOSEARCHSTORE

### 9.5 Lua Scripting (EVAL)

EVAL, EVALSHA, SCRIPT LOAD/EXISTS/FLUSH

**Design:** Embed mlua (LuaJIT). Scripts execute atomically on a single shard. `redis.call()` / `redis.pcall()` host functions dispatch to local ShardStore. SHA1 caching for EVALSHA.

### 9.6 SORT Command

SORT key [BY pattern] [LIMIT offset count] [GET pattern ...] [ASC|DESC] [ALPHA] [STORE dest]

---

## Phase 10 — Production Infrastructure

### 10.1 TLS Support

rustls-based TLS termination for TCP connections.

### 10.2 ACLs

ACL SETUSER, GETUSER, DELUSER, LIST, WHOAMI, CAT, LOG

Per-user command/key pattern permissions. Default user with full access.

### 10.3 Replication

REPLICAOF, PSYNC, replication stream

Leader-follower with partial resync. RDB transfer for full sync, WAL stream for incremental.

### 10.4 Slow Log

SLOWLOG GET/LEN/RESET

Per-shard ring buffer of slow commands (>N microseconds).

### 10.5 MEMORY Commands

MEMORY USAGE key, MEMORY DOCTOR, MEMORY STATS

### 10.6 Keyspace Notifications

CONFIG SET notify-keyspace-events, Pub/Sub on __keyevent__ and __keyspace__ channels.

### 10.7 RESP3 Full Support

Verbatim strings, doubles, big numbers, sets, maps, pushes.

---

## Execution Order

**Phase 8** (highest impact, broadest coverage):
1. Pipeline batch dispatch (8.1) — fixes -85% pipeline gap
2. Server/connection commands (8.8) — CONFIG, CLIENT, SELECT
3. Missing commands by type (8.2–8.7) — covers most Redis usage
4. Pub/Sub (8.9)
5. Transactions (8.10)

**Phase 9** (extended features):
1. Blocking commands (9.1) — important for job queues
2. HyperLogLog + Bitmaps (9.2, 9.3)
3. Geo (9.4)
4. Lua EVAL (9.5)
5. SORT (9.6)

**Phase 10** (production infrastructure):
1. TLS (10.1)
2. ACLs (10.2)
3. Slow log (10.4)
4. MEMORY commands (10.5)
5. Keyspace notifications (10.6)
6. Replication (10.3) — largest effort, last
7. RESP3 full (10.7)
