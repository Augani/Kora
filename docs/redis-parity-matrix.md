# Redis Parity Matrix

Compatibility status of Redis commands in Kora.

**Status key:**

| Status | Meaning |
|---|---|
| `full` | Behavior matches Redis |
| `partial` | Basic functionality works, some edge cases may differ |
| `stub` | Command is accepted but returns a fixed/simplified response |
| `unsupported` | Not implemented |

---

## String

| Command | Status | Notes |
|---|---|---|
| GET | `full` | |
| SET | `full` | EX, PX, NX, XX flags supported |
| SETNX | `full` | |
| GETSET | `full` | Deprecated but supported |
| APPEND | `full` | |
| STRLEN | `full` | |
| INCR | `full` | |
| DECR | `full` | |
| INCRBY | `full` | |
| DECRBY | `full` | |
| INCRBYFLOAT | `full` | |
| MGET | `full` | Cross-shard support |
| MSET | `full` | Cross-shard support |
| MSETNX | `full` | Atomic across shards |
| GETRANGE | `full` | |
| SETRANGE | `full` | |
| GETDEL | `full` | |
| GETEX | `full` | EX, PX, EXAT, PXAT, PERSIST flags |

## Key / Generic

| Command | Status | Notes |
|---|---|---|
| DEL | `full` | Multi-key, cross-shard |
| EXISTS | `full` | Multi-key, cross-shard |
| EXPIRE | `full` | |
| PEXPIRE | `full` | |
| EXPIREAT | `full` | |
| PEXPIREAT | `full` | |
| PERSIST | `full` | |
| TTL | `full` | |
| PTTL | `full` | |
| TYPE | `full` | |
| KEYS | `full` | Glob pattern matching |
| SCAN | `full` | MATCH and COUNT supported |
| RENAME | `full` | Cross-shard support |
| RENAMENX | `full` | Cross-shard support |
| COPY | `full` | REPLACE flag supported |
| TOUCH | `full` | Multi-key |
| UNLINK | `full` | Multi-key, cross-shard |
| RANDOMKEY | `full` | |
| OBJECT ENCODING | `full` | |
| OBJECT REFCOUNT | `full` | |
| OBJECT IDLETIME | `full` | |
| OBJECT FREQ | `full` | |
| OBJECT HELP | `full` | |
| DUMP | `partial` | Internal shard dump, not Redis-serialization compatible |
| SORT | `unsupported` | |
| SORT_RO | `unsupported` | |
| WAIT | `stub` | Returns 0 immediately (no replication) |

## List

| Command | Status | Notes |
|---|---|---|
| LPUSH | `full` | Multi-value |
| RPUSH | `full` | Multi-value |
| LPOP | `full` | |
| RPOP | `full` | |
| LLEN | `full` | |
| LRANGE | `full` | Negative indices supported |
| LINDEX | `full` | Negative indices supported |
| LSET | `full` | |
| LINSERT | `full` | BEFORE/AFTER pivot |
| LREM | `full` | |
| LTRIM | `full` | |
| LPOS | `full` | RANK, COUNT, MAXLEN options |
| RPOPLPUSH | `full` | Cross-shard support |
| LMOVE | `full` | LEFT/RIGHT direction, cross-shard |
| BLPOP | `full` | Event-driven wakeups |
| BRPOP | `full` | Event-driven wakeups |
| BLMOVE | `full` | Event-driven wakeups |

## Hash

| Command | Status | Notes |
|---|---|---|
| HSET | `full` | Multi-field |
| HGET | `full` | |
| HDEL | `full` | Multi-field |
| HGETALL | `full` | |
| HLEN | `full` | |
| HEXISTS | `full` | |
| HINCRBY | `full` | |
| HINCRBYFLOAT | `full` | |
| HMGET | `full` | |
| HKEYS | `full` | |
| HVALS | `full` | |
| HSETNX | `full` | |
| HRANDFIELD | `full` | COUNT, WITHVALUES options |
| HSCAN | `full` | MATCH and COUNT supported |

## Set

| Command | Status | Notes |
|---|---|---|
| SADD | `full` | Multi-member |
| SREM | `full` | Multi-member |
| SMEMBERS | `full` | |
| SISMEMBER | `full` | |
| SCARD | `full` | |
| SPOP | `full` | Optional count |
| SRANDMEMBER | `full` | Negative count (duplicates) supported |
| SMOVE | `full` | Cross-shard support |
| SMISMEMBER | `full` | Multi-member |
| SSCAN | `full` | MATCH and COUNT supported |
| SUNION | `full` | Cross-shard support |
| SUNIONSTORE | `full` | Cross-shard support |
| SINTER | `full` | Cross-shard support |
| SINTERSTORE | `full` | Cross-shard support |
| SDIFF | `full` | Cross-shard support |
| SDIFFSTORE | `full` | Cross-shard support |
| SINTERCARD | `full` | LIMIT option supported |

## Sorted Set

| Command | Status | Notes |
|---|---|---|
| ZADD | `full` | Multi-member |
| ZREM | `full` | Multi-member |
| ZSCORE | `full` | |
| ZRANK | `full` | |
| ZREVRANK | `full` | |
| ZCARD | `full` | |
| ZRANGE | `full` | WITHSCORES option |
| ZREVRANGE | `full` | WITHSCORES option |
| ZRANGEBYSCORE | `full` | WITHSCORES, LIMIT options |
| ZREVRANGEBYSCORE | `full` | WITHSCORES, LIMIT options |
| ZINCRBY | `full` | |
| ZCOUNT | `full` | |
| ZPOPMIN | `full` | Optional count |
| ZPOPMAX | `full` | Optional count |
| ZRANGEBYLEX | `full` | LIMIT option |
| ZREVRANGEBYLEX | `full` | LIMIT option |
| ZLEXCOUNT | `full` | |
| ZMSCORE | `full` | Multi-member |
| ZRANDMEMBER | `full` | COUNT, WITHSCORES options |
| ZSCAN | `full` | MATCH and COUNT supported |
| BZPOPMIN | `full` | Event-driven wakeups |
| BZPOPMAX | `full` | Event-driven wakeups |
| ZUNIONSTORE | `unsupported` | |
| ZINTERSTORE | `unsupported` | |
| ZDIFFSTORE | `unsupported` | |
| ZUNION | `unsupported` | |
| ZINTER | `unsupported` | |
| ZDIFF | `unsupported` | |
| ZRANGESTORE | `unsupported` | |
| ZREMRANGEBYRANK | `unsupported` | |
| ZREMRANGEBYSCORE | `unsupported` | |
| ZREMRANGEBYLEX | `unsupported` | |

## HyperLogLog

| Command | Status | Notes |
|---|---|---|
| PFADD | `full` | |
| PFCOUNT | `full` | Multi-key union cardinality |
| PFMERGE | `full` | Multi-source |

## Stream

| Command | Status | Notes |
|---|---|---|
| XADD | `full` | MAXLEN trimming, auto-ID generation |
| XLEN | `full` | |
| XRANGE | `full` | COUNT option |
| XREVRANGE | `full` | COUNT option |
| XTRIM | `full` | MAXLEN strategy |
| XREAD | `full` | Multi-key, COUNT option |
| XGROUP CREATE | `full` | MKSTREAM option |
| XGROUP DESTROY | `full` | |
| XGROUP DELCONSUMER | `full` | |
| XREADGROUP | `full` | GROUP/consumer, COUNT option |
| XACK | `full` | Multi-ID |
| XPENDING | `full` | Optional range filter |
| XCLAIM | `full` | Min-idle-time |
| XAUTOCLAIM | `full` | COUNT option |
| XINFO STREAM | `full` | |
| XINFO GROUPS | `full` | |
| XDEL | `full` | Multi-ID |

## Pub/Sub

| Command | Status | Notes |
|---|---|---|
| SUBSCRIBE | `full` | Multi-channel |
| UNSUBSCRIBE | `full` | |
| PSUBSCRIBE | `full` | Glob pattern matching |
| PUNSUBSCRIBE | `full` | |
| PUBLISH | `full` | |

## Transaction

| Command | Status | Notes |
|---|---|---|
| MULTI | `full` | |
| EXEC | `full` | |
| DISCARD | `full` | |
| WATCH | `full` | Optimistic locking, multi-key |
| UNWATCH | `full` | |

## Scripting

| Command | Status | Notes |
|---|---|---|
| EVAL | `unsupported` | Lua scripting not implemented |
| EVALSHA | `unsupported` | Lua scripting not implemented |
| SCRIPT LOAD | `unsupported` | Lua scripting not implemented |
| SCRIPT EXISTS | `unsupported` | Lua scripting not implemented |
| SCRIPT FLUSH | `unsupported` | Lua scripting not implemented |
| SCRIPTLOAD | `partial` | WASM module loading (Kora-specific, not Redis Lua) |
| SCRIPTCALL | `partial` | WASM function invocation (Kora-specific, not Redis Lua) |
| SCRIPTDEL | `partial` | WASM module unloading (Kora-specific, not Redis Lua) |

## Geo

| Command | Status | Notes |
|---|---|---|
| GEOADD | `full` | NX, XX, CH flags |
| GEODIST | `full` | m, km, ft, mi units |
| GEOHASH | `full` | Multi-member |
| GEOPOS | `full` | Multi-member |
| GEOSEARCH | `full` | FROMMEMBER, FROMLONLAT, BYRADIUS, ASC/DESC, WITHCOORD/WITHDIST/WITHHASH |
| GEOSEARCHSTORE | `unsupported` | |
| GEORADIUS | `unsupported` | Deprecated; use GEOSEARCH |
| GEORADIUSBYMEMBER | `unsupported` | Deprecated; use GEOSEARCH |

## Bitmap

| Command | Status | Notes |
|---|---|---|
| SETBIT | `full` | |
| GETBIT | `full` | |
| BITCOUNT | `full` | BYTE and BIT range modes |
| BITPOS | `full` | BYTE and BIT range modes |
| BITFIELD | `full` | GET, SET, INCRBY, OVERFLOW operations |
| BITOP | `full` | AND, OR, XOR, NOT operations |

## Server / Connection

| Command | Status | Notes |
|---|---|---|
| PING | `full` | Optional message echo |
| ECHO | `full` | |
| INFO | `full` | Section filtering |
| DBSIZE | `full` | Cross-shard aggregation |
| FLUSHDB | `full` | |
| FLUSHALL | `full` | |
| BGSAVE | `full` | Triggers RDB snapshot |
| BGREWRITEAOF | `full` | Triggers WAL rewrite |
| COMMAND COUNT | `full` | |
| COMMAND LIST | `full` | |
| COMMAND HELP | `full` | |
| COMMAND INFO | `full` | |
| COMMAND DOCS | `full` | |
| COMMAND GETKEYS | `full` | |
| COMMAND GETKEYSANDFLAGS | `full` | |
| CONFIG GET | `partial` | 29 common parameters; returns sensible defaults, not live state for most |
| CONFIG SET | `stub` | Accepts 10 parameters as no-ops |
| CONFIG RESETSTAT | `stub` | Accepted, no-op |
| CLIENT ID | `full` | |
| CLIENT GETNAME | `full` | |
| CLIENT SETNAME | `full` | |
| CLIENT LIST | `full` | |
| CLIENT INFO | `full` | |
| HELLO | `full` | RESP2/RESP3 negotiation |
| AUTH | `full` | Optional tenant identifier |
| SELECT | `partial` | DB 0 only; returns error for other indexes (single-DB architecture) |
| QUIT | `full` | |
| WAIT | `stub` | Returns 0 immediately (standalone, no replication) |
| TIME | `full` | |

## Cluster

| Command | Status | Notes |
|---|---|---|
| CLUSTER INFO | `unsupported` | Standalone architecture |
| CLUSTER NODES | `unsupported` | Standalone architecture |
| CLUSTER MEET | `unsupported` | Standalone architecture |
| CLUSTER ADDSLOTS | `unsupported` | Standalone architecture |
| CLUSTER DELSLOTS | `unsupported` | Standalone architecture |
| CLUSTER SETSLOT | `unsupported` | Standalone architecture |
| CLUSTER REPLICATE | `unsupported` | Standalone architecture |
| CLUSTER FAILOVER | `unsupported` | Standalone architecture |
| CLUSTER RESET | `unsupported` | Standalone architecture |
| CLUSTER SLOTS | `unsupported` | Standalone architecture |
| CLUSTER SHARDS | `unsupported` | Standalone architecture |
| CLUSTER MYID | `unsupported` | Standalone architecture |
| CLUSTER KEYSLOT | `unsupported` | Standalone architecture |
| CLUSTER COUNTKEYSINSLOT | `unsupported` | Standalone architecture |
| CLUSTER GETKEYSINSLOT | `unsupported` | Standalone architecture |
| READONLY | `unsupported` | Standalone architecture |
| READWRITE | `unsupported` | Standalone architecture |

## Unsupported Categories

| Category | Status | Notes |
|---|---|---|
| ACL commands | `unsupported` | ACL SETUSER, ACL GETUSER, ACL DELUSER, ACL LIST, ACL LOG, etc. |
| DEBUG commands | `unsupported` | DEBUG SLEEP, DEBUG OBJECT, DEBUG SET-ACTIVE-EXPIRE, etc. |
| LATENCY commands | `unsupported` | LATENCY LATEST, LATENCY HISTORY, LATENCY RESET, etc. |
| MEMORY commands | `unsupported` | MEMORY USAGE, MEMORY DOCTOR, MEMORY STATS, etc. |
| MODULE commands | `unsupported` | MODULE LOAD, MODULE UNLOAD, MODULE LIST, etc. |
| SLOWLOG commands | `unsupported` | SLOWLOG GET, SLOWLOG LEN, SLOWLOG RESET |
| SWAPDB | `unsupported` | Single-DB architecture |
| MIGRATE | `unsupported` | No cluster support |
| OBJECT HELP | `full` | Listed under Key/Generic |
| REPLICAOF / SLAVEOF | `unsupported` | No replication support |
| FAILOVER | `unsupported` | No replication support |
| PSYNC / REPLCONF | `unsupported` | No replication support |

---

## Known Divergences

1. **Single-DB architecture** -- `SELECT` only accepts database index 0. Any other index returns an error. Kora uses a single flat keyspace by design.

2. **WAIT returns 0 immediately** -- Kora is a standalone engine with no replication. `WAIT` is accepted for client compatibility but always returns 0 replicas acknowledged.

3. **CONFIG GET returns sensible defaults** -- Most parameters return reasonable default values rather than reflecting live server state. 29 common parameters are recognized. `CONFIG SET` accepts 10 parameters as no-ops for client library compatibility.

4. **WASM scripting instead of Lua** -- Kora uses wasmtime for server-side scripting via `SCRIPTLOAD`, `SCRIPTCALL`, and `SCRIPTDEL`. Redis Lua commands (`EVAL`, `EVALSHA`, `SCRIPT`) are not supported.

5. **No cluster mode** -- Kora is a standalone, multi-threaded engine. All cluster commands are unsupported. Sharding is internal (shared-nothing worker threads), not distributed across nodes.

6. **Blocking commands use event-driven wakeups** -- `BLPOP`, `BRPOP`, `BLMOVE`, `BZPOPMIN`, and `BZPOPMAX` have the same semantics as Redis but use event-driven notifications internally instead of polling. This provides equivalent behavior with better performance characteristics.

7. **DUMP is internal-only** -- The `DUMP` command extracts shard data for internal use. It does not produce Redis-compatible serialized representations.

8. **ZUNIONSTORE / ZINTERSTORE / ZDIFFSTORE not implemented** -- Aggregate sorted set operations that store results are not yet available.

9. **SORT / SORT_RO not implemented** -- The generic SORT command for lists, sets, and sorted sets is not available.

10. **GEOSEARCHSTORE not implemented** -- Geo search with result storage is not yet available.
