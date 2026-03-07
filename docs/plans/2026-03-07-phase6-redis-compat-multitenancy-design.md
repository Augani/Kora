# Phase 6: Redis Compatibility & Multi-Tenancy

## Overview

Phase 6 closes the major gaps between current implementation and the README architecture vision. Focus areas: missing Redis data structure (SortedSet), multi-tenancy, eviction policy, RESP3 protocol, and Unix socket support.

## Workstreams

### WS1: SortedSet Data Structure

**Scope:** SkipList-backed sorted set in kora-core with full command surface.

**Data structure:**
- `Value::SortedSet(BTreeMap<Vec<u8>, f64>, BTreeMap<OrderedFloat<f64>, BTreeSet<Vec<u8>>>)` — dual index for O(log N) by member and by score
- Alternative: custom skip list. BTreeMap dual-index is simpler and correct first.

**Commands:**
- ZADD, ZREM, ZSCORE, ZRANK, ZREVRANK, ZCARD
- ZRANGE, ZREVRANGE (by index with optional WITHSCORES)
- ZRANGEBYSCORE, ZREVRANGEBYSCORE (by score range with LIMIT)
- ZINCRBY, ZCOUNT

**Files touched:**
- `kora-core/src/types.rs` — add SortedSet variant to Value
- `kora-core/src/command.rs` — add ZSet command variants
- `kora-core/src/shard/store.rs` — implement sorted set operations
- `kora-protocol/src/command.rs` — parse ZADD, ZRANGE, etc.
- `kora-protocol/src/serializer.rs` — serialize sorted set responses

### WS2: Multi-Tenancy

**Scope:** Per-tenant isolation with memory quotas, rate limiting, and command filtering.

**Design:**
```
TenantConfig {
    id: TenantId (u32),
    max_memory: usize,
    max_keys: Option<u64>,
    ops_per_second: Option<u32>,  // token bucket
    blocked_commands: HashSet<String>,
}

TenantState {
    config: TenantConfig,
    memory_used: AtomicUsize,
    key_count: AtomicU64,
    tokens: AtomicU64,           // token bucket counter
    last_refill: AtomicU64,      // timestamp for token refill
}
```

**Key scoping:** Keys are prefixed internally with tenant ID. `tenant:0:` is the default tenant. AUTH command sets tenant context on the connection.

**Enforcement points:**
- Write commands check memory quota before executing
- All commands check rate limit (token bucket) before executing
- Blocked commands return error immediately
- KEYS/SCAN only iterate tenant's keyspace

**Files touched:**
- `kora-core/src/tenant.rs` (NEW) — TenantConfig, TenantState, TenantManager
- `kora-core/src/shard/store.rs` — tenant-scoped key operations
- `kora-core/src/command.rs` — AUTH command, tenant context
- `kora-server/src/lib.rs` — per-connection tenant tracking
- `kora-protocol/src/command.rs` — parse AUTH

### WS3: LFU Eviction & Memory Pressure

**Scope:** Redis-style probabilistic LFU with memory pressure detection and adaptive eviction.

**Design:**
- Add `lfu_counter: u8` and `last_access: u32` to KeyEntry
- LFU counter uses Redis's logarithmic probability: P(increment) = 1 / (counter * lfu_log_factor + 1)
- Decay: counter halves every `lfu_decay_time` minutes
- Memory limit configurable per-engine (and per-tenant via WS2)

**Eviction policy:**
- When memory exceeds limit, sample N random keys, evict the one with lowest LFU counter
- Sampling avoids full-scan cost (same approach as Redis)
- Eviction runs before each write command if memory pressure detected

**Commands:**
- OBJECT FREQ key — return LFU counter
- CONFIG SET maxmemory <bytes>
- CONFIG SET maxmemory-policy allkeys-lfu

**Files touched:**
- `kora-core/src/types.rs` — add lfu_counter, last_access to KeyEntry
- `kora-core/src/shard/store.rs` — LFU tracking on access, eviction logic
- `kora-core/src/shard/engine.rs` — memory limit config, eviction trigger
- `kora-core/src/command.rs` — OBJECT FREQ, CONFIG SET/GET

### WS4: RESP3 Protocol

**Scope:** RESP3 typed responses with HELLO negotiation, backward-compatible with RESP2.

**New RESP3 types:**
- Map (`%`) — for HGETALL, CONFIG GET (typed map instead of flat array)
- Set (`~`) — for SMEMBERS, SUNION
- Double (`,`) — for ZSCORE, float results
- Boolean (`#`) — for EXISTS, SISMEMBER
- Null (`_`) — replacing RESP2's $-1 and *-1
- Push (`>`) — for pub/sub, CDC push notifications
- Big number (`(`) — for large integers
- Verbatim string (`=`) — for human-readable text

**HELLO command:**
```
HELLO 3 [AUTH username password]
```
Returns server info map and switches connection to RESP3 mode.

**Backward compatibility:** Each connection starts in RESP2 mode. HELLO 3 upgrades. Server tracks protocol version per connection and serializes responses accordingly.

**Files touched:**
- `kora-protocol/src/resp.rs` — add RESP3 RespValue variants
- `kora-protocol/src/parser.rs` — parse RESP3 types
- `kora-protocol/src/serializer.rs` — serialize RESP3 types, protocol-aware output
- `kora-protocol/src/command.rs` — parse HELLO command
- `kora-server/src/lib.rs` — per-connection protocol version tracking

### WS5: Unix Socket Listener

**Scope:** Accept connections on Unix domain socket alongside TCP.

**Design:**
- `ServerConfig` gets optional `unix_socket: Option<PathBuf>`
- Server spawns additional Tokio task for UnixListener
- Connections handled identically to TCP (same handler, same dispatch)
- CLI arg: `--unix-socket /tmp/kora.sock`
- Config file: `unix_socket = "/tmp/kora.sock"`

**Files touched:**
- `kora-server/src/lib.rs` — UnixListener task, shared handler
- `kora-cli/src/config.rs` — unix_socket config field
- `kora-cli/src/main.rs` — --unix-socket CLI arg

## Implementation Order

All 5 workstreams are independent and can run in parallel. WS2 (multi-tenancy) and WS3 (LFU eviction) both touch kora-core/src/shard/store.rs but in different areas (key scoping vs. access tracking).

## Testing

Each workstream includes unit tests. Integration tests for new commands via TCP.
