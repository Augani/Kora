# Kōra — Implementation Plan

> Detailed engineering roadmap. Updated to reflect current implementation status.

---

## Phase 0 — Project Scaffolding ✅

**Status: Complete**

- Cargo workspace with 10 crates
- Shared dependency versions via `[workspace.dependencies]`
- Rust edition 2021, MSRV 1.75
- Release profile with thin LTO, codegen-units=1, strip=true

---

## Phase 1 — Core Engine ✅

**Status: Complete**

### 1.1 — kora-core: Data Structures ✅

- `Value` enum: String (Vec<u8>), Int (i64), List (VecDeque), Set (HashSet), Hash (HashMap)
- `KeyEntry`: key + value + optional TTL (Instant-based)
- `ShardStore`: per-thread HashMap-based storage with lazy expiry

### 1.2 — kora-core: Threading Model ✅

- `ShardEngine`: spawns N worker threads, routes commands by key hash
- Lock-free MPSC channels (crossbeam-channel) for command dispatch
- Blocking dispatch with oneshot response channels
- Multi-key fan-out for MGET/MSET/DEL/EXISTS across shards

### 1.3 — kora-protocol: RESP Parser ✅

- Streaming `RespParser` with `BytesMut` buffer
- `RespValue` enum: SimpleString, Error, Integer, BulkString, Array
- `parse_command`: converts RespValue arrays to `Command` enum
- `serialize_response`: converts `CommandResponse` to RESP wire format

### 1.4 — kora-server: TCP Server ✅

- Async TCP listener with Tokio
- Per-connection handler with pipeline support
- Command dispatch to ShardEngine
- Graceful shutdown via watch channel

### 1.5 — kora-embedded: Library API ✅

- `Database` struct wrapping `ShardEngine`
- Direct API: `get`, `set`, `del`, `expire`, `ttl`, `keys`, `dbsize`, `flushdb`
- Thread-safe via `Arc<ShardEngine>`

### 1.6 — kora-cli: Binary Entry Point ✅

- Clap-based CLI with args: `--bind`, `--port`, `--workers`, `--log-level`, `--data-dir`, `--config`
- TOML config file support with layered defaults

---

## Phase 2 — Storage & Persistence ✅

**Status: Complete**

### 2.1 — Write-Ahead Log ✅

- Binary format: `[len: u32][type: u8][payload...][crc32: u32]`
- `WalEntry` enum: Set, Del, Expire, LPush, RPush, HSet, SAdd, FlushDb
- `SyncPolicy`: EveryWrite, EverySecond, OsManaged
- Crash recovery: replays until first corrupt/truncated entry
- Rotation and truncation support

### 2.2 — RDB Snapshots ✅

- Custom binary format: `[magic: "KORA_RDB"][version: u32][entries...][crc32: u32]`
- `RdbEntry` with key, `RdbValue` (String, Int, List, Set, Hash), optional TTL
- Atomic writes via temp file + rename
- CRC-32C verification on load

### 2.3 — Cold-Tier Storage Backend ✅

- `StorageBackend` trait: read, write, delete, sync
- `FileBackend`: append-only data file with in-memory index
- LZ4 compression via `lz4_flex`
- Compaction to reclaim space from deleted entries
- Index persistence

### 2.4 — Storage Manager ✅

- `StorageManager`: coordinates WAL + RDB + FileBackend
- `StorageConfig`: data_dir, sync policy, WAL/RDB enable flags, max WAL size
- Auto-rotation when WAL exceeds size limit
- RDB save auto-truncates WAL

---

## Phase 3 — Advanced Features ✅

**Status: Complete**

### 3.1 — Vector Index (kora-vector) ✅

- HNSW (Hierarchical Navigable Small World) approximate nearest neighbor index
- Multi-layer navigable graph with greedy descent
- Distance metrics: Cosine, L2, InnerProduct
- Configurable M, ef_construction, ef_search parameters
- Insert, delete (lazy), search operations
- ≥80% recall at k=10 with 500 vectors (verified by test)

### 3.2 — CDC — Change Data Capture (kora-cdc) ✅

- `CdcRing`: fixed-size per-shard ring buffer with monotonic sequence numbers
- `CdcEvent`: seq, timestamp_ms, op, key, value
- Gap detection when consumer falls behind (ring overwrite)
- `SubscriptionManager`: subscribe/unsubscribe with glob-pattern filtering
- Cursor-based polling with seek support

### 3.3 — WASM Scripting (kora-scripting) ✅

- `WasmRuntime` wrapping wasmtime engine
- `FunctionRegistry` for loading/calling WASM modules
- Fuel-based execution limits
- Module caching by SHA-256 hash

### 3.4 — Observability (kora-observability) ✅

- `CountMinSketch`: probabilistic frequency estimation (~2KB default)
  - Configurable width/depth, increment, estimate, decay, reset
- `ShardStats`: atomic counters for commands, durations, keys, memory, bytes I/O
- `CommandTimer`: RAII guard that records duration on drop
- `StatsSnapshot` with merge for aggregating across shards
- Hot key tracking via embedded sketch

---

## Phase 4 — Production Hardening ✅

**Status: Complete**

### 4.1 — Stress Tests ✅

- **kora-core/tests/stress.rs** (7 tests):
  - Concurrent SET/GET (8 threads × 10K ops)
  - Mixed command stress (SET/GET/DEL/INCR/EXPIRE)
  - Multi-key fan-out (MSET/MGET/DEL 500 keys)
  - TTL expiration (500 keys with 100ms PX)
  - List/Hash/Set concurrent operations
  - Large key count (100K keys via MSET batches)
  - FlushDb under concurrent load

- **kora-protocol/tests/stress.rs** (11 tests):
  - Fuzz: 10K random byte sequences → parser never panics
  - Fuzz: random bytes with RESP prefixes
  - Roundtrip: 2K random CommandResponses serialize → parse → match
  - Incremental byte-by-byte parsing matches bulk parsing
  - Pipeline: 500 concatenated frames parsed correctly
  - Pipeline: standard Redis command sequences
  - Large bulk strings (1MB, 100KB binary)
  - Deeply nested arrays (50 levels)
  - Wide arrays (10K elements)

- **kora-server/tests/integration.rs** (16 tests):
  - TCP integration: PING, SET/GET, DEL, EXISTS, INCR
  - LPUSH/LRANGE, HSET/HGET, SADD/SMEMBERS
  - DBSIZE, FLUSHDB
  - Pipeline: 5 commands in single write
  - Error handling: wrong arity, WRONGTYPE, unknown command
  - SET overwrite, ECHO, INCR on non-integer
  - Multiple data type isolation

### 4.2 — Benchmarks ✅

- **kora-core/benches/engine.rs**: SET, GET (hit/miss), INCR, MGET (10/100 keys)
- **kora-protocol/benches/resp.rs**: parse simple string, integer, 1KB bulk string, SET command array, 10-command pipeline, serialize (OK, 1KB bulk, 10-element array)
- **kora-vector/benches/hnsw.rs**: insert 100 vectors, search k=10/k=50 on 1000 vectors, distance computation (Cosine/L2/InnerProduct, 128D)

### 4.3 — Configuration ✅

- TOML config file support (`kora-cli/src/config.rs`)
- Layered config: CLI args → config file → hardcoded defaults
- All server parameters configurable

### 4.4 — Documentation ✅

- All public APIs documented with `///` comments
- `cargo doc --workspace --no-deps` builds with zero warnings
- Module-level docs on all crates
- CLAUDE.md and PLAN.md updated

---

## Test Summary

| Crate | Unit Tests | Integration/Stress Tests | Benchmarks |
|-------|-----------|-------------------------|------------|
| kora-core | ✅ | 7 stress tests | 5 benchmarks |
| kora-protocol | ✅ | 11 stress tests | 8 benchmarks |
| kora-server | ✅ | 16 integration tests | — |
| kora-embedded | ✅ | — | — |
| kora-storage | ✅ (35 tests across modules) | — | — |
| kora-vector | ✅ (16 tests) | — | 6 benchmarks |
| kora-cdc | ✅ (14 tests) | — | — |
| kora-observability | ✅ (15 tests) | — | — |
| kora-scripting | ✅ | — | — |

---

## Architecture Decisions

1. **Shared-nothing sharding** over global locks — enables linear scaling with core count
2. **Crossbeam channels** over std mpsc — better performance for bounded channels
3. **ahash** over std HashMap — faster non-cryptographic hashing
4. **Custom RESP parser** over existing crate — zero-copy streaming, no allocations on parse path
5. **Custom RDB format** (not Redis-compatible) — simpler, includes CRC integrity
6. **LZ4 compression** for cold tier — best speed/ratio tradeoff for cache workloads
7. **HNSW** over IVF/LSH — better recall/speed tradeoff for moderate dataset sizes
8. **Count-Min Sketch** for hot keys — O(1) space, no heap allocation per key
9. **Atomic counters** for stats — zero contention on the data path

---

*Implementation complete. All phases delivered.*
