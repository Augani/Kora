# Phase 5: Deep Integration — Design Document

Date: 2026-03-07

## Goal

Wire all advanced features (vector search, CDC, scripting, observability) into the engine properly, making Kora a fully integrated system rather than a collection of standalone crates.

## Workstreams

### WS1: Per-Shard Vector Indexes

Move vector indexes from global server mutex to per-shard ownership.

- Add `HashMap<CompactKey, HnswIndex>` to `ShardStore`
- Handle `VecSet`/`VecDel` directly in `ShardStore::execute()`
- `VecQuery` becomes a fan-out command dispatched to all shards; dispatcher merges top-K results
- Remove global `Mutex<HashMap>` from server state
- Add `ProductQuantizer` to kora-vector: configurable codebook size, encode/decode vectors, ~4x memory reduction
- Add `VECSET key dim v1 v2 ... [METRIC cosine|l2|ip]` metric selection

Files: `kora-core/src/shard/store.rs`, `kora-core/src/shard/engine.rs`, `kora-vector/src/quantizer.rs` (new), `kora-server/src/lib.rs`

### WS2: WASM Host Functions

Bridge WASM scripts to the shard engine so they can read/write data.

- Define host functions via wasmtime linker: `kora_get(key) -> bytes`, `kora_set(key, value)`, `kora_del(key) -> bool`, `kora_hget(key, field) -> bytes`, `kora_hset(key, field, value)`
- Pass `Arc<ShardEngine>` into WASM execution context
- Linear memory import/export for byte buffer passing between host and guest
- Update `ScriptCall` command to accept byte args (not just i64)
- Add fuel and memory limits validation

Files: `kora-scripting/src/lib.rs`, `kora-scripting/src/host.rs` (new), `kora-core/src/command.rs`, `kora-server/src/lib.rs`

### WS3: CDC Consumer Groups

Add named consumer groups with persistent cursor tracking.

- `ConsumerGroup` struct: group name -> consumer name -> last_acked_seq
- `PendingEntry`: event seq, delivery count, first/last delivery timestamp
- Commands: `CDC.SUBSCRIBE pattern GROUP name CONSUMER id`, `CDC.ACK group seq`, `CDC.PENDING group`
- Push-based delivery via spawned Tokio task per subscription
- Timeout-based redelivery for unacknowledged entries (configurable, default 30s)
- Gap detection when ring buffer overwrites unacked entries

Files: `kora-cdc/src/consumer.rs` (new), `kora-cdc/src/subscription.rs`, `kora-core/src/command.rs`, `kora-server/src/lib.rs`

### WS4: Observability — Histograms + Prometheus

Add latency distributions, memory attribution, and metrics export.

- HDR histogram per command type in `ShardStats` (P50/P99/P999)
- Update `CommandTimer` to record into histogram on drop
- `PrefixTrie<AtomicUsize>` for memory attribution by key prefix
- Commands: `STATS.HOTKEYS count`, `STATS.LATENCY cmd P50 P99 P999`, `STATS.MEMORY prefix...`
- HTTP `/metrics` endpoint using hyper, bound to configurable port (default 9100)
- Prometheus exposition format output

Files: `kora-observability/src/histogram.rs` (new), `kora-observability/src/trie.rs` (new), `kora-observability/src/prometheus.rs` (new), `kora-observability/src/stats.rs`, `kora-server/src/lib.rs`, `kora-cli/src/config.rs`

### WS5: Embedded Mode Completion

Expose vector operations and hybrid mode through the library API.

- Add to `Database`: `vector_set(index, dim, vector)`, `vector_search(index, query, k)`, `vector_del(index)`
- Add `start_listener(addr) -> JoinHandle` for hybrid mode — spawns KoraServer on background Tokio runtime sharing the same engine
- Expose CDC polling and stats snapshot through embedded API
- Add integration tests for hybrid mode (embedded + TCP on same engine)

Files: `kora-embedded/src/lib.rs`, `kora-embedded/Cargo.toml`

### WS6: Per-Shard Storage Integration

Make WAL/RDB/cold-tier shard-aware for true shared-nothing persistence.

- `ShardStorage` struct: per-shard WAL file + cold backend partition
- Move WAL append into `ShardStore::execute()` (mutation logged in the owning shard's thread)
- Per-shard RDB partition files: `data_dir/shard-{N}.rdb`, `data_dir/shard-{N}.wal`
- `BGSAVE` triggers concurrent per-shard snapshots via `tokio::task::spawn_blocking`
- Recovery: replay each shard's WAL independently on startup

Files: `kora-storage/src/shard_storage.rs` (new), `kora-core/src/shard/store.rs`, `kora-core/src/shard/engine.rs`, `kora-server/src/lib.rs`

## Dependencies Between Workstreams

```
WS1 (per-shard vectors) — independent
WS2 (WASM host fns) — independent
WS3 (CDC consumer groups) — independent
WS4 (observability) — independent
WS5 (embedded) — depends on WS1 (vector API needs per-shard vectors)
WS6 (per-shard storage) — independent (but WS1 benefits from it)
```

WS1-WS4 and WS6 can run in parallel. WS5 should start after WS1 completes.

## Testing Strategy

Each workstream adds:
- Unit tests for new structs/functions
- Integration tests in kora-server for end-to-end command flow
- Stress tests where applicable (concurrent vector inserts, CDC consumer groups under load)

## Success Criteria

- All vector commands route through shards, not global mutex
- WASM scripts can read/write keys in the store
- CDC consumer groups track cursors and redeliver unacked events
- `STATS.LATENCY GET P99` returns a real percentile
- `/metrics` endpoint returns valid Prometheus format
- `Database::start_listener()` serves TCP clients on the same engine
- Each shard has its own WAL file; BGSAVE runs per-shard in parallel
