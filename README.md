# Kōra — Architecture

> A multi-threaded, embeddable, memory-safe cache engine in Rust.
> Redis protocol compatible. Built for what comes next.

*Kōra (कोर) — Sanskrit for “core, essence.” Also echoes “core” in English and “kɔra” in Twi.*

-----

## Why this exists

Redis is single-threaded by design. Every scaling strategy — clustering, sharding, multiple instances — exists to work around this. Dragonfly proved in C++ that a multi-threaded, Redis-compatible engine can achieve 25x throughput. Kōra takes that further: multi-threaded, embeddable, memory-safe, with tiered storage and native vector search.

**What we keep from Redis:** RESP protocol, command surface, simplicity, speed.

**What we fix:** single-thread ceiling, RAM-only constraint, no embedded mode, no native vectors, no change streaming, Lua-only scripting.

-----

## High-level overview

```
┌──────────────────────────────────────────────────────────┐
│                      Client Layer                        │
│         RESP3 Protocol  /  Embedded API (direct)         │
└──────────────┬──────────────────────┬────────────────────┘
               │ TCP/Unix socket      │ fn call (in-process)
               ▼                      ▼
┌──────────────────────────────────────────────────────────┐
│                    Router / Dispatcher                    │
│            hash(key) % N → worker thread                 │
└──────┬────────┬────────┬────────┬────────┬───────────────┘
       ▼        ▼        ▼        ▼        ▼
   ┌────────┬────────┬────────┬────────┬────────┐
   │Worker 0│Worker 1│Worker 2│Worker 3│Worker N│
   │        │        │        │        │        │
   │ Shard  │ Shard  │ Shard  │ Shard  │ Shard  │
   │ Store  │ Store  │ Store  │ Store  │ Store  │
   │        │        │        │        │        │
   │ Vector │ Vector │ Vector │ Vector │ Vector │
   │ Index  │ Index  │ Index  │ Index  │ Index  │
   │        │        │        │        │        │
   │ CDC    │ CDC    │ CDC    │ CDC    │ CDC    │
   │ Ring   │ Ring   │ Ring   │ Ring   │ Ring   │
   └───┬────┴───┬────┴───┬────┴───┬────┴───┬────┘
       │        │        │        │        │
       ▼        ▼        ▼        ▼        ▼
┌──────────────────────────────────────────────────────────┐
│                    Tiered Storage                         │
│         Hot (RAM) → Warm (mmap NVMe) → Cold (disk)       │
│                    io_uring backend                       │
└──────────────────────────────────────────────────────────┘
```

-----

## Crate structure

```
kora/
├── Cargo.toml                  # workspace root
├── kora-core/                  # data structures, shard engine, memory management
├── kora-protocol/              # RESP2/RESP3 parser and serializer
├── kora-server/                # TCP/Unix listener, connection handling, command dispatch
├── kora-embedded/              # library mode — direct API, no network
├── kora-storage/               # tiered storage, NVMe spill, io_uring async I/O
├── kora-vector/                # HNSW index, similarity search, quantization
├── kora-cdc/                   # change data capture, stream subscriptions
├── kora-scripting/             # WASM runtime (wasmtime), function registry
├── kora-observability/         # per-key stats, hot-key detection, memory attribution
└── kora-cli/                   # CLI binary, config parsing, server entrypoint
```

**Dependency flow (strict — no cycles):**

```
cli → server → core
              → protocol
              → storage
              → vector
              → cdc
              → scripting
              → observability

embedded → core
         → storage
         → vector
         → cdc
         → observability
```

`kora-core` depends on nothing else in the workspace. Everything flows downward.

-----

## Threading model: shared-nothing

The key architectural decision. Borrowed from Seastar/ScyllaDB/Dragonfly.

**Each worker thread owns a shard of the keyspace.** No locks on the data path.

```
Thread 0 owns keys where hash(key) % N == 0
Thread 1 owns keys where hash(key) % N == 1
...
Thread N owns keys where hash(key) % N == N
```

**How a command executes:**

1. Acceptor thread receives TCP connection, assigns it to a worker via round-robin.
1. Worker reads RESP command from its assigned connections.
1. Single-key commands (GET, SET, DEL): execute on the owning shard directly. If the key hashes to a different worker, forward via a lock-free MPSC channel.
1. Multi-key commands (MGET, MSET): fan out to all relevant workers, collect responses, assemble reply.
1. Response written back on the connection’s assigned worker.

**Why shared-nothing over shared-state with locks:**

- No mutex contention on the hot path. Ever.
- Each shard’s data structures are thread-local, so no atomic operations for reads.
- Memory allocation is per-thread (slab allocator), no global allocator lock.
- Cache-line friendly — each thread works on its own memory region.
- Scales linearly with cores. Dragonfly demonstrated this empirically.

**Cross-shard cost:** Message passing has latency (~100-200ns per hop). For single-key operations (the vast majority), there’s zero cross-shard traffic. Multi-key operations pay the fan-out cost, but this is bounded by the number of shards involved, not total shard count.

-----

## Memory architecture

### Per-thread slab allocator

Each worker thread gets its own slab allocator. No global `malloc` contention.

```rust
struct ShardAllocator {
    // Size classes: 64, 128, 256, 512, 1024, 2048, 4096 bytes
    slabs: [SlabPool; 7],
    // Large allocations fall through to system allocator
    large_threshold: usize, // 4096
}
```

**Small string optimization:** Keys under 64 bytes are stored inline in the hash table entry, avoiding a pointer chase. This covers the vast majority of real-world Redis keys.

### Value representation

```rust
enum Value {
    // Inline small strings (≤ 23 bytes stored in-place, no heap alloc)
    InlineStr([u8; 23], u8),  // data + length

    // Heap string with refcount for zero-copy reads
    HeapStr(Arc<[u8]>),

    // Integer (stored as i64, not serialized string)
    Int(i64),

    // Collections — each thread-local, no sync needed
    List(VecDeque<Value>),
    Set(HashSet<Value>),
    SortedSet(SkipList<f64, Value>),
    Hash(HashMap<Value, Value>),

    // New types
    Vector(Box<[f32]>),    // dense embedding
    Stream(StreamLog),      // append-only log entries
}
```

### Key metadata

```rust
struct KeyEntry {
    key: CompactKey,          // inline if ≤ 64 bytes, heap otherwise
    value: Value,
    ttl: Option<Instant>,     // expiry
    lfu_counter: u8,          // access frequency for eviction / tiering
    last_access: u32,         // seconds since shard epoch (compact timestamp)
    flags: u8,                // encoding hints, dirty bit, tier marker
}
```

The `lfu_counter` uses Redis’s probabilistic LFU algorithm (logarithmic counter with decay) — 1 byte tracks access frequency well enough for eviction and tier migration decisions.

-----

## Tiered storage (kora-storage)

Three tiers. Data migrates automatically based on access frequency.

```
┌─────────────────────────┐
│      Hot Tier (RAM)      │  All keys start here.
│   Thread-local hashmap   │  Full speed, no I/O.
└──────────┬──────────────┘
           │ lfu_counter drops below warm_threshold
           ▼
┌─────────────────────────┐
│   Warm Tier (mmap NVMe)  │  Values evicted from RAM.
│   Key stays in RAM index │  Read = page fault or io_uring prefetch.
│   Value on NVMe via mmap │  Transparent to command execution.
└──────────┬──────────────┘
           │ lfu_counter drops below cold_threshold
           ▼
┌─────────────────────────┐
│  Cold Tier (compressed)  │  LZ4-compressed on disk.
│  Key in bloom filter     │  Read requires decompress + promote.
│  Full eviction candidate │  Background compaction.
└─────────────────────────┘
```

**Migration is async.** A background fiber per worker scans key entries periodically and issues io_uring write commands for keys that should demote. Promotion on read is synchronous (the read blocks until the value is loaded) but uses io_uring’s `IORING_OP_READ` to avoid blocking the kernel thread.

```rust
trait StorageBackend: Send + 'static {
    async fn read(&self, key_hash: u64) -> io::Result<Option<Vec<u8>>>;
    async fn write(&self, key_hash: u64, value: &[u8]) -> io::Result<()>;
    async fn delete(&self, key_hash: u64) -> io::Result<()>;
    async fn sync(&self) -> io::Result<()>;
}

struct IoUringBackend {
    ring: io_uring::IoUring,
    data_dir: PathBuf,
    // Shard-local file — one file per worker thread, no contention
}
```

**Why this matters:** A Redis instance with 100GB of data currently needs 100GB of RAM. With tiered storage, you might keep 20GB hot in RAM and 80GB on a $0.08/GB NVMe drive instead of $5/GB RAM. 10x cost reduction for workloads with skewed access patterns (which is most of them).

-----

## Embedded mode (kora-embedded)

The SQLite play. Use Kōra as a library, not a server.

```rust
use kora_embedded::{Database, Config};

let db = Database::open(Config::default())?;

// Direct calls — no serialization, no network, no RESP parsing
db.set("user:1", b"Augustus")?;
let val = db.get("user:1")?;

// Typed API
db.hset("profile:1", "name", "Augustus")?;
db.hset("profile:1", "city", "Accra")?;
let name: Option<String> = db.hget("profile:1", "name")?;

// Vector operations
db.vector_set("emb:1", &[0.1, 0.2, 0.3, 0.8])?;
let neighbors = db.vector_search("emb:*", &query_vec, 10)?;

// Still multi-threaded internally — spawns N worker threads
// Keys are sharded across workers, same as server mode
// Caller thread sends commands via channels, receives via oneshot
```

**Architecture difference from server mode:** The `kora-server` and `kora-protocol` crates are simply not linked. The `kora-embedded` crate provides a `Database` struct that wraps the same `ShardEngine` the server uses, but routes commands through direct channel sends instead of TCP connections.

**Hybrid mode:** You can also start the embedded database AND bind a TCP listener, so your application uses direct calls while external tools (redis-cli, monitoring) connect over the network.

```rust
let db = Database::open(config)?;
db.start_listener("127.0.0.1:6379")?; // optional, non-blocking
db.set("key", b"works from both paths")?;
```

-----

## Vector index (kora-vector)

HNSW (Hierarchical Navigable Small World) graph, sharded per worker like everything else.

```rust
struct HnswIndex {
    layers: Vec<HnswLayer>,
    dim: usize,
    metric: DistanceMetric, // Cosine, L2, InnerProduct
    ef_construction: usize, // build quality parameter
    m: usize,               // max connections per node
    // Quantization for memory efficiency
    quantizer: Option<ProductQuantizer>,
}

// Commands (Redis-compatible extension)
// VSIM.SET key dim [METRIC cosine|l2|ip]
// VSIM.ADD key id vector_f32...
// VSIM.SEARCH key vector_f32... K [EF search_ef]
// VSIM.DEL key id
```

**Why per-shard indexes work:** Vector search is embarrassingly parallel. Each shard searches its local HNSW graph, returns top-K candidates, and the dispatcher merges results. This is how distributed vector databases (Milvus, Qdrant) already work — we get it for free from the shared-nothing architecture.

**Quantization:** Product quantization (PQ) reduces memory per vector from 4 bytes × dim to roughly 1 byte × dim with minimal recall loss. For a 768-dim embedding, that’s 3KB → 768 bytes per vector. At 10M vectors, that’s 30GB → 7.5GB.

-----

## CDC — Change Data Capture (kora-cdc)

Per-shard ring buffer capturing every mutation.

```rust
struct CdcRing {
    buffer: Box<[CdcEvent]>,  // fixed-size ring
    capacity: usize,
    write_head: u64,          // monotonic sequence number
}

struct CdcEvent {
    seq: u64,
    timestamp: u64,
    op: CdcOp,
    key: CompactKey,
    value: Option<Value>,     // None for DEL
}

enum CdcOp { Set, Del, Expire, HSet, LPush, SAdd, /* ... */ }
```

**Subscription model:**

```
# Subscribe to all mutations on keys matching "user:*"
SUBSCRIBE.CDC user:*

# Subscribe with cursor (resume from sequence number)
SUBSCRIBE.CDC user:* CURSOR 184729

# Consumer groups (like Redis Streams but for any key mutation)
SUBSCRIBE.CDC user:* GROUP analytics CONSUMER worker-1
```

**Delivery guarantees:** At-least-once. Each consumer group tracks its acknowledged sequence number. Unacknowledged events are redelivered after a timeout. The ring buffer has finite capacity — if a consumer falls too far behind, it gets a gap notification and must re-sync.

**Use cases this unlocks:**

- Cache invalidation buses (subscribe to changes, propagate to edge caches)
- Event sourcing (treat Kōra as the event log)
- Real-time analytics pipelines (stream mutations to a processor)
- Replication (replicas subscribe to the CDC stream)

-----

## WASM scripting (kora-scripting)

Replace Lua with WASM. Write server-side functions in any language that compiles to WASM.

```rust
struct WasmRuntime {
    engine: wasmtime::Engine,
    // Pre-compiled modules cached by SHA256
    module_cache: HashMap<[u8; 32], wasmtime::Module>,
    // Fuel limit per execution (prevents infinite loops)
    max_fuel: u64,
    // Memory limit per instance
    max_memory: usize,
}
```

**Host functions exposed to WASM:**

```rust
// The WASM module can call back into the shard engine
fn kora_get(key: &str) -> Option<Vec<u8>>;
fn kora_set(key: &str, value: &[u8]) -> Result<()>;
fn kora_del(key: &str) -> Result<bool>;
fn kora_hget(key: &str, field: &str) -> Option<Vec<u8>>;
// ... full command surface
```

**Usage:**

```
# Register a WASM function
FUNCTION.LOAD rate_limiter ./rate_limiter.wasm

# Call it
FUNCTION.CALL rate_limiter user:42 limit=100 window=60

# Lua compatibility layer — existing EVAL scripts work
EVAL "return redis.call('GET', KEYS[1])" 1 mykey
```

**Why WASM over Lua:**

- Fuel metering: hard cap on execution cost, no runaway scripts blocking the server.
- Memory sandboxing: each instance gets a fixed memory region, can’t corrupt server state.
- Language agnostic: write in Rust, Go, TypeScript, Python, AssemblyScript — anything targeting WASM.
- Precompilation: WASM modules are AOT-compiled and cached, faster invocation than Lua parsing.

-----

## Multi-tenancy (kora-core)

First-class tenant isolation in the engine, not a naming convention.

```rust
struct TenantConfig {
    id: TenantId,
    max_memory: usize,           // hard memory quota
    max_keys: Option<u64>,       // optional key count limit
    ops_per_second: Option<u32>, // rate limit (token bucket)
    blocked_commands: Vec<String>,// e.g., block KEYS, FLUSHALL
}

struct ShardEngine {
    tenants: HashMap<TenantId, TenantState>,
    default_tenant: TenantId,
    // ...
}
```

**How it works:** Each connection authenticates with a tenant ID (via AUTH or a connection parameter). All key operations are scoped to the tenant. Memory accounting is per-tenant — when tenant A hits its quota, its writes are rejected while tenant B continues normally.

**Isolation guarantees:**

- Memory: per-tenant accounting, hard limits enforced at write time.
- CPU: per-tenant rate limiting via token bucket. A `KEYS *` from one tenant doesn’t block others because it only scans that tenant’s keyspace.
- Eviction: per-tenant LFU. One tenant’s cold data doesn’t evict another’s hot data.

-----

## Observability (kora-observability)

Always-on, near-zero overhead.

```rust
struct ShardStats {
    // Per-command counters
    cmd_counts: [AtomicU64; NUM_COMMANDS],
    cmd_durations_ns: [AtomicU64; NUM_COMMANDS],

    // Hot key tracking (Count-Min Sketch, ~4KB memory)
    hot_key_sketch: CountMinSketch,

    // Memory attribution
    memory_by_prefix: PrefixTrie<AtomicUsize>, // "user:*" → 2.3GB

    // Per-tier stats
    tier_sizes: [AtomicUsize; 3], // hot, warm, cold

    // Latency histograms (HDR histogram, P50/P99/P999)
    latency_hist: HdrHistogram,
}
```

**Exposed via commands:**

```
# Top-10 hottest keys in the last 60 seconds
STATS.HOTKEYS 10

# Memory breakdown by key prefix pattern
STATS.MEMORY user:* session:* cache:*

# Command latency percentiles
STATS.LATENCY GET P50 P99 P999

# Per-tenant resource usage
STATS.TENANT tenant:42
```

Also exposed as a Prometheus `/metrics` endpoint for standard monitoring stack integration.

-----

## Build phases

### Phase 1 — Core engine (weeks 1-6)

- Shared-nothing threading with shard routing
- Per-thread slab allocator
- Basic data structures: String, List, Set, SortedSet, Hash
- RESP2/RESP3 protocol parsing
- TCP server with connection handling
- Core Redis commands: GET, SET, DEL, MGET, MSET, EXPIRE, TTL, KEYS, SCAN
- Embedded mode API
- `redis-benchmark` compatibility — target: beat single-threaded Redis on multi-core

### Phase 2 — Storage + persistence (weeks 7-10)

- RDB-compatible snapshot save/load (read existing Redis dumps)
- AOF-equivalent write-ahead log
- Tiered storage with io_uring backend
- Background key migration (hot → warm → cold)
- Memory pressure detection and adaptive eviction

### Phase 3 — Advanced features (weeks 11-16)

- Vector index (HNSW) with search commands
- CDC ring buffer and subscription protocol
- WASM scripting runtime
- Multi-tenancy with resource isolation
- Observability subsystem

### Phase 4 — Production hardening (weeks 17-20)

- Deterministic simulation testing (like Turso/FoundationDB)
- Crash recovery verification
- Fuzz testing on RESP parser and storage engine
- Benchmark suite against Redis, Dragonfly, KeyDB
- Documentation and redis-cli compatibility verification

-----

## Key technical risks and mitigations

|Risk                                         |Mitigation                                                                      |
|---------------------------------------------|--------------------------------------------------------------------------------|
|Cross-shard latency for multi-key ops        |Batch messages, amortize channel overhead. Measure early.                       |
|io_uring portability (not available on macOS)|Abstract behind `StorageBackend` trait. Use kqueue/epoll fallback.              |
|HNSW memory consumption at scale             |Product quantization, optional disk-backed index for cold vectors.              |
|RESP protocol edge cases                     |Fuzz test with AFL/cargo-fuzz against Redis’s own test suite output.            |
|Embedded mode API ergonomics                 |Ship a `kora` crate with builder pattern config. Iterate on API with real users.|
|WASM cold start latency                      |AOT compile modules at registration, cache compiled artifacts.                  |

-----

## Name candidates

|Name     |Origin            |Reasoning                                   |
|---------|------------------|--------------------------------------------|
|**Kōra** |Sanskrit कोर (core)|Essence. Also echoes “core” and Twi “kɔra.” |
|**Sika** |Twi (gold)        |Valuable, foundational.                     |
|**Ember**|English           |Persistent heat. Cache that doesn’t go cold.|
|**Bolt** |English           |Speed. Direct.                              |

-----

*This is a living document. Architecture decisions should be validated with benchmarks at each phase boundary.*
