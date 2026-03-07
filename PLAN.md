# Kōra — Implementation Plan

> Detailed, actionable engineering roadmap broken into phases, milestones, and concrete deliverables.

---

## Phase 0 — Project Scaffolding (Week 0)

**Goal:** Set up the Cargo workspace, CI, and all crate skeletons so development can begin in parallel across crates.

### Deliverables

1. **Workspace root `Cargo.toml`**
   - Define all workspace members
   - Shared dependency versions via `[workspace.dependencies]`
   - Shared Rust edition, resolver, and profile settings

2. **Crate skeletons** (each with `Cargo.toml`, `src/lib.rs` or `src/main.rs`):
   ```
   kora-core/        → src/lib.rs
   kora-protocol/    → src/lib.rs
   kora-server/      → src/lib.rs
   kora-embedded/    → src/lib.rs
   kora-storage/     → src/lib.rs
   kora-vector/      → src/lib.rs
   kora-cdc/         → src/lib.rs
   kora-scripting/   → src/lib.rs
   kora-observability/ → src/lib.rs
   kora-cli/         → src/main.rs
   ```

3. **Tooling configuration:**
   - `rustfmt.toml` — formatting rules
   - `clippy.toml` — lint configuration
   - `.github/workflows/ci.yml` — build, test, clippy, fmt check
   - `.gitignore` — target/, *.swp, etc.

4. **Dependency wiring** — Set inter-crate dependencies matching the dependency graph:
   ```toml
   # kora-core has zero workspace deps
   # kora-protocol depends on kora-core
   # kora-server depends on kora-core, kora-protocol, kora-storage, etc.
   # kora-embedded depends on kora-core, kora-storage, etc.
   # kora-cli depends on kora-server
   ```

### Definition of Done
- `cargo build --workspace` succeeds
- `cargo test --workspace` succeeds (empty tests)
- `cargo clippy --workspace` passes with no warnings
- `cargo fmt --all --check` passes
- CI pipeline runs green

---

## Phase 1 — Core Engine (Weeks 1–6)

The foundation everything else builds on. Implements the shared-nothing threading model, memory management, data structures, RESP protocol, TCP server, and basic Redis commands.

### 1.1 — kora-core: Data Structures & Memory (Weeks 1–2)

**Implementation order:**

1. **`kora-core/src/types/value.rs`** — The `Value` enum
   ```rust
   pub enum Value {
       InlineStr([u8; 23], u8),
       HeapStr(Arc<[u8]>),
       Int(i64),
       List(VecDeque<Value>),
       Set(HashSet<Value>),
       SortedSet(/* SkipList placeholder */),
       Hash(HashMap<CompactKey, Value>),
       Vector(Box<[f32]>),
       Stream(StreamLog),
   }
   ```
   - Implement `PartialEq`, `Eq`, `Hash`, `Clone` for `Value`
   - Implement conversions: `From<&str>`, `From<i64>`, `From<Vec<u8>>`

2. **`kora-core/src/types/key.rs`** — The `CompactKey` type
   ```rust
   pub enum CompactKey {
       Inline([u8; 64], u8),   // key ≤ 64 bytes, stored inline
       Heap(Arc<[u8]>),        // larger keys on heap
   }
   ```
   - Implement `Hash`, `Eq`, `Ord`, `AsRef<[u8]>`

3. **`kora-core/src/types/entry.rs`** — The `KeyEntry` struct
   ```rust
   pub struct KeyEntry {
       pub key: CompactKey,
       pub value: Value,
       pub ttl: Option<Instant>,
       pub lfu_counter: u8,
       pub last_access: u32,
       pub flags: u8,
   }
   ```
   - LFU counter increment logic (probabilistic logarithmic, Redis-compatible)
   - TTL check method: `is_expired(&self) -> bool`

4. **`kora-core/src/alloc/slab.rs`** — Per-thread slab allocator
   ```rust
   pub struct ShardAllocator {
       slabs: [SlabPool; 7], // 64, 128, 256, 512, 1024, 2048, 4096
       large_threshold: usize,
   }
   ```
   - `allocate(size: usize) -> *mut u8`
   - `deallocate(ptr: *mut u8, size: usize)`
   - Track allocation stats per size class

5. **`kora-core/src/shard/store.rs`** — The shard store (per-thread hashmap)
   ```rust
   pub struct ShardStore {
       entries: HashMap<CompactKey, KeyEntry>,
       allocator: ShardAllocator,
       key_count: usize,
       memory_used: usize,
   }
   ```
   - `get(&self, key: &[u8]) -> Option<&Value>`
   - `set(&mut self, key: CompactKey, value: Value, ttl: Option<Duration>)`
   - `del(&mut self, key: &[u8]) -> bool`
   - `expire(&mut self, key: &[u8], ttl: Duration) -> bool`
   - `ttl(&self, key: &[u8]) -> Option<Duration>`
   - `scan(&self, cursor: u64, pattern: &str, count: usize) -> (u64, Vec<&CompactKey>)`
   - Background expiry sweep

6. **`kora-core/src/types/mod.rs`** — Re-export all types

**Module structure:**
```
kora-core/src/
├── lib.rs
├── types/
│   ├── mod.rs
│   ├── value.rs
│   ├── key.rs
│   └── entry.rs
├── alloc/
│   ├── mod.rs
│   └── slab.rs
└── shard/
    ├── mod.rs
    └── store.rs
```

**Tests:**
- Unit tests for `Value` conversions and equality
- Unit tests for `CompactKey` inline vs. heap threshold
- Unit tests for `ShardStore` CRUD operations
- Unit tests for TTL expiry logic
- Unit tests for LFU counter behavior
- Slab allocator allocation/deallocation correctness

### 1.2 — kora-core: Threading Model (Weeks 2–3)

1. **`kora-core/src/shard/engine.rs`** — The `ShardEngine` coordinator
   ```rust
   pub struct ShardEngine {
       shard_count: usize,
       senders: Vec<mpsc::Sender<ShardCommand>>,
       // Each worker thread owns a ShardStore
   }
   ```
   - `new(shard_count: usize) -> Self` — spawn N worker threads
   - `route_key(key: &[u8]) -> usize` — `hash(key) % shard_count`
   - `send_command(shard: usize, cmd: ShardCommand) -> oneshot::Receiver<ShardResponse>`

2. **`kora-core/src/shard/command.rs`** — Command/response types
   ```rust
   pub enum ShardCommand {
       Get { key: Vec<u8>, reply: oneshot::Sender<ShardResponse> },
       Set { key: Vec<u8>, value: Vec<u8>, ttl: Option<Duration>, reply: oneshot::Sender<ShardResponse> },
       Del { key: Vec<u8>, reply: oneshot::Sender<ShardResponse> },
       MGet { keys: Vec<Vec<u8>>, reply: oneshot::Sender<ShardResponse> },
       Expire { key: Vec<u8>, ttl: Duration, reply: oneshot::Sender<ShardResponse> },
       Ttl { key: Vec<u8>, reply: oneshot::Sender<ShardResponse> },
       Keys { pattern: String, reply: oneshot::Sender<ShardResponse> },
       Scan { cursor: u64, pattern: String, count: usize, reply: oneshot::Sender<ShardResponse> },
       Shutdown,
   }

   pub enum ShardResponse {
       Value(Option<Value>),
       Values(Vec<Option<Value>>),
       Ok,
       Bool(bool),
       Int(i64),
       Keys(Vec<CompactKey>),
       ScanResult(u64, Vec<CompactKey>),
       Error(String),
   }
   ```

3. **`kora-core/src/shard/worker.rs`** — Worker thread loop
   ```rust
   pub struct ShardWorker {
       id: usize,
       store: ShardStore,
       receiver: mpsc::Receiver<ShardCommand>,
   }
   ```
   - `run(&mut self)` — event loop: receive command → execute → send reply
   - Periodic TTL sweep in the event loop

**Tests:**
- Single-shard command roundtrip (send Get/Set/Del, verify responses)
- Multi-shard routing correctness (keys hash to expected shards)
- Concurrent access from multiple sender threads
- Shutdown behavior

### 1.3 — kora-protocol: RESP Parser (Weeks 2–3)

**Implementation order:**

1. **`kora-protocol/src/resp.rs`** — RESP2/RESP3 data types
   ```rust
   pub enum RespValue {
       SimpleString(String),
       Error(String),
       Integer(i64),
       BulkString(Option<Vec<u8>>),
       Array(Option<Vec<RespValue>>),
       // RESP3 extensions
       Null,
       Boolean(bool),
       Double(f64),
       BigNumber(String),
       Map(Vec<(RespValue, RespValue)>),
       Set(Vec<RespValue>),
       VerbatimString(String, Vec<u8>),
       Push(Vec<RespValue>),
   }
   ```

2. **`kora-protocol/src/parser.rs`** — Streaming RESP parser
   ```rust
   pub struct RespParser {
       buffer: BytesMut,
   }
   impl RespParser {
       pub fn feed(&mut self, data: &[u8]);
       pub fn try_parse(&mut self) -> Result<Option<RespValue>, RespError>;
   }
   ```
   - Handle partial reads (streaming/incremental parsing)
   - Inline command parsing (e.g., `PING\r\n`)

3. **`kora-protocol/src/serializer.rs`** — RESP serializer
   ```rust
   pub fn serialize(value: &RespValue, buf: &mut Vec<u8>);
   ```

4. **`kora-protocol/src/command.rs`** — Command parsing from `RespValue`
   ```rust
   pub struct RedisCommand {
       pub name: String,       // uppercase: "GET", "SET", etc.
       pub args: Vec<Vec<u8>>, // raw byte arguments
   }
   impl RedisCommand {
       pub fn from_resp(value: RespValue) -> Result<Self, RespError>;
   }
   ```

**Module structure:**
```
kora-protocol/src/
├── lib.rs
├── resp.rs
├── parser.rs
├── serializer.rs
├── command.rs
└── error.rs
```

**Tests:**
- Parse every RESP2 type (simple string, error, integer, bulk string, array)
- Parse RESP3 extensions (null, boolean, double, map, set)
- Incremental parsing (feed data in chunks)
- Serialize → parse roundtrip for every type
- Edge cases: empty bulk string, null bulk string, nested arrays, very large payloads
- Command extraction from parsed RESP arrays

### 1.4 — kora-server: TCP Server & Command Dispatch (Weeks 3–5)

1. **`kora-server/src/listener.rs`** — TCP acceptor
   ```rust
   pub struct Server {
       engine: Arc<ShardEngine>,
       addr: SocketAddr,
   }
   impl Server {
       pub async fn run(&self) -> io::Result<()>;
   }
   ```
   - Accept TCP connections, assign to workers round-robin
   - Optional Unix socket support

2. **`kora-server/src/connection.rs`** — Per-connection handler
   ```rust
   pub struct Connection {
       stream: TcpStream,
       parser: RespParser,
       engine: Arc<ShardEngine>,
   }
   impl Connection {
       pub async fn handle(&mut self) -> io::Result<()>;
   }
   ```
   - Read loop: parse RESP → dispatch command → write response
   - Pipeline support: process multiple commands per read

3. **`kora-server/src/dispatch.rs`** — Command router
   ```rust
   pub async fn dispatch(
       engine: &ShardEngine,
       cmd: RedisCommand,
   ) -> RespValue;
   ```
   - Match command name → handler function
   - Single-key commands: route to owning shard
   - Multi-key commands (MGET/MSET): fan out to shards, collect, merge
   - Unknown command: return error

4. **`kora-server/src/commands/`** — Individual command handlers
   ```
   commands/
   ├── mod.rs
   ├── string.rs    # GET, SET, MGET, MSET, APPEND, INCR, DECR, SETNX, GETSET
   ├── key.rs       # DEL, EXISTS, EXPIRE, TTL, PTTL, PERSIST, KEYS, SCAN, TYPE, RENAME
   ├── server.rs    # PING, ECHO, INFO, DBSIZE, FLUSHDB, FLUSHALL, COMMAND, CONFIG
   └── connection.rs # AUTH, SELECT, QUIT, CLIENT
   ```

**Tests:**
- Integration tests using actual TCP connections with `redis-cli` or a Rust Redis client
- Command correctness: verify each command matches Redis behavior
- Pipeline support: send multiple commands in one write
- Connection lifecycle: connect, auth, execute, quit
- Error responses for invalid commands/arguments

### 1.5 — kora-embedded: Library API (Weeks 4–5)

1. **`kora-embedded/src/lib.rs`** — Public API
   ```rust
   pub struct Database {
       engine: ShardEngine,
   }

   pub struct Config {
       pub shard_count: usize,
       pub max_memory: usize,
       // ...
   }

   impl Database {
       pub fn open(config: Config) -> Result<Self>;
       pub fn set(&self, key: &str, value: &[u8]) -> Result<()>;
       pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
       pub fn del(&self, key: &str) -> Result<bool>;
       pub fn hset(&self, key: &str, field: &str, value: &[u8]) -> Result<()>;
       pub fn hget(&self, key: &str, field: &str) -> Result<Option<Vec<u8>>>;
       pub fn expire(&self, key: &str, ttl: Duration) -> Result<bool>;
       pub fn ttl(&self, key: &str) -> Result<Option<Duration>>;
       pub fn keys(&self, pattern: &str) -> Result<Vec<String>>;
       // Hybrid mode
       pub fn start_listener(&self, addr: &str) -> Result<()>;
   }
   ```

**Tests:**
- Basic CRUD through embedded API
- Multi-threaded concurrent access
- Hybrid mode: embedded + TCP listener simultaneously
- Config validation

### 1.6 — kora-cli: Binary Entry Point (Week 5–6)

1. **`kora-cli/src/main.rs`** — CLI binary
   ```rust
   fn main() {
       // Parse CLI args / config file
       // Build Config
       // Create ShardEngine
       // Start TCP server
       // Block on signal (SIGINT/SIGTERM)
       // Graceful shutdown
   }
   ```

2. **`kora-cli/src/config.rs`** — Config file parsing
   - Support TOML config file
   - CLI argument overrides
   - Sensible defaults (port 6379, shard_count = num_cpus)

**Tests:**
- Config parsing from file and CLI args
- Server starts and responds to PING
- Graceful shutdown on signal

### Phase 1 — Definition of Done
- [ ] `redis-benchmark` runs successfully against Kōra
- [ ] Core Redis commands work: GET, SET, DEL, MGET, MSET, EXPIRE, TTL, PTTL, PERSIST, KEYS, SCAN, EXISTS, TYPE, RENAME, PING, ECHO, INFO, DBSIZE, FLUSHDB
- [ ] Embedded mode API works for basic CRUD
- [ ] Multi-threaded: demonstrates throughput scaling with core count
- [ ] All tests pass, clippy clean, formatted

---

## Phase 2 — Storage & Persistence (Weeks 7–10)

### 2.1 — Write-Ahead Log (Weeks 7–8)

**`kora-storage/src/wal.rs`**
```rust
pub struct WriteAheadLog {
    file: File,
    sync_policy: SyncPolicy, // EveryWrite, EverySecond, OsManaged
}

pub enum WalEntry {
    Set { key: Vec<u8>, value: Vec<u8>, ttl: Option<u64> },
    Del { key: Vec<u8> },
    Expire { key: Vec<u8>, ttl: u64 },
    // ... all mutation commands
}

impl WriteAheadLog {
    pub fn append(&mut self, entry: &WalEntry) -> io::Result<()>;
    pub fn replay<F: FnMut(WalEntry)>(&self, callback: F) -> io::Result<()>;
    pub fn rotate(&mut self) -> io::Result<()>;
    pub fn sync(&self) -> io::Result<()>;
}
```

**Tests:**
- Write entries, replay, verify correctness
- Crash simulation: truncated writes, partial entries
- Rotation and cleanup

### 2.2 — RDB Snapshots (Weeks 7–8)

**`kora-storage/src/rdb.rs`**
```rust
pub struct RdbWriter { /* ... */ }
pub struct RdbReader { /* ... */ }

impl RdbWriter {
    pub fn save(path: &Path, shards: &[ShardStore]) -> io::Result<()>;
}
impl RdbReader {
    pub fn load(path: &Path) -> io::Result<Vec<(CompactKey, KeyEntry)>>;
}
```

- Compatible with Redis RDB format (read existing Redis dumps)
- Background save (fork-like snapshot using COW or checkpoint)

**Tests:**
- Save and reload roundtrip
- Load actual Redis RDB files
- Concurrent reads during background save

### 2.3 — Tiered Storage Backend (Weeks 8–10)

1. **`kora-storage/src/backend.rs`** — Storage trait
   ```rust
   #[async_trait]
   pub trait StorageBackend: Send + 'static {
       async fn read(&self, key_hash: u64) -> io::Result<Option<Vec<u8>>>;
       async fn write(&self, key_hash: u64, value: &[u8]) -> io::Result<()>;
       async fn delete(&self, key_hash: u64) -> io::Result<()>;
       async fn sync(&self) -> io::Result<()>;
   }
   ```

2. **`kora-storage/src/io_uring_backend.rs`** — io_uring implementation (Linux)
   ```rust
   pub struct IoUringBackend {
       ring: io_uring::IoUring,
       data_dir: PathBuf,
   }
   ```

3. **`kora-storage/src/mmap_backend.rs`** — mmap fallback (cross-platform)

4. **`kora-storage/src/tiering.rs`** — Tier manager
   ```rust
   pub struct TierManager {
       hot_threshold: u8,      // LFU counter threshold for warm→hot promotion
       warm_threshold: u8,     // LFU counter threshold for hot→warm demotion
       cold_threshold: u8,     // threshold for warm→cold demotion
       backend: Box<dyn StorageBackend>,
   }
   impl TierManager {
       pub async fn maybe_demote(&self, entry: &mut KeyEntry) -> io::Result<()>;
       pub async fn promote(&self, entry: &mut KeyEntry) -> io::Result<()>;
       pub fn scan_for_demotion(&self, store: &mut ShardStore);
   }
   ```

5. **`kora-storage/src/compressor.rs`** — LZ4 compression for cold tier

**Module structure:**
```
kora-storage/src/
├── lib.rs
├── backend.rs
├── io_uring_backend.rs
├── mmap_backend.rs
├── tiering.rs
├── compressor.rs
├── wal.rs
└── rdb.rs
```

**Tests:**
- StorageBackend trait tests (generic over implementations)
- io_uring read/write correctness
- Tier migration: hot → warm → cold → promoted back to hot
- Memory pressure eviction
- LZ4 compress/decompress roundtrip
- Integration: server restart with WAL replay + RDB load

### Phase 2 — Definition of Done
- [ ] Server survives restart — data recovered from WAL + RDB
- [ ] Can load Redis RDB dump files
- [ ] Tiered storage works: cold keys spill to disk, hot keys stay in RAM
- [ ] Memory pressure triggers eviction correctly
- [ ] io_uring backend functional on Linux; mmap fallback works elsewhere
- [ ] Background save doesn't block command processing

---

## Phase 3 — Advanced Features (Weeks 11–16)

### 3.1 — Vector Index (Weeks 11–13)

**`kora-vector/src/`**

1. **`hnsw.rs`** — HNSW graph implementation
   ```rust
   pub struct HnswIndex {
       layers: Vec<HnswLayer>,
       dim: usize,
       metric: DistanceMetric,
       ef_construction: usize,
       m: usize,
       quantizer: Option<ProductQuantizer>,
   }

   pub enum DistanceMetric { Cosine, L2, InnerProduct }

   impl HnswIndex {
       pub fn new(dim: usize, metric: DistanceMetric, m: usize, ef_construction: usize) -> Self;
       pub fn insert(&mut self, id: u64, vector: &[f32]);
       pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Vec<(u64, f32)>;
       pub fn delete(&mut self, id: u64);
   }
   ```

2. **`quantizer.rs`** — Product quantization for memory efficiency

3. **`commands.rs`** — Redis-compatible vector commands
   - `VSIM.SET key dim [METRIC cosine|l2|ip]`
   - `VSIM.ADD key id vector_f32...`
   - `VSIM.SEARCH key vector_f32... K [EF search_ef]`
   - `VSIM.DEL key id`

**Tests:**
- Insert vectors, search nearest neighbors, verify recall
- Delete and re-search
- Multi-shard search merge correctness
- Quantized vs. exact search recall comparison
- Edge cases: zero vectors, single vector, duplicate IDs

### 3.2 — CDC — Change Data Capture (Weeks 12–14)

**`kora-cdc/src/`**

1. **`ring.rs`** — Per-shard ring buffer
   ```rust
   pub struct CdcRing {
       buffer: Box<[CdcEvent]>,
       capacity: usize,
       write_head: u64,
   }

   pub struct CdcEvent {
       pub seq: u64,
       pub timestamp: u64,
       pub op: CdcOp,
       pub key: CompactKey,
       pub value: Option<Value>,
   }

   pub enum CdcOp { Set, Del, Expire, HSet, LPush, SAdd, /* ... */ }
   ```

2. **`subscription.rs`** — Subscription manager
   ```rust
   pub struct SubscriptionManager {
       subscriptions: Vec<Subscription>,
   }
   pub struct Subscription {
       pattern: String,
       cursor: u64,
       consumer_group: Option<ConsumerGroup>,
   }
   ```

3. **`commands.rs`** — CDC commands
   - `SUBSCRIBE.CDC pattern [CURSOR seq] [GROUP name CONSUMER id]`

**Tests:**
- Write mutations, read from ring buffer
- Ring buffer wrapping (overwrite oldest entries)
- Pattern matching on subscriptions
- Consumer group acknowledgment tracking
- Gap detection when consumer falls behind

### 3.3 — WASM Scripting (Weeks 13–15)

**`kora-scripting/src/`**

1. **`runtime.rs`** — WASM runtime wrapper
   ```rust
   pub struct WasmRuntime {
       engine: wasmtime::Engine,
       module_cache: HashMap<[u8; 32], wasmtime::Module>,
       max_fuel: u64,
       max_memory: usize,
   }
   ```

2. **`host_functions.rs`** — Host functions exposed to WASM
   - `kora_get`, `kora_set`, `kora_del`, `kora_hget`, `kora_hset`

3. **`commands.rs`** — Scripting commands
   - `FUNCTION.LOAD name path`
   - `FUNCTION.CALL name args...`
   - `EVAL script numkeys keys... args...` (Lua compat layer)

**Tests:**
- Load and execute simple WASM module
- Host function callbacks work correctly
- Fuel metering: infinite loop terminates
- Memory limits enforced
- Module caching works

### 3.4 — Multi-tenancy (Week 14–15)

**Additions to `kora-core/src/`**

1. **`tenant.rs`** — Tenant configuration and state
   ```rust
   pub struct TenantConfig {
       pub id: TenantId,
       pub max_memory: usize,
       pub max_keys: Option<u64>,
       pub ops_per_second: Option<u32>,
       pub blocked_commands: Vec<String>,
   }

   pub struct TenantState {
       pub config: TenantConfig,
       pub memory_used: AtomicUsize,
       pub key_count: AtomicU64,
       pub rate_limiter: TokenBucket,
   }
   ```

2. **Integration with `ShardEngine`** — tenant-scoped key namespacing, per-tenant memory enforcement, rate limiting

**Tests:**
- Tenant isolation: tenant A can't see tenant B's keys
- Memory quota enforcement
- Rate limiting
- Blocked commands per tenant
- AUTH with tenant ID

### 3.5 — Observability (Weeks 15–16)

**`kora-observability/src/`**

1. **`stats.rs`** — Per-shard statistics
   ```rust
   pub struct ShardStats {
       pub cmd_counts: [AtomicU64; NUM_COMMANDS],
       pub cmd_durations_ns: [AtomicU64; NUM_COMMANDS],
       pub hot_key_sketch: CountMinSketch,
       pub memory_by_prefix: PrefixTrie<AtomicUsize>,
       pub tier_sizes: [AtomicUsize; 3],
       pub latency_hist: HdrHistogram,
   }
   ```

2. **`count_min_sketch.rs`** — Hot key detection (~4KB memory)

3. **`commands.rs`** — Stats commands
   - `STATS.HOTKEYS count`
   - `STATS.MEMORY prefix...`
   - `STATS.LATENCY command percentiles...`
   - `STATS.TENANT tenant_id`

4. **`prometheus.rs`** — Prometheus `/metrics` endpoint

**Tests:**
- Count-Min Sketch accuracy for top-K detection
- Latency histogram correctness
- Memory attribution by prefix
- Prometheus output format validation

### Phase 3 — Definition of Done
- [ ] Vector search works: insert, search, delete across shards with good recall
- [ ] CDC subscriptions deliver mutations in real-time
- [ ] WASM functions can be loaded and called with fuel/memory limits
- [ ] Multi-tenant isolation works for memory, keys, rate limiting
- [ ] Observability commands return accurate statistics
- [ ] Prometheus endpoint serves valid metrics

---

## Phase 4 — Production Hardening (Weeks 17–20)

### 4.1 — Simulation Testing (Weeks 17–18)

- Deterministic simulation framework (inspired by FoundationDB/Turso)
- Inject controlled failures: network delays, disk errors, OOM
- Verify invariants hold under all failure scenarios
- Reproducible test seeds for debugging

### 4.2 — Fuzz Testing (Weeks 17–18)

- `cargo-fuzz` targets:
  - `fuzz_resp_parser` — random bytes → parser doesn't crash
  - `fuzz_rdb_reader` — random bytes → reader doesn't crash
  - `fuzz_command_dispatch` — random RESP commands → server doesn't crash
  - `fuzz_hnsw_operations` — random insert/delete/search sequences

### 4.3 — Benchmark Suite (Weeks 18–19)

- `kora-bench/` crate or `benches/` directories
- Benchmarks:
  - Single-key GET/SET throughput vs. core count
  - Multi-key MGET/MSET fan-out overhead
  - RESP parsing throughput (bytes/sec)
  - HNSW search latency vs. dataset size
  - Tiered storage: hot vs. warm read latency
- Compare against Redis, Dragonfly, KeyDB using `redis-benchmark`

### 4.4 — Redis Compatibility Verification (Weeks 19–20)

- Run Redis test suite against Kōra where applicable
- `redis-cli` interactive compatibility
- Document any intentional deviations from Redis behavior
- Verify all supported commands match Redis semantics exactly

### 4.5 — Documentation (Week 20)

- API reference (generated from doc comments via `cargo doc`)
- Configuration reference
- Migration guide from Redis
- Performance tuning guide
- Embedded mode quickstart

### Phase 4 — Definition of Done
- [ ] Simulation tests pass with injected failures
- [ ] Fuzz tests run 10M+ iterations with no crashes
- [ ] Benchmarks show linear throughput scaling with cores
- [ ] redis-cli works for all supported commands
- [ ] Documentation published

---

## Dependency Installation Checklist

```toml
# Expected key workspace dependencies
[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
bytes = "1"
crossbeam-channel = "0.5"
hashbrown = "0.14"
parking_lot = "0.12"
thiserror = "2"
anyhow = "1"
tracing = "0.1"
tracing-subscriber = "0.3"
serde = { version = "1", features = ["derive"] }
toml = "0.8"
io-uring = "0.7"           # Linux only, feature-gated
wasmtime = "27"
lz4_flex = "0.11"
hdrhistogram = "7"
rand = "0.8"
```

---

## Critical Path

The critical path through the phases is:

```
Phase 0: Scaffolding
    ↓
Phase 1.1: kora-core types + slab allocator
    ↓
Phase 1.2: kora-core threading (ShardEngine)
    ↓ (parallel with 1.3)
Phase 1.3: kora-protocol RESP parser ←──────┐
    ↓                                        │
Phase 1.4: kora-server (needs 1.2 + 1.3) ───┘
    ↓ (parallel with 1.5)
Phase 1.5: kora-embedded
    ↓
Phase 1.6: kora-cli
    ↓
Phase 2: Storage (builds on working engine)
    ↓
Phase 3: Advanced features (independent, parallelizable)
    ↓
Phase 4: Hardening
```

Phases 1.2 and 1.3 can proceed in parallel. Phase 3 sub-tasks (vector, CDC, WASM, tenancy, observability) are largely independent and can be worked on concurrently.

---

*This plan should be updated as implementation progresses and real-world constraints emerge.*
