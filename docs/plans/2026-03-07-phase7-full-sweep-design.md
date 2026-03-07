# Phase 7: Full Sweep — Tiered Storage, Slab Allocator, Stream, Simulation Testing

## Overview

Phase 7 closes all remaining gaps between the implementation and README architecture vision. Seven workstreams covering tiered storage (the marquee feature), per-thread memory allocation, the Stream data type, per-tenant eviction isolation, and deterministic simulation testing.

## Workstreams

### WS1: Per-Thread Slab Allocator

**Scope:** Custom allocator per worker thread to eliminate global malloc contention.

**Design:**
```
ShardAllocator {
    slabs: [SlabPool; 7],  // 64, 128, 256, 512, 1024, 2048, 4096 bytes
    large_threshold: 4096, // above this → system allocator
}

SlabPool {
    free_list: Vec<*mut u8>,
    chunk_size: usize,
    chunks: Vec<Box<[u8]>>,  // backing memory
}
```

- Each `ShardStore` owns a `ShardAllocator`
- `alloc(size)` → finds smallest slab class ≥ size, pops from free list (or allocates new chunk)
- `dealloc(ptr, size)` → pushes back to free list
- Values and keys allocated through slab when possible
- Fallback to system allocator for large allocations
- Trait-based: `trait ShardAlloc { fn alloc(&mut self, size: usize) -> *mut u8; fn dealloc(&mut self, ptr: *mut u8, size: usize); }`

**Files:**
- `kora-core/src/shard/allocator.rs` (NEW)
- `kora-core/src/shard/store.rs` — integrate allocator

### WS2: Warm Tier (mmap NVMe)

**Scope:** Memory-mapped file backend for values evicted from RAM but still frequently accessed.

**Design:**
```
WarmTier {
    mmap_file: MmapMut,          // memory-mapped data file
    index: HashMap<u64, WarmEntry>, // key_hash → offset+length
    write_offset: usize,         // append position
    file_path: PathBuf,
    max_size: usize,             // max mmap region size
}

WarmEntry {
    offset: u64,
    length: u32,
    key_hash: u64,
}
```

- Values are serialized and appended to the mmap file
- Key stays in RAM (ShardStore.entries) with a `TierMarker::Warm` flag
- On read: deserialize from mmap region (page fault handles I/O transparently)
- Compaction: rewrite file periodically to reclaim deleted entries
- Uses `memmap2` crate for cross-platform mmap

**Files:**
- `kora-storage/src/warm.rs` (NEW)
- `kora-core/src/types/key.rs` — add tier marker to flags field

### WS3: Automatic Tier Migration

**Scope:** Background task per shard that migrates keys between hot/warm/cold based on LFU counter thresholds.

**Design:**
```
TierConfig {
    warm_threshold: u8,   // LFU counter below this → demote to warm (default: 3)
    cold_threshold: u8,   // LFU counter below this → demote to cold (default: 1)
    scan_interval_ms: u64,// how often to scan (default: 1000)
    scan_batch_size: usize,// keys per scan cycle (default: 100)
}
```

- Each shard worker runs a periodic scan (non-blocking, interleaved with command processing)
- Demotion: hot→warm moves value to mmap file, replaces Value with a `Value::WarmRef(u64)` placeholder
- Demotion: warm→cold moves value to LZ4-compressed cold backend
- Promotion: on read of warm/cold key, load value back to RAM, update tier marker
- Promotion is synchronous (blocks the read until value is loaded)
- Decay LFU counters during scan

**Files:**
- `kora-core/src/shard/migration.rs` (NEW)
- `kora-core/src/shard/store.rs` — integrate migration scan, handle WarmRef/ColdRef on read
- `kora-core/src/types/value.rs` — add WarmRef/ColdRef variants

### WS4: io_uring Backend

**Scope:** Async I/O backend using io_uring on Linux, with sync fallback on other platforms.

**Design:**
```
#[cfg(target_os = "linux")]
struct IoUringBackend {
    ring: io_uring::IoUring,
    data_file: std::fs::File,
    pending: VecDeque<Completion>,
}

#[cfg(not(target_os = "linux"))]
struct SyncFallbackBackend {
    data_file: std::fs::File,
}

// Unified trait
trait AsyncStorageBackend: Send + 'static {
    fn submit_read(&mut self, offset: u64, len: u32) -> IoToken;
    fn submit_write(&mut self, offset: u64, data: &[u8]) -> IoToken;
    fn poll_completions(&mut self) -> Vec<(IoToken, io::Result<usize>)>;
    fn sync(&mut self) -> io::Result<()>;
}
```

- io_uring used for warm tier reads (prefetch) and cold tier writes
- `io-uring` crate (Linux only), feature-gated behind `cfg(target_os = "linux")`
- macOS/other: sync file I/O wrapped in the same trait
- Batch submissions: queue multiple I/O ops, submit once, poll completions

**Files:**
- `kora-storage/src/iouring.rs` (NEW)
- `kora-storage/src/lib.rs` — export new backend
- `Cargo.toml` — add `io-uring` dependency (Linux only)

### WS5: Stream Data Type

**Scope:** Append-only log entries (Redis Streams equivalent).

**Design:**
```
Value::Stream(StreamLog)

StreamLog {
    entries: VecDeque<StreamEntry>,
    last_id: StreamId,
    max_len: Option<usize>,  // MAXLEN trimming
}

StreamEntry {
    id: StreamId,           // timestamp-sequence
    fields: Vec<(Vec<u8>, Vec<u8>)>,
}

StreamId {
    ms: u64,                // millisecond timestamp
    seq: u64,               // sequence within same ms
}
```

**Commands:**
- XADD key [MAXLEN count] * field value [field value ...]
- XLEN key
- XRANGE key start end [COUNT count]
- XREVRANGE key end start [COUNT count]
- XREAD COUNT count STREAMS key [key ...] id [id ...]
- XTRIM key MAXLEN count

**Files:**
- `kora-core/src/types/value.rs` — add Stream variant and StreamLog/StreamEntry/StreamId types
- `kora-core/src/command.rs` — add XAdd, XLen, XRange, XRevRange, XRead, XTrim commands
- `kora-core/src/shard/store.rs` — implement stream command handlers
- `kora-protocol/src/command.rs` — parse stream commands

### WS6: Per-Tenant Eviction Isolation

**Scope:** Eviction scoped to tenant — one tenant's cold data doesn't evict another's hot data.

**Design:**
- Add `tenant_id: TenantId` to `KeyEntry`
- `maybe_evict()` filters sampled keys to only the tenant that triggered memory pressure
- Per-tenant memory accounting: `TenantAccounting.memory_used` is updated on every write/delete
- When a tenant exceeds its quota, only that tenant's keys are candidates for eviction

**Files:**
- `kora-core/src/types/key.rs` — add tenant_id to KeyEntry
- `kora-core/src/shard/store.rs` — tenant-scoped eviction in maybe_evict()
- `kora-core/src/tenant.rs` — update_accounting called from store on mutations

### WS7: Deterministic Simulation Testing

**Scope:** FoundationDB-style deterministic testing framework for catching concurrency bugs.

**Design:**
```
SimulationRuntime {
    rng: StdRng,              // seeded PRNG for reproducibility
    clock: SimClock,          // virtual monotonic clock
    scheduler: SimScheduler,  // deterministic task ordering
    network: SimNetwork,      // simulated message delivery with delays/drops
}

SimClock {
    now: AtomicU64,           // virtual time in nanoseconds
}

SimScheduler {
    ready_queue: BinaryHeap<(u64, TaskId)>,  // (wake_time, task)
    seed: u64,
}
```

- Replaces real time with virtual clock
- Replaces real channels with simulated ones that can introduce delays, reordering, drops
- Seeded PRNG controls all nondeterminism — same seed = same execution
- Test harness: run N operations with random interleavings, verify invariants
- Fault injection: simulate crash mid-WAL-write, network partition between shards

**Files:**
- `kora-core/src/sim/mod.rs` (NEW)
- `kora-core/src/sim/clock.rs` (NEW)
- `kora-core/src/sim/scheduler.rs` (NEW)
- `kora-core/src/sim/network.rs` (NEW)
- `kora-core/tests/simulation.rs` (NEW)

## Dependencies Between Workstreams

```
WS1 (slab allocator) ← independent
WS2 (warm tier) ← needed by WS3
WS3 (tier migration) ← depends on WS2, touches WS4
WS4 (io_uring) ← independent, enhances WS2/WS3
WS5 (stream) ← independent
WS6 (tenant eviction) ← independent
WS7 (sim testing) ← independent, but benefits from all others being done
```

**Execution order:**
1. First batch (parallel): WS1, WS4, WS5, WS6
2. Second batch: WS2 (warm tier)
3. Third batch: WS3 (tier migration, depends on WS2)
4. Final: WS7 (simulation testing, exercises everything)

## Testing

- WS1: Unit tests for alloc/dealloc, stress test with concurrent slab usage
- WS2: Unit tests for mmap read/write/compaction, crash recovery
- WS3: Integration test: write keys, advance virtual time, verify demotion/promotion
- WS4: Unit tests for io_uring submit/poll (Linux CI), fallback tests on all platforms
- WS5: Unit tests for all XADD/XRANGE/XREAD, integration tests via TCP
- WS6: Unit test: two tenants, one over quota, verify only its keys evicted
- WS7: Simulation tests with fixed seeds, crash injection, verify consistency
