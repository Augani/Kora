# Kora

**A multi-threaded, embeddable, memory-safe cache engine written in Rust.**

<!-- badges -->
[![Build](https://img.shields.io/github/actions/workflow/status/Augani/Kora/ci.yml?branch=main)](https://github.com/Augani/Kora/actions)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Crates.io](https://img.shields.io/crates/v/kora.svg)](https://crates.io/crates/kora)

*Kora (kora) — from Sanskrit (core, essence) and Twi (kora). Also echoes "core" in English.*

---

## What is Kora

Kora is a cache engine built on a shared-nothing, shard-affinity threading architecture inspired by Seastar and ScyllaDB. Each worker thread owns both its data and its connections' I/O — no locks on the data path, linear scaling with cores.

It speaks RESP2 on the wire, so existing Redis clients work out of the box. But Kora goes beyond caching: it includes a JSON document database with secondary indexes and WHERE queries, HNSW vector search, change data capture, and built-in observability. The entire engine compiles as an embeddable library for in-process use with zero network overhead.

---

## Key Features

- **Multi-threaded shard-affinity I/O** — each worker owns data + connections, scales linearly with cores
- **JSON document database** — secondary indexes (hash, sorted, array, unique), WHERE clause queries, field projection
- **HNSW vector search** — cosine, L2, and inner product distance metrics
- **Change data capture** — per-shard ring buffers with cursor-based subscriptions and gap detection
- **Sharded pub/sub** — SUBSCRIBE, PSUBSCRIBE, PUBLISH with glob pattern matching
- **Built-in observability** — Count-Min Sketch hot-key detection, per-command latency histograms, atomic shard stats
- **WAL + RDB persistence** — CRC-verified write-ahead log, atomic snapshots, LZ4-compressed cold-tier storage
- **Embeddable library mode** — same multi-threaded engine, no network required
- **RESP2 wire protocol** — works with redis-cli, Jedis, ioredis, redis-rs, and any Redis client

---

## Quick Start

### Build from source

```bash
git clone https://github.com/Augani/Kora.git
cd kora
cargo build --release
```

### Start the server

```bash
./target/release/kora-cli --port 6379 --workers 4
```

### Connect with redis-cli

```bash
redis-cli -p 6379

127.0.0.1:6379> SET greeting "hello world"
OK
127.0.0.1:6379> GET greeting
"hello world"
127.0.0.1:6379> INCR counter
(integer) 1
127.0.0.1:6379> HSET user:1 name "Augustus" city "Accra"
(integer) 2
127.0.0.1:6379> HGETALL user:1
1) "name"
2) "Augustus"
3) "city"
4) "Accra"
```

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────┐
│                      Client Layer                        │
│            RESP2 Protocol  /  Embedded API               │
└──────────────┬──────────────────────┬────────────────────┘
               │ TCP/Unix socket      │ fn call (in-process)
               v                      v
┌──────────────────────────────────────────────────────────┐
│                    Router / Dispatcher                    │
│            hash(key) % N -> worker thread                │
└──────┬────────┬────────┬────────┬────────┬───────────────┘
       v        v        v        v        v
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
       v        v        v        v        v
┌──────────────────────────────────────────────────────────┐
│                    Persistence Layer                      │
│           WAL + RDB Snapshots + LZ4 Cold Tier            │
└──────────────────────────────────────────────────────────┘
```

Each shard worker runs its own `current_thread` Tokio runtime. Local-key commands execute inline with zero channel hops. Foreign-key commands take a single async hop via `tokio::sync::mpsc` + `oneshot`. Data structures use `Rc<RefCell<>>` instead of `Arc<Mutex<>>` — no lock contention.

---

## Performance

Benchmarked on AWS m5.xlarge (4 vCPU, 16GB RAM) with memtier_benchmark, 200 clients, 256-byte values.

### Throughput (ops/sec)

| Workload        | Redis 8    | Dragonfly 1.37 | Kora       | vs Redis     | vs Dragonfly    |
|-----------------|------------|----------------|------------|--------------|-----------------|
| SET-only        | 138,239    | 236,885        | 229,535    | **+66.0%**   | -3.1%           |
| GET-only        | 144,240    | 241,305        | 239,230    | **+65.9%**   | -0.9%           |
| Mixed 1:1       | 139,014    | 232,507        | 233,377    | **+67.9%**   | **+0.4%**       |
| Pipeline x16    | 510,705    | 389,286        | 769,374    | **+50.7%**   | **+97.7%**      |

### p50 Latency (ms)

| Workload        | Redis 8 | Dragonfly | Kora      |
|-----------------|---------|-----------|-----------|
| SET-only        | 1.415   | 0.847     | **0.831** |
| GET-only        | 1.359   | 0.839     | **0.839** |
| Mixed 1:1       | 1.415   | 0.871     | **0.847** |
| Pipeline x16    | 6.303   | 8.191     | **4.063** |

### p99 Latency (ms)

| Workload        | Redis 8 | Dragonfly | Kora      |
|-----------------|---------|-----------|-----------|
| SET-only        | 2.111   | 1.175     | 1.223     |
| GET-only        | 1.943   | 1.143     | 1.327     |
| Mixed 1:1       | 1.999   | 1.175     | 1.631     |
| Pipeline x16    | 9.087   | 10.815    | **7.519** |

---

## Crate Structure

```
kora/
├── kora-core/           Core data structures, shard engine, memory management
├── kora-protocol/       RESP2 streaming parser and response serializer
├── kora-server/         TCP/Unix server, shard-affinity I/O engine
├── kora-embedded/       Library mode — direct API, no network
├── kora-storage/        WAL, RDB snapshots, LZ4-compressed cold-tier backend
├── kora-doc/            JSON document database, secondary indexes, WHERE queries
├── kora-vector/         HNSW approximate nearest neighbor index
├── kora-cdc/            Change data capture with per-shard ring buffers
├── kora-pubsub/         Publish/subscribe messaging with pattern support
├── kora-observability/  Hot-key detection, per-command stats, latency histograms
└── kora-cli/            CLI binary with TOML config support
```

`kora-core` has zero internal workspace dependencies. The dependency graph is strictly acyclic.

---

## Embedded Mode

Use Kora as a library, not a server. The same multi-threaded engine runs in-process with sub-microsecond dispatch latency.

```rust
use kora_embedded::{Config, Database};

let db = Database::open(Config::default());

db.set("user:1", b"Augustus");
let val = db.get("user:1"); // Some(b"Augustus")

db.hset("profile:1", "name", b"Augustus");
db.hset("profile:1", "city", b"Accra");
let name = db.hget("profile:1", "name"); // Some(b"Augustus")

db.lpush("queue", &[b"task-1", b"task-2"]);
let tasks = db.lrange("queue", 0, -1);
```

Hybrid mode — embed the database and expose a TCP listener for external tools:

```rust
let db = Database::open(config);
db.start_listener("127.0.0.1:6379")?; // non-blocking
db.set("key", b"works from both paths");
```

---

## Document Database

Kora includes a JSON-native document database with secondary indexes and a WHERE expression query engine.

### Via redis-cli

```bash
# Create a collection
127.0.0.1:6379> DOC.CREATE users

# Insert documents
127.0.0.1:6379> DOC.SET users user:1 '{"name":"Augustus","age":30,"city":"Accra"}'
127.0.0.1:6379> DOC.SET users user:2 '{"name":"Kwame","age":25,"city":"Kumasi"}'

# Create a secondary index
127.0.0.1:6379> DOC.CREATEINDEX users city hash

# Query with WHERE clause
127.0.0.1:6379> DOC.FIND users WHERE city = "Accra"
127.0.0.1:6379> DOC.FIND users WHERE age > 20 AND city = "Accra" LIMIT 10

# Count matching documents
127.0.0.1:6379> DOC.COUNT users WHERE age >= 25

# Field projection
127.0.0.1:6379> DOC.GET users user:1 FIELDS name city
```

### Via embedded API

```rust
use serde_json::json;

db.doc_create_collection("users", Default::default())?;
db.doc_set("users", "user:1", &json!({"name": "Augustus", "age": 30, "city": "Accra"}))?;
db.doc_create_index("users", "city", "hash")?;
let results = db.doc_find("users", "city = \"Accra\"", None, Some(10), 0)?;
```

Supported index types: `hash`, `sorted`, `array`, `unique`.

---

## Configuration

### CLI arguments

```bash
kora-cli \
  --bind 0.0.0.0 \
  --port 6379 \
  --workers 8 \
  --log-level info \
  --password "s3cret" \
  --data-dir /var/lib/kora \
  --snapshot-interval-secs 300 \
  --snapshot-retain 24 \
  --cdc-capacity 65536 \
  --metrics-port 9090 \
  --unix-socket /tmp/kora.sock
```

### TOML config file

```toml
bind = "0.0.0.0"
port = 6379
workers = 8
log_level = "info"
password = "s3cret"
cdc_capacity = 65536
metrics_port = 9090
unix_socket = "/tmp/kora.sock"

[storage]
data_dir = "/var/lib/kora"
wal_sync = "every_second"   # every_write | every_second | os_managed
wal_enabled = true
rdb_enabled = true
snapshot_interval_secs = 300
snapshot_retain = 24
wal_max_bytes = 67108864
```

CLI arguments override config file values. If no config file is specified, Kora looks for `kora.toml` in the working directory.

---

## Building from Source

```bash
# Build the workspace
cargo build --release

# Run all tests
cargo test --workspace

# Run clippy lints
cargo clippy --workspace --all-targets

# Format code
cargo fmt --all

# Run benchmarks
cargo bench -p kora-core
cargo bench -p kora-protocol
cargo bench -p kora-vector
```

Requires Rust 1.75+ (edition 2021).

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on submitting issues and pull requests.

---

## License

MIT
