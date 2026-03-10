# CLAUDE.md — Kōra Development Guide

> Guidelines for AI assistants working on the Kōra codebase.

## Project Overview

Kōra is a **multi-threaded, embeddable, memory-safe cache engine written in Rust**. It is Redis protocol (RESP2) compatible and designed to surpass Redis's single-threaded limitation using a shared-nothing threading architecture (inspired by Seastar/ScyllaDB/Dragonfly).

**Current status:** v0.1.0 — core engine, storage, advanced features, and production hardening are all implemented.

## Repository Structure

This is a **Rust workspace monorepo** with the following crate layout:

```
kora/
├── Cargo.toml              # workspace root
├── CLAUDE.md               # this file
├── README.md               # project overview & documentation
├── kora-core/              # data structures, shard engine, memory management
│   ├── src/
│   │   ├── command.rs      # Command/CommandResponse enums
│   │   ├── error.rs        # Core error types
│   │   ├── hash.rs         # Key hashing & shard routing
│   │   ├── shard.rs        # ShardEngine, ShardStore, worker threads
│   │   └── types.rs        # Value, KeyEntry types
│   ├── benches/engine.rs   # Criterion benchmarks (SET/GET/INCR/MGET)
│   └── tests/stress.rs     # Concurrent stress tests
├── kora-protocol/          # RESP2 parser and serializer
│   ├── src/
│   │   ├── command.rs      # parse_command (RespValue → Command)
│   │   ├── error.rs        # Protocol errors
│   │   ├── parser.rs       # Streaming RespParser
│   │   ├── resp.rs         # RespValue enum
│   │   └── serializer.rs   # serialize_response
│   ├── benches/resp.rs     # Criterion benchmarks (parse/serialize)
│   └── tests/stress.rs     # Fuzz-like, roundtrip, pipeline tests
├── kora-server/            # TCP/Unix server, shard-affinity I/O engine
│   ├── src/
│   │   ├── lib.rs          # KoraServer, ServerConfig
│   │   └── shard_io/       # ShardIoEngine, ShardRouter, connection handler
│   └── tests/
│       ├── integration.rs  # TCP integration tests
│       └── real_app_traffic.rs # Benchmark tests
├── kora-embedded/          # library mode — direct API, no network
│   └── src/lib.rs          # Database struct with get/set/del/etc.
├── kora-storage/           # persistence layer
│   └── src/
│       ├── backend.rs      # StorageBackend trait, FileBackend
│       ├── compressor.rs   # LZ4 compression
│       ├── error.rs        # Storage error types
│       ├── manager.rs      # StorageManager (WAL + RDB + backend)
│       ├── rdb.rs          # RDB snapshot save/load
│       └── wal.rs          # Write-Ahead Log
├── kora-doc/               # JSON document database with secondary indexes
│   └── src/
│       ├── engine.rs       # DocEngine, collections, indexes
│       ├── expr.rs         # WHERE expression parser & evaluator
│       └── packed.rs       # PackedDoc binary format
├── kora-vector/            # HNSW vector index
│   ├── src/
│   │   ├── distance.rs     # Cosine, L2, InnerProduct
│   │   └── hnsw.rs         # HnswIndex
│   └── benches/hnsw.rs     # Criterion benchmarks
├── kora-cdc/               # change data capture
│   └── src/
│       ├── ring.rs         # CdcRing (per-shard ring buffer)
│       └── subscription.rs # Subscription manager with glob patterns
├── kora-pubsub/            # publish/subscribe messaging
│   └── src/broker.rs       # PubSubBroker with pattern matching
├── kora-observability/     # statistics & hot-key detection
│   └── src/
│       ├── sketch.rs       # CountMinSketch
│       └── stats.rs        # ShardStats, CommandTimer, StatsSnapshot
└── kora-cli/               # CLI binary with TOML config
    └── src/
        ├── config.rs       # TOML config file parsing
        └── main.rs         # Entrypoint with layered config
```

### Dependency Graph (strict, acyclic)

```
cli → server → core, protocol, storage, vector, cdc, pubsub, observability, doc
embedded → core, storage, vector, cdc, observability
```

`kora-core` has **zero** internal workspace dependencies. Everything flows downward. Never introduce circular dependencies between crates.

## Language & Toolchain

- **Language:** Rust (edition 2021, MSRV 1.82)
- **Build system:** Cargo workspace
- **Async runtime:** Tokio (server crate)

## Key Architectural Principles

These principles are non-negotiable — all code must follow them:

1. **Shard-affinity I/O:** Each shard worker thread owns both its data AND its connections' I/O via a `current_thread` tokio runtime. Store access uses `Rc<RefCell<>>` (no locks). Cross-shard communication uses `tokio::sync::mpsc` + `oneshot`.

2. **Zero-copy where possible:** Use `Arc<[u8]>` for shared strings, store keys/values as `Vec<u8>`.

3. **Trait-based storage abstraction:** All storage backends implement the `StorageBackend` trait. Never hardcode I/O strategies.

4. **Memory safety:** This is Rust — avoid `unsafe` unless absolutely required for performance-critical paths, and document every `unsafe` block with a safety comment.

5. **RESP2 compatibility:** RESP2 wire protocol commands should behave correctly. When implementing standard commands, match expected Redis semantics.

## Build & Run Commands

```bash
# Build the entire workspace
cargo build

# Run all tests
cargo test --workspace

# Run tests for a specific crate
cargo test -p kora-core

# Run stress/integration tests
cargo test --test stress -p kora-core
cargo test --test integration -p kora-server

# Run clippy lints
cargo clippy --workspace --all-targets

# Format code
cargo fmt --all

# Run benchmarks
cargo bench -p kora-core
cargo bench -p kora-protocol
cargo bench -p kora-vector

# Build documentation
cargo doc --workspace --no-deps

# Run the server
cargo run -- --port 6379 --workers 4
cargo run -- --config kora.toml
```

## Code Conventions

### Rust Style

- Follow standard Rust naming: `snake_case` for functions/variables, `PascalCase` for types/traits, `SCREAMING_SNAKE_CASE` for constants.
- Use `cargo fmt` formatting (default rustfmt settings).
- All code must pass `cargo clippy` with no warnings.
- Prefer `thiserror` for library error types and `anyhow` for application-level error handling.
- All public items must have doc comments (`///`).

### Error Handling

- Core/library crates: return `Result<T, E>` with crate-specific error types.
- Server/CLI crates: can use `anyhow::Result` for convenience.
- Never `unwrap()` or `expect()` in library code unless the invariant is provably guaranteed.

### Testing

- Unit tests go in `#[cfg(test)] mod tests {}` blocks.
- Integration tests go in `tests/` directories within each crate.
- Stress tests use multiple threads and randomized inputs.
- Benchmarks use Criterion in `benches/` directories.

### Performance

- Profile before optimizing.
- Benchmark critical paths (RESP parsing, hash lookups, shard routing).
- Avoid allocations in hot paths.

## Implemented Features

### Core Engine (kora-core)
- Sharded key-value store with configurable shard count
- String commands: GET, SET, GETSET, APPEND, STRLEN, INCR, DECR, INCRBY, DECRBY, MGET, MSET, SETNX
- Key commands: DEL, EXISTS, EXPIRE, PEXPIRE, PERSIST, TTL, PTTL, TYPE, KEYS, SCAN, DBSIZE, FLUSHDB
- List commands: LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX
- Hash commands: HSET, HGET, HDEL, HGETALL, HLEN, HEXISTS, HINCRBY
- Set commands: SADD, SREM, SMEMBERS, SISMEMBER, SCARD
- Server commands: PING, ECHO, INFO
- Lazy TTL expiration with periodic sweep

### Protocol (kora-protocol)
- Streaming RESP2 parser with incremental parsing
- Response serializer
- Command parsing from RESP arrays

### Server (kora-server)
- Shard-affinity I/O: each shard owns data + connection I/O
- `ShardIoEngine` with per-shard `current_thread` tokio runtimes
- Pipeline support, graceful shutdown, Unix socket support

### Storage (kora-storage)
- Write-Ahead Log with CRC-32C integrity, configurable sync policy, rotation
- RDB snapshots with atomic writes and CRC verification
- Cold-tier file backend with LZ4 compression and compaction
- StorageManager coordinating WAL + RDB + backend

### Vector Search (kora-vector)
- HNSW approximate nearest neighbor index
- Cosine, L2, and Inner Product distance metrics
- Configurable M, ef_construction parameters

### CDC (kora-cdc)
- Per-shard ring buffer with monotonic sequence numbers
- Gap detection for slow consumers
- Glob-pattern subscription filtering with cursor tracking

### Observability (kora-observability)
- Count-Min Sketch for hot key detection (~2KB)
- Per-shard atomic statistics (command counts, durations, memory, bytes)
- RAII CommandTimer for automatic duration recording
- Snapshot merging across shards

### Pub/Sub (kora-pubsub)
- SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH
- Thread-safe PubSubBroker with glob-pattern matching
- Push-mode delivery to subscriber connections

### Document Database (kora-doc)
- JSON document storage with PackedDoc binary format
- Secondary indexes: hash, sorted, array, unique
- WHERE expression parser and query executor
- Field projection and LIMIT/OFFSET pagination

### CLI (kora-cli)
- TOML config file support with layered configuration
- CLI argument overrides (--bind, --port, --workers, --log-level, --data-dir)

## Common Pitfalls

- **Don't add locks to the data path.** If you need shared state, use message passing via channels.
- **Don't break the crate dependency graph.** `kora-core` must remain dependency-free within the workspace.
- **Don't deviate from expected RESP2 command semantics** without explicit discussion.
- **Don't use `unsafe` without a `// SAFETY:` comment.**
- **Escape brackets in doc comments** that rustdoc might interpret as links (e.g., `\[optional\]`).

## Git Workflow

- Use descriptive commit messages summarizing the "why" not the "what."
- Keep commits focused — one logical change per commit.
- Run `cargo fmt --all && cargo clippy --workspace --all-targets && cargo test --workspace` before committing.
