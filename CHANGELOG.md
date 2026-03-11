# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-03-11

### Added

- **Data persistence for embedded and server modes** — WAL records are now written for document (`DOC.SET`, `DOC.DEL`, `DOC.UPDATE`) and vector (`VECSET`, `VECDEL`) operations, ensuring all data types survive restarts
- **Startup recovery** — both `Database::open` (embedded) and `KoraServer::new` (server) replay RDB snapshots and WAL entries on startup, restoring full state including documents and vectors
- **Recovery barrier synchronization** — all shard workers complete recovery before accepting connections or returning from `open()`, preventing stale reads
- **`ShardEngine::new_with_recovery`** — new constructor accepting per-shard recovery callbacks for custom startup logic
- **Query operators** — `IN`, `EXISTS`, `NOT`, and `ORDER BY` for document queries

### Fixed

- MSRV bumped to 1.82 to match toolchain requirements
- Clippy warnings resolved for CI compatibility (`-D warnings`)

## [0.1.0] - 2026-03-10

Initial open-source release.

### Added

- Multi-threaded shard-affinity I/O engine with linear core scaling
- RESP2 wire protocol support (compatible with existing Redis clients)
- Core data structures: strings, lists, hashes, sets, sorted sets, streams, bitmaps, geo, HyperLogLog
- JSON document database (`kora-doc`) with packed binary storage, secondary indexes (hash, sorted, array, unique), and WHERE query execution
- HNSW vector search with cosine, L2, and inner product distance metrics
- Change data capture with per-shard ring buffers and consumer groups
- Sharded pub/sub messaging with glob pattern matching
- Built-in observability: hot-key detection via Count-Min Sketch, per-command latency histograms, Prometheus metrics endpoint
- WAL + RDB persistence with LZ4 cold-tier compression
- Embeddable library mode (`kora-embedded`) with optional hybrid TCP listener
- CLI binary with TOML configuration and layered CLI argument overrides
- Docker support with multi-stage build
- CI pipeline with format, lint, and test checks
