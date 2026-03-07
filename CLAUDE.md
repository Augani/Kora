# CLAUDE.md — Kōra Development Guide

> Guidelines for AI assistants working on the Kōra codebase.

## Project Overview

Kōra is a **multi-threaded, embeddable, memory-safe cache engine written in Rust**. It is Redis protocol (RESP2/RESP3) compatible and designed to surpass Redis's single-threaded limitation using a shared-nothing threading architecture (inspired by Seastar/ScyllaDB/Dragonfly).

**Current status:** Architecture/design phase. The repository contains the architectural specification (`README.md`) and is ready for implementation.

## Repository Structure

This is a **Rust workspace monorepo** with the following planned crate layout:

```
kora/
├── Cargo.toml              # workspace root
├── CLAUDE.md               # this file
├── README.md               # architecture specification
├── kora-core/              # data structures, shard engine, memory management
├── kora-protocol/          # RESP2/RESP3 parser and serializer
├── kora-server/            # TCP/Unix listener, connection handling, command dispatch
├── kora-embedded/          # library mode — direct API, no network
├── kora-storage/           # tiered storage, NVMe spill, io_uring async I/O
├── kora-vector/            # HNSW index, similarity search, quantization
├── kora-cdc/               # change data capture, stream subscriptions
├── kora-scripting/         # WASM runtime (wasmtime), function registry
├── kora-observability/     # per-key stats, hot-key detection, memory attribution
└── kora-cli/               # CLI binary, config parsing, server entrypoint
```

### Dependency Graph (strict, acyclic)

```
cli → server → core, protocol, storage, vector, cdc, scripting, observability
embedded → core, storage, vector, cdc, observability
```

`kora-core` has **zero** internal workspace dependencies. Everything flows downward. Never introduce circular dependencies between crates.

## Language & Toolchain

- **Language:** Rust (latest stable)
- **Build system:** Cargo workspace
- **Async I/O:** io_uring (Linux), with fallback trait abstraction for other platforms
- **WASM runtime:** wasmtime (for the scripting crate)

## Key Architectural Principles

These principles are non-negotiable — all code must follow them:

1. **Shared-nothing threading:** Each worker thread owns a shard of the keyspace. No locks or mutexes on the data path. Cross-shard communication uses lock-free MPSC channels only.

2. **Per-thread memory allocation:** Each worker uses its own slab allocator. No global `malloc` contention. Large allocations (>4096 bytes) fall through to the system allocator.

3. **Zero-copy where possible:** Use `Arc<[u8]>` for shared strings, inline small strings (≤23 bytes) directly in the `Value` enum, and store keys ≤64 bytes inline in hash table entries.

4. **Trait-based storage abstraction:** All storage backends implement the `StorageBackend` trait. Never hardcode I/O strategies.

5. **Memory safety:** This is Rust — avoid `unsafe` unless absolutely required for performance-critical paths, and document every `unsafe` block with a safety comment.

6. **Redis compatibility:** Commands should behave identically to Redis. When in doubt, match Redis behavior exactly. Test against `redis-cli`.

## Build & Run Commands

```bash
# Build the entire workspace
cargo build

# Build a specific crate
cargo build -p kora-core

# Run all tests
cargo test

# Run tests for a specific crate
cargo test -p kora-core

# Run with release optimizations
cargo build --release

# Run clippy lints
cargo clippy --workspace --all-targets

# Format code
cargo fmt --all

# Run benchmarks (when available)
cargo bench
```

## Code Conventions

### Rust Style

- Follow standard Rust naming: `snake_case` for functions/variables, `PascalCase` for types/traits, `SCREAMING_SNAKE_CASE` for constants.
- Use `cargo fmt` formatting (default rustfmt settings unless a `rustfmt.toml` is added).
- All code must pass `cargo clippy` with no warnings.
- Prefer `thiserror` for library error types and `anyhow` for application-level error handling (in the CLI/server crates).
- Use `#[must_use]` on functions that return values that should not be ignored.

### Error Handling

- Core/library crates: return `Result<T, E>` with crate-specific error types.
- Server/CLI crates: can use `anyhow::Result` for convenience.
- Never `unwrap()` or `expect()` in library code unless the invariant is provably guaranteed (document why in a comment).
- Use `?` operator for error propagation.

### Documentation

- Public APIs must have doc comments (`///`).
- Add `# Examples` sections to doc comments for key public functions.
- Internal implementation details don't need doc comments unless the logic is non-obvious.

### Testing

- Unit tests go in the same file as the code, inside a `#[cfg(test)] mod tests {}` block.
- Integration tests go in `tests/` directories within each crate.
- Use descriptive test names: `test_set_and_get_inline_string`, not `test1`.
- Test both happy paths and error cases.
- Planned testing methodologies (Phase 4):
  - Deterministic simulation testing (like FoundationDB)
  - Crash recovery verification
  - Fuzz testing on RESP parser and storage engine (`cargo-fuzz` / AFL)
  - Benchmark suite against Redis, Dragonfly, KeyDB

### Performance

- Profile before optimizing. Don't guess at bottlenecks.
- Benchmark critical paths (RESP parsing, hash lookups, shard routing).
- Avoid allocations in hot paths — use the slab allocator or pre-allocated buffers.
- Keep the read path allocation-free for single-key operations.

## Key Data Structures

Understand these before modifying core code:

- **`Value` enum:** Discriminated union holding all Redis data types plus vectors and streams. Inline small strings, `Arc` heap strings, native `i64` integers.
- **`KeyEntry`:** Key + value + TTL + LFU counter + access time + flags. The fundamental unit of storage.
- **`ShardAllocator`:** Per-thread slab allocator with size classes (64, 128, 256, 512, 1024, 2048, 4096 bytes).
- **`HnswIndex`:** Per-shard HNSW graph for vector similarity search.
- **`CdcRing`:** Per-shard fixed-size ring buffer capturing mutations with monotonic sequence numbers.

## Implementation Phases

When implementing features, follow this priority order:

1. **Phase 1 (Core):** Threading model, slab allocator, basic data structures, RESP parsing, TCP server, core Redis commands (GET/SET/DEL/MGET/MSET/EXPIRE/TTL/KEYS/SCAN), embedded API.
2. **Phase 2 (Storage):** RDB snapshots, WAL, tiered storage with io_uring, background migration, eviction.
3. **Phase 3 (Advanced):** Vector index, CDC, WASM scripting, multi-tenancy, observability.
4. **Phase 4 (Hardening):** Simulation testing, fuzz testing, benchmarks, documentation.

## Common Pitfalls

- **Don't add locks to the data path.** If you need shared state, use message passing via channels.
- **Don't use global allocators in hot paths.** Route through the per-thread slab allocator.
- **Don't break the crate dependency graph.** `kora-core` must remain dependency-free within the workspace.
- **Don't deviate from Redis command semantics** without explicit discussion. Compatibility is a core feature.
- **Don't use `unsafe` without a `// SAFETY:` comment** explaining the invariant being upheld.
- **Don't forget platform abstraction.** io_uring is Linux-only; always go through the `StorageBackend` trait.

## Git Workflow

- Use descriptive commit messages summarizing the "why" not the "what."
- Keep commits focused — one logical change per commit.
- Run `cargo fmt --all && cargo clippy --workspace --all-targets && cargo test` before committing.
