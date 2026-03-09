# Redis Parity Gap-Closure Plan (Post Commit `4da59f3`)

## Objective
Close the remaining functional gaps so Kora can credibly claim Redis-compatibility for the supported deployment mode.

## Definition of Done
Kora reaches this bar when all of the following are true:
1. Command behavior matches Redis for supported commands, including edge cases and error text families.
2. `redis_comparison` and advanced compatibility suites pass without expected-difference carveouts.
3. Remaining divergences are either eliminated or explicitly documented as deployment constraints (for example, cluster-mode vs standalone semantics).

## Current Remaining Gaps

### P0: Semantic correctness gaps
- `WATCH`/`UNWATCH` are accepted but not enforced for `EXEC` conflict detection.
- `SELECT` is limited (db 0 only) and does not provide full Redis multi-DB behavior.
- `WAIT` now handles timeout validation and blocking shape, but cannot return replica acknowledgements without replication state.

### P1: Server-introspection parity gaps
- `COMMAND INFO`/`DOCS` metadata is intentionally lightweight vs Redis metadata richness.
- `COMMAND` subcommands like `GETKEYS` / `GETKEYSANDFLAGS` are not implemented.
- `CLIENT LIST` / `CLIENT INFO` output is simplified.
- `CONFIG SET` supports a narrow parameter subset.

### P2: Behavioral edge gaps
- Blocking command wakeups are polling-based, not event-driven.
- Stream consumer-group edge semantics need tighter parity checks for pending/claim/autoclaim corner cases.
- Transaction + cross-shard interactions need stricter parity validation.

## Execution Plan

## Phase 1: Transactions and DB Model (P0)
### Deliverables
- Implement optimistic locking semantics for `WATCH`/`UNWATCH`:
  - per-connection watch state
  - key versioning or mutation sequence tracking
  - `EXEC` abort (`Nil` array reply) when watched keys changed
- Define and implement multi-DB strategy:
  - Option A: true multi-DB namespace support (`db:index + key` routing)
  - Option B: explicit single-DB mode contract with strict Redis-compatible erroring and docs
- Add full `SELECT` bounds/behavior matrix tests.

### Acceptance Tests
- New integration tests for `WATCH` conflict and non-conflict paths.
- Redis comparison tests for `MULTI`/`EXEC` with watched keys.
- Deterministic tests for `SELECT` behavior across db indexes.

## Phase 2: Replication Foundations for `WAIT` (P0)
### Deliverables
- Introduce minimal replication ack accounting:
  - per-write replication sequence tracking
  - replica acknowledged sequence tracking
- Implement `WAIT numreplicas timeout` based on real ack counts.
- Keep current behavior behind feature/config flag until replication is production-hardened.

### Acceptance Tests
- Unit tests for ack accounting and timeout behavior.
- Integration tests with mocked/fake replicas validating:
  - immediate success when ack threshold met
  - timeout return with partial/no acknowledgements

## Phase 3: COMMAND/CLIENT/CONFIG Fidelity (P1)
### Deliverables
- Expand `COMMAND INFO` and `COMMAND DOCS` metadata fields.
- Add `COMMAND GETKEYS` and `COMMAND GETKEYSANDFLAGS`.
- Improve `CLIENT LIST/INFO` field fidelity and formatting.
- Expand `CONFIG GET/SET` coverage for commonly used parameters.

### Acceptance Tests
- Golden-output tests for representative `COMMAND` replies.
- Redis comparison tests for `COMMAND` subcommands, `CLIENT`, and `CONFIG`.

## Phase 4: Blocking + Streams Edge Semantics (P2)
### Deliverables
- Replace polling wakeups with event-driven wakeup path for blocking list/zset operations.
- Tighten stream group parity for `XPENDING`, `XCLAIM`, `XAUTOCLAIM` corner cases.
- Verify cross-shard + blocking interactions under load.

### Acceptance Tests
- Latency-sensitive tests showing no polling-delay artifact.
- Redis comparison matrix for stream group edge cases.

## Phase 5: Parity Validation Gate
### Deliverables
- Add a parity matrix document (`docs/redis-parity-matrix.md`) listing:
  - command status: `full`, `partial`, `unsupported`
  - known semantic notes
- Add CI job to run:
  - `cargo fmt --all`
  - `cargo clippy --workspace --all-targets`
  - `cargo test --workspace`
  - redis side-by-side compatibility suite (when Redis is available)

### Exit Criteria
- No P0 gaps open.
- P1 gaps either closed or explicitly documented with rationale.
- Compatibility matrix published and referenced in README.

## Work Breakdown Recommendation
1. Phase 1 (`WATCH` semantics + DB strategy) first.
2. Phase 2 (`WAIT` real acknowledgements) next.
3. Phase 3 (`COMMAND`/`CLIENT`/`CONFIG` fidelity).
4. Phase 4 (blocking/streams edge refinements).
5. Phase 5 validation gate and final parity claim.
