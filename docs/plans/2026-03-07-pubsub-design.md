# Pub/Sub Design — Beat Redis

## Goal

Implement Redis-compatible Pub/Sub (SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH) that outperforms Redis by exploiting Kora's multi-threaded architecture.

## Design Decisions

1. **General-case optimization** — handle both high fan-out and high channel cardinality
2. **First-class pattern subscriptions** — PSUBSCRIBE/PUNSUBSCRIBE shipped together with exact subscriptions
3. **Server-layer sharded broker** — channels partitioned across shards using same ahash, no global lock
4. **Direct async sender per subscriber** — tokio mpsc unbounded sender per subscribed connection
5. **New `kora-pubsub` crate** — independent broker, own tests/benchmarks

## Architecture

### Crate: `kora-pubsub`

Dependencies: `ahash`, `parking_lot`. No tokio dependency — broker is generic over a `MessageSink` trait.

### Core Types

```rust
pub struct PubSubBroker {
    shards: Vec<RwLock<ShardSubscriptions>>,
    shard_count: usize,
}

struct ShardSubscriptions {
    channels: HashMap<Vec<u8>, Vec<Subscriber>>,
    patterns: Vec<PatternSubscriber>,
}

struct Subscriber {
    conn_id: u64,
    tx: Box<dyn MessageSink>,
}

struct PatternSubscriber {
    conn_id: u64,
    pattern: Vec<u8>,
    tx: Box<dyn MessageSink>,
}

pub trait MessageSink: Send + Sync {
    fn send(&self, msg: PubSubMessage) -> bool;
}

pub enum PubSubMessage {
    Message { channel: Arc<[u8]>, data: Arc<[u8]> },
    PatternMessage { pattern: Arc<[u8]>, channel: Arc<[u8]>, data: Arc<[u8]> },
}
```

### Sharding Strategy

- Same `ahash` + modulo as `shard_for_key` in kora-core
- PUBLISH takes a **read lock** on one shard (fan-out is read-only iteration)
- SUBSCRIBE/UNSUBSCRIBE take a **write lock** on one shard (rare operation)
- Pattern subscriptions stored per-shard, checked on every PUBLISH to that shard
- Zero contention between publishes to different channels

### Connection Flow

```
SUBSCRIBE "chat"
  → hash "chat" → shard N
  → write-lock shard N registry
  → register connection's mpsc sender
  → connection enters push mode
  → return subscription confirmation

PUBLISH "chat" "hello"
  → hash "chat" → shard N
  → read-lock shard N registry
  → iterate subscribers, send PubSubMessage via MessageSink
  → check pattern subscribers for glob matches
  → return subscriber count

Subscribed connection
  → tokio::select! between client reads and mpsc receiver
  → push messages serialized as RESP arrays
```

### Push Mode

When `subscription_count > 0`, connection only accepts:
SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PING, QUIT

Connection handler uses `tokio::select!` between:
1. Reading commands from client socket
2. Receiving PubSubMessages from mpsc and writing to socket

When `subscription_count` drops to 0, exits push mode.

### RESP Wire Format

```
subscribe confirm:   *3\r\n$9\r\nsubscribe\r\n$<len>\r\n<channel>\r\n:<count>\r\n
message push:        *3\r\n$7\r\nmessage\r\n$<len>\r\n<channel>\r\n$<len>\r\n<data>\r\n
pmessage push:       *4\r\n$8\r\npmessage\r\n$<len>\r\n<pattern>\r\n$<len>\r\n<channel>\r\n$<len>\r\n<data>\r\n
unsubscribe confirm: *3\r\n$11\r\nunsubscribe\r\n$<len>\r\n<channel>\r\n:<count>\r\n
```

### Dead Subscriber Cleanup

- `MessageSink::send()` returns false when channel closed
- Lazily removed during PUBLISH iteration
- Also removed on explicit UNSUBSCRIBE / connection drop

### Glob Pattern Matching

Adapted from `kora-cdc/src/subscription.rs` glob matching logic.

## File Layout

```
kora-pubsub/
├── Cargo.toml
├── src/
│   ├── lib.rs       # PubSubBroker, ShardSubscriptions, MessageSink trait
│   ├── glob.rs      # glob pattern matching
│   └── message.rs   # PubSubMessage enum
├── benches/
│   └── pubsub.rs    # Criterion benchmarks
```

## Modified Files

- `Cargo.toml` — workspace member
- `kora-server/Cargo.toml` — add kora-pubsub dependency
- `kora-server/src/lib.rs` — PubSubBroker in ServerState, push mode in handle_stream
- `kora-core/src/command.rs` — Subscribe/Unsubscribe/PSubscribe/PUnsubscribe/Publish variants
- `kora-protocol/src/parser.rs` — parse pub/sub commands
- `kora-protocol/src/serializer.rs` — serialize pub/sub push messages

## Testing

**Unit (kora-pubsub):** subscribe/unsubscribe, publish fan-out, pattern matching, dead cleanup, concurrent shards
**Integration (kora-server):** TCP subscribe→publish→receive, pattern messages, push mode enforcement, unsubscribe exits push mode
**Benchmarks:** fan-out scaling, channel cardinality throughput, pattern matching overhead
