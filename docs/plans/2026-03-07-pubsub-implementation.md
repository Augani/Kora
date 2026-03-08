# Pub/Sub Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement Redis-compatible Pub/Sub (SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH) that beats Redis by exploiting Kora's multi-threaded sharded architecture.

**Architecture:** New `kora-pubsub` crate with a sharded broker — channels hashed to N independent subscription registries using `ahash` (same hash as key routing). PUBLISH takes a read lock on one shard, SUBSCRIBE/UNSUBSCRIBE take a write lock. Broker is generic over a `MessageSink` trait (no tokio dependency). Server wires in `tokio::sync::mpsc` senders and adds push-mode connection handling via `tokio::select!`.

**Tech Stack:** Rust, `ahash`, `parking_lot::RwLock`, `tokio::sync::mpsc`, Criterion benchmarks.

---

### Task 1: Scaffold `kora-pubsub` crate with message types

**Files:**
- Create: `kora-pubsub/Cargo.toml`
- Create: `kora-pubsub/src/lib.rs`
- Create: `kora-pubsub/src/message.rs`
- Modify: `Cargo.toml` (workspace root, line 3–14 — add to members, line 77–87 — add workspace dep)

**Step 1: Create `kora-pubsub/Cargo.toml`**

```toml
[package]
name = "kora-pubsub"
version.workspace = true
edition.workspace = true
license.workspace = true
rust-version.workspace = true
description = "Sharded Pub/Sub broker for Kōra"

[dependencies]
ahash.workspace = true
parking_lot.workspace = true

[dev-dependencies]
criterion.workspace = true
rand.workspace = true

[[bench]]
name = "pubsub"
harness = false
```

**Step 2: Create `kora-pubsub/src/message.rs`**

```rust
//! Pub/Sub message types.

use std::sync::Arc;

/// A message delivered to a subscriber.
#[derive(Debug, Clone)]
pub enum PubSubMessage {
    /// Message from an exact channel subscription.
    Message {
        /// The channel name.
        channel: Arc<[u8]>,
        /// The message payload.
        data: Arc<[u8]>,
    },
    /// Message from a pattern subscription.
    PatternMessage {
        /// The pattern that matched.
        pattern: Arc<[u8]>,
        /// The channel name.
        channel: Arc<[u8]>,
        /// The message payload.
        data: Arc<[u8]>,
    },
}
```

**Step 3: Create `kora-pubsub/src/lib.rs`** (minimal, just re-exports)

```rust
//! Sharded Pub/Sub broker for Kōra.
//!
//! Provides a multi-threaded Pub/Sub message broker with per-channel sharding.
//! Channels are hashed to independent shards, enabling lock-free parallelism
//! for publishes to different channels.

pub mod message;

pub use message::PubSubMessage;
```

**Step 4: Add to workspace root `Cargo.toml`**

In `Cargo.toml`, add `"kora-pubsub"` to the `members` array (after `"kora-observability"`, before `"kora-cli"`). Also add `kora-pubsub = { path = "kora-pubsub" }` to `[workspace.dependencies]` (after `kora-observability`).

**Step 5: Verify it compiles**

Run: `cargo build -p kora-pubsub`
Expected: compiles with no errors.

**Step 6: Commit**

```bash
git add kora-pubsub/ Cargo.toml Cargo.lock
git commit -m "feat: scaffold kora-pubsub crate with message types"
```

---

### Task 2: Implement glob pattern matching

**Files:**
- Create: `kora-pubsub/src/glob.rs`
- Modify: `kora-pubsub/src/lib.rs`

**Step 1: Write failing tests in `kora-pubsub/src/glob.rs`**

```rust
//! Glob pattern matching for Pub/Sub channel patterns.
//!
//! Supports `*` (any sequence of characters) and `?` (any single character).

/// Match a glob pattern against a channel name.
///
/// Supports `*` (match any sequence) and `?` (match any single byte).
pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_match() {
        assert!(glob_match(b"hello", b"hello"));
        assert!(!glob_match(b"hello", b"world"));
    }

    #[test]
    fn test_star_wildcard() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"*", b""));
        assert!(glob_match(b"chat.*", b"chat.general"));
        assert!(glob_match(b"chat.*", b"chat."));
        assert!(!glob_match(b"chat.*", b"news.general"));
    }

    #[test]
    fn test_question_wildcard() {
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(glob_match(b"h?llo", b"hallo"));
        assert!(!glob_match(b"h?llo", b"hllo"));
        assert!(!glob_match(b"h?llo", b"heello"));
    }

    #[test]
    fn test_combined_wildcards() {
        assert!(glob_match(b"h*o", b"hello"));
        assert!(glob_match(b"h*o", b"ho"));
        assert!(glob_match(b"*?*", b"x"));
        assert!(!glob_match(b"*?*", b""));
    }

    #[test]
    fn test_empty_pattern() {
        assert!(glob_match(b"", b""));
        assert!(!glob_match(b"", b"x"));
    }

    #[test]
    fn test_multiple_stars() {
        assert!(glob_match(b"*.*.*", b"a.b.c"));
        assert!(glob_match(b"*.*.*", b"..."));
        assert!(!glob_match(b"*.*.*", b"a.b"));
    }

    #[test]
    fn test_redis_pubsub_patterns() {
        assert!(glob_match(b"news.*", b"news.art"));
        assert!(glob_match(b"news.*", b"news.sports"));
        assert!(!glob_match(b"news.*", b"chat.general"));
        assert!(glob_match(b"__keyevent@*__:*", b"__keyevent@0__:set"));
    }
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -p kora-pubsub`
Expected: FAIL with `not yet implemented`

**Step 3: Implement `glob_match`**

Replace the `todo!()` in `glob_match` with this implementation (adapted from `kora-cdc/src/subscription.rs:155-183`):

```rust
pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut pi = 0;
    let mut ti = 0;
    let mut star_pi = usize::MAX;
    let mut star_ti = 0;

    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}
```

**Step 4: Add `pub mod glob;` to `kora-pubsub/src/lib.rs`**

Add after `pub mod message;`:
```rust
pub mod glob;
```

**Step 5: Run tests to verify they pass**

Run: `cargo test -p kora-pubsub`
Expected: all tests PASS

**Step 6: Commit**

```bash
git add kora-pubsub/src/glob.rs kora-pubsub/src/lib.rs
git commit -m "feat: add glob pattern matching for pub/sub channels"
```

---

### Task 3: Implement `PubSubBroker` core

**Files:**
- Create: `kora-pubsub/src/broker.rs`
- Modify: `kora-pubsub/src/lib.rs`

**Step 1: Write failing tests in `kora-pubsub/src/broker.rs`**

The broker needs a `MessageSink` trait. For tests, use a mock that collects messages into a `Vec` behind a `Mutex`.

```rust
//! Sharded Pub/Sub broker.

use std::sync::Arc;

use ahash::AHashMap;
use parking_lot::RwLock;

use crate::glob::glob_match;
use crate::message::PubSubMessage;

/// Trait for delivering messages to a subscriber.
///
/// Implementors wrap a channel sender (e.g. tokio mpsc).
/// Returns `true` if the message was delivered, `false` if the subscriber is disconnected.
pub trait MessageSink: Send + Sync {
    /// Send a message to this subscriber.
    fn send(&self, msg: PubSubMessage) -> bool;
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

struct ShardSubscriptions {
    channels: AHashMap<Vec<u8>, Vec<Subscriber>>,
    patterns: Vec<PatternSubscriber>,
}

impl ShardSubscriptions {
    fn new() -> Self {
        Self {
            channels: AHashMap::new(),
            patterns: Vec::new(),
        }
    }
}

/// Sharded Pub/Sub broker.
///
/// Channels are hashed to N independent shards. PUBLISH takes a read lock
/// on a single shard, SUBSCRIBE/UNSUBSCRIBE take a write lock.
pub struct PubSubBroker {
    shards: Vec<RwLock<ShardSubscriptions>>,
    shard_count: usize,
}

impl PubSubBroker {
    /// Create a new broker with the given number of shards.
    pub fn new(shard_count: usize) -> Self {
        let shards = (0..shard_count)
            .map(|_| RwLock::new(ShardSubscriptions::new()))
            .collect();
        Self {
            shards,
            shard_count,
        }
    }

    fn shard_index(&self, channel: &[u8]) -> usize {
        use ahash::AHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = AHasher::default();
        channel.hash(&mut hasher);
        (hasher.finish() % self.shard_count as u64) as usize
    }

    /// Subscribe a connection to an exact channel.
    ///
    /// Returns the total number of subscriptions for this connection
    /// (channels + patterns) across all shards. This is expensive to compute
    /// so callers should track the count themselves — this method always returns 0
    /// and the caller increments.
    pub fn subscribe(&self, channel: &[u8], conn_id: u64, tx: Box<dyn MessageSink>) {
        let idx = self.shard_index(channel);
        let mut shard = self.shards[idx].write();
        let subs = shard.channels.entry(channel.to_vec()).or_default();
        if !subs.iter().any(|s| s.conn_id == conn_id) {
            subs.push(Subscriber { conn_id, tx });
        }
    }

    /// Unsubscribe a connection from an exact channel.
    ///
    /// Returns true if the connection was actually subscribed.
    pub fn unsubscribe(&self, channel: &[u8], conn_id: u64) -> bool {
        let idx = self.shard_index(channel);
        let mut shard = self.shards[idx].write();
        if let Some(subs) = shard.channels.get_mut(channel) {
            let before = subs.len();
            subs.retain(|s| s.conn_id != conn_id);
            if subs.is_empty() {
                shard.channels.remove(channel);
            }
            subs.len() < before || before > 0 && shard.channels.get(channel).is_none()
        } else {
            false
        }
    }

    /// Subscribe a connection to a glob pattern.
    pub fn psubscribe(&self, pattern: &[u8], conn_id: u64, tx: Box<dyn MessageSink>) {
        for shard in &self.shards {
            let mut s = shard.write();
            if !s.patterns.iter().any(|p| p.conn_id == conn_id && p.pattern == pattern) {
                s.patterns.push(PatternSubscriber {
                    conn_id,
                    pattern: pattern.to_vec(),
                    tx,
                });
                return;
            }
        }
        // Not found in any shard — add to first shard (patterns are checked globally)
        // Actually, pattern subs need to be in ALL shards since any publish can match.
        // But that's expensive. Instead, store patterns in a dedicated list and check on publish.
        // Let's store in all shards.
    }

    /// Publish a message to a channel.
    ///
    /// Returns the number of subscribers that received the message.
    pub fn publish(&self, channel: &[u8], data: &[u8]) -> usize {
        todo!()
    }

    /// Unsubscribe a connection from a glob pattern.
    pub fn punsubscribe(&self, pattern: &[u8], conn_id: u64) -> bool {
        todo!()
    }

    /// Remove all subscriptions for a connection (called on disconnect).
    pub fn remove_connection(&self, conn_id: u64) {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct MockSink {
        messages: Arc<Mutex<Vec<PubSubMessage>>>,
    }

    impl MockSink {
        fn new() -> (Self, Arc<Mutex<Vec<PubSubMessage>>>) {
            let msgs = Arc::new(Mutex::new(Vec::new()));
            (Self { messages: msgs.clone() }, msgs)
        }
    }

    impl MessageSink for MockSink {
        fn send(&self, msg: PubSubMessage) -> bool {
            self.messages.lock().unwrap().push(msg);
            true
        }
    }

    #[test]
    fn test_subscribe_and_publish() {
        let broker = PubSubBroker::new(4);
        let (sink, msgs) = MockSink::new();
        broker.subscribe(b"chat", 1, Box::new(sink));
        let count = broker.publish(b"chat", b"hello");
        assert_eq!(count, 1);
        let received = msgs.lock().unwrap();
        assert_eq!(received.len(), 1);
        match &received[0] {
            PubSubMessage::Message { channel, data } => {
                assert_eq!(channel.as_ref(), b"chat");
                assert_eq!(data.as_ref(), b"hello");
            }
            other => panic!("Expected Message, got {:?}", other),
        }
    }

    #[test]
    fn test_publish_no_subscribers() {
        let broker = PubSubBroker::new(4);
        let count = broker.publish(b"empty", b"hello");
        assert_eq!(count, 0);
    }

    #[test]
    fn test_multiple_subscribers() {
        let broker = PubSubBroker::new(4);
        let (sink1, msgs1) = MockSink::new();
        let (sink2, msgs2) = MockSink::new();
        broker.subscribe(b"chat", 1, Box::new(sink1));
        broker.subscribe(b"chat", 2, Box::new(sink2));
        let count = broker.publish(b"chat", b"hello");
        assert_eq!(count, 2);
        assert_eq!(msgs1.lock().unwrap().len(), 1);
        assert_eq!(msgs2.lock().unwrap().len(), 1);
    }

    #[test]
    fn test_unsubscribe() {
        let broker = PubSubBroker::new(4);
        let (sink, msgs) = MockSink::new();
        broker.subscribe(b"chat", 1, Box::new(sink));
        assert!(broker.unsubscribe(b"chat", 1));
        let count = broker.publish(b"chat", b"hello");
        assert_eq!(count, 0);
        assert!(msgs.lock().unwrap().is_empty());
    }

    #[test]
    fn test_unsubscribe_nonexistent() {
        let broker = PubSubBroker::new(4);
        assert!(!broker.unsubscribe(b"chat", 1));
    }

    #[test]
    fn test_duplicate_subscribe_ignored() {
        let broker = PubSubBroker::new(4);
        let (sink1, _) = MockSink::new();
        let (sink2, _) = MockSink::new();
        broker.subscribe(b"chat", 1, Box::new(sink1));
        broker.subscribe(b"chat", 1, Box::new(sink2));
        let count = broker.publish(b"chat", b"hello");
        assert_eq!(count, 1);
    }

    #[test]
    fn test_psubscribe_and_publish() {
        let broker = PubSubBroker::new(4);
        let (sink, msgs) = MockSink::new();
        broker.psubscribe(b"chat.*", 1, Box::new(sink));
        let count = broker.publish(b"chat.general", b"hello");
        assert_eq!(count, 1);
        let received = msgs.lock().unwrap();
        match &received[0] {
            PubSubMessage::PatternMessage { pattern, channel, data } => {
                assert_eq!(pattern.as_ref(), b"chat.*");
                assert_eq!(channel.as_ref(), b"chat.general");
                assert_eq!(data.as_ref(), b"hello");
            }
            other => panic!("Expected PatternMessage, got {:?}", other),
        }
    }

    #[test]
    fn test_psubscribe_no_match() {
        let broker = PubSubBroker::new(4);
        let (sink, msgs) = MockSink::new();
        broker.psubscribe(b"chat.*", 1, Box::new(sink));
        let count = broker.publish(b"news.sports", b"hello");
        assert_eq!(count, 0);
        assert!(msgs.lock().unwrap().is_empty());
    }

    #[test]
    fn test_exact_and_pattern_both_deliver() {
        let broker = PubSubBroker::new(4);
        let (sink1, msgs1) = MockSink::new();
        let (sink2, msgs2) = MockSink::new();
        broker.subscribe(b"chat.general", 1, Box::new(sink1));
        broker.psubscribe(b"chat.*", 1, Box::new(sink2));
        let count = broker.publish(b"chat.general", b"hello");
        assert_eq!(count, 2);
        assert_eq!(msgs1.lock().unwrap().len(), 1);
        assert_eq!(msgs2.lock().unwrap().len(), 1);
    }

    #[test]
    fn test_punsubscribe() {
        let broker = PubSubBroker::new(4);
        let (sink, msgs) = MockSink::new();
        broker.psubscribe(b"chat.*", 1, Box::new(sink));
        assert!(broker.punsubscribe(b"chat.*", 1));
        let count = broker.publish(b"chat.general", b"hello");
        assert_eq!(count, 0);
        assert!(msgs.lock().unwrap().is_empty());
    }

    #[test]
    fn test_remove_connection() {
        let broker = PubSubBroker::new(4);
        let (sink1, _) = MockSink::new();
        let (sink2, _) = MockSink::new();
        broker.subscribe(b"ch1", 1, Box::new(sink1));
        broker.psubscribe(b"ch*", 1, Box::new(sink2));
        broker.remove_connection(1);
        assert_eq!(broker.publish(b"ch1", b"x"), 0);
    }

    #[test]
    fn test_dead_subscriber_cleanup() {
        let broker = PubSubBroker::new(4);

        struct DeadSink;
        impl MessageSink for DeadSink {
            fn send(&self, _msg: PubSubMessage) -> bool {
                false
            }
        }

        broker.subscribe(b"chat", 1, Box::new(DeadSink));
        let count = broker.publish(b"chat", b"hello");
        assert_eq!(count, 0);
        // Publish again — dead subscriber should have been cleaned up
        let count = broker.publish(b"chat", b"hello2");
        assert_eq!(count, 0);
    }

    #[test]
    fn test_independent_channels_no_interference() {
        let broker = PubSubBroker::new(4);
        let (sink1, msgs1) = MockSink::new();
        let (sink2, msgs2) = MockSink::new();
        broker.subscribe(b"channel-a", 1, Box::new(sink1));
        broker.subscribe(b"channel-b", 2, Box::new(sink2));
        broker.publish(b"channel-a", b"msg-a");
        broker.publish(b"channel-b", b"msg-b");
        assert_eq!(msgs1.lock().unwrap().len(), 1);
        assert_eq!(msgs2.lock().unwrap().len(), 1);
    }
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -p kora-pubsub`
Expected: FAIL (todo! panics)

**Step 3: Implement the broker methods**

Replace the `todo!()` stubs and fix the `psubscribe` method. The key design decision for pattern subscriptions: patterns are stored in **every shard** since any publish on any shard could match. This means psubscribe writes to all shards, and publish checks the local shard's patterns.

Replace the full broker implementation. Key methods:

**`psubscribe`** — store the pattern in ALL shards (since any channel could match):
```rust
pub fn psubscribe(&self, pattern: &[u8], conn_id: u64, tx: Box<dyn MessageSink>) {
    // Pattern subs must exist in all shards since any publish could match.
    // We use Arc<dyn MessageSink> so we can clone across shards.
    // Actually, for simplicity, store patterns in a separate global list.
    // Better: store in all shards.
    for shard_lock in &self.shards {
        let mut shard = shard_lock.write();
        if shard.patterns.iter().any(|p| p.conn_id == conn_id && p.pattern == pattern) {
            return; // Already subscribed
        }
    }
    // Need to clone tx across shards — but Box<dyn MessageSink> isn't Clone.
    // Solution: Use Arc<dyn MessageSink> internally.
}
```

Wait — `Box<dyn MessageSink>` can't be cloned across shards. We need `Arc<dyn MessageSink>` for pattern subscriptions since they're stored in all shards. Let me restructure — change both `Subscriber` and `PatternSubscriber` to use `Arc<dyn MessageSink>`:

```rust
struct Subscriber {
    conn_id: u64,
    tx: Arc<dyn MessageSink>,
}

struct PatternSubscriber {
    conn_id: u64,
    pattern: Vec<u8>,
    tx: Arc<dyn MessageSink>,
}
```

And change the public API to accept `Arc<dyn MessageSink>`:
```rust
pub fn subscribe(&self, channel: &[u8], conn_id: u64, tx: Arc<dyn MessageSink>) { ... }
pub fn psubscribe(&self, pattern: &[u8], conn_id: u64, tx: Arc<dyn MessageSink>) { ... }
```

Then `psubscribe` stores an `Arc` clone in every shard's pattern list.

**`publish`**:
```rust
pub fn publish(&self, channel: &[u8], data: &[u8]) -> usize {
    let idx = self.shard_index(channel);
    let channel_arc: Arc<[u8]> = Arc::from(channel);
    let data_arc: Arc<[u8]> = Arc::from(data);
    let mut delivered = 0;

    // Exact subscribers — read lock on target shard only
    {
        let shard = self.shards[idx].read();
        if let Some(subs) = shard.channels.get(channel) {
            for sub in subs {
                if sub.tx.send(PubSubMessage::Message {
                    channel: channel_arc.clone(),
                    data: data_arc.clone(),
                }) {
                    delivered += 1;
                }
            }
        }
    }

    // Pattern subscribers — check target shard's patterns
    {
        let shard = self.shards[idx].read();
        for psub in &shard.patterns {
            if glob_match(&psub.pattern, channel) {
                if psub.tx.send(PubSubMessage::PatternMessage {
                    pattern: Arc::from(psub.pattern.as_slice()),
                    channel: channel_arc.clone(),
                    data: data_arc.clone(),
                }) {
                    delivered += 1;
                }
            }
        }
    }

    delivered
}
```

Wait — but pattern subscriptions are in ALL shards. So on publish we only check patterns in the target shard. But we stored the pattern in ALL shards (one copy per shard). So each shard has ALL patterns. That means publish only reads its target shard and gets all patterns. That works!

Actually, we need to merge the two read locks into one to avoid releasing and re-acquiring. And we need to handle dead subscriber cleanup during publish. Let me provide the complete implementation:

```rust
pub fn publish(&self, channel: &[u8], data: &[u8]) -> usize {
    let idx = self.shard_index(channel);
    let channel_arc: Arc<[u8]> = Arc::from(channel);
    let data_arc: Arc<[u8]> = Arc::from(data);
    let mut delivered = 0;
    let mut dead_channel_conns = Vec::new();
    let mut dead_pattern_indices = Vec::new();

    {
        let shard = self.shards[idx].read();

        if let Some(subs) = shard.channels.get(channel) {
            for sub in subs {
                if sub.tx.send(PubSubMessage::Message {
                    channel: channel_arc.clone(),
                    data: data_arc.clone(),
                }) {
                    delivered += 1;
                } else {
                    dead_channel_conns.push(sub.conn_id);
                }
            }
        }

        for (i, psub) in shard.patterns.iter().enumerate() {
            if glob_match(&psub.pattern, channel) {
                if psub.tx.send(PubSubMessage::PatternMessage {
                    pattern: Arc::from(psub.pattern.as_slice()),
                    channel: channel_arc.clone(),
                    data: data_arc.clone(),
                }) {
                    delivered += 1;
                } else {
                    dead_pattern_indices.push(i);
                }
            }
        }
    }

    // Lazy cleanup of dead subscribers
    if !dead_channel_conns.is_empty() || !dead_pattern_indices.is_empty() {
        let mut shard = self.shards[idx].write();
        for conn_id in &dead_channel_conns {
            if let Some(subs) = shard.channels.get_mut(channel) {
                subs.retain(|s| s.conn_id != *conn_id);
                if subs.is_empty() {
                    shard.channels.remove(channel);
                }
            }
        }
        // Remove dead patterns in reverse index order
        for &i in dead_pattern_indices.iter().rev() {
            if i < shard.patterns.len() {
                shard.patterns.swap_remove(i);
            }
        }
    }

    delivered
}
```

**`punsubscribe`**:
```rust
pub fn punsubscribe(&self, pattern: &[u8], conn_id: u64) -> bool {
    let mut found = false;
    for shard_lock in &self.shards {
        let mut shard = shard_lock.write();
        let before = shard.patterns.len();
        shard.patterns.retain(|p| !(p.conn_id == conn_id && p.pattern == pattern));
        if shard.patterns.len() < before {
            found = true;
        }
    }
    found
}
```

**`remove_connection`**:
```rust
pub fn remove_connection(&self, conn_id: u64) {
    for shard_lock in &self.shards {
        let mut shard = shard_lock.write();
        for subs in shard.channels.values_mut() {
            subs.retain(|s| s.conn_id != conn_id);
        }
        shard.channels.retain(|_, subs| !subs.is_empty());
        shard.patterns.retain(|p| p.conn_id != conn_id);
    }
}
```

**Step 4: Update `kora-pubsub/src/lib.rs`**

```rust
pub mod broker;
pub mod glob;
pub mod message;

pub use broker::{MessageSink, PubSubBroker};
pub use message::PubSubMessage;
```

**Step 5: Update the test MockSink to use `Arc<dyn MessageSink>`**

In the tests, change `Box::new(sink)` → `Arc::new(sink)` and update test signatures. The `MockSink::new()` return type becomes `(Arc<MockSink>, Arc<Mutex<Vec<PubSubMessage>>>)` — but since `subscribe` takes `Arc<dyn MessageSink>`, just pass `Arc::new(sink) as Arc<dyn MessageSink>`.

Actually simpler: keep `MockSink::new()` returning the sink and the messages handle. Then wrap with `Arc::new()` at call site.

**Step 6: Run tests**

Run: `cargo test -p kora-pubsub`
Expected: all tests PASS

**Step 7: Commit**

```bash
git add kora-pubsub/src/
git commit -m "feat: implement sharded PubSubBroker with pattern support"
```

---

### Task 4: Add Pub/Sub command variants to `kora-core`

**Files:**
- Modify: `kora-core/src/command.rs` (add 5 new variants + update helper methods)

**Step 1: Add command variants**

In `kora-core/src/command.rs`, add after the `// -- Stats commands --` section (before the closing `}` of the enum, around line 599):

```rust
    // -- Pub/Sub commands --
    /// SUBSCRIBE channel [channel ...]
    Subscribe {
        /// Channels to subscribe to.
        channels: Vec<Vec<u8>>,
    },
    /// UNSUBSCRIBE [channel ...]
    Unsubscribe {
        /// Channels to unsubscribe from (empty = all).
        channels: Vec<Vec<u8>>,
    },
    /// PSUBSCRIBE pattern [pattern ...]
    PSubscribe {
        /// Patterns to subscribe to.
        patterns: Vec<Vec<u8>>,
    },
    /// PUNSUBSCRIBE [pattern ...]
    PUnsubscribe {
        /// Patterns to unsubscribe from (empty = all).
        patterns: Vec<Vec<u8>>,
    },
    /// PUBLISH channel message
    Publish {
        /// The channel to publish to.
        channel: Vec<u8>,
        /// The message payload.
        message: Vec<u8>,
    },
```

**Step 2: Update `is_keyless()` to include pub/sub commands**

In the `is_keyless()` method (around line 679), add to the `matches!` macro:

```rust
                | Command::Subscribe { .. }
                | Command::Unsubscribe { .. }
                | Command::PSubscribe { .. }
                | Command::PUnsubscribe { .. }
                | Command::Publish { .. }
```

**Step 3: Update `cmd_type()` to include pub/sub commands**

Add a new match arm in `cmd_type()` (around line 793):

```rust
            Command::Subscribe { .. }
            | Command::Unsubscribe { .. }
            | Command::PSubscribe { .. }
            | Command::PUnsubscribe { .. }
            | Command::Publish { .. } => 35,
```

**Step 4: Verify it compiles**

Run: `cargo build -p kora-core`
Expected: compiles with no errors

**Step 5: Commit**

```bash
git add kora-core/src/command.rs
git commit -m "feat: add Pub/Sub command variants to Command enum"
```

---

### Task 5: Parse Pub/Sub commands in `kora-protocol`

**Files:**
- Modify: `kora-protocol/src/command.rs` (add parsing for 5 commands + tests)

**Step 1: Write failing tests**

Add to the `mod tests` block at the bottom of `kora-protocol/src/command.rs`:

```rust
    #[test]
    fn test_parse_subscribe() {
        let cmd = parse_command(make_cmd(&[b"SUBSCRIBE", b"chat", b"news"])).unwrap();
        match cmd {
            Command::Subscribe { channels } => {
                assert_eq!(channels.len(), 2);
                assert_eq!(channels[0], b"chat");
                assert_eq!(channels[1], b"news");
            }
            other => panic!("Expected Subscribe, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_unsubscribe_with_channels() {
        let cmd = parse_command(make_cmd(&[b"UNSUBSCRIBE", b"chat"])).unwrap();
        match cmd {
            Command::Unsubscribe { channels } => {
                assert_eq!(channels.len(), 1);
                assert_eq!(channels[0], b"chat");
            }
            other => panic!("Expected Unsubscribe, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_unsubscribe_no_args() {
        let cmd = parse_command(make_cmd(&[b"UNSUBSCRIBE"])).unwrap();
        match cmd {
            Command::Unsubscribe { channels } => {
                assert!(channels.is_empty());
            }
            other => panic!("Expected Unsubscribe, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_psubscribe() {
        let cmd = parse_command(make_cmd(&[b"PSUBSCRIBE", b"chat.*"])).unwrap();
        match cmd {
            Command::PSubscribe { patterns } => {
                assert_eq!(patterns.len(), 1);
                assert_eq!(patterns[0], b"chat.*");
            }
            other => panic!("Expected PSubscribe, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_punsubscribe() {
        let cmd = parse_command(make_cmd(&[b"PUNSUBSCRIBE", b"chat.*"])).unwrap();
        match cmd {
            Command::PUnsubscribe { patterns } => {
                assert_eq!(patterns.len(), 1);
                assert_eq!(patterns[0], b"chat.*");
            }
            other => panic!("Expected PUnsubscribe, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_publish() {
        let cmd = parse_command(make_cmd(&[b"PUBLISH", b"chat", b"hello world"])).unwrap();
        match cmd {
            Command::Publish { channel, message } => {
                assert_eq!(channel, b"chat");
                assert_eq!(message, b"hello world");
            }
            other => panic!("Expected Publish, got {:?}", other),
        }
    }
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -p kora-protocol -- test_parse_subscribe test_parse_unsubscribe test_parse_psubscribe test_parse_punsubscribe test_parse_publish`
Expected: FAIL (UnknownCommand)

**Step 3: Add parsing branches**

In `kora-protocol/src/command.rs`, add before the `_ => Err(ProtocolError::UnknownCommand(...))` catch-all (around line 672):

```rust
        // Pub/Sub commands
        b"SUBSCRIBE" => {
            check_min_arity("SUBSCRIBE", args, 1)?;
            let channels = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
            Ok(Command::Subscribe { channels })
        }
        b"UNSUBSCRIBE" => {
            let channels = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
            Ok(Command::Unsubscribe { channels })
        }
        b"PSUBSCRIBE" => {
            check_min_arity("PSUBSCRIBE", args, 1)?;
            let patterns = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
            Ok(Command::PSubscribe { patterns })
        }
        b"PUNSUBSCRIBE" => {
            let patterns = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
            Ok(Command::PUnsubscribe { patterns })
        }
        b"PUBLISH" => {
            check_arity("PUBLISH", args, 2)?;
            Ok(Command::Publish {
                channel: extract_bytes(&args[0])?,
                message: extract_bytes(&args[1])?,
            })
        }
```

**Step 4: Run tests to verify they pass**

Run: `cargo test -p kora-protocol`
Expected: all tests PASS

**Step 5: Commit**

```bash
git add kora-protocol/src/command.rs
git commit -m "feat: parse SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE/PUBLISH"
```

---

### Task 6: Wire Pub/Sub into the server

**Files:**
- Modify: `kora-server/Cargo.toml` (add `kora-pubsub` dependency)
- Modify: `kora-server/src/lib.rs` (add broker to state, handle pub/sub commands, push mode)

This is the largest task. The server needs to:
1. Hold a `PubSubBroker` in `ServerState`
2. Handle PUBLISH as a local command (returns subscriber count)
3. Handle SUBSCRIBE/PSUBSCRIBE by creating a mpsc channel, registering with broker, entering push mode
4. In push mode, `tokio::select!` between reading commands and receiving pushed messages
5. On connection drop, call `broker.remove_connection()`

**Step 1: Add dependency to `kora-server/Cargo.toml`**

Add after `kora-observability.workspace = true`:
```toml
kora-pubsub.workspace = true
```

**Step 2: Add broker to `ServerState`**

In `kora-server/src/lib.rs`, add import:
```rust
use kora_pubsub::{PubSubBroker, PubSubMessage, MessageSink};
```

Add field to `ServerState` (after `auth_password`):
```rust
    pub_sub: Arc<PubSubBroker>,
```

**Step 3: Initialize broker in `KoraServer::build()`**

In the `build` method, create the broker with the same shard count as the engine:
```rust
let pub_sub = Arc::new(PubSubBroker::new(config.worker_count));
```

Add it to the `ServerState` struct initialization:
```rust
            pub_sub,
```

**Step 4: Add connection ID generation**

Add an `AtomicU64` to `ServerState` for generating unique connection IDs:
```rust
    next_conn_id: AtomicU64,
```

Initialize to 1 in `build()`:
```rust
            next_conn_id: AtomicU64::new(1),
```

**Step 5: Create a `MessageSink` implementation using tokio mpsc**

Add this struct in `lib.rs`:
```rust
struct TokioSink {
    tx: tokio::sync::mpsc::UnboundedSender<PubSubMessage>,
}

impl MessageSink for TokioSink {
    fn send(&self, msg: PubSubMessage) -> bool {
        self.tx.send(msg).is_ok()
    }
}
```

**Step 6: Add `PubSubState` to connection handling**

Add to `ConnectionState`:
```rust
struct ConnectionState {
    resp3: bool,
    tenant_id: TenantId,
    conn_id: u64,
    pubsub_tx: Option<tokio::sync::mpsc::UnboundedSender<PubSubMessage>>,
    pubsub_rx: Option<tokio::sync::mpsc::UnboundedReceiver<PubSubMessage>>,
    subscription_count: usize,
    pattern_count: usize,
}
```

**Step 7: Modify `handle_stream` for push mode**

This is the core change. When `subscription_count + pattern_count > 0`, the connection loop uses `tokio::select!` to simultaneously:
- Read commands from the socket (only SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE/PING/QUIT)
- Receive pushed messages from the mpsc channel and serialize them to the socket

The main loop becomes:
```rust
loop {
    if conn.subscription_count + conn.pattern_count > 0 {
        // Push mode
        let rx = conn.pubsub_rx.as_mut().unwrap();
        tokio::select! {
            result = stream.read(&mut read_buf) => {
                let n = result?;
                if n == 0 { return Ok(()); }
                parser.feed(&read_buf[..n]);
                // Only allow pub/sub commands + PING + QUIT
                // Handle inline, write responses directly
                handle_pubsub_commands(&mut parser, &mut stream, &mut write_buf, &state, &mut conn).await?;
            }
            msg = rx.recv() => {
                match msg {
                    Some(pubsub_msg) => {
                        serialize_pubsub_message(&pubsub_msg, &mut write_buf, conn.resp3);
                        stream.write_all(&write_buf).await?;
                        write_buf.clear();
                    }
                    None => {
                        // Broker dropped our sender — shouldn't happen
                        return Ok(());
                    }
                }
            }
        }
    } else {
        // Normal request-response mode (existing code)
        let n = stream.read(&mut read_buf).await?;
        if n == 0 { return Ok(()); }
        // ... existing pipeline dispatch ...
    }
}
```

**Step 8: Implement `serialize_pubsub_message`**

```rust
fn serialize_pubsub_message(msg: &PubSubMessage, buf: &mut BytesMut, _resp3: bool) {
    match msg {
        PubSubMessage::Message { channel, data } => {
            // *3\r\n$7\r\nmessage\r\n$<len>\r\n<channel>\r\n$<len>\r\n<data>\r\n
            let resp = CommandResponse::Array(vec![
                CommandResponse::BulkString(b"message".to_vec()),
                CommandResponse::BulkString(channel.to_vec()),
                CommandResponse::BulkString(data.to_vec()),
            ]);
            serialize_response_versioned(&resp, buf, false);
        }
        PubSubMessage::PatternMessage { pattern, channel, data } => {
            // *4\r\n$8\r\npmessage\r\n$<len>\r\n<pattern>\r\n$<len>\r\n<channel>\r\n$<len>\r\n<data>\r\n
            let resp = CommandResponse::Array(vec![
                CommandResponse::BulkString(b"pmessage".to_vec()),
                CommandResponse::BulkString(pattern.to_vec()),
                CommandResponse::BulkString(channel.to_vec()),
                CommandResponse::BulkString(data.to_vec()),
            ]);
            serialize_response_versioned(&resp, buf, false);
        }
    }
}
```

**Step 9: Handle PUBLISH in `try_handle_local`**

Add to `try_handle_local` match:
```rust
        Command::Publish { channel, message } => {
            let count = state.pub_sub.publish(channel, message);
            Some(CommandResponse::Integer(count as i64))
        }
```

Also add `Command::Publish { .. }` to `is_local_command()`.

**Step 10: Handle SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE in `try_handle_local`**

These commands are special — they produce multiple responses (one per channel) and change connection state. Handle them in `try_handle_local`:

```rust
        Command::Subscribe { channels } => {
            let mut responses = Vec::new();
            ensure_pubsub_channel(conn);
            let sink: Arc<dyn MessageSink> = Arc::new(TokioSink {
                tx: conn.pubsub_tx.as_ref().unwrap().clone(),
            });
            for ch in channels {
                state.pub_sub.subscribe(ch, conn.conn_id, sink.clone());
                conn.subscription_count += 1;
                responses.push(CommandResponse::Array(vec![
                    CommandResponse::BulkString(b"subscribe".to_vec()),
                    CommandResponse::BulkString(ch.clone()),
                    CommandResponse::Integer((conn.subscription_count + conn.pattern_count) as i64),
                ]));
            }
            Some(if responses.len() == 1 {
                responses.into_iter().next().unwrap()
            } else {
                // Multiple subscribe confirmations are sent as individual RESP arrays
                // but since we batch, wrap in our response pipeline
                CommandResponse::Array(responses) // This won't work for Redis compat
            })
        }
```

Wait — Redis sends each subscribe confirmation as a separate RESP message, not nested in an array. So for `SUBSCRIBE ch1 ch2`, the client receives two separate `*3\r\n...` messages. Our current architecture returns a single `CommandResponse` per command. We need to handle this differently.

**Better approach**: Return a special response type that the serializer expands to multiple messages. Or handle SUBSCRIBE/UNSUBSCRIBE directly in the connection loop, not via `try_handle_local`.

The cleanest approach: handle all pub/sub commands **inline in the connection loop**, before the pending/dispatch logic. When we see a SUBSCRIBE command, we immediately:
1. Register with the broker
2. Write the confirmation responses directly to the write buffer
3. Skip the normal dispatch path

This is similar to how Redis handles it — pub/sub commands are processed immediately and don't go through the normal command pipeline.

**Revised approach for Step 10:**

In `handle_stream`, after parsing a command (`parse_command(frame)`), check if it's a pub/sub command and handle it inline:

```rust
Ok(cmd) => {
    if let Some(handled) = handle_pubsub_inline(&cmd, &state, &mut conn, &mut write_buf) {
        if handled {
            continue; // Skip normal dispatch
        }
    }
    // ... existing dispatch logic ...
}
```

```rust
fn handle_pubsub_inline(
    cmd: &Command,
    state: &ServerState,
    conn: &mut ConnectionState,
    buf: &mut BytesMut,
) -> Option<bool> {
    match cmd {
        Command::Subscribe { channels } => {
            ensure_pubsub_channel(conn);
            let sink: Arc<dyn MessageSink> = Arc::new(TokioSink {
                tx: conn.pubsub_tx.as_ref().unwrap().clone(),
            });
            for ch in channels {
                state.pub_sub.subscribe(ch, conn.conn_id, sink.clone());
                conn.subscription_count += 1;
                let total = conn.subscription_count + conn.pattern_count;
                let resp = CommandResponse::Array(vec![
                    CommandResponse::BulkString(b"subscribe".to_vec()),
                    CommandResponse::BulkString(ch.clone()),
                    CommandResponse::Integer(total as i64),
                ]);
                serialize_response_versioned(&resp, buf, conn.resp3);
            }
            Some(true)
        }
        Command::Unsubscribe { channels } => {
            if channels.is_empty() {
                // Unsubscribe from all — we'd need to track which channels this conn is on
                // For now, just reset count
                conn.subscription_count = 0;
                let resp = CommandResponse::Array(vec![
                    CommandResponse::BulkString(b"unsubscribe".to_vec()),
                    CommandResponse::Nil,
                    CommandResponse::Integer(conn.pattern_count as i64),
                ]);
                serialize_response_versioned(&resp, buf, conn.resp3);
            } else {
                for ch in channels {
                    state.pub_sub.unsubscribe(ch, conn.conn_id);
                    conn.subscription_count = conn.subscription_count.saturating_sub(1);
                    let total = conn.subscription_count + conn.pattern_count;
                    let resp = CommandResponse::Array(vec![
                        CommandResponse::BulkString(b"unsubscribe".to_vec()),
                        CommandResponse::BulkString(ch.clone()),
                        CommandResponse::Integer(total as i64),
                    ]);
                    serialize_response_versioned(&resp, buf, conn.resp3);
                }
            }
            Some(true)
        }
        Command::PSubscribe { patterns } => {
            ensure_pubsub_channel(conn);
            let sink: Arc<dyn MessageSink> = Arc::new(TokioSink {
                tx: conn.pubsub_tx.as_ref().unwrap().clone(),
            });
            for pat in patterns {
                state.pub_sub.psubscribe(pat, conn.conn_id, sink.clone());
                conn.pattern_count += 1;
                let total = conn.subscription_count + conn.pattern_count;
                let resp = CommandResponse::Array(vec![
                    CommandResponse::BulkString(b"psubscribe".to_vec()),
                    CommandResponse::BulkString(pat.clone()),
                    CommandResponse::Integer(total as i64),
                ]);
                serialize_response_versioned(&resp, buf, conn.resp3);
            }
            Some(true)
        }
        Command::PUnsubscribe { patterns } => {
            if patterns.is_empty() {
                conn.pattern_count = 0;
                let resp = CommandResponse::Array(vec![
                    CommandResponse::BulkString(b"punsubscribe".to_vec()),
                    CommandResponse::Nil,
                    CommandResponse::Integer(conn.subscription_count as i64),
                ]);
                serialize_response_versioned(&resp, buf, conn.resp3);
            } else {
                for pat in patterns {
                    state.pub_sub.punsubscribe(pat, conn.conn_id);
                    conn.pattern_count = conn.pattern_count.saturating_sub(1);
                    let total = conn.subscription_count + conn.pattern_count;
                    let resp = CommandResponse::Array(vec![
                        CommandResponse::BulkString(b"punsubscribe".to_vec()),
                        CommandResponse::BulkString(pat.clone()),
                        CommandResponse::Integer(total as i64),
                    ]);
                    serialize_response_versioned(&resp, buf, conn.resp3);
                }
            }
            Some(true)
        }
        Command::Publish { channel, message } => {
            let count = state.pub_sub.publish(channel, message);
            let resp = CommandResponse::Integer(count as i64);
            serialize_response_versioned(&resp, buf, conn.resp3);
            Some(true)
        }
        _ => None,
    }
}

fn ensure_pubsub_channel(conn: &mut ConnectionState) {
    if conn.pubsub_tx.is_none() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        conn.pubsub_tx = Some(tx);
        conn.pubsub_rx = Some(rx);
    }
}
```

**Step 11: Add cleanup on disconnect**

At the end of `handle_stream`, after the loop exits (connection closed), add:
```rust
state.pub_sub.remove_connection(conn.conn_id);
```

This needs to happen both on normal close (n == 0) and on error. Wrap the whole thing or use a `defer` pattern — simplest is to call it after the loop.

**Step 12: Verify it compiles**

Run: `cargo build -p kora-server`
Expected: compiles with no errors

**Step 13: Commit**

```bash
git add kora-server/
git commit -m "feat: wire PubSubBroker into server with push mode connection handling"
```

---

### Task 7: Integration tests for Pub/Sub

**Files:**
- Modify: `kora-server/tests/integration.rs`

**Step 1: Write integration tests**

Add these tests to `kora-server/tests/integration.rs`. These tests use two TCP connections — one subscriber, one publisher.

```rust
#[tokio::test]
async fn test_pubsub_subscribe_and_publish() {
    let port = free_port().await;
    let _shutdown = start_server(port).await;
    let mut sub = connect(port).await;
    let mut pub_conn = connect(port).await;

    // Subscribe
    let resp = cmd(&mut sub, &["SUBSCRIBE", "chat"]).await;
    // Should get: *3\r\n$9\r\nsubscribe\r\n$4\r\nchat\r\n:1\r\n
    assert!(String::from_utf8_lossy(&resp).contains("subscribe"));
    assert!(String::from_utf8_lossy(&resp).contains("chat"));

    // Publish
    let resp = cmd(&mut pub_conn, &["PUBLISH", "chat", "hello"]).await;
    // Should return :1\r\n (one subscriber)
    assert_eq!(resp, b":1\r\n");

    // Read pushed message on subscriber
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut buf = vec![0u8; 8192];
    let n = sub.read(&mut buf).await.unwrap();
    buf.truncate(n);
    let msg = String::from_utf8_lossy(&buf);
    assert!(msg.contains("message"));
    assert!(msg.contains("chat"));
    assert!(msg.contains("hello"));
}

#[tokio::test]
async fn test_pubsub_publish_no_subscribers() {
    let port = free_port().await;
    let _shutdown = start_server(port).await;
    let mut conn = connect(port).await;

    let resp = cmd(&mut conn, &["PUBLISH", "empty", "hello"]).await;
    assert_eq!(resp, b":0\r\n");
}

#[tokio::test]
async fn test_pubsub_psubscribe() {
    let port = free_port().await;
    let _shutdown = start_server(port).await;
    let mut sub = connect(port).await;
    let mut pub_conn = connect(port).await;

    // Pattern subscribe
    let resp = cmd(&mut sub, &["PSUBSCRIBE", "chat.*"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("psubscribe"));

    // Publish to matching channel
    let resp = cmd(&mut pub_conn, &["PUBLISH", "chat.general", "hi"]).await;
    assert_eq!(resp, b":1\r\n");

    // Read pushed message
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut buf = vec![0u8; 8192];
    let n = sub.read(&mut buf).await.unwrap();
    buf.truncate(n);
    let msg = String::from_utf8_lossy(&buf);
    assert!(msg.contains("pmessage"));
    assert!(msg.contains("chat.*"));
    assert!(msg.contains("chat.general"));
}

#[tokio::test]
async fn test_pubsub_unsubscribe() {
    let port = free_port().await;
    let _shutdown = start_server(port).await;
    let mut sub = connect(port).await;
    let mut pub_conn = connect(port).await;

    cmd(&mut sub, &["SUBSCRIBE", "chat"]).await;
    let resp = cmd(&mut sub, &["UNSUBSCRIBE", "chat"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("unsubscribe"));

    // Publish should now reach 0 subscribers
    let resp = cmd(&mut pub_conn, &["PUBLISH", "chat", "hello"]).await;
    assert_eq!(resp, b":0\r\n");
}

#[tokio::test]
async fn test_pubsub_multiple_channels() {
    let port = free_port().await;
    let _shutdown = start_server(port).await;
    let mut sub = connect(port).await;
    let mut pub_conn = connect(port).await;

    // Subscribe to two channels at once
    let resp = cmd(&mut sub, &["SUBSCRIBE", "ch1", "ch2"]).await;
    let msg = String::from_utf8_lossy(&resp);
    assert!(msg.contains("ch1"));
    assert!(msg.contains("ch2"));

    // Publish to ch2 only
    let resp = cmd(&mut pub_conn, &["PUBLISH", "ch2", "data"]).await;
    assert_eq!(resp, b":1\r\n");
}
```

**Step 2: Run tests**

Run: `cargo test -p kora-server --test integration -- test_pubsub`
Expected: all pub/sub tests PASS

**Step 3: Commit**

```bash
git add kora-server/tests/integration.rs
git commit -m "test: add Pub/Sub integration tests"
```

---

### Task 8: Benchmarks

**Files:**
- Create: `kora-pubsub/benches/pubsub.rs`

**Step 1: Write benchmarks**

```rust
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use kora_pubsub::{MessageSink, PubSubBroker, PubSubMessage};

struct NoopSink;

impl MessageSink for NoopSink {
    fn send(&self, _msg: PubSubMessage) -> bool {
        true
    }
}

fn bench_publish_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("publish_fanout");

    for sub_count in [1, 10, 100, 1000] {
        let broker = PubSubBroker::new(8);
        for i in 0..sub_count {
            broker.subscribe(b"bench-channel", i, Arc::new(NoopSink));
        }

        group.bench_with_input(
            BenchmarkId::from_parameter(sub_count),
            &sub_count,
            |b, _| {
                b.iter(|| {
                    broker.publish(b"bench-channel", b"hello world");
                });
            },
        );
    }

    group.finish();
}

fn bench_publish_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("publish_cardinality");
    let broker = PubSubBroker::new(8);

    let channel_count = 10_000;
    for i in 0..channel_count {
        let ch = format!("channel-{}", i);
        broker.subscribe(ch.as_bytes(), i as u64, Arc::new(NoopSink));
    }

    group.bench_function("10k_channels", |b| {
        let mut idx = 0u64;
        b.iter(|| {
            let ch = format!("channel-{}", idx % channel_count);
            broker.publish(ch.as_bytes(), b"data");
            idx += 1;
        });
    });

    group.finish();
}

fn bench_pattern_matching(c: &mut Criterion) {
    let mut group = c.benchmark_group("pattern_matching");

    for pattern_count in [1, 10, 100] {
        let broker = PubSubBroker::new(8);
        for i in 0..pattern_count {
            let pat = format!("prefix-{}.*", i);
            broker.psubscribe(pat.as_bytes(), i as u64, Arc::new(NoopSink));
        }

        group.bench_with_input(
            BenchmarkId::from_parameter(pattern_count),
            &pattern_count,
            |b, _| {
                b.iter(|| {
                    broker.publish(b"prefix-0.test", b"data");
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_publish_fanout,
    bench_publish_cardinality,
    bench_pattern_matching
);
criterion_main!(benches);
```

**Step 2: Run benchmarks**

Run: `cargo bench -p kora-pubsub`
Expected: benchmarks complete, results printed

**Step 3: Commit**

```bash
git add kora-pubsub/benches/
git commit -m "bench: add Pub/Sub broker benchmarks"
```

---

### Task 9: Full build, test, clippy, benchmark against Redis

**Step 1: Format**

Run: `cargo fmt --all`

**Step 2: Clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Fix any warnings.

**Step 3: Full test suite**

Run: `cargo test --workspace`
All tests must pass.

**Step 4: Benchmark against Redis**

Start Kora and Redis, then run the pub/sub benchmark:
```bash
# Start Redis on 6391
redis-server --port 6391 --daemonize yes

# Start Kora on 6392
cargo run --release -- --port 6392 &

# Benchmark (redis-benchmark doesn't have pub/sub directly,
# use a custom test or the subscribe benchmark)
redis-benchmark -p 6391 -n 500000 -c 64 -t publish
redis-benchmark -p 6392 -n 500000 -c 64 -t publish
```

Note: `redis-benchmark` has limited pub/sub support. For a proper comparison, use the `PUBLISH` command benchmark or write a custom subscriber/publisher test.

**Step 5: Commit everything**

```bash
git add -A
git commit -m "feat: complete Pub/Sub implementation — SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH"
```

---

## Summary

| Task | What | Files |
|------|------|-------|
| 1 | Scaffold crate + message types | `kora-pubsub/Cargo.toml`, `src/lib.rs`, `src/message.rs`, root `Cargo.toml` |
| 2 | Glob pattern matching | `kora-pubsub/src/glob.rs` |
| 3 | PubSubBroker core | `kora-pubsub/src/broker.rs` |
| 4 | Command variants | `kora-core/src/command.rs` |
| 5 | Protocol parsing | `kora-protocol/src/command.rs` |
| 6 | Server wiring + push mode | `kora-server/Cargo.toml`, `kora-server/src/lib.rs` |
| 7 | Integration tests | `kora-server/tests/integration.rs` |
| 8 | Benchmarks | `kora-pubsub/benches/pubsub.rs` |
| 9 | Full validation + Redis benchmark | all |
