//! Sharded Pub/Sub broker.
//!
//! Channels are hashed to independent shards using `ahash`, enabling
//! lock-free parallelism for publishes to different channels. Pattern
//! subscriptions are replicated to all shards since any publish could match.

use std::sync::Arc;

use ahash::AHashMap;
use parking_lot::RwLock;

use crate::glob::glob_match;
use crate::message::PubSubMessage;

/// Trait for delivering messages to a subscriber.
///
/// Returns `true` if the message was accepted, `false` if the subscriber
/// is dead and should be cleaned up.
pub trait MessageSink: Send + Sync {
    /// Send a message to the subscriber.
    fn send(&self, msg: PubSubMessage) -> bool;
}

struct Subscriber {
    conn_id: u64,
    tx: Arc<dyn MessageSink>,
}

struct PatternSubscriber {
    conn_id: u64,
    pattern: Vec<u8>,
    tx: Arc<dyn MessageSink>,
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

/// A sharded Pub/Sub message broker.
///
/// Channels are hashed across `N` independent shards. PUBLISH takes a read
/// lock on the target shard only. SUBSCRIBE/UNSUBSCRIBE take a write lock.
pub struct PubSubBroker {
    shards: Vec<RwLock<ShardSubscriptions>>,
    shard_mask: usize,
}

impl PubSubBroker {
    /// Create a new broker with the given number of shards.
    ///
    /// `num_shards` is rounded up to the next power of two.
    pub fn new(num_shards: usize) -> Self {
        let num_shards = num_shards.next_power_of_two().max(1);
        let shards = (0..num_shards)
            .map(|_| RwLock::new(ShardSubscriptions::new()))
            .collect();
        Self {
            shards,
            shard_mask: num_shards - 1,
        }
    }

    fn shard_index(&self, channel: &[u8]) -> usize {
        let hash = ahash::RandomState::with_seeds(0, 0, 0, 0).hash_one(channel);
        (hash as usize) & self.shard_mask
    }

    /// Subscribe a connection to an exact channel.
    ///
    /// Duplicate subscriptions (same conn_id + channel) are ignored.
    pub fn subscribe(&self, channel: &[u8], conn_id: u64, tx: Arc<dyn MessageSink>) {
        let idx = self.shard_index(channel);
        let mut shard = self.shards[idx].write();
        let subs = shard.channels.entry(channel.to_vec()).or_default();
        if subs.iter().any(|s| s.conn_id == conn_id) {
            return;
        }
        subs.push(Subscriber { conn_id, tx });
    }

    /// Unsubscribe a connection from an exact channel.
    ///
    /// Returns `true` if the subscription existed and was removed.
    pub fn unsubscribe(&self, channel: &[u8], conn_id: u64) -> bool {
        let idx = self.shard_index(channel);
        let mut shard = self.shards[idx].write();
        let Some(subs) = shard.channels.get_mut(channel) else {
            return false;
        };
        let before = subs.len();
        subs.retain(|s| s.conn_id != conn_id);
        let removed = subs.len() < before;
        if subs.is_empty() {
            shard.channels.remove(channel);
        }
        removed
    }

    /// Subscribe a connection to a glob pattern.
    ///
    /// The pattern is stored in ALL shards since any publish could match.
    /// Duplicate subscriptions (same conn_id + pattern) are ignored.
    pub fn psubscribe(&self, pattern: &[u8], conn_id: u64, tx: Arc<dyn MessageSink>) {
        if self.shards[0]
            .read()
            .patterns
            .iter()
            .any(|p| p.conn_id == conn_id && p.pattern == pattern)
        {
            return;
        }
        for shard_lock in &self.shards {
            let mut shard = shard_lock.write();
            shard.patterns.push(PatternSubscriber {
                conn_id,
                pattern: pattern.to_vec(),
                tx: tx.clone(),
            });
        }
    }

    /// Unsubscribe a connection from a glob pattern.
    ///
    /// Returns `true` if the subscription existed and was removed.
    pub fn punsubscribe(&self, pattern: &[u8], conn_id: u64) -> bool {
        let mut found = false;
        for shard_lock in &self.shards {
            let mut shard = shard_lock.write();
            let before = shard.patterns.len();
            shard
                .patterns
                .retain(|p| !(p.conn_id == conn_id && p.pattern == pattern));
            if shard.patterns.len() < before {
                found = true;
            }
        }
        found
    }

    /// Publish a message to a channel.
    ///
    /// Returns the number of subscribers that received the message.
    /// Dead subscribers (where `send` returns `false`) are lazily cleaned up.
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
            for &i in dead_pattern_indices.iter().rev() {
                if i < shard.patterns.len() {
                    shard.patterns.swap_remove(i);
                }
            }
        }

        delivered
    }

    /// Remove all subscriptions (exact and pattern) for a connection.
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
            let messages = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    messages: messages.clone(),
                },
                messages,
            )
        }
    }

    impl MessageSink for MockSink {
        fn send(&self, msg: PubSubMessage) -> bool {
            self.messages.lock().unwrap().push(msg);
            true
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
        broker.subscribe(b"chat", 1, Arc::new(sink1));
        broker.subscribe(b"chat", 2, Arc::new(sink2));
        let count = broker.publish(b"chat", b"hello");
        assert_eq!(count, 2);
        assert_eq!(msgs1.lock().unwrap().len(), 1);
        assert_eq!(msgs2.lock().unwrap().len(), 1);
    }

    #[test]
    fn test_unsubscribe() {
        let broker = PubSubBroker::new(4);
        let (sink, msgs) = MockSink::new();
        broker.subscribe(b"chat", 1, Arc::new(sink));
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
        broker.subscribe(b"chat", 1, Arc::new(sink1));
        broker.subscribe(b"chat", 1, Arc::new(sink2));
        let count = broker.publish(b"chat", b"hello");
        assert_eq!(count, 1);
    }

    #[test]
    fn test_psubscribe_and_publish() {
        let broker = PubSubBroker::new(4);
        let (sink, msgs) = MockSink::new();
        broker.psubscribe(b"chat.*", 1, Arc::new(sink));
        let count = broker.publish(b"chat.general", b"hello");
        assert_eq!(count, 1);
        let received = msgs.lock().unwrap();
        match &received[0] {
            PubSubMessage::PatternMessage {
                pattern,
                channel,
                data,
            } => {
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
        broker.psubscribe(b"chat.*", 1, Arc::new(sink));
        let count = broker.publish(b"news.sports", b"hello");
        assert_eq!(count, 0);
        assert!(msgs.lock().unwrap().is_empty());
    }

    #[test]
    fn test_exact_and_pattern_both_deliver() {
        let broker = PubSubBroker::new(4);
        let (sink1, msgs1) = MockSink::new();
        let (sink2, msgs2) = MockSink::new();
        broker.subscribe(b"chat.general", 1, Arc::new(sink1));
        broker.psubscribe(b"chat.*", 1, Arc::new(sink2));
        let count = broker.publish(b"chat.general", b"hello");
        assert_eq!(count, 2);
        assert_eq!(msgs1.lock().unwrap().len(), 1);
        assert_eq!(msgs2.lock().unwrap().len(), 1);
    }

    #[test]
    fn test_punsubscribe() {
        let broker = PubSubBroker::new(4);
        let (sink, msgs) = MockSink::new();
        broker.psubscribe(b"chat.*", 1, Arc::new(sink));
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
        broker.subscribe(b"ch1", 1, Arc::new(sink1));
        broker.psubscribe(b"ch*", 1, Arc::new(sink2));
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

        broker.subscribe(b"chat", 1, Arc::new(DeadSink));
        let count = broker.publish(b"chat", b"hello");
        assert_eq!(count, 0);
        let count = broker.publish(b"chat", b"hello2");
        assert_eq!(count, 0);
    }

    #[test]
    fn test_independent_channels_no_interference() {
        let broker = PubSubBroker::new(4);
        let (sink1, msgs1) = MockSink::new();
        let (sink2, msgs2) = MockSink::new();
        broker.subscribe(b"channel-a", 1, Arc::new(sink1));
        broker.subscribe(b"channel-b", 2, Arc::new(sink2));
        broker.publish(b"channel-a", b"msg-a");
        broker.publish(b"channel-b", b"msg-b");
        assert_eq!(msgs1.lock().unwrap().len(), 1);
        assert_eq!(msgs2.lock().unwrap().len(), 1);
    }
}
