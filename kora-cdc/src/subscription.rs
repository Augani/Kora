//! Lightweight CDC subscriptions with cursor tracking.
//!
//! A [`Subscription`] provides a stateful read cursor over a
//! [`CdcRing`](crate::ring::CdcRing). Each call to
//! [`Subscription::poll`] returns the next batch of events and advances
//! the cursor, optionally filtering keys through a glob pattern.
//!
//! Multiple subscriptions can be managed together through a
//! [`SubscriptionManager`], which assigns monotonic IDs and exposes
//! subscribe / unsubscribe lifecycle operations.

use crate::ring::{CdcEvent, CdcReadResult, CdcRing};

/// A cursor-based CDC subscription.
///
/// Holds a read position into a [`CdcRing`](crate::ring::CdcRing) and an
/// optional glob pattern used to filter events by key. Call [`poll`](Subscription::poll)
/// to consume the next batch of matching events.
pub struct Subscription {
    id: u64,
    pattern: Option<String>,
    cursor: u64,
}

impl Subscription {
    /// Create a subscription starting from sequence 0.
    pub fn new(id: u64, pattern: Option<String>) -> Self {
        Self {
            id,
            pattern,
            cursor: 0,
        }
    }

    /// Create a subscription starting from the given sequence number.
    pub fn with_cursor(id: u64, pattern: Option<String>, cursor: u64) -> Self {
        Self {
            id,
            pattern,
            cursor,
        }
    }

    /// Return the unique subscription ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Return the current read-cursor position (next sequence to consume).
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Return the glob pattern, if one was set.
    pub fn pattern(&self) -> Option<&str> {
        self.pattern.as_deref()
    }

    /// Read up to `limit` new events from `ring`, advancing the cursor.
    ///
    /// When a pattern is set, only events whose key matches the glob are
    /// included in the returned batch. The cursor still advances past
    /// non-matching events so they are not re-examined on the next poll.
    pub fn poll(&mut self, ring: &CdcRing, limit: usize) -> CdcReadResult {
        let result = ring.read(self.cursor, limit);

        let filtered_events: Vec<CdcEvent> = if let Some(ref pattern) = self.pattern {
            result
                .events
                .into_iter()
                .filter(|e| glob_match(pattern, &e.key))
                .collect()
        } else {
            result.events
        };

        self.cursor = result.next_seq;

        CdcReadResult {
            events: filtered_events,
            next_seq: result.next_seq,
            gap: result.gap,
        }
    }

    /// Reposition the read cursor to an arbitrary sequence number.
    pub fn seek(&mut self, seq: u64) {
        self.cursor = seq;
    }
}

/// Registry for multiple CDC subscriptions.
///
/// Assigns monotonically increasing IDs and provides lookup, creation, and
/// removal of [`Subscription`] instances.
pub struct SubscriptionManager {
    subscriptions: Vec<Subscription>,
    next_id: u64,
}

impl SubscriptionManager {
    /// Create an empty subscription manager.
    pub fn new() -> Self {
        Self {
            subscriptions: Vec::new(),
            next_id: 1,
        }
    }

    /// Create a subscription starting at sequence 0 and return its ID.
    pub fn subscribe(&mut self, pattern: Option<String>) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.subscriptions.push(Subscription::new(id, pattern));
        id
    }

    /// Create a subscription beginning at `cursor` and return its ID.
    pub fn subscribe_at(&mut self, pattern: Option<String>, cursor: u64) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.subscriptions
            .push(Subscription::with_cursor(id, pattern, cursor));
        id
    }

    /// Remove a subscription by ID. Returns `true` if it existed.
    pub fn unsubscribe(&mut self, id: u64) -> bool {
        let before = self.subscriptions.len();
        self.subscriptions.retain(|s| s.id != id);
        self.subscriptions.len() < before
    }

    /// Look up a subscription by ID, returning a mutable reference.
    pub fn get_mut(&mut self, id: u64) -> Option<&mut Subscription> {
        self.subscriptions.iter_mut().find(|s| s.id == id)
    }

    /// Return the number of active subscriptions.
    pub fn len(&self) -> usize {
        self.subscriptions.len()
    }

    /// Return `true` if there are no active subscriptions.
    pub fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Match a glob `pattern` against a UTF-8 `key`.
///
/// Supports `*` (zero or more characters) and `?` (exactly one character).
/// Returns `false` for non-UTF-8 keys.
fn glob_match(pattern: &str, key: &[u8]) -> bool {
    let key_str = match std::str::from_utf8(key) {
        Ok(s) => s,
        Err(_) => return false,
    };
    glob_match_str(pattern.as_bytes(), key_str.as_bytes())
}

fn glob_match_str(pattern: &[u8], text: &[u8]) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ring::CdcOp;

    fn make_ring_with_events(n: usize) -> CdcRing {
        let mut ring = CdcRing::new(100);
        for i in 0..n {
            ring.push(
                CdcOp::Set,
                format!("user:{}", i).into_bytes(),
                Some(format!("val{}", i).into_bytes()),
                i as u64,
            );
        }
        ring
    }

    #[test]
    fn test_subscription_poll() {
        let ring = make_ring_with_events(5);
        let mut sub = Subscription::new(1, None);

        let result = sub.poll(&ring, 3);
        assert_eq!(result.events.len(), 3);
        assert_eq!(sub.cursor(), 3);

        let result = sub.poll(&ring, 100);
        assert_eq!(result.events.len(), 2);
        assert_eq!(sub.cursor(), 5);

        // No more events
        let result = sub.poll(&ring, 100);
        assert!(result.events.is_empty());
    }

    #[test]
    fn test_subscription_with_pattern() {
        let mut ring = CdcRing::new(100);
        ring.push(CdcOp::Set, b"user:1".to_vec(), None, 1);
        ring.push(CdcOp::Set, b"order:1".to_vec(), None, 2);
        ring.push(CdcOp::Set, b"user:2".to_vec(), None, 3);
        ring.push(CdcOp::Set, b"order:2".to_vec(), None, 4);

        let mut sub = Subscription::new(1, Some("user:*".into()));
        let result = sub.poll(&ring, 100);
        assert_eq!(result.events.len(), 2);
        assert_eq!(result.events[0].key, b"user:1");
        assert_eq!(result.events[1].key, b"user:2");
    }

    #[test]
    fn test_subscription_seek() {
        let ring = make_ring_with_events(10);
        let mut sub = Subscription::new(1, None);
        sub.seek(7);

        let result = sub.poll(&ring, 100);
        assert_eq!(result.events.len(), 3);
        assert_eq!(result.events[0].seq, 7);
    }

    #[test]
    fn test_manager_subscribe_unsubscribe() {
        let mut mgr = SubscriptionManager::new();
        let id1 = mgr.subscribe(None);
        let id2 = mgr.subscribe(Some("user:*".into()));

        assert_eq!(mgr.len(), 2);
        assert!(mgr.unsubscribe(id1));
        assert_eq!(mgr.len(), 1);
        assert!(!mgr.unsubscribe(id1)); // already removed
        assert!(mgr.unsubscribe(id2));
        assert!(mgr.is_empty());
    }

    #[test]
    fn test_manager_subscribe_at() {
        let mut mgr = SubscriptionManager::new();
        let id = mgr.subscribe_at(None, 42);
        let sub = mgr.get_mut(id).unwrap();
        assert_eq!(sub.cursor(), 42);
    }

    #[test]
    fn test_glob_match_patterns() {
        assert!(glob_match("*", b"anything"));
        assert!(glob_match("user:*", b"user:123"));
        assert!(!glob_match("user:*", b"order:123"));
        assert!(glob_match("user:?", b"user:1"));
        assert!(!glob_match("user:?", b"user:12"));
        assert!(glob_match("*:*", b"foo:bar"));
        assert!(glob_match("", b""));
        assert!(!glob_match("", b"x"));
    }
}
