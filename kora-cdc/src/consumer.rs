//! CDC consumer groups with acknowledgement tracking and redelivery.
//!
//! A [`ConsumerGroup`] coordinates multiple named consumers reading from the
//! same [`CdcRing`](crate::ring::CdcRing). Each consumer maintains its own
//! cursor and a pending-entry list. Events are delivered at-least-once:
//! delivered entries remain pending until explicitly acknowledged, and entries
//! that exceed a configurable idle timeout become reclaimable by other
//! consumers via the [`claim`](ConsumerGroupManager::claim) operation.
//!
//! The [`ConsumerGroupManager`] owns all groups for a single ring and
//! exposes the full lifecycle: group creation, consumer reads, acknowledgement,
//! pending-entry inspection, ownership transfer, and gap detection.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use crate::ring::{CdcEvent, CdcRing};

/// An event that has been delivered to a consumer but not yet acknowledged.
///
/// Tracks delivery metadata so the system can detect stalled consumers
/// and redeliver entries that have exceeded the idle timeout.
#[derive(Debug, Clone)]
pub struct PendingEntry {
    /// Sequence number of the underlying [`CdcEvent`](crate::ring::CdcEvent).
    pub event_seq: u64,
    /// How many times this entry has been delivered (initial + redeliveries).
    pub delivery_count: u32,
    /// Instant of the very first delivery attempt.
    pub first_delivery: Instant,
    /// Instant of the most recent delivery attempt.
    pub last_delivery: Instant,
}

/// Mutable state for one consumer within a [`ConsumerGroup`].
#[derive(Debug)]
pub struct ConsumerState {
    /// Highest sequence number that has been acknowledged.
    pub last_acked_seq: u64,
    /// Ordered queue of entries delivered but not yet acknowledged.
    pub pending: VecDeque<PendingEntry>,
    /// Set to `true` when a gap is detected, indicating the consumer
    /// missed events and may need to reconcile externally.
    pub needs_resync: bool,
}

impl ConsumerState {
    fn new(start_seq: u64) -> Self {
        Self {
            last_acked_seq: start_seq,
            pending: VecDeque::new(),
            needs_resync: false,
        }
    }
}

/// A named group of consumers sharing a single CDC stream.
///
/// The group maintains a high-watermark (`last_delivered_seq`) so that
/// each new read delivers only events that no consumer in the group has
/// seen yet. Individual consumer state is tracked in [`ConsumerState`].
#[derive(Debug)]
pub struct ConsumerGroup {
    /// Human-readable group name.
    pub name: String,
    /// Per-consumer tracking state, keyed by consumer name.
    pub consumers: HashMap<String, ConsumerState>,
    /// Next sequence to deliver (high watermark for the group).
    pub last_delivered_seq: u64,
    /// Duration after which an unacknowledged entry becomes reclaimable.
    pub idle_timeout: Duration,
}

impl ConsumerGroup {
    /// Create a consumer group that begins reading at `start_seq`.
    pub fn new(name: String, start_seq: u64, idle_timeout: Duration) -> Self {
        Self {
            name,
            consumers: HashMap::new(),
            last_delivered_seq: start_seq,
            idle_timeout,
        }
    }
}

/// Outcome of a [`ConsumerGroupManager::read_group`] call.
#[derive(Debug)]
pub struct GroupReadResult {
    /// Events delivered to the consumer in this batch.
    pub events: Vec<CdcEvent>,
    /// `true` when the ring evicted events before they could be delivered,
    /// indicating the consumer has fallen behind.
    pub gap: bool,
}

/// Snapshot of a single pending entry, used for inspection.
#[derive(Debug, Clone)]
pub struct PendingSummary {
    /// Sequence number of the pending event.
    pub seq: u64,
    /// Name of the consumer that currently owns this entry.
    pub consumer: String,
    /// Milliseconds elapsed since the most recent delivery.
    pub idle_ms: u64,
    /// Total number of delivery attempts.
    pub delivery_count: u32,
}

/// Errors returned by [`ConsumerGroupManager`] operations.
#[derive(Debug, thiserror::Error)]
pub enum ConsumerGroupError {
    /// Attempted to create a group whose name is already taken.
    #[error("BUSYGROUP consumer group name already exists")]
    GroupExists,
    /// Referenced a group name that does not exist.
    #[error("NOGROUP no such consumer group '{0}'")]
    NoGroup(String),
    /// The backing CDC ring buffer is unavailable.
    #[error("ERR CDC ring not available")]
    NoRing,
}

/// Owns all consumer groups for a single CDC ring.
///
/// Provides the full group lifecycle: creation, per-consumer reads,
/// acknowledgement, pending-entry inspection, ownership transfer (claim),
/// and gap detection.
pub struct ConsumerGroupManager {
    groups: HashMap<String, ConsumerGroup>,
    default_idle_timeout: Duration,
}

impl ConsumerGroupManager {
    /// Create a manager whose newly created groups use `default_idle_timeout`
    /// for pending-entry redelivery.
    pub fn new(default_idle_timeout: Duration) -> Self {
        Self {
            groups: HashMap::new(),
            default_idle_timeout,
        }
    }

    /// Create a consumer group that begins delivering events at `start_seq`.
    ///
    /// Returns [`ConsumerGroupError::GroupExists`] if `name` is already taken.
    pub fn create_group(&mut self, name: &str, start_seq: u64) -> Result<(), ConsumerGroupError> {
        if self.groups.contains_key(name) {
            return Err(ConsumerGroupError::GroupExists);
        }
        self.groups.insert(
            name.to_string(),
            ConsumerGroup::new(name.to_string(), start_seq, self.default_idle_timeout),
        );
        Ok(())
    }

    /// Deliver up to `count` events to `consumer_name` within `group_name`.
    ///
    /// Before reading fresh events from the ring, timed-out pending entries
    /// owned by *other* consumers are reclaimed and delivered first. The
    /// consumer is auto-created if it does not already exist.
    pub fn read_group(
        &mut self,
        ring: &CdcRing,
        group_name: &str,
        consumer_name: &str,
        count: usize,
    ) -> Result<GroupReadResult, ConsumerGroupError> {
        let group = self
            .groups
            .get_mut(group_name)
            .ok_or_else(|| ConsumerGroupError::NoGroup(group_name.to_string()))?;

        let now = Instant::now();

        if !group.consumers.contains_key(consumer_name) {
            group.consumers.insert(
                consumer_name.to_string(),
                ConsumerState::new(group.last_delivered_seq),
            );
        }

        let mut delivered = Vec::new();
        let mut gap = false;

        let idle_timeout = group.idle_timeout;
        let timed_out = collect_timed_out_entries(group, consumer_name, idle_timeout, now, count);

        for (seq, original_consumer) in &timed_out {
            if let Some(event) = ring.get(*seq) {
                delivered.push(event.clone());

                if let Some(consumer_state) = group.consumers.get_mut(original_consumer.as_str()) {
                    consumer_state.pending.retain(|p| p.event_seq != *seq);
                }
            }
        }

        let consumer = group
            .consumers
            .get_mut(consumer_name)
            .ok_or_else(|| ConsumerGroupError::NoGroup(group_name.to_string()))?;

        for (seq, _) in &timed_out {
            consumer.pending.push_back(PendingEntry {
                event_seq: *seq,
                delivery_count: 2,
                first_delivery: now,
                last_delivery: now,
            });
        }

        let remaining = count.saturating_sub(delivered.len());
        if remaining > 0 {
            let read_result = ring.read(group.last_delivered_seq, remaining);
            if read_result.gap {
                gap = true;
                consumer.needs_resync = true;
            }

            for event in &read_result.events {
                consumer.pending.push_back(PendingEntry {
                    event_seq: event.seq,
                    delivery_count: 1,
                    first_delivery: now,
                    last_delivery: now,
                });
                delivered.push(event.clone());
            }

            group.last_delivered_seq = read_result.next_seq;
        }

        Ok(GroupReadResult {
            events: delivered,
            gap,
        })
    }

    /// Acknowledge one or more events by sequence number.
    ///
    /// Acknowledged entries are removed from every consumer's pending list
    /// within the group. Returns the total number of entries removed.
    pub fn ack(&mut self, group_name: &str, seqs: &[u64]) -> Result<usize, ConsumerGroupError> {
        let group = self
            .groups
            .get_mut(group_name)
            .ok_or_else(|| ConsumerGroupError::NoGroup(group_name.to_string()))?;

        let mut acked = 0;
        for consumer in group.consumers.values_mut() {
            let before = consumer.pending.len();
            consumer.pending.retain(|p| !seqs.contains(&p.event_seq));
            let removed = before - consumer.pending.len();
            acked += removed;

            for &seq in seqs {
                if seq > consumer.last_acked_seq {
                    consumer.last_acked_seq = seq;
                }
            }
        }

        Ok(acked)
    }

    /// Return a snapshot of every pending entry across all consumers in the group,
    /// sorted by sequence number.
    pub fn pending(&self, group_name: &str) -> Result<Vec<PendingSummary>, ConsumerGroupError> {
        let group = self
            .groups
            .get(group_name)
            .ok_or_else(|| ConsumerGroupError::NoGroup(group_name.to_string()))?;

        let now = Instant::now();
        let mut result = Vec::new();

        for (consumer_name, state) in &group.consumers {
            for entry in &state.pending {
                let idle = now.duration_since(entry.last_delivery);
                result.push(PendingSummary {
                    seq: entry.event_seq,
                    consumer: consumer_name.clone(),
                    idle_ms: idle.as_millis() as u64,
                    delivery_count: entry.delivery_count,
                });
            }
        }

        result.sort_by_key(|p| p.seq);
        Ok(result)
    }

    /// Transfer ownership of pending entries to `target_consumer`.
    ///
    /// Only entries whose idle time meets or exceeds `min_idle` are
    /// transferred. Returns the sequence numbers that were claimed.
    pub fn claim(
        &mut self,
        group_name: &str,
        target_consumer: &str,
        min_idle: Duration,
        seqs: &[u64],
    ) -> Result<Vec<u64>, ConsumerGroupError> {
        let group = self
            .groups
            .get_mut(group_name)
            .ok_or_else(|| ConsumerGroupError::NoGroup(group_name.to_string()))?;

        let now = Instant::now();
        let mut claimed_entries: Vec<(u64, PendingEntry)> = Vec::new();

        for consumer in group.consumers.values_mut() {
            let mut remaining = VecDeque::new();
            for entry in consumer.pending.drain(..) {
                let idle = now.duration_since(entry.last_delivery);
                if seqs.contains(&entry.event_seq) && idle >= min_idle {
                    claimed_entries.push((
                        entry.event_seq,
                        PendingEntry {
                            event_seq: entry.event_seq,
                            delivery_count: entry.delivery_count + 1,
                            first_delivery: entry.first_delivery,
                            last_delivery: now,
                        },
                    ));
                } else {
                    remaining.push_back(entry);
                }
            }
            consumer.pending = remaining;
        }

        if !group.consumers.contains_key(target_consumer) {
            group.consumers.insert(
                target_consumer.to_string(),
                ConsumerState::new(group.last_delivered_seq),
            );
        }

        let claimed_seqs: Vec<u64> = claimed_entries.iter().map(|(seq, _)| *seq).collect();

        let target = group
            .consumers
            .get_mut(target_consumer)
            .ok_or_else(|| ConsumerGroupError::NoGroup(group_name.to_string()))?;

        for (_, entry) in claimed_entries {
            target.pending.push_back(entry);
        }

        Ok(claimed_seqs)
    }

    /// Return `true` if a group with the given name exists.
    pub fn has_group(&self, name: &str) -> bool {
        self.groups.contains_key(name)
    }

    /// Return the number of registered groups.
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Check whether `consumer_name` in `group_name` has fallen behind the ring.
    ///
    /// Returns `true` if the consumer's last acknowledged sequence has been
    /// evicted from the ring, or if the consumer was already marked for resync.
    pub fn check_gap(
        &mut self,
        ring: &CdcRing,
        group_name: &str,
        consumer_name: &str,
    ) -> Result<bool, ConsumerGroupError> {
        let group = self
            .groups
            .get_mut(group_name)
            .ok_or_else(|| ConsumerGroupError::NoGroup(group_name.to_string()))?;

        let consumer = match group.consumers.get_mut(consumer_name) {
            Some(c) => c,
            None => return Ok(false),
        };

        if ring.start_seq() > consumer.last_acked_seq {
            consumer.needs_resync = true;
            return Ok(true);
        }

        Ok(consumer.needs_resync)
    }
}

impl Default for ConsumerGroupManager {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

fn collect_timed_out_entries(
    group: &ConsumerGroup,
    exclude_consumer: &str,
    idle_timeout: Duration,
    now: Instant,
    max: usize,
) -> Vec<(u64, String)> {
    let mut timed_out = Vec::new();
    for (name, state) in &group.consumers {
        if name == exclude_consumer {
            continue;
        }
        for entry in &state.pending {
            if now.duration_since(entry.last_delivery) >= idle_timeout {
                timed_out.push((entry.event_seq, name.clone()));
                if timed_out.len() >= max {
                    return timed_out;
                }
            }
        }
    }
    timed_out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ring::{CdcOp, CdcRing};

    fn make_ring(n: usize) -> CdcRing {
        let mut ring = CdcRing::new(100);
        for i in 0..n {
            ring.push(
                CdcOp::Set,
                format!("key:{}", i).into_bytes(),
                Some(format!("val:{}", i).into_bytes()),
                i as u64 * 100,
            );
        }
        ring
    }

    #[test]
    fn test_create_group() {
        let mut mgr = ConsumerGroupManager::default();
        assert!(mgr.create_group("mygroup", 0).is_ok());
        assert!(mgr.has_group("mygroup"));
        assert_eq!(mgr.group_count(), 1);
    }

    #[test]
    fn test_create_duplicate_group() {
        let mut mgr = ConsumerGroupManager::default();
        mgr.create_group("mygroup", 0).unwrap();
        let result = mgr.create_group("mygroup", 0);
        assert!(matches!(result, Err(ConsumerGroupError::GroupExists)));
    }

    #[test]
    fn test_read_group_basic() {
        let ring = make_ring(5);
        let mut mgr = ConsumerGroupManager::default();
        mgr.create_group("g1", 0).unwrap();

        let result = mgr.read_group(&ring, "g1", "c1", 3).unwrap();
        assert_eq!(result.events.len(), 3);
        assert!(!result.gap);
        assert_eq!(result.events[0].seq, 0);
        assert_eq!(result.events[2].seq, 2);

        let result = mgr.read_group(&ring, "g1", "c1", 10).unwrap();
        assert_eq!(result.events.len(), 2);
        assert_eq!(result.events[0].seq, 3);

        let result = mgr.read_group(&ring, "g1", "c1", 10).unwrap();
        assert_eq!(result.events.len(), 0);
    }

    #[test]
    fn test_ack_removes_pending() {
        let ring = make_ring(5);
        let mut mgr = ConsumerGroupManager::default();
        mgr.create_group("g1", 0).unwrap();

        mgr.read_group(&ring, "g1", "c1", 3).unwrap();
        let pending = mgr.pending("g1").unwrap();
        assert_eq!(pending.len(), 3);

        let acked = mgr.ack("g1", &[0, 1]).unwrap();
        assert_eq!(acked, 2);

        let pending = mgr.pending("g1").unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].seq, 2);
    }

    #[test]
    fn test_ack_nonexistent_group() {
        let mut mgr = ConsumerGroupManager::default();
        let result = mgr.ack("nogroup", &[0]);
        assert!(matches!(result, Err(ConsumerGroupError::NoGroup(_))));
    }

    #[test]
    fn test_pending_lists_all_consumers() {
        let ring = make_ring(10);
        let mut mgr = ConsumerGroupManager::default();
        mgr.create_group("g1", 0).unwrap();

        mgr.read_group(&ring, "g1", "c1", 3).unwrap();
        mgr.read_group(&ring, "g1", "c2", 3).unwrap();

        let pending = mgr.pending("g1").unwrap();
        assert_eq!(pending.len(), 6);
    }

    #[test]
    fn test_multiple_consumers_get_different_events() {
        let ring = make_ring(6);
        let mut mgr = ConsumerGroupManager::default();
        mgr.create_group("g1", 0).unwrap();

        let r1 = mgr.read_group(&ring, "g1", "c1", 3).unwrap();
        assert_eq!(r1.events.len(), 3);
        assert_eq!(r1.events[0].seq, 0);

        let r2 = mgr.read_group(&ring, "g1", "c2", 3).unwrap();
        assert_eq!(r2.events.len(), 3);
        assert_eq!(r2.events[0].seq, 3);
    }

    #[test]
    fn test_gap_detection() {
        let mut ring = CdcRing::new(4);
        for i in 0..10 {
            ring.push(CdcOp::Set, format!("k{}", i).into_bytes(), None, i as u64);
        }

        let mut mgr = ConsumerGroupManager::default();
        mgr.create_group("g1", 0).unwrap();

        let result = mgr.read_group(&ring, "g1", "c1", 10).unwrap();
        assert!(result.gap);

        let has_gap = mgr.check_gap(&ring, "g1", "c1").unwrap();
        assert!(has_gap);
    }

    #[test]
    fn test_claim_transfers_entries() {
        let ring = make_ring(5);
        let mut mgr = ConsumerGroupManager::new(Duration::from_millis(0));
        mgr.create_group("g1", 0).unwrap();

        mgr.read_group(&ring, "g1", "c1", 3).unwrap();

        let claimed = mgr
            .claim("g1", "c2", Duration::from_millis(0), &[0, 1])
            .unwrap();
        assert_eq!(claimed.len(), 2);

        let pending = mgr.pending("g1").unwrap();
        let c1_pending: Vec<_> = pending.iter().filter(|p| p.consumer == "c1").collect();
        let c2_pending: Vec<_> = pending.iter().filter(|p| p.consumer == "c2").collect();
        assert_eq!(c1_pending.len(), 1);
        assert_eq!(c2_pending.len(), 2);
    }

    #[test]
    fn test_claim_respects_min_idle() {
        let ring = make_ring(5);
        let mut mgr = ConsumerGroupManager::default();
        mgr.create_group("g1", 0).unwrap();

        mgr.read_group(&ring, "g1", "c1", 3).unwrap();

        let claimed = mgr
            .claim("g1", "c2", Duration::from_secs(9999), &[0, 1])
            .unwrap();
        assert_eq!(claimed.len(), 0);
    }

    #[test]
    fn test_read_nonexistent_group() {
        let ring = make_ring(5);
        let mut mgr = ConsumerGroupManager::default();
        let result = mgr.read_group(&ring, "nogroup", "c1", 5);
        assert!(matches!(result, Err(ConsumerGroupError::NoGroup(_))));
    }

    #[test]
    fn test_group_start_seq() {
        let ring = make_ring(10);
        let mut mgr = ConsumerGroupManager::default();
        mgr.create_group("g1", 5).unwrap();

        let result = mgr.read_group(&ring, "g1", "c1", 10).unwrap();
        assert_eq!(result.events.len(), 5);
        assert_eq!(result.events[0].seq, 5);
    }

    #[test]
    fn test_ack_advances_last_acked() {
        let ring = make_ring(5);
        let mut mgr = ConsumerGroupManager::default();
        mgr.create_group("g1", 0).unwrap();

        mgr.read_group(&ring, "g1", "c1", 5).unwrap();
        mgr.ack("g1", &[0, 1, 4]).unwrap();

        let pending = mgr.pending("g1").unwrap();
        assert_eq!(pending.len(), 2);

        let seqs: Vec<u64> = pending.iter().map(|p| p.seq).collect();
        assert!(seqs.contains(&2));
        assert!(seqs.contains(&3));
    }
}
