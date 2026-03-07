//! CDC consumer groups with ack tracking and redelivery.
//!
//! Modeled after Redis Streams consumer groups (XGROUP, XREADGROUP, XACK, XPENDING).
//! Each consumer group maintains independent cursors per consumer, tracks pending
//! (delivered but unacknowledged) entries, and supports timeout-based redelivery.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use crate::ring::{CdcEvent, CdcRing};

/// A pending entry that has been delivered but not yet acknowledged.
#[derive(Debug, Clone)]
pub struct PendingEntry {
    /// The sequence number of the event.
    pub event_seq: u64,
    /// Number of times this entry has been delivered.
    pub delivery_count: u32,
    /// When this entry was first delivered.
    pub first_delivery: Instant,
    /// When this entry was last delivered.
    pub last_delivery: Instant,
}

/// Per-consumer state within a group.
#[derive(Debug)]
pub struct ConsumerState {
    /// Last acknowledged sequence number.
    pub last_acked_seq: u64,
    /// Entries delivered but not yet acknowledged, keyed by sequence.
    pub pending: VecDeque<PendingEntry>,
    /// Whether this consumer has fallen behind the ring buffer.
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

/// A named consumer group tracking multiple consumers.
#[derive(Debug)]
pub struct ConsumerGroup {
    /// Group name.
    pub name: String,
    /// Per-consumer tracking state.
    pub consumers: HashMap<String, ConsumerState>,
    /// The group-level last delivered sequence (high watermark).
    pub last_delivered_seq: u64,
    /// Idle timeout after which pending entries become reclaimable.
    pub idle_timeout: Duration,
}

impl ConsumerGroup {
    /// Create a new consumer group starting at the given sequence.
    pub fn new(name: String, start_seq: u64, idle_timeout: Duration) -> Self {
        Self {
            name,
            consumers: HashMap::new(),
            last_delivered_seq: start_seq,
            idle_timeout,
        }
    }
}

/// Result of a group read operation.
#[derive(Debug)]
pub struct GroupReadResult {
    /// Events delivered to the consumer.
    pub events: Vec<CdcEvent>,
    /// Whether a gap was detected (consumer fell behind ring buffer).
    pub gap: bool,
}

/// Summary of a single pending entry for the PENDING command.
#[derive(Debug, Clone)]
pub struct PendingSummary {
    /// Sequence number.
    pub seq: u64,
    /// Consumer that owns this entry.
    pub consumer: String,
    /// Milliseconds since first delivery.
    pub idle_ms: u64,
    /// Number of delivery attempts.
    pub delivery_count: u32,
}

/// Error type for consumer group operations.
#[derive(Debug, thiserror::Error)]
pub enum ConsumerGroupError {
    /// The named group already exists.
    #[error("BUSYGROUP consumer group name already exists")]
    GroupExists,
    /// The named group was not found.
    #[error("NOGROUP no such consumer group '{0}'")]
    NoGroup(String),
    /// The ring buffer is not available.
    #[error("ERR CDC ring not available")]
    NoRing,
}

/// Manages multiple consumer groups for a single CDC ring.
pub struct ConsumerGroupManager {
    groups: HashMap<String, ConsumerGroup>,
    default_idle_timeout: Duration,
}

impl ConsumerGroupManager {
    /// Create a new manager with the given default idle timeout.
    pub fn new(default_idle_timeout: Duration) -> Self {
        Self {
            groups: HashMap::new(),
            default_idle_timeout,
        }
    }

    /// Create a new consumer group starting at the given sequence.
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

    /// Read the next `count` undelivered events for a consumer in a group.
    ///
    /// New events are read from the ring starting at the group's last delivered
    /// sequence. Timed-out pending entries from other consumers are also redelivered.
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

    /// Acknowledge events, removing them from the consumer's pending list.
    ///
    /// Returns the number of entries actually acknowledged.
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

    /// List all pending entries across all consumers in a group.
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

    /// Transfer pending entries to another consumer (claim).
    ///
    /// Only entries idle for at least `min_idle` are transferred.
    /// Returns the sequences that were actually claimed.
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

    /// Check if a group exists.
    pub fn has_group(&self, name: &str) -> bool {
        self.groups.contains_key(name)
    }

    /// Get the number of groups.
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Check gap detection: returns true if consumer has fallen behind the ring.
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
