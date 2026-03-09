//! Per-shard CDC ring buffer.
//!
//! Each Kōra shard owns a [`CdcRing`] that records every mutation as a
//! [`CdcEvent`] with a monotonically increasing sequence number. The ring has
//! a fixed capacity: once full, the oldest events are silently overwritten.
//! Consumers detect lost events through the gap flag returned by [`CdcRing::read`].

/// Discriminant for the kind of mutation recorded in a [`CdcEvent`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CdcOp {
    /// A string key was created or overwritten.
    Set,
    /// A key was explicitly deleted.
    Del,
    /// A key was removed by TTL expiration.
    Expire,
    /// A field was set inside a hash.
    HSet,
    /// A value was pushed to the head of a list.
    LPush,
    /// A value was pushed to the tail of a list.
    RPush,
    /// A member was added to a set.
    SAdd,
    /// The entire database was flushed.
    FlushDb,
}

/// A single captured mutation event.
///
/// Every write operation in a shard produces one `CdcEvent`. Events are
/// immutable once written to the ring and are identified by their unique,
/// monotonically increasing [`seq`](CdcEvent::seq).
#[derive(Debug, Clone)]
pub struct CdcEvent {
    /// Unique, monotonically increasing sequence number assigned at write time.
    pub seq: u64,
    /// Wall-clock timestamp in milliseconds provided by the caller.
    pub timestamp_ms: u64,
    /// The kind of mutation that occurred.
    pub op: CdcOp,
    /// The affected key. Empty for [`CdcOp::FlushDb`].
    pub key: Vec<u8>,
    /// The new value, when the operation carries one (e.g. `Set`, `HSet`).
    pub value: Option<Vec<u8>>,
}

/// Fixed-capacity circular buffer for CDC events.
///
/// Events are appended with [`push`](CdcRing::push) and assigned a
/// monotonically increasing sequence number. When the buffer is full the
/// oldest slot is silently overwritten. Consumers read batches via
/// [`read`](CdcRing::read) and are informed through
/// [`CdcReadResult::gap`] when events they have not yet consumed were
/// evicted.
pub struct CdcRing {
    buffer: Vec<Option<CdcEvent>>,
    capacity: usize,
    write_seq: u64,
    start_seq: u64,
}

impl CdcRing {
    /// Create a new ring buffer that can hold up to `capacity` events.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is zero.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "CDC ring capacity must be > 0");
        let mut buffer = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buffer.push(None);
        }
        Self {
            buffer,
            capacity,
            write_seq: 0,
            start_seq: 0,
        }
    }

    /// Append a mutation event to the ring, assigning it the next sequence number.
    ///
    /// If the buffer is full the oldest event is overwritten and
    /// [`start_seq`](CdcRing::start_seq) advances accordingly.
    pub fn push(&mut self, op: CdcOp, key: Vec<u8>, value: Option<Vec<u8>>, timestamp_ms: u64) {
        let seq = self.write_seq;
        let idx = (seq as usize) % self.capacity;

        self.buffer[idx] = Some(CdcEvent {
            seq,
            timestamp_ms,
            op,
            key,
            value,
        });

        self.write_seq += 1;

        if self.write_seq > self.capacity as u64 {
            self.start_seq = self.write_seq - self.capacity as u64;
        }
    }

    /// Return the sequence number that will be assigned to the next pushed event.
    pub fn write_seq(&self) -> u64 {
        self.write_seq
    }

    /// Return the earliest sequence number still available in the buffer.
    pub fn start_seq(&self) -> u64 {
        self.start_seq
    }

    /// Return the number of events currently retained in the buffer.
    pub fn len(&self) -> usize {
        (self.write_seq - self.start_seq) as usize
    }

    /// Return `true` if no events have been pushed yet.
    pub fn is_empty(&self) -> bool {
        self.write_seq == 0
    }

    /// Read up to `limit` events starting at `from_seq`.
    ///
    /// If `from_seq` has already been evicted the read begins at the
    /// earliest available sequence and [`CdcReadResult::gap`] is set to
    /// `true`, signalling that the consumer missed events.
    pub fn read(&self, from_seq: u64, limit: usize) -> CdcReadResult {
        if from_seq >= self.write_seq {
            return CdcReadResult {
                events: vec![],
                next_seq: self.write_seq,
                gap: false,
            };
        }

        let actual_start = if from_seq < self.start_seq {
            self.start_seq
        } else {
            from_seq
        };

        let gap = from_seq < self.start_seq;
        let available = (self.write_seq - actual_start) as usize;
        let count = available.min(limit);

        let mut events = Vec::with_capacity(count);
        for i in 0..count {
            let seq = actual_start + i as u64;
            let idx = (seq as usize) % self.capacity;
            if let Some(ref event) = self.buffer[idx] {
                events.push(event.clone());
            }
        }

        CdcReadResult {
            next_seq: actual_start + count as u64,
            events,
            gap,
        }
    }

    /// Look up a single event by its sequence number.
    ///
    /// Returns `None` if `seq` has been evicted or has not been written yet.
    pub fn get(&self, seq: u64) -> Option<&CdcEvent> {
        if seq < self.start_seq || seq >= self.write_seq {
            return None;
        }
        let idx = (seq as usize) % self.capacity;
        self.buffer[idx].as_ref()
    }
}

/// Outcome of a [`CdcRing::read`] call.
#[derive(Debug)]
pub struct CdcReadResult {
    /// The batch of events returned by this read.
    pub events: Vec<CdcEvent>,
    /// The sequence number the caller should pass to the next read.
    pub next_seq: u64,
    /// `true` when the requested `from_seq` had already been evicted,
    /// meaning the consumer missed one or more events.
    pub gap: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_and_read() {
        let mut ring = CdcRing::new(10);
        ring.push(CdcOp::Set, b"k1".to_vec(), Some(b"v1".to_vec()), 100);
        ring.push(CdcOp::Set, b"k2".to_vec(), Some(b"v2".to_vec()), 200);
        ring.push(CdcOp::Del, b"k1".to_vec(), None, 300);

        assert_eq!(ring.len(), 3);
        assert_eq!(ring.write_seq(), 3);
        assert_eq!(ring.start_seq(), 0);

        let result = ring.read(0, 100);
        assert_eq!(result.events.len(), 3);
        assert!(!result.gap);
        assert_eq!(result.next_seq, 3);
        assert_eq!(result.events[0].op, CdcOp::Set);
        assert_eq!(result.events[0].key, b"k1");
        assert_eq!(result.events[2].op, CdcOp::Del);
    }

    #[test]
    fn test_read_with_limit() {
        let mut ring = CdcRing::new(10);
        for i in 0..5 {
            ring.push(CdcOp::Set, format!("k{}", i).into_bytes(), None, i as u64);
        }

        let result = ring.read(0, 2);
        assert_eq!(result.events.len(), 2);
        assert_eq!(result.next_seq, 2);

        let result = ring.read(2, 2);
        assert_eq!(result.events.len(), 2);
        assert_eq!(result.next_seq, 4);
    }

    #[test]
    fn test_wrapping() {
        let mut ring = CdcRing::new(4);
        for i in 0..10 {
            ring.push(CdcOp::Set, format!("k{}", i).into_bytes(), None, i as u64);
        }

        assert_eq!(ring.len(), 4);
        assert_eq!(ring.start_seq(), 6);
        assert_eq!(ring.write_seq(), 10);

        // Can only read the last 4 events
        let result = ring.read(0, 100);
        assert!(result.gap); // consumer was behind
        assert_eq!(result.events.len(), 4);
        assert_eq!(result.events[0].seq, 6);
        assert_eq!(result.events[3].seq, 9);
    }

    #[test]
    fn test_gap_detection() {
        let mut ring = CdcRing::new(4);
        for i in 0..10 {
            ring.push(CdcOp::Set, format!("k{}", i).into_bytes(), None, i as u64);
        }

        // Consumer at seq 3 has fallen behind (events 3-5 are lost)
        let result = ring.read(3, 100);
        assert!(result.gap);
        assert_eq!(result.events[0].seq, 6); // starts from earliest available
    }

    #[test]
    fn test_read_at_write_head() {
        let mut ring = CdcRing::new(10);
        ring.push(CdcOp::Set, b"k".to_vec(), None, 0);

        let result = ring.read(1, 100);
        assert!(result.events.is_empty());
        assert_eq!(result.next_seq, 1);
    }

    #[test]
    fn test_get_single() {
        let mut ring = CdcRing::new(10);
        ring.push(CdcOp::Set, b"k0".to_vec(), None, 0);
        ring.push(CdcOp::Del, b"k1".to_vec(), None, 1);

        let event = ring.get(0).unwrap();
        assert_eq!(event.op, CdcOp::Set);

        let event = ring.get(1).unwrap();
        assert_eq!(event.op, CdcOp::Del);

        assert!(ring.get(2).is_none());
    }

    #[test]
    fn test_empty_ring() {
        let ring = CdcRing::new(10);
        assert!(ring.is_empty());
        assert_eq!(ring.len(), 0);

        let result = ring.read(0, 100);
        assert!(result.events.is_empty());
    }

    #[test]
    fn test_all_ops() {
        let mut ring = CdcRing::new(100);
        let ops = [
            CdcOp::Set,
            CdcOp::Del,
            CdcOp::Expire,
            CdcOp::HSet,
            CdcOp::LPush,
            CdcOp::RPush,
            CdcOp::SAdd,
            CdcOp::FlushDb,
        ];

        for (i, op) in ops.iter().enumerate() {
            ring.push(op.clone(), format!("k{}", i).into_bytes(), None, i as u64);
        }

        let result = ring.read(0, 100);
        assert_eq!(result.events.len(), 8);
        for (i, event) in result.events.iter().enumerate() {
            assert_eq!(event.op, ops[i]);
        }
    }
}
