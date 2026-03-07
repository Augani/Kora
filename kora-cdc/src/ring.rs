//! Per-shard CDC ring buffer.
//!
//! Captures every mutation in a fixed-size circular buffer. Old events are
//! overwritten when the buffer wraps. Consumers track their position via
//! a sequence number.

/// The type of mutation captured.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CdcOp {
    /// Key was set/updated.
    Set,
    /// Key was deleted.
    Del,
    /// Key was expired.
    Expire,
    /// Hash field was set.
    HSet,
    /// Value pushed to list.
    LPush,
    /// Value pushed to list (right).
    RPush,
    /// Member added to set.
    SAdd,
    /// Database was flushed.
    FlushDb,
}

/// A single CDC event.
#[derive(Debug, Clone)]
pub struct CdcEvent {
    /// Monotonically increasing sequence number.
    pub seq: u64,
    /// Timestamp (milliseconds since shard epoch).
    pub timestamp_ms: u64,
    /// The operation type.
    pub op: CdcOp,
    /// The affected key (empty for FlushDb).
    pub key: Vec<u8>,
    /// The new value, if applicable.
    pub value: Option<Vec<u8>>,
}

/// A fixed-size ring buffer for CDC events.
pub struct CdcRing {
    buffer: Vec<Option<CdcEvent>>,
    capacity: usize,
    write_seq: u64,
    start_seq: u64,
}

impl CdcRing {
    /// Create a new ring buffer with the given capacity.
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

    /// Push an event into the ring buffer.
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

        // Update start_seq if we've wrapped
        if self.write_seq > self.capacity as u64 {
            self.start_seq = self.write_seq - self.capacity as u64;
        }
    }

    /// Get the current write sequence number (next seq to be written).
    pub fn write_seq(&self) -> u64 {
        self.write_seq
    }

    /// Get the earliest available sequence number.
    pub fn start_seq(&self) -> u64 {
        self.start_seq
    }

    /// Get the number of events currently in the buffer.
    pub fn len(&self) -> usize {
        (self.write_seq - self.start_seq) as usize
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.write_seq == 0
    }

    /// Read events starting from the given sequence number.
    ///
    /// Returns up to `limit` events and the next sequence to read from.
    /// If `from_seq` is behind the buffer (events were overwritten), starts
    /// from `start_seq` and returns a gap indicator.
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

    /// Get a single event by sequence number, if available.
    pub fn get(&self, seq: u64) -> Option<&CdcEvent> {
        if seq < self.start_seq || seq >= self.write_seq {
            return None;
        }
        let idx = (seq as usize) % self.capacity;
        self.buffer[idx].as_ref()
    }
}

/// Result of reading from the CDC ring buffer.
#[derive(Debug)]
pub struct CdcReadResult {
    /// The events read.
    pub events: Vec<CdcEvent>,
    /// The next sequence number to read from.
    pub next_seq: u64,
    /// True if events were lost (consumer fell behind).
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
