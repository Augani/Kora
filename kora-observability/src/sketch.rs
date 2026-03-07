//! Count-Min Sketch for hot key detection.
//!
//! A probabilistic data structure that uses ~4KB of memory to approximate
//! key access frequencies with controlled error bounds.

use std::hash::{Hash, Hasher};

/// A Count-Min Sketch for estimating item frequencies.
///
/// Uses multiple hash functions and a 2D array of counters.
/// Memory usage: `width * depth * 8` bytes.
pub struct CountMinSketch {
    counters: Vec<Vec<u64>>,
    width: usize,
    depth: usize,
    seeds: Vec<u64>,
}

impl CountMinSketch {
    /// Create a new Count-Min Sketch.
    ///
    /// - `width`: number of columns (higher = more accurate, more memory)
    /// - `depth`: number of hash functions/rows (higher = less false positive rate)
    ///
    /// A width of 64 and depth of 4 uses 2KB and is sufficient for hot key detection.
    pub fn new(width: usize, depth: usize) -> Self {
        let seeds: Vec<u64> = (0..depth)
            .map(|i| {
                0x517cc1b727220a95u64.wrapping_add((i as u64).wrapping_mul(0x6c62272e07bb0142))
            })
            .collect();
        Self {
            counters: vec![vec![0u64; width]; depth],
            width,
            depth,
            seeds,
        }
    }

    /// Create a sketch with default parameters (~2KB, good for top-K detection).
    pub fn default_hot_key() -> Self {
        Self::new(64, 4)
    }

    /// Increment the count for a key.
    pub fn increment(&mut self, key: &[u8]) {
        for i in 0..self.depth {
            let idx = self.hash_index(key, i);
            self.counters[i][idx] = self.counters[i][idx].saturating_add(1);
        }
    }

    /// Increment by a specific amount.
    pub fn increment_by(&mut self, key: &[u8], count: u64) {
        for i in 0..self.depth {
            let idx = self.hash_index(key, i);
            self.counters[i][idx] = self.counters[i][idx].saturating_add(count);
        }
    }

    /// Estimate the count for a key.
    ///
    /// Returns the minimum count across all hash functions (conservative estimate).
    pub fn estimate(&self, key: &[u8]) -> u64 {
        (0..self.depth)
            .map(|i| {
                let idx = self.hash_index(key, i);
                self.counters[i][idx]
            })
            .min()
            .unwrap_or(0)
    }

    /// Reset all counters to zero.
    pub fn reset(&mut self) {
        for row in &mut self.counters {
            for counter in row.iter_mut() {
                *counter = 0;
            }
        }
    }

    /// Decay all counters by dividing by 2 (for periodic frequency decay).
    pub fn decay(&mut self) {
        for row in &mut self.counters {
            for counter in row.iter_mut() {
                *counter /= 2;
            }
        }
    }

    fn hash_index(&self, key: &[u8], row: usize) -> usize {
        let mut hasher = SimpleHasher::new(self.seeds[row]);
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.width
    }
}

/// A simple non-cryptographic hasher for the sketch.
struct SimpleHasher {
    state: u64,
}

impl SimpleHasher {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }
}

impl Hasher for SimpleHasher {
    fn finish(&self) -> u64 {
        // Finalize with avalanche
        let mut h = self.state;
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51afd7ed558ccd);
        h ^= h >> 33;
        h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
        h ^= h >> 33;
        h
    }

    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.state = self.state.wrapping_mul(31).wrapping_add(b as u64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_counting() {
        let mut sketch = CountMinSketch::new(128, 4);
        sketch.increment(b"foo");
        sketch.increment(b"foo");
        sketch.increment(b"foo");
        sketch.increment(b"bar");

        assert_eq!(sketch.estimate(b"foo"), 3);
        assert_eq!(sketch.estimate(b"bar"), 1);
    }

    #[test]
    fn test_unseen_key() {
        let sketch = CountMinSketch::new(128, 4);
        assert_eq!(sketch.estimate(b"never_seen"), 0);
    }

    #[test]
    fn test_overestimation_not_underestimation() {
        let mut sketch = CountMinSketch::new(16, 4); // small for more collisions
        sketch.increment_by(b"hot_key", 1000);

        // Should never underestimate
        assert!(sketch.estimate(b"hot_key") >= 1000);
    }

    #[test]
    fn test_reset() {
        let mut sketch = CountMinSketch::new(64, 4);
        sketch.increment_by(b"key", 100);
        assert!(sketch.estimate(b"key") > 0);

        sketch.reset();
        assert_eq!(sketch.estimate(b"key"), 0);
    }

    #[test]
    fn test_decay() {
        let mut sketch = CountMinSketch::new(64, 4);
        sketch.increment_by(b"key", 100);

        sketch.decay();
        assert_eq!(sketch.estimate(b"key"), 50);
    }

    #[test]
    fn test_hot_key_detection() {
        let mut sketch = CountMinSketch::default_hot_key();

        // Simulate: one hot key, many cold keys
        for _ in 0..10000 {
            sketch.increment(b"hot_key");
        }
        for i in 0..1000u32 {
            sketch.increment(&i.to_le_bytes());
        }

        // Hot key should have much higher estimate
        let hot = sketch.estimate(b"hot_key");
        let avg_cold: u64 = (0..1000u32)
            .map(|i| sketch.estimate(&i.to_le_bytes()))
            .sum::<u64>()
            / 1000;

        assert!(
            hot > avg_cold * 5,
            "hot={} should be >> avg_cold={}",
            hot,
            avg_cold
        );
    }

    #[test]
    fn test_different_keys_distinct() {
        let mut sketch = CountMinSketch::new(256, 4);
        sketch.increment_by(b"alpha", 100);
        sketch.increment_by(b"beta", 200);

        // With large enough width, these should be reasonably accurate
        let a = sketch.estimate(b"alpha");
        let b = sketch.estimate(b"beta");
        assert!(a >= 100);
        assert!(b >= 200);
        // Beta should be higher than alpha
        assert!(b > a);
    }
}
