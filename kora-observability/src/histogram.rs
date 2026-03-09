//! HDR histogram wrappers for per-command latency distributions.
//!
//! Wraps the [`hdrhistogram`] crate to provide a fixed-size array of
//! histograms — one per command type index (0..31). Each histogram is
//! protected by a [`parking_lot::Mutex`] for concurrent recording from
//! multiple shard worker threads.
//!
//! Default range is 1 microsecond to 60 seconds with 3 significant digits
//! of precision, covering the full spectrum of cache operation latencies.

use hdrhistogram::Histogram;
use parking_lot::Mutex;

const NUM_COMMANDS: usize = 32;
const MIN_VALUE: u64 = 1_000;
const MAX_VALUE: u64 = 60_000_000_000;
const SIG_FIGS: u8 = 3;

/// Per-command HDR histograms for latency distribution tracking.
///
/// Wraps a fixed-size array of histograms (one per command type index).
/// Range: 1us to 60s with 3 significant digits of precision.
pub struct CommandHistograms {
    histograms: [Mutex<Histogram<u64>>; NUM_COMMANDS],
}

impl CommandHistograms {
    /// Create a new set of command histograms.
    pub fn new() -> Self {
        Self {
            histograms: std::array::from_fn(|_| {
                Mutex::new(
                    Histogram::new_with_bounds(MIN_VALUE, MAX_VALUE, SIG_FIGS)
                        .or_else(|_| Histogram::new(SIG_FIGS))
                        .or_else(|_| Histogram::new_with_max(MAX_VALUE, SIG_FIGS))
                        .expect("at least one histogram constructor must succeed"),
                )
            }),
        }
    }

    /// Record a latency sample for a command type.
    ///
    /// `cmd_id` is the command type index (0..31).
    /// `duration_ns` is the duration in nanoseconds.
    pub fn record(&self, cmd_id: usize, duration_ns: u64) {
        if cmd_id >= NUM_COMMANDS {
            return;
        }
        let clamped = duration_ns.clamp(MIN_VALUE, MAX_VALUE);
        let mut hist = self.histograms[cmd_id].lock();
        let _ = hist.record(clamped);
    }

    /// Get a percentile value for a command type.
    ///
    /// `p` is in the range 0.0..100.0 (e.g. 99.0 for P99).
    /// Returns the value in nanoseconds, or 0 if no samples recorded.
    pub fn percentile(&self, cmd_id: usize, p: f64) -> u64 {
        if cmd_id >= NUM_COMMANDS {
            return 0;
        }
        let hist = self.histograms[cmd_id].lock();
        if hist.is_empty() {
            return 0;
        }
        hist.value_at_percentile(p)
    }

    /// Get the total count of recorded samples for a command type.
    pub fn count(&self, cmd_id: usize) -> u64 {
        if cmd_id >= NUM_COMMANDS {
            return 0;
        }
        self.histograms[cmd_id].lock().len()
    }

    /// Reset all histograms.
    pub fn reset(&self) {
        for hist in &self.histograms {
            hist.lock().reset();
        }
    }
}

impl Default for CommandHistograms {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_percentile() {
        let histograms = CommandHistograms::new();
        for i in 0..1000 {
            histograms.record(0, (i + 1) * 1_000);
        }
        let p50 = histograms.percentile(0, 50.0);
        let p99 = histograms.percentile(0, 99.0);
        assert!(p50 > 0);
        assert!(p99 >= p50);
        assert_eq!(histograms.count(0), 1000);
    }

    #[test]
    fn test_empty_percentile() {
        let histograms = CommandHistograms::new();
        assert_eq!(histograms.percentile(0, 50.0), 0);
        assert_eq!(histograms.count(0), 0);
    }

    #[test]
    fn test_out_of_bounds_cmd_id() {
        let histograms = CommandHistograms::new();
        histograms.record(99, 1_000_000);
        assert_eq!(histograms.percentile(99, 50.0), 0);
        assert_eq!(histograms.count(99), 0);
    }

    #[test]
    fn test_reset() {
        let histograms = CommandHistograms::new();
        histograms.record(5, 100_000);
        assert_eq!(histograms.count(5), 1);
        histograms.reset();
        assert_eq!(histograms.count(5), 0);
    }

    #[test]
    fn test_clamp_values() {
        let histograms = CommandHistograms::new();
        histograms.record(0, 1);
        histograms.record(0, 100_000_000_000);
        assert_eq!(histograms.count(0), 2);
    }

    #[test]
    fn test_concurrent_recording() {
        use std::sync::Arc;
        let histograms = Arc::new(CommandHistograms::new());
        let mut handles = Vec::new();
        for _ in 0..4 {
            let h = histograms.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..500 {
                    h.record(0, (i + 1) * 1_000);
                }
            }));
        }
        for handle in handles {
            handle.join().expect("thread join");
        }
        assert_eq!(histograms.count(0), 2000);
    }
}
