//! Per-shard and aggregate statistics.
//!
//! Tracks command counts, latencies, memory usage, byte throughput, and hot
//! key frequencies with near-zero overhead using atomic counters, HDR
//! histograms, and a Count-Min Sketch.
//!
//! [`ShardStats`] is the per-shard collector, designed for concurrent access
//! from connection handlers. [`StatsSnapshot`] captures a point-in-time copy
//! that can be merged across shards for server-wide aggregates.
//! [`CommandTimer`] provides RAII-based latency recording that fires on drop.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::histogram::CommandHistograms;
use crate::sketch::CountMinSketch;

/// Number of command types tracked.
const NUM_COMMANDS: usize = 32;

/// Per-shard statistics collector.
///
/// Aggregates command counts, durations, byte throughput, key counts,
/// memory usage, and hot key frequencies. All counters use relaxed
/// atomics for minimal overhead on the data path.
pub struct ShardStats {
    total_commands: AtomicU64,
    cmd_counts: [AtomicU64; NUM_COMMANDS],
    cmd_durations_ns: [AtomicU64; NUM_COMMANDS],
    hot_key_sketch: parking_lot::Mutex<CountMinSketch>,
    key_count: AtomicU64,
    memory_used: AtomicU64,
    bytes_out: AtomicU64,
    bytes_in: AtomicU64,
    histograms: CommandHistograms,
}

impl ShardStats {
    /// Create a new stats collector.
    pub fn new() -> Self {
        Self {
            total_commands: AtomicU64::new(0),
            cmd_counts: std::array::from_fn(|_| AtomicU64::new(0)),
            cmd_durations_ns: std::array::from_fn(|_| AtomicU64::new(0)),
            hot_key_sketch: parking_lot::Mutex::new(CountMinSketch::default_hot_key()),
            key_count: AtomicU64::new(0),
            memory_used: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            histograms: CommandHistograms::new(),
        }
    }

    /// Record a command execution (counter + histogram).
    pub fn record_command(&self, cmd_type: usize, duration_ns: u64) {
        self.total_commands.fetch_add(1, Ordering::Relaxed);
        if cmd_type < NUM_COMMANDS {
            self.cmd_counts[cmd_type].fetch_add(1, Ordering::Relaxed);
            self.cmd_durations_ns[cmd_type].fetch_add(duration_ns, Ordering::Relaxed);
        }
        self.histograms.record(cmd_type, duration_ns);
    }

    /// Get a reference to the command histograms.
    pub fn histograms(&self) -> &CommandHistograms {
        &self.histograms
    }

    /// Record a key access for hot key tracking.
    pub fn record_key_access(&self, key: &[u8]) {
        self.hot_key_sketch.lock().increment(key);
    }

    /// Estimate the access count for a key.
    pub fn estimate_key_frequency(&self, key: &[u8]) -> u64 {
        self.hot_key_sketch.lock().estimate(key)
    }

    /// Update the key count.
    pub fn set_key_count(&self, count: u64) {
        self.key_count.store(count, Ordering::Relaxed);
    }

    /// Update memory usage.
    pub fn set_memory_used(&self, bytes: u64) {
        self.memory_used.store(bytes, Ordering::Relaxed);
    }

    /// Record bytes transferred.
    pub fn record_bytes(&self, bytes_in: u64, bytes_out: u64) {
        self.bytes_in.fetch_add(bytes_in, Ordering::Relaxed);
        self.bytes_out.fetch_add(bytes_out, Ordering::Relaxed);
    }

    /// Get the total number of commands processed.
    pub fn total_commands(&self) -> u64 {
        self.total_commands.load(Ordering::Relaxed)
    }

    /// Get the count for a specific command type.
    pub fn command_count(&self, cmd_type: usize) -> u64 {
        if cmd_type < NUM_COMMANDS {
            self.cmd_counts[cmd_type].load(Ordering::Relaxed)
        } else {
            0
        }
    }

    /// Get the average duration in nanoseconds for a command type.
    pub fn command_avg_ns(&self, cmd_type: usize) -> u64 {
        if cmd_type < NUM_COMMANDS {
            let count = self.cmd_counts[cmd_type].load(Ordering::Relaxed);
            if count == 0 {
                return 0;
            }
            self.cmd_durations_ns[cmd_type].load(Ordering::Relaxed) / count
        } else {
            0
        }
    }

    /// Get the key count.
    pub fn key_count(&self) -> u64 {
        self.key_count.load(Ordering::Relaxed)
    }

    /// Get memory usage.
    pub fn memory_used(&self) -> u64 {
        self.memory_used.load(Ordering::Relaxed)
    }

    /// Get total bytes received from clients.
    pub fn bytes_in(&self) -> u64 {
        self.bytes_in.load(Ordering::Relaxed)
    }

    /// Get total bytes sent to clients.
    pub fn bytes_out(&self) -> u64 {
        self.bytes_out.load(Ordering::Relaxed)
    }

    /// Decay the hot key sketch (call periodically).
    pub fn decay_hot_keys(&self) {
        self.hot_key_sketch.lock().decay();
    }

    /// Reset the hot key sketch.
    pub fn reset_hot_keys(&self) {
        self.hot_key_sketch.lock().reset();
    }

    /// Get a snapshot of all stats.
    pub fn snapshot(&self) -> StatsSnapshot {
        let mut cmd_counts = [0u64; NUM_COMMANDS];
        let mut cmd_durations = [0u64; NUM_COMMANDS];
        for i in 0..NUM_COMMANDS {
            cmd_counts[i] = self.cmd_counts[i].load(Ordering::Relaxed);
            cmd_durations[i] = self.cmd_durations_ns[i].load(Ordering::Relaxed);
        }

        StatsSnapshot {
            total_commands: self.total_commands.load(Ordering::Relaxed),
            cmd_counts,
            cmd_durations_ns: cmd_durations,
            key_count: self.key_count.load(Ordering::Relaxed),
            memory_used: self.memory_used.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
        }
    }
}

impl Default for ShardStats {
    fn default() -> Self {
        Self::new()
    }
}

/// A point-in-time, copyable snapshot of shard statistics.
///
/// Produced by [`ShardStats::snapshot`] and designed for cheap merging
/// across shards via [`StatsSnapshot::merge`].
#[derive(Debug, Clone)]
pub struct StatsSnapshot {
    /// Total commands processed.
    pub total_commands: u64,
    /// Per-command type counts.
    pub cmd_counts: [u64; NUM_COMMANDS],
    /// Per-command total duration in nanoseconds.
    pub cmd_durations_ns: [u64; NUM_COMMANDS],
    /// Number of keys.
    pub key_count: u64,
    /// Memory used (bytes).
    pub memory_used: u64,
    /// Total bytes received.
    pub bytes_in: u64,
    /// Total bytes sent.
    pub bytes_out: u64,
}

impl StatsSnapshot {
    /// Merge multiple snapshots (sum all values).
    pub fn merge(snapshots: &[StatsSnapshot]) -> Self {
        let mut merged = StatsSnapshot {
            total_commands: 0,
            cmd_counts: [0; NUM_COMMANDS],
            cmd_durations_ns: [0; NUM_COMMANDS],
            key_count: 0,
            memory_used: 0,
            bytes_in: 0,
            bytes_out: 0,
        };

        for snap in snapshots {
            merged.total_commands += snap.total_commands;
            merged.key_count += snap.key_count;
            merged.memory_used += snap.memory_used;
            merged.bytes_in += snap.bytes_in;
            merged.bytes_out += snap.bytes_out;
            for i in 0..NUM_COMMANDS {
                merged.cmd_counts[i] += snap.cmd_counts[i];
                merged.cmd_durations_ns[i] += snap.cmd_durations_ns[i];
            }
        }

        merged
    }
}

/// RAII guard that records command duration on drop.
///
/// Captures the start time at construction. When the guard goes out of
/// scope, it measures elapsed time and calls [`ShardStats::record_command`].
pub struct CommandTimer<'a> {
    stats: &'a ShardStats,
    cmd_type: usize,
    start: Instant,
}

impl<'a> CommandTimer<'a> {
    /// Start a new command timer.
    pub fn start(stats: &'a ShardStats, cmd_type: usize) -> Self {
        Self {
            stats,
            cmd_type,
            start: Instant::now(),
        }
    }
}

impl Drop for CommandTimer<'_> {
    fn drop(&mut self) {
        let duration = self.start.elapsed().as_nanos() as u64;
        self.stats.record_command(self.cmd_type, duration);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_command() {
        let stats = ShardStats::new();
        stats.record_command(0, 1000);
        stats.record_command(0, 2000);
        stats.record_command(1, 500);

        assert_eq!(stats.total_commands(), 3);
        assert_eq!(stats.command_count(0), 2);
        assert_eq!(stats.command_count(1), 1);
        assert_eq!(stats.command_avg_ns(0), 1500);
    }

    #[test]
    fn test_key_count() {
        let stats = ShardStats::new();
        stats.set_key_count(42);
        assert_eq!(stats.key_count(), 42);
    }

    #[test]
    fn test_hot_key_tracking() {
        let stats = ShardStats::new();
        for _ in 0..100 {
            stats.record_key_access(b"hot");
        }
        stats.record_key_access(b"cold");

        assert!(stats.estimate_key_frequency(b"hot") >= 100);
        assert!(stats.estimate_key_frequency(b"cold") >= 1);
    }

    #[test]
    fn test_snapshot() {
        let stats = ShardStats::new();
        stats.record_command(0, 1000);
        stats.set_key_count(10);
        stats.set_memory_used(1024);
        stats.record_bytes(100, 200);

        let snap = stats.snapshot();
        assert_eq!(snap.total_commands, 1);
        assert_eq!(snap.key_count, 10);
        assert_eq!(snap.memory_used, 1024);
        assert_eq!(snap.bytes_in, 100);
        assert_eq!(snap.bytes_out, 200);
    }

    #[test]
    fn test_snapshot_merge() {
        let s1 = StatsSnapshot {
            total_commands: 100,
            cmd_counts: {
                let mut c = [0; NUM_COMMANDS];
                c[0] = 50;
                c
            },
            cmd_durations_ns: [0; NUM_COMMANDS],
            key_count: 10,
            memory_used: 1000,
            bytes_in: 500,
            bytes_out: 600,
        };
        let s2 = StatsSnapshot {
            total_commands: 200,
            cmd_counts: {
                let mut c = [0; NUM_COMMANDS];
                c[0] = 100;
                c
            },
            cmd_durations_ns: [0; NUM_COMMANDS],
            key_count: 20,
            memory_used: 2000,
            bytes_in: 1000,
            bytes_out: 1200,
        };

        let merged = StatsSnapshot::merge(&[s1, s2]);
        assert_eq!(merged.total_commands, 300);
        assert_eq!(merged.cmd_counts[0], 150);
        assert_eq!(merged.key_count, 30);
        assert_eq!(merged.memory_used, 3000);
    }

    #[test]
    fn test_command_timer() {
        let stats = ShardStats::new();
        {
            let _timer = CommandTimer::start(&stats, 5);
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(stats.command_count(5), 1);
        assert!(stats.command_avg_ns(5) > 0);
    }

    #[test]
    fn test_decay_and_reset() {
        let stats = ShardStats::new();
        for _ in 0..100 {
            stats.record_key_access(b"key");
        }
        assert!(stats.estimate_key_frequency(b"key") >= 100);

        stats.decay_hot_keys();
        assert!(stats.estimate_key_frequency(b"key") >= 40); // ~50 after decay

        stats.reset_hot_keys();
        assert_eq!(stats.estimate_key_frequency(b"key"), 0);
    }

    #[test]
    fn test_concurrent_recording() {
        use std::sync::Arc;
        let stats = Arc::new(ShardStats::new());
        let mut handles = Vec::new();

        for _ in 0..4 {
            let s = stats.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..1000 {
                    s.record_command(0, 100);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(stats.total_commands(), 4000);
        assert_eq!(stats.command_count(0), 4000);
    }
}
