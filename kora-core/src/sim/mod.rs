//! Deterministic simulation testing framework.
//!
//! Provides FoundationDB-style deterministic testing primitives:
//! a virtual clock, a seeded task scheduler, and a simulated network
//! layer with fault injection. Same seed = same execution = reproducible bugs.

/// Virtual clock for deterministic time control.
pub mod clock;
/// Simulated inter-shard network with fault injection.
pub mod network;
/// Deterministic task scheduler with seeded PRNG.
pub mod scheduler;

use clock::SimClock;
use network::SimNetwork;
use scheduler::SimScheduler;

/// Ties together clock, scheduler, and network into a single
/// deterministic simulation runtime.
pub struct SimRuntime {
    /// Virtual clock for the simulation.
    pub clock: SimClock,
    /// Deterministic task scheduler.
    pub scheduler: SimScheduler,
    /// Simulated inter-shard network.
    pub network: SimNetwork,
    seed: u64,
}

impl SimRuntime {
    /// Creates a new simulation runtime with the given seed.
    pub fn new(seed: u64) -> Self {
        Self {
            clock: SimClock::new(),
            scheduler: SimScheduler::new(seed),
            network: SimNetwork::new(100_000, 1_000_000),
            seed,
        }
    }

    /// Returns the seed this runtime was created with.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Runs one scheduler step. Returns `false` if no task was ready.
    pub fn step(&mut self) -> bool {
        self.scheduler.run_one(&self.clock)
    }

    /// Runs all tasks within a time window of `ms` milliseconds
    /// from the current clock time.
    ///
    /// Returns the number of tasks executed.
    pub fn run_for_ms(&mut self, ms: u64) -> usize {
        let target = self.clock.now_ns() + ms * 1_000_000;
        self.scheduler.run_until(&self.clock, target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn runtime_seed_preserved() {
        let rt = SimRuntime::new(42);
        assert_eq!(rt.seed(), 42);
    }

    #[test]
    fn step_returns_false_when_idle() {
        let mut rt = SimRuntime::new(1);
        assert!(!rt.step());
    }

    #[test]
    fn run_for_ms_executes_tasks() {
        let mut rt = SimRuntime::new(1);
        let log = Arc::new(Mutex::new(Vec::new()));

        for i in 0u64..3 {
            let l = Arc::clone(&log);
            rt.scheduler
                .schedule(i * 1_000_000, Box::new(move || l.lock().unwrap().push(i)));
        }

        let count = rt.run_for_ms(3);
        assert_eq!(count, 3);
        assert_eq!(*log.lock().unwrap(), vec![0, 1, 2]);
    }

    #[test]
    fn end_to_end_deterministic_replay() {
        fn simulate(seed: u64) -> Vec<(u16, Vec<u8>)> {
            let mut rt = SimRuntime::new(seed);
            let results = Arc::new(Mutex::new(Vec::new()));

            let results_clone = Arc::clone(&results);
            rt.scheduler.schedule(
                500_000,
                Box::new(move || {
                    results_clone.lock().unwrap().push((0, vec![1, 2, 3]));
                }),
            );

            let results_clone = Arc::clone(&results);
            rt.scheduler.schedule(
                1_000_000,
                Box::new(move || {
                    results_clone.lock().unwrap().push((1, vec![4, 5, 6]));
                }),
            );

            rt.run_for_ms(2);
            Arc::try_unwrap(results).unwrap().into_inner().unwrap()
        }

        let run1 = simulate(999);
        let run2 = simulate(999);
        assert_eq!(run1, run2);
        assert_eq!(run1.len(), 2);
    }

    #[test]
    fn network_integration() {
        let mut rt = SimRuntime::new(7);
        let rng_val = rt.scheduler.next_random();

        let mut rt2 = SimRuntime::new(7);
        let rng_val2 = rt2.scheduler.next_random();
        assert_eq!(rng_val, rng_val2);

        let mut rng_fn = || rng_val;
        rt.network.send(0, 1, vec![10, 20], 0, &mut rng_fn);
        rt.clock.advance_ms(2);

        let delivered = rt.network.deliver_until(rt.clock.now_ns());
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].payload, vec![10, 20]);
    }

    #[test]
    fn network_partition_in_runtime() {
        let mut rt = SimRuntime::new(1);
        rt.network.partition(0, 1);

        rt.network.send(0, 1, vec![1], 0, &mut || 0);
        let delivered = rt.network.deliver_until(u64::MAX);
        assert!(delivered.is_empty());

        rt.network.heal_partition();
        rt.network.send(0, 1, vec![2], 0, &mut || 0);
        let delivered = rt.network.deliver_until(u64::MAX);
        assert_eq!(delivered.len(), 1);
    }
}
