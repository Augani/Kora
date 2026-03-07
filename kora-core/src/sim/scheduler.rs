use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

use super::clock::SimClock;

/// Unique identifier for a scheduled task.
pub type TaskId = u64;

/// A deterministic task scheduler driven by virtual time.
///
/// Uses a seeded xorshift64 PRNG so that identical seeds produce
/// identical execution orders, enabling FoundationDB-style
/// deterministic simulation testing.
pub struct SimScheduler {
    ready_queue: BinaryHeap<Reverse<(u64, TaskId)>>,
    tasks: HashMap<TaskId, Box<dyn FnOnce() + Send>>,
    next_id: TaskId,
    seed: u64,
    rng_state: u64,
}

impl SimScheduler {
    /// Creates a new scheduler seeded for deterministic randomness.
    pub fn new(seed: u64) -> Self {
        let rng_state = if seed == 0 { 1 } else { seed };
        Self {
            ready_queue: BinaryHeap::new(),
            tasks: HashMap::new(),
            next_id: 0,
            seed,
            rng_state,
        }
    }

    /// Returns the seed this scheduler was created with.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Schedules a task to run at the given virtual time (nanoseconds).
    pub fn schedule(&mut self, wake_time_ns: u64, action: Box<dyn FnOnce() + Send>) -> TaskId {
        let id = self.next_id;
        self.next_id += 1;
        self.ready_queue.push(Reverse((wake_time_ns, id)));
        self.tasks.insert(id, action);
        id
    }

    /// Schedules a task to run immediately (wake time = 0).
    pub fn schedule_now(&mut self, action: Box<dyn FnOnce() + Send>) -> TaskId {
        self.schedule(0, action)
    }

    /// Runs all tasks with wake times up to `target_ns`, advancing the clock.
    ///
    /// Returns the number of tasks executed.
    pub fn run_until(&mut self, clock: &SimClock, target_ns: u64) -> usize {
        let mut executed = 0;
        while let Some(&Reverse((wake_time, id))) = self.ready_queue.peek() {
            if wake_time > target_ns {
                break;
            }
            self.ready_queue.pop();
            if wake_time > clock.now_ns() {
                clock.set_ns(wake_time);
            }
            if let Some(action) = self.tasks.remove(&id) {
                action();
                executed += 1;
            }
        }
        if clock.now_ns() < target_ns {
            clock.set_ns(target_ns);
        }
        executed
    }

    /// Runs the next ready task (one whose wake time <= clock.now_ns).
    ///
    /// Returns `false` if no task is ready.
    pub fn run_one(&mut self, clock: &SimClock) -> bool {
        let now = clock.now_ns();
        if let Some(&Reverse((wake_time, _))) = self.ready_queue.peek() {
            if wake_time > now {
                return false;
            }
            let Reverse((wt, id)) = self.ready_queue.pop().unwrap();
            if wt > clock.now_ns() {
                clock.set_ns(wt);
            }
            if let Some(action) = self.tasks.remove(&id) {
                action();
            }
            return true;
        }
        false
    }

    /// Returns the number of pending (not yet executed) tasks.
    pub fn pending_count(&self) -> usize {
        self.tasks.len()
    }

    /// Returns the next deterministic pseudo-random u64 (xorshift64).
    pub fn next_random(&mut self) -> u64 {
        let mut x = self.rng_state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng_state = x;
        x
    }

    /// Returns a deterministic random value in `[min, max)`.
    ///
    /// # Panics
    ///
    /// Panics if `min >= max`.
    pub fn next_random_range(&mut self, min: u64, max: u64) -> u64 {
        assert!(min < max, "min must be less than max");
        min + self.next_random() % (max - min)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn schedule_and_run_until() {
        let clock = SimClock::new();
        let mut sched = SimScheduler::new(42);
        let log = Arc::new(Mutex::new(Vec::new()));

        for i in 0u64..5 {
            let log_clone = Arc::clone(&log);
            sched.schedule(
                i * 1_000_000,
                Box::new(move || {
                    log_clone.lock().unwrap().push(i);
                }),
            );
        }

        let executed = sched.run_until(&clock, 5_000_000);
        assert_eq!(executed, 5);
        assert_eq!(*log.lock().unwrap(), vec![0, 1, 2, 3, 4]);
        assert_eq!(clock.now_ns(), 5_000_000);
    }

    #[test]
    fn run_until_partial() {
        let clock = SimClock::new();
        let mut sched = SimScheduler::new(1);
        let counter = Arc::new(Mutex::new(0u64));

        for t in [100, 200, 300, 400, 500] {
            let c = Arc::clone(&counter);
            sched.schedule(
                t,
                Box::new(move || {
                    *c.lock().unwrap() += 1;
                }),
            );
        }

        let executed = sched.run_until(&clock, 250);
        assert_eq!(executed, 2);
        assert_eq!(*counter.lock().unwrap(), 2);
        assert_eq!(sched.pending_count(), 3);
    }

    #[test]
    fn run_one_returns_false_when_empty() {
        let clock = SimClock::new();
        let mut sched = SimScheduler::new(1);
        assert!(!sched.run_one(&clock));
    }

    #[test]
    fn run_one_respects_wake_time() {
        let clock = SimClock::new();
        let mut sched = SimScheduler::new(1);
        let ran = Arc::new(Mutex::new(false));

        let ran_clone = Arc::clone(&ran);
        sched.schedule(
            1000,
            Box::new(move || {
                *ran_clone.lock().unwrap() = true;
            }),
        );

        assert!(!sched.run_one(&clock));
        assert!(!*ran.lock().unwrap());

        clock.set_ns(1000);
        assert!(sched.run_one(&clock));
        assert!(*ran.lock().unwrap());
    }

    #[test]
    fn deterministic_replay() {
        fn run_with_seed(seed: u64) -> Vec<u64> {
            let clock = SimClock::new();
            let mut sched = SimScheduler::new(seed);
            let log = Arc::new(Mutex::new(Vec::new()));

            for i in 0u64..10 {
                let log_clone = Arc::clone(&log);
                sched.schedule(
                    i * 100,
                    Box::new(move || {
                        log_clone.lock().unwrap().push(i);
                    }),
                );
            }

            sched.run_until(&clock, 1000);
            Arc::try_unwrap(log).unwrap().into_inner().unwrap()
        }

        let run1 = run_with_seed(12345);
        let run2 = run_with_seed(12345);
        assert_eq!(run1, run2);
    }

    #[test]
    fn next_random_is_deterministic() {
        let mut s1 = SimScheduler::new(99);
        let mut s2 = SimScheduler::new(99);
        let vals1: Vec<u64> = (0..10).map(|_| s1.next_random()).collect();
        let vals2: Vec<u64> = (0..10).map(|_| s2.next_random()).collect();
        assert_eq!(vals1, vals2);
    }

    #[test]
    fn next_random_range_bounds() {
        let mut sched = SimScheduler::new(77);
        for _ in 0..100 {
            let val = sched.next_random_range(10, 20);
            assert!((10..20).contains(&val));
        }
    }

    #[test]
    #[should_panic(expected = "min must be less than max")]
    fn next_random_range_panics_on_invalid() {
        let mut sched = SimScheduler::new(1);
        sched.next_random_range(10, 10);
    }

    #[test]
    fn schedule_now_runs_at_zero() {
        let clock = SimClock::new();
        let mut sched = SimScheduler::new(1);
        let ran = Arc::new(Mutex::new(false));

        let ran_clone = Arc::clone(&ran);
        sched.schedule_now(Box::new(move || {
            *ran_clone.lock().unwrap() = true;
        }));

        assert!(sched.run_one(&clock));
        assert!(*ran.lock().unwrap());
    }
}
