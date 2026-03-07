use std::sync::atomic::{AtomicU64, Ordering};

/// A deterministic simulated clock for testing.
///
/// Provides nanosecond-precision virtual time that only advances
/// when explicitly told to, enabling reproducible test scenarios.
pub struct SimClock {
    now_ns: AtomicU64,
}

impl SimClock {
    /// Creates a new clock starting at time zero.
    pub fn new() -> Self {
        Self {
            now_ns: AtomicU64::new(0),
        }
    }

    /// Returns the current virtual time in nanoseconds.
    pub fn now_ns(&self) -> u64 {
        self.now_ns.load(Ordering::SeqCst)
    }

    /// Returns the current virtual time in milliseconds.
    pub fn now_ms(&self) -> u64 {
        self.now_ns() / 1_000_000
    }

    /// Advances the clock by the given number of nanoseconds.
    pub fn advance_ns(&self, delta: u64) {
        self.now_ns.fetch_add(delta, Ordering::SeqCst);
    }

    /// Advances the clock by the given number of milliseconds.
    pub fn advance_ms(&self, delta: u64) {
        self.advance_ns(delta * 1_000_000);
    }

    /// Sets the clock to an exact nanosecond value.
    pub fn set_ns(&self, value: u64) {
        self.now_ns.store(value, Ordering::SeqCst);
    }
}

impl Default for SimClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_at_zero() {
        let clock = SimClock::new();
        assert_eq!(clock.now_ns(), 0);
        assert_eq!(clock.now_ms(), 0);
    }

    #[test]
    fn advance_ns_increments() {
        let clock = SimClock::new();
        clock.advance_ns(500);
        assert_eq!(clock.now_ns(), 500);
        clock.advance_ns(300);
        assert_eq!(clock.now_ns(), 800);
    }

    #[test]
    fn advance_ms_converts_correctly() {
        let clock = SimClock::new();
        clock.advance_ms(5);
        assert_eq!(clock.now_ns(), 5_000_000);
        assert_eq!(clock.now_ms(), 5);
    }

    #[test]
    fn set_ns_overwrites() {
        let clock = SimClock::new();
        clock.advance_ns(1000);
        clock.set_ns(42);
        assert_eq!(clock.now_ns(), 42);
    }

    #[test]
    fn now_ms_truncates() {
        let clock = SimClock::new();
        clock.set_ns(1_500_000);
        assert_eq!(clock.now_ms(), 1);
    }
}
