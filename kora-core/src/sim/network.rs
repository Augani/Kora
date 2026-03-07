use std::collections::VecDeque;

/// A message in flight between simulated shards.
pub struct SimMessage {
    /// Source shard identifier.
    pub from_shard: u16,
    /// Destination shard identifier.
    pub to_shard: u16,
    /// Message payload.
    pub payload: Vec<u8>,
    /// Virtual nanosecond at which this message should be delivered.
    pub deliver_at_ns: u64,
}

/// Simulates network communication between shards with configurable
/// delays, packet loss, and network partitions.
pub struct SimNetwork {
    in_flight: VecDeque<SimMessage>,
    min_delay_ns: u64,
    max_delay_ns: u64,
    drop_rate: f64,
    partition: Option<(u16, u16)>,
    delivered: Vec<SimMessage>,
}

impl SimNetwork {
    /// Creates a new simulated network with the given delay range.
    pub fn new(min_delay_ns: u64, max_delay_ns: u64) -> Self {
        assert!(
            min_delay_ns <= max_delay_ns,
            "min_delay_ns must be <= max_delay_ns"
        );
        Self {
            in_flight: VecDeque::new(),
            min_delay_ns,
            max_delay_ns,
            drop_rate: 0.0,
            partition: None,
            delivered: Vec::new(),
        }
    }

    /// Sends a message from one shard to another.
    ///
    /// The message receives a random delivery delay. It may be dropped
    /// based on the configured drop rate or an active partition.
    pub fn send(
        &mut self,
        from: u16,
        to: u16,
        payload: Vec<u8>,
        now_ns: u64,
        rng: &mut impl FnMut() -> u64,
    ) {
        if self.is_partitioned(from, to) {
            return;
        }

        let drop_threshold = (self.drop_rate * u64::MAX as f64) as u64;
        if drop_threshold > 0 && rng() < drop_threshold {
            return;
        }

        let delay = if self.min_delay_ns == self.max_delay_ns {
            self.min_delay_ns
        } else {
            let range = self.max_delay_ns - self.min_delay_ns;
            self.min_delay_ns + rng() % range
        };

        self.in_flight.push_back(SimMessage {
            from_shard: from,
            to_shard: to,
            payload,
            deliver_at_ns: now_ns + delay,
        });
    }

    /// Returns all messages whose delivery time has arrived.
    pub fn deliver_until(&mut self, now_ns: u64) -> Vec<SimMessage> {
        self.delivered.clear();
        let mut remaining = VecDeque::new();

        while let Some(msg) = self.in_flight.pop_front() {
            if msg.deliver_at_ns <= now_ns {
                self.delivered.push(msg);
            } else {
                remaining.push_back(msg);
            }
        }

        self.in_flight = remaining;
        std::mem::take(&mut self.delivered)
    }

    /// Sets the probability that any given message is dropped.
    ///
    /// `rate` must be in `[0.0, 1.0]`.
    pub fn set_drop_rate(&mut self, rate: f64) {
        assert!(
            (0.0..=1.0).contains(&rate),
            "drop rate must be between 0.0 and 1.0"
        );
        self.drop_rate = rate;
    }

    /// Simulates a network partition between two shards.
    ///
    /// Messages in both directions between `shard_a` and `shard_b` will be dropped.
    pub fn partition(&mut self, shard_a: u16, shard_b: u16) {
        self.partition = Some((shard_a, shard_b));
    }

    /// Removes any active network partition.
    pub fn heal_partition(&mut self) {
        self.partition = None;
    }

    /// Returns the number of messages currently in flight.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }

    fn is_partitioned(&self, from: u16, to: u16) -> bool {
        match self.partition {
            Some((a, b)) => (from == a && to == b) || (from == b && to == a),
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_rng(value: u64) -> impl FnMut() -> u64 {
        move || value
    }

    #[test]
    fn send_and_deliver() {
        let mut net = SimNetwork::new(100, 200);
        net.send(0, 1, vec![1, 2, 3], 1000, &mut fixed_rng(50));

        let delivered = net.deliver_until(1000);
        assert!(delivered.is_empty());

        let delivered = net.deliver_until(1200);
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].from_shard, 0);
        assert_eq!(delivered[0].to_shard, 1);
        assert_eq!(delivered[0].payload, vec![1, 2, 3]);
    }

    #[test]
    fn partition_drops_messages() {
        let mut net = SimNetwork::new(0, 0);
        net.partition(0, 1);

        net.send(0, 1, vec![1], 0, &mut fixed_rng(0));
        net.send(1, 0, vec![2], 0, &mut fixed_rng(0));
        net.send(0, 2, vec![3], 0, &mut fixed_rng(0));

        let delivered = net.deliver_until(u64::MAX);
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].payload, vec![3]);
    }

    #[test]
    fn heal_partition_restores() {
        let mut net = SimNetwork::new(0, 0);
        net.partition(0, 1);
        net.send(0, 1, vec![1], 0, &mut fixed_rng(0));
        assert_eq!(net.in_flight_count(), 0);

        net.heal_partition();
        net.send(0, 1, vec![2], 0, &mut fixed_rng(0));
        assert_eq!(net.in_flight_count(), 1);
    }

    #[test]
    fn drop_rate_drops_messages() {
        let mut net = SimNetwork::new(0, 0);
        net.set_drop_rate(1.0);

        net.send(0, 1, vec![1], 0, &mut fixed_rng(0));
        assert_eq!(net.in_flight_count(), 0);
    }

    #[test]
    fn zero_drop_rate_delivers_all() {
        let mut net = SimNetwork::new(0, 0);
        net.set_drop_rate(0.0);

        for i in 0..10u8 {
            net.send(0, 1, vec![i], 0, &mut fixed_rng(u64::MAX));
        }
        assert_eq!(net.in_flight_count(), 10);
    }

    #[test]
    fn delivery_ordering() {
        let mut net = SimNetwork::new(0, 1000);
        let delays = [100u64, 500, 300];
        let mut idx = 0;
        let mut delay_rng = || {
            let val = delays[idx];
            idx += 1;
            val
        };

        net.send(0, 1, vec![1], 0, &mut delay_rng);
        net.send(0, 1, vec![2], 0, &mut delay_rng);
        net.send(0, 1, vec![3], 0, &mut delay_rng);

        assert_eq!(net.in_flight_count(), 3);

        let msgs = net.deliver_until(150);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].payload, vec![1]);

        let msgs = net.deliver_until(400);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].payload, vec![3]);

        let msgs = net.deliver_until(600);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].payload, vec![2]);
    }

    #[test]
    fn in_flight_count_tracks() {
        let mut net = SimNetwork::new(100, 100);
        assert_eq!(net.in_flight_count(), 0);

        net.send(0, 1, vec![1], 0, &mut fixed_rng(0));
        net.send(0, 1, vec![2], 0, &mut fixed_rng(0));
        assert_eq!(net.in_flight_count(), 2);

        net.deliver_until(100);
        assert_eq!(net.in_flight_count(), 0);
    }
}
