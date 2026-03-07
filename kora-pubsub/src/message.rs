//! Pub/Sub message types.

use std::sync::Arc;

/// A message delivered to a subscriber.
#[derive(Debug, Clone)]
pub enum PubSubMessage {
    /// Message from an exact channel subscription.
    Message {
        /// The channel name.
        channel: Arc<[u8]>,
        /// The message payload.
        data: Arc<[u8]>,
    },
    /// Message from a pattern subscription.
    PatternMessage {
        /// The pattern that matched.
        pattern: Arc<[u8]>,
        /// The channel name.
        channel: Arc<[u8]>,
        /// The message payload.
        data: Arc<[u8]>,
    },
}
