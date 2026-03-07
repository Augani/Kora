//! Key and value types for the cache engine.

mod key;
mod value;

pub use key::{CompactKey, KeyEntry};
pub use value::Value;
