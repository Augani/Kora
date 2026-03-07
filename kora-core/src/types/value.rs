//! The core `Value` enum representing all data types in Kōra.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::types::CompactKey;

/// Represents a value stored in the cache.
///
/// Small strings (≤ 23 bytes) are stored inline to avoid heap allocation.
/// Integers are stored natively as `i64` rather than as serialized strings.
#[derive(Clone, Debug)]
pub enum Value {
    /// Inline small string (≤ 23 bytes stored in-place, no heap allocation).
    InlineStr {
        /// The inline byte storage.
        data: [u8; 23],
        /// The length of the stored string.
        len: u8,
    },

    /// Heap-allocated string with reference counting for zero-copy reads.
    HeapStr(Arc<[u8]>),

    /// Integer stored as native i64 (not a serialized string).
    Int(i64),

    /// Doubly-ended queue (Redis List).
    List(VecDeque<Value>),

    /// Unordered set of unique values (Redis Set).
    Set(HashSet<Value>),

    /// Hash map of field-value pairs (Redis Hash).
    Hash(HashMap<CompactKey, Value>),

    /// Dense float vector for similarity search.
    Vector(Box<[f32]>),

    /// Sorted set with dual index: member→score and score→members for O(log N) operations.
    SortedSet(BTreeMap<Vec<u8>, f64>),
}

impl Value {
    /// Create a Value from a byte slice, auto-detecting integer or string.
    pub fn from_bytes(data: &[u8]) -> Self {
        // Try to parse as integer first
        if let Ok(s) = std::str::from_utf8(data) {
            if let Ok(i) = s.parse::<i64>() {
                return Value::Int(i);
            }
        }

        if data.len() <= 23 {
            let mut buf = [0u8; 23];
            buf[..data.len()].copy_from_slice(data);
            Value::InlineStr {
                data: buf,
                len: data.len() as u8,
            }
        } else {
            Value::HeapStr(Arc::from(data))
        }
    }

    /// Get the value as a byte slice, if it is a string or integer.
    ///
    /// Returns `None` for collection types (List, Set, Hash, Vector).
    pub fn as_bytes(&self) -> Option<Vec<u8>> {
        match self {
            Value::InlineStr { data, len } => Some(data[..*len as usize].to_vec()),
            Value::HeapStr(arc) => Some(arc.to_vec()),
            Value::Int(i) => Some(i.to_string().into_bytes()),
            _ => None,
        }
    }

    /// Serialize this value to bytes for storage/dump purposes.
    ///
    /// For string/integer types, returns the raw bytes. For collection types,
    /// returns a simple serialized representation.
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Value::InlineStr { data, len } => data[..*len as usize].to_vec(),
            Value::HeapStr(arc) => arc.to_vec(),
            Value::Int(i) => i.to_string().into_bytes(),
            Value::List(deque) => {
                let parts: Vec<String> = deque
                    .iter()
                    .filter_map(|v| {
                        v.as_bytes()
                            .map(|b| String::from_utf8_lossy(&b).into_owned())
                    })
                    .collect();
                parts.join(",").into_bytes()
            }
            Value::Set(set) => {
                let parts: Vec<String> = set
                    .iter()
                    .filter_map(|v| {
                        v.as_bytes()
                            .map(|b| String::from_utf8_lossy(&b).into_owned())
                    })
                    .collect();
                parts.join(",").into_bytes()
            }
            Value::Hash(map) => {
                let parts: Vec<String> = map
                    .iter()
                    .filter_map(|(k, v)| {
                        v.as_bytes().map(|b| {
                            format!(
                                "{}={}",
                                String::from_utf8_lossy(k.as_bytes()),
                                String::from_utf8_lossy(&b)
                            )
                        })
                    })
                    .collect();
                parts.join(",").into_bytes()
            }
            Value::Vector(v) => {
                let parts: Vec<String> = v.iter().map(|f| f.to_string()).collect();
                parts.join(",").into_bytes()
            }
            Value::SortedSet(map) => {
                let parts: Vec<String> = map
                    .iter()
                    .map(|(member, score)| format!("{}={}", String::from_utf8_lossy(member), score))
                    .collect();
                parts.join(",").into_bytes()
            }
        }
    }

    /// Returns an estimate of the memory footprint of this value in bytes.
    pub fn estimated_size(&self) -> usize {
        match self {
            Value::InlineStr { .. } => std::mem::size_of::<Self>(),
            Value::HeapStr(arc) => std::mem::size_of::<Self>() + arc.len(),
            Value::Int(_) => std::mem::size_of::<Self>(),
            Value::List(deque) => {
                std::mem::size_of::<Self>()
                    + deque.iter().map(|v| v.estimated_size()).sum::<usize>()
            }
            Value::Set(set) => {
                std::mem::size_of::<Self>() + set.iter().map(|v| v.estimated_size()).sum::<usize>()
            }
            Value::Hash(map) => {
                std::mem::size_of::<Self>()
                    + map
                        .iter()
                        .map(|(k, v)| k.as_bytes().len() + v.estimated_size())
                        .sum::<usize>()
            }
            Value::Vector(v) => std::mem::size_of::<Self>() + v.len() * 4,
            Value::SortedSet(map) => {
                std::mem::size_of::<Self>()
                    + map
                        .keys()
                        .map(|member| member.len() + std::mem::size_of::<f64>())
                        .sum::<usize>()
            }
        }
    }

    /// Returns the type name as used by the Redis TYPE command.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::InlineStr { .. } | Value::HeapStr(_) | Value::Int(_) => "string",
            Value::List(_) => "list",
            Value::Set(_) => "set",
            Value::Hash(_) => "hash",
            Value::Vector(_) => "vector",
            Value::SortedSet(_) => "zset",
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::InlineStr { data: a, len: al }, Value::InlineStr { data: b, len: bl }) => {
                al == bl && a[..*al as usize] == b[..*bl as usize]
            }
            (Value::HeapStr(a), Value::HeapStr(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            // Cross-representation equality for strings
            (Value::InlineStr { data, len }, Value::HeapStr(arc))
            | (Value::HeapStr(arc), Value::InlineStr { data, len }) => {
                &data[..*len as usize] == arc.as_ref()
            }
            _ => false,
        }
    }
}

impl Eq for Value {}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::InlineStr { data, len } => {
                state.write_u8(0);
                state.write(&data[..*len as usize]);
            }
            Value::HeapStr(arc) => {
                state.write_u8(0);
                state.write(arc.as_ref());
            }
            Value::Int(i) => {
                state.write_u8(1);
                state.write_i64(*i);
            }
            // Collection types are not hashable; hashing them is a no-op.
            // The command layer prevents using collections as hash keys.
            _ => {
                state.write_u8(255);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inline_string() {
        let v = Value::from_bytes(b"hello");
        assert_eq!(v.as_bytes().unwrap(), b"hello");
        assert_eq!(v.type_name(), "string");
    }

    #[test]
    fn test_heap_string() {
        let data = b"this is a string that is longer than 23 bytes for sure";
        let v = Value::from_bytes(data);
        assert_eq!(v.as_bytes().unwrap(), data.as_slice());
        assert!(matches!(v, Value::HeapStr(_)));
    }

    #[test]
    fn test_integer_detection() {
        let v = Value::from_bytes(b"42");
        assert!(matches!(v, Value::Int(42)));
        assert_eq!(v.as_bytes().unwrap(), b"42");
    }

    #[test]
    fn test_negative_integer() {
        let v = Value::from_bytes(b"-100");
        assert!(matches!(v, Value::Int(-100)));
    }

    #[test]
    fn test_not_an_integer() {
        let v = Value::from_bytes(b"12abc");
        assert!(!matches!(v, Value::Int(_)));
    }

    #[test]
    fn test_equality_across_representations() {
        let inline = Value::from_bytes(b"short");
        let heap = Value::HeapStr(Arc::from(b"short".as_slice()));
        assert_eq!(inline, heap);
    }

    #[test]
    fn test_collection_as_bytes_is_none() {
        let v = Value::List(VecDeque::new());
        assert!(v.as_bytes().is_none());
        assert_eq!(v.type_name(), "list");
    }
}
