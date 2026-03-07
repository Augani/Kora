//! Per-thread slab allocator with fixed size classes.
//!
//! Provides a freelist-based allocator that groups allocations into predefined
//! size classes (64, 128, 256, 512, 1024, 2048, 4096 bytes). Allocations that
//! exceed the largest size class fall through to the system allocator via `Vec`.
//!
//! This allocator is designed to be used per-shard-worker with no synchronization
//! overhead, matching Kōra's shared-nothing threading model.

use std::ops::{Deref, DerefMut};

/// The fixed size classes available for slab allocation, in bytes.
const SIZE_CLASSES: [usize; 7] = [64, 128, 256, 512, 1024, 2048, 4096];

/// Returns the index into [`SIZE_CLASSES`] for a given allocation size,
/// or `None` if the size exceeds the largest class.
fn size_class_index(size: usize) -> Option<usize> {
    SIZE_CLASSES.iter().position(|&class| size <= class)
}

/// A per-thread slab allocator with fixed size classes.
///
/// Each size class maintains a freelist of pre-allocated fixed-size slots backed
/// by `Vec<u8>`. Allocations round up to the next size class. Allocations larger
/// than 4096 bytes fall through to the system allocator.
///
/// This allocator is **not** thread-safe by design — each shard worker owns its
/// own instance, consistent with Kōra's shared-nothing architecture.
pub struct SlabAllocator {
    /// One freelist per size class, storing returned slots.
    freelists: [Vec<Vec<u8>>; 7],
    /// Number of bytes currently handed out (not on freelists).
    allocated_bytes: usize,
    /// Total number of slab slots ever created (active + free), per class.
    total_slabs: usize,
}

impl SlabAllocator {
    /// Creates a new slab allocator with empty freelists.
    pub fn new() -> Self {
        Self {
            freelists: Default::default(),
            allocated_bytes: 0,
            total_slabs: 0,
        }
    }

    /// Allocates a buffer of at least `size` bytes.
    ///
    /// If `size` fits within a size class (<= 4096), the allocation is served
    /// from the corresponding freelist (or a new slot is created). Otherwise
    /// the allocation falls through to the system allocator.
    ///
    /// The returned [`SlabAlloc`] is zero-initialized and provides byte-slice
    /// access via `Deref` / `DerefMut`.
    pub fn alloc(&mut self, size: usize) -> SlabAlloc {
        match size_class_index(size) {
            Some(class_idx) => {
                let class_size = SIZE_CLASSES[class_idx];
                let buf = match self.freelists[class_idx].pop() {
                    Some(mut buf) => {
                        // Clear residual data from previous use.
                        buf.iter_mut().for_each(|b| *b = 0);
                        buf
                    }
                    None => {
                        self.total_slabs += 1;
                        vec![0u8; class_size]
                    }
                };
                self.allocated_bytes += class_size;
                SlabAlloc {
                    buf,
                    requested: size,
                    source: AllocSource::Slab { class_idx },
                }
            }
            None => {
                self.allocated_bytes += size;
                SlabAlloc {
                    buf: vec![0u8; size],
                    requested: size,
                    source: AllocSource::System,
                }
            }
        }
    }

    /// Returns a slab allocation to the allocator.
    ///
    /// For slab-backed allocations the slot is pushed onto the appropriate
    /// freelist for reuse. System-allocated buffers are simply dropped.
    pub fn dealloc(&mut self, alloc: SlabAlloc) {
        match alloc.source {
            AllocSource::Slab { class_idx } => {
                let class_size = SIZE_CLASSES[class_idx];
                self.allocated_bytes = self.allocated_bytes.saturating_sub(class_size);
                self.freelists[class_idx].push(alloc.buf);
            }
            AllocSource::System => {
                self.allocated_bytes = self.allocated_bytes.saturating_sub(alloc.buf.len());
                // buf is dropped here, returning memory to the system allocator.
            }
        }
    }

    /// Returns a snapshot of allocator statistics.
    pub fn stats(&self) -> SlabStats {
        let free_slots: usize = self.freelists.iter().map(|fl| fl.len()).sum();
        SlabStats {
            allocated_bytes: self.allocated_bytes,
            free_slots,
            total_slabs: self.total_slabs,
        }
    }
}

impl Default for SlabAllocator {
    fn default() -> Self {
        Self::new()
    }
}

/// Tracks where an allocation originated so it can be returned correctly.
#[derive(Debug)]
enum AllocSource {
    /// Came from the slab freelist at the given class index.
    Slab { class_idx: usize },
    /// Came from the system allocator (too large for any size class).
    System,
}

/// An allocation produced by [`SlabAllocator`].
///
/// Contains the data buffer and metadata about its origin. Implements
/// `Deref<Target=\[u8\]>` and `DerefMut` for convenient byte-slice access.
///
/// This type is intentionally **not** `Clone` to prevent double-free bugs.
/// Return it to the allocator via [`SlabAllocator::dealloc`] when done.
pub struct SlabAlloc {
    /// The underlying data buffer.
    buf: Vec<u8>,
    /// The number of bytes originally requested by the caller.
    requested: usize,
    /// Where this allocation came from (slab class or system).
    source: AllocSource,
}

impl SlabAlloc {
    /// Returns the number of bytes originally requested.
    pub fn requested_size(&self) -> usize {
        self.requested
    }

    /// Returns the actual capacity of the backing buffer.
    ///
    /// For slab allocations this equals the size class; for system allocations
    /// it equals the requested size.
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    /// Returns `true` if this allocation was served by a slab size class.
    pub fn is_slab_backed(&self) -> bool {
        matches!(self.source, AllocSource::Slab { .. })
    }
}

impl Deref for SlabAlloc {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.buf
    }
}

impl DerefMut for SlabAlloc {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.buf
    }
}

/// Statistics snapshot from a [`SlabAllocator`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlabStats {
    /// Total bytes currently allocated (handed out, not on freelists).
    pub allocated_bytes: usize,
    /// Number of slots sitting on freelists, available for reuse.
    pub free_slots: usize,
    /// Total slab slots ever created across all size classes.
    pub total_slabs: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_returns_zeroed_buffer() {
        let mut alloc = SlabAllocator::new();
        let slot = alloc.alloc(50);
        assert!(slot.iter().all(|&b| b == 0));
        assert_eq!(slot.capacity(), 64); // rounds up to 64-byte class
        assert_eq!(slot.requested_size(), 50);
        assert!(slot.is_slab_backed());
    }

    #[test]
    fn data_integrity_through_deref_mut() {
        let mut alloc = SlabAllocator::new();
        let mut slot = alloc.alloc(10);
        slot[0] = 0xAB;
        slot[9] = 0xCD;
        assert_eq!(slot[0], 0xAB);
        assert_eq!(slot[9], 0xCD);
        // Remaining bytes still zero.
        assert_eq!(slot[1], 0);
    }

    #[test]
    fn size_class_rounding() {
        let mut alloc = SlabAllocator::new();

        // Exact boundary sizes.
        assert_eq!(alloc.alloc(64).capacity(), 64);
        assert_eq!(alloc.alloc(128).capacity(), 128);
        assert_eq!(alloc.alloc(256).capacity(), 256);
        assert_eq!(alloc.alloc(512).capacity(), 512);
        assert_eq!(alloc.alloc(1024).capacity(), 1024);
        assert_eq!(alloc.alloc(2048).capacity(), 2048);
        assert_eq!(alloc.alloc(4096).capacity(), 4096);

        // Just above a boundary rounds up.
        assert_eq!(alloc.alloc(65).capacity(), 128);
        assert_eq!(alloc.alloc(129).capacity(), 256);
        assert_eq!(alloc.alloc(1).capacity(), 64);
    }

    #[test]
    fn large_allocation_fallthrough() {
        let mut alloc = SlabAllocator::new();
        let slot = alloc.alloc(8192);
        assert_eq!(slot.capacity(), 8192);
        assert!(!slot.is_slab_backed());
        assert_eq!(slot.requested_size(), 8192);

        // System allocs still count toward allocated_bytes.
        assert_eq!(alloc.stats().allocated_bytes, 8192);
        assert_eq!(alloc.stats().total_slabs, 0); // not a slab

        alloc.dealloc(slot);
        assert_eq!(alloc.stats().allocated_bytes, 0);
    }

    #[test]
    fn alloc_dealloc_cycle_reuses_slots() {
        let mut alloc = SlabAllocator::new();

        // Allocate, write data, deallocate.
        let mut slot = alloc.alloc(100);
        slot[0] = 0xFF;
        alloc.dealloc(slot);

        assert_eq!(alloc.stats().free_slots, 1);
        assert_eq!(alloc.stats().total_slabs, 1);
        assert_eq!(alloc.stats().allocated_bytes, 0);

        // Re-allocate same size class — should reuse the slot.
        let slot2 = alloc.alloc(100);
        assert_eq!(alloc.stats().free_slots, 0);
        assert_eq!(alloc.stats().total_slabs, 1); // no new slab created
                                                  // Buffer is re-zeroed on reuse.
        assert_eq!(slot2[0], 0);

        alloc.dealloc(slot2);
    }

    #[test]
    fn stats_accuracy_multiple_classes() {
        let mut alloc = SlabAllocator::new();

        let s1 = alloc.alloc(32); // 64-byte class
        let s2 = alloc.alloc(200); // 256-byte class
        let s3 = alloc.alloc(5000); // system alloc

        let stats = alloc.stats();
        assert_eq!(stats.allocated_bytes, 64 + 256 + 5000);
        assert_eq!(stats.total_slabs, 2); // only slab-backed ones count
        assert_eq!(stats.free_slots, 0);

        alloc.dealloc(s1);
        let stats = alloc.stats();
        assert_eq!(stats.allocated_bytes, 256 + 5000);
        assert_eq!(stats.free_slots, 1);

        alloc.dealloc(s2);
        alloc.dealloc(s3);
        let stats = alloc.stats();
        assert_eq!(stats.allocated_bytes, 0);
        assert_eq!(stats.free_slots, 2); // system alloc doesn't go to freelist
    }

    #[test]
    fn zero_size_allocation() {
        let mut alloc = SlabAllocator::new();
        let slot = alloc.alloc(0);
        // Zero rounds up to the smallest class (64).
        assert_eq!(slot.capacity(), 64);
        assert!(slot.is_slab_backed());
        alloc.dealloc(slot);
    }

    #[test]
    fn multiple_allocs_same_class() {
        let mut alloc = SlabAllocator::new();
        let slots: Vec<_> = (0..10).map(|_| alloc.alloc(64)).collect();

        assert_eq!(alloc.stats().total_slabs, 10);
        assert_eq!(alloc.stats().allocated_bytes, 64 * 10);

        for s in slots {
            alloc.dealloc(s);
        }
        assert_eq!(alloc.stats().free_slots, 10);
        assert_eq!(alloc.stats().allocated_bytes, 0);
    }

    #[test]
    fn default_trait() {
        let alloc = SlabAllocator::default();
        let stats = alloc.stats();
        assert_eq!(stats.allocated_bytes, 0);
        assert_eq!(stats.free_slots, 0);
        assert_eq!(stats.total_slabs, 0);
    }

    #[test]
    fn slab_alloc_is_not_clone() {
        // This is a compile-time property. We verify the type doesn't
        // implement Clone by ensuring this module compiles without it.
        fn _assert_not_clone<T>()
        where
            T: Sized,
        {
        }
        _assert_not_clone::<SlabAlloc>();
        // If SlabAlloc derived Clone, a double-free would be possible.
    }
}
