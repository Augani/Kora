//! Per-thread slab allocator for small allocations.
//!
//! Each shard worker gets its own `ShardAllocator` to eliminate global malloc
//! contention. Allocations up to [`LARGE_THRESHOLD`] bytes are served from
//! size-class pools; larger requests return `None`.

const SIZE_CLASSES: [usize; 7] = [64, 128, 256, 512, 1024, 2048, 4096];
const LARGE_THRESHOLD: usize = 4096;
const SLOTS_PER_CHUNK: usize = 64;

/// Handle returned by the slab allocator, identifying a specific slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlabPtr {
    /// Index of the size-class pool (0..6).
    pub pool_idx: u8,
    /// Index of the backing chunk within the pool.
    pub chunk_idx: u32,
    /// Slot index within the chunk.
    pub slot_idx: u32,
}

pub(crate) struct SlabPool {
    free_list: Vec<(u32, u32)>,
    chunk_size: usize,
    chunks: Vec<Vec<u8>>,
    slots_per_chunk: usize,
    allocated_count: usize,
}

#[allow(dead_code)]
impl SlabPool {
    pub(crate) fn new(chunk_size: usize) -> Self {
        Self {
            free_list: Vec::new(),
            chunk_size,
            chunks: Vec::new(),
            slots_per_chunk: SLOTS_PER_CHUNK,
            allocated_count: 0,
        }
    }

    pub(crate) fn alloc(&mut self, pool_idx: u8) -> SlabPtr {
        self.allocated_count += 1;

        if let Some((chunk_idx, slot_idx)) = self.free_list.pop() {
            return SlabPtr {
                pool_idx,
                chunk_idx,
                slot_idx,
            };
        }

        let chunk_idx = self.chunks.len() as u32;
        self.chunks
            .push(vec![0u8; self.chunk_size * self.slots_per_chunk]);

        for slot in (1..self.slots_per_chunk).rev() {
            self.free_list.push((chunk_idx, slot as u32));
        }

        SlabPtr {
            pool_idx,
            chunk_idx,
            slot_idx: 0,
        }
    }

    pub(crate) fn dealloc(&mut self, ptr: SlabPtr) {
        self.allocated_count = self.allocated_count.saturating_sub(1);
        self.free_list.push((ptr.chunk_idx, ptr.slot_idx));
    }

    pub(crate) fn get(&self, ptr: SlabPtr) -> &[u8] {
        let chunk = &self.chunks[ptr.chunk_idx as usize];
        let offset = ptr.slot_idx as usize * self.chunk_size;
        &chunk[offset..offset + self.chunk_size]
    }

    pub(crate) fn get_mut(&mut self, ptr: SlabPtr) -> &mut [u8] {
        let chunk = &mut self.chunks[ptr.chunk_idx as usize];
        let offset = ptr.slot_idx as usize * self.chunk_size;
        &mut chunk[offset..offset + self.chunk_size]
    }

    pub(crate) fn free_slots(&self) -> usize {
        self.free_list.len()
    }

    pub(crate) fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    pub(crate) fn allocated_count(&self) -> usize {
        self.allocated_count
    }
}

/// Per-shard allocator with size-class pools.
///
/// Manages 7 pools for sizes 64..4096. Allocations larger than
/// [`LARGE_THRESHOLD`] are not supported and return `None`.
pub struct ShardAllocator {
    pools: [SlabPool; 7],
    total_allocated: usize,
    total_freed: usize,
}

#[allow(dead_code)]
impl ShardAllocator {
    /// Create a new allocator with pools for each size class.
    pub fn new() -> Self {
        Self {
            pools: [
                SlabPool::new(SIZE_CLASSES[0]),
                SlabPool::new(SIZE_CLASSES[1]),
                SlabPool::new(SIZE_CLASSES[2]),
                SlabPool::new(SIZE_CLASSES[3]),
                SlabPool::new(SIZE_CLASSES[4]),
                SlabPool::new(SIZE_CLASSES[5]),
                SlabPool::new(SIZE_CLASSES[6]),
            ],
            total_allocated: 0,
            total_freed: 0,
        }
    }

    /// Allocate a slot that can hold at least `size` bytes.
    ///
    /// Returns `None` if `size` exceeds [`LARGE_THRESHOLD`].
    pub fn alloc(&mut self, size: usize) -> Option<SlabPtr> {
        if size == 0 || size > LARGE_THRESHOLD {
            return None;
        }

        let pool_idx = SIZE_CLASSES.iter().position(|&class| class >= size)?;

        self.total_allocated += 1;
        Some(self.pools[pool_idx].alloc(pool_idx as u8))
    }

    /// Return a slot to its pool.
    pub(crate) fn dealloc(&mut self, ptr: SlabPtr) {
        self.total_freed += 1;
        self.pools[ptr.pool_idx as usize].dealloc(ptr);
    }

    /// Read the contents of an allocated slot.
    pub(crate) fn get(&self, ptr: SlabPtr) -> &[u8] {
        self.pools[ptr.pool_idx as usize].get(ptr)
    }

    /// Mutably access the contents of an allocated slot.
    pub(crate) fn get_mut(&mut self, ptr: SlabPtr) -> &mut [u8] {
        self.pools[ptr.pool_idx as usize].get_mut(ptr)
    }

    /// Total number of allocations performed.
    pub fn total_allocated(&self) -> usize {
        self.total_allocated
    }

    /// Total number of deallocations performed.
    pub fn total_freed(&self) -> usize {
        self.total_freed
    }

    /// Per-pool statistics: `(class_size, allocated_chunks, free_slots)`.
    pub fn pool_stats(&self) -> Vec<(usize, usize, usize)> {
        SIZE_CLASSES
            .iter()
            .zip(self.pools.iter())
            .map(|(&class, pool)| (class, pool.chunk_count(), pool.free_slots()))
            .collect()
    }
}

impl Default for ShardAllocator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_dealloc_each_size_class() {
        let mut alloc = ShardAllocator::new();
        for &size in &SIZE_CLASSES {
            let ptr = alloc.alloc(size).expect("should allocate");
            assert_eq!(SIZE_CLASSES[ptr.pool_idx as usize], size);
            alloc.dealloc(ptr);
        }
    }

    #[test]
    fn slots_do_not_overlap() {
        let mut alloc = ShardAllocator::new();
        let mut ptrs = Vec::new();

        for _ in 0..128 {
            let ptr = alloc.alloc(64).expect("should allocate");
            alloc.get_mut(ptr).fill(0xAB);
            ptrs.push(ptr);
        }

        for ptr in &ptrs {
            let data = alloc.get(*ptr);
            assert!(data.iter().all(|&b| b == 0xAB));
        }
    }

    #[test]
    fn dealloc_reuses_slot() {
        let mut alloc = ShardAllocator::new();
        let ptr1 = alloc.alloc(64).expect("should allocate");
        alloc.dealloc(ptr1);
        let ptr2 = alloc.alloc(64).expect("should allocate");
        assert_eq!(ptr1, ptr2);
    }

    #[test]
    fn large_allocation_returns_none() {
        let mut alloc = ShardAllocator::new();
        assert!(alloc.alloc(4097).is_none());
        assert!(alloc.alloc(8192).is_none());
        assert!(alloc.alloc(0).is_none());
    }

    #[test]
    fn stats_tracking() {
        let mut alloc = ShardAllocator::new();
        let p1 = alloc.alloc(100).expect("should allocate");
        let p2 = alloc.alloc(200).expect("should allocate");
        let _p3 = alloc.alloc(500).expect("should allocate");

        assert_eq!(alloc.total_allocated(), 3);
        assert_eq!(alloc.total_freed(), 0);

        alloc.dealloc(p1);
        alloc.dealloc(p2);

        assert_eq!(alloc.total_allocated(), 3);
        assert_eq!(alloc.total_freed(), 2);

        let stats = alloc.pool_stats();
        assert_eq!(stats.len(), 7);
        assert_eq!(stats[0].0, 64);
    }

    #[test]
    fn stress_alloc_dealloc() {
        let mut alloc = ShardAllocator::new();
        let mut ptrs: Vec<SlabPtr> = Vec::new();

        for i in 0..10_000 {
            let size = SIZE_CLASSES[i % SIZE_CLASSES.len()];
            let ptr = alloc.alloc(size).expect("should allocate");
            alloc.get_mut(ptr).fill((i & 0xFF) as u8);
            ptrs.push(ptr);
        }

        assert_eq!(alloc.total_allocated(), 10_000);

        for (i, ptr) in ptrs.iter().enumerate() {
            let expected = (i & 0xFF) as u8;
            assert!(alloc.get(*ptr).iter().all(|&b| b == expected));
        }

        for ptr in ptrs.drain(..) {
            alloc.dealloc(ptr);
        }

        assert_eq!(alloc.total_freed(), 10_000);

        for i in 0..5_000 {
            let size = SIZE_CLASSES[i % SIZE_CLASSES.len()];
            let ptr = alloc.alloc(size).expect("should reuse freed slot");
            ptrs.push(ptr);
        }

        assert_eq!(alloc.total_allocated(), 15_000);
    }

    #[test]
    fn size_class_selection() {
        let mut alloc = ShardAllocator::new();

        let ptr = alloc.alloc(1).expect("should allocate");
        assert_eq!(ptr.pool_idx, 0);
        assert_eq!(alloc.get(ptr).len(), 64);

        let ptr = alloc.alloc(65).expect("should allocate");
        assert_eq!(ptr.pool_idx, 1);
        assert_eq!(alloc.get(ptr).len(), 128);

        let ptr = alloc.alloc(4096).expect("should allocate");
        assert_eq!(ptr.pool_idx, 6);
        assert_eq!(alloc.get(ptr).len(), 4096);
    }
}
