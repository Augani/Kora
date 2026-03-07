//! HNSW (Hierarchical Navigable Small World) index.
//!
//! A graph-based approximate nearest neighbor (ANN) index that provides
//! logarithmic search time with high recall.

use std::collections::{BinaryHeap, HashMap, HashSet};

use ordered_float::OrderedFloat;

use crate::distance::DistanceMetric;

/// An HNSW index for approximate nearest neighbor search.
pub struct HnswIndex {
    /// Dimensionality of vectors.
    dim: usize,
    /// Distance metric.
    metric: DistanceMetric,
    /// Maximum number of connections per node per layer.
    m: usize,
    /// Maximum connections for layer 0 (typically 2*m).
    m_max0: usize,
    /// Size of dynamic candidate list during construction.
    ef_construction: usize,
    /// Inverse of the log of the level multiplier.
    ml: f64,
    /// All nodes in the index.
    nodes: HashMap<u64, Node>,
    /// Entry point node ID.
    entry_point: Option<u64>,
    /// Maximum layer in the graph.
    max_layer: usize,
    /// RNG seed for level generation.
    rng_state: u64,
}

struct Node {
    id: u64,
    vector: Vec<f32>,
    layer: usize,
    neighbors: Vec<Vec<u64>>, // neighbors[level] = list of neighbor IDs
    deleted: bool,
}

/// A search result: (id, distance).
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    /// The vector ID.
    pub id: u64,
    /// The distance to the query.
    pub distance: f32,
}

// Min-heap item (for nearest neighbors)
#[derive(PartialEq, Eq)]
struct MinItem(OrderedFloat<f32>, u64);

impl Ord for MinItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.0.cmp(&self.0) // reversed for min-heap
    }
}
impl PartialOrd for MinItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Max-heap item (for farthest neighbors)
#[derive(PartialEq, Eq)]
struct MaxItem(OrderedFloat<f32>, u64);

impl Ord for MaxItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}
impl PartialOrd for MaxItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl HnswIndex {
    /// Create a new empty HNSW index.
    ///
    /// - `dim`: vector dimensionality
    /// - `metric`: distance metric to use
    /// - `m`: max connections per node per layer (typical: 16)
    /// - `ef_construction`: search width during construction (typical: 200)
    pub fn new(dim: usize, metric: DistanceMetric, m: usize, ef_construction: usize) -> Self {
        Self {
            dim,
            metric,
            m,
            m_max0: m * 2,
            ef_construction,
            ml: 1.0 / (m as f64).ln(),
            nodes: HashMap::new(),
            entry_point: None,
            max_layer: 0,
            rng_state: 42,
        }
    }

    /// Get the number of vectors in the index.
    pub fn len(&self) -> usize {
        self.nodes.values().filter(|n| !n.deleted).count()
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the dimensionality.
    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Get the distance metric.
    pub fn metric(&self) -> DistanceMetric {
        self.metric
    }

    /// Insert a vector with the given ID.
    ///
    /// If a vector with this ID already exists, it is replaced.
    pub fn insert(&mut self, id: u64, vector: &[f32]) {
        assert_eq!(
            vector.len(),
            self.dim,
            "vector dimension mismatch: expected {}, got {}",
            self.dim,
            vector.len()
        );

        // Remove old version if exists
        if self.nodes.contains_key(&id) {
            self.delete(id);
        }

        let level = self.random_level();
        let vector = vector.to_vec();

        let mut neighbors = Vec::with_capacity(level + 1);
        for _ in 0..=level {
            neighbors.push(Vec::new());
        }

        let node = Node {
            id,
            vector,
            layer: level,
            neighbors,
            deleted: false,
        };

        self.nodes.insert(id, node);

        if self.entry_point.is_none() {
            self.entry_point = Some(id);
            self.max_layer = level;
            return;
        }

        let ep = match self.entry_point {
            Some(ep) => ep,
            None => return,
        };

        // Phase 1: Greedily traverse layers above the new node's level
        let mut current_ep = ep;
        let query = &self.nodes[&id].vector.clone();

        for lc in (level + 1..=self.max_layer).rev() {
            current_ep = self.greedy_closest(query, current_ep, lc);
        }

        // Phase 2: Insert at each layer from min(level, max_layer) down to 0
        let insert_top = level.min(self.max_layer);
        let mut ep_set = vec![current_ep];

        for lc in (0..=insert_top).rev() {
            let m_max = if lc == 0 { self.m_max0 } else { self.m };

            let candidates = self.search_layer(query, &ep_set, self.ef_construction, lc);

            // Select M nearest neighbors
            let selected: Vec<u64> = candidates.iter().take(m_max).map(|&(_, nid)| nid).collect();

            if let Some(node) = self.nodes.get_mut(&id) {
                node.neighbors[lc] = selected.clone();
            }

            // Connect neighbors back to new node (bidirectional)
            for &neighbor_id in &selected {
                let needs_prune = {
                    let Some(neighbor) = self.nodes.get_mut(&neighbor_id) else {
                        continue;
                    };
                    if lc < neighbor.neighbors.len() {
                        neighbor.neighbors[lc].push(id);
                        neighbor.neighbors[lc].len() > m_max
                    } else {
                        false
                    }
                };

                if needs_prune {
                    // Collect data needed for scoring without holding a mutable borrow
                    let nv = self.nodes[&neighbor_id].vector.clone();
                    let neighbor_ids: Vec<u64> = self.nodes[&neighbor_id].neighbors[lc].clone();
                    let mut scored: Vec<(f32, u64)> = neighbor_ids
                        .iter()
                        .map(|&nid| {
                            let dist = self.metric.distance(&nv, &self.nodes[&nid].vector);
                            (dist, nid)
                        })
                        .collect();
                    scored
                        .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                    scored.truncate(m_max);
                    if let Some(neighbor) = self.nodes.get_mut(&neighbor_id) {
                        neighbor.neighbors[lc] = scored.into_iter().map(|(_, nid)| nid).collect();
                    }
                }
            }

            ep_set = candidates.iter().map(|&(_, nid)| nid).collect();
        }

        // Update entry point if new node has higher level
        if level > self.max_layer {
            self.entry_point = Some(id);
            self.max_layer = level;
        }
    }

    /// Mark a vector as deleted (lazy deletion).
    pub fn delete(&mut self, id: u64) {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.deleted = true;
        }

        // If this was the entry point, find a new one
        if self.entry_point == Some(id) {
            self.entry_point = self
                .nodes
                .values()
                .filter(|n| !n.deleted)
                .max_by_key(|n| n.layer)
                .map(|n| n.id);
            if let Some(ep) = self.entry_point {
                self.max_layer = self.nodes[&ep].layer;
            } else {
                self.max_layer = 0;
            }
        }
    }

    /// Search for the K nearest neighbors of the query vector.
    ///
    /// - `query`: the query vector
    /// - `k`: number of results to return
    /// - `ef`: search width (larger = better recall, slower; typical: 50-200)
    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Vec<SearchResult> {
        assert_eq!(query.len(), self.dim);

        let ep = match self.entry_point {
            Some(ep) if !self.nodes[&ep].deleted || !self.is_empty() => ep,
            _ => return vec![],
        };

        // Phase 1: Greedy descent from top layer
        let mut current_ep = ep;
        for lc in (1..=self.max_layer).rev() {
            current_ep = self.greedy_closest(query, current_ep, lc);
        }

        // Phase 2: Search layer 0 with ef candidates
        let ef = ef.max(k);
        let candidates = self.search_layer(query, &[current_ep], ef, 0);

        // Return top-k, filtering deleted nodes
        candidates
            .into_iter()
            .filter(|&(_, id)| !self.nodes[&id].deleted)
            .take(k)
            .map(|(dist, id)| SearchResult { id, distance: dist })
            .collect()
    }

    /// Check if a vector with the given ID exists (and is not deleted).
    pub fn contains(&self, id: u64) -> bool {
        self.nodes.get(&id).is_some_and(|n| !n.deleted)
    }

    // ─── Internal ───────────────────────────────────────────────────

    fn random_level(&mut self) -> usize {
        // Simple xorshift64
        self.rng_state ^= self.rng_state << 13;
        self.rng_state ^= self.rng_state >> 7;
        self.rng_state ^= self.rng_state << 17;

        let r = (self.rng_state as f64) / (u64::MAX as f64);
        (-r.ln() * self.ml) as usize
    }

    fn greedy_closest(&self, query: &[f32], mut ep: u64, layer: usize) -> u64 {
        let mut best_dist = self.metric.distance(query, &self.nodes[&ep].vector);

        loop {
            let mut changed = false;
            let node = &self.nodes[&ep];
            if layer < node.neighbors.len() {
                for &neighbor_id in &node.neighbors[layer] {
                    if let Some(neighbor) = self.nodes.get(&neighbor_id) {
                        let dist = self.metric.distance(query, &neighbor.vector);
                        if dist < best_dist {
                            best_dist = dist;
                            ep = neighbor_id;
                            changed = true;
                        }
                    }
                }
            }
            if !changed {
                break;
            }
        }
        ep
    }

    /// Search a single layer, returning sorted (distance, id) pairs.
    fn search_layer(
        &self,
        query: &[f32],
        entry_points: &[u64],
        ef: usize,
        layer: usize,
    ) -> Vec<(f32, u64)> {
        let mut visited = HashSet::new();
        let mut candidates: BinaryHeap<MinItem> = BinaryHeap::new();
        let mut results: BinaryHeap<MaxItem> = BinaryHeap::new();

        for &ep in entry_points {
            if !self.nodes.contains_key(&ep) {
                continue;
            }
            let dist = self.metric.distance(query, &self.nodes[&ep].vector);
            visited.insert(ep);
            candidates.push(MinItem(OrderedFloat(dist), ep));
            results.push(MaxItem(OrderedFloat(dist), ep));
        }

        while let Some(MinItem(c_dist, c_id)) = candidates.pop() {
            let f_dist = results
                .peek()
                .map(|r| r.0)
                .unwrap_or(OrderedFloat(f32::MAX));
            if c_dist > f_dist {
                break;
            }

            let node = match self.nodes.get(&c_id) {
                Some(n) => n,
                None => continue,
            };

            if layer < node.neighbors.len() {
                for &neighbor_id in &node.neighbors[layer] {
                    if !visited.insert(neighbor_id) {
                        continue;
                    }
                    if let Some(neighbor) = self.nodes.get(&neighbor_id) {
                        let dist = self.metric.distance(query, &neighbor.vector);
                        let f_dist = results
                            .peek()
                            .map(|r| r.0)
                            .unwrap_or(OrderedFloat(f32::MAX));

                        if dist < f_dist.0 || results.len() < ef {
                            candidates.push(MinItem(OrderedFloat(dist), neighbor_id));
                            results.push(MaxItem(OrderedFloat(dist), neighbor_id));
                            if results.len() > ef {
                                results.pop();
                            }
                        }
                    }
                }
            }
        }

        let mut result: Vec<(f32, u64)> = results
            .into_iter()
            .map(|MaxItem(d, id)| (d.0, id))
            .collect();
        result.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_index(n: usize, dim: usize) -> (HnswIndex, Vec<Vec<f32>>) {
        let mut index = HnswIndex::new(dim, DistanceMetric::L2, 16, 200);
        let mut vectors = Vec::new();
        for i in 0..n {
            let v: Vec<f32> = (0..dim).map(|d| ((i * dim + d) as f32) * 0.01).collect();
            vectors.push(v.clone());
            index.insert(i as u64, &v);
        }
        (index, vectors)
    }

    #[test]
    fn test_insert_and_search() {
        let (index, vectors) = make_index(100, 8);
        assert_eq!(index.len(), 100);

        // Search for vector 42; it should be its own nearest neighbor
        let results = index.search(&vectors[42], 5, 50);
        assert!(!results.is_empty());
        assert_eq!(results[0].id, 42);
        assert!(results[0].distance < 1e-6);
    }

    #[test]
    fn test_search_empty_index() {
        let index = HnswIndex::new(4, DistanceMetric::L2, 16, 200);
        let results = index.search(&[1.0, 2.0, 3.0, 4.0], 5, 50);
        assert!(results.is_empty());
    }

    #[test]
    fn test_single_vector() {
        let mut index = HnswIndex::new(3, DistanceMetric::L2, 16, 200);
        index.insert(1, &[1.0, 2.0, 3.0]);

        let results = index.search(&[1.0, 2.0, 3.0], 5, 50);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 1);
    }

    #[test]
    fn test_delete() {
        let (mut index, vectors) = make_index(50, 4);
        assert_eq!(index.len(), 50);

        index.delete(25);
        assert_eq!(index.len(), 49);
        assert!(!index.contains(25));

        // Search should not return deleted vector
        let results = index.search(&vectors[25], 5, 50);
        assert!(results.iter().all(|r| r.id != 25));
    }

    #[test]
    fn test_cosine_metric() {
        let mut index = HnswIndex::new(3, DistanceMetric::Cosine, 16, 200);
        // Two parallel vectors should have distance ~0
        index.insert(1, &[1.0, 0.0, 0.0]);
        index.insert(2, &[2.0, 0.0, 0.0]); // same direction, different magnitude
        index.insert(3, &[0.0, 1.0, 0.0]); // orthogonal

        let results = index.search(&[3.0, 0.0, 0.0], 3, 50);
        // Both 1 and 2 should be closer than 3
        assert!(results.len() >= 2);
        let ids: Vec<u64> = results.iter().map(|r| r.id).collect();
        // Vector 3 (orthogonal) should be last
        assert!(ids[0] == 1 || ids[0] == 2);
    }

    #[test]
    fn test_inner_product() {
        let mut index = HnswIndex::new(2, DistanceMetric::InnerProduct, 16, 200);
        index.insert(1, &[1.0, 0.0]);
        index.insert(2, &[0.0, 1.0]);
        index.insert(3, &[10.0, 0.0]); // highest inner product with [1,0]

        let results = index.search(&[1.0, 0.0], 3, 50);
        // id=3 has highest inner product (10), so lowest negative_ip distance
        assert_eq!(results[0].id, 3);
    }

    #[test]
    fn test_recall_quality() {
        // Insert 500 random-ish vectors and verify recall > 80% for k=10
        let n = 500;
        let dim = 16;
        let (index, vectors) = make_index(n, dim);

        let query = &vectors[0];
        let k = 10;

        // Brute-force ground truth
        let mut dists: Vec<(f32, u64)> = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| (DistanceMetric::L2.distance(query, v), i as u64))
            .collect();
        dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let ground_truth: HashSet<u64> = dists.iter().take(k).map(|&(_, id)| id).collect();

        let results = index.search(query, k, 100);
        let found: HashSet<u64> = results.iter().map(|r| r.id).collect();

        let recall = ground_truth.intersection(&found).count() as f32 / k as f32;
        assert!(
            recall >= 0.8,
            "Recall too low: {:.2} (expected >= 0.80)",
            recall
        );
    }

    #[test]
    fn test_duplicate_insert() {
        let mut index = HnswIndex::new(3, DistanceMetric::L2, 16, 200);
        index.insert(1, &[1.0, 2.0, 3.0]);
        index.insert(1, &[4.0, 5.0, 6.0]); // replace

        assert_eq!(index.len(), 1);
        let results = index.search(&[4.0, 5.0, 6.0], 1, 50);
        assert_eq!(results[0].id, 1);
        assert!(results[0].distance < 1e-6);
    }

    #[test]
    fn test_k_larger_than_index() {
        let (index, _) = make_index(5, 4);
        let results = index.search(&[0.0; 4], 100, 200);
        assert_eq!(results.len(), 5); // can't return more than exist
    }

    #[test]
    fn test_contains() {
        let mut index = HnswIndex::new(3, DistanceMetric::L2, 16, 200);
        assert!(!index.contains(1));
        index.insert(1, &[1.0, 2.0, 3.0]);
        assert!(index.contains(1));
        index.delete(1);
        assert!(!index.contains(1));
    }
}
