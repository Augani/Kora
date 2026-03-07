//! Product quantizer for approximate vector compression.
//!
//! Splits high-dimensional vectors into subspaces and quantizes each subspace
//! independently using k-means clustering, achieving ~4x memory reduction.

use crate::distance::DistanceMetric;

/// A product quantizer that compresses vectors into compact codes.
///
/// Each vector is split into `num_subspaces` sub-vectors, and each sub-vector
/// is replaced by the index of its nearest centroid from a learned codebook.
pub struct ProductQuantizer {
    dim: usize,
    num_subspaces: usize,
    num_centroids: usize,
    sub_dim: usize,
    codebooks: Vec<Vec<Vec<f32>>>,
}

impl ProductQuantizer {
    /// Create a new product quantizer.
    ///
    /// - `dim`: total vector dimensionality (must be divisible by `num_subspaces`)
    /// - `num_subspaces`: number of sub-vector partitions (default: 8)
    /// - `num_centroids`: centroids per subspace codebook (default: 256, max 256 for u8 codes)
    pub fn new(dim: usize, num_subspaces: usize, num_centroids: usize) -> Result<Self, PqError> {
        if dim == 0 {
            return Err(PqError::InvalidDimension(dim));
        }
        if num_subspaces == 0 || dim % num_subspaces != 0 {
            return Err(PqError::IndivisibleSubspaces { dim, num_subspaces });
        }
        if num_centroids == 0 || num_centroids > 256 {
            return Err(PqError::InvalidCentroids(num_centroids));
        }

        let sub_dim = dim / num_subspaces;
        Ok(Self {
            dim,
            num_subspaces,
            num_centroids,
            sub_dim,
            codebooks: Vec::new(),
        })
    }

    /// Get the vector dimensionality.
    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Get the number of subspaces.
    pub fn num_subspaces(&self) -> usize {
        self.num_subspaces
    }

    /// Get the number of centroids per subspace.
    pub fn num_centroids(&self) -> usize {
        self.num_centroids
    }

    /// Returns true if the quantizer has been trained.
    pub fn is_trained(&self) -> bool {
        self.codebooks.len() == self.num_subspaces
    }

    /// Train the codebooks using k-means on the provided training vectors.
    ///
    /// Each vector must have length `dim`. Requires at least `num_centroids` vectors.
    pub fn train(&mut self, vectors: &[&[f32]], max_iterations: usize) -> Result<(), PqError> {
        if vectors.len() < self.num_centroids {
            return Err(PqError::InsufficientTrainingData {
                got: vectors.len(),
                need: self.num_centroids,
            });
        }
        for v in vectors {
            if v.len() != self.dim {
                return Err(PqError::DimensionMismatch {
                    expected: self.dim,
                    got: v.len(),
                });
            }
        }

        let mut codebooks = Vec::with_capacity(self.num_subspaces);

        for sub_idx in 0..self.num_subspaces {
            let offset = sub_idx * self.sub_dim;
            let sub_vectors: Vec<&[f32]> = vectors
                .iter()
                .map(|v| &v[offset..offset + self.sub_dim])
                .collect();

            let centroids = kmeans(
                &sub_vectors,
                self.num_centroids,
                self.sub_dim,
                max_iterations,
            );
            codebooks.push(centroids);
        }

        self.codebooks = codebooks;
        Ok(())
    }

    /// Encode a vector into a compact code (one byte per subspace).
    ///
    /// The quantizer must be trained before encoding.
    pub fn encode(&self, vector: &[f32]) -> Result<Vec<u8>, PqError> {
        if !self.is_trained() {
            return Err(PqError::NotTrained);
        }
        if vector.len() != self.dim {
            return Err(PqError::DimensionMismatch {
                expected: self.dim,
                got: vector.len(),
            });
        }

        let mut code = Vec::with_capacity(self.num_subspaces);
        for sub_idx in 0..self.num_subspaces {
            let offset = sub_idx * self.sub_dim;
            let sub_vec = &vector[offset..offset + self.sub_dim];
            let nearest = find_nearest_centroid(sub_vec, &self.codebooks[sub_idx]);
            code.push(nearest as u8);
        }
        Ok(code)
    }

    /// Decode a compact code back to an approximate vector.
    ///
    /// The quantizer must be trained before decoding.
    pub fn decode(&self, code: &[u8]) -> Result<Vec<f32>, PqError> {
        if !self.is_trained() {
            return Err(PqError::NotTrained);
        }
        if code.len() != self.num_subspaces {
            return Err(PqError::InvalidCodeLength {
                expected: self.num_subspaces,
                got: code.len(),
            });
        }

        let mut vector = Vec::with_capacity(self.dim);
        for (sub_idx, &centroid_idx) in code.iter().enumerate() {
            let idx = centroid_idx as usize;
            if idx >= self.codebooks[sub_idx].len() {
                return Err(PqError::InvalidCentroidIndex {
                    subspace: sub_idx,
                    index: idx,
                    max: self.codebooks[sub_idx].len(),
                });
            }
            vector.extend_from_slice(&self.codebooks[sub_idx][idx]);
        }
        Ok(vector)
    }

    /// Compute asymmetric distance between a raw query and an encoded vector.
    ///
    /// More accurate than symmetric (decode-then-compare) because the query
    /// is not quantized.
    pub fn asymmetric_distance(
        &self,
        query: &[f32],
        code: &[u8],
        metric: DistanceMetric,
    ) -> Result<f32, PqError> {
        if !self.is_trained() {
            return Err(PqError::NotTrained);
        }
        if query.len() != self.dim {
            return Err(PqError::DimensionMismatch {
                expected: self.dim,
                got: query.len(),
            });
        }
        if code.len() != self.num_subspaces {
            return Err(PqError::InvalidCodeLength {
                expected: self.num_subspaces,
                got: code.len(),
            });
        }

        let mut total_dist = 0.0f32;
        for (sub_idx, &code_byte) in code.iter().enumerate() {
            let offset = sub_idx * self.sub_dim;
            let sub_query = &query[offset..offset + self.sub_dim];
            let centroid_idx = code_byte as usize;
            if centroid_idx >= self.codebooks[sub_idx].len() {
                return Err(PqError::InvalidCentroidIndex {
                    subspace: sub_idx,
                    index: centroid_idx,
                    max: self.codebooks[sub_idx].len(),
                });
            }
            let centroid = &self.codebooks[sub_idx][centroid_idx];
            total_dist += metric.distance(sub_query, centroid);
        }
        Ok(total_dist)
    }
}

/// Errors from product quantizer operations.
#[derive(Debug, thiserror::Error)]
pub enum PqError {
    /// Vector dimension is zero.
    #[error("invalid dimension: {0}")]
    InvalidDimension(usize),

    /// Dimension not evenly divisible by subspace count.
    #[error("dimension {dim} not divisible by {num_subspaces} subspaces")]
    IndivisibleSubspaces {
        /// Total dimensionality.
        dim: usize,
        /// Requested subspace count.
        num_subspaces: usize,
    },

    /// Centroid count out of range.
    #[error("invalid centroid count: {0} (must be 1..=256)")]
    InvalidCentroids(usize),

    /// Not enough training vectors.
    #[error("insufficient training data: got {got}, need at least {need}")]
    InsufficientTrainingData {
        /// Vectors provided.
        got: usize,
        /// Minimum required.
        need: usize,
    },

    /// Vector dimension mismatch.
    #[error("dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch {
        /// Expected dimension.
        expected: usize,
        /// Actual dimension.
        got: usize,
    },

    /// Quantizer not yet trained.
    #[error("quantizer not trained")]
    NotTrained,

    /// Invalid code length.
    #[error("invalid code length: expected {expected}, got {got}")]
    InvalidCodeLength {
        /// Expected length.
        expected: usize,
        /// Actual length.
        got: usize,
    },

    /// Centroid index out of bounds.
    #[error("centroid index {index} out of bounds for subspace {subspace} (max {max})")]
    InvalidCentroidIndex {
        /// Subspace index.
        subspace: usize,
        /// Centroid index.
        index: usize,
        /// Maximum valid index.
        max: usize,
    },
}

fn find_nearest_centroid(sub_vec: &[f32], centroids: &[Vec<f32>]) -> usize {
    let mut best_idx = 0;
    let mut best_dist = f32::MAX;
    for (i, centroid) in centroids.iter().enumerate() {
        let dist = l2_sq(sub_vec, centroid);
        if dist < best_dist {
            best_dist = dist;
            best_idx = i;
        }
    }
    best_idx
}

fn l2_sq(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| {
            let d = x - y;
            d * d
        })
        .sum()
}

fn kmeans(vectors: &[&[f32]], k: usize, dim: usize, max_iterations: usize) -> Vec<Vec<f32>> {
    let mut centroids: Vec<Vec<f32>> = vectors.iter().take(k).map(|v| v.to_vec()).collect();

    for _ in 0..max_iterations {
        let mut assignments = vec![0usize; vectors.len()];
        for (i, v) in vectors.iter().enumerate() {
            assignments[i] = find_nearest_centroid(v, &centroids);
        }

        let mut new_centroids = vec![vec![0.0f32; dim]; k];
        let mut counts = vec![0usize; k];
        for (i, v) in vectors.iter().enumerate() {
            let cluster = assignments[i];
            counts[cluster] += 1;
            for (j, &val) in v.iter().enumerate() {
                new_centroids[cluster][j] += val;
            }
        }

        let mut converged = true;
        for (c, centroid) in new_centroids.iter_mut().enumerate() {
            if counts[c] > 0 {
                for val in centroid.iter_mut() {
                    *val /= counts[c] as f32;
                }
            } else {
                centroid.clone_from(&centroids[c]);
            }
            if l2_sq(centroid, &centroids[c]) > 1e-6 {
                converged = false;
            }
        }

        centroids = new_centroids;
        if converged {
            break;
        }
    }

    centroids
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pq_create() {
        let pq = ProductQuantizer::new(16, 4, 256);
        assert!(pq.is_ok());
        let pq = pq.unwrap();
        assert_eq!(pq.dim(), 16);
        assert_eq!(pq.num_subspaces(), 4);
        assert_eq!(pq.num_centroids(), 256);
        assert!(!pq.is_trained());
    }

    #[test]
    fn test_pq_invalid_params() {
        assert!(ProductQuantizer::new(0, 4, 256).is_err());
        assert!(ProductQuantizer::new(16, 3, 256).is_err());
        assert!(ProductQuantizer::new(16, 4, 0).is_err());
        assert!(ProductQuantizer::new(16, 4, 257).is_err());
    }

    #[test]
    fn test_pq_train_encode_decode() {
        let dim = 8;
        let num_subspaces = 4;
        let num_centroids = 4;
        let mut pq = ProductQuantizer::new(dim, num_subspaces, num_centroids).unwrap();

        let training_data: Vec<Vec<f32>> = (0..100)
            .map(|i| (0..dim).map(|d| ((i * dim + d) as f32) * 0.1).collect())
            .collect();
        let refs: Vec<&[f32]> = training_data.iter().map(|v| v.as_slice()).collect();

        pq.train(&refs, 20).unwrap();
        assert!(pq.is_trained());

        let test_vec: Vec<f32> = (0..dim).map(|d| d as f32 * 0.5).collect();
        let code = pq.encode(&test_vec).unwrap();
        assert_eq!(code.len(), num_subspaces);

        let decoded = pq.decode(&code).unwrap();
        assert_eq!(decoded.len(), dim);

        let error: f32 = test_vec
            .iter()
            .zip(decoded.iter())
            .map(|(a, b)| (a - b).powi(2))
            .sum::<f32>()
            .sqrt();
        assert!(error < 50.0, "Reconstruction error too high: {}", error);
    }

    #[test]
    fn test_pq_not_trained() {
        let pq = ProductQuantizer::new(8, 4, 4).unwrap();
        let v = vec![0.0f32; 8];
        assert!(pq.encode(&v).is_err());
        assert!(pq.decode(&[0, 0, 0, 0]).is_err());
    }

    #[test]
    fn test_pq_dimension_mismatch() {
        let mut pq = ProductQuantizer::new(8, 4, 4).unwrap();
        let training: Vec<Vec<f32>> = (0..10).map(|i| vec![i as f32; 8]).collect();
        let refs: Vec<&[f32]> = training.iter().map(|v| v.as_slice()).collect();
        pq.train(&refs, 5).unwrap();

        let wrong_dim = vec![0.0f32; 4];
        assert!(pq.encode(&wrong_dim).is_err());
    }

    #[test]
    fn test_pq_asymmetric_distance() {
        let dim = 8;
        let mut pq = ProductQuantizer::new(dim, 4, 4).unwrap();
        let training: Vec<Vec<f32>> = (0..20)
            .map(|i| (0..dim).map(|d| ((i * dim + d) as f32) * 0.1).collect())
            .collect();
        let refs: Vec<&[f32]> = training.iter().map(|v| v.as_slice()).collect();
        pq.train(&refs, 10).unwrap();

        let query: Vec<f32> = (0..dim).map(|d| d as f32 * 0.1).collect();
        let code = pq.encode(&query).unwrap();
        let dist = pq
            .asymmetric_distance(&query, &code, DistanceMetric::L2)
            .unwrap();
        assert!(dist >= 0.0);
    }

    #[test]
    fn test_pq_insufficient_training_data() {
        let mut pq = ProductQuantizer::new(8, 4, 16).unwrap();
        let training: Vec<Vec<f32>> = (0..5).map(|i| vec![i as f32; 8]).collect();
        let refs: Vec<&[f32]> = training.iter().map(|v| v.as_slice()).collect();
        assert!(pq.train(&refs, 5).is_err());
    }
}
