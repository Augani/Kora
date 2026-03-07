//! Distance metrics for vector similarity search.

/// Supported distance metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    /// Cosine similarity (1 - cosine_sim).
    Cosine,
    /// Euclidean (L2) distance.
    L2,
    /// Negative inner product (for maximum inner product search).
    InnerProduct,
}

impl DistanceMetric {
    /// Compute the distance between two vectors using this metric.
    ///
    /// Lower values mean more similar vectors for all metrics.
    pub fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len());
        match self {
            DistanceMetric::L2 => l2_distance(a, b),
            DistanceMetric::Cosine => cosine_distance(a, b),
            DistanceMetric::InnerProduct => negative_inner_product(a, b),
        }
    }
}

fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| {
            let d = x - y;
            d * d
        })
        .sum::<f32>()
        .sqrt()
}

fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;

    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }

    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom < f32::EPSILON {
        return 1.0; // undefined for zero vectors, return max distance
    }
    1.0 - (dot / denom)
}

fn negative_inner_product(a: &[f32], b: &[f32]) -> f32 {
    -a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f32>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l2_distance() {
        let a = [1.0, 0.0, 0.0];
        let b = [0.0, 1.0, 0.0];
        let d = DistanceMetric::L2.distance(&a, &b);
        assert!((d - std::f32::consts::SQRT_2).abs() < 1e-6);
    }

    #[test]
    fn test_l2_same_vector() {
        let a = [1.0, 2.0, 3.0];
        assert!(DistanceMetric::L2.distance(&a, &a) < 1e-6);
    }

    #[test]
    fn test_cosine_identical() {
        let a = [1.0, 2.0, 3.0];
        let d = DistanceMetric::Cosine.distance(&a, &a);
        assert!(d.abs() < 1e-6);
    }

    #[test]
    fn test_cosine_orthogonal() {
        let a = [1.0, 0.0];
        let b = [0.0, 1.0];
        let d = DistanceMetric::Cosine.distance(&a, &b);
        assert!((d - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_zero_vector() {
        let a = [0.0, 0.0, 0.0];
        let b = [1.0, 2.0, 3.0];
        let d = DistanceMetric::Cosine.distance(&a, &b);
        assert!((d - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_inner_product() {
        let a = [1.0, 2.0, 3.0];
        let b = [4.0, 5.0, 6.0];
        // dot = 4 + 10 + 18 = 32
        let d = DistanceMetric::InnerProduct.distance(&a, &b);
        assert!((d - (-32.0)).abs() < 1e-6);
    }
}
