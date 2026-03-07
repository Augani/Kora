use criterion::{black_box, criterion_group, criterion_main, Criterion};

use kora_vector::distance::DistanceMetric;
use kora_vector::hnsw::HnswIndex;

fn random_vector(dim: usize, seed: u64) -> Vec<f32> {
    // Simple deterministic pseudo-random using xorshift
    let mut state = seed.wrapping_add(1);
    (0..dim)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state as f32) / (u64::MAX as f32)) * 2.0 - 1.0
        })
        .collect()
}

fn bench_insert(c: &mut Criterion) {
    c.bench_function("hnsw_insert_100", |b| {
        b.iter(|| {
            let mut index = HnswIndex::new(32, DistanceMetric::Cosine, 16, 200);
            for i in 0..100u64 {
                let vec = random_vector(32, i);
                index.insert(i, &vec);
            }
            black_box(&index);
        });
    });
}

fn bench_search(c: &mut Criterion) {
    let dim = 64;
    let n = 1000;
    let mut index = HnswIndex::new(dim, DistanceMetric::Cosine, 16, 200);
    for i in 0..n {
        let vec = random_vector(dim, i);
        index.insert(i, &vec);
    }

    let query = random_vector(dim, 99999);

    c.bench_function("hnsw_search_k10_n1000_d64", |b| {
        b.iter(|| {
            black_box(index.search(black_box(&query), 10, 50));
        });
    });

    c.bench_function("hnsw_search_k50_n1000_d64", |b| {
        b.iter(|| {
            black_box(index.search(black_box(&query), 50, 100));
        });
    });
}

fn bench_distance(c: &mut Criterion) {
    let dim = 128;
    let a = random_vector(dim, 1);
    let b_vec = random_vector(dim, 2);

    c.bench_function("distance_cosine_128d", |b| {
        b.iter(|| {
            black_box(DistanceMetric::Cosine.distance(black_box(&a), black_box(&b_vec)));
        });
    });

    c.bench_function("distance_l2_128d", |b| {
        b.iter(|| {
            black_box(DistanceMetric::L2.distance(black_box(&a), black_box(&b_vec)));
        });
    });

    c.bench_function("distance_inner_product_128d", |b| {
        b.iter(|| {
            black_box(DistanceMetric::InnerProduct.distance(black_box(&a), black_box(&b_vec)));
        });
    });
}

criterion_group!(benches, bench_insert, bench_search, bench_distance);
criterion_main!(benches);
