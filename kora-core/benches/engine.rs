use criterion::{black_box, criterion_group, criterion_main, Criterion};
use kora_core::command::Command;
use kora_core::shard::ShardEngine;

fn bench_set_get(c: &mut Criterion) {
    let engine = ShardEngine::new(4);

    // Pre-populate
    for i in 0..1000 {
        engine.dispatch_blocking(Command::Set {
            key: format!("key:{}", i).into_bytes(),
            value: format!("value:{}", i).into_bytes(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });
    }

    c.bench_function("set", |b| {
        let mut i = 0u64;
        b.iter(|| {
            engine.dispatch_blocking(Command::Set {
                key: format!("bench:{}", i).into_bytes(),
                value: b"value".to_vec(),
                ex: None,
                px: None,
                nx: false,
                xx: false,
            });
            i += 1;
        });
    });

    c.bench_function("get_hit", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key:{}", i % 1000).into_bytes();
            black_box(engine.dispatch_blocking(Command::Get { key }));
            i += 1;
        });
    });

    c.bench_function("get_miss", |b| {
        b.iter(|| {
            black_box(engine.dispatch_blocking(Command::Get {
                key: b"nonexistent".to_vec(),
            }));
        });
    });
}

fn bench_incr(c: &mut Criterion) {
    let engine = ShardEngine::new(4);

    c.bench_function("incr", |b| {
        b.iter(|| {
            black_box(engine.dispatch_blocking(Command::Incr {
                key: b"counter".to_vec(),
            }));
        });
    });
}

fn bench_mget(c: &mut Criterion) {
    let engine = ShardEngine::new(4);

    for i in 0..100 {
        engine.dispatch_blocking(Command::Set {
            key: format!("mkey:{}", i).into_bytes(),
            value: format!("mval:{}", i).into_bytes(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });
    }

    c.bench_function("mget_10", |b| {
        let keys: Vec<Vec<u8>> = (0..10)
            .map(|i| format!("mkey:{}", i).into_bytes())
            .collect();
        b.iter(|| {
            black_box(engine.dispatch_blocking(Command::MGet { keys: keys.clone() }));
        });
    });

    c.bench_function("mget_100", |b| {
        let keys: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("mkey:{}", i).into_bytes())
            .collect();
        b.iter(|| {
            black_box(engine.dispatch_blocking(Command::MGet { keys: keys.clone() }));
        });
    });
}

criterion_group!(benches, bench_set_get, bench_incr, bench_mget);
criterion_main!(benches);
