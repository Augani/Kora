use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use kora_pubsub::glob::glob_match;
use kora_pubsub::{MessageSink, PubSubBroker, PubSubMessage};

struct NoopSink;

impl MessageSink for NoopSink {
    fn send(&self, _msg: PubSubMessage) -> bool {
        true
    }
}

fn bench_glob_match(c: &mut Criterion) {
    c.bench_function("glob_exact", |b| {
        b.iter(|| glob_match(b"news.sports", b"news.sports"))
    });

    c.bench_function("glob_star", |b| {
        b.iter(|| glob_match(b"news.*", b"news.sports"))
    });

    c.bench_function("glob_complex", |b| {
        b.iter(|| glob_match(b"__keyevent@*__:*", b"__keyevent@0__:set"))
    });
}

fn bench_publish_fanout(c: &mut Criterion) {
    for count in [1, 10, 100, 1000] {
        let broker = PubSubBroker::new(4);
        for i in 0..count {
            broker.subscribe(b"fanout", i, Arc::new(NoopSink));
        }
        c.bench_function(&format!("publish_fanout_{}", count), |b| {
            b.iter(|| broker.publish(b"fanout", b"payload"))
        });
    }
}

fn bench_publish_cardinality(c: &mut Criterion) {
    let broker = PubSubBroker::new(16);
    let channels: Vec<Vec<u8>> = (0..10_000)
        .map(|i| format!("channel:{}", i).into_bytes())
        .collect();
    for (i, ch) in channels.iter().enumerate() {
        broker.subscribe(ch, i as u64, Arc::new(NoopSink));
    }
    c.bench_function("publish_10k_channels", |b| {
        let mut idx = 0;
        b.iter(|| {
            broker.publish(&channels[idx % channels.len()], b"data");
            idx += 1;
        })
    });
}

fn bench_pattern_matching(c: &mut Criterion) {
    for count in [1, 10, 100] {
        let broker = PubSubBroker::new(4);
        for i in 0..count {
            let pattern = format!("ns{}.*", i).into_bytes();
            broker.psubscribe(&pattern, i, Arc::new(NoopSink));
        }
        c.bench_function(&format!("publish_with_{}_patterns", count), |b| {
            b.iter(|| broker.publish(b"ns0.channel", b"payload"))
        });
    }
}

criterion_group!(
    benches,
    bench_glob_match,
    bench_publish_fanout,
    bench_publish_cardinality,
    bench_pattern_matching
);
criterion_main!(benches);
