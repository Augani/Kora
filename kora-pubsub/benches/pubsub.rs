use criterion::{criterion_group, criterion_main, Criterion};
use kora_pubsub::glob::glob_match;

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

criterion_group!(benches, bench_glob_match);
criterion_main!(benches);
