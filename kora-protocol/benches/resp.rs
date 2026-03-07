use criterion::{black_box, criterion_group, criterion_main, Criterion};

use bytes::BytesMut;
use kora_core::command::CommandResponse;
use kora_protocol::{serialize_response, RespParser};

fn bench_parse_simple(c: &mut Criterion) {
    let data = b"+OK\r\n";

    c.bench_function("parse_simple_string", |b| {
        b.iter(|| {
            let mut parser = RespParser::new();
            parser.feed(black_box(data));
            black_box(parser.try_parse().unwrap());
        });
    });
}

fn bench_parse_integer(c: &mut Criterion) {
    let data = b":123456\r\n";

    c.bench_function("parse_integer", |b| {
        b.iter(|| {
            let mut parser = RespParser::new();
            parser.feed(black_box(data));
            black_box(parser.try_parse().unwrap());
        });
    });
}

fn bench_parse_bulk_string(c: &mut Criterion) {
    let payload = "x".repeat(1000);
    let data = format!("${}\r\n{}\r\n", payload.len(), payload).into_bytes();

    c.bench_function("parse_bulk_1kb", |b| {
        b.iter(|| {
            let mut parser = RespParser::new();
            parser.feed(black_box(&data));
            black_box(parser.try_parse().unwrap());
        });
    });
}

fn bench_parse_array(c: &mut Criterion) {
    // *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    let data = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";

    c.bench_function("parse_array_set_cmd", |b| {
        b.iter(|| {
            let mut parser = RespParser::new();
            parser.feed(black_box(data.as_slice()));
            black_box(parser.try_parse().unwrap());
        });
    });
}

fn bench_parse_pipeline(c: &mut Criterion) {
    // 10 SET commands pipelined
    let mut pipeline = Vec::new();
    for i in 0..10 {
        let key = format!("key{}", i);
        let val = format!("val{}", i);
        pipeline.extend_from_slice(
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                val.len(),
                val
            )
            .as_bytes(),
        );
    }

    c.bench_function("parse_pipeline_10_cmds", |b| {
        b.iter(|| {
            let mut parser = RespParser::new();
            parser.feed(black_box(&pipeline));
            for _ in 0..10 {
                black_box(parser.try_parse().unwrap());
            }
        });
    });
}

fn bench_serialize(c: &mut Criterion) {
    let mut buf = BytesMut::with_capacity(4096);

    c.bench_function("serialize_ok", |b| {
        b.iter(|| {
            buf.clear();
            serialize_response(black_box(&CommandResponse::Ok), &mut buf);
            black_box(&buf);
        });
    });

    c.bench_function("serialize_bulk_1kb", |b| {
        let resp = CommandResponse::BulkString(vec![b'x'; 1000]);
        b.iter(|| {
            buf.clear();
            serialize_response(black_box(&resp), &mut buf);
            black_box(&buf);
        });
    });

    c.bench_function("serialize_array_10", |b| {
        let resp = CommandResponse::Array(
            (0..10)
                .map(|i| CommandResponse::BulkString(format!("value-{}", i).into_bytes()))
                .collect(),
        );
        b.iter(|| {
            buf.clear();
            serialize_response(black_box(&resp), &mut buf);
            black_box(&buf);
        });
    });
}

criterion_group!(
    benches,
    bench_parse_simple,
    bench_parse_integer,
    bench_parse_bulk_string,
    bench_parse_array,
    bench_parse_pipeline,
    bench_serialize,
);
criterion_main!(benches);
