//! Stress and property tests for kora-core's ShardEngine.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rand::Rng;

use kora_core::command::{Command, CommandResponse};
use kora_core::shard::ShardEngine;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_engine() -> Arc<ShardEngine> {
    Arc::new(ShardEngine::new(4))
}

fn set_key(engine: &ShardEngine, key: &[u8], value: &[u8]) {
    engine.dispatch_blocking(Command::Set {
        key: key.to_vec(),
        value: value.to_vec(),
        ex: None,
        px: None,
        nx: false,
        xx: false,
    });
}

fn assert_bulk_eq(resp: CommandResponse, expected: &[u8]) {
    match resp {
        CommandResponse::BulkString(v) => assert_eq!(v, expected),
        other => panic!(
            "expected BulkString({:?}), got {:?}",
            String::from_utf8_lossy(expected),
            other
        ),
    }
}

fn assert_integer(resp: CommandResponse, expected: i64) {
    match resp {
        CommandResponse::Integer(n) => assert_eq!(n, expected),
        other => panic!("expected Integer({}), got {:?}", expected, other),
    }
}

// ---------------------------------------------------------------------------
// 1. Concurrent SET/GET stress
// ---------------------------------------------------------------------------

#[test]
fn stress_concurrent_set_get() {
    let engine = make_engine();
    let num_threads = 8;
    let ops_per_thread = 10_000;

    // Phase 1: all threads write their keys
    let mut handles = Vec::new();
    for t in 0..num_threads {
        let eng = engine.clone();
        handles.push(thread::spawn(move || {
            for i in 0..ops_per_thread {
                let key = format!("csg:{}:{}", t, i).into_bytes();
                let val = format!("val:{}:{}", t, i).into_bytes();
                eng.dispatch_blocking(Command::Set {
                    key,
                    value: val,
                    ex: None,
                    px: None,
                    nx: false,
                    xx: false,
                });
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // Phase 2: all threads read back and verify their keys
    let mut handles = Vec::new();
    for t in 0..num_threads {
        let eng = engine.clone();
        handles.push(thread::spawn(move || {
            for i in 0..ops_per_thread {
                let key = format!("csg:{}:{}", t, i).into_bytes();
                let expected = format!("val:{}:{}", t, i).into_bytes();
                let resp = eng.dispatch_blocking(Command::Get { key });
                assert_bulk_eq(resp, &expected);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

// ---------------------------------------------------------------------------
// 2. Mixed command stress
// ---------------------------------------------------------------------------

#[test]
fn stress_mixed_commands() {
    let engine = make_engine();
    let num_threads = 4;
    let ops_per_thread = 5_000;

    let mut handles = Vec::new();
    for t in 0..num_threads {
        let eng = engine.clone();
        handles.push(thread::spawn(move || {
            let mut rng = rand::thread_rng();
            for i in 0..ops_per_thread {
                let key = format!("mix:{}:{}", t, i % 200).into_bytes();
                let op = rng.gen_range(0..5u32);
                match op {
                    0 => {
                        // SET
                        eng.dispatch_blocking(Command::Set {
                            key,
                            value: format!("v{}", i).into_bytes(),
                            ex: None,
                            px: None,
                            nx: false,
                            xx: false,
                        });
                    }
                    1 => {
                        // GET — any result is fine, just no panics
                        let _ = eng.dispatch_blocking(Command::Get { key });
                    }
                    2 => {
                        // DEL
                        let _ = eng.dispatch_blocking(Command::Del { keys: vec![key] });
                    }
                    3 => {
                        // INCR — may error on type mismatch, that's OK
                        let _ = eng.dispatch_blocking(Command::Incr { key });
                    }
                    4 => {
                        // EXPIRE
                        let _ = eng.dispatch_blocking(Command::Expire { key, seconds: 300 });
                    }
                    _ => unreachable!(),
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    // If we got here without panics or deadlocks, success.
}

// ---------------------------------------------------------------------------
// 3. Multi-key fan-out stress (MSET/MGET)
// ---------------------------------------------------------------------------

#[test]
fn stress_multi_key_fanout() {
    let engine = make_engine();
    let key_count = 500;

    // Build entries that should spread across all 4 shards
    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..key_count)
        .map(|i| {
            (
                format!("mk:{}", i).into_bytes(),
                format!("mv:{}", i).into_bytes(),
            )
        })
        .collect();

    // MSET
    let resp = engine.dispatch_blocking(Command::MSet {
        entries: entries.clone(),
    });
    assert!(matches!(resp, CommandResponse::Ok));

    // MGET all keys and verify ordering
    let keys: Vec<Vec<u8>> = entries.iter().map(|(k, _)| k.clone()).collect();
    let resp = engine.dispatch_blocking(Command::MGet { keys });
    match resp {
        CommandResponse::Array(values) => {
            assert_eq!(values.len(), key_count);
            for (i, v) in values.iter().enumerate() {
                let expected = format!("mv:{}", i).into_bytes();
                match v {
                    CommandResponse::BulkString(b) => assert_eq!(*b, expected),
                    other => panic!("index {}: expected BulkString, got {:?}", i, other),
                }
            }
        }
        other => panic!("expected Array, got {:?}", other),
    }

    // DEL all keys via multi-key DEL and verify count
    let keys: Vec<Vec<u8>> = entries.iter().map(|(k, _)| k.clone()).collect();
    let resp = engine.dispatch_blocking(Command::Del { keys });
    assert_integer(resp, key_count as i64);

    // Verify all gone
    let keys: Vec<Vec<u8>> = entries.iter().map(|(k, _)| k.clone()).collect();
    let resp = engine.dispatch_blocking(Command::Exists { keys });
    assert_integer(resp, 0);
}

// ---------------------------------------------------------------------------
// 4. TTL stress
// ---------------------------------------------------------------------------

#[test]
fn stress_ttl_expiration() {
    let engine = make_engine();
    let key_count = 500;

    // Set keys with a very short PX (100ms)
    for i in 0..key_count {
        engine.dispatch_blocking(Command::Set {
            key: format!("ttl:{}", i).into_bytes(),
            value: b"ephemeral".to_vec(),
            ex: None,
            px: Some(100),
            nx: false,
            xx: false,
        });
    }

    // Immediately some should still exist
    let resp = engine.dispatch_blocking(Command::Get {
        key: b"ttl:0".to_vec(),
    });
    assert!(
        matches!(resp, CommandResponse::BulkString(_)),
        "key should exist immediately after set"
    );

    // Wait for expiration
    thread::sleep(Duration::from_millis(250));

    // Drive the expiry check by sending enough commands to each shard
    // (the engine runs evict_expired every 100 ops per shard)
    for i in 0..500 {
        engine.dispatch_blocking(Command::Ping {
            message: Some(format!("tick{}", i).into_bytes()),
        });
    }

    // Now all TTL keys should be gone
    let mut expired_count = 0;
    for i in 0..key_count {
        let resp = engine.dispatch_blocking(Command::Get {
            key: format!("ttl:{}", i).into_bytes(),
        });
        if matches!(resp, CommandResponse::Nil) {
            expired_count += 1;
        }
    }
    // Allow a small tolerance — lazy expiry may not catch every single one,
    // but the vast majority should be gone.
    assert!(
        expired_count >= key_count * 80 / 100,
        "expected at least 80% expired, but only {}/{} expired",
        expired_count,
        key_count,
    );
}

// ---------------------------------------------------------------------------
// 5. List/Hash/Set concurrent stress
// ---------------------------------------------------------------------------

#[test]
fn stress_list_concurrent() {
    let engine = make_engine();
    let num_threads = 4;
    let ops = 1_000;

    // Each thread pushes to its own list key
    let mut handles = Vec::new();
    for t in 0..num_threads {
        let eng = engine.clone();
        handles.push(thread::spawn(move || {
            let key = format!("list:{}", t).into_bytes();
            for i in 0..ops {
                if i % 2 == 0 {
                    eng.dispatch_blocking(Command::LPush {
                        key: key.clone(),
                        values: vec![format!("l{}", i).into_bytes()],
                    });
                } else {
                    eng.dispatch_blocking(Command::RPush {
                        key: key.clone(),
                        values: vec![format!("r{}", i).into_bytes()],
                    });
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // Verify each list has the right length
    for t in 0..num_threads {
        let resp = engine.dispatch_blocking(Command::LLen {
            key: format!("list:{}", t).into_bytes(),
        });
        assert_integer(resp, ops as i64);

        // LRANGE full
        let resp = engine.dispatch_blocking(Command::LRange {
            key: format!("list:{}", t).into_bytes(),
            start: 0,
            stop: -1,
        });
        match resp {
            CommandResponse::Array(arr) => assert_eq!(arr.len(), ops as usize),
            other => panic!("expected Array, got {:?}", other),
        }
    }
}

#[test]
fn stress_hash_concurrent() {
    let engine = make_engine();
    let num_threads = 4;
    let fields_per_thread = 500;

    let mut handles = Vec::new();
    for t in 0..num_threads {
        let eng = engine.clone();
        handles.push(thread::spawn(move || {
            let key = format!("hash:{}", t).into_bytes();
            for i in 0..fields_per_thread {
                eng.dispatch_blocking(Command::HSet {
                    key: key.clone(),
                    fields: vec![(
                        format!("f{}", i).into_bytes(),
                        format!("v{}", i).into_bytes(),
                    )],
                });
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // Verify
    for t in 0..num_threads {
        let key = format!("hash:{}", t).into_bytes();
        let resp = engine.dispatch_blocking(Command::HLen { key: key.clone() });
        assert_integer(resp, fields_per_thread as i64);

        // Spot-check a field
        let resp = engine.dispatch_blocking(Command::HGet {
            key: key.clone(),
            field: b"f0".to_vec(),
        });
        assert_bulk_eq(resp, b"v0");

        // HGETALL
        let resp = engine.dispatch_blocking(Command::HGetAll { key });
        match resp {
            CommandResponse::Array(arr) => {
                // HGETALL returns field, value pairs → 2 * fields_per_thread entries
                assert_eq!(arr.len(), (fields_per_thread * 2) as usize);
            }
            other => panic!("expected Array, got {:?}", other),
        }
    }
}

#[test]
fn stress_set_concurrent() {
    let engine = make_engine();
    let num_threads = 4;
    let members_per_thread = 500;

    let mut handles = Vec::new();
    for t in 0..num_threads {
        let eng = engine.clone();
        handles.push(thread::spawn(move || {
            let key = format!("set:{}", t).into_bytes();
            for i in 0..members_per_thread {
                eng.dispatch_blocking(Command::SAdd {
                    key: key.clone(),
                    members: vec![format!("m{}", i).into_bytes()],
                });
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    for t in 0..num_threads {
        let key = format!("set:{}", t).into_bytes();
        let resp = engine.dispatch_blocking(Command::SCard { key: key.clone() });
        assert_integer(resp, members_per_thread as i64);

        let resp = engine.dispatch_blocking(Command::SIsMember {
            key: key.clone(),
            member: b"m0".to_vec(),
        });
        assert_integer(resp, 1);

        let resp = engine.dispatch_blocking(Command::SMembers { key });
        match resp {
            CommandResponse::Array(arr) => assert_eq!(arr.len(), members_per_thread as usize),
            other => panic!("expected Array, got {:?}", other),
        }
    }
}

// ---------------------------------------------------------------------------
// 6. Large key count — 100K keys, verify DBSIZE
// ---------------------------------------------------------------------------

#[test]
fn stress_large_key_count() {
    let engine = make_engine();
    let total_keys: usize = 100_000;

    // Bulk insert
    // Use MSET in batches of 1000 for speed
    let batch_size = 1_000;
    for batch_start in (0..total_keys).step_by(batch_size) {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (batch_start..batch_start + batch_size)
            .map(|i| {
                (
                    format!("lk:{}", i).into_bytes(),
                    format!("lv:{}", i).into_bytes(),
                )
            })
            .collect();
        engine.dispatch_blocking(Command::MSet { entries });
    }

    let resp = engine.dispatch_blocking(Command::DbSize);
    assert_integer(resp, total_keys as i64);
}

// ---------------------------------------------------------------------------
// 7. FlushDb under concurrent load
// ---------------------------------------------------------------------------

#[test]
fn stress_flushdb_concurrent() {
    let engine = make_engine();

    // Pre-populate
    for i in 0..1_000 {
        set_key(&engine, format!("fl:{}", i).as_bytes(), b"x");
    }

    // Spawn writers
    let mut handles = Vec::new();
    for t in 0..4 {
        let eng = engine.clone();
        handles.push(thread::spawn(move || {
            for i in 0..2_000 {
                let key = format!("flw:{}:{}", t, i).into_bytes();
                eng.dispatch_blocking(Command::Set {
                    key,
                    value: b"y".to_vec(),
                    ex: None,
                    px: None,
                    nx: false,
                    xx: false,
                });
            }
        }));
    }

    // Issue FlushDb a few times while writers are active
    for _ in 0..3 {
        thread::sleep(Duration::from_millis(5));
        engine.dispatch_blocking(Command::FlushDb);
    }

    for h in handles {
        h.join().unwrap();
    }

    // Final flush and verify
    engine.dispatch_blocking(Command::FlushDb);
    let resp = engine.dispatch_blocking(Command::DbSize);
    assert_integer(resp, 0);
}
