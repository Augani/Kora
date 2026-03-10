//! TCP integration tests for kora-server Redis protocol compatibility.

use std::path::PathBuf;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use kora_core::hash::shard_for_key;
use kora_protocol::{RespParser, RespValue};
use kora_server::{KoraServer, ServerConfig};
use kora_storage::manager::StorageConfig;
use kora_storage::wal::SyncPolicy;

mod test_support;
use test_support::{
    assert_resp, assert_resp_prefix, cmd, connect, free_port, resp_cmd, send_and_read,
    start_server, start_server_with_config, start_server_with_workers,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn read_exact_response(stream: &mut TcpStream, len: usize) -> Vec<u8> {
    let mut buf = vec![0u8; len];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut buf))
        .await
        .unwrap()
        .unwrap();
    buf
}

fn keys_on_different_shards(shard_count: usize, prefix: &str) -> (String, String) {
    let mut shard0 = None;
    let mut shard1 = None;

    for i in 0..10_000 {
        let candidate = format!("{}{}", prefix, i);
        match shard_for_key(candidate.as_bytes(), shard_count) {
            0 if shard0.is_none() => shard0 = Some(candidate),
            1 if shard1.is_none() => shard1 = Some(candidate),
            _ => {}
        }

        if let (Some(a), Some(b)) = (shard0.as_ref(), shard1.as_ref()) {
            return (a.clone(), b.clone());
        }
    }

    panic!(
        "failed to find keys on shards 0 and 1 for prefix {}",
        prefix
    );
}

fn extract_info_metric(resp: &[u8], key: &str) -> Option<u64> {
    let body = String::from_utf8_lossy(resp);
    let needle = format!("{key}:");
    let start = body.find(&needle)? + needle.len();
    let value = body[start..].lines().next()?.trim();
    value.parse().ok()
}

fn decode_bulk_string(resp: &[u8]) -> Option<String> {
    if !resp.starts_with(b"$") {
        return None;
    }
    let first_crlf = resp.windows(2).position(|w| w == b"\r\n")?;
    let len = std::str::from_utf8(&resp[1..first_crlf])
        .ok()?
        .parse::<usize>()
        .ok()?;
    let payload_start = first_crlf + 2;
    let payload_end = payload_start + len;
    if resp.len() < payload_end + 2 || &resp[payload_end..payload_end + 2] != b"\r\n" {
        return None;
    }
    String::from_utf8(resp[payload_start..payload_end].to_vec()).ok()
}

fn parse_resp_frame(resp: &[u8]) -> RespValue {
    let mut parser = RespParser::new();
    parser.feed(resp);
    parser
        .try_parse()
        .expect("response should parse")
        .expect("response should contain one frame")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_ping() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    // PING with no argument -> +PONG
    let resp = cmd(&mut stream, &["PING"]).await;
    assert_resp(&resp, b"+PONG\r\n");

    // PING with argument -> bulk string echo
    let resp = cmd(&mut stream, &["PING", "hello"]).await;
    assert_resp(&resp, b"$5\r\nhello\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_set_get() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    // SET key value -> +OK
    let resp = cmd(&mut stream, &["SET", "mykey", "myvalue"]).await;
    assert_resp(&resp, b"+OK\r\n");

    // GET key -> bulk string
    let resp = cmd(&mut stream, &["GET", "mykey"]).await;
    assert_resp(&resp, b"$7\r\nmyvalue\r\n");

    // GET nonexistent -> nil
    let resp = cmd(&mut stream, &["GET", "nokey"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_del() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    // Set two keys
    cmd(&mut stream, &["SET", "a", "1"]).await;
    cmd(&mut stream, &["SET", "b", "2"]).await;

    // DEL both -> :2
    let resp = cmd(&mut stream, &["DEL", "a", "b"]).await;
    assert_resp(&resp, b":2\r\n");

    // GET deleted key -> nil
    let resp = cmd(&mut stream, &["GET", "a"]).await;
    assert_resp(&resp, b"$-1\r\n");

    // DEL nonexistent -> :0
    let resp = cmd(&mut stream, &["DEL", "nonexistent"]).await;
    assert_resp(&resp, b":0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_exists() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "ex1", "val"]).await;

    // EXISTS on existing key -> :1
    let resp = cmd(&mut stream, &["EXISTS", "ex1"]).await;
    assert_resp(&resp, b":1\r\n");

    // EXISTS on non-existing key -> :0
    let resp = cmd(&mut stream, &["EXISTS", "nope"]).await;
    assert_resp(&resp, b":0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_incr() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    // INCR on nonexistent key -> :1 (starts from 0)
    let resp = cmd(&mut stream, &["INCR", "counter"]).await;
    assert_resp(&resp, b":1\r\n");

    // INCR again -> :2
    let resp = cmd(&mut stream, &["INCR", "counter"]).await;
    assert_resp(&resp, b":2\r\n");

    // SET then INCR
    cmd(&mut stream, &["SET", "num", "10"]).await;
    let resp = cmd(&mut stream, &["INCR", "num"]).await;
    assert_resp(&resp, b":11\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_doc_commands_basic() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(
        &mut stream,
        &["DOC.CREATE", "users", "COMPRESSION", "dictionary"],
    )
    .await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(
        &mut stream,
        &[
            "DOC.SET",
            "users",
            "doc:1",
            r#"{"name":"Augustus","age":30,"address":{"city":"Accra"}}"#,
        ],
    )
    .await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["DOC.EXISTS", "users", "doc:1"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(
        &mut stream,
        &["DOC.GET", "users", "doc:1", "FIELDS", "name"],
    )
    .await;
    let payload = decode_bulk_string(&resp).expect("DOC.GET should return bulk JSON");
    assert_eq!(payload, r#"{"name":"Augustus"}"#);

    let resp = cmd(&mut stream, &["DOC.DEL", "users", "doc:1"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["DOC.EXISTS", "users", "doc:1"]).await;
    assert_resp(&resp, b":0\r\n");

    let resp = cmd(&mut stream, &["DOC.DROP", "users"]).await;
    assert_resp(&resp, b":1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_doc_batch_commands() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["DOC.CREATE", "users"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(
        &mut stream,
        &[
            "DOC.MSET",
            "users",
            "doc:1",
            r#"{"name":"A"}"#,
            "doc:2",
            r#"{"name":"B"}"#,
        ],
    )
    .await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(
        &mut stream,
        &["DOC.MGET", "users", "doc:1", "doc:2", "doc:3"],
    )
    .await;
    assert_resp(
        &resp,
        b"*3\r\n$12\r\n{\"name\":\"A\"}\r\n$12\r\n{\"name\":\"B\"}\r\n$-1\r\n",
    );

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_doc_update_commands() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["DOC.CREATE", "users"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(
        &mut stream,
        &[
            "DOC.SET",
            "users",
            "doc:1",
            r#"{"name":"Augustus","score":10,"active":true,"address":{"city":"Accra"},"tags":["rust","systems","rust"]}"#,
        ],
    )
    .await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(
        &mut stream,
        &[
            "DOC.UPDATE",
            "users",
            "doc:1",
            "SET",
            "address.city",
            r#""London""#,
            "INCR",
            "score",
            "2.5",
            "PUSH",
            "tags",
            r#""cache""#,
            "PULL",
            "tags",
            r#""rust""#,
            "DEL",
            "active",
        ],
    )
    .await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["DOC.GET", "users", "doc:1"]).await;
    let payload = decode_bulk_string(&resp).expect("DOC.GET should return bulk JSON");
    let parsed: Value = serde_json::from_str(&payload).expect("payload should be valid JSON");
    assert_eq!(
        parsed,
        json!({
            "name": "Augustus",
            "score": 12.5,
            "address": {"city": "London"},
            "tags": ["systems", "cache"]
        })
    );

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_doc_dictinfo_command() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["DOC.CREATE", "users"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(
        &mut stream,
        &[
            "DOC.SET",
            "users",
            "doc:1",
            r#"{"city":"Accra","status":"active"}"#,
        ],
    )
    .await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(
        &mut stream,
        &[
            "DOC.SET",
            "users",
            "doc:2",
            r#"{"city":"Accra","status":"inactive"}"#,
        ],
    )
    .await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["HELLO", "3"]).await;
    assert_resp_prefix(&resp, b"%");

    let resp = cmd(&mut stream, &["DOC.DICTINFO", "users"]).await;
    let frame = parse_resp_frame(&resp);
    let RespValue::Map(entries) = frame else {
        panic!("expected RESP3 map for DOC.DICTINFO");
    };

    let mut dictionary_entries = None;
    let mut fields = None;
    for (key, value) in entries {
        if let RespValue::SimpleString(key) | RespValue::BulkString(Some(key)) = key {
            if key == b"dictionary_entries" {
                if let RespValue::Integer(count) = value {
                    dictionary_entries = Some(count);
                }
            }
            if key == b"fields" {
                fields = Some(value);
            }
        }
    }

    assert!(dictionary_entries.expect("dictionary_entries should exist") >= 2);
    let fields = fields.expect("fields entry should exist");
    let RespValue::Array(Some(items)) = fields else {
        panic!("fields should be an array");
    };
    assert!(!items.is_empty(), "fields array should not be empty");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_doc_storage_command() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["DOC.CREATE", "users"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(
        &mut stream,
        &[
            "DOC.SET",
            "users",
            "doc:1",
            r#"{"name":"Augustus","age":30}"#,
        ],
    )
    .await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(
        &mut stream,
        &["DOC.SET", "users", "doc:2", r#"{"name":"Ada","age":28}"#],
    )
    .await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["HELLO", "3"]).await;
    assert_resp_prefix(&resp, b"%");

    let resp = cmd(&mut stream, &["DOC.STORAGE", "users"]).await;
    let frame = parse_resp_frame(&resp);
    let RespValue::Map(entries) = frame else {
        panic!("expected RESP3 map for DOC.STORAGE");
    };

    let mut doc_count = None;
    let mut total_packed_bytes = None;
    for (key, value) in entries {
        if let RespValue::SimpleString(key) | RespValue::BulkString(Some(key)) = key {
            if key == b"doc_count" {
                if let RespValue::Integer(count) = value {
                    doc_count = Some(count);
                }
            }
            if key == b"total_packed_bytes" {
                if let RespValue::Integer(bytes) = value {
                    total_packed_bytes = Some(bytes);
                }
            }
        }
    }

    assert_eq!(doc_count, Some(2));
    assert!(total_packed_bytes.expect("total_packed_bytes should exist") > 0);

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_lpush_lrange() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    // LPUSH returns list length
    let resp = cmd(&mut stream, &["LPUSH", "mylist", "a"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["LPUSH", "mylist", "b"]).await;
    assert_resp(&resp, b":2\r\n");

    let resp = cmd(&mut stream, &["LPUSH", "mylist", "c"]).await;
    assert_resp(&resp, b":3\r\n");

    // LRANGE 0 -1 -> all elements: c, b, a (LPUSH prepends)
    let resp = cmd(&mut stream, &["LRANGE", "mylist", "0", "-1"]).await;
    let expected = b"*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n";
    assert_resp(&resp, expected);

    // LRANGE with partial range
    let resp = cmd(&mut stream, &["LRANGE", "mylist", "0", "1"]).await;
    let expected = b"*2\r\n$1\r\nc\r\n$1\r\nb\r\n";
    assert_resp(&resp, expected);

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_hset_hget() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    // HSET returns number of fields added
    let resp = cmd(&mut stream, &["HSET", "myhash", "f1", "v1"]).await;
    assert_resp(&resp, b":1\r\n");

    // HGET existing field
    let resp = cmd(&mut stream, &["HGET", "myhash", "f1"]).await;
    assert_resp(&resp, b"$2\r\nv1\r\n");

    // HGET non-existing field -> nil
    let resp = cmd(&mut stream, &["HGET", "myhash", "nosuchfield"]).await;
    assert_resp(&resp, b"$-1\r\n");

    // HSET multiple fields at once
    let resp = cmd(&mut stream, &["HSET", "myhash", "f2", "v2", "f3", "v3"]).await;
    assert_resp(&resp, b":2\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_sadd_smembers() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    // SADD returns count of added members
    let resp = cmd(&mut stream, &["SADD", "myset", "a", "b", "c"]).await;
    assert_resp(&resp, b":3\r\n");

    // Adding duplicate
    let resp = cmd(&mut stream, &["SADD", "myset", "a"]).await;
    assert_resp(&resp, b":0\r\n");

    // SMEMBERS returns an array (order may vary for sets, so we parse and check)
    let resp = cmd(&mut stream, &["SMEMBERS", "myset"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    // Should be an array of 3 elements
    assert!(
        resp_str.starts_with("*3\r\n"),
        "Expected array of 3, got: {}",
        resp_str
    );
    // Check all members are present
    assert!(
        resp_str.contains("a"),
        "Missing member 'a' in: {}",
        resp_str
    );
    assert!(
        resp_str.contains("b"),
        "Missing member 'b' in: {}",
        resp_str
    );
    assert!(
        resp_str.contains("c"),
        "Missing member 'c' in: {}",
        resp_str
    );

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_dbsize() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    // Empty DB -> :0
    let resp = cmd(&mut stream, &["DBSIZE"]).await;
    assert_resp(&resp, b":0\r\n");

    cmd(&mut stream, &["SET", "k1", "v1"]).await;
    cmd(&mut stream, &["SET", "k2", "v2"]).await;

    let resp = cmd(&mut stream, &["DBSIZE"]).await;
    assert_resp(&resp, b":2\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_flushdb() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "k1", "v1"]).await;
    cmd(&mut stream, &["SET", "k2", "v2"]).await;

    // FLUSHDB -> +OK
    let resp = cmd(&mut stream, &["FLUSHDB"]).await;
    assert_resp(&resp, b"+OK\r\n");

    // DBSIZE should be 0
    let resp = cmd(&mut stream, &["DBSIZE"]).await;
    assert_resp(&resp, b":0\r\n");

    // Keys should be gone
    let resp = cmd(&mut stream, &["GET", "k1"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_pipelining() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    // Send multiple commands in a single write (pipelining)
    let mut pipeline = Vec::new();
    pipeline.extend_from_slice(&resp_cmd(&["SET", "pk1", "pv1"]));
    pipeline.extend_from_slice(&resp_cmd(&["SET", "pk2", "pv2"]));
    pipeline.extend_from_slice(&resp_cmd(&["GET", "pk1"]));
    pipeline.extend_from_slice(&resp_cmd(&["GET", "pk2"]));
    pipeline.extend_from_slice(&resp_cmd(&["DEL", "pk1", "pk2"]));

    let resp = send_and_read(&mut stream, &pipeline).await;
    let expected = [
        &b"+OK\r\n"[..],
        b"+OK\r\n",
        b"$3\r\npv1\r\n",
        b"$3\r\npv2\r\n",
        b":2\r\n",
    ]
    .concat();

    assert_resp(&resp, &expected);

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_pipeline_order_with_local_publish() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    // Mix deferred shard work (SET/GET) with local pub/sub handling (PUBLISH).
    let mut pipeline = Vec::new();
    pipeline.extend_from_slice(&resp_cmd(&["SET", "mix:key", "v1"]));
    pipeline.extend_from_slice(&resp_cmd(&["PUBLISH", "mix:ch", "msg"]));
    pipeline.extend_from_slice(&resp_cmd(&["GET", "mix:key"]));

    let resp = send_and_read(&mut stream, &pipeline).await;
    let expected = [&b"+OK\r\n"[..], b":0\r\n", b"$2\r\nv1\r\n"].concat();
    assert_resp(&resp, &expected);

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_info_perf_reports_stage_metrics() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["INFO", "perf"]).await;
    assert_resp_prefix(&resp, b"$");
    assert!(
        String::from_utf8_lossy(&resp).contains("# StageMetrics"),
        "expected INFO perf stage metrics body"
    );
    assert!(
        String::from_utf8_lossy(&resp).contains("stage_remote_delivery_count:"),
        "expected INFO perf to expose remote delivery metric"
    );

    let _ = cmd(&mut stream, &["SET", "perf:key", "value"]).await;
    let _ = cmd(&mut stream, &["GET", "perf:key"]).await;

    let resp = cmd(&mut stream, &["INFO", "perf"]).await;
    let execute_count = extract_info_metric(&resp, "stage_execute_count")
        .expect("stage_execute_count should be present");
    let serialize_count = extract_info_metric(&resp, "stage_serialize_count")
        .expect("stage_serialize_count should be present");

    assert!(execute_count > 0, "expected execute stage samples");
    assert!(serialize_count > 0, "expected serialize stage samples");

    let _ = shutdown.send(true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_single_worker_pipeline_16_concurrent_stability() {
    let port = free_port().await;
    let shutdown = start_server_with_workers(port, 1).await;

    let client_count = 24usize;
    let rounds = 24usize;
    let mut tasks = Vec::with_capacity(client_count);

    for client_id in 0..client_count {
        tasks.push(tokio::spawn(async move {
            let mut stream = connect(port).await;

            for round in 0..rounds {
                let mut set_pipeline = Vec::new();
                let expected_set = b"+OK\r\n".repeat(16);

                for slot in 0..16 {
                    let key = format!("w1:{client_id}:{round}:{slot}");
                    let value = format!("value:{slot}");
                    set_pipeline.extend_from_slice(&resp_cmd(&["SET", &key, &value]));
                }

                stream.write_all(&set_pipeline).await.unwrap();
                let set_resp = read_exact_response(&mut stream, expected_set.len()).await;
                assert_eq!(set_resp, expected_set);

                let mut get_pipeline = Vec::new();
                let mut expected_get = Vec::new();

                for slot in 0..16 {
                    let key = format!("w1:{client_id}:{round}:{slot}");
                    let value = format!("value:{slot}");
                    get_pipeline.extend_from_slice(&resp_cmd(&["GET", &key]));
                    expected_get
                        .extend_from_slice(format!("${}\r\n{}\r\n", value.len(), value).as_bytes());
                }

                stream.write_all(&get_pipeline).await.unwrap();
                let get_resp = read_exact_response(&mut stream, expected_get.len()).await;
                assert_eq!(get_resp, expected_get);
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_error_wrong_arity() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    // GET with no arguments -> wrong arity error
    let resp = cmd(&mut stream, &["GET"]).await;
    assert_resp_prefix(&resp, b"-ERR wrong number of arguments for 'GET'");

    // SET with one argument -> wrong arity error
    let resp = cmd(&mut stream, &["SET", "onlykey"]).await;
    assert_resp_prefix(&resp, b"-ERR wrong number of arguments for 'SET'");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_error_wrongtype() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    // Set a string key, then try list operation on it
    cmd(&mut stream, &["SET", "strkey", "value"]).await;

    let resp = cmd(&mut stream, &["LPUSH", "strkey", "item"]).await;
    // Should get a WRONGTYPE error
    assert_resp_prefix(&resp, b"-WRONGTYPE");

    // Set a list key, then try string operation on it
    cmd(&mut stream, &["LPUSH", "listkey", "item"]).await;
    let resp = cmd(&mut stream, &["INCR", "listkey"]).await;
    assert_resp_prefix(&resp, b"-WRONGTYPE");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_unknown_command() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["FOOBAR"]).await;
    assert_resp_prefix(&resp, b"-ERR unknown command");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_set_overwrite() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "key", "first"]).await;
    cmd(&mut stream, &["SET", "key", "second"]).await;
    let resp = cmd(&mut stream, &["GET", "key"]).await;
    assert_resp(&resp, b"$6\r\nsecond\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_echo() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["ECHO", "hello world"]).await;
    assert_resp(&resp, b"$11\r\nhello world\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_incr_on_non_integer() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "notnum", "abc"]).await;
    let resp = cmd(&mut stream, &["INCR", "notnum"]).await;
    // Should be an error about not being an integer
    assert_resp_prefix(&resp, b"-");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_multiple_data_types_isolation() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    // Create different data types on different keys
    cmd(&mut stream, &["SET", "str", "val"]).await;
    cmd(&mut stream, &["LPUSH", "lst", "a"]).await;
    cmd(&mut stream, &["HSET", "hsh", "f", "v"]).await;
    cmd(&mut stream, &["SADD", "st", "m"]).await;

    // Verify each works independently
    let resp = cmd(&mut stream, &["GET", "str"]).await;
    assert_resp(&resp, b"$3\r\nval\r\n");

    let resp = cmd(&mut stream, &["LRANGE", "lst", "0", "-1"]).await;
    assert_resp(&resp, b"*1\r\n$1\r\na\r\n");

    let resp = cmd(&mut stream, &["HGET", "hsh", "f"]).await;
    assert_resp(&resp, b"$1\r\nv\r\n");

    let resp = cmd(&mut stream, &["SMEMBERS", "st"]).await;
    assert_resp(&resp, b"*1\r\n$1\r\nm\r\n");

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// Pub/Sub Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pubsub_subscribe_and_publish() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut sub = connect(port).await;
    let mut pub_conn = connect(port).await;

    let resp = cmd(&mut sub, &["SUBSCRIBE", "chat"]).await;
    assert_resp(&resp, b"*3\r\n$9\r\nsubscribe\r\n$4\r\nchat\r\n:1\r\n");

    sub.write_all(&resp_cmd(&["SUBSCRIBE", "news"]))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut buf = vec![0u8; 8192];
    let n = sub.read(&mut buf).await.unwrap();
    buf.truncate(n);
    assert_resp(&buf, b"*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:2\r\n");

    let resp = cmd(&mut pub_conn, &["PUBLISH", "chat", "hello"]).await;
    assert_resp(&resp, b":1\r\n");

    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut buf = vec![0u8; 8192];
    let n = sub.read(&mut buf).await.unwrap();
    buf.truncate(n);
    let expected = b"*3\r\n$7\r\nmessage\r\n$4\r\nchat\r\n$5\r\nhello\r\n";
    assert_resp(&buf, expected);

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_pubsub_publish_no_subscribers() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut pub_conn = connect(port).await;

    let resp = cmd(&mut pub_conn, &["PUBLISH", "empty", "msg"]).await;
    assert_resp(&resp, b":0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_pubsub_psubscribe() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut sub = connect(port).await;
    let mut pub_conn = connect(port).await;

    let resp = cmd(&mut sub, &["PSUBSCRIBE", "chat.*"]).await;
    assert_resp(&resp, b"*3\r\n$10\r\npsubscribe\r\n$6\r\nchat.*\r\n:1\r\n");

    let resp = cmd(&mut pub_conn, &["PUBLISH", "chat.general", "hi"]).await;
    assert_resp(&resp, b":1\r\n");

    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut buf = vec![0u8; 8192];
    let n = sub.read(&mut buf).await.unwrap();
    buf.truncate(n);
    let expected = b"*4\r\n$8\r\npmessage\r\n$6\r\nchat.*\r\n$12\r\nchat.general\r\n$2\r\nhi\r\n";
    assert_resp(&buf, expected);

    let resp = cmd(&mut pub_conn, &["PUBLISH", "news.sports", "goal"]).await;
    assert_resp(&resp, b":0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_pubsub_unsubscribe() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut sub = connect(port).await;
    let mut pub_conn = connect(port).await;

    cmd(&mut sub, &["SUBSCRIBE", "ch1"]).await;

    let resp = cmd(&mut sub, &["UNSUBSCRIBE", "ch1"]).await;
    assert_resp(&resp, b"*3\r\n$11\r\nunsubscribe\r\n$3\r\nch1\r\n:0\r\n");

    let resp = cmd(&mut pub_conn, &["PUBLISH", "ch1", "gone"]).await;
    assert_resp(&resp, b":0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_pubsub_multiple_channels() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut sub = connect(port).await;
    let mut pub_conn = connect(port).await;

    let resp = cmd(&mut sub, &["SUBSCRIBE", "ch-a", "ch-b"]).await;
    let expected = [
        &b"*3\r\n$9\r\nsubscribe\r\n$4\r\nch-a\r\n:1\r\n"[..],
        b"*3\r\n$9\r\nsubscribe\r\n$4\r\nch-b\r\n:2\r\n",
    ]
    .concat();
    assert_resp(&resp, &expected);

    let resp = cmd(&mut pub_conn, &["PUBLISH", "ch-a", "msg-a"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut pub_conn, &["PUBLISH", "ch-b", "msg-b"]).await;
    assert_resp(&resp, b":1\r\n");

    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut buf = vec![0u8; 16384];
    let n = sub.read(&mut buf).await.unwrap();
    buf.truncate(n);

    let expected = [
        &b"*3\r\n$7\r\nmessage\r\n$4\r\nch-a\r\n$5\r\nmsg-a\r\n"[..],
        b"*3\r\n$7\r\nmessage\r\n$4\r\nch-b\r\n$5\r\nmsg-b\r\n",
    ]
    .concat();
    assert_resp(&buf, &expected);

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// Persistence Tests
// ---------------------------------------------------------------------------

async fn start_server_with_storage(
    port: u16,
    data_dir: PathBuf,
) -> tokio::sync::watch::Sender<bool> {
    start_server_with_config(ServerConfig {
        bind_address: format!("127.0.0.1:{}", port),
        worker_count: 2,
        storage: Some(StorageConfig {
            data_dir,
            wal_sync_policy: SyncPolicy::EveryWrite,
            wal_enabled: true,
            rdb_enabled: true,
            snapshot_interval_secs: None,
            snapshot_retain: Some(24),
            wal_max_bytes: 64 * 1024 * 1024,
        }),
        ..Default::default()
    })
    .await
}

#[tokio::test]
async fn test_wal_writes_persist_to_disk() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let data_dir = tmp_dir.path().to_path_buf();
    let port = free_port().await;

    let shutdown = start_server_with_storage(port, data_dir.clone()).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "persist:key1", "value1"]).await;
    cmd(&mut stream, &["SET", "persist:key2", "value2"]).await;
    cmd(&mut stream, &["INCR", "persist:counter"]).await;
    cmd(&mut stream, &["LPUSH", "persist:list", "a"]).await;
    cmd(&mut stream, &["HSET", "persist:hash", "f1", "v1"]).await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    let resp = cmd(&mut stream, &["GET", "persist:key1"]).await;
    assert_resp(&resp, b"$6\r\nvalue1\r\n");

    let resp = cmd(&mut stream, &["GET", "persist:counter"]).await;
    assert_resp(&resp, b"$1\r\n1\r\n");

    let _ = shutdown.send(true);
    drop(stream);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let shard0_wal = data_dir.join("shard-0/shard.wal");
    let shard1_wal = data_dir.join("shard-1/shard.wal");
    assert!(
        shard0_wal.exists() || shard1_wal.exists(),
        "At least one shard WAL file should exist after writes"
    );

    let total_wal_size: u64 = [&shard0_wal, &shard1_wal]
        .iter()
        .filter_map(|p| std::fs::metadata(p).ok())
        .map(|m| m.len())
        .sum();
    assert!(
        total_wal_size > 0,
        "WAL files should contain data after writes"
    );
}

#[tokio::test]
async fn test_wal_replay_restores_data() {
    use kora_storage::shard_storage::ShardStorage;
    use kora_storage::wal::WalEntry;

    let tmp_dir = tempfile::tempdir().unwrap();
    let data_dir = tmp_dir.path().to_path_buf();
    let port = free_port().await;

    let shutdown = start_server_with_storage(port, data_dir.clone()).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "replay:key1", "hello"]).await;
    cmd(&mut stream, &["SET", "replay:key2", "world"]).await;
    cmd(&mut stream, &["SET", "replay:key3", "test"]).await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = shutdown.send(true);
    drop(stream);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut replayed_entries = Vec::new();
    for shard_id in 0..2u16 {
        if let Ok(storage) = ShardStorage::open_with_config(
            shard_id,
            &data_dir,
            SyncPolicy::EveryWrite,
            true,
            true,
            0,
        ) {
            let _ = storage.wal_replay(|entry| {
                replayed_entries.push(entry);
            });
        }
    }

    let set_count = replayed_entries
        .iter()
        .filter(|e| matches!(e, WalEntry::Set { .. }))
        .count();
    assert!(
        set_count >= 3,
        "Expected at least 3 SET entries in WAL replay, got {}",
        set_count
    );

    let has_key1 = replayed_entries.iter().any(|e| {
        if let WalEntry::Set { key, value, .. } = e {
            key == b"replay:key1" && value == b"hello"
        } else {
            false
        }
    });
    assert!(has_key1, "WAL replay should contain replay:key1 = hello");
}

#[tokio::test]
async fn test_bgsave_creates_rdb() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let data_dir = tmp_dir.path().to_path_buf();

    let port = free_port().await;
    let shutdown = start_server_with_storage(port, data_dir.clone()).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "restart:key", "persistent_value"]).await;

    let resp = cmd(&mut stream, &["BGSAVE"]).await;
    assert_resp(&resp, b"+Background saving started\r\n");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let _ = shutdown.send(true);
    drop(stream);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let has_rdb = (0..2u16).any(|id| data_dir.join(format!("shard-{}/shard.rdb", id)).exists());
    assert!(has_rdb, "RDB snapshot should exist after BGSAVE");
}

#[tokio::test]
async fn test_bgrewriteaof_errors_when_persistence_missing_or_wal_disabled() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["BGREWRITEAOF"]).await;
    assert_resp_prefix(&resp, b"-ERR persistence not configured");

    let _ = shutdown.send(true);

    let tmp_dir = tempfile::tempdir().unwrap();
    let data_dir = tmp_dir.path().to_path_buf();
    let disabled_port = free_port().await;
    let shutdown = start_server_with_config(ServerConfig {
        bind_address: format!("127.0.0.1:{}", disabled_port),
        worker_count: 1,
        storage: Some(StorageConfig {
            data_dir,
            wal_sync_policy: SyncPolicy::EveryWrite,
            wal_enabled: false,
            rdb_enabled: true,
            snapshot_interval_secs: None,
            snapshot_retain: Some(24),
            wal_max_bytes: 64 * 1024 * 1024,
        }),
        ..Default::default()
    })
    .await;
    let mut stream = connect(disabled_port).await;

    let resp = cmd(&mut stream, &["BGREWRITEAOF"]).await;
    assert_resp_prefix(&resp, b"-ERR WAL disabled");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_bgrewriteaof_truncates_wal() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let data_dir = tmp_dir.path().to_path_buf();
    let wal_path = data_dir.join("shard-0").join("shard.wal");
    let port = free_port().await;
    let shutdown = start_server_with_config(ServerConfig {
        bind_address: format!("127.0.0.1:{}", port),
        worker_count: 1,
        storage: Some(StorageConfig {
            data_dir: data_dir.clone(),
            wal_sync_policy: SyncPolicy::EveryWrite,
            wal_enabled: true,
            rdb_enabled: true,
            snapshot_interval_secs: None,
            snapshot_retain: Some(24),
            wal_max_bytes: 64 * 1024 * 1024,
        }),
        ..Default::default()
    })
    .await;
    let mut stream = connect(port).await;

    for i in 0..400 {
        let key = format!("rewrite:key:{i}");
        let value = format!("payload-{}-{}", i, "x".repeat(64));
        let resp = cmd(&mut stream, &["SET", key.as_str(), value.as_str()]).await;
        assert_resp(&resp, b"+OK\r\n");
    }

    let initial_len = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    assert!(
        initial_len > 0,
        "expected WAL to contain data before rewrite"
    );

    let resp = cmd(&mut stream, &["BGREWRITEAOF"]).await;
    assert_resp(&resp, b"+Background AOF rewrite started\r\n");

    let truncate_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let len = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        if len == 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < truncate_deadline,
            "WAL did not truncate to zero after BGREWRITEAOF (latest len={len})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_periodic_snapshots_create_backups_and_prune_retention() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let data_dir = tmp_dir.path().to_path_buf();
    let port = free_port().await;

    let (shutdown, rx) = tokio::sync::watch::channel(false);
    let config = ServerConfig {
        bind_address: format!("127.0.0.1:{}", port),
        worker_count: 2,
        storage: Some(StorageConfig {
            data_dir: data_dir.clone(),
            wal_sync_policy: SyncPolicy::EveryWrite,
            wal_enabled: true,
            rdb_enabled: true,
            snapshot_interval_secs: Some(1),
            snapshot_retain: Some(2),
            wal_max_bytes: 64 * 1024 * 1024,
        }),
        ..Default::default()
    };
    let server = KoraServer::new(config);
    tokio::spawn(async move {
        let _ = server.run(rx).await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut stream = connect(port).await;
    cmd(&mut stream, &["SET", "autosnap:key", "value"]).await;

    tokio::time::sleep(Duration::from_millis(3300)).await;

    let mut snapshot_count = 0usize;
    let mut found_latest = false;
    for shard in 0..2u16 {
        let shard_dir = data_dir.join(format!("shard-{}", shard));
        if shard_dir.join("shard.rdb").exists() {
            found_latest = true;
        }
        let snapshot_dir = shard_dir.join("snapshots");
        if snapshot_dir.exists() {
            let count = std::fs::read_dir(&snapshot_dir)
                .unwrap()
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    entry.path().extension().and_then(|ext| ext.to_str()) == Some("rdb")
                })
                .count();
            assert!(
                count <= 2,
                "expected retention pruning to keep at most 2 backups"
            );
            snapshot_count += count;
        }
    }

    assert!(found_latest, "latest shard snapshot should exist");
    assert!(
        snapshot_count > 0,
        "expected at least one timestamped snapshot backup"
    );

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// Phase 1: String & Key commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_setex_psetex() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["SETEX", "sk", "100", "val"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["GET", "sk"]).await;
    assert_resp(&resp, b"$3\r\nval\r\n");

    let resp = cmd(&mut stream, &["TTL", "sk"]).await;
    assert!(resp.starts_with(b":"));

    let resp = cmd(&mut stream, &["PSETEX", "pk", "100000", "pval"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["GET", "pk"]).await;
    assert_resp(&resp, b"$4\r\npval\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_incrbyfloat() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["SET", "fkey", "10.5"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["INCRBYFLOAT", "fkey", "0.1"]).await;
    assert_resp(&resp, b"$4\r\n10.6\r\n");

    let resp = cmd(&mut stream, &["INCRBYFLOAT", "fkey", "-5"]).await;
    assert_resp(&resp, b"$3\r\n5.6\r\n");

    let resp = cmd(&mut stream, &["INCRBYFLOAT", "newf", "3.14"]).await;
    assert_resp(&resp, b"$4\r\n3.14\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_getrange() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "grk", "Hello, World!"]).await;

    let resp = cmd(&mut stream, &["GETRANGE", "grk", "0", "4"]).await;
    assert_resp(&resp, b"$5\r\nHello\r\n");

    let resp = cmd(&mut stream, &["GETRANGE", "grk", "-6", "-1"]).await;
    assert_resp(&resp, b"$6\r\nWorld!\r\n");

    let resp = cmd(&mut stream, &["GETRANGE", "grk", "100", "200"]).await;
    assert_resp(&resp, b"$0\r\n\r\n");

    let resp = cmd(&mut stream, &["GETRANGE", "nokey", "0", "10"]).await;
    assert_resp(&resp, b"$0\r\n\r\n");

    let resp = cmd(&mut stream, &["SUBSTR", "grk", "0", "4"]).await;
    assert_resp(&resp, b"$5\r\nHello\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_setrange() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "srk", "Hello World"]).await;

    let resp = cmd(&mut stream, &["SETRANGE", "srk", "6", "Redis"]).await;
    assert_resp(&resp, b":11\r\n");

    let resp = cmd(&mut stream, &["GET", "srk"]).await;
    assert_resp(&resp, b"$11\r\nHello Redis\r\n");

    let resp = cmd(&mut stream, &["SETRANGE", "newsr", "5", "hi"]).await;
    assert_resp(&resp, b":7\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_getdel() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "gdk", "val"]).await;

    let resp = cmd(&mut stream, &["GETDEL", "gdk"]).await;
    assert_resp(&resp, b"$3\r\nval\r\n");

    let resp = cmd(&mut stream, &["GET", "gdk"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let resp = cmd(&mut stream, &["GETDEL", "noexist"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_getex() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "gek", "val"]).await;

    let resp = cmd(&mut stream, &["GETEX", "gek", "EX", "100"]).await;
    assert_resp(&resp, b"$3\r\nval\r\n");

    let resp = cmd(&mut stream, &["TTL", "gek"]).await;
    assert!(resp.starts_with(b":"));
    let ttl_str = String::from_utf8_lossy(&resp);
    let ttl: i64 = ttl_str
        .trim()
        .trim_start_matches(':')
        .trim()
        .parse()
        .unwrap_or(-1);
    assert!(ttl > 0 && ttl <= 100);

    let resp = cmd(&mut stream, &["GETEX", "gek", "PERSIST"]).await;
    assert_resp(&resp, b"$3\r\nval\r\n");

    let resp = cmd(&mut stream, &["TTL", "gek"]).await;
    assert_resp(&resp, b":-1\r\n");

    let resp = cmd(&mut stream, &["GETEX", "noexist"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_msetnx() {
    let port = free_port().await;
    let shutdown = start_server_with_workers(port, 1).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["MSETNX", "mn1", "v1", "mn2", "v2"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["GET", "mn1"]).await;
    assert_resp(&resp, b"$2\r\nv1\r\n");

    let resp = cmd(&mut stream, &["MSETNX", "mn1", "new", "mn3", "v3"]).await;
    assert_resp(&resp, b":0\r\n");

    let resp = cmd(&mut stream, &["GET", "mn1"]).await;
    assert_resp(&resp, b"$2\r\nv1\r\n");

    let resp = cmd(&mut stream, &["GET", "mn3"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_msetnx_cross_shard_no_partial_write() {
    let port = free_port().await;
    let shutdown = start_server_with_workers(port, 2).await;
    let mut stream = connect(port).await;
    let (existing_key, other_key) = keys_on_different_shards(2, "msetnx-x:");

    let resp = cmd(&mut stream, &["SET", existing_key.as_str(), "base"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(
        &mut stream,
        &[
            "MSETNX",
            existing_key.as_str(),
            "new",
            other_key.as_str(),
            "other",
        ],
    )
    .await;
    assert_resp(&resp, b":0\r\n");

    let resp = cmd(&mut stream, &["GET", existing_key.as_str()]).await;
    assert_resp(&resp, b"$4\r\nbase\r\n");

    let resp = cmd(&mut stream, &["GET", other_key.as_str()]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_expireat_pexpireat() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "eak", "val"]).await;

    let future_ts = (std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 1000)
        .to_string();

    let resp = cmd(&mut stream, &["EXPIREAT", "eak", &future_ts]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["TTL", "eak"]).await;
    let ttl_str = String::from_utf8_lossy(&resp);
    let ttl: i64 = ttl_str
        .trim()
        .trim_start_matches(':')
        .trim()
        .parse()
        .unwrap_or(-1);
    assert!(ttl > 0);

    let resp = cmd(&mut stream, &["EXPIREAT", "noexist", &future_ts]).await;
    assert_resp(&resp, b":0\r\n");

    cmd(&mut stream, &["SET", "peak", "val"]).await;
    let future_ms = (std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 1_000_000)
        .to_string();
    let resp = cmd(&mut stream, &["PEXPIREAT", "peak", &future_ms]).await;
    assert_resp(&resp, b":1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_rename() {
    let port = free_port().await;
    let shutdown = start_server_with_workers(port, 1).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "rk1", "val"]).await;

    let resp = cmd(&mut stream, &["RENAME", "rk1", "rk2"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["GET", "rk1"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let resp = cmd(&mut stream, &["GET", "rk2"]).await;
    assert_resp(&resp, b"$3\r\nval\r\n");

    let resp = cmd(&mut stream, &["RENAME", "noexist", "rk3"]).await;
    assert_resp_prefix(&resp, b"-ERR no such key");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_renamenx() {
    let port = free_port().await;
    let shutdown = start_server_with_workers(port, 1).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "rnx1", "v1"]).await;
    cmd(&mut stream, &["SET", "rnx2", "v2"]).await;

    let resp = cmd(&mut stream, &["RENAMENX", "rnx1", "rnx2"]).await;
    assert_resp(&resp, b":0\r\n");

    let resp = cmd(&mut stream, &["GET", "rnx1"]).await;
    assert_resp(&resp, b"$2\r\nv1\r\n");

    let resp = cmd(&mut stream, &["RENAMENX", "rnx1", "rnx3"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["GET", "rnx3"]).await;
    assert_resp(&resp, b"$2\r\nv1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_unlink() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "uk1", "v1"]).await;
    cmd(&mut stream, &["SET", "uk2", "v2"]).await;

    let resp = cmd(&mut stream, &["UNLINK", "uk1", "uk2", "uk3"]).await;
    assert_resp(&resp, b":2\r\n");

    let resp = cmd(&mut stream, &["GET", "uk1"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_copy() {
    let port = free_port().await;
    let shutdown = start_server_with_workers(port, 1).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "csrc", "val"]).await;

    let resp = cmd(&mut stream, &["COPY", "csrc", "cdst"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["GET", "cdst"]).await;
    assert_resp(&resp, b"$3\r\nval\r\n");

    let resp = cmd(&mut stream, &["GET", "csrc"]).await;
    assert_resp(&resp, b"$3\r\nval\r\n");

    cmd(&mut stream, &["SET", "cdst", "existing"]).await;
    let resp = cmd(&mut stream, &["COPY", "csrc", "cdst"]).await;
    assert_resp(&resp, b":0\r\n");

    let resp = cmd(&mut stream, &["COPY", "csrc", "cdst", "REPLACE"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["GET", "cdst"]).await;
    assert_resp(&resp, b"$3\r\nval\r\n");

    let resp = cmd(&mut stream, &["COPY", "noexist", "cdst2"]).await;
    assert_resp(&resp, b":0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_randomkey() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["RANDOMKEY"]).await;
    assert_resp(&resp, b"$-1\r\n");

    cmd(&mut stream, &["SET", "rk", "val"]).await;
    let resp = cmd(&mut stream, &["RANDOMKEY"]).await;
    assert!(resp.starts_with(b"$"));
    assert!(!resp.starts_with(b"$-1"));

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_touch() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "tk1", "v1"]).await;
    cmd(&mut stream, &["SET", "tk2", "v2"]).await;

    let resp = cmd(&mut stream, &["TOUCH", "tk1", "tk2", "noexist"]).await;
    assert_resp(&resp, b":2\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_object_refcount_idletime_help() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "ok", "val"]).await;

    let resp = cmd(&mut stream, &["OBJECT", "REFCOUNT", "ok"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["OBJECT", "IDLETIME", "ok"]).await;
    assert_resp(&resp, b":0\r\n");

    let resp = cmd(&mut stream, &["OBJECT", "REFCOUNT", "noexist"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let resp = cmd(&mut stream, &["OBJECT", "HELP"]).await;
    assert!(resp.starts_with(b"*"));

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// Phase 2: List commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_lset() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["RPUSH", "mylist", "a", "b", "c"]).await;

    let resp = cmd(&mut stream, &["LSET", "mylist", "1", "B"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["LINDEX", "mylist", "1"]).await;
    assert_resp(&resp, b"$1\r\nB\r\n");

    let resp = cmd(&mut stream, &["LSET", "mylist", "-1", "C"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["LINDEX", "mylist", "2"]).await;
    assert_resp(&resp, b"$1\r\nC\r\n");

    let resp = cmd(&mut stream, &["LSET", "mylist", "10", "x"]).await;
    assert_resp_prefix(&resp, b"-ERR index out of range");

    let resp = cmd(&mut stream, &["LSET", "noexist", "0", "x"]).await;
    assert_resp_prefix(&resp, b"-ERR no such key");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_linsert() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["RPUSH", "mylist", "a", "c"]).await;

    let resp = cmd(&mut stream, &["LINSERT", "mylist", "BEFORE", "c", "b"]).await;
    assert_resp(&resp, b":3\r\n");

    let resp = cmd(&mut stream, &["LRANGE", "mylist", "0", "-1"]).await;
    assert_resp(&resp, b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");

    let resp = cmd(&mut stream, &["LINSERT", "mylist", "AFTER", "c", "d"]).await;
    assert_resp(&resp, b":4\r\n");

    let resp = cmd(
        &mut stream,
        &["LINSERT", "mylist", "BEFORE", "notfound", "x"],
    )
    .await;
    assert_resp(&resp, b":-1\r\n");

    let resp = cmd(&mut stream, &["LINSERT", "noexist", "BEFORE", "a", "x"]).await;
    assert_resp(&resp, b":0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_lrem() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["RPUSH", "mylist", "a", "b", "a", "c", "a"]).await;

    let resp = cmd(&mut stream, &["LREM", "mylist", "2", "a"]).await;
    assert_resp(&resp, b":2\r\n");

    let resp = cmd(&mut stream, &["LRANGE", "mylist", "0", "-1"]).await;
    assert_resp(&resp, b"*3\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\na\r\n");

    cmd(&mut stream, &["DEL", "mylist"]).await;
    cmd(&mut stream, &["RPUSH", "mylist", "a", "b", "a", "c", "a"]).await;

    let resp = cmd(&mut stream, &["LREM", "mylist", "-2", "a"]).await;
    assert_resp(&resp, b":2\r\n");

    let resp = cmd(&mut stream, &["LRANGE", "mylist", "0", "-1"]).await;
    assert_resp(&resp, b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_ltrim() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["RPUSH", "mylist", "a", "b", "c", "d", "e"]).await;

    let resp = cmd(&mut stream, &["LTRIM", "mylist", "1", "3"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["LRANGE", "mylist", "0", "-1"]).await;
    assert_resp(&resp, b"*3\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_lpos() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(
        &mut stream,
        &["RPUSH", "mylist", "a", "b", "c", "a", "b", "a"],
    )
    .await;

    let resp = cmd(&mut stream, &["LPOS", "mylist", "a"]).await;
    assert_resp(&resp, b":0\r\n");

    let resp = cmd(&mut stream, &["LPOS", "mylist", "a", "RANK", "2"]).await;
    assert_resp(&resp, b":3\r\n");

    let resp = cmd(&mut stream, &["LPOS", "mylist", "a", "COUNT", "0"]).await;
    assert_resp(&resp, b"*3\r\n:0\r\n:3\r\n:5\r\n");

    let resp = cmd(&mut stream, &["LPOS", "mylist", "a", "COUNT", "2"]).await;
    assert_resp(&resp, b"*2\r\n:0\r\n:3\r\n");

    let resp = cmd(&mut stream, &["LPOS", "mylist", "z"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_rpoplpush() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["RPUSH", "src", "a", "b", "c"]).await;

    let resp = cmd(&mut stream, &["RPOPLPUSH", "src", "dst"]).await;
    assert_resp(&resp, b"$1\r\nc\r\n");

    let resp = cmd(&mut stream, &["LRANGE", "src", "0", "-1"]).await;
    assert_resp(&resp, b"*2\r\n$1\r\na\r\n$1\r\nb\r\n");

    let resp = cmd(&mut stream, &["LRANGE", "dst", "0", "-1"]).await;
    assert_resp(&resp, b"*1\r\n$1\r\nc\r\n");

    let resp = cmd(&mut stream, &["RPOPLPUSH", "noexist", "dst"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_lmove() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["RPUSH", "src", "a", "b", "c"]).await;

    let resp = cmd(&mut stream, &["LMOVE", "src", "dst", "LEFT", "RIGHT"]).await;
    assert_resp(&resp, b"$1\r\na\r\n");

    let resp = cmd(&mut stream, &["LMOVE", "src", "dst", "RIGHT", "LEFT"]).await;
    assert_resp(&resp, b"$1\r\nc\r\n");

    let resp = cmd(&mut stream, &["LRANGE", "src", "0", "-1"]).await;
    assert_resp(&resp, b"*1\r\n$1\r\nb\r\n");

    let resp = cmd(&mut stream, &["LRANGE", "dst", "0", "-1"]).await;
    assert_resp(&resp, b"*2\r\n$1\r\nc\r\n$1\r\na\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_lmove_cross_shard_wrongtype_preserves_source() {
    let port = free_port().await;
    let shutdown = start_server_with_workers(port, 2).await;
    let mut stream = connect(port).await;
    let (source, destination) = keys_on_different_shards(2, "lmove-x:");

    cmd(&mut stream, &["RPUSH", source.as_str(), "a"]).await;
    cmd(&mut stream, &["SET", destination.as_str(), "notlist"]).await;

    let resp = cmd(
        &mut stream,
        &[
            "LMOVE",
            source.as_str(),
            destination.as_str(),
            "RIGHT",
            "LEFT",
        ],
    )
    .await;
    assert_resp_prefix(&resp, b"-WRONGTYPE");

    let resp = cmd(&mut stream, &["LLEN", source.as_str()]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["GET", destination.as_str()]).await;
    assert_resp(&resp, b"$7\r\nnotlist\r\n");

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// Phase 2: Hash commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_hmget() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(
        &mut stream,
        &["HSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3"],
    )
    .await;

    let resp = cmd(&mut stream, &["HMGET", "myhash", "f1", "f3", "nofield"]).await;
    assert_resp(&resp, b"*3\r\n$2\r\nv1\r\n$2\r\nv3\r\n$-1\r\n");

    let resp = cmd(&mut stream, &["HMGET", "noexist", "f1", "f2"]).await;
    assert_resp(&resp, b"*2\r\n$-1\r\n$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_hmset() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["HMSET", "myhash", "f1", "v1", "f2", "v2"]).await;
    assert_resp(&resp, b":2\r\n");

    let resp = cmd(&mut stream, &["HGET", "myhash", "f1"]).await;
    assert_resp(&resp, b"$2\r\nv1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_hkeys_hvals() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["HSET", "myhash", "f1", "v1", "f2", "v2"]).await;

    let resp = cmd(&mut stream, &["HKEYS", "myhash"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(resp_str.starts_with("*2\r\n"));
    assert!(resp_str.contains("f1"));
    assert!(resp_str.contains("f2"));

    let resp = cmd(&mut stream, &["HVALS", "myhash"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(resp_str.starts_with("*2\r\n"));
    assert!(resp_str.contains("v1"));
    assert!(resp_str.contains("v2"));

    let resp = cmd(&mut stream, &["HKEYS", "noexist"]).await;
    assert_resp(&resp, b"*0\r\n");

    let resp = cmd(&mut stream, &["HVALS", "noexist"]).await;
    assert_resp(&resp, b"*0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_hsetnx() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["HSETNX", "myhash", "f1", "v1"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["HSETNX", "myhash", "f1", "v2"]).await;
    assert_resp(&resp, b":0\r\n");

    let resp = cmd(&mut stream, &["HGET", "myhash", "f1"]).await;
    assert_resp(&resp, b"$2\r\nv1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_hincrbyfloat() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["HINCRBYFLOAT", "myhash", "f1", "10.5"]).await;
    assert_resp(&resp, b"$4\r\n10.5\r\n");

    let resp = cmd(&mut stream, &["HINCRBYFLOAT", "myhash", "f1", "0.1"]).await;
    assert_resp(&resp, b"$4\r\n10.6\r\n");

    let resp = cmd(&mut stream, &["HINCRBYFLOAT", "myhash", "f1", "-5"]).await;
    assert_resp(&resp, b"$3\r\n5.6\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_hrandfield() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(
        &mut stream,
        &["HSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3"],
    )
    .await;

    let resp = cmd(&mut stream, &["HRANDFIELD", "myhash"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(resp_str.contains("f1") || resp_str.contains("f2") || resp_str.contains("f3"));

    let resp = cmd(&mut stream, &["HRANDFIELD", "myhash", "2"]).await;
    assert!(resp.starts_with(b"*2\r\n"));

    let resp = cmd(&mut stream, &["HRANDFIELD", "myhash", "-5"]).await;
    assert!(resp.starts_with(b"*5\r\n"));

    let resp = cmd(&mut stream, &["HRANDFIELD", "noexist"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_hscan() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(
        &mut stream,
        &["HSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3"],
    )
    .await;

    let resp = cmd(&mut stream, &["HSCAN", "myhash", "0"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(resp_str.starts_with("*2\r\n"));
    assert!(resp_str.contains("f1"));

    let resp = cmd(
        &mut stream,
        &["HSCAN", "myhash", "0", "MATCH", "f*", "COUNT", "2"],
    )
    .await;
    assert!(resp.starts_with(b"*2\r\n"));

    let resp = cmd(&mut stream, &["HSCAN", "noexist", "0"]).await;
    assert_resp(&resp, b"*2\r\n$1\r\n0\r\n*0\r\n");

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// Phase 3: Set commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_spop() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SADD", "myset", "a", "b", "c"]).await;

    let resp = cmd(&mut stream, &["SPOP", "myset"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.contains("a") || resp_str.contains("b") || resp_str.contains("c"),
        "Expected a member, got: {}",
        resp_str
    );

    let resp = cmd(&mut stream, &["SPOP", "myset", "2"]).await;
    assert!(resp.starts_with(b"*2\r\n") || resp.starts_with(b"*1\r\n"));

    let resp = cmd(&mut stream, &["SPOP", "noexist"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let resp = cmd(&mut stream, &["SPOP", "noexist", "1"]).await;
    assert_resp(&resp, b"*0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_srandmember() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SADD", "myset", "a", "b", "c"]).await;

    let resp = cmd(&mut stream, &["SRANDMEMBER", "myset"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.contains("a") || resp_str.contains("b") || resp_str.contains("c"),
        "Expected a member, got: {}",
        resp_str
    );

    let resp = cmd(&mut stream, &["SRANDMEMBER", "myset", "2"]).await;
    assert!(resp.starts_with(b"*2\r\n"));

    let resp = cmd(&mut stream, &["SRANDMEMBER", "myset", "-5"]).await;
    assert!(resp.starts_with(b"*5\r\n"));

    let resp = cmd(&mut stream, &["SRANDMEMBER", "noexist"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_sunion_sinter_sdiff() {
    let port = free_port().await;
    let shutdown = start_server_with_workers(port, 1).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SADD", "s1", "a", "b", "c"]).await;
    cmd(&mut stream, &["SADD", "s2", "b", "c", "d"]).await;

    let resp = cmd(&mut stream, &["SUNION", "s1", "s2"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.starts_with("*4\r\n"),
        "Expected 4 members in union, got: {}",
        resp_str
    );

    let resp = cmd(&mut stream, &["SINTER", "s1", "s2"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.starts_with("*2\r\n"),
        "Expected 2 members in intersection, got: {}",
        resp_str
    );

    let resp = cmd(&mut stream, &["SDIFF", "s1", "s2"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.starts_with("*1\r\n"),
        "Expected 1 member in diff, got: {}",
        resp_str
    );
    assert!(resp_str.contains("a"));

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_sunionstore_sinterstore_sdiffstore() {
    let port = free_port().await;
    let shutdown = start_server_with_workers(port, 1).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SADD", "s1", "a", "b", "c"]).await;
    cmd(&mut stream, &["SADD", "s2", "b", "c", "d"]).await;

    let resp = cmd(&mut stream, &["SUNIONSTORE", "dest1", "s1", "s2"]).await;
    assert_resp(&resp, b":4\r\n");

    let resp = cmd(&mut stream, &["SCARD", "dest1"]).await;
    assert_resp(&resp, b":4\r\n");

    let resp = cmd(&mut stream, &["SINTERSTORE", "dest2", "s1", "s2"]).await;
    assert_resp(&resp, b":2\r\n");

    let resp = cmd(&mut stream, &["SCARD", "dest2"]).await;
    assert_resp(&resp, b":2\r\n");

    let resp = cmd(&mut stream, &["SDIFFSTORE", "dest3", "s1", "s2"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["SCARD", "dest3"]).await;
    assert_resp(&resp, b":1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_sintercard() {
    let port = free_port().await;
    let shutdown = start_server_with_workers(port, 1).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SADD", "s1", "a", "b", "c"]).await;
    cmd(&mut stream, &["SADD", "s2", "b", "c", "d"]).await;

    let resp = cmd(&mut stream, &["SINTERCARD", "2", "s1", "s2"]).await;
    assert_resp(&resp, b":2\r\n");

    let resp = cmd(&mut stream, &["SINTERCARD", "2", "s1", "s2", "LIMIT", "1"]).await;
    assert_resp(&resp, b":1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_smove() {
    let port = free_port().await;
    let shutdown = start_server_with_workers(port, 1).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SADD", "src", "a", "b"]).await;
    cmd(&mut stream, &["SADD", "dst", "c"]).await;

    let resp = cmd(&mut stream, &["SMOVE", "src", "dst", "a"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["SISMEMBER", "src", "a"]).await;
    assert_resp(&resp, b":0\r\n");

    let resp = cmd(&mut stream, &["SISMEMBER", "dst", "a"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["SMOVE", "src", "dst", "nonexist"]).await;
    assert_resp(&resp, b":0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_smove_cross_shard_wrongtype_preserves_source() {
    let port = free_port().await;
    let shutdown = start_server_with_workers(port, 2).await;
    let mut stream = connect(port).await;
    let (source, destination) = keys_on_different_shards(2, "smove-x:");

    cmd(&mut stream, &["SADD", source.as_str(), "member"]).await;
    cmd(&mut stream, &["SET", destination.as_str(), "notset"]).await;

    let resp = cmd(
        &mut stream,
        &["SMOVE", source.as_str(), destination.as_str(), "member"],
    )
    .await;
    assert_resp_prefix(&resp, b"-WRONGTYPE");

    let resp = cmd(&mut stream, &["SISMEMBER", source.as_str(), "member"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["GET", destination.as_str()]).await;
    assert_resp(&resp, b"$6\r\nnotset\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_smismember() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SADD", "myset", "a", "b", "c"]).await;

    let resp = cmd(&mut stream, &["SMISMEMBER", "myset", "a", "d", "c"]).await;
    assert_resp(&resp, b"*3\r\n:1\r\n:0\r\n:1\r\n");

    let resp = cmd(&mut stream, &["SMISMEMBER", "noexist", "a"]).await;
    assert_resp(&resp, b"*1\r\n:0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_sscan() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SADD", "myset", "a", "b", "c"]).await;

    let resp = cmd(&mut stream, &["SSCAN", "myset", "0"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(resp_str.starts_with("*2\r\n"));
    assert!(resp_str.contains("a") || resp_str.contains("b") || resp_str.contains("c"));

    let resp = cmd(&mut stream, &["SSCAN", "noexist", "0"]).await;
    assert_resp(&resp, b"*2\r\n$1\r\n0\r\n*0\r\n");

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// Phase 3: Sorted Set commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_zrevrangebyscore() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["ZADD", "zs", "1", "a", "2", "b", "3", "c"]).await;

    let resp = cmd(&mut stream, &["ZREVRANGEBYSCORE", "zs", "3", "1"]).await;
    assert_resp(&resp, b"*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n");

    let resp = cmd(
        &mut stream,
        &["ZREVRANGEBYSCORE", "zs", "2", "1", "WITHSCORES"],
    )
    .await;
    assert_resp(&resp, b"*4\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\na\r\n$1\r\n1\r\n");

    let resp = cmd(
        &mut stream,
        &["ZREVRANGEBYSCORE", "zs", "3", "1", "LIMIT", "0", "2"],
    )
    .await;
    assert_resp(&resp, b"*2\r\n$1\r\nc\r\n$1\r\nb\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_zpopmin_zpopmax() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["ZADD", "zs", "1", "a", "2", "b", "3", "c"]).await;

    let resp = cmd(&mut stream, &["ZPOPMIN", "zs"]).await;
    assert_resp(&resp, b"*2\r\n$1\r\na\r\n$1\r\n1\r\n");

    let resp = cmd(&mut stream, &["ZPOPMAX", "zs"]).await;
    assert_resp(&resp, b"*2\r\n$1\r\nc\r\n$1\r\n3\r\n");

    let resp = cmd(&mut stream, &["ZCARD", "zs"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["ZPOPMIN", "noexist"]).await;
    assert_resp(&resp, b"*0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_zrangebylex() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(
        &mut stream,
        &[
            "ZADD", "zs", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e",
        ],
    )
    .await;

    let resp = cmd(&mut stream, &["ZRANGEBYLEX", "zs", "-", "+"]).await;
    assert_resp(
        &resp,
        b"*5\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n",
    );

    let resp = cmd(&mut stream, &["ZRANGEBYLEX", "zs", "[b", "[d"]).await;
    assert_resp(&resp, b"*3\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n");

    let resp = cmd(&mut stream, &["ZRANGEBYLEX", "zs", "(b", "[d"]).await;
    assert_resp(&resp, b"*2\r\n$1\r\nc\r\n$1\r\nd\r\n");

    let resp = cmd(
        &mut stream,
        &["ZRANGEBYLEX", "zs", "-", "+", "LIMIT", "1", "2"],
    )
    .await;
    assert_resp(&resp, b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_zrevrangebylex() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(
        &mut stream,
        &["ZADD", "zs", "0", "a", "0", "b", "0", "c", "0", "d"],
    )
    .await;

    let resp = cmd(&mut stream, &["ZREVRANGEBYLEX", "zs", "+", "-"]).await;
    assert_resp(&resp, b"*4\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n");

    let resp = cmd(&mut stream, &["ZREVRANGEBYLEX", "zs", "[c", "[a"]).await;
    assert_resp(&resp, b"*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_zlexcount() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(
        &mut stream,
        &["ZADD", "zs", "0", "a", "0", "b", "0", "c", "0", "d"],
    )
    .await;

    let resp = cmd(&mut stream, &["ZLEXCOUNT", "zs", "-", "+"]).await;
    assert_resp(&resp, b":4\r\n");

    let resp = cmd(&mut stream, &["ZLEXCOUNT", "zs", "[b", "[d"]).await;
    assert_resp(&resp, b":3\r\n");

    let resp = cmd(&mut stream, &["ZLEXCOUNT", "zs", "(b", "(d"]).await;
    assert_resp(&resp, b":1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_zmscore() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["ZADD", "zs", "1.5", "a", "2.5", "b"]).await;

    let resp = cmd(&mut stream, &["ZMSCORE", "zs", "a", "nonexist", "b"]).await;
    assert_resp(&resp, b"*3\r\n$3\r\n1.5\r\n$-1\r\n$3\r\n2.5\r\n");

    let resp = cmd(&mut stream, &["ZMSCORE", "noexist", "a"]).await;
    assert_resp(&resp, b"*1\r\n$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_zrandmember() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["ZADD", "zs", "1", "a", "2", "b", "3", "c"]).await;

    let resp = cmd(&mut stream, &["ZRANDMEMBER", "zs"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(resp_str.contains("a") || resp_str.contains("b") || resp_str.contains("c"));

    let resp = cmd(&mut stream, &["ZRANDMEMBER", "zs", "2"]).await;
    assert!(resp.starts_with(b"*2\r\n"));

    let resp = cmd(&mut stream, &["ZRANDMEMBER", "zs", "-5"]).await;
    assert!(resp.starts_with(b"*5\r\n"));

    let resp = cmd(&mut stream, &["ZRANDMEMBER", "zs", "2", "WITHSCORES"]).await;
    assert!(resp.starts_with(b"*4\r\n"));

    let resp = cmd(&mut stream, &["ZRANDMEMBER", "noexist"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_zscan() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["ZADD", "zs", "1", "a", "2", "b", "3", "c"]).await;

    let resp = cmd(&mut stream, &["ZSCAN", "zs", "0"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(resp_str.starts_with("*2\r\n"));
    assert!(resp_str.contains("a") || resp_str.contains("b") || resp_str.contains("c"));

    let resp = cmd(&mut stream, &["ZSCAN", "noexist", "0"]).await;
    assert_resp(&resp, b"*2\r\n$1\r\n0\r\n*0\r\n");

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// HyperLogLog tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pfadd_pfcount() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["PFADD", "hll", "a", "b", "c"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["PFADD", "hll", "a", "b", "c"]).await;
    assert_resp(&resp, b":0\r\n");

    let resp = cmd(&mut stream, &["PFADD", "hll", "d"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["PFCOUNT", "hll"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    let count: i64 = resp_str
        .trim()
        .trim_start_matches(':')
        .trim_end()
        .parse()
        .unwrap();
    assert!(
        (3..=5).contains(&count),
        "HLL count should be ~4, got {}",
        count
    );

    let resp = cmd(&mut stream, &["PFCOUNT", "nonexistent"]).await;
    assert_resp(&resp, b":0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_pfmerge() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["PFADD", "hll1", "a", "b", "c"]).await;
    cmd(&mut stream, &["PFADD", "hll2", "c", "d", "e"]).await;

    let resp = cmd(&mut stream, &["PFMERGE", "merged", "hll1", "hll2"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["PFCOUNT", "merged"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    let count: i64 = resp_str
        .trim()
        .trim_start_matches(':')
        .trim_end()
        .parse()
        .unwrap();
    assert!(
        (4..=6).contains(&count),
        "Merged HLL count should be ~5, got {}",
        count
    );

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// Bitmap tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_setbit_getbit() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["SETBIT", "bm", "7", "1"]).await;
    assert_resp(&resp, b":0\r\n");

    let resp = cmd(&mut stream, &["GETBIT", "bm", "7"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["GETBIT", "bm", "0"]).await;
    assert_resp(&resp, b":0\r\n");

    let resp = cmd(&mut stream, &["SETBIT", "bm", "7", "0"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["GETBIT", "bm", "7"]).await;
    assert_resp(&resp, b":0\r\n");

    let resp = cmd(&mut stream, &["GETBIT", "nonexistent", "100"]).await;
    assert_resp(&resp, b":0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_bitcount() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "bc", "foobar"]).await;

    let resp = cmd(&mut stream, &["BITCOUNT", "bc"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    let count: i64 = resp_str
        .trim()
        .trim_start_matches(':')
        .trim_end()
        .parse()
        .unwrap();
    assert_eq!(count, 26);

    let resp = cmd(&mut stream, &["BITCOUNT", "bc", "0", "0"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    let count: i64 = resp_str
        .trim()
        .trim_start_matches(':')
        .trim_end()
        .parse()
        .unwrap();
    assert_eq!(count, 4);

    let resp = cmd(&mut stream, &["BITCOUNT", "bc", "1", "1"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    let count: i64 = resp_str
        .trim()
        .trim_start_matches(':')
        .trim_end()
        .parse()
        .unwrap();
    assert_eq!(count, 6);

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_bitop() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "a", "abc"]).await;
    cmd(&mut stream, &["SET", "b", "def"]).await;

    let resp = cmd(&mut stream, &["BITOP", "AND", "dest", "a", "b"]).await;
    assert_resp(&resp, b":3\r\n");

    let resp = cmd(&mut stream, &["BITOP", "NOT", "notdest", "a"]).await;
    assert_resp(&resp, b":3\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_bitpos() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    for bit in 0..12 {
        cmd(&mut stream, &["SETBIT", "bp", &bit.to_string(), "1"]).await;
    }

    let resp = cmd(&mut stream, &["BITPOS", "bp", "0"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    let pos: i64 = resp_str
        .trim()
        .trim_start_matches(':')
        .trim_end()
        .parse()
        .unwrap();
    assert_eq!(pos, 12);

    let resp = cmd(&mut stream, &["BITPOS", "bp", "1"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    let pos: i64 = resp_str
        .trim()
        .trim_start_matches(':')
        .trim_end()
        .parse()
        .unwrap();
    assert_eq!(pos, 0);

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_bitfield_basic() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["BITFIELD", "mykey", "SET", "u8", "0", "255"]).await;
    assert_resp(&resp, b"*1\r\n:0\r\n");

    let resp = cmd(&mut stream, &["BITFIELD", "mykey", "GET", "u8", "0"]).await;
    assert_resp(&resp, b"*1\r\n:255\r\n");

    let resp = cmd(
        &mut stream,
        &["BITFIELD", "mykey", "INCRBY", "u8", "0", "1"],
    )
    .await;
    assert_resp(&resp, b"*1\r\n:0\r\n");

    let resp = cmd(
        &mut stream,
        &[
            "BITFIELD", "mykey", "OVERFLOW", "SAT", "INCRBY", "u8", "0", "300",
        ],
    )
    .await;
    assert_resp(&resp, b"*1\r\n:255\r\n");

    let resp = cmd(
        &mut stream,
        &[
            "BITFIELD", "mykey", "OVERFLOW", "FAIL", "INCRBY", "u8", "0", "1",
        ],
    )
    .await;
    assert_resp(&resp, b"*1\r\n$-1\r\n");

    let resp = cmd(&mut stream, &["BITFIELD", "mykey", "GET", "u8", "0"]).await;
    assert_resp(&resp, b"*1\r\n:255\r\n");

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// Geo tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_geoadd_geopos() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(
        &mut stream,
        &[
            "GEOADD",
            "geo",
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania",
        ],
    )
    .await;
    assert_resp(&resp, b":2\r\n");

    let resp = cmd(&mut stream, &["GEOPOS", "geo", "Palermo"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.contains("13.36"),
        "Expected longitude ~13.36, got: {}",
        resp_str
    );

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_geodist() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(
        &mut stream,
        &[
            "GEOADD",
            "geo",
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania",
        ],
    )
    .await;

    let resp = cmd(&mut stream, &["GEODIST", "geo", "Palermo", "Catania", "km"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    let dist_str = resp_str.trim_start_matches('$');
    let lines: Vec<&str> = dist_str.split("\r\n").collect();
    let dist_val: f64 = lines.get(1).unwrap_or(&"0").parse().unwrap_or(0.0);
    assert!(
        dist_val > 150.0 && dist_val < 200.0,
        "Distance should be ~166km, got: {}",
        dist_val
    );

    let resp = cmd(&mut stream, &["GEODIST", "geo", "Palermo", "nonexistent"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_geohash() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(
        &mut stream,
        &["GEOADD", "geo", "13.361389", "38.115556", "Palermo"],
    )
    .await;

    let resp = cmd(&mut stream, &["GEOHASH", "geo", "Palermo"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.contains("sqc8b49r"),
        "Expected geohash starting with sqc8b49, got: {}",
        resp_str
    );

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_geosearch() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(
        &mut stream,
        &[
            "GEOADD",
            "geo",
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania",
            "2.349014",
            "48.864716",
            "Paris",
        ],
    )
    .await;

    let resp = cmd(
        &mut stream,
        &[
            "GEOSEARCH",
            "geo",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "200",
            "km",
            "ASC",
        ],
    )
    .await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.contains("Catania") || resp_str.contains("Palermo"),
        "Expected geo members in range, got: {}",
        resp_str
    );
    assert!(!resp_str.contains("Paris"), "Paris should be out of range");

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// Phase 5 — Transactions & Server commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_multi_exec() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["MULTI"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["SET", "txkey1", "val1"]).await;
    assert_resp(&resp, b"+QUEUED\r\n");

    let resp = cmd(&mut stream, &["SET", "txkey2", "val2"]).await;
    assert_resp(&resp, b"+QUEUED\r\n");

    let resp = cmd(&mut stream, &["GET", "txkey1"]).await;
    assert_resp(&resp, b"+QUEUED\r\n");

    let resp = cmd(&mut stream, &["EXEC"]).await;
    assert_resp(&resp, b"*3\r\n+OK\r\n+OK\r\n$4\r\nval1\r\n");

    let resp = cmd(&mut stream, &["GET", "txkey2"]).await;
    assert_resp(&resp, b"$4\r\nval2\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_multi_discard() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["MULTI"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["SET", "discardkey", "val"]).await;
    assert_resp(&resp, b"+QUEUED\r\n");

    let resp = cmd(&mut stream, &["DISCARD"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["GET", "discardkey"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_multi_errors() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["EXEC"]).await;
    assert_resp_prefix(&resp, b"-ERR EXEC without MULTI");

    let resp = cmd(&mut stream, &["DISCARD"]).await;
    assert_resp_prefix(&resp, b"-ERR DISCARD without MULTI");

    let resp = cmd(&mut stream, &["MULTI"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["MULTI"]).await;
    assert_resp_prefix(&resp, b"-ERR MULTI calls can not be nested");

    let resp = cmd(&mut stream, &["DISCARD"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_watch_unwatch() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["WATCH", "wkey1", "wkey2"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["UNWATCH"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["MULTI"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["WATCH", "wkey1"]).await;
    assert_resp_prefix(&resp, b"-ERR WATCH inside MULTI");

    let resp = cmd(&mut stream, &["DISCARD"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_client_commands() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["CLIENT", "ID"]).await;
    assert!(
        resp.starts_with(b":"),
        "Expected integer response for CLIENT ID"
    );

    let resp = cmd(&mut stream, &["CLIENT", "GETNAME"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let resp = cmd(&mut stream, &["CLIENT", "SETNAME", "test-conn"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["CLIENT", "GETNAME"]).await;
    assert_resp(&resp, b"$9\r\ntest-conn\r\n");

    let resp = cmd(&mut stream, &["CLIENT", "INFO"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.contains("name=test-conn"),
        "CLIENT INFO should contain connection name"
    );

    let resp = cmd(&mut stream, &["CLIENT", "LIST"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.contains("id="),
        "CLIENT LIST should contain connection info"
    );

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_auth_gate_and_auth_variants() {
    let port = free_port().await;
    let shutdown = start_server_with_config(ServerConfig {
        bind_address: format!("127.0.0.1:{}", port),
        worker_count: 2,
        password: Some("secret".into()),
        ..Default::default()
    })
    .await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["GET", "auth:key"]).await;
    assert_resp_prefix(&resp, b"-NOAUTH Authentication required");

    let resp = cmd(&mut stream, &["HELLO", "3"]).await;
    assert_resp_prefix(&resp, b"%");

    let resp = cmd(&mut stream, &["AUTH", "wrong"]).await;
    assert_resp_prefix(&resp, b"-ERR invalid password");

    let resp = cmd(&mut stream, &["SET", "auth:key", "value"]).await;
    assert_resp_prefix(&resp, b"-NOAUTH Authentication required");

    let resp = cmd(&mut stream, &["AUTH", "default", "secret"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["SET", "auth:key", "value"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["GET", "auth:key"]).await;
    assert_resp(&resp, b"$5\r\nvalue\r\n");

    let mut unauth = connect(port).await;
    let resp = cmd(&mut unauth, &["GET", "auth:key"]).await;
    assert_resp_prefix(&resp, b"-NOAUTH Authentication required");

    let resp = cmd(&mut unauth, &["QUIT"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_config_set_requirepass_runtime_behavior() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut authenticated = connect(port).await;

    let resp = cmd(&mut authenticated, &["SET", "runtime:key", "value"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(
        &mut authenticated,
        &["CONFIG", "SET", "requirepass", "runtime-pass"],
    )
    .await;
    assert_resp(&resp, b"+OK\r\n");

    // Existing authenticated connection remains authenticated after password change.
    let resp = cmd(&mut authenticated, &["GET", "runtime:key"]).await;
    assert_resp(&resp, b"$5\r\nvalue\r\n");

    let mut blocked = connect(port).await;
    let resp = cmd(&mut blocked, &["GET", "runtime:key"]).await;
    assert_resp_prefix(&resp, b"-NOAUTH Authentication required");

    let resp = cmd(&mut blocked, &["AUTH", "runtime-pass"]).await;
    assert_resp(&resp, b"+OK\r\n");
    let resp = cmd(&mut blocked, &["GET", "runtime:key"]).await;
    assert_resp(&resp, b"$5\r\nvalue\r\n");

    let resp = cmd(&mut authenticated, &["CONFIG", "SET", "requirepass", ""]).await;
    assert_resp(&resp, b"+OK\r\n");

    let mut open = connect(port).await;
    let resp = cmd(&mut open, &["GET", "runtime:key"]).await;
    assert_resp(&resp, b"$5\r\nvalue\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_config_get_set() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["CONFIG", "GET", "maxmemory"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.contains("maxmemory"),
        "CONFIG GET should return maxmemory"
    );

    let resp = cmd(&mut stream, &["CONFIG", "SET", "maxmemory", "100mb"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["CONFIG", "GET", "maxmemory"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.contains("104857600"),
        "CONFIG GET maxmemory should reflect parsed bytes"
    );

    let resp = cmd(&mut stream, &["CONFIG", "SET", "maxmemory", "128KB"]).await;
    assert_resp(&resp, b"+OK\r\n");
    let resp = cmd(&mut stream, &["CONFIG", "GET", "max*"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.contains("131072"),
        "CONFIG GET with wildcard should include updated maxmemory bytes"
    );

    let resp = cmd(&mut stream, &["CONFIG", "SET", "maxmemory", "bad"]).await;
    assert_resp_prefix(&resp, b"-ERR invalid maxmemory value");

    let resp = cmd(&mut stream, &["CONFIG", "SET", "unsupported", "val"]).await;
    assert_resp(&resp, b"-ERR Unsupported CONFIG parameter\r\n");

    let resp = cmd(&mut stream, &["CONFIG", "RESETSTAT"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_time() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["TIME"]).await;
    assert!(resp.starts_with(b"*2\r\n"), "TIME should return array of 2");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_select() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["SELECT", "0"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["SELECT", "1"]).await;
    assert_resp_prefix(&resp, b"-ERR");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_quit() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "quitkey", "val"]).await;

    stream.write_all(&resp_cmd(&["QUIT"])).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    buf.truncate(n);
    assert_resp(&buf, b"+OK\r\n");

    let result = stream.read(&mut buf).await;
    match result {
        Ok(0) => {}
        Err(_) => {}
        Ok(n) => panic!("Expected connection close, got {} bytes", n),
    }

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_wait() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["WAIT", "1", "1"]).await;
    assert_resp(&resp, b":0\r\n");

    let resp = cmd(&mut stream, &["WAIT", "1", "-1"]).await;
    assert_resp_prefix(&resp, b"-ERR timeout is negative");

    let resp = cmd(&mut stream, &["WAIT", "-1", "1"]).await;
    assert_resp(&resp, b":0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_command_subcommands() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["COMMAND", "COUNT"]).await;
    assert!(
        resp.starts_with(b":"),
        "COMMAND COUNT should return integer"
    );

    let resp = cmd(&mut stream, &["COMMAND", "LIST"]).await;
    assert!(resp.starts_with(b"*"), "COMMAND LIST should return array");
    assert!(
        String::from_utf8_lossy(&resp).contains("get"),
        "COMMAND LIST should include known commands"
    );

    let resp = cmd(&mut stream, &["COMMAND", "INFO", "GET", "BOGUS"]).await;
    assert!(resp.starts_with(b"*"), "COMMAND INFO should return array");
    assert!(
        String::from_utf8_lossy(&resp).contains("$-1"),
        "COMMAND INFO should include nil for unknown commands"
    );

    let resp = cmd(&mut stream, &["COMMAND", "DOCS", "GET"]).await;
    assert!(
        resp.starts_with(b"*"),
        "COMMAND DOCS should return a non-empty aggregate"
    );
    assert!(
        String::from_utf8_lossy(&resp).contains("summary"),
        "COMMAND DOCS should include summary fields"
    );

    let resp = cmd(&mut stream, &["COMMAND", "HELP"]).await;
    assert!(resp.starts_with(b"*"), "COMMAND HELP should return array");

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// Phase 6: Blocking Operations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_blpop_immediate() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["RPUSH", "blist", "a", "b"]).await;

    let resp = cmd(&mut stream, &["BLPOP", "blist", "0"]).await;
    assert_resp(&resp, b"*2\r\n$5\r\nblist\r\n$1\r\na\r\n");

    let resp = cmd(&mut stream, &["BLPOP", "empty_list", "0"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_brpop_immediate() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["RPUSH", "blist", "x", "y", "z"]).await;

    let resp = cmd(&mut stream, &["BRPOP", "blist", "0"]).await;
    assert_resp(&resp, b"*2\r\n$5\r\nblist\r\n$1\r\nz\r\n");

    let resp = cmd(&mut stream, &["LLEN", "blist"]).await;
    assert_resp(&resp, b":2\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_blpop_multi_key() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["RPUSH", "k2", "val"]).await;

    let resp = cmd(&mut stream, &["BLPOP", "k1", "k2", "0"]).await;
    assert_resp(&resp, b"*2\r\n$2\r\nk2\r\n$3\r\nval\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_blpop_unblocks_immediately_on_push() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut blocker = connect(port).await;
    let mut producer = connect(port).await;

    blocker
        .write_all(&resp_cmd(&["BLPOP", "wake:list", "1"]))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let start = tokio::time::Instant::now();
    let resp = cmd(&mut producer, &["LPUSH", "wake:list", "value"]).await;
    assert_resp(&resp, b":1\r\n");

    let expected = b"*2\r\n$9\r\nwake:list\r\n$5\r\nvalue\r\n";
    let response = tokio::time::timeout(
        Duration::from_millis(300),
        read_exact_response(&mut blocker, expected.len()),
    )
    .await
    .expect("BLPOP response should arrive quickly");
    assert_resp(&response, expected);

    assert!(
        start.elapsed() < Duration::from_millis(120),
        "BLPOP wakeup took too long ({:?})",
        start.elapsed()
    );

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_blmove_immediate() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["RPUSH", "bsrc", "a", "b", "c"]).await;

    let resp = cmd(
        &mut stream,
        &["BLMOVE", "bsrc", "bdst", "LEFT", "RIGHT", "0"],
    )
    .await;
    assert_resp(&resp, b"$1\r\na\r\n");

    let resp = cmd(&mut stream, &["LRANGE", "bdst", "0", "-1"]).await;
    assert_resp(&resp, b"*1\r\n$1\r\na\r\n");

    let resp = cmd(
        &mut stream,
        &["BLMOVE", "empty_src", "bdst", "LEFT", "RIGHT", "0"],
    )
    .await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_bzpopmin_immediate() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["ZADD", "bzs", "1", "a", "2", "b", "3", "c"]).await;

    let resp = cmd(&mut stream, &["BZPOPMIN", "bzs", "0"]).await;
    assert_resp(&resp, b"*2\r\n$3\r\nbzs\r\n*2\r\n$1\r\na\r\n$1\r\n1\r\n");

    let resp = cmd(&mut stream, &["BZPOPMIN", "nope", "0"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_bzpopmax_immediate() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["ZADD", "bzs2", "1", "a", "2", "b", "3", "c"]).await;

    let resp = cmd(&mut stream, &["BZPOPMAX", "bzs2", "0"]).await;
    assert_resp(&resp, b"*2\r\n$4\r\nbzs2\r\n*2\r\n$1\r\nc\r\n$1\r\n3\r\n");

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// Phase 6: Stream Consumer Groups
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_xread_cross_shard_multi_stream() {
    let port = free_port().await;
    let shutdown = start_server_with_workers(port, 2).await;
    let mut stream = connect(port).await;
    let (stream_a, stream_b) = keys_on_different_shards(2, "xread-x:");

    cmd(
        &mut stream,
        &["XADD", stream_a.as_str(), "1-0", "field", "value-a"],
    )
    .await;
    cmd(
        &mut stream,
        &["XADD", stream_b.as_str(), "1-0", "field", "value-b"],
    )
    .await;

    let resp = cmd(
        &mut stream,
        &[
            "XREAD",
            "COUNT",
            "10",
            "STREAMS",
            stream_a.as_str(),
            stream_b.as_str(),
            "0-0",
            "0-0",
        ],
    )
    .await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.contains(stream_a.as_str()),
        "XREAD response missing {}: {}",
        stream_a,
        resp_str
    );
    assert!(
        resp_str.contains(stream_b.as_str()),
        "XREAD response missing {}: {}",
        stream_b,
        resp_str
    );
    assert!(
        resp_str.contains("value-a") && resp_str.contains("value-b"),
        "XREAD response missing values: {}",
        resp_str
    );

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xgroup_create_destroy() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["XADD", "mystream", "1-0", "f", "v"]).await;

    let resp = cmd(&mut stream, &["XGROUP", "CREATE", "mystream", "grp1", "0"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["XGROUP", "CREATE", "mystream", "grp1", "0"]).await;
    assert_resp_prefix(&resp, b"-BUSYGROUP");

    let resp = cmd(&mut stream, &["XGROUP", "DESTROY", "mystream", "grp1"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["XGROUP", "DESTROY", "mystream", "grp1"]).await;
    assert_resp(&resp, b":0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xgroup_create_mkstream() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["XGROUP", "CREATE", "noexist", "grp1", "0"]).await;
    assert_resp_prefix(&resp, b"-ERR");

    let resp = cmd(
        &mut stream,
        &["XGROUP", "CREATE", "noexist", "grp1", "0", "MKSTREAM"],
    )
    .await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["XLEN", "noexist"]).await;
    assert_resp(&resp, b":0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xreadgroup_new_messages() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["XADD", "s1", "1-0", "k", "v1"]).await;
    cmd(&mut stream, &["XADD", "s1", "2-0", "k", "v2"]).await;
    cmd(&mut stream, &["XADD", "s1", "3-0", "k", "v3"]).await;

    cmd(&mut stream, &["XGROUP", "CREATE", "s1", "mygroup", "0"]).await;

    let resp = cmd(
        &mut stream,
        &[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "alice",
            "COUNT",
            "2",
            "STREAMS",
            "s1",
            ">",
        ],
    )
    .await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.contains("s1"),
        "response should contain stream key"
    );
    assert!(resp_str.contains("1-0"), "response should contain first ID");
    assert!(
        resp_str.contains("2-0"),
        "response should contain second ID"
    );
    assert!(
        !resp_str.contains("3-0"),
        "response should NOT contain third ID (COUNT=2)"
    );

    let resp = cmd(
        &mut stream,
        &[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "alice",
            "STREAMS",
            "s1",
            ">",
        ],
    )
    .await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.contains("3-0"),
        "second read should deliver remaining entry"
    );

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xack() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["XADD", "s", "1-0", "f", "v1"]).await;
    cmd(&mut stream, &["XADD", "s", "2-0", "f", "v2"]).await;
    cmd(&mut stream, &["XGROUP", "CREATE", "s", "g", "0"]).await;
    cmd(
        &mut stream,
        &["XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">"],
    )
    .await;

    let resp = cmd(&mut stream, &["XACK", "s", "g", "1-0"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["XACK", "s", "g", "1-0"]).await;
    assert_resp(&resp, b":0\r\n");

    let resp = cmd(&mut stream, &["XACK", "s", "g", "2-0"]).await;
    assert_resp(&resp, b":1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xpending_summary() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["XADD", "s", "1-0", "f", "v1"]).await;
    cmd(&mut stream, &["XADD", "s", "2-0", "f", "v2"]).await;
    cmd(&mut stream, &["XGROUP", "CREATE", "s", "g", "0"]).await;
    cmd(
        &mut stream,
        &["XREADGROUP", "GROUP", "g", "bob", "STREAMS", "s", ">"],
    )
    .await;

    let resp = cmd(&mut stream, &["XPENDING", "s", "g"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.starts_with("*4\r\n"),
        "summary should be 4-element array"
    );
    assert!(resp_str.contains(":2\r\n"), "should have 2 pending entries");
    assert!(resp_str.contains("1-0"), "should contain min ID");
    assert!(resp_str.contains("2-0"), "should contain max ID");
    assert!(resp_str.contains("bob"), "should contain consumer name");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xpending_detail() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["XADD", "s", "1-0", "f", "v"]).await;
    cmd(&mut stream, &["XGROUP", "CREATE", "s", "g", "0"]).await;
    cmd(
        &mut stream,
        &["XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">"],
    )
    .await;

    let resp = cmd(&mut stream, &["XPENDING", "s", "g", "-", "+", "10"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(resp_str.contains("1-0"), "should contain entry ID");
    assert!(resp_str.contains("c1"), "should contain consumer name");
    assert!(resp_str.contains(":1\r\n"), "delivery count should be 1");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xpending_no_group() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["XADD", "s", "1-0", "f", "v"]).await;

    let resp = cmd(&mut stream, &["XPENDING", "s", "nogroup"]).await;
    assert_resp_prefix(&resp, b"-NOGROUP");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xgroup_delconsumer() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["XADD", "s", "1-0", "f", "v"]).await;
    cmd(&mut stream, &["XGROUP", "CREATE", "s", "g", "0"]).await;
    cmd(
        &mut stream,
        &["XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">"],
    )
    .await;

    let resp = cmd(&mut stream, &["XGROUP", "DELCONSUMER", "s", "g", "c1"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["XGROUP", "DELCONSUMER", "s", "g", "c1"]).await;
    assert_resp(&resp, b":0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xdel() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["XADD", "s", "1-0", "f", "v1"]).await;
    cmd(&mut stream, &["XADD", "s", "2-0", "f", "v2"]).await;
    cmd(&mut stream, &["XADD", "s", "3-0", "f", "v3"]).await;

    let resp = cmd(&mut stream, &["XDEL", "s", "2-0"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["XLEN", "s"]).await;
    assert_resp(&resp, b":2\r\n");

    let resp = cmd(&mut stream, &["XDEL", "s", "2-0"]).await;
    assert_resp(&resp, b":0\r\n");

    let resp = cmd(&mut stream, &["XDEL", "noexist", "1-0"]).await;
    assert_resp(&resp, b":0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xdel_multiple() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["XADD", "s", "1-0", "f", "v1"]).await;
    cmd(&mut stream, &["XADD", "s", "2-0", "f", "v2"]).await;
    cmd(&mut stream, &["XADD", "s", "3-0", "f", "v3"]).await;

    let resp = cmd(&mut stream, &["XDEL", "s", "1-0", "3-0"]).await;
    assert_resp(&resp, b":2\r\n");

    let resp = cmd(&mut stream, &["XLEN", "s"]).await;
    assert_resp(&resp, b":1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xinfo_stream() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["XADD", "s", "1-0", "f1", "v1"]).await;
    cmd(&mut stream, &["XADD", "s", "2-0", "f2", "v2"]).await;

    let resp = cmd(&mut stream, &["XINFO", "STREAM", "s"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(resp_str.contains("length"), "should contain length field");
    assert!(resp_str.contains(":2\r\n"), "length should be 2");
    assert!(
        resp_str.contains("first-entry"),
        "should contain first-entry"
    );
    assert!(resp_str.contains("last-entry"), "should contain last-entry");
    assert!(resp_str.contains("1-0"), "should contain first entry ID");
    assert!(resp_str.contains("2-0"), "should contain last entry ID");
    assert!(
        resp_str.contains("last-generated-id"),
        "should contain last-generated-id"
    );

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xinfo_stream_nonexistent() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["XINFO", "STREAM", "nokey"]).await;
    assert_resp_prefix(&resp, b"-ERR");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xinfo_groups() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["XADD", "s", "1-0", "f", "v"]).await;
    cmd(&mut stream, &["XGROUP", "CREATE", "s", "g1", "0"]).await;
    cmd(&mut stream, &["XGROUP", "CREATE", "s", "g2", "$"]).await;

    let resp = cmd(&mut stream, &["XINFO", "GROUPS", "s"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(resp_str.contains("g1"), "should contain group g1");
    assert!(resp_str.contains("g2"), "should contain group g2");
    assert!(resp_str.contains("name"), "should contain name field");
    assert!(resp_str.contains("pending"), "should contain pending field");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xinfo_groups_no_stream() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["XINFO", "GROUPS", "nokey"]).await;
    assert_resp_prefix(&resp, b"-ERR");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xclaim() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["XADD", "s", "1-0", "f", "v"]).await;
    cmd(&mut stream, &["XGROUP", "CREATE", "s", "g", "0"]).await;
    cmd(
        &mut stream,
        &["XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">"],
    )
    .await;

    tokio::time::sleep(Duration::from_millis(10)).await;

    let resp = cmd(&mut stream, &["XCLAIM", "s", "g", "c2", "0", "1-0"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(resp_str.contains("1-0"), "claimed entry should be returned");
    assert!(resp_str.contains("v"), "claimed entry should contain value");

    let resp = cmd(&mut stream, &["XPENDING", "s", "g"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.contains("c2"),
        "pending should now show c2 as owner"
    );

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xautoclaim() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["XADD", "s", "1-0", "f", "v1"]).await;
    cmd(&mut stream, &["XADD", "s", "2-0", "f", "v2"]).await;
    cmd(&mut stream, &["XGROUP", "CREATE", "s", "g", "0"]).await;
    cmd(
        &mut stream,
        &["XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">"],
    )
    .await;

    tokio::time::sleep(Duration::from_millis(10)).await;

    let resp = cmd(&mut stream, &["XAUTOCLAIM", "s", "g", "c2", "0", "0-0"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(resp_str.contains("1-0"), "should auto-claim first entry");
    assert!(resp_str.contains("2-0"), "should auto-claim second entry");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xreadgroup_pending_reread() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["XADD", "s", "1-0", "k", "v1"]).await;
    cmd(&mut stream, &["XADD", "s", "2-0", "k", "v2"]).await;
    cmd(&mut stream, &["XGROUP", "CREATE", "s", "g", "0"]).await;

    cmd(
        &mut stream,
        &["XREADGROUP", "GROUP", "g", "alice", "STREAMS", "s", ">"],
    )
    .await;

    let resp = cmd(
        &mut stream,
        &["XREADGROUP", "GROUP", "g", "alice", "STREAMS", "s", "0"],
    )
    .await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.contains("1-0"),
        "pending re-read should return first entry"
    );
    assert!(
        resp_str.contains("2-0"),
        "pending re-read should return second entry"
    );

    cmd(&mut stream, &["XACK", "s", "g", "1-0", "2-0"]).await;

    let resp = cmd(
        &mut stream,
        &["XREADGROUP", "GROUP", "g", "alice", "STREAMS", "s", "0"],
    )
    .await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_stream_consumer_group_full_workflow() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["XADD", "jobs", "1-0", "task", "build"]).await;
    cmd(&mut stream, &["XADD", "jobs", "2-0", "task", "test"]).await;
    cmd(&mut stream, &["XADD", "jobs", "3-0", "task", "deploy"]).await;

    cmd(&mut stream, &["XGROUP", "CREATE", "jobs", "workers", "0"]).await;

    let resp = cmd(
        &mut stream,
        &[
            "XREADGROUP",
            "GROUP",
            "workers",
            "w1",
            "COUNT",
            "1",
            "STREAMS",
            "jobs",
            ">",
        ],
    )
    .await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(resp_str.contains("1-0"), "w1 should get entry 1-0");

    let resp = cmd(
        &mut stream,
        &[
            "XREADGROUP",
            "GROUP",
            "workers",
            "w2",
            "COUNT",
            "1",
            "STREAMS",
            "jobs",
            ">",
        ],
    )
    .await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(resp_str.contains("2-0"), "w2 should get entry 2-0");

    let resp = cmd(&mut stream, &["XACK", "jobs", "workers", "1-0"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["XPENDING", "jobs", "workers"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.contains(":1\r\n"),
        "should have 1 pending after ACK"
    );

    let resp = cmd(&mut stream, &["XINFO", "STREAM", "jobs"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(resp_str.contains(":3\r\n"), "stream length should be 3");
    assert!(resp_str.contains("1-0"), "first entry should be 1-0");
    assert!(resp_str.contains("3-0"), "last entry should be 3-0");

    let resp = cmd(&mut stream, &["XINFO", "GROUPS", "jobs"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(resp_str.contains("workers"), "should list workers group");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_doc_createindex_and_find() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["DOC.CREATE", "people"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(
        &mut stream,
        &[
            "DOC.SET",
            "people",
            "p:1",
            r#"{"name":"Kofi","city":"Accra","age":30}"#,
        ],
    )
    .await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(
        &mut stream,
        &[
            "DOC.SET",
            "people",
            "p:2",
            r#"{"name":"Ama","city":"Kumasi","age":25}"#,
        ],
    )
    .await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(
        &mut stream,
        &[
            "DOC.SET",
            "people",
            "p:3",
            r#"{"name":"Yaw","city":"Accra","age":28}"#,
        ],
    )
    .await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["DOC.CREATEINDEX", "people", "city", "hash"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(
        &mut stream,
        &["DOC.FIND", "people", "WHERE", "city", "=", r#""Accra""#],
    )
    .await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(resp_str.contains("Kofi"), "should find Kofi in Accra");
    assert!(resp_str.contains("Yaw"), "should find Yaw in Accra");
    assert!(!resp_str.contains("Ama"), "should not find Ama (Kumasi)");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_doc_count_query() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["DOC.CREATE", "people"]).await;
    assert_resp(&resp, b"+OK\r\n");

    for (id, city) in &[("p:1", "Accra"), ("p:2", "Kumasi"), ("p:3", "Accra")] {
        let json = format!(r#"{{"city":"{}"}}"#, city);
        let resp = cmd(&mut stream, &["DOC.SET", "people", id, &json]).await;
        assert_resp(&resp, b"+OK\r\n");
    }

    let resp = cmd(&mut stream, &["DOC.CREATEINDEX", "people", "city", "hash"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(
        &mut stream,
        &["DOC.COUNT", "people", "WHERE", "city", "=", r#""Accra""#],
    )
    .await;
    assert_resp(&resp, b":2\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_doc_indexes_lists_created() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["DOC.CREATE", "people"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(
        &mut stream,
        &["DOC.SET", "people", "p:1", r#"{"city":"Accra","age":30}"#],
    )
    .await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["DOC.CREATEINDEX", "people", "city", "hash"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["DOC.CREATEINDEX", "people", "age", "sorted"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["DOC.INDEXES", "people"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(resp_str.contains("age"), "should list age index");
    assert!(resp_str.contains("sorted"), "should list sorted type");
    assert!(resp_str.contains("city"), "should list city index");
    assert!(resp_str.contains("hash"), "should list hash type");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_doc_dropindex_removes() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["DOC.CREATE", "people"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(
        &mut stream,
        &["DOC.SET", "people", "p:1", r#"{"city":"Accra"}"#],
    )
    .await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["DOC.CREATEINDEX", "people", "city", "hash"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["DOC.INDEXES", "people"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(resp_str.contains("city"), "index should exist before drop");

    let resp = cmd(&mut stream, &["DOC.DROPINDEX", "people", "city"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["DOC.INDEXES", "people"]).await;
    assert_resp(&resp, b"*0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_doc_find_with_limit_offset() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["DOC.CREATE", "people"]).await;
    assert_resp(&resp, b"+OK\r\n");

    for i in 1..=5 {
        let id = format!("p:{}", i);
        let json = format!(r#"{{"city":"Accra","seq":{}}}"#, i);
        let resp = cmd(&mut stream, &["DOC.SET", "people", &id, &json]).await;
        assert_resp(&resp, b"+OK\r\n");
    }

    let resp = cmd(&mut stream, &["DOC.CREATEINDEX", "people", "city", "hash"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(
        &mut stream,
        &[
            "DOC.FIND",
            "people",
            "WHERE",
            "city",
            "=",
            r#""Accra""#,
            "LIMIT",
            "2",
        ],
    )
    .await;
    let frame = parse_resp_frame(&resp);
    if let RespValue::Array(Some(items)) = frame {
        assert_eq!(items.len(), 2, "LIMIT 2 should return exactly 2 results");
    } else {
        panic!("expected array response from DOC.FIND");
    }

    let resp = cmd(
        &mut stream,
        &[
            "DOC.FIND",
            "people",
            "WHERE",
            "city",
            "=",
            r#""Accra""#,
            "LIMIT",
            "2",
            "OFFSET",
            "3",
        ],
    )
    .await;
    let frame = parse_resp_frame(&resp);
    if let RespValue::Array(Some(items)) = frame {
        assert_eq!(
            items.len(),
            2,
            "LIMIT 2 OFFSET 3 should return 2 results (items 4 and 5)"
        );
    } else {
        panic!("expected array response from DOC.FIND with OFFSET");
    }

    let _ = shutdown.send(true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_doc_read_concurrency_no_deadlock() {
    let port = free_port().await;
    let shutdown = start_server(port).await;

    let mut setup = connect(port).await;
    let resp = cmd(&mut setup, &["DOC.CREATE", "bench"]).await;
    assert_resp(&resp, b"+OK\r\n");
    let resp = cmd(
        &mut setup,
        &[
            "DOC.SET",
            "bench",
            "doc:1",
            r#"{"name":"Ada","city":"Accra"}"#,
        ],
    )
    .await;
    assert_resp(&resp, b"+OK\r\n");

    let mut workers = Vec::new();
    for _ in 0..16 {
        workers.push(tokio::spawn(async move {
            let mut stream = connect(port).await;
            for _ in 0..80 {
                let get_resp = cmd(&mut stream, &["DOC.GET", "bench", "doc:1"]).await;
                assert!(
                    get_resp.starts_with(b"$"),
                    "DOC.GET should return bulk JSON"
                );

                let exists_resp = cmd(&mut stream, &["DOC.EXISTS", "bench", "doc:1"]).await;
                assert_resp(&exists_resp, b":1\r\n");
            }
        }));
    }

    tokio::time::timeout(Duration::from_secs(10), async {
        for worker in workers {
            worker.await.expect("concurrency worker task panicked");
        }
    })
    .await
    .expect("parallel DOC read workload timed out");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_server_kv_data_survives_restart() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let data_dir = tmp_dir.path().to_path_buf();

    let port = free_port().await;
    let shutdown = start_server_with_storage(port, data_dir.clone()).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "restart:k1", "hello"]).await;
    cmd(&mut stream, &["SET", "restart:k2", "world"]).await;
    cmd(&mut stream, &["LPUSH", "restart:list", "a", "b"]).await;
    cmd(&mut stream, &["HSET", "restart:hash", "f1", "v1"]).await;
    cmd(&mut stream, &["SADD", "restart:set", "x", "y"]).await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = shutdown.send(true);
    drop(stream);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let port2 = free_port().await;
    let shutdown2 = start_server_with_storage(port2, data_dir).await;
    let mut stream2 = connect(port2).await;

    let resp = cmd(&mut stream2, &["GET", "restart:k1"]).await;
    assert_resp(&resp, b"$5\r\nhello\r\n");

    let resp = cmd(&mut stream2, &["GET", "restart:k2"]).await;
    assert_resp(&resp, b"$5\r\nworld\r\n");

    let resp = cmd(&mut stream2, &["LLEN", "restart:list"]).await;
    assert_resp(&resp, b":2\r\n");

    let resp = cmd(&mut stream2, &["HGET", "restart:hash", "f1"]).await;
    assert_resp(&resp, b"$2\r\nv1\r\n");

    let resp = cmd(&mut stream2, &["SCARD", "restart:set"]).await;
    assert_resp(&resp, b":2\r\n");

    let _ = shutdown2.send(true);
}

#[tokio::test]
async fn test_server_doc_data_survives_restart() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let data_dir = tmp_dir.path().to_path_buf();

    let port = free_port().await;
    let shutdown = start_server_with_storage(port, data_dir.clone()).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["DOC.CREATE", "users"]).await;
    cmd(
        &mut stream,
        &[
            "DOC.SET",
            "users",
            "user:1",
            r#"{"name":"Augustus","city":"Accra"}"#,
        ],
    )
    .await;
    cmd(
        &mut stream,
        &[
            "DOC.SET",
            "users",
            "user:2",
            r#"{"name":"Kwame","city":"Kumasi"}"#,
        ],
    )
    .await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = shutdown.send(true);
    drop(stream);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let port2 = free_port().await;
    let shutdown2 = start_server_with_storage(port2, data_dir).await;
    let mut stream2 = connect(port2).await;

    let resp = cmd(&mut stream2, &["DOC.GET", "users", "user:1"]).await;
    let parsed: Value = serde_json::from_slice(&extract_bulk_string(&resp)).unwrap();
    assert_eq!(parsed["name"], "Augustus");
    assert_eq!(parsed["city"], "Accra");

    let resp = cmd(&mut stream2, &["DOC.GET", "users", "user:2"]).await;
    let parsed: Value = serde_json::from_slice(&extract_bulk_string(&resp)).unwrap();
    assert_eq!(parsed["name"], "Kwame");
    assert_eq!(parsed["city"], "Kumasi");

    let _ = shutdown2.send(true);
}

fn extract_bulk_string(resp: &[u8]) -> Vec<u8> {
    let s = String::from_utf8_lossy(resp);
    let lines: Vec<&str> = s.split("\r\n").collect();
    if lines.len() >= 2 && lines[0].starts_with('$') {
        lines[1].as_bytes().to_vec()
    } else {
        panic!("Expected bulk string response, got: {:?}", s);
    }
}
