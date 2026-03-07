use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use kora_server::{KoraServer, ServerConfig};

async fn free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn bulk(s: &str) -> Vec<u8> {
    format!("${}\r\n{}\r\n", s.len(), s).into_bytes()
}

fn resp_cmd(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        out.extend_from_slice(&bulk(arg));
    }
    out
}

async fn start_server(port: u16) -> tokio::sync::watch::Sender<bool> {
    let (tx, rx) = tokio::sync::watch::channel(false);
    let config = ServerConfig {
        bind_address: format!("127.0.0.1:{}", port),
        worker_count: 2,
        ..Default::default()
    };
    let server = KoraServer::new(config);
    tokio::spawn(async move {
        let _ = server.run(rx).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    tx
}

async fn connect(port: u16) -> TcpStream {
    for _ in 0..20 {
        match TcpStream::connect(format!("127.0.0.1:{}", port)).await {
            Ok(s) => return s,
            Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
        }
    }
    panic!("could not connect to server on port {}", port);
}

async fn send_and_read(stream: &mut TcpStream, cmd_bytes: &[u8]) -> Vec<u8> {
    stream.write_all(cmd_bytes).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let mut buf = vec![0u8; 256 * 1024];
    let n = stream.read(&mut buf).await.unwrap();
    buf.truncate(n);
    buf
}

async fn cmd(stream: &mut TcpStream, args: &[&str]) -> Vec<u8> {
    send_and_read(stream, &resp_cmd(args)).await
}

fn assert_resp(resp: &[u8], expected: &[u8]) {
    assert_eq!(
        resp,
        expected,
        "\nExpected: {:?}\n     Got: {:?}",
        String::from_utf8_lossy(expected),
        String::from_utf8_lossy(resp),
    );
}

fn assert_resp_prefix(resp: &[u8], prefix: &[u8]) {
    assert!(
        resp.starts_with(prefix),
        "\nExpected prefix: {:?}\n           Got: {:?}",
        String::from_utf8_lossy(prefix),
        String::from_utf8_lossy(resp),
    );
}

fn resp_bulk(data: &[u8]) -> Vec<u8> {
    let mut out = format!("${}\r\n", data.len()).into_bytes();
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
    out
}

// ---------------------------------------------------------------------------
// String Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_get_set_basic() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["SET", "key1", "value1"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["GET", "key1"]).await;
    assert_resp(&resp, b"$6\r\nvalue1\r\n");

    let resp = cmd(&mut stream, &["GET", "nonexistent"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_set_ex() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["SET", "exkey", "exval", "EX", "1"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["GET", "exkey"]).await;
    assert_resp(&resp, b"$5\r\nexval\r\n");

    tokio::time::sleep(Duration::from_millis(1200)).await;

    let resp = cmd(&mut stream, &["GET", "exkey"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_set_px() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["SET", "pxkey", "pxval", "PX", "200"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["GET", "pxkey"]).await;
    assert_resp(&resp, b"$5\r\npxval\r\n");

    tokio::time::sleep(Duration::from_millis(300)).await;

    let resp = cmd(&mut stream, &["GET", "pxkey"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_set_nx() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["SET", "nxkey", "val1", "NX"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["SET", "nxkey", "val2", "NX"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let resp = cmd(&mut stream, &["GET", "nxkey"]).await;
    assert_resp(&resp, b"$4\r\nval1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_set_xx() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["SET", "xxkey", "val1", "XX"]).await;
    assert_resp(&resp, b"$-1\r\n");

    cmd(&mut stream, &["SET", "xxkey", "val1"]).await;

    let resp = cmd(&mut stream, &["SET", "xxkey", "val2", "XX"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["GET", "xxkey"]).await;
    assert_resp(&resp, b"$4\r\nval2\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_getset() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "gskey", "old"]).await;

    let resp = cmd(&mut stream, &["GETSET", "gskey", "new"]).await;
    assert_resp(&resp, b"$3\r\nold\r\n");

    let resp = cmd(&mut stream, &["GET", "gskey"]).await;
    assert_resp(&resp, b"$3\r\nnew\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_append() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["APPEND", "appkey", "hello"]).await;
    assert_resp(&resp, b":5\r\n");

    let resp = cmd(&mut stream, &["APPEND", "appkey", " world"]).await;
    assert_resp(&resp, b":11\r\n");

    let resp = cmd(&mut stream, &["GET", "appkey"]).await;
    assert_resp(&resp, b"$11\r\nhello world\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_strlen() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "strkey", "hello"]).await;

    let resp = cmd(&mut stream, &["STRLEN", "strkey"]).await;
    assert_resp(&resp, b":5\r\n");

    let resp = cmd(&mut stream, &["STRLEN", "nonexistent"]).await;
    assert_resp(&resp, b":0\r\n");

    cmd(&mut stream, &["SET", "numkey", "12345"]).await;
    let resp = cmd(&mut stream, &["STRLEN", "numkey"]).await;
    assert_resp(&resp, b":5\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_incr_decr() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["INCR", "counter"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["INCR", "counter"]).await;
    assert_resp(&resp, b":2\r\n");

    let resp = cmd(&mut stream, &["DECR", "counter"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["INCRBY", "counter", "10"]).await;
    assert_resp(&resp, b":11\r\n");

    let resp = cmd(&mut stream, &["DECRBY", "counter", "5"]).await;
    assert_resp(&resp, b":6\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_incr_overflow() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "notnum", "abc"]).await;

    let resp = cmd(&mut stream, &["INCR", "notnum"]).await;
    assert_resp_prefix(&resp, b"-");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_mget_mset() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["MSET", "k1", "v1", "k2", "v2", "k3", "v3"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["MGET", "k1", "k2", "k3", "k_nonexistent"]).await;
    let expected = b"*4\r\n$2\r\nv1\r\n$2\r\nv2\r\n$2\r\nv3\r\n$-1\r\n";
    assert_resp(&resp, expected);

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_setnx_command() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["SETNX", "snxkey", "val"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["SETNX", "snxkey", "val2"]).await;
    assert_resp(&resp, b":0\r\n");

    let resp = cmd(&mut stream, &["GET", "snxkey"]).await;
    assert_resp(&resp, b"$3\r\nval\r\n");

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// Key Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_del_multiple() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    for i in 1..=5 {
        cmd(
            &mut stream,
            &["SET", &format!("dk{}", i), &format!("dv{}", i)],
        )
        .await;
    }

    let resp = cmd(&mut stream, &["DEL", "dk1", "dk2", "dk3"]).await;
    assert_resp(&resp, b":3\r\n");

    let resp = cmd(&mut stream, &["GET", "dk4"]).await;
    assert_resp(&resp, b"$3\r\ndv4\r\n");

    let resp = cmd(&mut stream, &["GET", "dk5"]).await;
    assert_resp(&resp, b"$3\r\ndv5\r\n");

    let resp = cmd(&mut stream, &["GET", "dk1"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_exists_multiple() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "ea", "1"]).await;
    cmd(&mut stream, &["SET", "eb", "2"]).await;

    let resp = cmd(&mut stream, &["EXISTS", "ea", "eb", "ec"]).await;
    assert_resp(&resp, b":2\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_expire_ttl() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "ttlkey", "val"]).await;

    let resp = cmd(&mut stream, &["EXPIRE", "ttlkey", "10"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["TTL", "ttlkey"]).await;
    let ttl_str = String::from_utf8_lossy(&resp);
    let ttl_val: i64 = ttl_str
        .trim()
        .trim_start_matches(':')
        .trim_end()
        .parse()
        .unwrap();
    assert!(
        ttl_val > 0 && ttl_val <= 10,
        "TTL should be between 1 and 10, got {}",
        ttl_val
    );

    let resp = cmd(&mut stream, &["PERSIST", "ttlkey"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["TTL", "ttlkey"]).await;
    assert_resp(&resp, b":-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_pexpire_pttl() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "pttlkey", "val"]).await;

    let resp = cmd(&mut stream, &["PEXPIRE", "pttlkey", "5000"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["PTTL", "pttlkey"]).await;
    let pttl_str = String::from_utf8_lossy(&resp);
    let pttl_val: i64 = pttl_str
        .trim()
        .trim_start_matches(':')
        .trim_end()
        .parse()
        .unwrap();
    assert!(
        pttl_val > 0 && pttl_val <= 5000,
        "PTTL should be between 1 and 5000, got {}",
        pttl_val
    );

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_type_command() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "tstr", "val"]).await;
    let resp = cmd(&mut stream, &["TYPE", "tstr"]).await;
    assert_resp(&resp, b"+string\r\n");

    cmd(&mut stream, &["LPUSH", "tlist", "a"]).await;
    let resp = cmd(&mut stream, &["TYPE", "tlist"]).await;
    assert_resp(&resp, b"+list\r\n");

    cmd(&mut stream, &["HSET", "thash", "f", "v"]).await;
    let resp = cmd(&mut stream, &["TYPE", "thash"]).await;
    assert_resp(&resp, b"+hash\r\n");

    cmd(&mut stream, &["SADD", "tset", "m"]).await;
    let resp = cmd(&mut stream, &["TYPE", "tset"]).await;
    assert_resp(&resp, b"+set\r\n");

    cmd(&mut stream, &["ZADD", "tzset", "1.0", "m"]).await;
    let resp = cmd(&mut stream, &["TYPE", "tzset"]).await;
    assert_resp(&resp, b"+zset\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_keys_pattern() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "user:1", "alice"]).await;
    cmd(&mut stream, &["SET", "user:2", "bob"]).await;
    cmd(&mut stream, &["SET", "order:1", "item"]).await;

    let resp = cmd(&mut stream, &["KEYS", "user:*"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.starts_with("*2\r\n"),
        "Expected 2 results, got: {}",
        resp_str
    );
    assert!(
        resp_str.contains("user:1"),
        "Missing user:1 in: {}",
        resp_str
    );
    assert!(
        resp_str.contains("user:2"),
        "Missing user:2 in: {}",
        resp_str
    );

    let resp = cmd(&mut stream, &["KEYS", "*"]).await;
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.starts_with("*3\r\n"),
        "Expected 3 results, got: {}",
        resp_str
    );

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_scan_basic() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    for i in 0..20 {
        cmd(
            &mut stream,
            &["SET", &format!("scankey:{}", i), &format!("v{}", i)],
        )
        .await;
    }

    let resp = cmd(&mut stream, &["SCAN", "0", "COUNT", "100"]).await;
    let resp_str = String::from_utf8_lossy(&resp).to_string();

    let mut key_count = 0;
    for i in 0..20 {
        if resp_str.contains(&format!("scankey:{}", i)) {
            key_count += 1;
        }
    }
    assert_eq!(
        key_count, 20,
        "Expected all 20 keys in SCAN response, found {}",
        key_count
    );

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_persist_removes_ttl() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "perskey", "val", "EX", "100"]).await;

    let resp = cmd(&mut stream, &["TTL", "perskey"]).await;
    let ttl_str = String::from_utf8_lossy(&resp);
    let ttl_val: i64 = ttl_str
        .trim()
        .trim_start_matches(':')
        .trim_end()
        .parse()
        .unwrap();
    assert!(ttl_val > 0, "TTL should be positive, got {}", ttl_val);

    let resp = cmd(&mut stream, &["PERSIST", "perskey"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["TTL", "perskey"]).await;
    assert_resp(&resp, b":-1\r\n");

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// Edge Cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_empty_string_value() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["SET", "emptykey", ""]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["GET", "emptykey"]).await;
    assert_resp(&resp, b"$0\r\n\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_binary_safe_values() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let binary_value = b"hello\r\nworld\x00binary";
    let mut set_cmd = resp_cmd(&["SET", "binkey", "placeholder"]);
    set_cmd.clear();
    let key = b"binkey";
    set_cmd.extend_from_slice(b"*3\r\n");
    set_cmd.extend_from_slice(&bulk("SET"));
    set_cmd.extend_from_slice(&resp_bulk(key));
    set_cmd.extend_from_slice(&resp_bulk(binary_value));

    let resp = send_and_read(&mut stream, &set_cmd).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["GET", "binkey"]).await;
    let mut expected = format!("${}\r\n", binary_value.len()).into_bytes();
    expected.extend_from_slice(binary_value);
    expected.extend_from_slice(b"\r\n");
    assert_resp(&resp, &expected);

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_large_value() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let large_val = "x".repeat(100 * 1024);
    let mut set_cmd = b"*3\r\n".to_vec();
    set_cmd.extend_from_slice(&bulk("SET"));
    set_cmd.extend_from_slice(&bulk("largekey"));
    set_cmd.extend_from_slice(&bulk(&large_val));

    let resp = send_and_read(&mut stream, &set_cmd).await;
    assert_resp(&resp, b"+OK\r\n");

    let get_cmd = resp_cmd(&["GET", "largekey"]);
    stream.write_all(&get_cmd).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut result = Vec::new();
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        match tokio::time::timeout(Duration::from_millis(200), stream.read(&mut buf)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => {
                result.extend_from_slice(&buf[..n]);
                if result.len() >= large_val.len() + 20 {
                    break;
                }
            }
            Ok(Err(_)) => break,
            Err(_) => break,
        }
    }

    let header = format!("${}\r\n", large_val.len());
    assert!(
        result.starts_with(header.as_bytes()),
        "Expected bulk string header for 100KB value"
    );
    assert!(result.ends_with(b"\r\n"), "Expected CRLF terminator");
    let payload = &result[header.len()..result.len() - 2];
    assert_eq!(payload.len(), large_val.len(), "Payload length mismatch");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_key_overwrite_type() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "morphkey", "stringval"]).await;
    let resp = cmd(&mut stream, &["TYPE", "morphkey"]).await;
    assert_resp(&resp, b"+string\r\n");

    cmd(&mut stream, &["DEL", "morphkey"]).await;

    cmd(&mut stream, &["LPUSH", "morphkey", "a"]).await;
    let resp = cmd(&mut stream, &["TYPE", "morphkey"]).await;
    assert_resp(&resp, b"+list\r\n");

    let _ = shutdown.send(true);
}
