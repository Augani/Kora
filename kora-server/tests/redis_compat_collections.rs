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

async fn send_and_read(stream: &mut TcpStream, cmd: &[u8]) -> Vec<u8> {
    stream.write_all(cmd).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let mut buf = vec![0u8; 8192];
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

fn parse_resp_array(resp: &[u8]) -> Vec<String> {
    let text = String::from_utf8_lossy(resp).to_string();
    let mut results = Vec::new();
    let lines: Vec<&str> = text.split("\r\n").collect();
    let mut idx = 1;
    while idx < lines.len() {
        let line = lines[idx];
        if let Some(stripped) = line.strip_prefix('$') {
            let len: i64 = stripped.parse().unwrap_or(-1);
            if len >= 0 {
                idx += 1;
                if idx < lines.len() {
                    results.push(lines[idx].to_string());
                }
            }
        } else if let Some(stripped) = line.strip_prefix(':') {
            results.push(stripped.to_string());
        }
        idx += 1;
    }
    results
}

fn resp_array_count(resp: &[u8]) -> Option<usize> {
    let text = String::from_utf8_lossy(resp);
    let first_line = text.split("\r\n").next()?;
    if let Some(stripped) = first_line.strip_prefix('*') {
        stripped.parse().ok()
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// List Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_lpush_rpush() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["LPUSH", "list1", "a"]).await;
    cmd(&mut stream, &["LPUSH", "list1", "b"]).await;
    cmd(&mut stream, &["LPUSH", "list1", "c"]).await;

    let resp = cmd(&mut stream, &["LRANGE", "list1", "0", "-1"]).await;
    assert_resp(&resp, b"*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n");

    cmd(&mut stream, &["RPUSH", "list2", "a"]).await;
    cmd(&mut stream, &["RPUSH", "list2", "b"]).await;
    cmd(&mut stream, &["RPUSH", "list2", "c"]).await;

    let resp = cmd(&mut stream, &["LRANGE", "list2", "0", "-1"]).await;
    assert_resp(&resp, b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_lpop_rpop() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["RPUSH", "poplist", "a", "b", "c"]).await;

    let resp = cmd(&mut stream, &["LPOP", "poplist"]).await;
    assert_resp(&resp, b"$1\r\na\r\n");

    let resp = cmd(&mut stream, &["RPOP", "poplist"]).await;
    assert_resp(&resp, b"$1\r\nc\r\n");

    let resp = cmd(&mut stream, &["LLEN", "poplist"]).await;
    assert_resp(&resp, b":1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_llen() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["LLEN", "nolist"]).await;
    assert_resp(&resp, b":0\r\n");

    cmd(&mut stream, &["RPUSH", "mylist", "a", "b", "c"]).await;

    let resp = cmd(&mut stream, &["LLEN", "mylist"]).await;
    assert_resp(&resp, b":3\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_lrange_bounds() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["RPUSH", "rlist", "a", "b", "c", "d", "e"]).await;

    let resp = cmd(&mut stream, &["LRANGE", "rlist", "-3", "-1"]).await;
    assert_resp(&resp, b"*3\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n");

    let resp = cmd(&mut stream, &["LRANGE", "rlist", "0", "100"]).await;
    assert_resp(
        &resp,
        b"*5\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n",
    );

    let resp = cmd(&mut stream, &["LRANGE", "rlist", "10", "20"]).await;
    assert_resp(&resp, b"*0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_lindex() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["RPUSH", "ilist", "a", "b", "c"]).await;

    let resp = cmd(&mut stream, &["LINDEX", "ilist", "0"]).await;
    assert_resp(&resp, b"$1\r\na\r\n");

    let resp = cmd(&mut stream, &["LINDEX", "ilist", "-1"]).await;
    assert_resp(&resp, b"$1\r\nc\r\n");

    let resp = cmd(&mut stream, &["LINDEX", "ilist", "100"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_lpush_multiple() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["LPUSH", "mlist", "a", "b", "c"]).await;
    assert_resp(&resp, b":3\r\n");

    let resp = cmd(&mut stream, &["LRANGE", "mlist", "0", "-1"]).await;
    assert_resp(&resp, b"*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_list_wrongtype() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "strkey", "value"]).await;

    let resp = cmd(&mut stream, &["LPUSH", "strkey", "item"]).await;
    assert_resp_prefix(&resp, b"-WRONGTYPE");

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// Hash Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_hset_hget_basic() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["HSET", "h1", "field1", "value1"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["HGET", "h1", "field1"]).await;
    assert_resp(&resp, b"$6\r\nvalue1\r\n");

    let resp = cmd(&mut stream, &["HGET", "h1", "nosuchfield"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_hset_multiple_fields() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(
        &mut stream,
        &["HSET", "h2", "f1", "v1", "f2", "v2", "f3", "v3"],
    )
    .await;
    assert_resp(&resp, b":3\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_hdel() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(
        &mut stream,
        &["HSET", "h3", "f1", "v1", "f2", "v2", "f3", "v3"],
    )
    .await;

    let resp = cmd(&mut stream, &["HDEL", "h3", "f1", "f2"]).await;
    assert_resp(&resp, b":2\r\n");

    let resp = cmd(&mut stream, &["HLEN", "h3"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["HGET", "h3", "f1"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_hgetall() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(
        &mut stream,
        &["HSET", "h4", "f1", "v1", "f2", "v2", "f3", "v3"],
    )
    .await;

    let resp = cmd(&mut stream, &["HGETALL", "h4"]).await;
    let count = resp_array_count(&resp);
    assert_eq!(
        count,
        Some(6),
        "HGETALL should return 6 elements (3 field-value pairs)"
    );

    let elements = parse_resp_array(&resp);
    assert!(elements.contains(&"f1".to_string()));
    assert!(elements.contains(&"v1".to_string()));
    assert!(elements.contains(&"f2".to_string()));
    assert!(elements.contains(&"v2".to_string()));
    assert!(elements.contains(&"f3".to_string()));
    assert!(elements.contains(&"v3".to_string()));

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_hlen() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["HLEN", "nohash"]).await;
    assert_resp(&resp, b":0\r\n");

    cmd(&mut stream, &["HSET", "h5", "f1", "v1", "f2", "v2"]).await;

    let resp = cmd(&mut stream, &["HLEN", "h5"]).await;
    assert_resp(&resp, b":2\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_hexists() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["HSET", "h6", "f1", "v1"]).await;

    let resp = cmd(&mut stream, &["HEXISTS", "h6", "f1"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["HEXISTS", "h6", "missing"]).await;
    assert_resp(&resp, b":0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_hincrby() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["HINCRBY", "h7", "counter", "5"]).await;
    assert_resp(&resp, b":5\r\n");

    let resp = cmd(&mut stream, &["HINCRBY", "h7", "counter", "3"]).await;
    assert_resp(&resp, b":8\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_hash_wrongtype() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "skey", "val"]).await;

    let resp = cmd(&mut stream, &["HSET", "skey", "f", "v"]).await;
    assert_resp_prefix(&resp, b"-WRONGTYPE");

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// Set Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sadd_srem() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SADD", "s1", "a", "b", "c"]).await;

    let resp = cmd(&mut stream, &["SREM", "s1", "b"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["SCARD", "s1"]).await;
    assert_resp(&resp, b":2\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_sadd_duplicates() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["SADD", "s2", "x"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["SADD", "s2", "x"]).await;
    assert_resp(&resp, b":0\r\n");

    let resp = cmd(&mut stream, &["SCARD", "s2"]).await;
    assert_resp(&resp, b":1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_sismember() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SADD", "s3", "hello"]).await;

    let resp = cmd(&mut stream, &["SISMEMBER", "s3", "hello"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["SISMEMBER", "s3", "world"]).await;
    assert_resp(&resp, b":0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_smembers_order() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SADD", "s4", "alpha", "beta", "gamma"]).await;

    let resp = cmd(&mut stream, &["SMEMBERS", "s4"]).await;
    let count = resp_array_count(&resp);
    assert_eq!(count, Some(3));

    let members = parse_resp_array(&resp);
    assert!(members.contains(&"alpha".to_string()));
    assert!(members.contains(&"beta".to_string()));
    assert!(members.contains(&"gamma".to_string()));

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_scard() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["SCARD", "noset"]).await;
    assert_resp(&resp, b":0\r\n");

    cmd(&mut stream, &["SADD", "s5", "a", "b"]).await;

    let resp = cmd(&mut stream, &["SCARD", "s5"]).await;
    assert_resp(&resp, b":2\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_set_wrongtype() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "strk", "val"]).await;

    let resp = cmd(&mut stream, &["SADD", "strk", "member"]).await;
    assert_resp_prefix(&resp, b"-WRONGTYPE");

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// Sorted Set Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_zadd_zscore() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["ZADD", "zs1", "1", "a", "2", "b", "3", "c"]).await;
    assert_resp(&resp, b":3\r\n");

    let resp = cmd(&mut stream, &["ZSCORE", "zs1", "a"]).await;
    assert_resp(&resp, b"$1\r\n1\r\n");

    let resp = cmd(&mut stream, &["ZSCORE", "zs1", "nonexistent"]).await;
    assert_resp(&resp, b"$-1\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_zadd_update() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["ZADD", "zs2", "1", "a"]).await;

    let resp = cmd(&mut stream, &["ZADD", "zs2", "5", "a"]).await;
    assert_resp(&resp, b":0\r\n");

    let resp = cmd(&mut stream, &["ZSCORE", "zs2", "a"]).await;
    assert_resp(&resp, b"$1\r\n5\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_zrem() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["ZADD", "zs3", "1", "a", "2", "b", "3", "c"]).await;

    let resp = cmd(&mut stream, &["ZREM", "zs3", "b"]).await;
    assert_resp(&resp, b":1\r\n");

    let resp = cmd(&mut stream, &["ZCARD", "zs3"]).await;
    assert_resp(&resp, b":2\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_zrank_zrevrank() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["ZADD", "zs4", "1", "a", "2", "b", "3", "c"]).await;

    let resp = cmd(&mut stream, &["ZRANK", "zs4", "a"]).await;
    assert_resp(&resp, b":0\r\n");

    let resp = cmd(&mut stream, &["ZRANK", "zs4", "c"]).await;
    assert_resp(&resp, b":2\r\n");

    let resp = cmd(&mut stream, &["ZREVRANK", "zs4", "c"]).await;
    assert_resp(&resp, b":0\r\n");

    let resp = cmd(&mut stream, &["ZREVRANK", "zs4", "a"]).await;
    assert_resp(&resp, b":2\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_zcard() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["ZCARD", "nozset"]).await;
    assert_resp(&resp, b":0\r\n");

    cmd(&mut stream, &["ZADD", "zs5", "1", "a", "2", "b"]).await;

    let resp = cmd(&mut stream, &["ZCARD", "zs5"]).await;
    assert_resp(&resp, b":2\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_zrange() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(
        &mut stream,
        &[
            "ZADD", "zs6", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
        ],
    )
    .await;

    let resp = cmd(&mut stream, &["ZRANGE", "zs6", "0", "2"]).await;
    assert_resp(&resp, b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");

    let resp = cmd(&mut stream, &["ZRANGE", "zs6", "0", "-1"]).await;
    assert_resp(
        &resp,
        b"*5\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n",
    );

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_zrange_withscores() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["ZADD", "zs7", "1", "a", "2", "b", "3", "c"]).await;

    let resp = cmd(&mut stream, &["ZRANGE", "zs7", "0", "-1", "WITHSCORES"]).await;
    assert_resp(
        &resp,
        b"*6\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n",
    );

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_zrevrange() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(
        &mut stream,
        &[
            "ZADD", "zs8", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
        ],
    )
    .await;

    let resp = cmd(&mut stream, &["ZREVRANGE", "zs8", "0", "2"]).await;
    assert_resp(&resp, b"*3\r\n$1\r\ne\r\n$1\r\nd\r\n$1\r\nc\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_zrangebyscore() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(
        &mut stream,
        &[
            "ZADD", "zs9", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
        ],
    )
    .await;

    let resp = cmd(&mut stream, &["ZRANGEBYSCORE", "zs9", "1", "3"]).await;
    assert_resp(&resp, b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_zrangebyscore_limit() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(
        &mut stream,
        &[
            "ZADD", "zs10", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
        ],
    )
    .await;

    let resp = cmd(
        &mut stream,
        &["ZRANGEBYSCORE", "zs10", "-inf", "+inf", "LIMIT", "1", "2"],
    )
    .await;
    assert_resp(&resp, b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_zincrby() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["ZADD", "zs11", "2", "member"]).await;

    let resp = cmd(&mut stream, &["ZINCRBY", "zs11", "5", "member"]).await;
    assert_resp(&resp, b"$1\r\n7\r\n");

    let resp = cmd(&mut stream, &["ZSCORE", "zs11", "member"]).await;
    assert_resp(&resp, b"$1\r\n7\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_zcount() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(
        &mut stream,
        &[
            "ZADD", "zs12", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
        ],
    )
    .await;

    let resp = cmd(&mut stream, &["ZCOUNT", "zs12", "1", "3"]).await;
    assert_resp(&resp, b":3\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_zset_wrongtype() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "zstr", "value"]).await;

    let resp = cmd(&mut stream, &["ZADD", "zstr", "1", "member"]).await;
    assert_resp_prefix(&resp, b"-WRONGTYPE");

    let _ = shutdown.send(true);
}
