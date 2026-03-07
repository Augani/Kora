//! TCP integration tests for kora-server Redis protocol compatibility.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use kora_server::{KoraServer, ServerConfig};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Find a free port by binding to :0 and reading the assigned port.
async fn free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// Build a RESP bulk string: $<len>\r\n<data>\r\n
fn bulk(s: &str) -> Vec<u8> {
    format!("${}\r\n{}\r\n", s.len(), s).into_bytes()
}

/// Encode a command as a RESP array of bulk strings.
fn resp_cmd(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        out.extend_from_slice(&bulk(arg));
    }
    out
}

/// Start a server on the given port and return the shutdown sender.
/// The server runs in a background task.
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
    // Give the server a moment to bind.
    tokio::time::sleep(Duration::from_millis(50)).await;
    tx
}

/// Connect to the server, retrying briefly if necessary.
async fn connect(port: u16) -> TcpStream {
    for _ in 0..20 {
        match TcpStream::connect(format!("127.0.0.1:{}", port)).await {
            Ok(s) => return s,
            Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
        }
    }
    panic!("could not connect to server on port {}", port);
}

/// Send a RESP command and read the full response as bytes.
async fn send_and_read(stream: &mut TcpStream, cmd: &[u8]) -> Vec<u8> {
    stream.write_all(cmd).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let mut buf = vec![0u8; 8192];
    let n = stream.read(&mut buf).await.unwrap();
    buf.truncate(n);
    buf
}

/// Send a command (given as string args) and return the raw RESP response.
async fn cmd(stream: &mut TcpStream, args: &[&str]) -> Vec<u8> {
    send_and_read(stream, &resp_cmd(args)).await
}

/// Assert the response equals the given bytes exactly.
fn assert_resp(resp: &[u8], expected: &[u8]) {
    assert_eq!(
        resp,
        expected,
        "\nExpected: {:?}\n     Got: {:?}",
        String::from_utf8_lossy(expected),
        String::from_utf8_lossy(resp),
    );
}

/// Assert the response starts with the given prefix (useful for errors).
fn assert_resp_prefix(resp: &[u8], prefix: &[u8]) {
    assert!(
        resp.starts_with(prefix),
        "\nExpected prefix: {:?}\n           Got: {:?}",
        String::from_utf8_lossy(prefix),
        String::from_utf8_lossy(resp),
    );
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
