mod test_support;
use test_support::{
    assert_resp, assert_resp_prefix, cmd, connect, free_port, resp_cmd, send_and_read,
    send_and_read_large, start_server,
};

fn resp_str(resp: &[u8]) -> String {
    String::from_utf8_lossy(resp).to_string()
}

// ---------------------------------------------------------------------------
// Stream Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_xadd_xlen() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["XADD", "mystream", "*", "field1", "value1"]).await;
    cmd(&mut stream, &["XADD", "mystream", "*", "field2", "value2"]).await;

    let resp = cmd(&mut stream, &["XLEN", "mystream"]).await;
    assert_resp(&resp, b":2\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xadd_returns_id() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["XADD", "mystream", "*", "f", "v"]).await;
    let s = resp_str(&resp);
    assert!(s.starts_with('$'), "Expected bulk string, got: {}", s);
    assert!(s.contains('-'), "Expected ID with '-', got: {}", s);

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xrange_all() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["XADD", "mystream", "*", "a", "1"]).await;
    cmd(&mut stream, &["XADD", "mystream", "*", "b", "2"]).await;
    cmd(&mut stream, &["XADD", "mystream", "*", "c", "3"]).await;

    let resp = cmd(&mut stream, &["XRANGE", "mystream", "-", "+"]).await;
    let s = resp_str(&resp);
    assert!(s.starts_with("*3\r\n"), "Expected 3 entries, got: {}", s);

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xrange_count() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    for i in 0..5 {
        cmd(&mut stream, &["XADD", "mystream", "*", "f", &i.to_string()]).await;
    }

    let resp = cmd(&mut stream, &["XRANGE", "mystream", "-", "+", "COUNT", "2"]).await;
    let s = resp_str(&resp);
    assert!(s.starts_with("*2\r\n"), "Expected 2 entries, got: {}", s);

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xrevrange() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let id1_resp = cmd(&mut stream, &["XADD", "mystream", "*", "f", "1"]).await;
    cmd(&mut stream, &["XADD", "mystream", "*", "f", "2"]).await;
    let id3_resp = cmd(&mut stream, &["XADD", "mystream", "*", "f", "3"]).await;

    let resp = cmd(&mut stream, &["XREVRANGE", "mystream", "+", "-"]).await;
    let s = resp_str(&resp);
    assert!(s.starts_with("*3\r\n"), "Expected 3 entries, got: {}", s);

    let id1_str = resp_str(&id1_resp);
    let id3_str = resp_str(&id3_resp);
    let id1_pos = s.find(&extract_id(&id1_str));
    let id3_pos = s.find(&extract_id(&id3_str));
    assert!(
        id3_pos < id1_pos,
        "Expected reverse order: id3 before id1 in response"
    );

    let _ = shutdown.send(true);
}

fn extract_id(bulk_resp: &str) -> String {
    let lines: Vec<&str> = bulk_resp.split("\r\n").collect();
    if lines.len() >= 2 {
        lines[1].to_string()
    } else {
        String::new()
    }
}

#[tokio::test]
async fn test_xadd_maxlen() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    for i in 0..5 {
        cmd(
            &mut stream,
            &["XADD", "mystream", "MAXLEN", "3", "*", "f", &i.to_string()],
        )
        .await;
    }

    let resp = cmd(&mut stream, &["XLEN", "mystream"]).await;
    assert_resp(&resp, b":3\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xtrim() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    for i in 0..5 {
        cmd(&mut stream, &["XADD", "mystream", "*", "f", &i.to_string()]).await;
    }

    cmd(&mut stream, &["XTRIM", "mystream", "MAXLEN", "2"]).await;

    let resp = cmd(&mut stream, &["XLEN", "mystream"]).await;
    assert_resp(&resp, b":2\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_xread_basic() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["XADD", "mystream", "*", "a", "1"]).await;
    cmd(&mut stream, &["XADD", "mystream", "*", "b", "2"]).await;

    let resp = cmd(
        &mut stream,
        &["XREAD", "COUNT", "10", "STREAMS", "mystream", "0-0"],
    )
    .await;
    let s = resp_str(&resp);
    assert!(
        s.starts_with("*1\r\n") || s.starts_with("-ERR"),
        "Expected array with 1 stream or ERR (multi-key not yet supported), got: {}",
        s
    );

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_stream_wrongtype() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "strkey", "value"]).await;
    let resp = cmd(&mut stream, &["XADD", "strkey", "*", "f", "v"]).await;
    assert_resp_prefix(&resp, b"-WRONGTYPE");

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// Server Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_ping_pong() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["PING"]).await;
    assert_resp(&resp, b"+PONG\r\n");

    let resp = cmd(&mut stream, &["PING", "message"]).await;
    assert_resp(&resp, b"$7\r\nmessage\r\n");

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
async fn test_info() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["INFO"]).await;
    let s = resp_str(&resp);
    assert!(s.starts_with('$'), "Expected bulk string, got: {}", s);
    assert!(
        s.contains("kora_version"),
        "Expected 'kora_version' in INFO response, got: {}",
        s
    );

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_dbsize_accuracy() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    for i in 0..10 {
        cmd(&mut stream, &["SET", &format!("key:{}", i), "val"]).await;
    }

    let resp = cmd(&mut stream, &["DBSIZE"]).await;
    assert_resp(&resp, b":10\r\n");

    cmd(&mut stream, &["DEL", "key:0", "key:1", "key:2"]).await;

    let resp = cmd(&mut stream, &["DBSIZE"]).await;
    assert_resp(&resp, b":7\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_flushdb_clears_all() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "str1", "val"]).await;
    cmd(&mut stream, &["SET", "str2", "val"]).await;
    cmd(&mut stream, &["LPUSH", "list1", "a", "b"]).await;
    cmd(&mut stream, &["HSET", "hash1", "f", "v"]).await;

    let resp = cmd(&mut stream, &["FLUSHDB"]).await;
    assert_resp(&resp, b"+OK\r\n");

    let resp = cmd(&mut stream, &["DBSIZE"]).await;
    assert_resp(&resp, b":0\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_command_info() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["COMMAND"]).await;
    let s = resp_str(&resp);
    assert!(
        s.starts_with('*'),
        "Expected array response from COMMAND, got: {}",
        s
    );

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// Pipelining & Concurrency
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pipeline_10_commands() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let mut pipeline = Vec::new();
    for i in 0..10 {
        pipeline.extend_from_slice(&resp_cmd(&[
            "SET",
            &format!("pk{}", i),
            &format!("pv{}", i),
        ]));
    }

    let resp = send_and_read(&mut stream, &pipeline).await;
    let expected = b"+OK\r\n".repeat(10);
    assert_resp(&resp, &expected);

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_pipeline_mixed_types() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let mut pipeline = Vec::new();
    pipeline.extend_from_slice(&resp_cmd(&["SET", "pstr", "hello"]));
    pipeline.extend_from_slice(&resp_cmd(&["LPUSH", "plst", "a", "b"]));
    pipeline.extend_from_slice(&resp_cmd(&["HSET", "phsh", "f1", "v1"]));
    pipeline.extend_from_slice(&resp_cmd(&["SADD", "pset", "m1", "m2"]));
    pipeline.extend_from_slice(&resp_cmd(&["ZADD", "pzset", "1.0", "z1"]));
    pipeline.extend_from_slice(&resp_cmd(&["GET", "pstr"]));
    pipeline.extend_from_slice(&resp_cmd(&["LRANGE", "plst", "0", "-1"]));
    pipeline.extend_from_slice(&resp_cmd(&["HGET", "phsh", "f1"]));
    pipeline.extend_from_slice(&resp_cmd(&["SMEMBERS", "pset"]));
    pipeline.extend_from_slice(&resp_cmd(&["ZSCORE", "pzset", "z1"]));

    let resp = send_and_read(&mut stream, &pipeline).await;
    let s = resp_str(&resp);

    assert!(
        s.contains("+OK\r\n"),
        "Expected SET OK in pipeline response"
    );
    assert!(
        s.contains(":2\r\n"),
        "Expected LPUSH :2 in pipeline response"
    );
    assert!(
        s.contains("$5\r\nhello\r\n"),
        "Expected GET hello in pipeline response"
    );
    assert!(
        s.contains("$2\r\nv1\r\n"),
        "Expected HGET v1 in pipeline response"
    );

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_concurrent_connections() {
    let port = free_port().await;
    let shutdown = start_server(port).await;

    let mut handles = Vec::new();
    for conn_id in 0..5u32 {
        let handle = tokio::spawn(async move {
            let mut stream = connect(port).await;
            for i in 0..10u32 {
                let key = format!("conn{}:key{}", conn_id, i);
                let val = format!("conn{}:val{}", conn_id, i);
                cmd(&mut stream, &["SET", &key, &val]).await;
            }
            for i in 0..10u32 {
                let key = format!("conn{}:key{}", conn_id, i);
                let expected_val = format!("conn{}:val{}", conn_id, i);
                let resp = cmd(&mut stream, &["GET", &key]).await;
                let expected = format!("${}\r\n{}\r\n", expected_val.len(), expected_val);
                assert_resp(&resp, expected.as_bytes());
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_pipeline_with_errors() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let mut pipeline = Vec::new();
    pipeline.extend_from_slice(&resp_cmd(&["SET", "errkey", "value"]));
    pipeline.extend_from_slice(&resp_cmd(&["LPUSH", "errkey", "item"]));
    pipeline.extend_from_slice(&resp_cmd(&["GET", "errkey"]));

    let resp = send_and_read(&mut stream, &pipeline).await;
    let s = resp_str(&resp);

    assert!(s.starts_with("+OK\r\n"), "First response should be +OK");
    assert!(
        s.contains("-WRONGTYPE"),
        "Second response should be WRONGTYPE error"
    );
    assert!(
        s.contains("$5\r\nvalue\r\n"),
        "Third response should return the value despite prior error"
    );

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_large_pipeline() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let mut set_pipeline = Vec::new();
    for i in 0..100 {
        set_pipeline.extend_from_slice(&resp_cmd(&[
            "SET",
            &format!("lp:{}", i),
            &format!("val:{}", i),
        ]));
    }
    let resp = send_and_read_large(&mut stream, &set_pipeline).await;
    let expected_sets = b"+OK\r\n".repeat(100);
    assert_resp(&resp, &expected_sets);

    let mut get_pipeline = Vec::new();
    for i in 0..100 {
        get_pipeline.extend_from_slice(&resp_cmd(&["GET", &format!("lp:{}", i)]));
    }
    let resp = send_and_read_large(&mut stream, &get_pipeline).await;
    for i in 0..100 {
        let val = format!("val:{}", i);
        let expected_fragment = format!("${}\r\n{}\r\n", val.len(), val);
        assert!(
            resp_str(&resp).contains(&expected_fragment),
            "Missing value for key lp:{} in response",
            i
        );
    }

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// Error Handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_wrong_arg_count_all_commands() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["GET"]).await;
    assert_resp_prefix(&resp, b"-ERR wrong number of arguments for 'GET'");

    let resp = cmd(&mut stream, &["SET", "onlykey"]).await;
    assert_resp_prefix(&resp, b"-ERR wrong number of arguments for 'SET'");

    let resp = cmd(&mut stream, &["DEL"]).await;
    assert_resp_prefix(&resp, b"-ERR wrong number of arguments for 'DEL'");

    let resp = cmd(&mut stream, &["LPUSH", "onlykey"]).await;
    assert_resp_prefix(&resp, b"-ERR wrong number of arguments for 'LPUSH'");

    let resp = cmd(&mut stream, &["HSET", "key", "field"]).await;
    assert_resp_prefix(&resp, b"-ERR wrong number of arguments for 'HSET'");

    let resp = cmd(&mut stream, &["SADD", "onlyset"]).await;
    assert_resp_prefix(&resp, b"-ERR wrong number of arguments for 'SADD'");

    let resp = cmd(&mut stream, &["ZADD", "key", "1.0"]).await;
    assert_resp_prefix(&resp, b"-ERR wrong number of arguments for 'ZADD'");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_wrongtype_cross_all() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    cmd(&mut stream, &["SET", "str_key", "hello"]).await;
    cmd(&mut stream, &["LPUSH", "list_key", "a"]).await;
    cmd(&mut stream, &["HSET", "hash_key", "f", "v"]).await;
    cmd(&mut stream, &["SADD", "set_key", "m"]).await;
    cmd(&mut stream, &["ZADD", "zset_key", "1.0", "m"]).await;

    let resp = cmd(&mut stream, &["INCR", "list_key"]).await;
    assert_resp_prefix(&resp, b"-WRONGTYPE");

    let resp = cmd(&mut stream, &["LPUSH", "hash_key", "item"]).await;
    assert_resp_prefix(&resp, b"-WRONGTYPE");

    let resp = cmd(&mut stream, &["HGET", "set_key", "f"]).await;
    assert_resp_prefix(&resp, b"-WRONGTYPE");

    let resp = cmd(&mut stream, &["SADD", "zset_key", "new"]).await;
    assert_resp_prefix(&resp, b"-WRONGTYPE");

    let resp = cmd(&mut stream, &["ZADD", "str_key", "1.0", "m"]).await;
    assert_resp_prefix(&resp, b"-WRONGTYPE");

    let _ = shutdown.send(true);
}

// ---------------------------------------------------------------------------
// HELLO / RESP3
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_hello_resp2() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["HELLO", "2"]).await;
    let s = resp_str(&resp);
    assert!(
        s.starts_with("*6\r\n"),
        "Expected RESP2 array (map downgraded to flat array), got: {}",
        s
    );
    assert!(s.contains("server"), "Expected 'server' in HELLO response");
    assert!(s.contains("kora"), "Expected 'kora' in HELLO response");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_hello_resp3() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["HELLO", "3"]).await;
    let s = resp_str(&resp);
    assert!(
        s.starts_with("%3\r\n"),
        "Expected RESP3 map type, got: {}",
        s
    );
    assert!(s.contains("server"), "Expected 'server' in HELLO response");

    let resp = cmd(&mut stream, &["PING"]).await;
    assert_resp(&resp, b"+PONG\r\n");

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_hello_invalid_version() {
    let port = free_port().await;
    let shutdown = start_server(port).await;
    let mut stream = connect(port).await;

    let resp = cmd(&mut stream, &["HELLO", "4"]).await;
    assert_resp_prefix(&resp, b"-NOPROTO");

    let _ = shutdown.send(true);
}
