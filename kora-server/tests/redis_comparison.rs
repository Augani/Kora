//! Side-by-side comparison tests: run identical commands against Redis and Kōra,
//! verify byte-identical RESP responses.

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

async fn start_kora(port: u16) -> tokio::sync::watch::Sender<bool> {
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

async fn connect_to(port: u16) -> TcpStream {
    for _ in 0..20 {
        match TcpStream::connect(format!("127.0.0.1:{}", port)).await {
            Ok(s) => return s,
            Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
        }
    }
    panic!("could not connect to port {}", port);
}

async fn send_cmd(stream: &mut TcpStream, args: &[&str]) -> Vec<u8> {
    let cmd = resp_cmd(args);
    stream.write_all(&cmd).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let mut buf = vec![0u8; 65536];
    let n = stream.read(&mut buf).await.unwrap();
    buf.truncate(n);
    buf
}

fn resp_to_string(resp: &[u8]) -> String {
    String::from_utf8_lossy(resp).to_string()
}

const REDIS_PORT: u16 = 6399;

fn is_redis_available() -> bool {
    std::net::TcpStream::connect(format!("127.0.0.1:{}", REDIS_PORT)).is_ok()
}

struct DualTarget {
    kora_stream: TcpStream,
    redis_stream: TcpStream,
    _shutdown: tokio::sync::watch::Sender<bool>,
}

impl DualTarget {
    async fn new() -> Option<Self> {
        if !is_redis_available() {
            return None;
        }

        let kora_port = free_port().await;
        let shutdown = start_kora(kora_port).await;
        let kora_stream = connect_to(kora_port).await;
        let redis_stream = connect_to(REDIS_PORT).await;

        let mut redis = redis_stream;
        send_cmd(&mut redis, &["FLUSHALL"]).await;

        Some(Self {
            kora_stream,
            redis_stream: redis,
            _shutdown: shutdown,
        })
    }

    async fn compare(&mut self, args: &[&str]) -> (Vec<u8>, Vec<u8>) {
        let kora_resp = send_cmd(&mut self.kora_stream, args).await;
        let redis_resp = send_cmd(&mut self.redis_stream, args).await;
        (kora_resp, redis_resp)
    }

    async fn assert_match(&mut self, args: &[&str]) {
        let (kora, redis) = self.compare(args).await;
        assert_eq!(
            resp_to_string(&kora),
            resp_to_string(&redis),
            "MISMATCH for {:?}\n  Kōra:  {}\n  Redis: {}",
            args,
            resp_to_string(&kora),
            resp_to_string(&redis),
        );
    }

    async fn assert_kora_prefix_matches_redis_prefix(&mut self, args: &[&str], prefix_len: usize) {
        let (kora, redis) = self.compare(args).await;
        let kora_prefix = &kora[..kora.len().min(prefix_len)];
        let redis_prefix = &redis[..redis.len().min(prefix_len)];
        assert_eq!(
            resp_to_string(kora_prefix),
            resp_to_string(redis_prefix),
            "PREFIX MISMATCH for {:?}\n  Kōra:  {}\n  Redis: {}",
            args,
            resp_to_string(&kora),
            resp_to_string(&redis),
        );
    }

    async fn run_cmd(&mut self, args: &[&str]) {
        send_cmd(&mut self.kora_stream, args).await;
        send_cmd(&mut self.redis_stream, args).await;
    }
}

macro_rules! dual_test {
    ($name:ident, |$t:ident : &mut DualTarget| async move $body:block) => {
        #[tokio::test]
        async fn $name() {
            let Some(mut target) = DualTarget::new().await else {
                eprintln!(
                    "Skipping {} — Redis not available on port {}",
                    stringify!($name),
                    REDIS_PORT
                );
                return;
            };
            let $t = &mut target;
            $body
        }
    };
}

dual_test!(cmp_ping, |t: &mut DualTarget| async move {
    t.assert_match(&["PING"]).await;
});

dual_test!(cmp_ping_message, |t: &mut DualTarget| async move {
    t.assert_match(&["PING", "hello"]).await;
});

dual_test!(cmp_echo, |t: &mut DualTarget| async move {
    t.assert_match(&["ECHO", "test message"]).await;
});

dual_test!(cmp_set_get, |t: &mut DualTarget| async move {
    t.assert_match(&["SET", "key1", "value1"]).await;
    t.assert_match(&["GET", "key1"]).await;
    t.assert_match(&["GET", "nonexistent"]).await;
});

dual_test!(cmp_set_nx, |t: &mut DualTarget| async move {
    t.assert_match(&["SET", "nx_key", "first", "NX"]).await;
    t.assert_match(&["SET", "nx_key", "second", "NX"]).await;
    t.assert_match(&["GET", "nx_key"]).await;
});

dual_test!(cmp_set_xx, |t: &mut DualTarget| async move {
    t.assert_match(&["SET", "xx_key", "value", "XX"]).await;
    t.run_cmd(&["SET", "xx_key", "first"]).await;
    t.assert_match(&["SET", "xx_key", "second", "XX"]).await;
    t.assert_match(&["GET", "xx_key"]).await;
});

dual_test!(cmp_getset, |t: &mut DualTarget| async move {
    t.assert_match(&["SET", "gs_key", "old"]).await;
    t.assert_match(&["GETSET", "gs_key", "new"]).await;
    t.assert_match(&["GET", "gs_key"]).await;
});

dual_test!(cmp_append, |t: &mut DualTarget| async move {
    t.assert_match(&["APPEND", "ap_key", "hello"]).await;
    t.assert_match(&["APPEND", "ap_key", " world"]).await;
    t.assert_match(&["GET", "ap_key"]).await;
});

dual_test!(cmp_strlen, |t: &mut DualTarget| async move {
    t.run_cmd(&["SET", "sl_key", "hello"]).await;
    t.assert_match(&["STRLEN", "sl_key"]).await;
    t.assert_match(&["STRLEN", "nonexistent"]).await;
});

dual_test!(cmp_incr_decr, |t: &mut DualTarget| async move {
    t.assert_match(&["INCR", "counter"]).await;
    t.assert_match(&["INCR", "counter"]).await;
    t.assert_match(&["DECR", "counter"]).await;
    t.assert_match(&["INCRBY", "counter", "10"]).await;
    t.assert_match(&["DECRBY", "counter", "3"]).await;
    t.assert_match(&["GET", "counter"]).await;
});

dual_test!(cmp_incr_not_integer, |t: &mut DualTarget| async move {
    t.run_cmd(&["SET", "notnum", "abc"]).await;
    t.assert_kora_prefix_matches_redis_prefix(&["INCR", "notnum"], 5)
        .await;
});

dual_test!(cmp_mset_mget, |t: &mut DualTarget| async move {
    t.assert_match(&["MSET", "mk1", "mv1", "mk2", "mv2", "mk3", "mv3"])
        .await;
    t.assert_match(&["MGET", "mk1", "mk2", "mk3", "nonexistent"])
        .await;
});

dual_test!(cmp_setnx, |t: &mut DualTarget| async move {
    t.assert_match(&["SETNX", "snx", "val1"]).await;
    t.assert_match(&["SETNX", "snx", "val2"]).await;
    t.assert_match(&["GET", "snx"]).await;
});

dual_test!(cmp_del, |t: &mut DualTarget| async move {
    t.run_cmd(&["SET", "d1", "v"]).await;
    t.run_cmd(&["SET", "d2", "v"]).await;
    t.assert_match(&["DEL", "d1", "d2", "d3"]).await;
    t.assert_match(&["GET", "d1"]).await;
});

dual_test!(cmp_exists, |t: &mut DualTarget| async move {
    t.run_cmd(&["SET", "e1", "v"]).await;
    t.run_cmd(&["SET", "e2", "v"]).await;
    t.assert_match(&["EXISTS", "e1"]).await;
    t.assert_match(&["EXISTS", "e1", "e2", "e3"]).await;
    t.assert_match(&["EXISTS", "nope"]).await;
});

dual_test!(cmp_type, |t: &mut DualTarget| async move {
    t.run_cmd(&["SET", "t_str", "v"]).await;
    t.run_cmd(&["LPUSH", "t_list", "v"]).await;
    t.run_cmd(&["HSET", "t_hash", "f", "v"]).await;
    t.run_cmd(&["SADD", "t_set", "v"]).await;
    t.run_cmd(&["ZADD", "t_zset", "1.0", "v"]).await;
    t.assert_match(&["TYPE", "t_str"]).await;
    t.assert_match(&["TYPE", "t_list"]).await;
    t.assert_match(&["TYPE", "t_hash"]).await;
    t.assert_match(&["TYPE", "t_set"]).await;
    t.assert_match(&["TYPE", "t_zset"]).await;
    t.assert_match(&["TYPE", "nonexistent"]).await;
});

dual_test!(cmp_expire_ttl, |t: &mut DualTarget| async move {
    t.run_cmd(&["SET", "ttl_key", "val"]).await;
    t.assert_match(&["TTL", "ttl_key"]).await;
    t.assert_match(&["EXPIRE", "ttl_key", "100"]).await;
    let (kora_ttl, redis_ttl) = t.compare(&["TTL", "ttl_key"]).await;
    let k = resp_to_string(&kora_ttl);
    let r = resp_to_string(&redis_ttl);
    assert!(
        k.contains("100") || k.contains("99"),
        "Kōra TTL unexpected: {}",
        k
    );
    assert!(
        r.contains("100") || r.contains("99"),
        "Redis TTL unexpected: {}",
        r
    );
});

dual_test!(cmp_persist, |t: &mut DualTarget| async move {
    t.run_cmd(&["SET", "p_key", "val"]).await;
    t.run_cmd(&["EXPIRE", "p_key", "100"]).await;
    t.assert_match(&["PERSIST", "p_key"]).await;
    t.assert_match(&["TTL", "p_key"]).await;
});

dual_test!(cmp_dbsize, |t: &mut DualTarget| async move {
    t.assert_match(&["DBSIZE"]).await;
    t.run_cmd(&["SET", "db1", "v"]).await;
    t.run_cmd(&["SET", "db2", "v"]).await;
    t.assert_match(&["DBSIZE"]).await;
});

dual_test!(cmp_flushdb, |t: &mut DualTarget| async move {
    t.run_cmd(&["SET", "f1", "v"]).await;
    t.assert_match(&["FLUSHDB"]).await;
    t.assert_match(&["DBSIZE"]).await;
});

dual_test!(cmp_lpush_lrange, |t: &mut DualTarget| async move {
    t.assert_match(&["LPUSH", "list", "a"]).await;
    t.assert_match(&["LPUSH", "list", "b"]).await;
    t.assert_match(&["RPUSH", "list", "c"]).await;
    t.assert_match(&["LRANGE", "list", "0", "-1"]).await;
    t.assert_match(&["LLEN", "list"]).await;
});

dual_test!(cmp_lpop_rpop, |t: &mut DualTarget| async move {
    t.run_cmd(&["RPUSH", "poplist", "a", "b", "c"]).await;
    t.assert_match(&["LPOP", "poplist"]).await;
    t.assert_match(&["RPOP", "poplist"]).await;
    t.assert_match(&["LLEN", "poplist"]).await;
});

dual_test!(cmp_lindex, |t: &mut DualTarget| async move {
    t.run_cmd(&["RPUSH", "idx", "a", "b", "c"]).await;
    t.assert_match(&["LINDEX", "idx", "0"]).await;
    t.assert_match(&["LINDEX", "idx", "2"]).await;
    t.assert_match(&["LINDEX", "idx", "-1"]).await;
    t.assert_match(&["LINDEX", "idx", "99"]).await;
});

dual_test!(cmp_hset_hget, |t: &mut DualTarget| async move {
    t.assert_match(&["HSET", "hash", "f1", "v1", "f2", "v2"])
        .await;
    t.assert_match(&["HGET", "hash", "f1"]).await;
    t.assert_match(&["HGET", "hash", "nosuch"]).await;
    t.assert_match(&["HLEN", "hash"]).await;
    t.assert_match(&["HEXISTS", "hash", "f1"]).await;
    t.assert_match(&["HEXISTS", "hash", "nope"]).await;
});

dual_test!(cmp_hdel, |t: &mut DualTarget| async move {
    t.run_cmd(&["HSET", "hdel", "f1", "v1", "f2", "v2"]).await;
    t.assert_match(&["HDEL", "hdel", "f1", "f3"]).await;
    t.assert_match(&["HLEN", "hdel"]).await;
});

dual_test!(cmp_hincrby, |t: &mut DualTarget| async move {
    t.assert_match(&["HINCRBY", "hinc", "counter", "5"]).await;
    t.assert_match(&["HINCRBY", "hinc", "counter", "3"]).await;
    t.assert_match(&["HGET", "hinc", "counter"]).await;
});

dual_test!(cmp_sadd_scard, |t: &mut DualTarget| async move {
    t.assert_match(&["SADD", "set", "a", "b", "c"]).await;
    t.assert_match(&["SADD", "set", "a"]).await;
    t.assert_match(&["SCARD", "set"]).await;
    t.assert_match(&["SISMEMBER", "set", "a"]).await;
    t.assert_match(&["SISMEMBER", "set", "z"]).await;
});

dual_test!(cmp_srem, |t: &mut DualTarget| async move {
    t.run_cmd(&["SADD", "srem", "a", "b", "c"]).await;
    t.assert_match(&["SREM", "srem", "b", "z"]).await;
    t.assert_match(&["SCARD", "srem"]).await;
});

dual_test!(cmp_zadd_zscore, |t: &mut DualTarget| async move {
    t.assert_match(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"])
        .await;
    t.assert_match(&["ZSCORE", "zs", "b"]).await;
    t.assert_match(&["ZSCORE", "zs", "nonexistent"]).await;
    t.assert_match(&["ZCARD", "zs"]).await;
});

dual_test!(cmp_zrank, |t: &mut DualTarget| async move {
    t.run_cmd(&["ZADD", "zr", "1", "a", "2", "b", "3", "c"])
        .await;
    t.assert_match(&["ZRANK", "zr", "a"]).await;
    t.assert_match(&["ZRANK", "zr", "c"]).await;
    t.assert_match(&["ZRANK", "zr", "nonexistent"]).await;
    t.assert_match(&["ZREVRANK", "zr", "a"]).await;
});

dual_test!(cmp_zrange, |t: &mut DualTarget| async move {
    t.run_cmd(&["ZADD", "zrng", "1", "a", "2", "b", "3", "c", "4", "d"])
        .await;
    t.assert_match(&["ZRANGE", "zrng", "0", "-1"]).await;
    t.assert_match(&["ZRANGE", "zrng", "0", "1"]).await;
    t.assert_match(&["ZRANGE", "zrng", "1", "2", "WITHSCORES"])
        .await;
});

dual_test!(cmp_zrevrange, |t: &mut DualTarget| async move {
    t.run_cmd(&["ZADD", "zrr", "1", "a", "2", "b", "3", "c"])
        .await;
    t.assert_match(&["ZREVRANGE", "zrr", "0", "-1"]).await;
    t.assert_match(&["ZREVRANGE", "zrr", "0", "1", "WITHSCORES"])
        .await;
});

dual_test!(cmp_zrangebyscore, |t: &mut DualTarget| async move {
    t.run_cmd(&["ZADD", "zbs", "1", "a", "2", "b", "3", "c", "4", "d"])
        .await;
    t.assert_match(&["ZRANGEBYSCORE", "zbs", "2", "3"]).await;
    t.assert_match(&["ZRANGEBYSCORE", "zbs", "-inf", "+inf"])
        .await;
    t.assert_match(&["ZRANGEBYSCORE", "zbs", "-inf", "+inf", "LIMIT", "1", "2"])
        .await;
});

dual_test!(cmp_zincrby, |t: &mut DualTarget| async move {
    t.run_cmd(&["ZADD", "zi", "1", "member"]).await;
    t.assert_match(&["ZINCRBY", "zi", "5", "member"]).await;
    t.assert_match(&["ZSCORE", "zi", "member"]).await;
});

dual_test!(cmp_zcount, |t: &mut DualTarget| async move {
    t.run_cmd(&["ZADD", "zc", "1", "a", "2", "b", "3", "c", "4", "d"])
        .await;
    t.assert_match(&["ZCOUNT", "zc", "2", "3"]).await;
    t.assert_match(&["ZCOUNT", "zc", "-inf", "+inf"]).await;
});

dual_test!(cmp_wrongtype_errors, |t: &mut DualTarget| async move {
    t.run_cmd(&["SET", "str", "val"]).await;
    t.assert_kora_prefix_matches_redis_prefix(&["LPUSH", "str", "item"], 10)
        .await;
    t.assert_kora_prefix_matches_redis_prefix(&["SADD", "str", "item"], 10)
        .await;
    t.assert_kora_prefix_matches_redis_prefix(&["ZADD", "str", "1", "item"], 10)
        .await;
    t.assert_kora_prefix_matches_redis_prefix(&["HSET", "str", "f", "v"], 10)
        .await;
});

dual_test!(cmp_wrong_arity, |t: &mut DualTarget| async move {
    t.assert_kora_prefix_matches_redis_prefix(&["GET"], 4).await;
    t.assert_kora_prefix_matches_redis_prefix(&["SET", "only"], 4)
        .await;
});

dual_test!(cmp_unknown_command, |t: &mut DualTarget| async move {
    t.assert_kora_prefix_matches_redis_prefix(&["TOTALLYUNKNOWN"], 4)
        .await;
});

dual_test!(cmp_empty_string, |t: &mut DualTarget| async move {
    t.assert_match(&["SET", "empty", ""]).await;
    t.assert_match(&["GET", "empty"]).await;
    t.assert_match(&["STRLEN", "empty"]).await;
});

dual_test!(cmp_large_value, |t: &mut DualTarget| async move {
    let big = "x".repeat(10000);
    t.run_cmd(&["SET", "big", &big]).await;
    t.assert_match(&["GET", "big"]).await;
    t.assert_match(&["STRLEN", "big"]).await;
});

dual_test!(cmp_overwrite_type, |t: &mut DualTarget| async move {
    t.run_cmd(&["SET", "morph", "string"]).await;
    t.assert_match(&["TYPE", "morph"]).await;
    t.run_cmd(&["DEL", "morph"]).await;
    t.run_cmd(&["LPUSH", "morph", "item"]).await;
    t.assert_match(&["TYPE", "morph"]).await;
});

dual_test!(cmp_nonexistent_type, |t: &mut DualTarget| async move {
    t.assert_match(&["TYPE", "nosuchkey"]).await;
});

dual_test!(cmp_pexpire_pttl, |t: &mut DualTarget| async move {
    t.run_cmd(&["SET", "pttl_key", "val"]).await;
    t.assert_match(&["PEXPIRE", "pttl_key", "5000"]).await;
    let (kora_pttl, redis_pttl) = t.compare(&["PTTL", "pttl_key"]).await;
    let k = resp_to_string(&kora_pttl);
    let r = resp_to_string(&redis_pttl);
    let kv: i64 = k
        .trim()
        .trim_start_matches(':')
        .trim()
        .parse()
        .unwrap_or(-999);
    let rv: i64 = r
        .trim()
        .trim_start_matches(':')
        .trim()
        .parse()
        .unwrap_or(-999);
    assert!(kv > 4000 && kv <= 5000, "Kōra PTTL out of range: {}", kv);
    assert!(rv > 4000 && rv <= 5000, "Redis PTTL out of range: {}", rv);
});

dual_test!(cmp_pipeline, |t: &mut DualTarget| async move {
    let mut pipeline = Vec::new();
    pipeline.extend_from_slice(&resp_cmd(&["SET", "pk1", "pv1"]));
    pipeline.extend_from_slice(&resp_cmd(&["SET", "pk2", "pv2"]));
    pipeline.extend_from_slice(&resp_cmd(&["GET", "pk1"]));
    pipeline.extend_from_slice(&resp_cmd(&["GET", "pk2"]));
    pipeline.extend_from_slice(&resp_cmd(&["DEL", "pk1", "pk2"]));

    t.kora_stream.write_all(&pipeline).await.unwrap();
    t.redis_stream.write_all(&pipeline).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut kbuf = vec![0u8; 65536];
    let mut rbuf = vec![0u8; 65536];
    let kn = t.kora_stream.read(&mut kbuf).await.unwrap();
    let rn = t.redis_stream.read(&mut rbuf).await.unwrap();
    kbuf.truncate(kn);
    rbuf.truncate(rn);

    assert_eq!(
        resp_to_string(&kbuf),
        resp_to_string(&rbuf),
        "Pipeline mismatch\n  Kōra:  {}\n  Redis: {}",
        resp_to_string(&kbuf),
        resp_to_string(&rbuf),
    );
});
