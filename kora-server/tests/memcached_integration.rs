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

async fn start_server(resp_port: u16, mem_port: u16) -> tokio::sync::watch::Sender<bool> {
    let (tx, rx) = tokio::sync::watch::channel(false);
    let config = ServerConfig {
        bind_address: format!("127.0.0.1:{}", resp_port),
        memcached_bind_address: Some(format!("127.0.0.1:{}", mem_port)),
        worker_count: 2,
        ..Default::default()
    };
    let server = KoraServer::new(config);
    tokio::spawn(async move {
        let _ = server.run(rx).await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    tx
}

async fn connect(port: u16) -> TcpStream {
    for _ in 0..20 {
        match TcpStream::connect(format!("127.0.0.1:{}", port)).await {
            Ok(stream) => return stream,
            Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
        }
    }
    panic!("could not connect to memcached port {}", port);
}

async fn send_and_read(stream: &mut TcpStream, command: &[u8]) -> Vec<u8> {
    stream.write_all(command).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let mut buf = vec![0u8; 8192];
    let n = stream.read(&mut buf).await.unwrap();
    buf.truncate(n);
    buf
}

fn assert_bytes(actual: &[u8], expected: &[u8]) {
    assert_eq!(
        actual,
        expected,
        "\nExpected: {:?}\n     Got: {:?}",
        String::from_utf8_lossy(expected),
        String::from_utf8_lossy(actual),
    );
}

#[tokio::test]
async fn test_memcached_set_get_and_version() {
    let resp_port = free_port().await;
    let mem_port = free_port().await;
    let shutdown = start_server(resp_port, mem_port).await;
    let mut stream = connect(mem_port).await;

    let resp = send_and_read(&mut stream, b"set alpha 7 0 5\r\nvalue\r\n").await;
    assert_bytes(&resp, b"STORED\r\n");

    let resp = send_and_read(&mut stream, b"get alpha\r\n").await;
    assert_bytes(&resp, b"VALUE alpha 7 5\r\nvalue\r\nEND\r\n");

    let resp = send_and_read(&mut stream, b"version\r\n").await;
    assert!(resp.starts_with(b"VERSION "));

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_memcached_add_replace_and_multiget() {
    let resp_port = free_port().await;
    let mem_port = free_port().await;
    let shutdown = start_server(resp_port, mem_port).await;
    let mut stream = connect(mem_port).await;

    let resp = send_and_read(&mut stream, b"add first 1 0 1\r\na\r\n").await;
    assert_bytes(&resp, b"STORED\r\n");

    let resp = send_and_read(&mut stream, b"add first 9 0 1\r\nb\r\n").await;
    assert_bytes(&resp, b"NOT_STORED\r\n");

    let resp = send_and_read(&mut stream, b"replace missing 3 0 1\r\nx\r\n").await;
    assert_bytes(&resp, b"NOT_STORED\r\n");

    let resp = send_and_read(&mut stream, b"replace first 2 0 1\r\nb\r\n").await;
    assert_bytes(&resp, b"STORED\r\n");

    let _ = send_and_read(&mut stream, b"set second 4 0 1\r\nz\r\n").await;

    let resp = send_and_read(&mut stream, b"get second missing first\r\n").await;
    assert_bytes(
        &resp,
        b"VALUE second 4 1\r\nz\r\nVALUE first 2 1\r\nb\r\nEND\r\n",
    );

    let _ = shutdown.send(true);
}

#[tokio::test]
async fn test_memcached_counters_touch_delete_flush_and_stats() {
    let resp_port = free_port().await;
    let mem_port = free_port().await;
    let shutdown = start_server(resp_port, mem_port).await;
    let mut stream = connect(mem_port).await;

    let _ = send_and_read(&mut stream, b"set counter 5 0 2\r\n10\r\n").await;

    let resp = send_and_read(&mut stream, b"incr counter 5\r\n").await;
    assert_bytes(&resp, b"15\r\n");

    let resp = send_and_read(&mut stream, b"decr counter 20\r\n").await;
    assert_bytes(&resp, b"0\r\n");

    let resp = send_and_read(&mut stream, b"touch counter 1\r\n").await;
    assert_bytes(&resp, b"TOUCHED\r\n");

    tokio::time::sleep(Duration::from_millis(1200)).await;
    let resp = send_and_read(&mut stream, b"get counter\r\n").await;
    assert_bytes(&resp, b"END\r\n");

    let _ = send_and_read(&mut stream, b"set doomed 1 0 3\r\nbye\r\n").await;
    let resp = send_and_read(&mut stream, b"delete doomed\r\n").await;
    assert_bytes(&resp, b"DELETED\r\n");

    let _ = send_and_read(&mut stream, b"set flushme 0 0 1\r\nx\r\n").await;
    let resp = send_and_read(&mut stream, b"flush_all\r\n").await;
    assert_bytes(&resp, b"OK\r\n");

    let resp = send_and_read(&mut stream, b"get flushme\r\n").await;
    assert_bytes(&resp, b"END\r\n");

    let resp = send_and_read(&mut stream, b"stats\r\n").await;
    let text = String::from_utf8_lossy(&resp);
    assert!(text.contains("STAT cmd_get "));
    assert!(text.contains("STAT cmd_set "));
    assert!(text.ends_with("END\r\n"));

    let _ = shutdown.send(true);
}
