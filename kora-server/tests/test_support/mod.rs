use std::time::Duration;

use kora_server::{KoraServer, ServerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const CONNECT_RETRIES: usize = 40;
const CONNECT_RETRY_DELAY_MS: u64 = 25;
const FIRST_READ_TIMEOUT_MS: u64 = 1500;
const QUIET_READ_TIMEOUT_MS: u64 = 5;
const DEFAULT_MAX_READ_CHUNKS: usize = 32;
const DEFAULT_READ_BUF_SIZE: usize = 8192;
#[allow(dead_code)]
const LARGE_MAX_READ_CHUNKS: usize = 256;
#[allow(dead_code)]
const LARGE_READ_BUF_SIZE: usize = 65536;

pub(crate) async fn free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

pub(crate) fn bulk(s: &str) -> Vec<u8> {
    format!("${}\r\n{}\r\n", s.len(), s).into_bytes()
}

pub(crate) fn resp_cmd(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        out.extend_from_slice(&bulk(arg));
    }
    out
}

pub(crate) async fn start_server(port: u16) -> tokio::sync::watch::Sender<bool> {
    start_server_with_workers(port, 2).await
}

pub(crate) async fn start_server_with_workers(
    port: u16,
    worker_count: usize,
) -> tokio::sync::watch::Sender<bool> {
    start_server_with_config(ServerConfig {
        bind_address: format!("127.0.0.1:{}", port),
        worker_count,
        ..Default::default()
    })
    .await
}

pub(crate) async fn start_server_with_config(
    config: ServerConfig,
) -> tokio::sync::watch::Sender<bool> {
    let (tx, rx) = tokio::sync::watch::channel(false);
    let server = KoraServer::new(config);
    tokio::spawn(async move {
        let _ = server.run(rx).await;
    });
    tx
}

pub(crate) async fn connect(port: u16) -> TcpStream {
    for _ in 0..CONNECT_RETRIES {
        match TcpStream::connect(format!("127.0.0.1:{}", port)).await {
            Ok(stream) => return stream,
            Err(_) => tokio::time::sleep(Duration::from_millis(CONNECT_RETRY_DELAY_MS)).await,
        }
    }
    panic!("could not connect to server on port {}", port);
}

async fn read_with_retries(
    stream: &mut TcpStream,
    max_chunks: usize,
    read_buf_size: usize,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(read_buf_size);
    let mut buf = vec![0u8; read_buf_size];
    let mut first = true;

    for _ in 0..max_chunks {
        let wait_for = if first {
            Duration::from_millis(FIRST_READ_TIMEOUT_MS)
        } else {
            Duration::from_millis(QUIET_READ_TIMEOUT_MS)
        };
        first = false;

        match tokio::time::timeout(wait_for, stream.read(&mut buf)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => out.extend_from_slice(&buf[..n]),
            Ok(Err(err)) => panic!("failed reading response: {err}"),
            Err(_) if out.is_empty() => panic!(
                "timed out waiting for first response chunk ({}ms)",
                FIRST_READ_TIMEOUT_MS
            ),
            Err(_) => break,
        }
    }

    out
}

pub(crate) async fn send_and_read(stream: &mut TcpStream, cmd_bytes: &[u8]) -> Vec<u8> {
    stream.write_all(cmd_bytes).await.unwrap();
    read_with_retries(stream, DEFAULT_MAX_READ_CHUNKS, DEFAULT_READ_BUF_SIZE).await
}

#[allow(dead_code)]
pub(crate) async fn send_and_read_large(stream: &mut TcpStream, cmd_bytes: &[u8]) -> Vec<u8> {
    stream.write_all(cmd_bytes).await.unwrap();
    read_with_retries(stream, LARGE_MAX_READ_CHUNKS, LARGE_READ_BUF_SIZE).await
}

pub(crate) async fn cmd(stream: &mut TcpStream, args: &[&str]) -> Vec<u8> {
    send_and_read(stream, &resp_cmd(args)).await
}

pub(crate) fn assert_resp(resp: &[u8], expected: &[u8]) {
    assert_eq!(
        resp,
        expected,
        "\nExpected: {:?}\n     Got: {:?}",
        String::from_utf8_lossy(expected),
        String::from_utf8_lossy(resp),
    );
}

pub(crate) fn assert_resp_prefix(resp: &[u8], prefix: &[u8]) {
    assert!(
        resp.starts_with(prefix),
        "\nExpected prefix: {:?}\n           Got: {:?}",
        String::from_utf8_lossy(prefix),
        String::from_utf8_lossy(resp),
    );
}
