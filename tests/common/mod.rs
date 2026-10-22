//! Shared helpers for integration tests.
#![allow(dead_code)]

use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rft::hash::sha256_file;
use rft::progress::Progress;
use rft::protocol::{Digest, FrameReader, Hello, Message, PROTOCOL_VERSION, write_message};
use rft::{SendOptions, Server, ServerConfig};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// A server running in a background task on an ephemeral port.
pub struct TestServer {
    pub addr: SocketAddr,
    pub root: PathBuf,
    pub handle: JoinHandle<std::io::Result<()>>,
}

impl TestServer {
    pub fn addr_str(&self) -> String {
        self.addr.to_string()
    }

    /// Simulate a crash: abort the accept loop and every connection without
    /// a final checkpoint.
    pub async fn kill(self) {
        self.handle.abort();
        let _ = self.handle.await;
    }
}

pub fn test_config(root: &Path) -> ServerConfig {
    ServerConfig {
        io_timeout: Duration::from_secs(10),
        ..ServerConfig::new(root)
    }
}

pub async fn start_server(config: ServerConfig) -> TestServer {
    start_server_on("127.0.0.1:0", config).await
}

pub async fn start_server_on(addr: &str, config: ServerConfig) -> TestServer {
    let root = config.root.clone();
    let server = Server::bind(addr, config).await.expect("bind");
    let addr = server.local_addr().unwrap();
    let handle = tokio::spawn(server.run());
    TestServer { addr, root, handle }
}

/// Client options tuned for tests: small chunks so even small files have
/// many chunks, and short backoff so retries are fast.
pub fn test_opts() -> SendOptions {
    SendOptions {
        chunk_size: 64 * 1024,
        streams: 3,
        window: 4,
        backoff_initial: Duration::from_millis(20),
        backoff_max: Duration::from_millis(200),
        connect_timeout: Duration::from_secs(5),
        io_timeout: Duration::from_secs(10),
        ..SendOptions::default()
    }
}

/// Write `size` bytes of deterministic pseudo-random data, streaming in 1 MiB
/// blocks (never holding the file in memory). Returns its SHA-256.
pub async fn write_random_file(path: &Path, size: u64, seed: u64) -> Digest {
    let p = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut rng = fastrand::Rng::with_seed(seed);
        let mut f = std::io::BufWriter::new(std::fs::File::create(&p).unwrap());
        let mut block = vec![0u8; 1024 * 1024];
        let mut left = size;
        while left > 0 {
            let n = left.min(block.len() as u64) as usize;
            rng.fill(&mut block[..n]);
            f.write_all(&block[..n]).unwrap();
            left -= n as u64;
        }
        f.flush().unwrap();
    })
    .await
    .unwrap();
    sha256_file(path, None).await.unwrap()
}

pub async fn file_sha(path: &Path) -> Digest {
    sha256_file(path, None).await.unwrap()
}

/// Count of entries in the server's partial directory (should be 0 when idle).
pub fn partial_entries(root: &Path) -> Vec<String> {
    std::fs::read_dir(root.join(rft::names::PARTIAL_DIR))
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default()
}

/// Progress sink that counts acknowledged bytes and fires `tripped` once
/// `trip_at` bytes have been acknowledged.
pub struct TripWire {
    pub acked: AtomicU64,
    pub trip_at: u64,
    pub tripped: Notify,
}

impl TripWire {
    pub fn new(trip_at: u64) -> Arc<Self> {
        Arc::new(Self {
            acked: AtomicU64::new(0),
            trip_at,
            tripped: Notify::new(),
        })
    }
}

impl Progress for TripWire {
    fn advance(&self, _name: &str, bytes: u64) {
        let before = self.acked.fetch_add(bytes, Ordering::SeqCst);
        if before < self.trip_at && before + bytes >= self.trip_at {
            self.tripped.notify_one();
        }
    }
}

/// A hand-driven protocol client for testing the server's validation.
pub struct RawClient {
    pub stream: TcpStream,
    pub frames: FrameReader,
    scratch: Vec<u8>,
}

impl RawClient {
    pub async fn connect(addr: SocketAddr) -> Self {
        Self {
            stream: TcpStream::connect(addr).await.unwrap(),
            frames: FrameReader::default(),
            scratch: Vec::new(),
        }
    }

    pub async fn send(&mut self, msg: &Message<'_>) {
        write_message(&mut self.stream, msg, &mut self.scratch)
            .await
            .unwrap();
        self.stream.flush().await.unwrap();
    }

    /// Read the next message as an owned description (kind, offset/code).
    pub async fn recv(&mut self) -> Option<Reply> {
        let msg = tokio::time::timeout(Duration::from_secs(10), self.frames.read(&mut self.stream))
            .await
            .expect("reply timed out")
            .ok()??;
        Some(match msg {
            Message::HelloAck { completed } => Reply::HelloAck {
                have: completed.count(),
                of: completed.len(),
            },
            Message::ChunkAck { offset } => Reply::Ack(offset),
            Message::ChunkNack { offset } => Reply::Nack(offset),
            Message::FinishAck => Reply::FinishAck,
            Message::Error { code, message } => Reply::Error(code, message),
            other => panic!("unexpected {}", other.kind()),
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Reply {
    HelloAck { have: u64, of: u64 },
    Ack(u64),
    Nack(u64),
    FinishAck,
    Error(rft::protocol::ErrorCode, String),
}

pub fn hello(name: &str, size: u64, chunk_size: u32, sha256: Digest) -> Message<'static> {
    Message::Hello(Hello {
        version: PROTOCOL_VERSION,
        file_name: name.to_owned(),
        file_size: size,
        chunk_size,
        sha256,
    })
}
