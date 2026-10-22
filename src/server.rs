//! The receiving side: accepts connections and assembles files on disk.
//!
//! ## Lifecycle of one file on the receiver
//!
//! 1. A `Hello` names the file. The name is validated (no traversal), the
//!    size and chunk size are checked against limits.
//! 2. The server looks for an in-memory transfer with the same identity
//!    (name, size, chunk size, SHA-256). If none, it loads the sidecar state
//!    file from disk, so progress survives a server restart. Otherwise it
//!    creates a fresh, pre-sized `.part` file.
//! 3. It replies `HelloAck` with the bitmap of chunks it already has.
//! 4. Each `Chunk` is hashed and compared with the SHA-256 the client sent.
//!    Good chunks are written at their offset and acknowledged; bad chunks
//!    get a `ChunkNack` and are resent by the client.
//! 5. Every `checkpoint_every` chunks, and whenever the last connection for a
//!    transfer goes away, the `.part` file is fsynced and *then* the bitmap is
//!    saved. That ordering is what makes the bitmap trustworthy.
//! 6. On `Finish`, the whole `.part` file is re-hashed. Only if it matches is
//!    it renamed into place and the directory fsynced.
//!
//! Many connections may work on the same file at once (parallel streams);
//! they share one [`Transfer`] through an `Arc`.

use std::collections::HashMap;
use std::future::Future;
use std::io::{self, SeekFrom};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tracing::{Instrument, debug, error, info, info_span, warn};

use crate::bitmap::ChunkBitmap;
use crate::error::TransferError;
use crate::hash::{sha256, sha256_file, to_hex};
use crate::names::{PARTIAL_DIR, validate_file_name};
use crate::protocol::{
    Digest, ErrorCode, FrameReader, Hello, MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, Message,
    PROTOCOL_VERSION, write_message,
};
use crate::state::{TransferState, sync_dir};
use crate::{chunk_count, chunk_len};

/// Receiver configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Directory that completed files are placed in.
    pub root: PathBuf,
    /// Largest file the server will accept, in bytes.
    pub max_file_size: u64,
    /// Maximum simultaneous connections. Further connections wait in the
    /// kernel's accept backlog until a slot frees up (backpressure).
    pub max_connections: usize,
    /// How long a connection may sit idle (no complete frame read or written)
    /// before it is dropped.
    pub io_timeout: Duration,
    /// Persist resume state after this many newly completed chunks.
    pub checkpoint_every: u32,
}

impl ServerConfig {
    /// Defaults: 1 TiB max file, 64 connections, 30 s idle timeout,
    /// checkpoint every 64 chunks.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            max_file_size: 1 << 40,
            max_connections: 64,
            io_timeout: Duration::from_secs(30),
            checkpoint_every: 64,
        }
    }
}

/// A bound, not-yet-running receiver.
pub struct Server {
    listener: TcpListener,
    shared: Arc<Shared>,
}

/// State shared by every connection task.
struct Shared {
    config: ServerConfig,
    partial_dir: PathBuf,
    /// In-progress transfers by file name. A `tokio::sync::Mutex` because it is
    /// held across `.await` while a transfer's state is loaded from disk.
    transfers: Mutex<HashMap<String, Arc<Transfer>>>,
}

/// One file being received, shared by all connections working on it.
struct Transfer {
    name: String,
    final_path: PathBuf,
    part_path: PathBuf,
    state_path: PathBuf,
    file_size: u64,
    chunk_size: u32,
    sha256: Digest,
    /// Persist state after this many newly completed chunks.
    checkpoint_every: u32,
    /// Connections currently attached. Only modified while holding the
    /// registry lock, so attach/detach/remove decisions are consistent.
    active: AtomicUsize,
    /// Set once the file was committed or discarded; no further checkpoints.
    finished: AtomicBool,
    inner: Mutex<TransferInner>,
}

struct TransferInner {
    completed: ChunkBitmap,
    /// Chunks completed since the last checkpoint.
    dirty: u32,
    /// A handle used only for fsync. `fsync` flushes the whole file no matter
    /// which descriptor wrote the data.
    sync_handle: File,
}

/// A refusal to send back to the client.
type Rejection = (ErrorCode, String);

fn internal(e: impl std::fmt::Display) -> Rejection {
    (ErrorCode::Internal, e.to_string())
}

impl Server {
    /// Create the destination directories and bind the listening socket.
    /// Bind to port 0 to get an ephemeral port (see [`Server::local_addr`]).
    pub async fn bind(addr: impl ToSocketAddrs, config: ServerConfig) -> io::Result<Self> {
        tokio::fs::create_dir_all(&config.root).await?;
        let partial_dir = config.root.join(PARTIAL_DIR);
        tokio::fs::create_dir_all(&partial_dir).await?;
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            shared: Arc::new(Shared {
                config,
                partial_dir,
                transfers: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// The address the server is listening on.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Serve forever.
    pub async fn run(self) -> io::Result<()> {
        self.run_until(std::future::pending()).await
    }

    /// Serve until `shutdown` completes, then abort open connections and
    /// checkpoint every in-progress transfer so it can be resumed.
    ///
    /// If this future is instead dropped or aborted, connections are aborted
    /// *without* a final checkpoint: that is how tests simulate a crash.
    pub async fn run_until<F>(self, shutdown: F) -> io::Result<()>
    where
        F: Future<Output = ()>,
    {
        let limit = Arc::new(Semaphore::new(self.shared.config.max_connections));
        // A JoinSet owns every connection task; dropping it aborts them all.
        let mut conns = JoinSet::new();
        tokio::pin!(shutdown);
        info!(
            addr = %self.listener.local_addr()?,
            root = %self.shared.config.root.display(),
            "listening"
        );
        loop {
            while let Some(res) = conns.try_join_next() {
                if let Err(e) = res {
                    if e.is_panic() {
                        error!(error = %e, "connection task panicked");
                    }
                }
            }
            // Take a connection slot *before* accepting, so at the limit we
            // stop accepting and let the kernel backlog absorb new clients.
            let permit = tokio::select! {
                _ = &mut shutdown => break,
                p = limit.clone().acquire_owned() => p.expect("semaphore is never closed"),
            };
            let (stream, peer) = tokio::select! {
                _ = &mut shutdown => break,
                r = self.listener.accept() => match r {
                    Ok(x) => x,
                    Err(e) => {
                        // Typically EMFILE; back off instead of spinning.
                        warn!(error = %e, "accept failed");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                },
            };
            let shared = Arc::clone(&self.shared);
            let span = info_span!("conn", %peer);
            conns.spawn(
                async move {
                    let _permit = permit; // released when the connection ends
                    handle_connection(stream, shared).await;
                }
                .instrument(span),
            );
        }
        info!("shutting down");
        conns.shutdown().await;
        self.shared.checkpoint_all().await;
        Ok(())
    }
}

/// Per-connection I/O state.
struct Conn {
    rd: BufReader<OwnedReadHalf>,
    wr: BufWriter<OwnedWriteHalf>,
    frames: FrameReader,
    scratch: Vec<u8>,
    io_timeout: Duration,
}

/// Write one message and flush it, with a deadline.
async fn send(
    wr: &mut BufWriter<OwnedWriteHalf>,
    scratch: &mut Vec<u8>,
    io_timeout: Duration,
    msg: &Message<'_>,
) -> Result<(), TransferError> {
    let fut = async {
        write_message(wr, msg, scratch).await?;
        wr.flush().await
    };
    timeout(io_timeout, fut)
        .await
        .map_err(|_| TransferError::Timeout("sending a reply"))??;
    Ok(())
}

async fn reject(conn: &mut Conn, (code, message): Rejection) {
    warn!(%code, %message, "rejecting");
    let msg = Message::Error { code, message };
    let _ = send(&mut conn.wr, &mut conn.scratch, conn.io_timeout, &msg).await;
}

async fn handle_connection(stream: TcpStream, shared: Arc<Shared>) {
    let _ = stream.set_nodelay(true);
    let (rd, wr) = stream.into_split();
    let mut conn = Conn {
        rd: BufReader::with_capacity(256 * 1024, rd),
        wr: BufWriter::new(wr),
        frames: FrameReader::default(),
        scratch: Vec::with_capacity(64),
        io_timeout: shared.config.io_timeout,
    };

    // 1. Handshake.
    let hello = match timeout(conn.io_timeout, conn.frames.read(&mut conn.rd)).await {
        Err(_) => return debug!("handshake timed out"),
        Ok(Err(e)) => return debug!(error = %e, "handshake failed"),
        Ok(Ok(None)) => return debug!("closed before handshake"),
        Ok(Ok(Some(Message::Hello(h)))) => h,
        Ok(Ok(Some(other))) => {
            let msg = format!("expected Hello, got {}", other.kind());
            return reject(&mut conn, (ErrorCode::ProtocolViolation, msg)).await;
        }
    };
    let (transfer, completed) = match shared.open_transfer(&hello).await {
        Ok(x) => x,
        Err(rej) => return reject(&mut conn, rej).await,
    };
    let span = info_span!("file", name = %transfer.name);
    async {
        debug!(
            size = transfer.file_size,
            have = completed.count(),
            of = completed.len(),
            "attached"
        );
        let ack = Message::HelloAck { completed };
        let result = match send(&mut conn.wr, &mut conn.scratch, conn.io_timeout, &ack).await {
            Ok(()) => serve_transfer(&mut conn, &transfer).await,
            Err(e) => Err(e),
        };
        match result {
            Ok(()) => debug!("connection finished"),
            Err(e) => warn!(error = %e, "connection ended with error"),
        }
        shared.release(&transfer).await;
    }
    .instrument(span)
    .await
}

/// The main receive loop for one connection.
async fn serve_transfer(conn: &mut Conn, t: &Transfer) -> Result<(), TransferError> {
    // Each connection gets its own write handle so streams never contend on
    // a shared file cursor: seek + write here cannot race another stream.
    let mut file = OpenOptions::new().write(true).open(&t.part_path).await?;
    loop {
        let next = timeout(conn.io_timeout, conn.frames.read(&mut conn.rd)).await;
        let msg = match next {
            Err(_) => return Err(TransferError::Timeout("waiting for the next message")),
            Ok(r) => r?,
        };
        match msg {
            None => return Ok(()), // the client closed the connection
            Some(Message::Chunk {
                offset,
                sha256: expected,
                data,
            }) => {
                let Some(index) = t.chunk_index(offset, data.len()) else {
                    let msg = format!("bad chunk offset {offset} / length {}", data.len());
                    let err = Message::Error {
                        code: ErrorCode::ProtocolViolation,
                        message: msg,
                    };
                    let _ = send(&mut conn.wr, &mut conn.scratch, conn.io_timeout, &err).await;
                    return Err(TransferError::Unexpected {
                        got: "Chunk",
                        context: "validating chunk bounds",
                    });
                };
                if sha256(data) != expected {
                    warn!(
                        offset,
                        "chunk failed SHA-256 verification; requesting resend"
                    );
                    let nack = Message::ChunkNack { offset };
                    send(&mut conn.wr, &mut conn.scratch, conn.io_timeout, &nack).await?;
                    continue;
                }
                if !t.is_complete(index).await {
                    file.seek(SeekFrom::Start(offset)).await?;
                    file.write_all(data).await?;
                    // tokio's File buffers internally; flush hands the bytes
                    // to the OS before we mark the chunk complete.
                    file.flush().await?;
                    t.mark_complete(index).await?;
                }
                let ack = Message::ChunkAck { offset };
                send(&mut conn.wr, &mut conn.scratch, conn.io_timeout, &ack).await?;
            }
            Some(Message::Finish) => {
                let reply = match t.finalize().await {
                    Ok(()) => Message::FinishAck,
                    Err((code, message)) => {
                        warn!(%code, %message, "finalize failed");
                        Message::Error { code, message }
                    }
                };
                send(&mut conn.wr, &mut conn.scratch, conn.io_timeout, &reply).await?;
                return Ok(());
            }
            Some(other) => {
                let got = other.kind();
                let err = Message::Error {
                    code: ErrorCode::ProtocolViolation,
                    message: format!("unexpected {got}"),
                };
                let _ = send(&mut conn.wr, &mut conn.scratch, conn.io_timeout, &err).await;
                return Err(TransferError::Unexpected {
                    got,
                    context: "receiving chunks",
                });
            }
        }
    }
}

impl Shared {
    /// Find or create the transfer described by `h` and attach to it.
    async fn open_transfer(&self, h: &Hello) -> Result<(Arc<Transfer>, ChunkBitmap), Rejection> {
        if h.version != PROTOCOL_VERSION {
            return Err((
                ErrorCode::UnsupportedVersion,
                format!(
                    "protocol version {} (server speaks {PROTOCOL_VERSION})",
                    h.version
                ),
            ));
        }
        let name = validate_file_name(&h.file_name)
            .map_err(|e| (ErrorCode::InvalidName, format!("{:?}: {e}", h.file_name)))?;
        if h.file_size > self.config.max_file_size {
            return Err((
                ErrorCode::FileTooLarge,
                format!(
                    "{} bytes exceeds the {}-byte limit",
                    h.file_size, self.config.max_file_size
                ),
            ));
        }
        if !(MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&h.chunk_size) {
            return Err((
                ErrorCode::InvalidChunkSize,
                format!("chunk size must be in [{MIN_CHUNK_SIZE}, {MAX_CHUNK_SIZE}]"),
            ));
        }

        let mut map = self.transfers.lock().await;
        if let Some(t) = map.get(name) {
            if !t.finished.load(Ordering::Acquire) {
                if t.file_size != h.file_size
                    || t.chunk_size != h.chunk_size
                    || t.sha256 != h.sha256
                {
                    return Err((
                        ErrorCode::Conflict,
                        "a different file with this name is being received".into(),
                    ));
                }
                t.active.fetch_add(1, Ordering::AcqRel);
                let snapshot = t.inner.lock().await.completed.clone();
                return Ok((Arc::clone(t), snapshot));
            }
            // Finished (committed or discarded): start over below.
        }
        let final_path = self.config.root.join(name);
        if tokio::fs::try_exists(&final_path).await.map_err(internal)? {
            return Err((ErrorCode::AlreadyExists, format!("{name} already exists")));
        }
        let t = Arc::new(
            self.load_or_create(name, h, final_path)
                .await
                .map_err(internal)?,
        );
        t.active.store(1, Ordering::Release);
        let snapshot = t.inner.lock().await.completed.clone();
        map.insert(name.to_owned(), Arc::clone(&t));
        Ok((t, snapshot))
    }

    /// Resume from the sidecar state if it describes this exact file and the
    /// `.part` file is intact; otherwise start fresh.
    async fn load_or_create(
        &self,
        name: &str,
        h: &Hello,
        final_path: PathBuf,
    ) -> io::Result<Transfer> {
        let part_path = self.partial_dir.join(format!("{name}.part"));
        let state_path = self.partial_dir.join(format!("{name}.state"));
        let resumed = match TransferState::load(&state_path).await {
            Ok(Some(s))
                if s.file_size == h.file_size
                    && s.chunk_size == h.chunk_size
                    && s.sha256 == h.sha256 =>
            {
                match tokio::fs::metadata(&part_path).await {
                    Ok(m) if m.len() == h.file_size => Some(s.completed),
                    _ => {
                        warn!("state file present but .part missing or wrong size; restarting");
                        None
                    }
                }
            }
            Ok(Some(_)) => {
                info!("state file describes a different file; restarting");
                None
            }
            Ok(None) => None,
            Err(e) => {
                warn!(error = %e, "state file unreadable; restarting");
                None
            }
        };

        let sync_handle = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&part_path)
            .await?;
        let completed = match resumed {
            Some(c) => {
                info!(have = c.count(), of = c.len(), "resuming from saved state");
                c
            }
            None => {
                // Pre-size the file (sparse on most filesystems) so chunks can
                // be written at any offset in any order.
                sync_handle.set_len(0).await?;
                sync_handle.set_len(h.file_size).await?;
                match tokio::fs::remove_file(&state_path).await {
                    Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
                    _ => {}
                }
                ChunkBitmap::new(chunk_count(h.file_size, h.chunk_size))
            }
        };
        Ok(Transfer {
            name: name.to_owned(),
            final_path,
            part_path,
            state_path,
            file_size: h.file_size,
            chunk_size: h.chunk_size,
            sha256: h.sha256,
            checkpoint_every: self.config.checkpoint_every.max(1),
            active: AtomicUsize::new(0),
            finished: AtomicBool::new(false),
            inner: Mutex::new(TransferInner {
                completed,
                dirty: 0,
                sync_handle,
            }),
        })
    }

    /// Detach a connection. The last one out checkpoints and unregisters.
    async fn release(&self, t: &Arc<Transfer>) {
        let mut map = self.transfers.lock().await;
        if t.active.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        if !t.finished.load(Ordering::Acquire) {
            let mut inner = t.inner.lock().await;
            if let Err(e) = t.checkpoint(&mut inner).await {
                warn!(error = %e, "checkpoint on disconnect failed");
            }
        }
        if map.get(&t.name).is_some_and(|x| Arc::ptr_eq(x, t)) {
            map.remove(&t.name);
        }
    }

    async fn checkpoint_all(&self) {
        let map = self.transfers.lock().await;
        for t in map.values() {
            let mut inner = t.inner.lock().await;
            if let Err(e) = t.checkpoint(&mut inner).await {
                warn!(file = %t.name, error = %e, "checkpoint on shutdown failed");
            }
        }
    }
}

impl Transfer {
    /// Map a chunk's offset and length to its index, or `None` if they do not
    /// describe exactly one chunk of this file.
    fn chunk_index(&self, offset: u64, len: usize) -> Option<u64> {
        let cs = u64::from(self.chunk_size);
        if offset % cs != 0 {
            return None;
        }
        let index = offset / cs;
        if index >= chunk_count(self.file_size, self.chunk_size) {
            return None;
        }
        (chunk_len(self.file_size, self.chunk_size, index) == len).then_some(index)
    }

    async fn is_complete(&self, index: u64) -> bool {
        self.inner.lock().await.completed.get(index)
    }

    async fn mark_complete(&self, index: u64) -> io::Result<()> {
        let mut inner = self.inner.lock().await;
        if inner.completed.set(index) {
            inner.dirty += 1;
        }
        if inner.dirty >= self.checkpoint_every {
            self.checkpoint(&mut inner).await?;
        }
        Ok(())
    }

    /// Make every completed chunk durable, then record them in the state file.
    ///
    /// Order matters: fsync the data first, then write the bitmap. If we
    /// crash between the two, the old bitmap undercounts (some chunks get
    /// resent), which is safe. The reverse order could claim chunks that
    /// never reached the disk.
    async fn checkpoint(&self, inner: &mut TransferInner) -> io::Result<()> {
        if inner.dirty == 0 || self.finished.load(Ordering::Acquire) {
            return Ok(());
        }
        inner.sync_handle.sync_data().await?;
        let state = TransferState {
            file_size: self.file_size,
            chunk_size: self.chunk_size,
            sha256: self.sha256,
            completed: inner.completed.clone(),
        };
        state.save(&self.state_path).await?;
        inner.dirty = 0;
        debug!(have = inner.completed.count(), "checkpointed");
        Ok(())
    }

    /// Verify the whole file and atomically move it into place.
    async fn finalize(&self) -> Result<(), Rejection> {
        let inner = self.inner.lock().await;
        if self.finished.load(Ordering::Acquire) {
            return Err((ErrorCode::Conflict, "transfer already finalized".into()));
        }
        if !inner.completed.is_complete() {
            let (have, of) = (inner.completed.count(), inner.completed.len());
            return Err((ErrorCode::Incomplete, format!("have {have} of {of} chunks")));
        }
        let verify_started = std::time::Instant::now();
        inner.sync_handle.sync_all().await.map_err(internal)?;
        let actual = sha256_file(&self.part_path, Some(self.file_size))
            .await
            .map_err(internal)?;
        if actual != self.sha256 {
            // The data on disk is wrong even though every chunk verified, so
            // the bitmap cannot be trusted either. Throw both away.
            self.finished.store(true, Ordering::Release);
            let _ = tokio::fs::remove_file(&self.part_path).await;
            let _ = tokio::fs::remove_file(&self.state_path).await;
            return Err((
                ErrorCode::ChecksumMismatch,
                format!(
                    "whole-file SHA-256 mismatch: expected {}, got {}",
                    to_hex(&self.sha256),
                    to_hex(&actual)
                ),
            ));
        }
        if tokio::fs::try_exists(&self.final_path)
            .await
            .map_err(internal)?
        {
            return Err((
                ErrorCode::AlreadyExists,
                format!("{} already exists", self.name),
            ));
        }
        tokio::fs::rename(&self.part_path, &self.final_path)
            .await
            .map_err(internal)?;
        self.finished.store(true, Ordering::Release);
        sync_dir(parent(&self.final_path)).await.map_err(internal)?;
        match tokio::fs::remove_file(&self.state_path).await {
            Err(e) if e.kind() != io::ErrorKind::NotFound => {
                warn!(error = %e, "could not remove state file");
            }
            _ => {}
        }
        info!(
            bytes = self.file_size,
            sha256 = %to_hex(&self.sha256),
            verify = ?verify_started.elapsed(),
            "verified and committed"
        );
        Ok(())
    }
}

fn parent(p: &Path) -> &Path {
    p.parent().unwrap_or(Path::new("."))
}
