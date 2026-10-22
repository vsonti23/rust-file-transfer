//! The sending side.
//!
//! ## How one file is sent
//!
//! 1. Hash the whole file (streaming, fixed buffer) to get the SHA-256 the
//!    server will verify against at the end.
//! 2. Connect and send `Hello`. The server answers with a bitmap of the chunks
//!    it already has: zero on a fresh transfer, more on a resume.
//! 3. Put the missing chunk indices in a shared work queue and open up to
//!    `streams` connections. Each stream task pulls an index, reads that chunk
//!    from its own file handle into its own reusable buffer, and sends it.
//!    Up to `window` chunks per stream may be unacknowledged at once; the
//!    window is a [`Semaphore`] whose permits are returned by acks.
//! 4. A `ChunkNack` (the server's hash did not match) puts the chunk back on
//!    the queue, up to `chunk_retries` times.
//! 5. When the queue is empty and everything is acknowledged, send `Finish`
//!    and wait for the server's whole-file verification.
//!
//! Any connection-level failure aborts the attempt; the outer loop backs off
//! and starts again from step 2, which naturally resumes because the server
//! reports what it already has.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Notify, Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tracing::{Instrument, debug, info, info_span, warn};

use crate::bitmap::ChunkBitmap;
use crate::error::{Result, TransferError};
use crate::hash::{sha256, sha256_file};
use crate::names::validate_file_name;
use crate::progress::Progress;
use crate::protocol::{
    DEFAULT_CHUNK_SIZE, Digest, FrameReader, Hello, MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, Message,
    PROTOCOL_VERSION, write_message,
};
use crate::{chunk_count, chunk_len};

/// Sender configuration.
#[derive(Debug, Clone)]
pub struct SendOptions {
    /// Bytes per chunk. Must be within the protocol's limits.
    pub chunk_size: u32,
    /// Parallel TCP connections per file.
    pub streams: usize,
    /// Files sent concurrently by [`send_files`].
    pub max_files: usize,
    /// Unacknowledged chunks allowed in flight per stream.
    pub window: usize,
    /// Deadline for establishing a TCP connection.
    pub connect_timeout: Duration,
    /// Deadline for any single read or write on an established connection.
    pub io_timeout: Duration,
    /// Whole-transfer retries after a retryable error.
    pub retries: u32,
    /// First backoff delay; doubles on each retry.
    pub backoff_initial: Duration,
    /// Upper bound on the backoff delay.
    pub backoff_max: Duration,
    /// How many times one chunk may be rejected by the server before giving up.
    pub chunk_retries: u32,
}

impl Default for SendOptions {
    fn default() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
            streams: 4,
            max_files: 4,
            window: 8,
            connect_timeout: Duration::from_secs(10),
            io_timeout: Duration::from_secs(30),
            retries: 5,
            backoff_initial: Duration::from_millis(200),
            backoff_max: Duration::from_secs(10),
            chunk_retries: 3,
        }
    }
}

/// What happened during a successful [`send_file`].
#[derive(Debug, Clone)]
pub struct SendReport {
    /// Local path that was sent.
    pub path: PathBuf,
    /// Name the file was stored under on the server.
    pub name: String,
    /// Size in bytes.
    pub file_size: u64,
    /// Whole-file SHA-256, verified by the server.
    pub sha256: Digest,
    /// Bytes the server already had when the first attempt started (resume).
    pub resumed_bytes: u64,
    /// Chunk payload bytes written to the network across all attempts,
    /// including resends.
    pub bytes_sent: u64,
    /// Number of attempts (1 means no retries were needed).
    pub attempts: u32,
    /// Wall-clock time including hashing and verification.
    pub elapsed: Duration,
}

/// Chunks still to be sent, shared by all streams of one attempt.
struct WorkQueue {
    pending: Mutex<VecDeque<Pending>>,
    /// Flipped to `true` the first time a stream finds `pending` empty.
    drained: watch::Sender<bool>,
}

/// A chunk waiting to be (re)sent.
#[derive(Debug, Clone, Copy)]
struct Pending {
    index: u64,
    nacks: u32,
}

/// Everything about one file that its stream tasks share (via `Arc`).
struct FileCtx {
    addr: String,
    path: PathBuf,
    hello: Hello,
    opts: SendOptions,
    progress: Arc<dyn Progress>,
    bytes_sent: AtomicU64,
}

/// Client side of one connection.
struct Conn {
    rd: BufReader<OwnedReadHalf>,
    wr: BufWriter<OwnedWriteHalf>,
    frames: FrameReader,
    scratch: Vec<u8>,
}

fn validate_options(opts: &SendOptions) -> Result<()> {
    if !(MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&opts.chunk_size) {
        return Err(TransferError::InvalidInput(format!(
            "chunk size must be between {MIN_CHUNK_SIZE} and {MAX_CHUNK_SIZE} bytes"
        )));
    }
    if opts.streams == 0 || opts.window == 0 || opts.max_files == 0 {
        return Err(TransferError::InvalidInput(
            "streams, window and max_files must be at least 1".into(),
        ));
    }
    Ok(())
}

/// Send one file to the server at `addr` (`host:port`), resuming if the server
/// already has part of it and retrying connection failures with backoff.
pub async fn send_file(
    addr: &str,
    path: &Path,
    opts: &SendOptions,
    progress: Arc<dyn Progress>,
) -> Result<SendReport> {
    validate_options(opts)?;
    let started = Instant::now();
    let meta = tokio::fs::metadata(path).await?;
    if !meta.is_file() {
        return Err(TransferError::InvalidInput(format!(
            "{} is not a regular file",
            path.display()
        )));
    }
    let name = path.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
        TransferError::InvalidInput(format!("{} has no UTF-8 name", path.display()))
    })?;
    let name = validate_file_name(name)
        .map_err(|e| TransferError::InvalidInput(e.to_string()))?
        .to_owned();

    let span = info_span!("send", file = %name);
    async move {
        let file_size = meta.len();
        debug!(file_size, "hashing");
        // If the file changes after this point, the server's whole-file
        // check fails and nothing is committed.
        let digest = sha256_file(path, None).await?;
        let ctx = Arc::new(FileCtx {
            addr: addr.to_owned(),
            path: path.to_owned(),
            hello: Hello {
                version: PROTOCOL_VERSION,
                file_name: name.clone(),
                file_size,
                chunk_size: opts.chunk_size,
                sha256: digest,
            },
            opts: opts.clone(),
            progress,
            bytes_sent: AtomicU64::new(0),
        });

        let mut resumed_bytes = None;
        let mut attempt = 0;
        loop {
            attempt += 1;
            match attempt_once(&ctx, &mut resumed_bytes).await {
                Ok(()) => break,
                Err(e) if e.is_retryable() && attempt <= opts.retries => {
                    let delay = backoff(opts, attempt);
                    warn!(attempt, error = %e, ?delay, "attempt failed; retrying");
                    ctx.progress.retry(&name, attempt, &e);
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(e),
            }
        }
        ctx.progress.finish(&name);
        let report = SendReport {
            path: path.to_owned(),
            name,
            file_size,
            sha256: digest,
            resumed_bytes: resumed_bytes.unwrap_or(0),
            bytes_sent: ctx.bytes_sent.load(Ordering::Relaxed),
            attempts: attempt,
            elapsed: started.elapsed(),
        };
        info!(
            bytes = report.file_size,
            resumed = report.resumed_bytes,
            sent = report.bytes_sent,
            attempts = report.attempts,
            elapsed = ?report.elapsed,
            "transfer complete"
        );
        Ok(report)
    }
    .instrument(span)
    .await
}

/// Send several files, at most `opts.max_files` at a time. Results are
/// returned in input order; one file failing does not stop the others.
pub async fn send_files(
    addr: &str,
    paths: &[PathBuf],
    opts: &SendOptions,
    progress: Arc<dyn Progress>,
) -> Vec<(PathBuf, Result<SendReport>)> {
    let limit = Arc::new(Semaphore::new(opts.max_files.max(1)));
    let mut seen = HashSet::new();
    let mut results: Vec<Option<(PathBuf, Result<SendReport>)>> = Vec::new();
    let mut set = JoinSet::new();
    for (i, path) in paths.iter().enumerate() {
        results.push(None);
        let base = path.file_name().map(|n| n.to_owned());
        if !seen.insert(base.clone()) {
            let msg = format!(
                "duplicate file name {:?} in one send",
                base.unwrap_or_default()
            );
            results[i] = Some((path.clone(), Err(TransferError::InvalidInput(msg))));
            continue;
        }
        // Each task must own its data ('static): clone the cheap handles in.
        let (addr, path, opts) = (addr.to_owned(), path.clone(), opts.clone());
        let (progress, limit) = (Arc::clone(&progress), Arc::clone(&limit));
        set.spawn(async move {
            let _permit = limit
                .acquire_owned()
                .await
                .expect("semaphore is never closed");
            let r = send_file(&addr, &path, &opts, progress).await;
            (i, path, r)
        });
    }
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((i, path, r)) => results[i] = Some((path, r)),
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Err(e) => unreachable!("send task cancelled: {e}"),
        }
    }
    results.into_iter().flatten().collect()
}

/// Exponential backoff with "equal jitter": half the delay is fixed, half
/// random, so many clients retrying at once do not stampede in lockstep.
fn backoff(opts: &SendOptions, attempt: u32) -> Duration {
    let exp = opts
        .backoff_initial
        .saturating_mul(1u32 << (attempt - 1).min(16))
        .min(opts.backoff_max);
    let half = exp / 2;
    half + half.mul_f64(fastrand::f64())
}

/// Bytes covered by the set bits of `completed`.
fn completed_bytes(completed: &ChunkBitmap, file_size: u64, chunk_size: u32) -> u64 {
    let n = completed.len();
    if n == 0 {
        return 0;
    }
    let full = completed.count() * u64::from(chunk_size);
    if completed.get(n - 1) {
        full - u64::from(chunk_size) + chunk_len(file_size, chunk_size, n - 1) as u64
    } else {
        full
    }
}

/// One attempt: handshake, send every missing chunk over parallel streams,
/// then ask the server to verify and commit.
async fn attempt_once(ctx: &Arc<FileCtx>, resumed_bytes: &mut Option<u64>) -> Result<()> {
    let h = &ctx.hello;
    let (mut primary, completed) = connect_and_hello(ctx).await?;
    let already = completed_bytes(&completed, h.file_size, h.chunk_size);
    resumed_bytes.get_or_insert(already);
    ctx.progress.start(&h.file_name, h.file_size, already);
    if already > 0 {
        info!(
            have = completed.count(),
            of = completed.len(),
            "server already has part of this file; resuming"
        );
    }

    let missing: VecDeque<Pending> = completed
        .missing()
        .map(|index| Pending { index, nacks: 0 })
        .collect();
    if !missing.is_empty() {
        let streams = ctx.opts.streams.min(missing.len());
        let queue = Arc::new(WorkQueue {
            pending: Mutex::new(missing),
            drained: watch::channel(false).0,
        });
        // Dropping this JoinSet (e.g. on the first error, via `?`) aborts
        // every other stream of this attempt.
        let mut set = JoinSet::new();
        let first = run_stream(Arc::clone(ctx), primary, Arc::clone(&queue));
        set.spawn(async move { first.await.map(Some) });
        for _ in 1..streams {
            let (ctx, queue) = (Arc::clone(ctx), Arc::clone(&queue));
            set.spawn(async move {
                // If every chunk has been handed out before this stream even
                // connects (e.g. the server is at its connection limit and we
                // are queued in its backlog), there is nothing left for it to
                // do: give up instead of holding the attempt open.
                let mut drained = queue.drained.subscribe();
                let conn = tokio::select! {
                    r = connect_and_hello(&ctx) => r?.0,
                    _ = drained.wait_for(|d| *d) => return Ok(None),
                };
                run_stream(ctx, conn, queue).await.map(Some)
            });
        }
        let mut kept = None;
        while let Some(joined) = set.join_next().await {
            let conn = match joined {
                Ok(r) => r?,
                Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                Err(e) => return Err(TransferError::Io(io::Error::other(e))),
            };
            // Keep one connection open for `Finish`; the rest close on drop.
            if kept.is_none() {
                kept = conn;
            }
        }
        primary = kept.expect("the first stream always returns its connection");
    }

    // Whole-file verification on the server reads the file once more; give
    // it time proportional to the size (assume at least ~50 MB/s).
    let verify_budget = ctx.opts.io_timeout + Duration::from_secs(h.file_size / 50_000_000);
    let Conn {
        rd,
        wr,
        frames,
        scratch,
    } = &mut primary;
    timeout(ctx.opts.io_timeout, async {
        write_message(wr, &Message::Finish, scratch).await?;
        wr.flush().await
    })
    .await
    .map_err(|_| TransferError::Timeout("sending Finish"))??;
    let reply = timeout(verify_budget, frames.read(rd))
        .await
        .map_err(|_| TransferError::Timeout("waiting for verification"))??;
    match reply {
        Some(Message::FinishAck) => Ok(()),
        Some(Message::Error { code, message }) => Err(TransferError::Rejected { code, message }),
        Some(other) => Err(TransferError::Unexpected {
            got: other.kind(),
            context: "waiting for FinishAck",
        }),
        None => Err(TransferError::ConnectionClosed("waiting for verification")),
    }
}

/// Open a connection and perform the `Hello` handshake.
async fn connect_and_hello(ctx: &FileCtx) -> Result<(Conn, ChunkBitmap)> {
    let stream = timeout(ctx.opts.connect_timeout, TcpStream::connect(&ctx.addr))
        .await
        .map_err(|_| TransferError::Timeout("connecting"))??;
    stream.set_nodelay(true)?;
    let (rd, wr) = stream.into_split();
    let mut conn = Conn {
        rd: BufReader::with_capacity(16 * 1024, rd),
        wr: BufWriter::with_capacity(64 * 1024, wr),
        frames: FrameReader::default(),
        scratch: Vec::with_capacity(128),
    };
    let io_timeout = ctx.opts.io_timeout;
    let hello = Message::Hello(ctx.hello.clone());
    timeout(io_timeout, async {
        write_message(&mut conn.wr, &hello, &mut conn.scratch).await?;
        conn.wr.flush().await
    })
    .await
    .map_err(|_| TransferError::Timeout("sending Hello"))??;
    let reply = timeout(io_timeout, conn.frames.read(&mut conn.rd))
        .await
        .map_err(|_| TransferError::Timeout("waiting for HelloAck"))??;
    let completed = match reply {
        Some(Message::HelloAck { completed }) => completed,
        Some(Message::Error { code, message }) => {
            return Err(TransferError::Rejected { code, message });
        }
        Some(other) => {
            return Err(TransferError::Unexpected {
                got: other.kind(),
                context: "waiting for HelloAck",
            });
        }
        None => return Err(TransferError::ConnectionClosed("handshaking")),
    };
    if completed.len() != chunk_count(ctx.hello.file_size, ctx.hello.chunk_size) {
        return Err(TransferError::Unexpected {
            got: "HelloAck",
            context: "checking the chunk count",
        });
    }
    Ok((conn, completed))
}

/// What the writer half of a stream should do next.
enum Step {
    Send(Pending),
    Wait,
    Done,
}

/// Drive one connection: a writer that streams chunks and a reader that
/// consumes acknowledgements, running concurrently in the same task.
/// Returns the connection when every chunk this stream sent is acknowledged
/// and the shared queue is empty.
async fn run_stream(ctx: Arc<FileCtx>, mut conn: Conn, queue: Arc<WorkQueue>) -> Result<Conn> {
    let h = &ctx.hello;
    let opts = &ctx.opts;
    let cs = h.chunk_size;
    let mut file = File::open(&ctx.path).await?;
    // The only data buffer this stream ever uses: one chunk, reused.
    let mut buf = vec![0u8; cs as usize];
    let window = Semaphore::new(opts.window);
    let inflight: Mutex<HashMap<u64, Pending>> = Mutex::new(HashMap::new());
    let wake = Notify::new();
    let Conn {
        rd,
        wr,
        frames,
        scratch,
    } = &mut conn;

    let writer = async {
        loop {
            // Backpressure: wait for a free slot in the window before reading
            // more of the file. The permit is handed back by the ack reader.
            let permit = window.acquire().await.expect("semaphore is never closed");
            let step = {
                // Lock order is always inflight -> queue (see the reader).
                let mut inflight = inflight.lock().unwrap();
                let next = queue.pending.lock().unwrap().pop_front();
                match next {
                    Some(p) => {
                        inflight.insert(p.index * u64::from(cs), p);
                        Step::Send(p)
                    }
                    None => {
                        queue.drained.send_replace(true);
                        if inflight.is_empty() {
                            Step::Done
                        } else {
                            Step::Wait
                        }
                    }
                }
            };
            match step {
                Step::Done => return Ok::<(), TransferError>(()),
                Step::Wait => {
                    drop(permit);
                    wake.notified().await;
                }
                Step::Send(p) => {
                    permit.forget();
                    let offset = p.index * u64::from(cs);
                    let len = chunk_len(h.file_size, cs, p.index);
                    let data = &mut buf[..len];
                    file.seek(SeekFrom::Start(offset)).await?;
                    file.read_exact(data).await?;
                    let msg = Message::Chunk {
                        offset,
                        sha256: sha256(data),
                        data,
                    };
                    timeout(opts.io_timeout, async {
                        write_message(wr, &msg, scratch).await?;
                        wr.flush().await
                    })
                    .await
                    .map_err(|_| TransferError::Timeout("sending a chunk"))??;
                    ctx.bytes_sent.fetch_add(len as u64, Ordering::Relaxed);
                }
            }
        }
    };

    let reader = async {
        loop {
            let msg = timeout(opts.io_timeout, frames.read(rd))
                .await
                .map_err(|_| TransferError::Timeout("waiting for chunk acknowledgements"))??;
            match msg {
                Some(Message::ChunkAck { offset }) => {
                    let p = inflight.lock().unwrap().remove(&offset);
                    let Some(p) = p else {
                        return Err::<(), _>(TransferError::Unexpected {
                            got: "ChunkAck",
                            context: "matching it to a chunk in flight",
                        });
                    };
                    let len = chunk_len(h.file_size, cs, p.index) as u64;
                    ctx.progress.advance(&h.file_name, len);
                }
                Some(Message::ChunkNack { offset }) => {
                    let mut inflight = inflight.lock().unwrap();
                    let Some(mut p) = inflight.remove(&offset) else {
                        return Err(TransferError::Unexpected {
                            got: "ChunkNack",
                            context: "matching it to a chunk in flight",
                        });
                    };
                    p.nacks += 1;
                    if p.nacks > opts.chunk_retries {
                        return Err(TransferError::ChunkRetriesExhausted {
                            offset,
                            attempts: p.nacks,
                        });
                    }
                    warn!(offset, nacks = p.nacks, "server rejected chunk; resending");
                    // Requeue while still holding `inflight`, so the writer
                    // never sees "queue empty and nothing in flight" in between.
                    queue.pending.lock().unwrap().push_back(p);
                }
                Some(Message::Error { code, message }) => {
                    return Err(TransferError::Rejected { code, message });
                }
                Some(other) => {
                    return Err(TransferError::Unexpected {
                        got: other.kind(),
                        context: "waiting for chunk acknowledgements",
                    });
                }
                None => {
                    return Err(TransferError::ConnectionClosed(
                        "waiting for chunk acknowledgements",
                    ));
                }
            }
            window.add_permits(1);
            wake.notify_one();
        }
    };

    // The writer finishing means everything it sent was acknowledged, so the
    // reader is idle at a frame boundary and is safe to cancel.
    tokio::select! {
        r = writer => r?,
        r = reader => r?,
    }
    drop(file);
    Ok(conn)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_is_capped() {
        let opts = SendOptions {
            backoff_initial: Duration::from_millis(100),
            backoff_max: Duration::from_millis(1000),
            ..SendOptions::default()
        };
        for attempt in 1..30 {
            let d = backoff(&opts, attempt);
            let exp = Duration::from_millis(100 * (1u64 << (attempt - 1).min(16)))
                .min(Duration::from_millis(1000));
            assert!(
                d >= exp / 2 && d <= exp,
                "attempt {attempt}: {d:?} vs {exp:?}"
            );
        }
    }

    #[test]
    fn completed_bytes_accounts_for_short_last_chunk() {
        let mut b = ChunkBitmap::new(3);
        assert_eq!(completed_bytes(&b, 2 * 4096 + 10, 4096), 0);
        b.set(0);
        assert_eq!(completed_bytes(&b, 2 * 4096 + 10, 4096), 4096);
        b.set(2);
        assert_eq!(completed_bytes(&b, 2 * 4096 + 10, 4096), 4096 + 10);
        b.set(1);
        assert_eq!(completed_bytes(&b, 2 * 4096 + 10, 4096), 2 * 4096 + 10);
    }

    #[test]
    fn rejects_bad_options() {
        let mut o = SendOptions {
            chunk_size: 100,
            ..SendOptions::default()
        };
        assert!(validate_options(&o).is_err());
        o.chunk_size = DEFAULT_CHUNK_SIZE;
        o.streams = 0;
        assert!(validate_options(&o).is_err());
    }
}
