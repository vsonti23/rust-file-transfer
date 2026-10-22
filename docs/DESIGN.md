# Design notes

This document explains why `rft` is built the way it is. It is written for
someone who knows distributed systems but is new to Rust, and it doubles as
interview preparation: every section ends up answering a "why did you..."
question. Code references are to files under `src/`.

Contents:

1. [The problem](#1-the-problem)
2. [Async I/O vs. threads, and the Tokio runtime](#2-async-io-vs-threads-and-the-tokio-runtime)
3. [Why chunking, and choosing a chunk size](#3-why-chunking-and-choosing-a-chunk-size)
4. [The wire protocol](#4-the-wire-protocol)
5. [Backpressure and bounded concurrency](#5-backpressure-and-bounded-concurrency)
6. [Parallel streams for one file](#6-parallel-streams-for-one-file)
7. [SHA-256 vs. CRC32](#7-sha-256-vs-crc32)
8. [Durability: write, flush, fsync, and atomic rename](#8-durability-write-flush-fsync-and-atomic-rename)
9. [Resume state design](#9-resume-state-design)
10. [Retries, backoff, and timeouts](#10-retries-backoff-and-timeouts)
11. [Treating the network as hostile](#11-treating-the-network-as-hostile)
12. [Failure modes, one by one](#12-failure-modes-one-by-one)
13. [What this deliberately does not do](#13-what-this-deliberately-does-not-do)
14. [Rust concepts used here](#14-rust-concepts-used-here)
15. [Interview questions and model answers](#15-interview-questions-and-model-answers)

---

## 1. The problem

Move a file of any size from machine A to machine B so that:

- memory use does not grow with file size,
- the link is used well (high throughput on fast networks, sensible behavior
  on slow ones),
- the receiver ends up with exactly the sender's bytes or with nothing under
  that name (never a silently corrupt or half-written file), and
- an interruption costs only the work in flight, not the whole transfer.

Each requirement maps onto one mechanism: streaming chunks, bounded parallel
streams, SHA-256 plus atomic rename, and a resume bitmap. The rest of this
document goes through them.

## 2. Async I/O vs. threads, and the Tokio runtime

A file transfer spends most of its time *waiting*: for the socket to accept
more bytes, for the peer's ack, for the disk. There are two classic ways to
wait on many things at once.

**One OS thread per connection.** It is simple and works fine for tens of
connections. Each thread costs a stack (8 MiB of virtual address space by
default, tens of KiB resident) and a kernel scheduling entity. Waiting means
blocking in a syscall, and the kernel switches threads. At hundreds or
thousands of connections, the memory and context-switch overhead add up, and
cross-thread coordination needs locks around everything.

**Async tasks on a small thread pool.** Each connection is a *task*: a state
machine that the compiler generates from an `async fn`. When a task would
block (the socket is not readable yet), it returns `Pending` and the runtime
runs another task on the same thread. When epoll reports that the socket is
ready, the task is woken and continues where it left off. A task costs a few
hundred bytes to a few KiB, and switching between tasks is a function call.

`rft` uses Tokio's **multi-threaded runtime** (`#[tokio::main]` in
`main.rs`). It runs one worker thread per core, with work stealing between
them, so tasks spread across cores and an idle core takes work from a busy
one. Two kinds of work cannot simply be awaited:

- **CPU-heavy work.** Hashing a 1 GiB file takes about 0.8 s, and doing that
  on a worker thread would stall every task queued behind it.
  `hash::sha256_file` therefore runs on Tokio's **blocking pool** via
  `spawn_blocking`, a separate set of threads meant for this. Per-chunk
  hashing (about 1 ms per 1 MiB with SHA-NI) stays on the worker, because it
  is short and the thread hop would cost more than it saves.
- **File I/O.** Linux has no readiness model for regular files, since they
  are always "ready". `tokio::fs` runs each file operation on the blocking
  pool behind the scenes. This is why the server calls `file.flush()` after
  `write_all`: the write is handed to a background thread, and `flush` waits
  for it to finish before the chunk is marked complete (`server.rs`,
  `serve_transfer`).

Why not `io_uring`? It would make file I/O truly asynchronous and cut thread
hops. But it is Linux-only, its Rust runtimes (`tokio-uring`, `glommio`) use
a different, thread-per-core model, and the measured bottleneck here is
hashing, not the thread hop.

## 3. Why chunking, and choosing a chunk size

**Why not just `read` the file and `write_all` it to the socket?** Streaming
a single unbroken byte stream would also use constant memory, but you lose
everything else:

- **Resume.** With chunks, progress is "chunks 0 to 811 are done". With one
  stream, progress is "some prefix arrived". That is workable for a single
  connection, but it cannot express out-of-order progress from parallel
  streams.
- **Localized verification.** A corrupt chunk is detected and resent alone.
  With only a whole-file hash, you find out at the end and resend everything.
- **Parallelism.** Independent units with explicit offsets can be spread
  over several connections and written in any order.

**Memory.** Each sender stream owns exactly one chunk buffer
(`client.rs`, `run_stream`: `let mut buf = vec![0u8; cs as usize]`), and
each server connection owns one frame buffer that grows to the largest
frame and is then reused (`protocol.rs`, `FrameReader`). Peak memory is
about `streams x chunk_size` per side, plus socket buffers. Measured: about
13 MiB RSS on each side for a 1 GiB file.

**Chunk size tradeoffs:**

| Smaller chunks (e.g. 64 KiB) | Larger chunks (e.g. 16 MiB) |
| --- | --- |
| Less data lost and resent on a failure | More per-chunk overhead amortized (frame headers, acks, syscalls, lock acquisitions) |
| Less memory per stream | More memory per stream (`streams x window` does *not* multiply memory, but `streams x chunk` does) |
| Bigger bitmap (1 bit per chunk) and more acks | Coarser resume, and a slow link can take a long time per chunk, which interacts badly with timeouts |

The default of **1 MiB** sits in the flat part of the curve. On the
benchmark machine, 256 KiB was 2% to 18% slower than 1 MiB (barely
different on tmpfs, more on disk) and 4 MiB was no faster. Resume state for
a 1 TiB file is 128 KiB of bitmap, and a lost chunk costs at most 1 MiB. The
protocol allows 4 KiB to 16 MiB. The upper bound is what caps the server's
per-connection memory, since no frame can be bigger.

## 4. The wire protocol

The message table is in the README. The decisions behind it:

**Length-prefixed frames.** TCP is a byte stream with no message boundaries.
A 4-byte length in front of every frame tells the reader exactly how many
bytes make up the next message. The alternative, delimiters, requires
scanning and escaping binary data. The length is also checked against
`MAX_FRAME_LEN` *before* allocating, so a peer cannot make us allocate 4 GiB
by sending `0xFFFFFFFF` (`FrameReader::read`).

**Fixed-width little-endian integers.** These are trivial to encode and
decode, have no varint edge cases, and match x86 and ARM byte order. A few
bytes of padding per message do not matter next to a 1 MiB payload.

**A `Hello` that fully identifies the file** (name, size, chunk size,
whole-file SHA-256). This is what makes resume and parallel streams work:
any connection that presents the same identity attaches to the same
transfer, and a different file under the same name is detected (`Conflict`
while one is in progress, a fresh start otherwise).

**Explicit offsets in every `Chunk`.** A chunk is self-describing, so order
does not matter, any stream can carry any chunk, and the server validates
that the offset is chunk-aligned and the length is exactly right for that
index (`Transfer::chunk_index`). A sequence-number-only design would force
ordering or a lookup table.

**Acks and nacks per chunk, with a window.** The ack is the server saying
"verified and written". It frees a window slot on the client and moves the
progress bar. A `ChunkNack` is a negative ack: the hash did not match, so
resend it. Without a window (stop-and-wait, `--window 1`), each stream idles
for a full round trip per chunk. The benchmark shows that clearly: 218 vs.
299 MiB/s on one stream even over loopback, and the gap grows with latency.
With a window of `W` chunks, a stream can keep up to `W x chunk_size` bytes
in flight, which should cover the link's bandwidth-delay product.

**Why have acks at all if TCP is reliable?** TCP guarantees that bytes
handed to the *kernel* arrive at the peer's *kernel*. It says nothing about
whether the receiving application verified them, wrote them, or crashed
first. Application-level acks are what let the client know which chunks it
can forget about, and they are the only reliable progress signal.

**Zero-copy decode.** `Message::Chunk { data: &'a [u8] }` borrows from the
reader's buffer instead of copying 1 MiB into a fresh `Vec`. On the send
side, `write_message` writes the small header and then the payload slice
directly, so the payload is never copied into a frame buffer either.

**A version and magic in `Hello`.** `"RFT1"` makes a non-rft peer fail on the
first message. The version allows incompatible changes later.

## 5. Backpressure and bounded concurrency

*Backpressure* means a fast producer is slowed to the pace of a slow
consumer instead of piling up work in memory. Unbounded concurrency is how
services fall over: accept every connection, spawn a task for each, buffer
everything, and memory grows until the OOM killer arrives. `rft` bounds work
at every level:

| Bound | Where | Mechanism |
| --- | --- | --- |
| Files in flight | `client.rs`, `send_files` | `Semaphore::new(max_files)`. Each file task acquires a permit first. |
| Connections per file | `client.rs`, `attempt_once` | At most `streams` stream tasks in a `JoinSet`. |
| Unacked chunks per stream | `client.rs`, `run_stream` | `Semaphore::new(window)`. The writer takes a permit per chunk and calls `forget()`; the ack reader calls `add_permits(1)`. |
| Server connections | `server.rs`, `run_until` | `Semaphore::new(max_connections)`. A permit is acquired *before* `accept()`. |
| Bytes in the TCP pipe | the kernel | Socket buffers fill, so `write_all` returns `Pending` and the writer pauses. |
| Frame size | `protocol.rs` | `MAX_FRAME_LEN`, checked before allocation. |

Two details are worth calling out.

**The server acquires the permit before `accept()`.** Accepting first and
then waiting for a permit would still accept unbounded sockets, each
holding a file descriptor. Acquiring first means that at the limit the
server simply stops calling `accept()`, and new connections queue in the
kernel's listen backlog. That is backpressure pushed all the way to the
client's `connect`.

**The deadlock this exposed, and its fix.** With the server limited to 2
connections and a client sending several files with 4 streams each, the
first version could deadlock. Each file got its first connection; its other
3 streams were stuck in the server's backlog. The first stream finished all
the chunks itself, but the file still waited for its stuck siblings, each
holding a server slot the others needed. The fix is in `attempt_once`:
secondary streams race their handshake against a `watch` channel that flips
when the work queue first runs empty. A stream that has not connected by
then has nothing to do, so it gives up. The test
`completes_when_server_connection_limit_is_below_demand` covers this. The
general lesson: when a task holds one bounded resource while waiting for
another, check whether the waiting can form a cycle.

## 6. Parallel streams for one file

Why open several TCP connections for one file, when TCP is already
reliable and ordered?

- **A single TCP flow is often not enough.** Congestion control (CUBIC,
  BBR) treats each flow separately. On long fat networks with some loss, one
  flow recovers slowly after each loss event, and N flows recover N times as
  fast in aggregate. This is why tools like GridFTP, `aws s3 cp`, and
  download accelerators use parallel connections.
- **Per-connection CPU work** (hashing each chunk, copying through the
  kernel) spreads across cores because each stream is its own task.
- **Head-of-line blocking.** One slow chunk does not stall the others.

The cost is fairness to other users of the link (N flows take N shares), so
`--streams` is a knob, not a fixed value.

On the measured machine, 4 streams moved the actual transfer phase from
about 1.9 s to about 0.56 s for 1 GiB, a 3.4x speedup, even over loopback.

How the streams cooperate (`client.rs`):

- A shared `WorkQueue` holds the missing chunk indices (`Mutex<VecDeque>`).
  Each stream's writer pops the next index, reads that chunk with **its own
  file handle** (so no stream fights over a shared file cursor), hashes it,
  and sends it.
- Each stream has a writer future and an ack-reader future, run concurrently
  in one task by `tokio::select!`. They share an `inflight` map, the window
  semaphore, and a `Notify`.
- A nacked chunk goes back on the shared queue, so any stream may resend it.
  The requeue and the inflight removal happen under one lock, so a writer
  can never observe "queue empty and nothing in flight" in between and exit
  early. The lock order is always inflight, then queue.
- On the server, all connections for one file share one `Arc<Transfer>`.
  Each connection opens its own write handle and does `seek` then `write_all`
  at the chunk's offset, so there is no shared cursor to race on.

## 7. SHA-256 vs. CRC32

The sibling project `rust-wal` uses CRC32 per record, and that is the right
choice there. Here SHA-256 is, and the difference is the threat model.

| | CRC32 | SHA-256 |
| --- | --- | --- |
| Designed for | Accidental errors (bursts, bit flips) | Collision resistance, even against an adversary |
| Size | 4 bytes | 32 bytes |
| Speed | ~10+ GB/s with hardware support | ~1.2 GB/s per core with SHA-NI (measured: 1 GiB in 0.88 s), much slower without |
| Chance two different chunks match | 1 in 2^32 per pair, so at millions of chunks per day, real | Negligible |

Reasons to pay for SHA-256 here:

1. **End-to-end identity.** The whole-file digest names *the file*. It
   decides whether saved resume state belongs to this file. A 32-bit value
   is too small to be an identity across many files and transfers.
2. **The data crosses a network and other people's disks.** TCP's checksum
   is only 16 bits and is recomputed by offloading NICs and middleboxes.
   Real-world studies find corrupt packets that pass it. An application
   checksum is cheap insurance.
3. **Users compare it.** `rft hash` prints the same digest as `sha256sum`,
   so the result can be checked against a published hash.

What SHA-256 here does *not* give you: authentication. An attacker who can
modify traffic can also replace the digest. That needs a MAC or TLS (see
section 13).

**Why both per-chunk and whole-file hashes?** The per-chunk hash catches
corruption *in transit* immediately, so only that chunk is resent. The
whole-file hash catches everything else: a bug in offset handling, a chunk
written to the wrong place, corruption of the `.part` file on disk between
runs (tested in `corruption_of_partial_file_is_caught_by_whole_file_hash`),
or a source file that changed mid-transfer. It is the end-to-end check, and
the end-to-end argument says only the endpoints can verify correctness.

## 8. Durability: write, flush, fsync, and atomic rename

Four layers sit between `write_all` and the disk platter:

1. **The process buffer.** `tokio::fs::File` and `BufWriter` buffer data in
   user space. `flush()` hands it to the kernel.
2. **The page cache.** After `write(2)` returns, the data is in kernel
   memory. It survives the *process* crashing, but not the *machine*
   crashing.
3. **The device cache.** `fsync(2)` / `fdatasync(2)` push dirty pages to the
   device and issue a cache flush. When they return, the data survives power
   loss, assuming the device does not lie about flushes.
4. **Metadata.** A new file name or a rename lives in the *directory*, which
   is its own inode. Fsyncing the file does not make its name durable. You
   must fsync the directory (`state.rs`, `sync_dir`).

How `rft` uses this:

- **Chunks are written but not fsynced individually.** Fsyncing every 1 MiB
  would cap throughput at the disk's fsync rate. Instead, a **checkpoint**
  every `checkpoint_every` chunks (default 64, so 64 MiB) does one
  `sync_data()` on the `.part` file and then saves the bitmap
  (`Transfer::checkpoint`).
- **Order matters: data first, then state.** If the server crashes between
  the two, the old state file *undercounts*. Those chunks are resent, which
  is wasteful but harmless. The opposite order could leave a state file
  *claiming* chunks that never reached the disk. Those would be skipped, and
  the whole-file hash would fail at the end. The invariant is that **the
  state file never claims more than is durable**.
- **The state file is replaced atomically.** Write `NAME.state.tmp`, fsync
  it, `rename` it over `NAME.state`, fsync the directory. `rename` is atomic
  on POSIX filesystems, so a reader sees the old version or the new one,
  never a torn mix. A trailing SHA-256 inside the file catches anything else.
- **Commit is an atomic rename.** On `Finish`, the server runs
  `sync_all()` on `.part`, re-hashes it, renames it to the final name, and
  fsyncs the destination directory (`Transfer::finalize`). Anyone watching
  the destination directory sees either no file or the complete, verified
  file. That is why `.part` files live in a separate `.rft-partial/`
  directory: tools that pick up new files never see partial ones.
  `.rft-partial` is inside the destination so the rename stays on one
  filesystem. A cross-filesystem `rename` fails with `EXDEV`.
- **The `.part` file is pre-sized** with `set_len(file_size)`. Chunks can then
  be written at any offset in any order, the file's size never changes
  during the transfer (so `fdatasync` has no size metadata to write), and on
  most filesystems the unwritten parts are sparse holes that cost no space.

## 9. Resume state design

**The server is the source of truth.** The client keeps no state at all.
It asks "what do you have?" in every `Hello` and sends the rest. This is the
simplest design that is correct, because only the receiver knows what it
actually made durable. A killed client loses nothing, and a different
machine could even finish the transfer.

**A bitmap, one bit per chunk** (`bitmap.rs`). It is the natural
representation once chunks arrive out of order from parallel streams. A
"highest contiguous offset" watermark (like a TCP cumulative ack) would be
smaller, but one slow chunk would hold back the recorded progress of every
chunk after it. At 1 bit per MiB the bitmap stays tiny (128 KiB for 1 TiB),
and the protocol sends it once per `Hello`.

**What is in the state file**: file size, chunk size, whole-file SHA-256,
the bitmap, and a checksum (`state.rs`). The identity fields decide whether
the saved state applies to the file that is being sent now. If the user
edited the file between runs, its SHA-256 differs and the old progress is
thrown away (tested in `changed_source_file_does_not_reuse_stale_state`).
Without the digest in the identity, resuming would splice old and new
content together. The whole-file check would catch it at the end, but only
after a wasted transfer.

**When state is written:**

| Trigger | Why |
| --- | --- |
| Every `checkpoint_every` completed chunks | Bounds the work lost to a server crash |
| Last connection for a file disconnects | A killed client leaves an exact checkpoint, so resume is precise |
| Graceful shutdown (Ctrl-C) | Same, for every in-progress file |

The checkpoint interval is a **classic durability vs. throughput knob**, the
same tradeoff as group commit in a database WAL. A shorter interval loses
less on a crash but fsyncs more often.

**In memory vs. on disk.** While any connection is attached, the
authoritative bitmap is in memory (`TransferInner.completed`) and new
connections get a snapshot of it. When the last connection leaves, the
transfer is checkpointed and dropped from the registry. The next `Hello`
reloads it from disk. That keeps memory proportional to *active* transfers.

**Corrupt or mismatched state is discarded, not fatal.** Starting over is
always safe because the sender still has the data. A resume system should
never be able to make a transfer *impossible*.

## 10. Retries, backoff, and timeouts

**Which errors are retryable** (`TransferError::is_retryable`):

- Retryable: I/O errors, timeouts, connection closed, and the server
  rejecting with `Conflict` or `Internal`. These are transient.
- Not retryable: invalid name, file too large, already exists, whole-file
  checksum mismatch, protocol violations, local errors such as a missing
  file. Retrying would fail the same way, and retrying something permanent
  only adds load.

**Every retry is a resume.** The retry loop in `send_file` just runs
`attempt_once` again, which starts with a fresh `Hello`, so the server's
bitmap decides what to send. No special resume code path exists, which means
the resume path is exercised by every retry and does not rot.

**Exponential backoff with jitter** (`client.rs`, `backoff`): the delay starts
at 200 ms, doubles each attempt, and is capped at 10 s. "Equal jitter" makes
each delay half fixed and half random. Without jitter, many clients that
failed at the same moment, for example because the server restarted, retry
in lockstep and knock it over again: the *thundering herd*.

**Timeouts everywhere a peer can stall:**

| Timeout | Default | Guards against |
| --- | --- | --- |
| Connect | 10 s | Black-holed host (SYNs silently dropped) |
| Every read and write | 30 s | A dead peer that never sends FIN or RST, such as a pulled cable or a NAT that dropped the mapping |
| Server idle per connection | 30 s | Slowloris-style clients holding slots forever |
| Waiting for `FinishAck` | 30 s + 1 s per 50 MB | The server's whole-file re-hash takes time proportional to size |

Without timeouts, a TCP connection to a vanished peer can hang for hours,
until keepalive fires (2 hours by default on Linux) or forever.

## 11. Treating the network as hostile

Everything the server receives is attacker-controlled. The server's
defenses:

- **Path traversal** (`names.rs`). The file name ends up in
  `root.join(name)`. If `name` is `../../etc/cron.d/x` or `/etc/passwd`, a
  naive join escapes the directory or replaces it entirely: `Path::join`
  with an absolute path discards the base. The rule is to *reject*, not
  sanitize. A name must be a single normal path component with no `/`, `\`,
  `:`, NUL, or control characters, and it must not be `.`, `..`, or the
  reserved `.rft-partial`. As a final check, `Path::components()` must yield
  exactly one `Normal` component equal to the input. Sanitizing (for
  example stripping `../`) is where traversal bugs live: strip `../` once
  from `....//` and you get `../`.
- **Resource limits**: maximum file size (checked in `Hello`, before
  pre-sizing the file), chunk size between 4 KiB and 16 MiB, maximum frame
  length checked before allocation, maximum connections, and idle timeouts.
- **Strict protocol validation**: unknown message types, trailing bytes,
  bad magic, invalid UTF-8, misaligned offsets, wrong chunk lengths, and
  out-of-order messages are all rejected with an `Error` and the connection
  is closed. A strict parser is a smaller attack surface than a lenient one.
- **No overwrite**: an existing destination file is never replaced.

Not addressed: authentication and encryption. Anyone who can reach the port
can upload files up to the size limit. See section 13.

## 12. Failure modes, one by one

| What happens | Detected by | Outcome |
| --- | --- | --- |
| Bit flip in a chunk in transit | Per-chunk SHA-256 on the server | `ChunkNack`, resend, up to 3 times, then the client gives up |
| Client process killed | Server reads EOF or reset | Server checkpoints on disconnect. Next `send` resumes exactly. |
| Network drops mid-transfer | Client I/O error or timeout | Client backs off and retries, resuming from the server's bitmap |
| Server process killed | Client I/O error; restart loads `.state` | Resume from the last checkpoint; up to `checkpoint_every` chunks are resent |
| Server machine loses power | Same | Same, because only fsynced data was ever recorded in `.state` |
| `.state` corrupt or truncated | Trailing SHA-256 in the state file | Ignored; the transfer restarts from zero |
| `.part` corrupted on disk | Whole-file SHA-256 at `Finish` | `ChecksumMismatch`; both files deleted; nothing committed |
| Source file edited between runs | Different SHA-256 in `Hello` | Old state discarded; fresh transfer |
| Source file edited *during* a run | Whole-file SHA-256 at `Finish` | `ChecksumMismatch`; nothing committed |
| Two different files with the same name at once | Identity mismatch in the registry | Second one gets `Conflict` (retryable) |
| Destination file already exists | Existence check in `Hello` and before rename | `AlreadyExists`; never overwritten |
| Disk full on the server | `write` / `fsync` error | Connection error; the client retries, and fails again until there is space |
| Peer silent forever | Read and write timeouts | Connection dropped; slot freed |
| Malicious name, giant frame, junk bytes | Validation | `Error` reply; connection closed |

## 13. What this deliberately does not do

- **Encryption and authentication.** A production version would wrap the
  `TcpStream` in TLS (`tokio-rustls`). The rest of the code is generic over
  `AsyncRead + AsyncWrite`, so this is a contained change. A pre-shared-key
  HMAC per chunk would add authentication without TLS.
- **Overlap hashing with sending.** The sender hashes the file before
  `Hello` (0.83 s per GiB), and the server re-hashes after the last chunk
  (0.78 s per GiB). Together they are about 75% of end-to-end time on
  tmpfs. The server's pass could be removed by hashing chunks in order as
  they arrive, using a small reorder buffer, but chunks from parallel
  streams arrive out of order and resumed chunks are never re-sent at all.
  The sender's pass could overlap with streaming if the digest moved to
  `Finish`, but then resume would need another way to recognize "the same
  file", such as size plus mtime, which is weaker. I chose correctness and
  simplicity and documented the cost.
- **Directory trees, compression, delta sync.** Each is a layer on top of
  the same chunk protocol.
- **`renameat2(RENAME_NOREPLACE)`.** There is a small race window between
  "final file does not exist" and the rename. The Linux-only syscall would
  close it.

## 14. Rust concepts used here

A map from each concept to where it shows up in the code.

**`async fn` and `.await`.** An `async fn` does not run when called. It
returns a *future*, a value describing the work, and nothing happens until
something awaits or polls it. `.await` means "pause this task here until the
inner future is ready, and let the thread run other tasks meanwhile".
Example: `connect_and_hello` in `client.rs` awaits the connect, the write,
and the read in sequence, reading like blocking code while never blocking a
thread.

**Futures are lazy, can be combined, and can be cancelled by dropping.**
Because a future is a value, you can run several at once in one task with
`tokio::select!` (`run_stream` runs its writer and ack-reader futures
concurrently) and stop one by dropping it. Dropping a future cancels it at
its current `.await` point. The tests rely on this: `tokio::select!` drops the
in-progress `send_file` future to simulate killing the client. *Cancellation
safety* matters. `run_stream` only lets the reader be cancelled when the
writer has finished, at which point the reader is idle between frames, so no
half-read frame is lost.

**`tokio::spawn` and `JoinSet`.** `spawn` hands a future to the runtime as an
independent task that may run on any worker thread. A `JoinSet` owns a group
of tasks: `join_next()` waits for the next one to finish, and dropping the
set **aborts** all of them. This gives structured cleanup. In `attempt_once`,
if one stream fails, `?` returns early, the `JoinSet` is dropped, and every
sibling stream stops. The server's accept loop keeps all connection tasks in
a `JoinSet` for the same reason.

**`Send` and `'static` bounds.** `tokio::spawn` requires the future to be
`Send + 'static`:

- `'static`: the task may outlive the function that spawned it, so it cannot
  borrow local variables. It must *own* everything it uses. That is why
  `send_files` clones `addr`, `path`, and `opts` into each task and moves
  them in with `async move`.
- `Send`: the work-stealing scheduler may move the task between threads at
  any `.await`, so everything the task holds across an `.await` must be safe
  to move between threads. This is why the code never holds a
  `std::sync::MutexGuard` across an `.await` (the guard is not `Send`).
  Locks in `run_stream` are taken in a small `{ ... }` block that ends before
  the next `.await`. If you get this wrong, the program does not compile,
  instead of deadlocking at runtime.

**`Arc<T>`: shared ownership across tasks.** `Arc` is a thread-safe,
reference-counted pointer. `Arc::clone` gives another handle to the *same*
value and bumps a counter, and the value is freed when the last handle is
dropped. Examples: the server's `Arc<Shared>` (config plus registry) is cloned
into every connection task. Each in-progress file is an `Arc<Transfer>`
shared by all its connections. `Arc::ptr_eq` in `Shared::release` checks
whether the registry still points to *this* transfer and not a newer one
with the same name.

**Interior mutability: `Mutex`, atomics.** `Arc` gives shared *read* access.
Mutating shared state requires synchronization:

- `std::sync::Mutex`: cheap, for short critical sections that contain no
  `.await` (the client's work queue and inflight map).
- `tokio::sync::Mutex`: can be held across `.await`, for example the server
  registry while state loads from disk, or `TransferInner` during a
  checkpoint fsync. It is slower, so use it only when needed.
- `AtomicUsize`, `AtomicBool`, `AtomicU64`: lock-free counters and flags
  (`Transfer::active`, `Transfer::finished`, bytes sent).

**`Semaphore`.** A counter of permits. `acquire().await` waits until one is
free. The permit is an RAII guard that returns itself when dropped. Three
uses: `max_files` and `max_connections` (permit held for a task's lifetime,
`acquire_owned` so the permit can move into the task), and the per-stream
window, where the writer calls `permit.forget()` and the *reader* returns it
with `add_permits(1)` when the ack arrives. That is how one task's progress
unblocks another.

**`Notify` and `watch`.** `Notify` wakes a waiting task without carrying
data. The ack reader wakes the writer when a window slot frees up or a chunk
is requeued. `notify_one` stores a permit if nobody is waiting yet, so
wakeups are not lost. `watch` is a single value that many tasks can observe.
The "queue drained" flag that lets idle streams give up is one.

**Ownership and borrowing across concurrent futures.** In `run_stream`,
`let Conn { rd, wr, frames, scratch } = &mut conn;` splits one struct into
separate mutable borrows of its fields. The writer future uses `wr` and
`scratch`, and the reader uses `rd` and `frames`. The compiler proves they
do not overlap, so both futures can run at once without locks. The same
borrow rules give the zero-copy `Message<'a>`: a `Chunk`'s `data: &'a [u8]`
borrows the `FrameReader`'s buffer, and the compiler will not let you call
`read` again (which would overwrite the buffer) while that borrow is alive.

**Lifetimes in signatures.** `FrameReader::read(&mut self, ...) ->
Result<Option<Message<'_>>, _>` ties the returned message's lifetime to
`self`. This is the borrow checker enforcing "do not use this chunk after
reading the next frame".

**Error handling: `Result`, `?`, `thiserror`, `anyhow`.** Rust has no
exceptions. Fallible functions return `Result<T, E>`, and `?` returns early
with the error, converting it via `From`.

- In the **library**, errors are a typed enum built with `thiserror`
  (`error.rs`, `TransferError`). `#[error("...")]` generates `Display`, and
  `#[from]` generates the `From` impl, which is what lets `?` turn an
  `io::Error` into `TransferError::Io`. Callers can `match` on variants. The
  retry loop does exactly that through `is_retryable`, and the tests assert
  on specific codes.
- In the **binary**, `anyhow::Result` is used. It is one opaque error type
  with context: `.with_context(|| format!("binding {}", addr))` adds a
  human-readable layer, and `{:#}` prints the chain. Use `anyhow` at the
  application edge, where errors are reported, not matched on.
- Timeouts show both layers: `timeout(d, fut).await` returns
  `Result<Result<T, io::Error>, Elapsed>`, hence the `.map_err(...)??` in
  `client.rs`. The first `?` handles the timeout and the second handles the
  I/O error.

**Traits and trait objects.** `Progress` (`progress.rs`) is a trait with
default (empty) methods. The CLI implements it with progress bars, and the
tests implement it with counters and a trip wire that interrupts a transfer
at a precise point. The library takes `Arc<dyn Progress>`, a trait object
(dynamic dispatch), so it does not need to be generic. The trait requires
`Send + Sync` because events arrive from many tasks at once.

**Enums with data and exhaustive `match`.** `Message` is an enum whose
variants carry different fields. `match` must handle every variant, so
adding a message type makes the compiler list every place that needs
updating. `let Some(x) = ... else { return ... };` (let-else) is used for
early exits.

**RAII and `Drop`.** Resources are released when their owner goes out of
scope: semaphore permits, file handles, sockets (dropping half of a split
`TcpStream` closes it), and `JoinSet`s (which abort their tasks). There is no
manual cleanup code on the error paths.

## 15. Interview questions and model answers

**Q: Walk me through what happens when I run `rft send big.iso --to host`.**
The client stats and SHA-256-hashes the file on the blocking pool, then
connects and sends `Hello` with the name, size, chunk size, and digest. The
server validates the name and limits, finds or loads the transfer, and
replies with a bitmap of chunks it already has. The client puts the missing
chunk indices in a queue and opens up to 4 streams. Each stream pulls
indices, reads that chunk at its offset into a reusable buffer, hashes it,
and sends it, with up to 8 unacked chunks in flight. The server verifies
each chunk's hash, writes it at its offset into a pre-sized `.part` file,
marks the bit, and acks. Every 64 chunks it fsyncs and checkpoints the
bitmap. When everything is acked, the client sends `Finish`. The server
fsyncs, re-hashes the whole file, renames it into place, fsyncs the
directory, and replies `FinishAck`.

**Q: How do you avoid loading the whole file into memory?**
Everything is streamed in fixed-size chunks through buffers that are
allocated once: one chunk buffer per sending stream, one frame buffer per
receiving connection, and a 1 MiB buffer for hashing. Peak memory is about
`streams x chunk_size` per side, independent of file size. I verified it:
13 MiB RSS for a 1 GiB file, and a test asserts that a 256 MiB transfer stays
under 64 MiB.

**Q: Why async rather than a thread per connection?**
The work is I/O-bound: mostly waiting on sockets and disks. Async tasks cost
a few KB and switch in user space, so thousands of connections are cheap,
and the code still reads sequentially. With threads, each connection costs
a stack and kernel scheduling, and you need locks for everything shared.
CPU-heavy work (hashing a whole file) goes to `spawn_blocking` so it does not
starve the async workers.

**Q: What is backpressure and where does your system apply it?**
Backpressure means slowing the producer to the consumer's pace instead of
buffering without limit. A per-stream window semaphore limits unacked chunks
(acks return permits). Semaphores cap concurrent files and server
connections, and the server takes its permit *before* `accept()`, so at
capacity clients wait in the kernel backlog. TCP flow control does the rest:
when the receiver is slow, socket buffers fill and `write` stops completing.

**Q: How does resume work, and why is it correct after a crash?**
The server keeps a bitmap of completed chunks in a sidecar file next to the
`.part` file. Checkpoints fsync the data *before* atomically writing the
bitmap, so the bitmap never claims a chunk that is not durable. After a
crash it can only undercount, which means some chunks are resent. On
reconnect, `HelloAck` carries the bitmap and the client sends only what is
missing. The whole-file SHA-256 at the end is the final backstop. Retries
reuse exactly this path, so resume is exercised constantly.

**Q: Why put the state on the server instead of the client?**
Only the receiver knows what it actually made durable. A client-side log
says "I sent chunk 7", not "chunk 7 is safely on the server's disk". With
server-side state the client is stateless, so it can be killed, rebooted, or
replaced by another machine, and the next `send` still resumes.

**Q: Explain your fsync strategy. Why not fsync every chunk?**
`write` only reaches the page cache. Durability needs `fsync`, and new names
and renames need an fsync of the directory too. Fsyncing every chunk would
cap throughput at the disk's flush rate, so I batch: one `fdatasync` every 64
chunks, then an atomic state update. That is the same tradeoff as group
commit in a database. The interval bounds how much is resent after a
crash. At commit: `fsync` the file, rename, `fsync` the directory.

**Q: Why atomic rename instead of writing straight to the final name?**
So that no observer ever sees a partial or unverified file under the real
name. `rename` within one filesystem is atomic: the name points to the old
thing or the new thing. Writing in place means a crash or a failed checksum
leaves a corrupt file that looks finished. The `.part` file lives in a
subdirectory of the destination so the rename is on the same filesystem.

**Q: TCP already has checksums. Why add SHA-256, and why both per chunk and
per file?**
TCP's checksum is 16 bits, is weak against some error patterns, and is
recomputed by NICs and middleboxes, so corruption does get through, and it
says nothing about what happens after the kernel. The per-chunk hash
localizes in-transit corruption to one chunk, which gets resent. The
whole-file hash is the end-to-end check. It catches bugs in offset
handling, disk corruption of the partial file between runs, and a source
file that changed. I chose SHA-256 over CRC32 because the digest also
serves as the file's identity for resume, where a 32-bit value is too
collision-prone. With SHA-NI it runs at about 1.2 GB/s.

**Q: Why multiple TCP connections per file?**
A single TCP flow often cannot fill a high bandwidth-delay-product link,
especially with some loss, because congestion control backs off per flow.
Several flows recover in parallel. They also spread per-chunk hashing
across cores. Measured here, 4 streams cut the network phase from 1.9 s to
0.56 s for 1 GiB. Chunks carry explicit offsets, so they can arrive in any
order on any connection, and each server connection writes at its own
offset with its own file handle.

**Q: How do you choose the chunk size?**
It trades per-chunk overhead (headers, acks, syscalls, locks) against
memory, resume granularity, and retransmit cost. I measured: 256 KiB was
slower than 1 MiB, and 4 MiB gave no further gain, so the default is 1 MiB.
It bounds per-stream memory to 1 MiB, costs at most 1 MiB per lost chunk,
and keeps the resume bitmap at 128 KiB for a 1 TiB file.

**Q: How do you prevent path traversal?**
I reject rather than sanitize. The name must be a single plain path
component: no separators (`/`, `\`), no `:`, NUL, or control characters, not
`.` or `..`, and not the reserved partial directory. As a last check,
`Path::components()` must return exactly one `Normal` component equal to the
input. That matters because `Path::join` with an absolute path silently
discards the base directory.

**Q: What happens if the source file changes during or between transfers?**
Between runs, the new SHA-256 differs from the saved state's, so the stale
progress is discarded and the transfer starts over. During a run, the chunks
still verify individually (each was hashed when read), but the whole-file
hash at `Finish` does not match the one in `Hello`. The server refuses to
commit and deletes the partial data.

**Q: How do retries avoid making an outage worse?**
Only transient errors are retried. Permanent ones, such as a bad name, too
large, already exists, or a checksum mismatch, fail fast. Delays grow
exponentially with a cap, and half of each delay is random jitter, so
clients that failed together do not retry in lockstep (the thundering herd).
Every retry is a resume, so it only resends missing chunks.

**Q: Tell me about a concurrency bug you hit.**
With the server limited to 2 connections and several files using 4 streams
each, transfers deadlocked. Each file's first connection got a slot, and its
other streams sat in the accept backlog. The first stream drained the work
queue on its own, but the file still waited for its siblings, which could
never connect because other files held the slots. The fix was a `watch` flag
set when the queue first empties. Streams still handshaking race against
it and give up. I added a test that runs 6 files x 4 streams against a
2-connection server. The general lesson: holding one bounded resource while
waiting for another is how cycles form.

**Q: What does `Send + 'static` on `tokio::spawn` mean, and how did it shape
your code?**
`'static` means the task cannot borrow from the spawning function, because it
may outlive it, so tasks own their data. I clone cheap handles (`Arc`s,
small strings, options) into each task with `async move`. `Send` means the
task may move between worker threads at any `.await`, so nothing it holds
across an `.await` may be thread-unsafe. In particular, I never hold a
`std::sync::MutexGuard` across an `.await`, and I use `tokio::sync::Mutex`
where a lock must span I/O. The compiler enforces both, so these mistakes
fail at compile time instead of in production.

**Q: How would you make this production-ready?**
Add TLS and authentication (mTLS or tokens), and a per-tenant quota.
Expose metrics (bytes, chunk retries, checkpoint latency, active transfers)
in addition to logs. Add a manifest for directory trees. Use
`RENAME_NOREPLACE`. Move the server's final verification off the critical
path, for example by hashing in order as chunks arrive. Add adaptive stream
count and window based on the measured RTT and throughput. Garbage-collect
`.part` files abandoned for longer than N days.
