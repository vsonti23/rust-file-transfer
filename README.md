# rust-file-transfer

[![CI](https://github.com/vsonti23/rust-file-transfer/actions/workflows/ci.yml/badge.svg)](https://github.com/vsonti23/rust-file-transfer/actions/workflows/ci.yml)

A concurrent, resumable, checksum-verified file transfer tool in Rust, built
on Tokio. One binary, `rft`, runs either as a server that receives files into
a directory or as a client that sends files to it.

Files are streamed in fixed-size chunks through reusable buffers, so memory
use does not depend on file size: sending a 1 GiB file peaks at about 13 MiB
of RSS on each side. Every chunk carries its own SHA-256, and the receiver
re-hashes the whole file before atomically moving it into place. If a
transfer is interrupted (killed client, dropped connection, crashed server),
running the same command again continues from the chunks the server already
has.

## Features

- **Streaming chunked I/O.** Reads and writes go one chunk at a time
  (default 1 MiB) through a buffer that is allocated once per connection.
  Files are never loaded into memory.
- **Bounded concurrency at three levels.** A semaphore limits files in
  flight, each file is split across several parallel TCP streams that write
  at independent offsets, and each stream has a window of unacknowledged
  chunks. The server has its own connection limit.
- **End-to-end integrity.** A per-chunk SHA-256 is checked before data
  touches the disk; a corrupted chunk is rejected and resent. The whole-file
  SHA-256 from the handshake is checked before the file is committed.
- **Crash-safe commit.** Data lands in a pre-sized `.part` file. On verified
  completion it is fsynced, renamed into place, and the directory is
  fsynced. A reader never sees a half-written file under the final name.
- **Resume.** A small sidecar state file records which chunks are durable.
  The handshake reply tells the client what to skip. Resume works across
  client restarts and server restarts.
- **Retries with exponential backoff and jitter** on connection errors.
  Each retry is a resume, so a flaky link loses at most the chunks that were
  in flight.
- **Hardened receiver.** Received names are validated against path
  traversal, sizes and chunk sizes are limited, frames are length-checked
  before any allocation, and idle connections time out.
- **Observability.** `indicatif` progress bars on the client and structured
  `tracing` logs (per-connection and per-file spans) on both sides.
- 55 tests, including a 256 MiB streamed transfer with a peak-RSS
  assertion, concurrent multi-file transfers, a client killed mid-transfer,
  a server crashed mid-transfer, on-disk corruption, and protocol abuse.

## Quick start

```sh
cargo build --release

# Terminal 1: receive into ./inbox
./target/release/rft serve --dir ./inbox --listen 0.0.0.0:7878

# Terminal 2: send files
./target/release/rft send big.iso photos.tar --to 127.0.0.1:7878

# Interrupted? Run the same command again; it resumes.
./target/release/rft send big.iso photos.tar --to 127.0.0.1:7878

# Check a file's SHA-256 (streamed, constant memory)
./target/release/rft hash big.iso
```

### `rft serve`

| Flag | Default | Meaning |
| --- | --- | --- |
| `-d, --dir` | `.` | Directory for received files (created if missing) |
| `-l, --listen` | `0.0.0.0:7878` | Address to bind |
| `--max-connections` | `64` | Simultaneous connections; extra clients wait in the accept backlog |
| `--max-file-size` | `1T` | Largest accepted file (`500M`, `10G`, ...) |
| `--timeout` | `30` | Idle timeout per connection, seconds |
| `--checkpoint-every` | `64` | Persist resume state every N completed chunks |

### `rft send`

| Flag | Default | Meaning |
| --- | --- | --- |
| `-t, --to` | required | Server `host:port` |
| `-c, --chunk-size` | `1M` | Chunk size, 4 KiB to 16 MiB |
| `-s, --streams` | `4` | Parallel TCP streams per file |
| `-j, --max-files` | `4` | Files sent concurrently |
| `-w, --window` | `8` | Unacknowledged chunks in flight per stream |
| `-r, --retries` | `5` | Retries after connection errors |
| `--timeout` | `30` | Per-operation read/write timeout, seconds |
| `--no-progress` | off | Disable progress bars |

Logging uses `RUST_LOG` (for example `RUST_LOG=rft=debug`), or `-v` / `-vv`.

### As a library

```rust
use std::sync::Arc;
use rft::{SendOptions, Server, ServerConfig, progress::NoProgress};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = Server::bind("127.0.0.1:0", ServerConfig::new("inbox")).await?;
    let addr = server.local_addr()?.to_string();
    tokio::spawn(server.run());

    let report = rft::send_file(&addr, "big.iso".as_ref(), &SendOptions::default(),
                                Arc::new(NoProgress)).await?;
    println!("sent {} bytes, {} resumed", report.file_size, report.resumed_bytes);
    Ok(())
}
```

## Architecture

```text
            client (rft send)                                server (rft serve)
 ┌──────────────────────────────────────┐        ┌──────────────────────────────────────────┐
 │ send_files                           │        │ accept loop                              │
 │  Semaphore(max_files)                │        │  Semaphore(max_connections)              │
 │   └─ send_file  (retry + backoff)    │        │   └─ connection task (one per TCP conn)  │
 │       1. SHA-256 whole file          │        │       Hello -> validate name/limits      │
 │       2. Hello  ───────────────────────────────────► registry: find/create Transfer      │
 │          HelloAck(bitmap) ◄─────────────────────────  (load sidecar state if present)    │
 │       3. queue = missing chunks      │        │                                          │
 │          stream 1..N (JoinSet)       │  TCP   │  Transfer (Arc, shared by its streams)   │
 │           writer: read chunk at off, │ ×N per │   ├─ .part file, pre-sized, pwrite-style │
 │             hash, send  ─────────────┼─ file ─┼──►├─ verify chunk SHA-256, seek+write    │
 │           reader: ack -> free window │        │   ├─ bitmap of completed chunks          │
 │             nack -> requeue  ◄───────┼────────┼───┤─ every K chunks: fsync .part, then   │
 │       4. Finish ─────────────────────┼────────┼──►│   atomically rewrite .state          │
 │          FinishAck ◄─────────────────┼────────┼───┴─ Finish: re-hash, rename, fsync dir  │
 └──────────────────────────────────────┘        └──────────────────────────────────────────┘
```

Source layout:

| File | Responsibility |
| --- | --- |
| `src/protocol.rs` | Framing, message encode/decode, zero-copy `FrameReader` |
| `src/client.rs` | Sender: work queue, parallel streams, windowing, retries |
| `src/server.rs` | Receiver: accept loop, transfer registry, checkpoints, commit |
| `src/state.rs` | Sidecar resume-state format and atomic save |
| `src/bitmap.rs` | One bit per chunk |
| `src/names.rs` | Path-traversal-safe name validation |
| `src/hash.rs` | SHA-256 helpers (streaming file hash on the blocking pool) |
| `src/progress.rs` | Progress callback trait |
| `src/main.rs` | CLI (`clap`), progress bars (`indicatif`), logging setup |

## Protocol

Every message is a frame: a 4-byte little-endian length, a 1-byte type, and
a body. The length covers the type byte and body, so the reader knows exactly
how much to read and can reject anything over the limit (16 MiB plus a small
header) before allocating.

```text
 0               4       5
 +---------------+-------+--------------------------------------+
 |  length (u32) | type  |  body (length - 1 bytes)             |
 +---------------+-------+--------------------------------------+
```

| Type | Message | Direction | Body (all integers little-endian) |
| --- | --- | --- | --- |
| `0x01` | `Hello` | C to S | `"RFT1"`, version u16, file size u64, chunk size u32, file SHA-256 [32], name length u16, name (UTF-8) |
| `0x02` | `HelloAck` | S to C | chunk count u64, bitmap length u32, bitmap (bit *i* set = chunk *i* already stored) |
| `0x03` | `Chunk` | C to S | offset u64, chunk SHA-256 [32], data length u32, data |
| `0x04` | `ChunkAck` | S to C | offset u64 |
| `0x05` | `ChunkNack` | S to C | offset u64 (hash mismatch, resend) |
| `0x06` | `Finish` | C to S | empty |
| `0x07` | `FinishAck` | S to C | empty (verified, renamed, durable) |
| `0x08` | `Error` | either | code u16, message length u16, message (UTF-8) |

A typical conversation on one stream:

```text
client                                            server
  | Hello{name, size, chunk_size, sha256}            |
  |------------------------------------------------->|  validate, find or load state
  |                    HelloAck{bitmap: 0b...0110}   |
  |<-------------------------------------------------|
  | Chunk{offset 0, sha256, data}                    |
  |------------------------------------------------->|  hash ok -> write at offset 0
  | Chunk{offset 3M, ...}     (window: up to 8       |
  |------------------------------------------------->|  unacked chunks per stream)
  |                               ChunkAck{0}        |
  |<-------------------------------------------------|
  |                               ChunkNack{3M}      |  hash mismatch
  |<-------------------------------------------------|
  | Chunk{offset 3M, ...}     (resent)               |
  |------------------------------------------------->|
  |                               ChunkAck{3M}       |
  |<-------------------------------------------------|
  | Finish                                           |
  |------------------------------------------------->|  fsync, re-hash whole file,
  |                               FinishAck          |  rename .part -> name, fsync dir
  |<-------------------------------------------------|
```

With `--streams N`, the client opens N connections that each send the same
`Hello`. The server attaches them to one shared transfer, and they pull
chunk indices from one queue. Chunk offsets are explicit, so chunks can
arrive in any order on any connection. `Finish` goes over whichever
connection is left.

Error codes: `InvalidName`, `FileTooLarge`, `InvalidChunkSize`,
`AlreadyExists`, `Conflict`, `ChecksumMismatch`, `Incomplete`,
`ProtocolViolation`, `UnsupportedVersion`, `Internal`.

## Integrity and resume semantics

On the receiver, each in-progress file `NAME` has two files in
`<dir>/.rft-partial/`:

- `NAME.part`: the data, pre-sized to the final length and written at
  chunk offsets.
- `NAME.state`: file size, chunk size, whole-file SHA-256, the
  completed-chunk bitmap, and a trailing SHA-256 over all of that.

The rules:

1. A chunk is written only after its SHA-256 matches the one in the `Chunk`
   message. Otherwise the server sends `ChunkNack`, and the client resends it
   up to 3 times.
2. A **checkpoint** fsyncs `.part` *first* and only then saves the bitmap
   (write temp, fsync, rename, fsync dir). The state file can therefore only
   undercount what is on disk, never overcount it. Checkpoints run every
   `--checkpoint-every` chunks, when the last connection for a file goes
   away, and on graceful shutdown.
3. A `Hello` resumes only if name, size, chunk size, **and** whole-file
   SHA-256 all match the saved state. If the source file changed, the old
   state is discarded. A state file that fails its checksum is ignored.
4. On `Finish`, the server fsyncs `.part`, re-hashes it end to end, and
   compares against the `Hello` digest. On a match it renames `.part` to the
   final name and fsyncs the directory. On a mismatch it deletes both files
   and returns `ChecksumMismatch`, so the next attempt starts clean.
5. The server never overwrites an existing file (`AlreadyExists`).

What survives what:

| Failure | Outcome |
| --- | --- |
| Client killed / network drops | Server checkpoints on disconnect. Re-running `send` resends only the missing chunks. |
| Connection reset mid-transfer | Client retries with backoff and resumes automatically. |
| Server process killed (`SIGKILL`) | Restart the server and re-run `send`. It resumes from the last checkpoint (at most `checkpoint_every` chunks are resent). |
| Power loss on the server | Same as above. Only fsynced data is claimed by the state file. |
| Bit flip in transit | Chunk SHA-256 mismatch, `ChunkNack`, resend. |
| Corruption of `.part` on disk between runs | Whole-file SHA-256 mismatch at `Finish`. Nothing is committed. |
| Source file modified between runs | Different SHA-256, so stale state is discarded and the transfer restarts. |

## Benchmarks

Measured with `scripts/bench.sh` on this project's development VM: 8 vCPU
Intel Xeon with SHA-NI, 15 GiB RAM, ext4 on a virtual disk. The disk
sustains about 455 MB/s of `dd ... conv=fdatasync` writes. Transfers go
over loopback. Times are the median of 3 runs of `rft send` (release
build), end to end: client-side hashing, the transfer, and server-side
verification and commit. The file is 1 GiB of random data.

**Source and destination on tmpfs (`/dev/shm`)**, which isolates CPU and
network from disk:

| Streams | Chunk | Window | Time (s) | Throughput (MiB/s) |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 256 KiB | 8 | 3.49 | 293 |
| 1 | 1 MiB | 8 | 3.43 | 299 |
| 4 | 1 MiB | 8 | 2.34 | 437 |
| 8 | 1 MiB | 8 | 2.16 | 474 |
| 4 | 4 MiB | 4 | 2.20 | 466 |
| 1 | 1 MiB | 1 (stop-and-wait) | 4.69 | 218 |

**Destination on ext4** (same machine):

| Streams | Chunk | Window | Time (s) | Throughput (MiB/s) |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 256 KiB | 8 | 5.01 | 204 |
| 1 | 1 MiB | 8 | 4.13 | 248 |
| 4 | 1 MiB | 8 | 3.11 | 330 |
| 8 | 1 MiB | 8 | 3.16 | 324 |
| 4 | 4 MiB | 4 | 3.09 | 331 |
| 1 | 1 MiB | 1 (stop-and-wait) | 5.82 | 176 |

Where the time goes (tmpfs, 1 GiB, 4 streams, 2.17 s total): about 0.83 s
for the sender's whole-file SHA-256 pass, about 0.78 s for the receiver's
verification pass, and about 0.56 s for the transfer itself, or roughly
1.8 GiB/s on the wire. With 1 stream the transfer phase takes about 1.9 s,
so parallel streams speed up the network part by about 3.4x. End-to-end
time is dominated by the two full-file hash passes, which are the price of
end-to-end verification. On ext4 the run is bounded by the disk.

Other measurements:

- **Memory**: peak RSS of 13.0 MiB (client) and 13.4 MiB (server) while
  sending a 1 GiB file with 4 streams. The `large` integration test checks
  that a 256 MiB transfer stays under 64 MiB peak RSS with client and server
  in one process. It measures about 23 MiB.
- **Resume**: sending the 1 GiB file with 1 stream, killing the client with
  `SIGKILL` after 2 s, and re-running resumed 808 of 1024 chunks. The second
  run took 1.83 s, most of it hashing.

Reproduce with `scripts/bench.sh [size_mib] [runs]` (set `WORK=/dev/shm/x` to
use tmpfs).

## Testing

```sh
cargo test            # 55 tests; a few seconds once built
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

| Suite | What it covers |
| --- | --- |
| unit (`src/`) | Encode/decode round trip of every message, stable byte layout, truncated, oversized, and malformed frames, bitmap edge cases, state file bit-flip and truncation detection, name validation, backoff bounds, SHA-256 test vectors |
| `tests/roundtrip.rs` | Empty, 1-byte, sub-chunk, exact-multiple, and short-last-chunk files; 1 to 8 streams; window of 1; 8 concurrent files; 6 files x 4 streams against a 2-connection server (no deadlock); duplicate names; no overwrite |
| `tests/large.rs` | 256 MiB generated file streamed end to end, with a peak-RSS assertion |
| `tests/resume.rs` | Client killed mid-transfer then resumed (sends exactly the missing bytes); server crashed then restarted (resumes from the on-disk checkpoint); client retries across a server restart; on-disk corruption caught by the whole-file hash; corrupt state file; changed source file |
| `tests/validation.rs` | Corrupted chunk NACKed and resent; wrong whole-file hash never committed; early `Finish`; 10 path-traversal names; size and chunk-size limits (not retried); misaligned and out-of-range chunks; handshake and version checks; idle timeouts; conflicting concurrent transfers; retry exhaustion |

All tests use `#[tokio::test]` servers on ephemeral ports and temp
directories, so they run in parallel.

## Design notes

[`docs/DESIGN.md`](docs/DESIGN.md) explains every decision in plain
language: async vs. threads, backpressure, chunk size, SHA-256 vs. CRC,
fsync ordering, the resume-state design, and failure modes. It also maps the
Rust concepts used (async/await, `Send + 'static`, `Arc`, `Semaphore`,
`JoinSet`, error handling) to the code, and ends with interview questions and
model answers.

## Non-goals and possible extensions

- **Encryption and authentication.** Not included. Run it over a VPN or SSH
  tunnel, or add TLS with `tokio-rustls` around the `TcpStream`.
- **Directory trees.** Each file is sent by its base name. Sending a tree
  would need a manifest and relative-path validation.
- **Hashing during the transfer.** The sender hashes the file before
  `Hello`, so the server can detect a changed source when it resumes.
  Computing the digest while streaming and sending it in `Finish` would hide
  that pass, but resume would then have to identify files by size and mtime.
- **Delta sync** (rsync-style rolling checksums) for files that changed a
  little.
- `RENAME_NOREPLACE` (`renameat2`) to close the small window between the
  existence check and the rename.

## License

MIT. See [LICENSE](LICENSE).
