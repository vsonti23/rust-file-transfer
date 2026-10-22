//! # rft: concurrent, resumable, verified file transfer
//!
//! `rft` moves large files over TCP with:
//!
//! - **Streaming, chunked I/O.** Files are read and written one fixed-size
//!   chunk at a time through reusable buffers; a file is never loaded into
//!   memory, so a 200 GB file costs the same RAM as a 2 MB one.
//! - **Bounded concurrency.** Several files in parallel, and several TCP
//!   streams per file, each limited by a [`tokio::sync::Semaphore`]. A
//!   per-stream window bounds how many chunks are in flight unacknowledged.
//! - **End-to-end integrity.** Every chunk carries its SHA-256, checked before
//!   it is written; the whole file is re-hashed on the receiver before it is
//!   committed.
//! - **Crash-safe commit.** Data lands in a `.part` file. A sidecar state file
//!   records which chunks are durable. On verified completion the `.part` is
//!   fsynced and atomically renamed into place.
//! - **Resume.** After a dropped connection, a killed client, or a restarted
//!   server, re-running the send continues from the chunks the server already
//!   has. Connection errors are retried automatically with exponential backoff.
//!
//! The crate is split into small modules:
//!
//! | Module | Responsibility |
//! | --- | --- |
//! | [`protocol`] | Frame format, message encode/decode, [`protocol::FrameReader`] |
//! | [`bitmap`] | Which chunks are complete |
//! | [`state`] | Durable sidecar resume state |
//! | [`names`] | Path-traversal-safe file name validation |
//! | [`server`] | The receiver |
//! | [`client`] | The sender |
//! | [`progress`] | Progress reporting hooks |
//!
//! See `README.md` for usage and `docs/DESIGN.md` for the reasoning behind
//! every design decision.

pub mod bitmap;
pub mod error;
pub mod hash;
pub mod names;
pub mod progress;
pub mod protocol;
pub mod state;

pub use error::{Result, TransferError};

/// Number of chunks a file of `file_size` bytes splits into.
pub fn chunk_count(file_size: u64, chunk_size: u32) -> u64 {
    file_size.div_ceil(u64::from(chunk_size))
}

/// Length of chunk `index` (the last chunk may be short).
pub fn chunk_len(file_size: u64, chunk_size: u32, index: u64) -> usize {
    let start = index * u64::from(chunk_size);
    (file_size - start).min(u64::from(chunk_size)) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_math() {
        assert_eq!(chunk_count(0, 4096), 0);
        assert_eq!(chunk_count(1, 4096), 1);
        assert_eq!(chunk_count(4096, 4096), 1);
        assert_eq!(chunk_count(4097, 4096), 2);
        assert_eq!(chunk_len(4097, 4096, 0), 4096);
        assert_eq!(chunk_len(4097, 4096, 1), 1);
        assert_eq!(chunk_len(8192, 4096, 1), 4096);
    }
}
