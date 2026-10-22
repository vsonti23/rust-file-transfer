//! Progress reporting hooks.
//!
//! The library never prints. Instead the sender calls a [`Progress`]
//! implementation, which the CLI backs with `indicatif` progress bars and
//! tests back with counters (or use to inject failures).

/// Receives progress events from [`crate::send_file`].
///
/// Methods take `&self` and the trait requires `Send + Sync` because events
/// arrive concurrently from every stream task of every file.
pub trait Progress: Send + Sync {
    /// A transfer attempt started; `already` bytes are on the server.
    fn start(&self, _name: &str, _total: u64, _already: u64) {}
    /// `bytes` more bytes were acknowledged by the server.
    fn advance(&self, _name: &str, _bytes: u64) {}
    /// Attempt number `attempt` failed with `error` and will be retried.
    fn retry(&self, _name: &str, _attempt: u32, _error: &crate::TransferError) {}
    /// The file was verified and committed by the server.
    fn finish(&self, _name: &str) {}
}

/// A [`Progress`] that ignores everything.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoProgress;

impl Progress for NoProgress {}
