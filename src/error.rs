//! The library's error type.

use std::io;

use thiserror::Error;

use crate::protocol::{ErrorCode, ProtocolError};

/// Everything that can go wrong while sending or receiving a file.
#[derive(Debug, Error)]
pub enum TransferError {
    /// A local or socket I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// The peer sent something we could not decode.
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    /// The server refused the request with an explicit error code.
    #[error("server rejected transfer ({code}): {message}")]
    Rejected { code: ErrorCode, message: String },
    /// An operation did not complete within its deadline.
    #[error("timed out while {0}")]
    Timeout(&'static str),
    /// The peer closed the connection when we expected a reply.
    #[error("connection closed by peer while {0}")]
    ConnectionClosed(&'static str),
    /// The peer sent a valid message that makes no sense at this point.
    #[error("unexpected {got} message while {context}")]
    Unexpected {
        got: &'static str,
        context: &'static str,
    },
    /// The same chunk failed server-side verification too many times.
    #[error("chunk at offset {offset} failed verification {attempts} times")]
    ChunkRetriesExhausted { offset: u64, attempts: u32 },
    /// The caller asked for something impossible (not a file, bad name, ...).
    #[error("invalid input: {0}")]
    InvalidInput(String),
}

impl TransferError {
    /// Whether retrying the whole transfer (which resumes from the server's
    /// recorded progress) has a reasonable chance of succeeding.
    ///
    /// Network trouble is retryable. A server that deliberately rejected the
    /// request, or data that keeps failing verification, is not: retrying
    /// would just fail the same way.
    pub fn is_retryable(&self) -> bool {
        match self {
            TransferError::Io(_)
            | TransferError::Timeout(_)
            | TransferError::ConnectionClosed(_) => true,
            TransferError::Protocol(ProtocolError::Io(_)) => true,
            TransferError::Rejected { code, .. } => {
                matches!(code, ErrorCode::Conflict | ErrorCode::Internal)
            }
            _ => false,
        }
    }
}

/// Convenience alias.
pub type Result<T, E = TransferError> = std::result::Result<T, E>;
