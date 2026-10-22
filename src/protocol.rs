//! The wire protocol: length-prefixed frames carrying typed messages.
//!
//! Every message on the wire is a *frame*:
//!
//! ```text
//! +----------------+-----------+---------------------------+
//! | length: u32 LE | type: u8  | body: (length - 1) bytes  |
//! +----------------+-----------+---------------------------+
//! ```
//!
//! `length` counts the type byte plus the body, so a reader always knows
//! exactly how many bytes to wait for before it can decode a message, and can
//! refuse oversized frames *before* allocating anything. All integers are
//! little-endian and fixed width. See `README.md` for the full message table.
//!
//! Decoding is zero-copy for the hot path: a [`Message::Chunk`] borrows its
//! payload directly from the [`FrameReader`]'s reusable buffer, so receiving a
//! chunk never allocates.

use std::fmt;

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::bitmap::ChunkBitmap;

/// Protocol version carried in every `Hello`. Bump on incompatible changes.
pub const PROTOCOL_VERSION: u16 = 1;

/// Magic bytes at the start of every `Hello`, to fail fast on a non-rft peer.
pub const MAGIC: [u8; 4] = *b"RFT1";

/// Smallest chunk size a server accepts (4 KiB, one typical page).
pub const MIN_CHUNK_SIZE: u32 = 4 * 1024;

/// Largest chunk size a server accepts (16 MiB). This bounds per-connection memory.
pub const MAX_CHUNK_SIZE: u32 = 16 * 1024 * 1024;

/// Default chunk size (1 MiB). See `docs/DESIGN.md` for the reasoning.
pub const DEFAULT_CHUNK_SIZE: u32 = 1024 * 1024;

/// Longest accepted file name, in bytes (the common filesystem limit).
pub const MAX_NAME_LEN: usize = 255;

/// Size of the frame header: 4-byte length plus 1-byte type.
pub const FRAME_HEADER_LEN: usize = 5;

/// Size of a `Chunk` body before the payload: offset (8) + sha256 (32) + data length (4).
pub const CHUNK_HEADER_LEN: usize = 8 + 32 + 4;

/// Largest frame (type byte + body) either side will accept.
pub const MAX_FRAME_LEN: u32 = MAX_CHUNK_SIZE + CHUNK_HEADER_LEN as u32 + 1;

/// A SHA-256 digest.
pub type Digest = [u8; 32];

/// Message type tags (the byte after the length prefix).
mod tag {
    pub const HELLO: u8 = 0x01;
    pub const HELLO_ACK: u8 = 0x02;
    pub const CHUNK: u8 = 0x03;
    pub const CHUNK_ACK: u8 = 0x04;
    pub const CHUNK_NACK: u8 = 0x05;
    pub const FINISH: u8 = 0x06;
    pub const FINISH_ACK: u8 = 0x07;
    pub const ERROR: u8 = 0x08;
}

/// Errors produced while reading or decoding frames.
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// The underlying socket failed (including EOF in the middle of a frame).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The peer announced a frame larger than [`MAX_FRAME_LEN`].
    #[error("frame of {len} bytes exceeds the {max}-byte limit")]
    FrameTooLarge { len: u32, max: u32 },
    /// The type byte is not one we know.
    #[error("unknown message type 0x{0:02x}")]
    UnknownMessageType(u8),
    /// The body does not match the layout for its type.
    #[error("malformed message: {0}")]
    Malformed(&'static str),
}

/// Reasons a server rejects a request, sent in an `Error` message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// The file name is empty, too long, or tries to escape the target directory.
    InvalidName,
    /// The file is larger than the server's configured limit.
    FileTooLarge,
    /// The chunk size is outside `[MIN_CHUNK_SIZE, MAX_CHUNK_SIZE]`.
    InvalidChunkSize,
    /// A completed file with this name already exists on the server.
    AlreadyExists,
    /// Another, different transfer with the same name is in progress.
    Conflict,
    /// The reassembled file did not match the announced SHA-256.
    ChecksumMismatch,
    /// `Finish` arrived before every chunk was received.
    Incomplete,
    /// The peer broke the protocol (bad offset, unexpected message, ...).
    ProtocolViolation,
    /// The `Hello` carried an unsupported version or bad magic.
    UnsupportedVersion,
    /// The server failed internally (usually a disk error).
    Internal,
    /// A code this build does not know about.
    Unknown(u16),
}

impl ErrorCode {
    /// Wire representation.
    pub fn as_u16(self) -> u16 {
        match self {
            ErrorCode::InvalidName => 1,
            ErrorCode::FileTooLarge => 2,
            ErrorCode::InvalidChunkSize => 3,
            ErrorCode::AlreadyExists => 4,
            ErrorCode::Conflict => 5,
            ErrorCode::ChecksumMismatch => 6,
            ErrorCode::Incomplete => 7,
            ErrorCode::ProtocolViolation => 8,
            ErrorCode::UnsupportedVersion => 9,
            ErrorCode::Internal => 10,
            ErrorCode::Unknown(c) => c,
        }
    }

    /// Parse the wire representation. Unknown values are preserved.
    pub fn from_u16(v: u16) -> Self {
        match v {
            1 => ErrorCode::InvalidName,
            2 => ErrorCode::FileTooLarge,
            3 => ErrorCode::InvalidChunkSize,
            4 => ErrorCode::AlreadyExists,
            5 => ErrorCode::Conflict,
            6 => ErrorCode::ChecksumMismatch,
            7 => ErrorCode::Incomplete,
            8 => ErrorCode::ProtocolViolation,
            9 => ErrorCode::UnsupportedVersion,
            10 => ErrorCode::Internal,
            other => ErrorCode::Unknown(other),
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// The handshake a client sends first on every connection.
///
/// It fully identifies the file: two `Hello`s with the same name, size,
/// chunk size and digest refer to the same transfer, which is what lets a
/// later connection resume an earlier one and lets several connections
/// cooperate on one file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    pub version: u16,
    pub file_name: String,
    pub file_size: u64,
    pub chunk_size: u32,
    pub sha256: Digest,
}

/// Every message in the protocol. `'a` is the lifetime of the reader's buffer
/// that a `Chunk` payload borrows from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message<'a> {
    /// Client -> server: start or join a transfer.
    Hello(Hello),
    /// Server -> client: transfer accepted; `completed` marks chunks already stored.
    HelloAck { completed: ChunkBitmap },
    /// Client -> server: one chunk of file data at `offset`, with its own SHA-256.
    Chunk {
        offset: u64,
        sha256: Digest,
        data: &'a [u8],
    },
    /// Server -> client: chunk at `offset` verified and written.
    ChunkAck { offset: u64 },
    /// Server -> client: chunk at `offset` failed verification; resend it.
    ChunkNack { offset: u64 },
    /// Client -> server: all chunks acknowledged; verify and commit the file.
    Finish,
    /// Server -> client: whole-file SHA-256 verified, file is in place and durable.
    FinishAck,
    /// Either direction: fatal error for this connection.
    Error { code: ErrorCode, message: String },
}

impl Message<'_> {
    /// Short human-readable name, for logs and error messages.
    pub fn kind(&self) -> &'static str {
        match self {
            Message::Hello(_) => "Hello",
            Message::HelloAck { .. } => "HelloAck",
            Message::Chunk { .. } => "Chunk",
            Message::ChunkAck { .. } => "ChunkAck",
            Message::ChunkNack { .. } => "ChunkNack",
            Message::Finish => "Finish",
            Message::FinishAck => "FinishAck",
            Message::Error { .. } => "Error",
        }
    }
}

/// Serialize everything except a chunk's payload into `out`, as a complete
/// frame (length prefix included). For `Chunk`, the frame header and chunk
/// header are written and the caller appends `data` separately, so the
/// payload is never copied into an intermediate buffer.
fn encode_head(msg: &Message<'_>, out: &mut Vec<u8>) {
    out.clear();
    out.extend_from_slice(&[0u8; 4]); // length placeholder, patched below
    let mut payload_len = 0usize;
    match msg {
        Message::Hello(h) => {
            out.push(tag::HELLO);
            out.extend_from_slice(&MAGIC);
            out.extend_from_slice(&h.version.to_le_bytes());
            out.extend_from_slice(&h.file_size.to_le_bytes());
            out.extend_from_slice(&h.chunk_size.to_le_bytes());
            out.extend_from_slice(&h.sha256);
            let name = h.file_name.as_bytes();
            out.extend_from_slice(&(name.len() as u16).to_le_bytes());
            out.extend_from_slice(name);
        }
        Message::HelloAck { completed } => {
            out.push(tag::HELLO_ACK);
            out.extend_from_slice(&completed.len().to_le_bytes());
            let bits = completed.as_bytes();
            out.extend_from_slice(&(bits.len() as u32).to_le_bytes());
            out.extend_from_slice(bits);
        }
        Message::Chunk {
            offset,
            sha256,
            data,
        } => {
            out.push(tag::CHUNK);
            out.extend_from_slice(&offset.to_le_bytes());
            out.extend_from_slice(sha256);
            out.extend_from_slice(&(data.len() as u32).to_le_bytes());
            payload_len = data.len();
        }
        Message::ChunkAck { offset } => {
            out.push(tag::CHUNK_ACK);
            out.extend_from_slice(&offset.to_le_bytes());
        }
        Message::ChunkNack { offset } => {
            out.push(tag::CHUNK_NACK);
            out.extend_from_slice(&offset.to_le_bytes());
        }
        Message::Finish => out.push(tag::FINISH),
        Message::FinishAck => out.push(tag::FINISH_ACK),
        Message::Error { code, message } => {
            out.push(tag::ERROR);
            out.extend_from_slice(&code.as_u16().to_le_bytes());
            // Truncate on a char boundary so the receiver always sees valid UTF-8.
            let mut end = message.len().min(u16::MAX as usize);
            while !message.is_char_boundary(end) {
                end -= 1;
            }
            out.extend_from_slice(&(end as u16).to_le_bytes());
            out.extend_from_slice(&message.as_bytes()[..end]);
        }
    }
    let frame_len = (out.len() - 4 + payload_len) as u32;
    out[..4].copy_from_slice(&frame_len.to_le_bytes());
}

/// Encode a message into a standalone byte vector (frame header included).
/// Used by tests and for small control messages.
pub fn encode_to_vec(msg: &Message<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    encode_head(msg, &mut out);
    if let Message::Chunk { data, .. } = msg {
        out.extend_from_slice(data);
    }
    out
}

/// Write one message to `w`. Does not flush; callers using a `BufWriter`
/// decide when to flush.
pub async fn write_message<W>(
    w: &mut W,
    msg: &Message<'_>,
    scratch: &mut Vec<u8>,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    encode_head(msg, scratch);
    w.write_all(scratch).await?;
    if let Message::Chunk { data, .. } = msg {
        w.write_all(data).await?;
    }
    Ok(())
}

/// A tiny cursor over a frame body that turns short reads into `Malformed`.
struct Cursor<'a> {
    buf: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], ProtocolError> {
        if self.buf.len() < n {
            return Err(ProtocolError::Malformed("body too short"));
        }
        let (head, rest) = self.buf.split_at(n);
        self.buf = rest;
        Ok(head)
    }
    fn u16(&mut self) -> Result<u16, ProtocolError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, ProtocolError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, ProtocolError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn digest(&mut self) -> Result<Digest, ProtocolError> {
        Ok(self.take(32)?.try_into().unwrap())
    }
    fn finish(self) -> Result<(), ProtocolError> {
        if self.buf.is_empty() {
            Ok(())
        } else {
            Err(ProtocolError::Malformed("trailing bytes"))
        }
    }
}

/// Decode a frame (type byte followed by body, *without* the length prefix).
pub fn decode(frame: &[u8]) -> Result<Message<'_>, ProtocolError> {
    let (&ty, body) = frame
        .split_first()
        .ok_or(ProtocolError::Malformed("empty frame"))?;
    let mut c = Cursor { buf: body };
    let msg = match ty {
        tag::HELLO => {
            if c.take(4)? != MAGIC {
                return Err(ProtocolError::Malformed("bad magic"));
            }
            let version = c.u16()?;
            let file_size = c.u64()?;
            let chunk_size = c.u32()?;
            let sha256 = c.digest()?;
            let name_len = c.u16()? as usize;
            let name = c.take(name_len)?;
            let file_name = std::str::from_utf8(name)
                .map_err(|_| ProtocolError::Malformed("file name is not UTF-8"))?
                .to_owned();
            Message::Hello(Hello {
                version,
                file_name,
                file_size,
                chunk_size,
                sha256,
            })
        }
        tag::HELLO_ACK => {
            let chunk_count = c.u64()?;
            let bits_len = c.u32()? as usize;
            let bits = c.take(bits_len)?;
            let completed = ChunkBitmap::from_bytes(chunk_count, bits).ok_or(
                ProtocolError::Malformed("bitmap length does not match chunk count"),
            )?;
            Message::HelloAck { completed }
        }
        tag::CHUNK => {
            let offset = c.u64()?;
            let sha256 = c.digest()?;
            let len = c.u32()? as usize;
            let data = c.take(len)?;
            Message::Chunk {
                offset,
                sha256,
                data,
            }
        }
        tag::CHUNK_ACK => Message::ChunkAck { offset: c.u64()? },
        tag::CHUNK_NACK => Message::ChunkNack { offset: c.u64()? },
        tag::FINISH => Message::Finish,
        tag::FINISH_ACK => Message::FinishAck,
        tag::ERROR => {
            let code = ErrorCode::from_u16(c.u16()?);
            let len = c.u16()? as usize;
            let message = String::from_utf8_lossy(c.take(len)?).into_owned();
            Message::Error { code, message }
        }
        other => return Err(ProtocolError::UnknownMessageType(other)),
    };
    c.finish()?;
    Ok(msg)
}

/// Reads frames from a stream into one reusable buffer.
///
/// The buffer grows to the largest frame seen (at most [`MAX_FRAME_LEN`]) and
/// is then reused for every subsequent frame, so steady-state receiving does
/// no allocation and memory per connection is bounded by the chunk size.
#[derive(Debug)]
pub struct FrameReader {
    buf: Vec<u8>,
    max_frame_len: u32,
}

impl Default for FrameReader {
    fn default() -> Self {
        Self::new(MAX_FRAME_LEN)
    }
}

impl FrameReader {
    /// Create a reader that rejects frames longer than `max_frame_len`.
    pub fn new(max_frame_len: u32) -> Self {
        Self {
            buf: Vec::new(),
            max_frame_len,
        }
    }

    /// Read and decode the next message.
    ///
    /// Returns `Ok(None)` if the peer closed the connection cleanly *between*
    /// frames; EOF in the middle of a frame is an `UnexpectedEof` I/O error.
    pub async fn read<R>(&mut self, r: &mut R) -> Result<Option<Message<'_>>, ProtocolError>
    where
        R: AsyncRead + Unpin,
    {
        let mut len_buf = [0u8; 4];
        // Read the first byte separately to distinguish clean EOF from a torn frame.
        if r.read(&mut len_buf[..1]).await? == 0 {
            return Ok(None);
        }
        r.read_exact(&mut len_buf[1..]).await?;
        let len = u32::from_le_bytes(len_buf);
        if len == 0 {
            return Err(ProtocolError::Malformed("empty frame"));
        }
        if len > self.max_frame_len {
            return Err(ProtocolError::FrameTooLarge {
                len,
                max: self.max_frame_len,
            });
        }
        let len = len as usize;
        if self.buf.len() < len {
            self.buf.resize(len, 0);
        }
        r.read_exact(&mut self.buf[..len]).await?;
        decode(&self.buf[..len]).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(msg: Message<'_>) {
        let bytes = encode_to_vec(&msg);
        let len = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        assert_eq!(len, bytes.len() - 4, "length prefix must cover type + body");
        let decoded = decode(&bytes[4..]).expect("decode");
        assert_eq!(decoded, msg);
    }

    fn sample_hello() -> Hello {
        Hello {
            version: PROTOCOL_VERSION,
            file_name: "report-2026.tar.gz".into(),
            file_size: 123_456_789,
            chunk_size: DEFAULT_CHUNK_SIZE,
            sha256: [7u8; 32],
        }
    }

    #[test]
    fn every_message_roundtrips() {
        let mut bitmap = ChunkBitmap::new(19);
        bitmap.set(0);
        bitmap.set(7);
        bitmap.set(18);
        let data = vec![0xAB; 1000];
        roundtrip(Message::Hello(sample_hello()));
        roundtrip(Message::HelloAck { completed: bitmap });
        roundtrip(Message::HelloAck {
            completed: ChunkBitmap::new(0),
        });
        roundtrip(Message::Chunk {
            offset: 1 << 40,
            sha256: [1u8; 32],
            data: &data,
        });
        roundtrip(Message::Chunk {
            offset: 0,
            sha256: [0u8; 32],
            data: &[],
        });
        roundtrip(Message::ChunkAck { offset: 42 });
        roundtrip(Message::ChunkNack { offset: u64::MAX });
        roundtrip(Message::Finish);
        roundtrip(Message::FinishAck);
        roundtrip(Message::Error {
            code: ErrorCode::InvalidName,
            message: "nope".into(),
        });
        roundtrip(Message::Error {
            code: ErrorCode::Unknown(999),
            message: String::new(),
        });
    }

    #[test]
    fn hello_layout_is_stable() {
        let bytes = encode_to_vec(&Message::Hello(Hello {
            version: 1,
            file_name: "a".into(),
            file_size: 2,
            chunk_size: 4096,
            sha256: [0u8; 32],
        }));
        // len(4) type(1) magic(4) ver(2) size(8) chunk(4) sha(32) name_len(2) name(1)
        assert_eq!(bytes.len(), 4 + 1 + 4 + 2 + 8 + 4 + 32 + 2 + 1);
        assert_eq!(&bytes[..4], &(bytes.len() as u32 - 4).to_le_bytes());
        assert_eq!(bytes[4], tag::HELLO);
        assert_eq!(&bytes[5..9], b"RFT1");
        assert_eq!(&bytes[9..11], &1u16.to_le_bytes());
    }

    #[test]
    fn error_message_is_truncated_on_char_boundary() {
        let long = "é".repeat(40_000); // 80,000 bytes
        let bytes = encode_to_vec(&Message::Error {
            code: ErrorCode::Internal,
            message: long,
        });
        match decode(&bytes[4..]).unwrap() {
            Message::Error { message, .. } => {
                assert!(message.len() <= u16::MAX as usize);
                assert!(message.chars().all(|c| c == 'é'));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_type_and_trailing_bytes() {
        assert!(matches!(
            decode(&[0x7f]),
            Err(ProtocolError::UnknownMessageType(0x7f))
        ));
        let mut bytes = encode_to_vec(&Message::Finish);
        bytes.push(0);
        assert!(matches!(
            decode(&bytes[4..]),
            Err(ProtocolError::Malformed("trailing bytes"))
        ));
    }

    #[test]
    fn rejects_truncated_bodies() {
        let bytes = encode_to_vec(&Message::Hello(sample_hello()));
        for cut in 5..bytes.len() {
            assert!(
                decode(&bytes[4..cut]).is_err(),
                "truncated hello at {cut} decoded"
            );
        }
        let bytes = encode_to_vec(&Message::Chunk {
            offset: 0,
            sha256: [0; 32],
            data: &[1, 2, 3],
        });
        for cut in 5..bytes.len() {
            assert!(decode(&bytes[4..cut]).is_err());
        }
    }

    #[test]
    fn rejects_bad_magic_and_non_utf8_names() {
        let mut bytes = encode_to_vec(&Message::Hello(sample_hello()));
        bytes[5] = b'X';
        assert!(matches!(
            decode(&bytes[4..]),
            Err(ProtocolError::Malformed("bad magic"))
        ));
        let mut bytes = encode_to_vec(&Message::Hello(sample_hello()));
        let last = bytes.len() - 1;
        bytes[last] = 0xFF;
        assert!(matches!(
            decode(&bytes[4..]),
            Err(ProtocolError::Malformed("file name is not UTF-8"))
        ));
    }

    #[test]
    fn rejects_bitmap_with_wrong_length() {
        let mut body = vec![tag::HELLO_ACK];
        body.extend_from_slice(&100u64.to_le_bytes()); // 100 chunks need 13 bytes
        body.extend_from_slice(&3u32.to_le_bytes());
        body.extend_from_slice(&[0, 0, 0]);
        assert!(decode(&body).is_err());
    }

    #[tokio::test]
    async fn frame_reader_streams_multiple_frames_and_detects_eof() {
        let data = vec![9u8; 70_000];
        let mut wire = Vec::new();
        let mut scratch = Vec::new();
        write_message(&mut wire, &Message::Finish, &mut scratch)
            .await
            .unwrap();
        write_message(
            &mut wire,
            &Message::Chunk {
                offset: 4096,
                sha256: [3; 32],
                data: &data,
            },
            &mut scratch,
        )
        .await
        .unwrap();
        write_message(&mut wire, &Message::ChunkAck { offset: 4096 }, &mut scratch)
            .await
            .unwrap();

        let mut r = wire.as_slice();
        let mut fr = FrameReader::default();
        assert_eq!(fr.read(&mut r).await.unwrap(), Some(Message::Finish));
        match fr.read(&mut r).await.unwrap() {
            Some(Message::Chunk {
                offset, data: d, ..
            }) => {
                assert_eq!(offset, 4096);
                assert_eq!(d, &data[..]);
            }
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(
            fr.read(&mut r).await.unwrap(),
            Some(Message::ChunkAck { offset: 4096 })
        );
        assert_eq!(fr.read(&mut r).await.unwrap(), None, "clean EOF");

        // EOF in the middle of a frame is an error, not a clean close.
        let torn = &wire[..wire.len() - 3];
        let mut r = torn;
        let mut fr = FrameReader::default();
        fr.read(&mut r).await.unwrap();
        fr.read(&mut r).await.unwrap();
        assert!(matches!(fr.read(&mut r).await, Err(ProtocolError::Io(_))));
    }

    #[tokio::test]
    async fn frame_reader_rejects_oversized_frames_before_allocating() {
        let mut wire = (MAX_FRAME_LEN + 1).to_le_bytes().to_vec();
        wire.push(tag::CHUNK);
        let mut r = wire.as_slice();
        let mut fr = FrameReader::default();
        assert!(matches!(
            fr.read(&mut r).await,
            Err(ProtocolError::FrameTooLarge { .. })
        ));
        assert_eq!(fr.buf.capacity(), 0);

        let mut r: &[u8] = &[0, 0, 0, 0];
        assert!(matches!(
            fr.read(&mut r).await,
            Err(ProtocolError::Malformed("empty frame"))
        ));
    }
}
