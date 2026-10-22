//! The sidecar resume-state file stored next to each `.part` file.
//!
//! Layout (all integers little-endian):
//!
//! ```text
//! magic "RFTS" | version u16 | file_size u64 | chunk_size u32 | sha256 [32]
//! | bitmap_len u32 | bitmap [bitmap_len] | checksum: sha256 of everything before [32]
//! ```
//!
//! The file is replaced atomically (write temp, fsync, rename, fsync dir), so
//! a reader sees either the previous checkpoint or the new one, never a mix.
//! The trailing checksum catches anything else (bit rot, a truncated copy):
//! a state file that fails validation is discarded and the transfer restarts
//! from zero, which is always safe because the client still has the source.

use std::io;
use std::path::Path;

use tokio::io::AsyncWriteExt;

use crate::bitmap::ChunkBitmap;
use crate::hash::sha256;
use crate::protocol::Digest;

const STATE_MAGIC: [u8; 4] = *b"RFTS";
const STATE_VERSION: u16 = 1;

/// Everything the receiver needs to resume a transfer after a restart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferState {
    pub file_size: u64,
    pub chunk_size: u32,
    pub sha256: Digest,
    pub completed: ChunkBitmap,
}

impl TransferState {
    /// Serialize to the on-disk format.
    pub fn encode(&self) -> Vec<u8> {
        let bits = self.completed.as_bytes();
        let mut out = Vec::with_capacity(4 + 2 + 8 + 4 + 32 + 4 + bits.len() + 32);
        out.extend_from_slice(&STATE_MAGIC);
        out.extend_from_slice(&STATE_VERSION.to_le_bytes());
        out.extend_from_slice(&self.file_size.to_le_bytes());
        out.extend_from_slice(&self.chunk_size.to_le_bytes());
        out.extend_from_slice(&self.sha256);
        out.extend_from_slice(&(bits.len() as u32).to_le_bytes());
        out.extend_from_slice(bits);
        let sum = sha256(&out);
        out.extend_from_slice(&sum);
        out
    }

    /// Parse and validate the on-disk format.
    pub fn decode(buf: &[u8]) -> Result<Self, &'static str> {
        const FIXED: usize = 4 + 2 + 8 + 4 + 32 + 4;
        if buf.len() < FIXED + 32 {
            return Err("state file too short");
        }
        let (body, sum) = buf.split_at(buf.len() - 32);
        if sha256(body) != sum {
            return Err("state file checksum mismatch");
        }
        if body[..4] != STATE_MAGIC {
            return Err("bad state magic");
        }
        if u16::from_le_bytes(body[4..6].try_into().unwrap()) != STATE_VERSION {
            return Err("unsupported state version");
        }
        let file_size = u64::from_le_bytes(body[6..14].try_into().unwrap());
        let chunk_size = u32::from_le_bytes(body[14..18].try_into().unwrap());
        let sha256: Digest = body[18..50].try_into().unwrap();
        let bits_len = u32::from_le_bytes(body[50..54].try_into().unwrap()) as usize;
        let bits = &body[FIXED..];
        if bits.len() != bits_len || chunk_size == 0 {
            return Err("state file length mismatch");
        }
        let chunks = file_size.div_ceil(u64::from(chunk_size));
        let completed =
            ChunkBitmap::from_bytes(chunks, bits).ok_or("bitmap does not match file size")?;
        Ok(Self {
            file_size,
            chunk_size,
            sha256,
            completed,
        })
    }

    /// Load from `path`. `Ok(None)` if the file does not exist; an
    /// `InvalidData` error if it exists but is corrupt.
    pub async fn load(path: &Path) -> io::Result<Option<Self>> {
        match tokio::fs::read(path).await {
            Ok(buf) => Self::decode(&buf)
                .map(Some)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Durably and atomically replace the state file at `path`.
    pub async fn save(&self, path: &Path) -> io::Result<()> {
        let tmp = path.with_extension("state.tmp");
        let mut f = tokio::fs::File::create(&tmp).await?;
        f.write_all(&self.encode()).await?;
        f.sync_all().await?;
        drop(f);
        tokio::fs::rename(&tmp, path).await?;
        if let Some(dir) = path.parent() {
            sync_dir(dir).await?;
        }
        Ok(())
    }
}

/// fsync a directory so that a rename or create inside it is durable.
///
/// On Linux, `rename` updates the directory, and that update lives in the
/// directory's own metadata: without this, a crash can "undo" the rename.
pub async fn sync_dir(dir: &Path) -> io::Result<()> {
    let dir = dir.to_owned();
    tokio::task::spawn_blocking(move || {
        #[cfg(unix)]
        {
            std::fs::File::open(&dir)?.sync_all()
        }
        #[cfg(not(unix))]
        {
            let _ = dir;
            Ok(())
        }
    })
    .await
    .map_err(io::Error::other)?
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> TransferState {
        let mut completed = ChunkBitmap::new(21);
        completed.set(0);
        completed.set(20);
        TransferState {
            file_size: 21 * 4096 - 100,
            chunk_size: 4096,
            sha256: [5u8; 32],
            completed,
        }
    }

    #[test]
    fn encode_decode_roundtrip() {
        let s = sample();
        assert_eq!(TransferState::decode(&s.encode()).unwrap(), s);
    }

    #[test]
    fn any_bit_flip_is_detected() {
        let bytes = sample().encode();
        for i in 0..bytes.len() {
            for bit in 0..8 {
                let mut b = bytes.clone();
                b[i] ^= 1 << bit;
                assert!(
                    TransferState::decode(&b).is_err(),
                    "flip at byte {i} bit {bit} not detected"
                );
            }
        }
    }

    #[test]
    fn any_truncation_is_detected() {
        let bytes = sample().encode();
        for cut in 0..bytes.len() {
            assert!(TransferState::decode(&bytes[..cut]).is_err());
        }
    }

    #[tokio::test]
    async fn save_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("x.state");
        assert_eq!(TransferState::load(&p).await.unwrap(), None);
        let s = sample();
        s.save(&p).await.unwrap();
        assert_eq!(TransferState::load(&p).await.unwrap(), Some(s));
        assert!(!dir.path().join("x.state.tmp").exists());
        std::fs::write(&p, b"garbage").unwrap();
        let err = TransferState::load(&p).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
