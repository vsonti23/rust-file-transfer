//! SHA-256 helpers.

use std::io::Read;
use std::path::Path;

use sha2::{Digest as _, Sha256};

use crate::protocol::Digest;

/// Buffer size used when hashing whole files.
const HASH_BUF: usize = 1024 * 1024;

/// SHA-256 of an in-memory slice (used per chunk).
pub fn sha256(data: &[u8]) -> Digest {
    let out = Sha256::digest(data);
    let mut d = [0u8; 32];
    d.copy_from_slice(&out);
    d
}

/// Streaming SHA-256 of the first `len` bytes of a file (or all of it when
/// `len` is `None`), using one fixed 1 MiB buffer.
///
/// Hashing is CPU-bound and uses blocking reads, so it runs on Tokio's
/// blocking thread pool instead of stalling an async worker thread.
pub async fn sha256_file(path: &Path, len: Option<u64>) -> std::io::Result<Digest> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path)?;
        let mut reader: Box<dyn Read> = match len {
            Some(n) => Box::new(file.take(n)),
            None => Box::new(file),
        };
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; HASH_BUF];
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        let mut d = [0u8; 32];
        d.copy_from_slice(&hasher.finalize());
        Ok(d)
    })
    .await
    .map_err(std::io::Error::other)?
}

/// Lower-case hex rendering of a digest.
pub fn to_hex(d: &Digest) -> String {
    hex::encode(d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors() {
        assert_eq!(
            to_hex(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            to_hex(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[tokio::test]
    async fn file_hash_matches_slice_hash() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("f");
        let data: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&p, &data).unwrap();
        assert_eq!(sha256_file(&p, None).await.unwrap(), sha256(&data));
        assert_eq!(
            sha256_file(&p, Some(1000)).await.unwrap(),
            sha256(&data[..1000])
        );
    }
}
