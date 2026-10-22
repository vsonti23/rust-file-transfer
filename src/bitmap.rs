//! A fixed-size bitset recording which chunks of a file have been received.
//!
//! One bit per chunk keeps resume state tiny: a 1 TiB file in 1 MiB chunks
//! needs 1,048,576 bits, which is 128 KiB of state.

/// Which chunks of a transfer are complete. Bit `i` set means chunk `i` is stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkBitmap {
    bits: Vec<u8>,
    len: u64,
    ones: u64,
}

impl ChunkBitmap {
    /// A bitmap of `len` chunks, all missing.
    pub fn new(len: u64) -> Self {
        Self {
            bits: vec![0u8; Self::byte_len(len)],
            len,
            ones: 0,
        }
    }

    /// Number of bytes needed to hold `len` bits.
    pub fn byte_len(len: u64) -> usize {
        len.div_ceil(8) as usize
    }

    /// Rebuild from the wire/disk representation. Returns `None` if the byte
    /// length is wrong or padding bits past `len` are set.
    pub fn from_bytes(len: u64, bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::byte_len(len) {
            return None;
        }
        let rem = (len % 8) as u32;
        if rem != 0 {
            let last = *bytes.last()?;
            if last >> rem != 0 {
                return None;
            }
        }
        let ones = bytes.iter().map(|b| u64::from(b.count_ones())).sum();
        Some(Self {
            bits: bytes.to_vec(),
            len,
            ones,
        })
    }

    /// Total number of chunks tracked.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// True for a zero-chunk (empty file) bitmap.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Is chunk `i` complete? Out-of-range indices are reported as not complete.
    pub fn get(&self, i: u64) -> bool {
        i < self.len && self.bits[(i / 8) as usize] & (1 << (i % 8)) != 0
    }

    /// Mark chunk `i` complete. Returns `true` if it was newly set.
    ///
    /// # Panics
    /// Panics if `i >= len()`; callers validate indices first.
    pub fn set(&mut self, i: u64) -> bool {
        assert!(i < self.len, "chunk index {i} out of range {}", self.len);
        let byte = &mut self.bits[(i / 8) as usize];
        let mask = 1 << (i % 8);
        let newly = *byte & mask == 0;
        *byte |= mask;
        if newly {
            self.ones += 1;
        }
        newly
    }

    /// Number of completed chunks.
    pub fn count(&self) -> u64 {
        self.ones
    }

    /// True when every chunk is complete (vacuously true for an empty file).
    pub fn is_complete(&self) -> bool {
        self.ones == self.len
    }

    /// Indices of chunks that are still missing, in ascending order.
    pub fn missing(&self) -> impl Iterator<Item = u64> + '_ {
        (0..self.len).filter(move |&i| !self.get(i))
    }

    /// Raw little-endian-bit-order bytes (bit `i` is `bytes[i/8] >> (i%8)`).
    pub fn as_bytes(&self) -> &[u8] {
        &self.bits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_get_count_and_missing() {
        let mut b = ChunkBitmap::new(10);
        assert!(!b.is_complete());
        assert!(b.set(0));
        assert!(b.set(9));
        assert!(!b.set(9), "setting twice is idempotent");
        assert_eq!(b.count(), 2);
        assert!(b.get(0) && b.get(9) && !b.get(5) && !b.get(10));
        assert_eq!(
            b.missing().collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5, 6, 7, 8]
        );
        for i in 0..10 {
            b.set(i);
        }
        assert!(b.is_complete());
    }

    #[test]
    fn empty_bitmap_is_complete() {
        let b = ChunkBitmap::new(0);
        assert!(b.is_complete());
        assert!(b.as_bytes().is_empty());
    }

    #[test]
    fn from_bytes_validates_length_and_padding() {
        let mut b = ChunkBitmap::new(12);
        b.set(3);
        b.set(11);
        let copy = ChunkBitmap::from_bytes(12, b.as_bytes()).unwrap();
        assert_eq!(copy, b);
        assert!(ChunkBitmap::from_bytes(12, &[0]).is_none());
        assert!(ChunkBitmap::from_bytes(12, &[0, 0, 0]).is_none());
        // Bit 12 is padding for a 12-chunk bitmap and must be zero.
        assert!(ChunkBitmap::from_bytes(12, &[0, 0b0001_0000]).is_none());
    }
}
