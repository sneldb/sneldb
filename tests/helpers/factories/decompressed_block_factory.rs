use std::sync::Arc;

use crate::engine::core::read::cache::DecompressedBlock;

/// Factory for constructing in-memory `DecompressedBlock` instances for tests.
pub struct DecompressedBlockFactory {
    bytes: Vec<u8>,
}

impl DecompressedBlockFactory {
    pub fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    /// Replace the internal buffer with provided raw bytes.
    pub fn with_raw_bytes(mut self, bytes: Vec<u8>) -> Self {
        self.bytes = bytes;
        self
    }

    /// Append raw bytes to the internal buffer.
    pub fn append_raw_bytes(mut self, bytes: &[u8]) -> Self {
        self.bytes.extend_from_slice(bytes);
        self
    }

    /// Append values encoded as [u16 little-endian length][UTF-8 bytes] per value.
    pub fn with_values(mut self, values: &[&str]) -> Self {
        for v in values {
            let v_bytes = v.as_bytes();
            let len = v_bytes.len() as u16;
            self.bytes.extend_from_slice(&len.to_le_bytes());
            self.bytes.extend_from_slice(v_bytes);
        }
        self
    }

    /// Finalize and create an `Arc<DecompressedBlock>` from the current bytes.
    pub fn create(self) -> Arc<DecompressedBlock> {
        Arc::new(DecompressedBlock::from_bytes(self.bytes))
    }

    /// Convenience: Build a block and matching (start,len) ranges for the provided values.
    /// Each value is encoded as [u16 length][bytes], and ranges point directly to the value bytes.
    pub fn create_with_ranges(values: &[&str]) -> (Arc<DecompressedBlock>, Vec<(usize, usize)>) {
        let mut bytes: Vec<u8> = Vec::new();
        let mut ranges: Vec<(usize, usize)> = Vec::new();
        for v in values {
            let v_bytes = v.as_bytes();
            let len = v_bytes.len() as u16;
            let start = bytes.len();
            bytes.extend_from_slice(&len.to_le_bytes());
            bytes.extend_from_slice(v_bytes);
            ranges.push((start + 2, v_bytes.len()));
        }
        (Arc::new(DecompressedBlock::from_bytes(bytes)), ranges)
    }
}
