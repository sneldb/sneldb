use std::sync::Arc;

use crate::engine::core::read::cache::DecompressedBlock;

/// Zero-copy view over values stored inside a decompressed column block.
#[derive(Clone, Debug)]
pub struct ColumnValues {
    pub block: Arc<DecompressedBlock>,
    pub ranges: Vec<(usize, usize)>, // (start, len)
}

impl ColumnValues {
    pub fn new(block: Arc<DecompressedBlock>, ranges: Vec<(usize, usize)>) -> Self {
        Self { block, ranges }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    #[inline]
    pub fn get_str_at(&self, index: usize) -> Option<&str> {
        let (start, len) = *self.ranges.get(index)?;
        let bytes = &self.block.bytes[start..start + len];
        // Values are UTF-8 encoded when written; validate at read time
        std::str::from_utf8(bytes).ok()
    }

    #[inline]
    pub fn get_i64_at(&self, index: usize) -> Option<i64> {
        self.get_str_at(index)?.parse::<i64>().ok()
    }
}

impl PartialEq for ColumnValues {
    fn eq(&self, other: &Self) -> bool {
        // Consider equality based on ranges only (content layout). The backing
        // block identity is ignored for equality semantics in tests/containers.
        self.ranges == other.ranges
    }
}

impl Eq for ColumnValues {}
