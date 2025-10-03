use std::sync::{Arc, OnceLock};

use crate::engine::core::read::cache::DecompressedBlock;

/// Zero-copy view over values stored inside a decompressed column block.
#[derive(Clone, Debug)]
pub struct ColumnValues {
    pub block: Arc<DecompressedBlock>,
    pub ranges: Vec<(usize, usize)>, // (start, len)
    // Lazily constructed cache of parsed i64 values; None for non-integers.
    numeric_cache: Arc<OnceLock<Arc<Vec<Option<i64>>>>>,
}

impl ColumnValues {
    pub fn new(block: Arc<DecompressedBlock>, ranges: Vec<(usize, usize)>) -> Self {
        Self {
            block,
            ranges,
            numeric_cache: Arc::new(OnceLock::new()),
        }
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
        let cache = self.numeric_cache.get_or_init(|| {
            let mut parsed: Vec<Option<i64>> = Vec::with_capacity(self.ranges.len());
            for i in 0..self.ranges.len() {
                let v = self.get_str_at(i).and_then(|s| s.parse::<i64>().ok());
                parsed.push(v);
            }
            Arc::new(parsed)
        });
        cache.get(index)?.clone()
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
