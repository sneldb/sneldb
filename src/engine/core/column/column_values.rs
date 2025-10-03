use std::sync::Arc;

use crate::engine::core::read::cache::DecompressedBlock;

/// Zero-copy view over values stored inside a decompressed column block.
#[derive(Clone, Debug)]
pub struct ColumnValues {
    pub block: Arc<DecompressedBlock>,
    value_spans: Vec<(usize, usize)>, // (start, len)
}

impl ColumnValues {
    pub fn new(block: Arc<DecompressedBlock>, ranges: Vec<(usize, usize)>) -> Self {
        Self {
            block,
            value_spans: ranges,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.value_spans.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.value_spans.is_empty()
    }

    #[inline]
    pub fn get_str_at(&self, index: usize) -> Option<&str> {
        let (start, len) = *self.value_spans.get(index)?;
        let bytes = &self.block.as_bytes()[start..start + len];
        // Values are UTF-8 encoded when written; validate at read time
        std::str::from_utf8(bytes).ok()
    }

    #[inline]
    pub fn get_i64_at(&self, index: usize) -> Option<i64> {
        self.get_str_at(index)?.parse::<i64>().ok()
    }

    #[inline]
    pub fn iter(&self) -> ColumnValuesIter<'_> {
        ColumnValuesIter {
            col: self,
            index: 0,
        }
    }
}

impl PartialEq for ColumnValues {
    fn eq(&self, other: &Self) -> bool {
        // Consider equality based on ranges only (content layout). The backing
        // block identity is ignored for equality semantics in tests/containers.
        self.value_spans == other.value_spans
    }
}

impl Eq for ColumnValues {}

pub struct ColumnValuesIter<'a> {
    col: &'a ColumnValues,
    index: usize,
}

impl<'a> Iterator for ColumnValuesIter<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.index;
        self.index += 1;
        // Safe to tie lifetime to self.col
        self.col.get_str_at(i)
    }
}
