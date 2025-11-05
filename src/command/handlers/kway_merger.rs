use super::row_comparator::RowComparator;
use crate::engine::types::ScalarValue;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Performs k-way merge of sorted result sets using indices (zero-copy).
///
/// Instead of cloning rows during the merge, this uses indices into the
/// source arrays, significantly reducing memory allocations.
pub struct KWayMerger<'a> {
    shard_results: &'a [Vec<Vec<ScalarValue>>],
    field: &'a str,
    ascending: bool,
    limit: usize,
}

/// Heap entry that references a row by index instead of cloning it.
struct HeapEntry<'a> {
    shard_idx: usize,
    row_idx: usize,
    shard_results: &'a [Vec<Vec<ScalarValue>>],
    field: &'a str,
    ascending: bool,
}

impl<'a> HeapEntry<'a> {
    fn get_row(&self) -> &Vec<ScalarValue> {
        &self.shard_results[self.shard_idx][self.row_idx]
    }
}

impl<'a> Eq for HeapEntry<'a> {}

impl<'a> PartialEq for HeapEntry<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.shard_idx == other.shard_idx
            && self.row_idx == other.row_idx
            && RowComparator::compare(self.get_row(), other.get_row(), self.field)
                == Ordering::Equal
    }
}

impl<'a> Ord for HeapEntry<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        let ord = RowComparator::compare(self.get_row(), other.get_row(), self.field);
        if self.ascending {
            ord.reverse() // min-heap behavior for ascending
        } else {
            ord // max-heap behavior for descending
        }
    }
}

impl<'a> PartialOrd for HeapEntry<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> KWayMerger<'a> {
    /// Creates a new k-way merger.
    ///
    /// # Arguments
    /// * `shard_results` - Reference to per-shard result sets (already sorted)
    /// * `field` - Field name to sort by
    /// * `ascending` - Sort direction
    /// * `limit` - Maximum number of results to return
    pub fn new(
        shard_results: &'a [Vec<Vec<ScalarValue>>],
        field: &'a str,
        ascending: bool,
        limit: usize,
    ) -> Self {
        Self {
            shard_results,
            field,
            ascending,
            limit,
        }
    }

    /// Performs the k-way merge and returns the merged results.
    ///
    /// Clones only the final selected rows, not all rows during processing.
    pub fn merge(&self) -> Vec<Vec<ScalarValue>> {
        let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();

        // Initialize heap with first row from each shard
        for (shard_idx, rows) in self.shard_results.iter().enumerate() {
            if !rows.is_empty() {
                heap.push(HeapEntry {
                    shard_idx,
                    row_idx: 0,
                    shard_results: self.shard_results,
                    field: self.field,
                    ascending: self.ascending,
                });
            }
        }

        let mut merged = Vec::with_capacity(self.limit.min(1000));

        // Extract rows in sorted order
        while merged.len() < self.limit {
            let Some(top) = heap.pop() else {
                break;
            };

            // Clone only the final selected row
            merged.push(top.get_row().clone());

            // Add next row from the same shard
            let next_idx = top.row_idx + 1;
            if next_idx < self.shard_results[top.shard_idx].len() {
                heap.push(HeapEntry {
                    shard_idx: top.shard_idx,
                    row_idx: next_idx,
                    shard_results: self.shard_results,
                    field: self.field,
                    ascending: self.ascending,
                });
            }
        }

        merged
    }

    /// Applies offset and limit to the merged results.
    pub fn apply_pagination(
        rows: Vec<Vec<ScalarValue>>,
        offset: Option<u32>,
        limit: Option<u32>,
    ) -> Vec<Vec<ScalarValue>> {
        let mut result = rows;

        // Apply offset
        if let Some(off) = offset {
            let n = off as usize;
            if n >= result.len() {
                result.clear();
            } else {
                result.drain(0..n);
            }
        }

        // Apply limit
        if let Some(lim) = limit {
            let lim = lim as usize;
            if result.len() > lim {
                result.truncate(lim);
            }
        }

        result
    }
}
