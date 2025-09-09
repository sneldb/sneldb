use crate::engine::core::{ZoneCursor, ZoneRow};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use tracing::{debug, trace};

/// A struct that merges multiple zone cursors into a single sorted stream of rows.
/// Uses a binary heap to efficiently merge rows from multiple cursors based on their context IDs.
pub struct ZoneMerger {
    cursors: Vec<ZoneCursor>,
    heap: BinaryHeap<Reverse<HeapItem>>,
}

struct HeapItem {
    context_id: String,
    cursor_index: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.context_id == other.context_id
    }
}
impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.context_id.cmp(&other.context_id))
    }
}
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.context_id.cmp(&other.context_id)
    }
}

impl ZoneMerger {
    /// Creates a new ZoneMerger from a vector of zone cursors.
    /// Initializes the binary heap with the first context ID from each cursor.
    pub fn new(mut cursors: Vec<ZoneCursor>) -> Self {
        let mut heap = BinaryHeap::new();

        for (i, cursor) in cursors.iter_mut().enumerate() {
            if let Some(ctx_id) = cursor.peek_context_id() {
                trace!(
                    target: "sneldb::query",
                    cursor_index = i,
                    context_id = ctx_id,
                    "Initializing heap with first context_id"
                );
                heap.push(Reverse(HeapItem {
                    context_id: ctx_id.to_string(),
                    cursor_index: i,
                }));
            }
        }

        debug!(
            target: "sneldb::query",
            total_cursors = cursors.len(),
            initialized_entries = heap.len(),
            "ZoneMerger initialized"
        );

        Self { cursors, heap }
    }

    /// Returns the next row from the merged stream of zone cursors.
    /// The rows are returned in order based on their context IDs.
    /// Returns None when all cursors have been exhausted.
    pub fn next_row(&mut self) -> Option<ZoneRow> {
        if let Some(Reverse(top)) = self.heap.pop() {
            let cursor = &mut self.cursors[top.cursor_index];
            let row = cursor.next_row();

            trace!(
                target: "sneldb::query",
                cursor_index = top.cursor_index,
                context_id = top.context_id,
                "Returning next row from cursor"
            );

            if let Some(next_ctx) = cursor.peek_context_id() {
                self.heap.push(Reverse(HeapItem {
                    context_id: next_ctx.to_string(),
                    cursor_index: top.cursor_index,
                }));
                trace!(
                    target: "sneldb::query",
                    cursor_index = top.cursor_index,
                    context_id = next_ctx,
                    "Pushed next context_id to heap"
                );
            }

            row
        } else {
            trace!(
                target: "sneldb::query",
                "No more rows to merge, heap exhausted"
            );
            None
        }
    }

    /// Returns the next batch of rows from the merged stream, up to the specified maximum size.
    /// Returns None if there are no more rows to return.
    /// This is useful for processing rows in chunks rather than one at a time.
    pub fn next_zone(&mut self, max_rows: usize) -> Option<Vec<ZoneRow>> {
        let mut batch = Vec::with_capacity(max_rows);

        while batch.len() < max_rows {
            if let Some(row) = self.next_row() {
                batch.push(row);
            } else {
                break;
            }
        }

        if batch.is_empty() {
            trace!(
                target: "sneldb::query",
                "next_zone returned None (no rows available)"
            );
            None
        } else {
            debug!(
                target: "sneldb::query",
                batch_size = batch.len(),
                "next_zone returned batch"
            );
            Some(batch)
        }
    }
}
