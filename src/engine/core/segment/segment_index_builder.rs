use crate::engine::core::{SegmentEntry, SegmentIndex};
use crate::engine::errors::StoreError;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

/// Builder for adding new segment entries to the segment index.
pub struct SegmentIndexBuilder<'a> {
    pub segment_id: u64,
    pub segment_dir: &'a Path,
    pub event_type_uids: Vec<String>,
    pub flush_coordination_lock: Arc<Mutex<()>>,
}

impl<'a> SegmentIndexBuilder<'a> {
    /// Adds a new segment entry to the index, saving it to disk.
    ///
    /// This method acquires a per-shard lock to serialize index updates,
    /// preventing race conditions when multiple segments flush concurrently.
    pub async fn add_segment_entry(&self, shard_dir: Option<&Path>) -> Result<(), StoreError> {
        // Build new segment entry (numeric id)
        let entry = SegmentEntry {
            id: self.segment_id as u32,
            uids: self.event_type_uids.clone(),
        };

        info!(target: "segment_index_builder::add_segment_entry", segment_id = self.segment_id, uids = ?self.event_type_uids, "Adding new segment entry");

        // Determine shard directory
        let shard_dir = shard_dir.unwrap_or_else(|| {
            self.segment_dir
                .parent()
                .expect("Segment directory should have a parent")
        });

        // 🔒 Acquire lock to serialize segment index updates (prevents race conditions)
        let _guard = self.flush_coordination_lock.lock().await;
        debug!(target: "segment_index_builder::add_segment_entry", ?shard_dir, "Acquired flush coordination lock");

        // Load segment index
        let mut segment_index = match SegmentIndex::load(shard_dir).await {
            Ok(idx) => {
                debug!(target: "segment_index_builder::add_segment_entry", ?shard_dir, count = idx.len(), "Loaded existing segment index");
                idx
            }
            Err(e) => {
                error!(target: "segment_index_builder::add_segment_entry", ?shard_dir, "Failed to load segment index: {}", e);
                return Err(StoreError::FlushFailed(format!(
                    "Failed to load SegmentIndex: {}",
                    e
                )));
            }
        };

        // Append entry
        segment_index.insert_entry(entry);
        debug!(target: "segment_index_builder::add_segment_entry", "Inserted new segment entry");

        // Save updated index
        if let Err(e) = segment_index.save(shard_dir).await {
            error!(target: "segment_index_builder::add_segment_entry", ?shard_dir, "Failed to save segment index: {}", e);
            return Err(StoreError::FlushFailed(format!(
                "Failed to save SegmentIndex: {}",
                e
            )));
        }

        info!(target: "segment_index_builder::add_segment_entry", ?shard_dir, "Successfully added segment entry");
        // Lock automatically released when _guard drops
        Ok(())
    }
}
