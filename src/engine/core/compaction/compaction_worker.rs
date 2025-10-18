use crate::engine::core::segment::range_allocator::RangeAllocator;
use crate::engine::core::segment::segment_id::SegmentId;
use crate::engine::core::{Compactor, SegmentEntry, SegmentIndex};
use crate::engine::errors::CompactorError;
use crate::engine::schema::SchemaRegistry;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

impl From<CompactorError> for std::io::Error {
    fn from(err: CompactorError) -> Self {
        std::io::Error::new(std::io::ErrorKind::Other, err.to_string())
    }
}

pub struct CompactionWorker {
    pub shard_id: u32,
    pub shard_dir: PathBuf,
    pub registry: Arc<RwLock<SchemaRegistry>>,
}

impl CompactionWorker {
    pub fn new(shard_id: u32, shard_dir: PathBuf, registry: Arc<RwLock<SchemaRegistry>>) -> Self {
        Self {
            shard_id,
            shard_dir,
            registry,
        }
    }

    pub async fn run(&self) -> Result<(), CompactorError> {
        // Step 1: Load segment index
        let mut segment_index = SegmentIndex::load(&self.shard_dir)
            .await
            .map_err(|e| CompactorError::SegmentIndex(e.to_string()))?;
        info!(target: "compaction_worker::run", shard = self.shard_id, entries = segment_index.entries.len(), "Loaded segment index");

        // Step 2: Group L0 segments
        let l0_segments: Vec<_> = segment_index
            .entries
            .iter()
            .filter(|e| e.id < 10_000)
            .cloned()
            .collect();
        debug!(target: "compaction_worker::run", shard = self.shard_id, segments = ?l0_segments, "Collected L0 segments");

        if l0_segments.len() < 2 {
            info!(target: "compaction_worker::run", shard = self.shard_id, "Not enough L0 segments for compaction");
            return Ok(());
        }

        // Step 3: Group segments by UID
        let mut uid_to_segments: HashMap<String, Vec<String>> = HashMap::new();
        for entry in &l0_segments {
            for uid in &entry.uids {
                uid_to_segments
                    .entry(uid.clone())
                    .or_default()
                    .push(format!("{:05}", entry.id));
            }
        }
        debug!(target: "compaction_worker::run", shard = self.shard_id, uid_map = ?uid_to_segments, "Mapped UIDs to segments");

        // Step 4: Seed allocator from existing ids and prepare L1 allocation
        let existing_labels = segment_index.all_labels();
        let mut allocator =
            RangeAllocator::from_existing_ids(existing_labels.iter().map(|s| s.as_str()));

        // Step 5: Iterate over UID groups for compaction
        for (uid, segment_ids) in uid_to_segments {
            if segment_ids.len() < 2 {
                continue;
            }

            let new_id = allocator.next_for_level(1);
            let label = SegmentId::from(new_id).dir_name();
            let output_dir = self.shard_dir.join(&label);
            info!(target: "compaction_worker::run", shard = self.shard_id, uid = %uid, new_label = %label, "Compacting segments");

            // Run compaction for this UID
            let compactor = Compactor::new(
                uid.clone(),
                segment_ids.clone(),
                new_id as u64,
                self.shard_dir.clone(),
                output_dir.clone(),
                Arc::clone(&self.registry),
            );

            compactor
                .run()
                .await
                .map_err(|e| CompactorError::ZoneWriter(e.to_string()))?;

            // Add new entry to index
            let new_entry = SegmentEntry {
                id: new_id as u32,
                uids: vec![uid.clone()],
            };
            segment_index
                .append(new_entry)
                .await
                .map_err(|e| CompactorError::SegmentIndex(e.to_string()))?;

            // Remove old entries
            let before_count = segment_index.entries.len();
            segment_index
                .entries
                .retain(|e| !segment_ids.contains(&format!("{:05}", e.id)));
            let after_count = segment_index.entries.len();
            debug!(target: "compaction_worker::run", removed = (before_count - after_count), "Removed old segment entries");

            // allocator state already advanced
        }

        // Step 6: Save final segment index
        segment_index
            .save(&self.shard_dir)
            .await
            .map_err(|e| CompactorError::SegmentIndex(e.to_string()))?;
        info!(target: "compaction_worker::run", shard = self.shard_id, total_entries = segment_index.entries.len(), "Compaction complete");

        Ok(())
    }
}
