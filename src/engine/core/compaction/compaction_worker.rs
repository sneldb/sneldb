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
            .filter(|e| e.level == 0)
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
                    .push(entry.label.clone());
            }
        }
        debug!(target: "compaction_worker::run", shard = self.shard_id, uid_map = ?uid_to_segments, "Mapped UIDs to segments");

        // Step 4: Determine next segment ID
        let mut new_counter = segment_index
            .entries
            .iter()
            .map(|e| e.counter)
            .max()
            .unwrap_or(0)
            + 1;

        // Step 5: Iterate over UID groups for compaction
        for (uid, segment_ids) in uid_to_segments {
            if segment_ids.len() < 2 {
                continue;
            }

            let label = format!("segment-L1-{:05}", new_counter);
            let output_dir = self.shard_dir.join(&label);
            info!(target: "compaction_worker::run", shard = self.shard_id, uid = %uid, new_label = %label, "Compacting segments");

            // Run compaction for this UID
            let compactor = Compactor::new(
                uid.clone(),
                segment_ids.clone(),
                new_counter as u64,
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
                level: 1,
                label: label.clone(),
                counter: new_counter,
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
                .retain(|e| !segment_ids.contains(&e.label));
            let after_count = segment_index.entries.len();
            debug!(target: "compaction_worker::run", removed = (before_count - after_count), "Removed old segment entries");

            new_counter += 1;
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
