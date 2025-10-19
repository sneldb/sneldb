use super::merge_plan::MergePlan;
use super::policy::{CompactionPolicy, KWayCountPolicy};
use crate::engine::core::segment::segment_id::SegmentId;
use crate::engine::core::{Compactor, SegmentEntry, SegmentIndex};
use crate::engine::errors::CompactorError;
use crate::engine::schema::SchemaRegistry;
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

        // Step 2: Plan compaction using policy (k-way per uid)
        let policy = KWayCountPolicy::default();
        let plans: Vec<MergePlan> = policy.plan(&segment_index);
        if plans.is_empty() {
            info!(target: "compaction_worker::run", shard = self.shard_id, "No compaction plans generated");
            return Ok(());
        }

        // Step 3: Execute plans serially
        for plan in plans {
            let label = SegmentId::from(plan.output_segment_id).dir_name();
            let output_dir = self.shard_dir.join(&label);
            info!(target: "compaction_worker::run", shard = self.shard_id, uid = %plan.uid, new_label = %label, inputs = ?plan.input_segment_labels, "Compacting segments by policy");

            let compactor = Compactor::new(
                plan.uid.clone(),
                plan.input_segment_labels.clone(),
                plan.output_segment_id as u64,
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
                id: plan.output_segment_id as u32,
                uids: vec![plan.uid.clone()],
            };
            segment_index
                .append(new_entry)
                .await
                .map_err(|e| CompactorError::SegmentIndex(e.to_string()))?;

            // Remove old entries
            let before_count = segment_index.entries.len();
            segment_index
                .entries
                .retain(|e| !plan.input_segment_labels.contains(&format!("{:05}", e.id)));
            let after_count = segment_index.entries.len();
            debug!(target: "compaction_worker::run", removed = (before_count - after_count), "Removed old segment entries");
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
