use super::handover::CompactionHandover;
use super::merge_plan::MergePlan;
use super::multi_uid_compactor::MultiUidCompactor;
use super::policy::{CompactionPolicy, KWayCountPolicy};
use super::segment_batch::SegmentBatch;
use crate::engine::core::{SegmentEntry, SegmentIndex};
use crate::engine::errors::CompactorError;
use crate::engine::schema::SchemaRegistry;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

impl From<CompactorError> for std::io::Error {
    fn from(err: CompactorError) -> Self {
        std::io::Error::new(std::io::ErrorKind::Other, err.to_string())
    }
}

/// Coordinates compaction work for a shard.
/// Processes compaction plans efficiently by batching UIDs that share input segments.
pub struct CompactionWorker {
    shard_id: u32,
    shard_dir: PathBuf,
    registry: Arc<RwLock<SchemaRegistry>>,
    handover: Arc<CompactionHandover>,
}

impl CompactionWorker {
    pub fn new(
        shard_id: u32,
        shard_dir: PathBuf,
        registry: Arc<RwLock<SchemaRegistry>>,
        handover: Arc<CompactionHandover>,
    ) -> Self {
        Self {
            shard_id,
            shard_dir,
            registry,
            handover,
        }
    }

    /// Runs compaction for the shard.
    /// Groups plans by input segments and processes all UIDs from shared segments together.
    pub async fn run(&self) -> Result<(), CompactorError> {
        let segment_index = SegmentIndex::load(&self.shard_dir)
            .await
            .map_err(|e| CompactorError::SegmentIndex(e.to_string()))?;
        info!(
            target: "compaction_worker::run",
            shard = self.shard_id,
            entries = segment_index.len(),
            "Loaded segment index"
        );

        let policy = KWayCountPolicy::default();
        let plans: Vec<MergePlan> = policy.plan(&segment_index);
        if plans.is_empty() {
            info!(
                target: "compaction_worker::run",
                shard = self.shard_id,
                "No compaction plans generated"
            );
            return Ok(());
        }

        info!(
            target: "compaction_worker::run",
            shard = self.shard_id,
            plan_count = plans.len(),
            "Starting compaction execution"
        );

        // Group plans by input segments for efficient processing
        let total_plans = plans.len();
        let batches = SegmentBatch::group_plans(plans);
        warn!(
            target: "compaction_worker::run",
            shard = self.shard_id,
            batch_count = batches.len(),
            total_plans = total_plans,
            batches = ?batches.iter().map(|b| {
                (b.input_segment_labels.clone(), b.uid_plans.iter().map(|p| p.uid.clone()).collect::<Vec<_>>())
            }).collect::<Vec<_>>(),
            "Grouped plans into batches for multi-UID compaction"
        );

        let mut all_drained_segments = Vec::new();

        // Process each batch (all UIDs from shared segments)
        for (batch_idx, batch) in batches.into_iter().enumerate() {
            warn!(
                target: "compaction_worker::run",
                shard = self.shard_id,
                batch_index = batch_idx,
                input_segments = ?batch.input_segment_labels,
                uid_count = batch.uid_count(),
                uids = ?batch.uid_plans.iter().map(|p| p.uid.clone()).collect::<Vec<_>>(),
                "Starting batch processing"
            );
            let drained = self.process_batch(batch).await?;
            all_drained_segments.extend(drained);
        }

        // Delete all drained segments after all batches complete
        if !all_drained_segments.is_empty() {
            warn!(
                target: "compaction_worker::run",
                shard = self.shard_id,
                total_drained = all_drained_segments.len(),
                drained_segments = ?all_drained_segments,
                "All batches complete, scheduling deletion of drained segments"
            );
            self.handover.schedule_reclaim(all_drained_segments);
        } else {
            warn!(
                target: "compaction_worker::run",
                shard = self.shard_id,
                "All batches complete, no segments drained (all segments still have remaining UIDs)"
            );
        }

        let final_index = SegmentIndex::load(&self.shard_dir)
            .await
            .map_err(|e| CompactorError::SegmentIndex(e.to_string()))?;
        info!(
            target: "compaction_worker::run",
            shard = self.shard_id,
            total_entries = final_index.len(),
            "Compaction complete"
        );

        Ok(())
    }

    /// Processes a batch of UIDs from shared input segments.
    /// Returns drained segment labels that can be deleted.
    async fn process_batch(&self, batch: SegmentBatch) -> Result<Vec<String>, CompactorError> {
        warn!(
            target: "compaction_worker::process_batch",
            shard = self.shard_id,
            input_segments = ?batch.input_segment_labels,
            uid_count = batch.uid_count(),
            uids = ?batch.uid_plans.iter().map(|p| p.uid.clone()).collect::<Vec<_>>(),
            output_segments = ?batch.uid_plans.iter().map(|p| p.output_segment_id).collect::<Vec<_>>(),
            "Processing batch: compacting multiple UIDs from shared segments"
        );

        // Compact all UIDs from the shared segments
        let compactor = MultiUidCompactor::new(
            batch.clone(),
            self.shard_dir.clone(),
            self.shard_dir.clone(),
            Arc::clone(&self.registry),
        );
        let results = compactor
            .run()
            .await
            .map_err(|e| CompactorError::ZoneWriter(e.to_string()))?;

        // Prepare new entries for handover
        // When multiple UIDs are compacted from the same input segments,
        // they should all go into ONE output segment (not separate segments per UID)
        // Use the first output segment ID for all UIDs in the batch
        let shared_output_segment_id = batch.uid_plans[0].output_segment_id;
        let all_uids: Vec<String> = batch.uid_plans.iter().map(|p| p.uid.clone()).collect();

        warn!(
            target: "compaction_worker::process_batch",
            shard = self.shard_id,
            shared_output_segment_id = shared_output_segment_id,
            all_uids = ?all_uids,
            "Using single output segment for all UIDs in batch"
        );

        // Create a single SegmentEntry with ALL UIDs for the shared output segment
        let new_entries = vec![SegmentEntry {
            id: shared_output_segment_id,
            uids: all_uids.clone(),
        }];

        // Commit batch handover
        let drained = self
            .handover
            .commit_batch(&batch, new_entries)
            .await
            .map_err(|e| CompactorError::SegmentIndex(e.to_string()))?;

        warn!(
            target: "compaction_worker::process_batch",
            shard = self.shard_id,
            input_segments = ?batch.input_segment_labels,
            processed_uids = results.len(),
            drained_count = drained.len(),
            drained_segments = ?drained,
            shared_output_segment_id = shared_output_segment_id,
            all_uids_in_output = ?all_uids,
            "Completed batch processing"
        );

        Ok(drained)
    }
}
