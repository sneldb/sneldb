use super::segment_batch::{SegmentBatch, UidPlan};
use crate::engine::core::segment::segment_id::SegmentId;
use crate::engine::core::zone::zone_batch_sizer::ZoneBatchSizer;
use crate::engine::core::zone::zone_cursor_loader::LoadedZoneCursors;
use crate::engine::core::{ZoneCursorLoader, ZoneMerger, ZonePlan, ZoneWriter};
use crate::engine::errors::CompactorError;
use crate::engine::schema::SchemaRegistry;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Compacts multiple UIDs from the same input segments in a single pass.
/// This is more efficient than processing each UID separately, as it:
/// - Opens segment directories once
/// - Processes all UIDs from shared segments together
/// - Writes all outputs atomically
pub struct MultiUidCompactor {
    batch: SegmentBatch,
    input_dir: PathBuf,
    shard_dir: PathBuf,
    registry: Arc<RwLock<SchemaRegistry>>,
}

impl MultiUidCompactor {
    pub fn new(
        batch: SegmentBatch,
        input_dir: PathBuf,
        shard_dir: PathBuf,
        registry: Arc<RwLock<SchemaRegistry>>,
    ) -> Self {
        Self {
            batch,
            input_dir,
            shard_dir,
            registry,
        }
    }

    /// Processes all UIDs in the batch, compacting them from the shared input segments.
    /// All UIDs are written to the same output segment directory.
    /// Returns a map of UID to its compaction result.
    pub async fn run(&self) -> Result<HashMap<String, CompactionResult>, CompactorError> {
        if self.batch.uid_plans.is_empty() {
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(
                    target: "multi_uid_compactor::run",
                    "Batch has no UID plans, returning empty results"
                );
            }
            return Ok(HashMap::new());
        }

        info!(
            target: "multi_uid_compactor::run",
            input_segments = ?self.batch.input_segment_labels,
            uid_count = self.batch.uid_count(),
            uids = ?self.batch.uids(),
            "Starting multi-UID compaction"
        );

        // Use the first output segment ID for all UIDs in the batch
        // All UIDs from the same input segments should go into one output segment
        let shared_output_segment_id = self.batch.uid_plans[0].output_segment_id;
        let output_label = SegmentId::from(shared_output_segment_id).dir_name();
        let output_dir = self.shard_dir.join(&output_label);

        // Ensure output directory exists once for all UIDs
        std::fs::create_dir_all(&output_dir)
            .map_err(|e| CompactorError::ZoneWriter(e.to_string()))?;

        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "multi_uid_compactor::run",
                shared_output_segment_id = shared_output_segment_id,
                output_dir = %output_dir.display(),
                "Using shared output segment for all UIDs in batch"
            );
        }

        let mut results = HashMap::new();

        // Process each UID separately (they have different schemas)
        // But write them all to the same output segment directory
        for (uid_idx, uid_plan) in self.batch.uid_plans.iter().enumerate() {
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(
                    target: "multi_uid_compactor::run",
                    uid_index = uid_idx,
                    total_uids = self.batch.uid_count(),
                    uid = %uid_plan.uid,
                    shared_output_segment_id = shared_output_segment_id,
                    input_segments = ?self.batch.input_segment_labels,
                    "Compacting UID to shared output segment"
                );
            }
            let result = self
                .compact_uid(uid_plan, shared_output_segment_id, &output_dir)
                .await
                .map_err(|e| CompactorError::ZoneWriter(format!("UID {}: {}", uid_plan.uid, e)))?;
            results.insert(uid_plan.uid.clone(), result);
        }

        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "multi_uid_compactor::run",
                input_segments = ?self.batch.input_segment_labels,
                shared_output_segment_id = shared_output_segment_id,
                processed_uids = results.len(),
                results = ?results.iter().map(|(uid, r)| (uid.clone(), r.zones_written)).collect::<HashMap<_, _>>(),
                "Completed multi-UID compaction for batch"
            );
        }

        Ok(results)
    }

    /// Compacts a single UID from the shared input segments into the shared output segment
    async fn compact_uid(
        &self,
        uid_plan: &UidPlan,
        output_segment_id: u32,
        output_dir: &std::path::Path,
    ) -> Result<CompactionResult, CompactorError> {
        debug!(
            target: "multi_uid_compactor::compact_uid",
            uid = %uid_plan.uid,
            output_segment_id = output_segment_id,
            input_segments = ?self.batch.input_segment_labels,
            "Compacting UID to shared output segment"
        );

        // Load zone cursors for this UID
        let loader = ZoneCursorLoader::new(
            uid_plan.uid.clone(),
            self.batch.input_segment_labels.clone(),
            Arc::clone(&self.registry),
            self.input_dir.clone(),
        );
        let LoadedZoneCursors {
            cursors,
            type_catalog,
        } = loader
            .load_all()
            .await
            .map_err(|e| CompactorError::ZoneCursorLoad(e.to_string()))?;

        if cursors.is_empty() {
            debug!(
                target: "multi_uid_compactor::compact_uid",
                uid = %uid_plan.uid,
                "No cursors found for UID, skipping"
            );
            return Ok(CompactionResult {
                output_segment_id,
                zones_written: 0,
            });
        }

        info!(
            target: "multi_uid_compactor::compact_uid",
            uid = %uid_plan.uid,
            cursor_count = cursors.len(),
            "Loaded zone cursors"
        );

        // Merge zones into plans
        let mut merger = ZoneMerger::new(cursors);
        let mut zone_plans = Vec::new();
        let mut zone_id = 0;
        let level = SegmentId::from(output_segment_id as u32).level();
        let target_rows = ZoneBatchSizer::target_rows(level);

        while let Some((batch, max_created_at)) = merger.next_zone(target_rows) {
            let plan = ZonePlan::from_rows(
                batch,
                uid_plan.uid.clone(),
                output_segment_id as u64,
                zone_id,
                max_created_at,
            )
            .map_err(|e| CompactorError::ZoneWriter(e.to_string()))?;
            zone_plans.push(plan);
            zone_id += 1;
        }

        if zone_plans.is_empty() {
            debug!(
                target: "multi_uid_compactor::compact_uid",
                uid = %uid_plan.uid,
                "No zones to write, skipping output creation"
            );
            return Ok(CompactionResult {
                output_segment_id,
                zones_written: 0,
            });
        }

        // Write merged zones to the shared output directory
        let writer = ZoneWriter::new(&uid_plan.uid, &output_dir, Arc::clone(&self.registry))
            .with_type_catalog(type_catalog);
        writer
            .write_all(&zone_plans)
            .await
            .map_err(|e| CompactorError::ZoneWriter(e.to_string()))?;

        info!(
            target: "multi_uid_compactor::compact_uid",
            uid = %uid_plan.uid,
            output_segment_id = output_segment_id,
            zones_written = zone_plans.len(),
            "Wrote compacted zones to shared output segment"
        );

        Ok(CompactionResult {
            output_segment_id,
            zones_written: zone_plans.len(),
        })
    }
}

/// Result of compacting a single UID
#[derive(Debug, Clone)]
pub struct CompactionResult {
    pub output_segment_id: u32,
    pub zones_written: usize,
}
