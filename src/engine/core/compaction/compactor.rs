use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::engine::core::segment::segment_id::SegmentId;
use crate::engine::core::zone::zone_batch_sizer::ZoneBatchSizer;
use crate::engine::core::zone::zone_cursor_loader::LoadedZoneCursors;
use crate::engine::core::{
    SegmentIndexBuilder, ZoneCursorLoader, ZoneMerger, ZonePlan, ZoneWriter,
};
use crate::engine::errors::CompactorError;
use crate::engine::schema::SchemaRegistry;
use tracing::{debug, info, warn};

pub struct Compactor {
    pub uid: String,
    pub segment_ids: Vec<String>,
    pub output_segment_id: u64,
    pub input_dir: PathBuf,
    pub output_dir: PathBuf,
    pub registry: Arc<RwLock<SchemaRegistry>>,
}

impl Compactor {
    pub fn new(
        uid: String,
        segment_ids: Vec<String>,
        output_segment_id: u64,
        input_dir: PathBuf,
        output_dir: PathBuf,
        registry: Arc<RwLock<SchemaRegistry>>,
    ) -> Self {
        Self {
            uid,
            segment_ids,
            output_segment_id,
            input_dir,
            output_dir,
            registry,
        }
    }

    /// Runs the compaction process: load zones, merge them, write new segments, and update index.
    pub async fn run(&self) -> Result<(), CompactorError> {
        // Step 1: Load zone cursors
        let loader = ZoneCursorLoader::new(
            self.uid.clone(),
            self.segment_ids.clone(),
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
        info!(target: "compactor::run", uid = %self.uid, count = cursors.len(), "Loaded zone cursors");

        // Step 2: Merge zones into plans, sizing by target level
        let mut merger = ZoneMerger::new(cursors);
        let mut zone_plans = Vec::new();
        let mut zone_id = 0;
        let level = SegmentId::from(self.output_segment_id as u32).level();
        let target_rows = ZoneBatchSizer::target_rows(level);
        while let Some(batch) = merger.next_zone(target_rows) {
            let plan =
                ZonePlan::from_rows(batch, self.uid.clone(), self.output_segment_id, zone_id)
                    .map_err(|e| CompactorError::ZoneWriter(e.to_string()))?;
            debug!(target: "compactor::run", zone_id, "Created zone plan");
            zone_plans.push(plan);
            zone_id += 1;
        }

        // Step 3: Ensure output directory exists
        info!(target: "compactor::run", dir = %self.output_dir.display(), "Creating output directory");
        std::fs::create_dir_all(&self.output_dir)
            .map_err(|e| CompactorError::ZoneWriter(e.to_string()))?;

        // Step 4: Write merged zones
        let writer = ZoneWriter::new(&self.uid, &self.output_dir, Arc::clone(&self.registry))
            .with_type_catalog(type_catalog);
        writer
            .write_all(&zone_plans)
            .await
            .map_err(|e| CompactorError::ZoneWriter(e.to_string()))?;
        info!(target: "compactor::run", zones_written = zone_plans.len(), "Wrote compacted zones");

        // Step 5: Update segment index
        // Note: Compaction uses its own lock since it's a separate process
        let compaction_lock = Arc::new(tokio::sync::Mutex::new(()));
        SegmentIndexBuilder {
            segment_id: self.output_segment_id,
            segment_dir: &self.output_dir,
            event_type_uids: vec![self.uid.clone()],
            flush_coordination_lock: compaction_lock,
        }
        .add_segment_entry(Some(&self.output_dir))
        .await
        .map_err(|e| CompactorError::SegmentIndex(e.to_string()))?;
        info!(target: "compactor::run", segment_id = self.output_segment_id, "Updated segment index");

        Ok(())
    }
}
