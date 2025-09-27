use crate::engine::core::ColumnWriter;
use crate::engine::core::FieldXorFilter;
use crate::engine::core::filter::zone_surf_filter::ZoneSurfFilter;
use crate::engine::core::zone::enum_bitmap_index::EnumBitmapBuilder;
use crate::engine::core::zone::zone_xor_index::build_all_zxf;
use crate::engine::core::{ZoneIndex, ZoneMeta, ZonePlan};
use crate::engine::errors::StoreError;
use crate::engine::schema::registry::SchemaRegistry;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Writes all zone-related files for a given event type (uid)
pub struct ZoneWriter<'a> {
    pub uid: &'a str,
    pub segment_dir: &'a Path,
    pub registry: Arc<RwLock<SchemaRegistry>>,
}

impl<'a> ZoneWriter<'a> {
    pub fn new(uid: &'a str, segment_dir: &'a Path, registry: Arc<RwLock<SchemaRegistry>>) -> Self {
        Self {
            uid,
            segment_dir,
            registry,
        }
    }

    pub async fn write_all(&self, zone_plans: &[ZonePlan]) -> Result<(), StoreError> {
        info!(
            target: "sneldb::flush",
            uid = self.uid,
            segment_dir = %self.segment_dir.display(),
            zone_count = zone_plans.len(),
            "Starting zone file write"
        );

        // Write .zones metadata
        let zone_meta = ZoneMeta::build_all(zone_plans);
        debug!(
            target: "sneldb::flush",
            uid = self.uid,
            "Writing .zones metadata"
        );
        ZoneMeta::save(self.uid, &zone_meta, self.segment_dir)?;

        // Write .col files
        let writer = ColumnWriter::new(self.segment_dir.to_path_buf(), self.registry.clone());
        debug!(
            target: "sneldb::flush",
            uid = self.uid,
            "Writing .col files"
        );
        writer.write_all(zone_plans).await?;

        // Build XOR filters
        debug!(
            target: "sneldb::flush",
            uid = self.uid,
            "Building XOR filters"
        );
        FieldXorFilter::build_all(zone_plans, self.segment_dir)
            .map_err(|e| StoreError::FlushFailed(format!("Failed to build XOR filters: {}", e)))?;

        // Build per-zone XOR index (.zxf)
        debug!(
            target: "sneldb::flush",
            uid = self.uid,
            "Building zone XOR filters (.zxf)"
        );
        if let Err(e) = build_all_zxf(zone_plans, self.segment_dir) {
            debug!(target: "sneldb::flush", uid = self.uid, error = %e, "Skipping .zxf due to error");
        }
        
        // Build Zone-level SuRF filters (best-effort)
        debug!(
            target: "sneldb::flush",
            uid = self.uid,
            "Building Zone-level SuRF filters"
        );
        if let Err(e) = ZoneSurfFilter::build_all(zone_plans, self.segment_dir) {
            debug!(target: "sneldb::flush", uid = self.uid, error = %e, "Skipping Zone SuRF due to error");
        }

        // Build Enum Bitmap Indexes for enum fields (best-effort)
        debug!(
            target: "sneldb::flush",
            uid = self.uid,
            "Building enum bitmap indexes"
        );
        if let Err(e) =
            EnumBitmapBuilder::build_all(zone_plans, self.segment_dir, &self.registry).await
        {
            debug!(target: "sneldb::flush", uid = self.uid, error = %e, "Skipping EBM due to error");
        }

        // Build and write index
        debug!(
            target: "sneldb::flush",
            uid = self.uid,
            "Building zone index"
        );
        let mut index = ZoneIndex::default();
        for zp in zone_plans {
            for ev in &zp.events {
                index.insert(&zp.event_type, &ev.context_id, zp.id);
            }
        }

        let index_path = self.segment_dir.join(format!("{}.idx", self.uid));
        debug!(
            target: "sneldb::flush",
            uid = self.uid,
            path = %index_path.display(),
            "Writing zone index"
        );
        index.write_to_path(index_path)?;

        info!(
            target: "sneldb::flush",
            uid = self.uid,
            "Zone write completed"
        );

        Ok(())
    }
}
