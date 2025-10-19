use crate::engine::core::ColumnWriter;
use crate::engine::core::FieldXorFilter;
use crate::engine::core::filter::zone_surf_filter::ZoneSurfFilter;
use crate::engine::core::time::{CalendarDir, ZoneTemporalIndex};
use crate::engine::core::zone::enum_bitmap_index::EnumBitmapBuilder;
use crate::engine::core::zone::rlte_index::RlteIndex;
use crate::engine::core::zone::zone_metadata_writer::ZoneMetadataWriter;
use crate::engine::core::zone::zone_xor_index::build_all_zxf;
use crate::engine::core::{ZoneIndex, ZonePlan};
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
        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::flush",
                uid = self.uid,
                segment_dir = %self.segment_dir.display(),
                zone_count = zone_plans.len(),
                "Starting zone file write"
            );
        }

        // Write .zones metadata (delegated)
        let metadata_writer = ZoneMetadataWriter::new(self.uid, self.segment_dir);
        metadata_writer.write(zone_plans)?;

        // Write .col files
        let writer = ColumnWriter::new(self.segment_dir.to_path_buf(), self.registry.clone());
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::flush",
                uid = self.uid,
                "Writing .col files"
            );
        }
        writer.write_all(zone_plans).await?;

        // Build TEF-CB (calendar + zone temporal index)
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::flush",
                uid = self.uid,
                "Building TEF-CB calendar and temporal indexes"
            );
        }
        // CalendarDir from ZoneMeta ranges
        let zones_meta_path = self.segment_dir.join(format!("{}.zones", self.uid));
        let metas = crate::engine::core::zone::zone_meta::ZoneMeta::load(&zones_meta_path)
            .map_err(|e| StoreError::FlushFailed(format!("Failed to load zone meta: {}", e)))?;
        let mut cal = CalendarDir::new();
        for m in &metas {
            cal.add_zone_range(m.zone_id, m.timestamp_min, m.timestamp_max);
        }
        cal.save(self.uid, self.segment_dir)
            .map_err(|e| StoreError::FlushFailed(format!("Failed to save calendar: {}", e)))?;

        // Build per-zone temporal index from timestamps (seconds)
        for zp in zone_plans {
            let mut ts: Vec<i64> = Vec::with_capacity(zp.events.len());
            for ev in &zp.events {
                ts.push(ev.timestamp as i64);
            }
            if ts.is_empty() {
                continue;
            }
            let zti = ZoneTemporalIndex::from_timestamps(ts, 1, 64);
            zti.save(self.uid, zp.id, self.segment_dir).map_err(|e| {
                StoreError::FlushFailed(format!("Failed to save temporal index: {}", e))
            })?;
        }

        // Build XOR filters
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::flush",
                uid = self.uid,
                "Building XOR filters"
            );
        }
        FieldXorFilter::build_all(zone_plans, self.segment_dir)
            .map_err(|e| StoreError::FlushFailed(format!("Failed to build XOR filters: {}", e)))?;

        // Build per-zone XOR index (.zxf)
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::flush",
                uid = self.uid,
                "Building zone XOR filters (.zxf)"
            );
        }
        if let Err(e) = build_all_zxf(zone_plans, self.segment_dir) {
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(target: "sneldb::flush", uid = self.uid, error = %e, "Skipping .zxf due to error");
            }
        }

        // Build Zone-level SuRF filters (best-effort)
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::flush",
                uid = self.uid,
                "Building Zone-level SuRF filters"
            );
        }
        let schema = self
            .registry
            .read()
            .await
            .get(&zone_plans[0].event_type)
            .cloned();
        if let Some(schema) = schema {
            if let Err(e) =
                ZoneSurfFilter::build_all_with_schema(zone_plans, self.segment_dir, &schema)
            {
                if tracing::enabled!(tracing::Level::DEBUG) {
                    debug!(target: "sneldb::flush", uid = self.uid, error = %e, "Skipping Zone SuRF (schema-aware) due to error");
                }
            }
        } else if let Err(e) = ZoneSurfFilter::build_all(zone_plans, self.segment_dir) {
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(target: "sneldb::flush", uid = self.uid, error = %e, "Skipping Zone SuRF due to error");
            }
        }

        // Build RLTE index (best-effort)
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::flush",
                uid = self.uid,
                "Building RLTE index"
            );
        }
        match std::panic::catch_unwind(|| RlteIndex::build_from_zones(zone_plans)) {
            Ok(rlte) => {
                if let Err(e) = rlte.save(self.uid, self.segment_dir) {
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        debug!(target: "sneldb::flush", uid = self.uid, error = %e, "Skipping RLTE due to IO error");
                    }
                }
            }
            Err(_) => {
                if tracing::enabled!(tracing::Level::DEBUG) {
                    debug!(target: "sneldb::flush", uid = self.uid, "Skipping RLTE due to build panic");
                }
            }
        }

        // Build Enum Bitmap Indexes for enum fields (best-effort)
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::flush",
                uid = self.uid,
                "Building enum bitmap indexes"
            );
        }
        if let Err(e) =
            EnumBitmapBuilder::build_all(zone_plans, self.segment_dir, &self.registry).await
        {
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(target: "sneldb::flush", uid = self.uid, error = %e, "Skipping EBM due to error");
            }
        }

        // Build and write index
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::flush",
                uid = self.uid,
                "Building zone index"
            );
        }
        let mut index = ZoneIndex::default();
        for zp in zone_plans {
            for ev in &zp.events {
                index.insert(&zp.event_type, &ev.context_id, zp.id);
            }
        }

        let index_path = self.segment_dir.join(format!("{}.idx", self.uid));
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::flush",
                uid = self.uid,
                path = %index_path.display(),
                "Writing zone index"
            );
        }
        index.write_to_path(index_path)?;

        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::flush",
                uid = self.uid,
                "Zone write completed"
            );
        }

        Ok(())
    }
}
