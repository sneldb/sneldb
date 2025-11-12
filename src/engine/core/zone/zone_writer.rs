use crate::engine::core::ColumnWriter;
use crate::engine::core::FieldXorFilter;
use crate::engine::core::column::type_catalog::ColumnTypeCatalog;
use crate::engine::core::filter::zone_surf_filter::ZoneSurfFilter;
use crate::engine::core::read::catalog::{IndexKind, SegmentIndexCatalog};
use crate::engine::core::time::{CalendarDir, TemporalIndexBuilder};
use crate::engine::core::zone::enum_bitmap_index::EnumBitmapBuilder;
use crate::engine::core::zone::index_build_planner::{BuildPlan, IndexBuildPlanner};
use crate::engine::core::zone::index_build_policy::IndexBuildPolicy;
use crate::engine::core::zone::rlte_index::RlteIndex;
use crate::engine::core::zone::zone_meta::ZoneMeta;
use crate::engine::core::zone::zone_metadata_writer::ZoneMetadataWriter;
use crate::engine::core::zone::zone_xor_index::build_all_zxf_filtered;
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
    type_catalog: Option<ColumnTypeCatalog>,
}

impl<'a> ZoneWriter<'a> {
    pub fn new(uid: &'a str, segment_dir: &'a Path, registry: Arc<RwLock<SchemaRegistry>>) -> Self {
        Self {
            uid,
            segment_dir,
            registry,
            type_catalog: None,
        }
    }

    pub fn with_type_catalog(mut self, catalog: ColumnTypeCatalog) -> Self {
        self.type_catalog = Some(catalog);
        self
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
        let mut writer = ColumnWriter::new(self.segment_dir.to_path_buf(), self.registry.clone());
        if let Some(catalog) = &self.type_catalog {
            writer = writer.with_type_hints(catalog.clone());
        }
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::flush",
                uid = self.uid,
                "Writing .col files"
            );
        }
        writer.write_all(zone_plans).await?;

        // Build plan: decide which indexes to build per field/global
        let schema = self
            .registry
            .read()
            .await
            .get(&zone_plans[0].event_type)
            .cloned();
        let mut build_plan: Option<BuildPlan> = None;
        let segment_id_str = zone_plans[0].segment_id.to_string();
        if let Some(schema_ref) = &schema {
            let planner = IndexBuildPlanner::new(
                self.uid,
                &segment_id_str,
                schema_ref,
                IndexBuildPolicy::default(),
            );
            build_plan = Some(planner.plan());
        }

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
        let metas = ZoneMeta::load(&zones_meta_path)
            .map_err(|e| StoreError::FlushFailed(format!("Failed to load zone meta: {}", e)))?;
        let mut cal = CalendarDir::new();
        for m in &metas {
            cal.add_zone_range(m.zone_id, m.timestamp_min, m.timestamp_max);
        }
        cal.save(self.uid, self.segment_dir)
            .map_err(|e| StoreError::FlushFailed(format!("Failed to save calendar: {}", e)))?;

        // Removed legacy per-zone event timestamp .tfi build (redundant with per-field temporal indexes)

        // Build per-field temporal artifacts (first-class): {uid}_{field}.cal and {uid}_{field}_{zone}.tfi
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::flush",
                uid = self.uid,
                "Building per-field temporal calendars and indexes"
            );
        }
        TemporalIndexBuilder::new(self.uid, self.segment_dir, self.registry.clone())
            .build_for_zone_plans(zone_plans)
            .await?;

        // Build XOR filters
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::flush",
                uid = self.uid,
                "Building XOR filters"
            );
        }
        if let Some(plan) = &build_plan {
            use std::collections::HashSet;
            let allowed: HashSet<String> = plan
                .per_field
                .iter()
                .filter_map(|(f, k)| {
                    if k.contains(IndexKind::XOR_FIELD_FILTER) {
                        Some(f.clone())
                    } else {
                        None
                    }
                })
                .collect();
            FieldXorFilter::build_all_filtered(zone_plans, self.segment_dir, &allowed).map_err(
                |e| StoreError::FlushFailed(format!("Failed to build XOR filters: {}", e)),
            )?;
        }

        // Build per-zone XOR index (.zxf)
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::flush",
                uid = self.uid,
                "Building zone XOR filters (.zxf)"
            );
        }
        if let Some(plan) = &build_plan {
            use std::collections::HashSet;
            let allowed: HashSet<String> = plan
                .per_field
                .iter()
                .filter_map(|(f, k)| {
                    if k.contains(IndexKind::ZONE_XOR_INDEX) {
                        Some(f.clone())
                    } else {
                        None
                    }
                })
                .collect();
            if let Err(e) = build_all_zxf_filtered(zone_plans, self.segment_dir, &allowed) {
                if tracing::enabled!(tracing::Level::DEBUG) {
                    debug!(target: "sneldb::flush", uid = self.uid, error = %e, "Skipping .zxf due to error");
                }
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
        if let Some(_schema_val) = schema.clone() {
            if let Some(plan) = &build_plan {
                use std::collections::HashSet;
                let allowed: HashSet<String> = plan
                    .per_field
                    .iter()
                    .filter_map(|(f, k)| {
                        if k.contains(IndexKind::ZONE_SURF) {
                            Some(f.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                if let Err(e) =
                    ZoneSurfFilter::build_all_filtered(zone_plans, self.segment_dir, &allowed)
                {
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        debug!(target: "sneldb::flush", uid = self.uid, error = %e, "Skipping filtered Zone SuRF due to error");
                    }
                }
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
        let rlte_enabled = build_plan
            .as_ref()
            .map(|bp| bp.global.contains(IndexKind::RLTE))
            .unwrap_or(false);
        match std::panic::catch_unwind(|| {
            if rlte_enabled {
                Some(RlteIndex::build_from_zones(zone_plans))
            } else {
                None
            }
        }) {
            Ok(rlte) => {
                if let Some(rlte) = rlte {
                    if let Err(e) = rlte.save(self.uid, self.segment_dir) {
                        if tracing::enabled!(tracing::Level::DEBUG) {
                            debug!(target: "sneldb::flush", uid = self.uid, error = %e, "Skipping RLTE due to IO error");
                        }
                    }
                } else {
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        debug!(target: "sneldb::flush", uid = self.uid, "RLTE disabled by policy");
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

        // Write index catalog (.icx) if plan exists
        if let Some(plan) = build_plan.clone() {
            if let Some(schema_ref) = schema.as_ref() {
                let planner = IndexBuildPlanner::new(
                    self.uid,
                    &segment_id_str,
                    schema_ref,
                    IndexBuildPolicy::default(),
                );
                let catalog: SegmentIndexCatalog = planner.to_catalog(&plan);
                let icx_path = self.segment_dir.join(format!("{}.icx", self.uid));
                if let Err(e) = catalog.save(&icx_path) {
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        debug!(target: "sneldb::flush", uid = self.uid, error = %e, path = %icx_path.display(), "Failed to save index catalog");
                    }
                }
            }
        }

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
