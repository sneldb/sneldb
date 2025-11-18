use crate::engine::core::{Event, ZonePlan};
use crate::engine::errors::{StoreError, ZoneMetaError};
use crate::shared::storage_header::{BinaryHeader, FileKind};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use tracing::{debug, error, trace, warn};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ZoneMeta {
    pub zone_id: u32,
    pub uid: String,
    pub segment_id: u64,
    pub start_row: u32,
    pub end_row: u32,
    pub timestamp_min: u64,
    pub timestamp_max: u64,
    #[serde(default)]
    pub created_at: u64,
}

impl ZoneMeta {
    pub fn load(path: &Path) -> Result<Vec<ZoneMeta>, ZoneMetaError> {
        if tracing::enabled!(tracing::Level::TRACE) {
            trace!(
                target: "sneldb::flush",
                path = %path.display(),
                "Loading zone meta from disk"
            );
        }

        let mut file = File::open(path)?;
        let header = BinaryHeader::read_from(&mut file)?;
        if header.magic != FileKind::ZoneMeta.magic() {
            return Err(ZoneMetaError::Other("invalid magic for .zones".into()));
        }
        let reader = BufReader::new(file);
        let zones: Vec<ZoneMeta> = bincode::deserialize_from(reader)?;

        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::flush",
                zone_count = zones.len(),
                path = %path.display(),
                "Zone meta loaded successfully"
            );
        }
        Ok(zones)
    }

    pub fn save(uid: &str, zones: &[ZoneMeta], segment_dir: &Path) -> Result<(), ZoneMetaError> {
        let path = segment_dir.join(format!("{}.zones", uid));

        if tracing::enabled!(tracing::Level::TRACE) {
            trace!(
                target: "sneldb::flush",
                uid,
                zone_count = zones.len(),
                path = %path.display(),
                "Saving zone meta to disk"
            );
        }

        let mut file = File::create(&path)?;
        let header = BinaryHeader::new(FileKind::ZoneMeta.magic(), 1, 0);
        header.write_to(&mut file)?;
        let writer = BufWriter::new(file);
        bincode::serialize_into(writer, zones)?;

        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::flush",
                uid,
                zone_count = zones.len(),
                path = %path.display(),
                "Wrote zone meta file"
            );
        }

        Ok(())
    }

    pub async fn save_async(
        uid: &str,
        zones: &[ZoneMeta],
        segment_dir: &Path,
    ) -> Result<(), ZoneMetaError> {
        use tokio::io::AsyncWriteExt;

        let path = segment_dir.join(format!("{}.zones", uid));

        if tracing::enabled!(tracing::Level::TRACE) {
            trace!(
                target: "sneldb::flush",
                uid,
                zone_count = zones.len(),
                path = %path.display(),
                "Saving zone meta to disk (async)"
            );
        }

        let mut file = tokio::fs::File::create(&path).await?;

        // Write header
        let header = BinaryHeader::new(FileKind::ZoneMeta.magic(), 1, 0);
        let mut header_buf = Vec::with_capacity(BinaryHeader::TOTAL_LEN);
        header.write_to(&mut header_buf)?;
        file.write_all(&header_buf).await?;

        // Serialize zones to buffer then write
        let serialized = bincode::serialize(zones)?;
        file.write_all(&serialized).await?;
        file.sync_all().await?;

        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::flush",
                uid,
                zone_count = zones.len(),
                path = %path.display(),
                "Wrote zone meta file (async)"
            );
        }

        Ok(())
    }

    pub fn build(zone_plan: &ZonePlan) -> Result<ZoneMeta, StoreError> {
        if zone_plan.events.is_empty() {
            if tracing::enabled!(tracing::Level::WARN) {
                warn!(
                    target: "sneldb::flush",
                    uid = zone_plan.uid,
                    segment_id = zone_plan.segment_id,
                    zone_id = zone_plan.id,
                    "Skipping empty zone plan"
                );
            }
            return Err(StoreError::EmptyFlush);
        }

        if tracing::enabled!(tracing::Level::TRACE) {
            trace!(
                target: "sneldb::flush",
                uid = zone_plan.uid,
                segment_id = zone_plan.segment_id,
                zone_id = zone_plan.id,
                event_count = zone_plan.events.len(),
                "Building ZoneMeta from zone plan"
            );
        }

        let sorted_by_time = Event::order_by(&zone_plan.events, "timestamp");

        Ok(ZoneMeta {
            zone_id: zone_plan.id,
            start_row: zone_plan.start_index as u32,
            end_row: zone_plan.end_index as u32,
            timestamp_min: sorted_by_time.first().unwrap().timestamp,
            timestamp_max: sorted_by_time.last().unwrap().timestamp,
            uid: zone_plan.uid.clone(),
            segment_id: zone_plan.segment_id,
            created_at: zone_plan.created_at,
        })
    }

    pub fn build_all(zone_plans: &[ZonePlan]) -> Vec<ZoneMeta> {
        zone_plans
            .iter()
            .filter_map(|plan| match ZoneMeta::build(plan) {
                Ok(meta) => Some(meta),
                Err(e) => {
                    if tracing::enabled!(tracing::Level::ERROR) {
                        error!(
                            target: "sneldb::flush",
                            uid = plan.uid,
                            segment_id = plan.segment_id,
                            zone_id = plan.id,
                            error = %e,
                            "Failed to build ZoneMeta"
                        );
                    }
                    None
                }
            })
            .collect()
    }

    pub fn sort_by<'a>(zones: &'a mut [ZoneMeta], field: &str) -> &'a [ZoneMeta] {
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::flush",
                field,
                "Sorting zone meta by field"
            );
        }

        match field {
            "zone_id" => zones.sort_by_key(|z| z.zone_id),
            "start_row" => zones.sort_by_key(|z| z.start_row),
            "end_row" => zones.sort_by_key(|z| z.end_row),
            "timestamp_min" => zones.sort_by_key(|z| z.timestamp_min),
            "timestamp_max" => zones.sort_by_key(|z| z.timestamp_max),
            _ => {
                if tracing::enabled!(tracing::Level::ERROR) {
                    error!(
                        target: "sneldb::flush",
                        field,
                        "Invalid field provided to ZoneMeta::sort_by"
                    );
                }
            }
        }

        zones
    }
}
