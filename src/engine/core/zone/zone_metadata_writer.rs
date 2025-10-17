use std::path::Path;

use tracing::debug;

use crate::engine::core::{ZoneMeta, ZonePlan};
use crate::engine::errors::StoreError;

/// Writes zone metadata (the `{uid}.zones` file) derived from `ZonePlan`s
pub struct ZoneMetadataWriter<'a> {
    pub uid: &'a str,
    pub segment_dir: &'a Path,
}

impl<'a> ZoneMetadataWriter<'a> {
    pub fn new(uid: &'a str, segment_dir: &'a Path) -> Self {
        Self { uid, segment_dir }
    }

    /// Build `ZoneMeta` entries for all plans and persist them to disk
    pub fn write(&self, zone_plans: &[ZonePlan]) -> Result<(), StoreError> {
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::flush",
                uid = self.uid,
                "Writing .zones metadata"
            );
        }

        let zone_meta = ZoneMeta::build_all(zone_plans);
        ZoneMeta::save(self.uid, &zone_meta, self.segment_dir)?;

        Ok(())
    }
}
