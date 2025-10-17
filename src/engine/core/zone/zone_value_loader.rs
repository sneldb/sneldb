use crate::engine::core::ColumnLoader;
use crate::engine::core::{CandidateZone, QueryCaches};
use std::path::PathBuf;
use tracing::{debug, info};

/// Handles loading values for zones
pub struct ZoneValueLoader<'a> {
    segment_base_dir: PathBuf,
    uid: String,
    caches: Option<&'a QueryCaches>,
}

impl<'a> ZoneValueLoader<'a> {
    /// Creates a new ZoneValueLoader for the given segment and event type
    pub fn new(segment_base_dir: PathBuf, uid: String) -> Self {
        Self {
            segment_base_dir,
            uid,
            caches: None,
        }
    }

    pub fn with_caches(mut self, caches: Option<&'a QueryCaches>) -> Self {
        self.caches = caches;
        self
    }

    /// Loads values for all zones
    pub fn load_zone_values(&self, zones: &mut [CandidateZone], columns: &[String]) {
        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::query",
                uid = self.uid,
                zone_count = zones.len(),
                columns = ?columns,
                "Loading values for candidate zones"
            );
        }

        let loader = ColumnLoader::new(self.segment_base_dir.clone(), self.uid.clone())
            .with_caches(self.caches);

        for zone in zones {
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(
                    target: "sneldb::query",
                    uid = self.uid,
                    segment_id = %zone.segment_id,
                    zone_id = zone.zone_id,
                    "Loading column values for zone"
                );
            }

            let values = loader.load_all_columns(zone, columns);
            zone.set_values(values);
        }

        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::query",
                uid = self.uid,
                "Completed loading values for all zones"
            );
        }
    }
}
