use crate::engine::core::{CandidateZone, QueryCaches};
use crate::engine::core::ColumnLoader;
use std::path::PathBuf;
use tracing::{debug, info};

/// Handles loading values for zones
pub struct ZoneValueLoader {
    segment_base_dir: PathBuf,
    uid: String,
    caches: Option<*const QueryCaches>,
}

impl ZoneValueLoader {
    /// Creates a new ZoneValueLoader for the given segment and event type
    pub fn new(segment_base_dir: PathBuf, uid: String) -> Self {
        Self {
            segment_base_dir,
            uid,
            caches: None,
        }
    }

    pub fn with_caches(mut self, caches: Option<&QueryCaches>) -> Self {
        self.caches = caches.map(|c| c as *const QueryCaches);
        self
    }

    /// Loads values for all zones
    pub fn load_zone_values(&self, zones: &mut [CandidateZone], columns: &[String]) {
        info!(
            target: "sneldb::query",
            uid = self.uid,
            zone_count = zones.len(),
            columns = ?columns,
            "Loading values for candidate zones"
        );

        let loader = ColumnLoader::new(self.segment_base_dir.clone(), self.uid.clone())
            .with_caches(self.caches);

        for zone in zones {
            debug!(
                target: "sneldb::query",
                uid = self.uid,
                segment_id = zone.segment_id,
                zone_id = zone.zone_id,
                "Loading column values for zone"
            );

            let values = loader.load_all_columns(zone, columns);
            zone.set_values(values);
        }

        info!(
            target: "sneldb::query",
            uid = self.uid,
            "Completed loading values for all zones"
        );
    }
}
