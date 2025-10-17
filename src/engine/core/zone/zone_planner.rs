use crate::engine::core::{Event, ZonePlan};
use crate::engine::errors::StoreError;
use crate::shared::config::CONFIG;
use tracing::{debug, info};

pub struct ZonePlanner {
    pub uid: String,
    pub segment_id: u64,
}

impl ZonePlanner {
    pub fn new(uid: impl Into<String>, segment_id: u64) -> Self {
        let uid = uid.into();
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::flush",
                uid,
                segment_id,
                "Created ZonePlanner"
            );
        }
        Self { uid, segment_id }
    }

    /// Plans zones from events based on config
    pub fn plan(&self, events: &[Event]) -> Result<Vec<ZonePlan>, StoreError> {
        let zone_size = CONFIG.engine.event_per_zone;
        let total = events.len();

        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::flush",
                uid = self.uid,
                segment_id = self.segment_id,
                event_count = total,
                zone_size,
                "Planning zones from events"
            );
        }

        let zones = ZonePlan::build_all(events, zone_size, self.uid.clone(), self.segment_id)?;

        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::flush",
                uid = self.uid,
                segment_id = self.segment_id,
                zone_count = zones.len(),
                "Zone planning completed"
            );
        }

        Ok(zones)
    }
}
