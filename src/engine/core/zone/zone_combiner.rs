use crate::engine::core::{CandidateZone, LogicalOp};
use std::collections::HashMap;
use tracing::{debug, warn};

pub struct ZoneCombiner {
    zones: Vec<Vec<CandidateZone>>,
    op: LogicalOp,
}

impl ZoneCombiner {
    pub fn new(zones: Vec<Vec<CandidateZone>>, op: LogicalOp) -> Self {
        Self { zones, op }
    }

    pub fn combine(&self) -> Vec<CandidateZone> {
        if self.zones.is_empty() {
            return vec![];
        }

        if self.zones.len() == 1 {
            return CandidateZone::uniq(self.zones[0].clone());
        }

        let maps: Vec<HashMap<(u32, String), CandidateZone>> = self
            .zones
            .iter()
            .map(|set| {
                set.iter()
                    .map(|z| ((z.zone_id, z.segment_id.clone()), z.clone()))
                    .collect()
            })
            .collect();

        let result = match self.op {
            LogicalOp::And => {
                let mut base = maps[0].clone();
                if tracing::enabled!(tracing::Level::DEBUG) {
                    debug!(target: "sneldb::zone_combiner", count = %base.len(), "Base map size before AND");
                }
                for m in &maps[1..] {
                    base.retain(|k, _| m.contains_key(k));
                }
                if tracing::enabled!(tracing::Level::DEBUG) {
                    debug!(target: "sneldb::zone_combiner", count = %base.len(), "Base map size after AND");
                }
                base
            }
            LogicalOp::Or => {
                let mut all = HashMap::new();
                for m in maps {
                    all.extend(m);
                }
                if tracing::enabled!(tracing::Level::DEBUG) {
                    debug!(target: "sneldb::zone_combiner", count = %all.len(), "Combined map size for OR");
                }
                all
            }
            LogicalOp::Not => {
                if tracing::enabled!(tracing::Level::WARN) {
                    warn!(target: "sneldb::zone_combiner", "ZoneCombiner: NOT operation not implemented");
                }
                HashMap::new()
            }
        };

        let mut zones: Vec<CandidateZone> = result.into_values().collect();
        // Sort zones deterministically by (segment_id, zone_id) to ensure consistent processing order
        zones.sort_by(|a, b| {
            a.segment_id
                .cmp(&b.segment_id)
                .then_with(|| a.zone_id.cmp(&b.zone_id))
        });
        CandidateZone::uniq(zones)
    }
}
