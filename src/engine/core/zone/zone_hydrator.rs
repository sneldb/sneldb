use crate::engine::core::{
    CandidateZone, ExecutionStep, QueryCaches, QueryPlan, SegmentZoneId, ZoneCollector, ZoneFilter,
    ZoneValueLoader,
};
use std::collections::HashSet;
use tracing::{debug, info};

pub struct ZoneHydrator<'a> {
    plan: &'a QueryPlan,
    steps: Vec<ExecutionStep<'a>>,
    caches: Option<&'a QueryCaches>,
    zone_filter: Option<ZoneFilter>,
}

impl<'a> ZoneHydrator<'a> {
    pub fn new(plan: &'a QueryPlan, steps: Vec<ExecutionStep<'a>>) -> Self {
        Self {
            plan,
            steps,
            caches: None,
            zone_filter: None,
        }
    }

    pub async fn hydrate(self) -> Vec<CandidateZone> {
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(target: "sneldb::query", "Starting zone hydration for plan {:?}", self.plan);
        }

        let mut candidate_zones = ZoneCollector::new(self.plan, self.steps.clone())
            .with_caches(self.caches)
            .collect_zones();

        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(target: "sneldb::query", "Collected {} candidate zones", candidate_zones.len());
        }

        // Deduplicate zones by (zone_id, segment_id) to avoid double-processing the same zone
        // when missing-index fallbacks enumerate AllZones across multiple filter steps.
        let mut seen: HashSet<(u32, String)> = HashSet::with_capacity(candidate_zones.len());
        candidate_zones.retain(|z| seen.insert((z.zone_id, z.segment_id.clone())));

        // If coordinator supplied a zone filter, apply it now
        if let Some(filter) = &self.zone_filter {
            filter.apply(&mut candidate_zones);
        }
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(target: "sneldb::query", "Deduplicated to {} candidate zones", candidate_zones.len());
        }

        match self.plan.event_type_uid().await {
            Some(uid) => {
                // Build minimal column set: filters + projection + core fields
                let columns = self.plan.columns_to_load().await;

                if tracing::enabled!(tracing::Level::INFO) {
                    info!(target: "sneldb::query", "Hydrating zones with columns: {:?}", columns);
                }

                let loader = ZoneValueLoader::new(self.plan.segment_base_dir.clone(), uid)
                    .with_caches(self.caches);
                loader.load_zone_values(&mut candidate_zones, &columns);

                if tracing::enabled!(tracing::Level::DEBUG) {
                    debug!(target: "sneldb::query", "Loaded values into {} zones", candidate_zones.len());
                }
            }
            None => {
                if tracing::enabled!(tracing::Level::DEBUG) {
                    debug!(target: "sneldb::query", "No UID found for event_type {:?}", self.plan.event_type());
                }
            }
        }

        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(target: "sneldb::query", "Candidate zones: {:?}", candidate_zones.len());
        }
        candidate_zones
    }

    pub fn with_caches(mut self, caches: Option<&'a QueryCaches>) -> Self {
        self.caches = caches;
        self
    }

    pub fn with_allowed_zones(
        mut self,
        allowed: Option<std::collections::HashSet<(String, u32)>>,
    ) -> Self {
        self.zone_filter = allowed.map(|tuples| {
            let zone_ids: HashSet<SegmentZoneId> =
                tuples.into_iter().map(SegmentZoneId::from).collect();
            ZoneFilter::new(zone_ids)
        });
        self
    }
}
