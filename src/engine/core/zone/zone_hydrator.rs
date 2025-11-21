use crate::engine::core::read::event_scope::EventScope;
use crate::engine::core::{
    CandidateZone, ExecutionStep, QueryCaches, QueryPlan, SegmentZoneId, ZoneCollector, ZoneFilter,
    ZoneValueLoader,
};
use std::collections::HashSet;
use tracing::{debug, info, warn};

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
        let hydrate_start = std::time::Instant::now();
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(target: "sneldb::query", "Starting zone hydration for plan {:?}", self.plan);
        }

        let mut candidate_zones = ZoneCollector::new(self.plan, self.steps.clone())
            .with_caches(self.caches)
            .collect_zones();

        let zones_after_collect = candidate_zones.len();

        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(target: "sneldb::query", "Collected {} candidate zones", candidate_zones.len());
        }

        // Deduplicate zones by (zone_id, segment_id) to avoid double-processing the same zone
        // when missing-index fallbacks enumerate AllZones across multiple filter steps.
        let mut seen: HashSet<(u32, String)> = HashSet::with_capacity(candidate_zones.len());
        candidate_zones.retain(|z| seen.insert((z.zone_id, z.segment_id.clone())));

        let zones_after_dedup = candidate_zones.len();

        // If coordinator supplied a zone filter, apply it now
        if let Some(filter) = &self.zone_filter {
            filter.apply(&mut candidate_zones);
        }
        let zones_after_filter = candidate_zones.len();

        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::zone_hydrator",
                collected = zones_after_collect,
                deduplicated = zones_after_dedup,
                filtered = zones_after_filter,
                "Zone collection stats prior to hydration"
            );
        }

        let columns = self.plan.columns_to_load().await;
        let mut zones_by_uid: std::collections::HashMap<String, Vec<usize>> =
            std::collections::HashMap::new();
        for (idx, zone) in candidate_zones.iter().enumerate() {
            if let Some(uid) = zone.uid() {
                zones_by_uid.entry(uid.to_string()).or_default().push(idx);
            }
        }

        if zones_by_uid.is_empty() {
            if matches!(self.plan.event_scope(), EventScope::Wildcard { .. })
                && tracing::enabled!(tracing::Level::WARN)
            {
                let missing_uid = candidate_zones.iter().filter(|z| z.uid().is_none()).count();
                if missing_uid > 0 {
                    warn!(
                        target: "sneldb::zone_hydrator",
                        missing_uid_zones = missing_uid,
                        total_zones = candidate_zones.len(),
                        "Wildcard query produced zones without UID metadata; falling back to legacy loader"
                    );
                }
            }
            match self.plan.event_type_uid().await {
                Some(uid) => {
                    if tracing::enabled!(tracing::Level::INFO) {
                        info!(
                            target: "sneldb::zone_hydrator",
                            %uid,
                            zone_count = candidate_zones.len(),
                            column_count = columns.len(),
                            columns = ?columns,
                            "Hydrating specific event type"
                        );
                    }

                    let loader = ZoneValueLoader::new(self.plan.segment_base_dir.clone(), uid)
                        .with_caches(self.caches);
                    loader.load_zone_values(&mut candidate_zones, &columns);

                    if tracing::enabled!(tracing::Level::DEBUG) {
                        debug!(
                            target: "sneldb::query",
                            "Loaded values into {} zones",
                            candidate_zones.len()
                        );
                    }
                }
                None => {
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        debug!(
                            target: "sneldb::query",
                            "No UID found for event_type {:?}",
                            self.plan.event_type()
                        );
                    }
                }
            }
        } else {
            if tracing::enabled!(tracing::Level::INFO) {
                let mut uid_summary: Vec<String> = zones_by_uid
                    .iter()
                    .map(|(uid, indices)| format!("{}:{}", uid, indices.len()))
                    .collect();
                uid_summary.sort();
                info!(
                    target: "sneldb::zone_hydrator",
                    uid_count = zones_by_uid.len(),
                    total_zones = candidate_zones.len(),
                    column_count = columns.len(),
                    columns = ?columns,
                    uid_breakdown = ?uid_summary,
                    "Hydrating wildcard zones with per-UID loaders"
                );
            }
            for (uid, indices) in zones_by_uid {
                let zone_count = indices.len();
                let loader = ZoneValueLoader::new(self.plan.segment_base_dir.clone(), uid.clone())
                    .with_caches(self.caches);
                for idx in indices {
                    if let Some(zone) = candidate_zones.get_mut(idx) {
                        loader.load_zone_values(std::slice::from_mut(zone), &columns);
                    }
                }
                if tracing::enabled!(tracing::Level::INFO) {
                    info!(
                        target: "sneldb::zone_hydrator",
                        %uid,
                        zone_count = zone_count,
                        column_count = columns.len(),
                        "Finished hydrating UID subset for wildcard query"
                    );
                }
            }
        }

        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(target: "sneldb::query", "Candidate zones: {:?}", candidate_zones.len());
        }

        let hydrate_time = hydrate_start.elapsed();
        if tracing::enabled!(tracing::Level::TRACE) {
            tracing::trace!(
                target: "sneldb::zone_hydrator",
                zones_after_collect = zones_after_collect,
                zones_after_dedup = zones_after_dedup,
                zones_after_filter = zones_after_filter,
                final_zones = candidate_zones.len(),
                hydrate_time_ms = hydrate_time.as_millis(),
                "Zone hydration completed"
            );
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
