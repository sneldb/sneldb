use crate::engine::core::{
    CandidateZone, ExecutionStep, QueryCaches, QueryPlan, ZoneCollector, ZoneValueLoader,
};
use tracing::{debug, info, warn};

pub struct ZoneHydrator<'a> {
    plan: &'a QueryPlan,
    steps: Vec<ExecutionStep<'a>>,
    caches: Option<&'a QueryCaches>,
}

impl<'a> ZoneHydrator<'a> {
    pub fn new(plan: &'a QueryPlan, steps: Vec<ExecutionStep<'a>>) -> Self {
        Self {
            plan,
            steps,
            caches: None,
        }
    }

    pub async fn hydrate(self) -> Vec<CandidateZone> {
        debug!(target: "sneldb::query", "Starting zone hydration for plan {:?}", self.plan);

        let mut candidate_zones = ZoneCollector::new(self.plan, self.steps.clone())
            .with_caches(self.caches)
            .collect_zones();

        debug!(target: "sneldb::query", "Collected {} candidate zones", candidate_zones.len());

        match self.plan.event_type_uid().await {
            Some(uid) => {
                // Build minimal column set: filters + projection + core fields
                let columns = self.plan.columns_to_load().await;

                info!(target: "sneldb::query", "Hydrating zones with columns: {:?}", columns);

                let loader = ZoneValueLoader::new(self.plan.segment_base_dir.clone(), uid)
                    .with_caches(self.caches);
                loader.load_zone_values(&mut candidate_zones, &columns);

                debug!(target: "sneldb::query", "Loaded values into {} zones", candidate_zones.len());
            }
            None => {
                warn!(target: "sneldb::query", "No UID found for event_type {:?}", self.plan.event_type());
            }
        }

        candidate_zones
    }

    pub fn with_caches(mut self, caches: Option<&'a QueryCaches>) -> Self {
        self.caches = caches;
        self
    }
}
