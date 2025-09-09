use crate::engine::core::{
    CandidateZone, ExecutionStep, QueryPlan, ZoneCollector, ZoneValueLoader,
};
use tracing::{debug, info, warn};

pub struct ZoneHydrator<'a> {
    plan: &'a QueryPlan,
    steps: Vec<ExecutionStep<'a>>,
}

impl<'a> ZoneHydrator<'a> {
    pub fn new(plan: &'a QueryPlan, steps: Vec<ExecutionStep<'a>>) -> Self {
        Self { plan, steps }
    }

    pub async fn hydrate(self) -> Vec<CandidateZone> {
        debug!(target: "sneldb::query", "Starting zone hydration for plan {:?}", self.plan);

        let mut candidate_zones = ZoneCollector::new(self.plan, self.steps.clone()).collect_zones();

        debug!(target: "sneldb::query", "Collected {} candidate zones", candidate_zones.len());

        match self.plan.event_type_uid().await {
            Some(uid) => {
                let columns: Vec<String> = self
                    .steps
                    .iter()
                    .map(|step| step.filter.column.clone())
                    .collect();

                info!(target: "sneldb::query", "Hydrating zones with columns: {:?}", columns);

                let loader = ZoneValueLoader::new(self.plan.segment_base_dir.clone(), uid);
                loader.load_zone_values(&mut candidate_zones, &columns);

                debug!(target: "sneldb::query", "Loaded values into {} zones", candidate_zones.len());
            }
            None => {
                warn!(target: "sneldb::query", "No UID found for event_type {:?}", self.plan.event_type());
            }
        }

        candidate_zones
    }
}
