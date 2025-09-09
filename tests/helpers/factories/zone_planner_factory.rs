use crate::engine::core::{Event, ZonePlan, ZonePlanner};

#[derive(Debug, Clone)]
pub struct ZonePlannerFactory {
    uid: String,
    segment_id: u64,
    events: Vec<Event>,
}

impl ZonePlannerFactory {
    pub fn new(events: Vec<Event>, uid: &str) -> Self {
        Self {
            uid: uid.to_string(),
            segment_id: 1,
            events,
        }
    }

    pub fn with_uid(mut self, uid: &str) -> Self {
        self.uid = uid.to_string();
        self
    }

    pub fn with_segment_id(mut self, id: u64) -> Self {
        self.segment_id = id;
        self
    }

    pub fn plan(self) -> Vec<ZonePlan> {
        let planner = ZonePlanner::new(self.uid, self.segment_id);
        planner.plan(&self.events).expect("zone planning failed")
    }
}
