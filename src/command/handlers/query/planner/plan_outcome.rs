use std::collections::HashMap;

use crate::command::types::PickedZones;

/// Outputs produced by the planning stage.
pub struct PlanOutcome {
    pub picked_zones: Option<HashMap<usize, PickedZones>>,
}

impl PlanOutcome {
    pub fn without_zones() -> Self {
        Self { picked_zones: None }
    }

    pub fn with_zones(picked_zones: HashMap<usize, PickedZones>) -> Self {
        Self {
            picked_zones: Some(picked_zones),
        }
    }
}
