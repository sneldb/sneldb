use crate::command::types::Command;

use super::full_scan::FullScanPlanner;
use super::rlte::RltePlanner;
use super::traits::QueryPlanner;

/// Builder for creating query planners based on commands.
pub struct QueryPlannerBuilder<'a> {
    command: &'a Command,
}

impl<'a> QueryPlannerBuilder<'a> {
    /// Creates a new builder for the given command.
    pub fn new(command: &'a Command) -> Self {
        Self { command }
    }

    /// Builds and returns the appropriate query planner for the command.
    pub fn build(self) -> Box<dyn QueryPlanner> {
        if crate::command::handlers::rlte_coordinator::RlteCoordinator::should_plan(self.command) {
            Box::new(RltePlanner::new())
        } else {
            Box::new(FullScanPlanner::new())
        }
    }
}
