use crate::command::types::{Command, PickedZones};
use crate::engine::core::read::query_plan::QueryPlan;
use crate::engine::query::rlte_planner::plan_with_rlte;
use crate::engine::schema::SchemaRegistry;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

/// Output from RLTE planning containing per-shard zone assignments.
#[derive(Debug)]
pub struct RltePlanOutput {
    pub per_shard: HashMap<usize, PickedZones>,
}

/// Coordinates RLTE (Range-Limited Top-k Evaluation) planning.
///
/// This encapsulates the logic for determining which zones each shard
/// should scan when ORDER BY is present.
pub struct RlteCoordinator;

impl RlteCoordinator {
    /// Determines if RLTE planning should be performed for this command.
    pub fn should_plan(cmd: &Command) -> bool {
        matches!(
            cmd,
            Command::Query {
                order_by: Some(_),
                ..
            }
        )
    }

    /// Performs RLTE planning to determine per-shard zone assignments.
    ///
    /// Returns None if planning is not applicable or fails.
    pub async fn plan(
        cmd: &Command,
        registry: Arc<RwLock<SchemaRegistry>>,
        shard_bases: &HashMap<usize, PathBuf>,
        shard_segments: &HashMap<usize, Vec<String>>,
    ) -> Option<RltePlanOutput> {
        if !Self::should_plan(cmd) {
            debug!(
                target: "sneldb::rlte_coordinator",
                "RLTE planning not applicable (no ORDER BY)"
            );
            return None;
        }

        debug!(
            target: "sneldb::rlte_coordinator",
            shard_count = shard_bases.len(),
            "Starting RLTE planning"
        );

        // Build query plan for RLTE
        let plan = QueryPlan::build(cmd, registry).await;

        // Invoke RLTE planner
        match plan_with_rlte(&plan, shard_bases, shard_segments).await {
            Some(output) => {
                let total_zones: usize = output.per_shard.values().map(|pz| pz.zones.len()).sum();
                debug!(
                    target: "sneldb::rlte_coordinator",
                    shard_count = output.per_shard.len(),
                    total_zones = total_zones,
                    "RLTE planning completed successfully"
                );
                Some(RltePlanOutput {
                    per_shard: output.per_shard,
                })
            }
            None => {
                debug!(
                    target: "sneldb::rlte_coordinator",
                    "RLTE planning returned no output"
                );
                None
            }
        }
    }
}
