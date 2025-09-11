use std::collections::HashSet;

use crate::command::types::Command;
use crate::engine::core::QueryPlan;
use tracing::{debug, info};

/// Projection policy derived from the RETURN clause
enum ProjectionMode {
    /// Load all payload columns
    AllPayload,
    /// Load only the listed fields (plus cores and filters)
    Fields(Vec<String>),
}

/// Computes the minimal set of columns that need to be loaded
/// for a given `QueryPlan`, taking filters and RETURN projection into account.
pub struct ProjectionPlanner<'a> {
    plan: &'a QueryPlan,
}

impl<'a> ProjectionPlanner<'a> {
    pub fn new(plan: &'a QueryPlan) -> Self {
        Self { plan }
    }

    pub async fn columns_to_load(&self) -> Vec<String> {
        // Start with core fields
        let mut needed = self.core_fields();
        debug!(target: "query::projection", core=?needed, "Initialized core projection fields");

        // Add filter columns (from filter plans)
        let filter_cols = self.collect_filter_columns();
        debug!(target: "query::projection", ?filter_cols, "Collected filter columns from plans");
        needed.extend(filter_cols);

        // Determine projection mode and payload fields from schema
        let mode = self.resolve_projection_mode();
        info!(target: "query::projection", event_type=%self.plan.event_type(), mode=?proj_mode_to_str(&mode), "Resolved projection mode");
        let all_payload = self.all_payload_fields(self.plan.event_type()).await;
        debug!(target: "query::projection", payload_fields=?all_payload, count=all_payload.len(), "Resolved payload fields from schema");

        match mode {
            ProjectionMode::AllPayload => {
                debug!(target: "query::projection", "Including all payload fields due to mode=AllPayload");
                needed.extend(all_payload);
            }
            ProjectionMode::Fields(list) => {
                let projected: HashSet<String> = list
                    .iter()
                    .filter(|f| Self::is_core_field(f) || all_payload.contains(*f))
                    .cloned()
                    .collect();
                debug!(target: "query::projection", requested=?list, selected=?projected, "Filtered requested fields against schema and core");
                needed.extend(projected);
            }
        }

        let cols: Vec<String> = needed.into_iter().collect();
        info!(target: "query::projection", columns=?cols, "Final columns to load computed");
        cols
    }

    fn core_fields(&self) -> HashSet<String> {
        [
            "context_id".to_string(),
            "event_type".to_string(),
            "timestamp".to_string(),
        ]
        .into_iter()
        .collect()
    }

    fn is_core_field(name: &str) -> bool {
        matches!(name, "context_id" | "event_type" | "timestamp")
    }

    fn collect_filter_columns(&self) -> HashSet<String> {
        let mut cols = HashSet::new();
        for filter in &self.plan.filter_plans {
            if filter.operation.is_some() {
                cols.insert(filter.column.clone());
            }
        }
        cols
    }

    fn resolve_projection_mode(&self) -> ProjectionMode {
        if let Command::Query { return_fields, .. } = &self.plan.command {
            match return_fields {
                None => ProjectionMode::AllPayload,
                Some(v) if v.is_empty() => ProjectionMode::AllPayload,
                Some(v) => ProjectionMode::Fields(v.clone()),
            }
        } else {
            ProjectionMode::AllPayload
        }
    }

    async fn all_payload_fields(&self, event_type: &str) -> HashSet<String> {
        if let Some(schema) = self.plan.registry.read().await.get(event_type) {
            schema.fields().cloned().collect()
        } else {
            HashSet::new()
        }
    }
}

fn proj_mode_to_str(mode: &ProjectionMode) -> &'static str {
    match mode {
        ProjectionMode::AllPayload => "AllPayload",
        ProjectionMode::Fields(_) => "Fields",
    }
}
