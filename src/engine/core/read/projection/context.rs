use crate::engine::core::QueryPlan;
use std::collections::HashSet;

pub struct ProjectionContext<'a> {
    pub plan: &'a QueryPlan,
}

impl<'a> ProjectionContext<'a> {
    pub fn new(plan: &'a QueryPlan) -> Self {
        Self { plan }
    }

    pub fn core_fields(&self) -> HashSet<String> {
        [
            "context_id".to_string(),
            "event_type".to_string(),
            "timestamp".to_string(),
            "event_id".to_string(),
        ]
        .into_iter()
        .collect()
    }

    pub fn is_core_field(name: &str) -> bool {
        matches!(name, "context_id" | "event_type" | "timestamp" | "event_id")
    }

    pub fn filter_columns(&self) -> HashSet<String> {
        let mut cols = HashSet::new();
        for filter in &self.plan.filter_plans {
            if filter.operation.is_some() {
                cols.insert(filter.column.clone());
            }
        }
        cols
    }

    pub async fn payload_fields(&self) -> HashSet<String> {
        if let Some(schema) = self.plan.registry.read().await.get(self.plan.event_type()) {
            schema.fields().cloned().collect()
        } else {
            HashSet::new()
        }
    }
}
