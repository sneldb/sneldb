use crate::engine::core::QueryPlan;
pub struct ProjectionContext<'a> {
    pub plan: &'a QueryPlan,
}

impl<'a> ProjectionContext<'a> {
    pub fn new(plan: &'a QueryPlan) -> Self {
        Self { plan }
    }

    pub fn core_fields(&self) -> Vec<String> {
        vec![
            "context_id".to_string(),
            "event_type".to_string(),
            "timestamp".to_string(),
            "event_id".to_string(),
        ]
    }

    pub fn is_core_field(name: &str) -> bool {
        matches!(name, "context_id" | "event_type" | "timestamp" | "event_id")
    }

    pub fn filter_columns(&self) -> Vec<String> {
        let mut cols: Vec<String> = self
            .plan
            .filter_groups
            .iter()
            .filter_map(|filter| {
                if filter.operation().is_some() {
                    filter.column().map(|c| c.to_string())
                } else {
                    None
                }
            })
            .collect();
        cols.sort();
        cols.dedup();
        cols
    }

    pub async fn payload_fields(&self) -> Vec<String> {
        let registry = self.plan.registry.read().await;

        // Handle wildcard event_type by collecting fields from all schemas
        if self.plan.event_type() == "*" {
            let mut all_fields: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            for schema in registry.get_all().values() {
                for field in schema.fields() {
                    all_fields.insert(field.clone());
                }
            }
            let mut fields: Vec<String> = all_fields.into_iter().collect();
            fields.sort();
            fields
        } else if let Some(schema) = registry.get(self.plan.event_type()) {
            let mut fields: Vec<String> = schema.fields().cloned().collect();
            fields.sort();
            fields
        } else {
            Vec::new()
        }
    }
}
