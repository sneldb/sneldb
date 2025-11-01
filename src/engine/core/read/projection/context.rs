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
            .filter_plans
            .iter()
            .filter_map(|filter| {
                if filter.operation.is_some() {
                    Some(filter.column.clone())
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
        if let Some(schema) = self.plan.registry.read().await.get(self.plan.event_type()) {
            let mut fields: Vec<String> = schema.fields().cloned().collect();
            fields.sort();
            fields
        } else {
            Vec::new()
        }
    }
}
