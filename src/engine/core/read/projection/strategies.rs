use super::columns::ProjectionColumns;
use super::context::ProjectionContext;
use crate::engine::core::QueryPlan;
use crate::engine::core::read::aggregate::plan::{AggregateOpSpec, AggregatePlan};
use std::collections::HashSet;

#[async_trait::async_trait]
pub trait ProjectionStrategy {
    async fn compute(&self) -> ProjectionColumns;
}

pub struct SelectionProjection<'a> {
    pub plan: &'a QueryPlan,
}

#[async_trait::async_trait]
impl<'a> ProjectionStrategy for SelectionProjection<'a> {
    async fn compute(&self) -> ProjectionColumns {
        let mut set = ProjectionColumns::new();
        let ctx = ProjectionContext::new(self.plan);
        set.add_many(ctx.core_fields());
        set.add_many(ctx.filter_columns());

        let mode_all = match &self.plan.command {
            crate::command::types::Command::Query { return_fields, .. } => match return_fields {
                None => true,
                Some(v) if v.is_empty() => true,
                Some(_v) => false,
            },
            _ => true,
        };

        let all_payload = ctx.payload_fields().await;
        if mode_all {
            set.add_many(all_payload.clone());
        } else if let crate::command::types::Command::Query {
            return_fields: Some(list),
            ..
        } = &self.plan.command
        {
            let payload_set: HashSet<String> = all_payload.into_iter().collect();
            let projected: HashSet<String> = list
                .iter()
                .filter(|f| {
                    ProjectionContext::is_core_field(f) || payload_set.contains(&f.to_string())
                })
                .cloned()
                .collect();
            set.add_many(projected);
        }

        set.add("event_id");
        set
    }
}

pub struct AggregationProjection<'a> {
    pub plan: &'a QueryPlan,
    pub agg: &'a AggregatePlan,
}

#[async_trait::async_trait]
impl<'a> ProjectionStrategy for AggregationProjection<'a> {
    async fn compute(&self) -> ProjectionColumns {
        let mut set = ProjectionColumns::new();
        let ctx = ProjectionContext::new(self.plan);

        let filter_cols = ctx.filter_columns();
        let filtered = filter_cols
            .into_iter()
            .filter(|c| !ProjectionContext::is_core_field(c))
            .collect::<Vec<String>>();
        set.add_many(filtered);

        // group by
        if let Some(group_by) = &self.agg.group_by {
            for g in group_by {
                set.add(g.clone());
            }
        }

        // time bucket requires the selected time field (defaults to core "timestamp")
        if self.agg.time_bucket.is_some() {
            let tf = match &self.plan.command {
                crate::command::types::Command::Query { time_field, .. } => {
                    time_field.as_deref().unwrap_or("timestamp")
                }
                _ => "timestamp",
            };
            set.add(tf);
        }

        // agg inputs
        for op in &self.agg.ops {
            match op {
                AggregateOpSpec::CountAll => {}
                AggregateOpSpec::CountField { field }
                | AggregateOpSpec::CountUnique { field }
                | AggregateOpSpec::Total { field }
                | AggregateOpSpec::Avg { field }
                | AggregateOpSpec::Min { field }
                | AggregateOpSpec::Max { field } => set.add(field.clone()),
            }
        }

        // intersect
        let payload_vec = ctx.payload_fields().await;
        let payload_set: HashSet<String> = payload_vec.into_iter().collect();
        let items: Vec<String> = set
            .into_vec()
            .into_iter()
            .filter(|c| ProjectionContext::is_core_field(c) || payload_set.contains(c))
            .collect();
        let mut final_set = ProjectionColumns::new();
        final_set.add_many(items);
        // Ensure at least one column is loaded for COUNT ALL-only queries so we can
        // determine the number of events in a zone. Using a core field avoids
        // depending on payload schema. Timestamp is present in all zones.
        if final_set.is_empty() {
            final_set.add("timestamp");
        }
        final_set.add("event_id");
        final_set
    }
}
