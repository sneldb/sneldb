use crate::engine::core::QueryPlan;
use crate::engine::core::read::projection::strategies::{
    AggregationProjection, ProjectionStrategy, SelectionProjection,
};

/// Computes the minimal set of columns that need to be loaded for the given plan.
pub struct ProjectionPlanner<'a> {
    plan: &'a QueryPlan,
}

impl<'a> ProjectionPlanner<'a> {
    pub fn new(plan: &'a QueryPlan) -> Self {
        Self { plan }
    }

    pub async fn columns_to_load(&self) -> Vec<String> {
        if let Some(agg) = &self.plan.aggregate_plan {
            let s = AggregationProjection {
                plan: self.plan,
                agg,
            };
            s.compute().await.into_vec()
        } else {
            let s = SelectionProjection { plan: self.plan };
            s.compute().await.into_vec()
        }
    }
}
