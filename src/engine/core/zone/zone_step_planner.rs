use crate::engine::core::{ExecutionStep, LogicalOp, QueryPlan};

/// Decides execution order and per-step segment subsets (for pruning)
pub struct ZoneStepPlanner<'a> {
    plan: &'a QueryPlan,
}

impl<'a> ZoneStepPlanner<'a> {
    pub fn new(plan: &'a QueryPlan) -> Self {
        Self { plan }
    }

    /// Returns a vector of (original_step_index, pruned_segments)
    pub fn plan(&self, steps: &[ExecutionStep<'a>]) -> Vec<(usize, Option<Vec<String>>)> {
        let op = LogicalOp::from_expr(self.plan.where_clause());
        if !matches!(op, LogicalOp::And) {
            return (0..steps.len()).map(|i| (i, None)).collect();
        }

        // Find context step; if absent, no reordering/pruning
        if let Some(ctx_idx) = steps.iter().position(|s| s.filter.is_context_id()) {
            // First run ctx over all segments; consumers of this plan will compute pruned list
            let mut plan: Vec<(usize, Option<Vec<String>>)> = Vec::with_capacity(steps.len());
            plan.push((ctx_idx, None));
            // Subsequent steps will run against pruned segments; we signal with Some(vec) later
            // Here we leave None placeholders; ZoneCollector will fill the actual subset once ctx runs
            for (i, _s) in steps.iter().enumerate() {
                if i == ctx_idx {
                    continue;
                }
                plan.push((i, None));
            }
            plan
        } else {
            (0..steps.len()).map(|i| (i, None)).collect()
        }
    }
}
