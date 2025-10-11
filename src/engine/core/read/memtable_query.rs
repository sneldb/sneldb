use crate::engine::core::ConditionEvaluatorBuilder;
use crate::engine::core::{Event, MemTable, QueryPlan};
use tracing::{debug, info};

pub struct MemTableQuery<'a> {
    memtable: &'a MemTable,
    plan: &'a QueryPlan,
    limit_override: Option<Option<usize>>, // None = use plan limit, Some(x) = override
}

impl<'a> MemTableQuery<'a> {
    pub fn new(memtable: &'a MemTable, plan: &'a QueryPlan) -> Self {
        Self {
            memtable,
            plan,
            limit_override: None,
        }
    }

    /// Overrides the limit from the plan.
    ///
    /// This allows the caller to specify a different limit than what's in the plan,
    /// which is useful when ORDER BY is present and we need to defer limiting.
    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit_override = Some(limit);
        self
    }

    pub fn query(&self) -> Vec<Event> {
        let evaluator = ConditionEvaluatorBuilder::build_from_plan(self.plan);
        let event_type = self.plan.event_type();
        let context_id = self.plan.context_id();

        // Use override if present, otherwise fall back to plan limit
        let limit = self.limit_override.unwrap_or_else(|| self.plan.limit());

        debug!(
            target: "sneldb::query_memtable",
            event_type = %event_type,
            context_id = ?context_id,
            memtable_size = self.memtable.len(),
            limit = ?limit,
            "Starting MemTable scan"
        );

        let mut events = Vec::new();

        for event in self.memtable.iter() {
            if let Some(lim) = limit {
                if events.len() >= lim {
                    break;
                }
            }
            if evaluator.evaluate_event(event) {
                events.push(event.clone());
            }
        }

        info!(
            target: "sneldb::query_memtable",
            matches = events.len(),
            "MemTable scan complete"
        );

        events
    }
}
