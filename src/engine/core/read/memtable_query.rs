use crate::engine::core::ConditionEvaluatorBuilder;
use crate::engine::core::{Event, MemTable, QueryPlan};
use tracing::{debug, info};

pub struct MemTableQuery<'a> {
    memtable: &'a MemTable,
    plan: &'a QueryPlan,
}

impl<'a> MemTableQuery<'a> {
    pub fn new(memtable: &'a MemTable, plan: &'a QueryPlan) -> Self {
        Self { memtable, plan }
    }

    pub fn query(&self) -> Vec<Event> {
        let evaluator = ConditionEvaluatorBuilder::build_from_plan(self.plan);
        let event_type = self.plan.event_type();
        let context_id = self.plan.context_id();

        debug!(
            target: "sneldb::query_memtable",
            event_type = %event_type,
            context_id = ?context_id,
            memtable_size = self.memtable.len(),
            "Starting MemTable scan"
        );

        let mut events = Vec::new();

        for event in self.memtable.iter() {
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
