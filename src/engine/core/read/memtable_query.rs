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
        // Compute scan window: we need to scan up to offset+limit matches if both present,
        // otherwise just limit, otherwise no cap.
        let limit = self.plan.limit();
        let offset = self.plan.offset();
        let scan_limit = match (limit, offset) {
            (Some(l), Some(o)) => Some(l.saturating_add(o)),
            (Some(l), None) => Some(l),
            _ => None,
        };

        debug!(
            target: "sneldb::query_memtable",
            event_type = %event_type,
            context_id = ?context_id,
            memtable_size = self.memtable.len(),
            "Starting MemTable scan"
        );

        let mut events = Vec::new();

        for event in self.memtable.iter() {
            if let Some(lim) = scan_limit {
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
