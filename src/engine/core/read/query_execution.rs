use crate::engine::core::{
    Event, ExecutionStep, MemTable, MemTableQueryRunner, QueryCaches, QueryPlan, SegmentQueryRunner,
};
use crate::engine::errors::QueryExecutionError;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// Represents the execution steps for a query
#[derive(Debug, Clone)]
pub struct QueryExecution<'a> {
    plan: &'a QueryPlan,
    steps: Vec<ExecutionStep<'a>>,
    metadata: HashMap<String, String>,
    memtable: Option<&'a MemTable>,
    passive_memtables: Vec<&'a Arc<tokio::sync::Mutex<MemTable>>>,
    caches: Option<&'a QueryCaches>,
}

impl<'a> QueryExecution<'a> {
    pub fn new(plan: &'a QueryPlan) -> Self {
        let steps: Vec<ExecutionStep<'a>> = plan
            .filter_plans
            .iter()
            .map(|filter| ExecutionStep::new(filter.clone(), plan))
            .collect();

        debug!(
            target: "sneldb::query_exec",
            step_count = steps.len(),
            "Initialized query execution with steps"
        );

        Self {
            plan,
            steps,
            metadata: HashMap::new(),
            memtable: None,
            passive_memtables: Vec::new(),
            caches: None,
        }
    }

    pub fn with_memtable(mut self, memtable: &'a MemTable) -> Self {
        self.memtable = Some(memtable);
        self
    }

    pub fn with_passive_memtables(
        mut self,
        passives: Vec<&'a Arc<tokio::sync::Mutex<MemTable>>>,
    ) -> Self {
        self.passive_memtables = passives;
        self
    }

    pub fn with_caches(mut self, caches: &'a QueryCaches) -> Self {
        self.caches = Some(caches);
        self
    }

    pub fn step_count(&self) -> usize {
        self.steps.len()
    }

    pub fn steps(&self) -> &[ExecutionStep<'a>] {
        &self.steps
    }

    pub fn find_step_for_column(&self, column: &str) -> Option<&ExecutionStep<'a>> {
        self.steps.iter().find(|step| step.filter.column == column)
    }

    pub fn set_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    pub fn get_metadata(&self, key: &str) -> Option<&str> {
        self.metadata.get(key).map(String::as_str)
    }

    pub async fn event_type_uid(&self) -> Option<String> {
        self.plan.event_type_uid().await
    }

    pub async fn run(&mut self) -> Result<Vec<Event>, QueryExecutionError> {
        info!(
            target: "sneldb::query_exec",
            event_type = %self.plan.event_type(),
            step_count = self.steps.len(),
            "Starting query execution"
        );

        let mut events = Vec::new();

        // Step 1: Memtable
        let memtable_events =
            MemTableQueryRunner::new(self.memtable, &self.passive_memtables, self.plan)
                .run()
                .await;
        debug!(
            target: "sneldb::query_exec",
            memtable_hits = memtable_events.len(),
            "Retrieved events from MemTable"
        );
        events.extend(memtable_events);

        // Early return only when no OFFSET is used; in that case LIMIT alone is safe to cap.
        if self.plan.offset().is_none() {
            if let Some(limit) = self.plan.limit() {
                if events.len() >= limit {
                    events.truncate(limit);
                    return Ok(events);
                }
            }
        }

        // Step 2: Disk segments
        // Cap segment fetch:
        // - With OFFSET: use window (offset+limit) remaining so we don't over-fetch
        // - Without OFFSET: use remaining LIMIT
        let remaining_limit = match (self.plan.offset(), self.plan.limit()) {
            (Some(_), Some(lim)) => {
                // For OFFSET queries, fetch up to full window from segments as well,
                // to avoid missing earlier rows after global sort.
                let window = lim.saturating_add(self.plan.offset().unwrap_or(0));
                Some(window)
            }
            (Some(_), None) => {
                // No LIMIT but OFFSET present — fetch all (no cap)
                None
            }
            (None, Some(lim)) => Some(lim.saturating_sub(events.len())),
            (None, None) => None,
        };

        let segment_events = SegmentQueryRunner::new(self.plan, self.steps.clone())
            .with_caches(self.caches)
            .with_limit(remaining_limit)
            .run()
            .await;
        debug!(
            target: "sneldb::query_exec",
            segment_hits = segment_events.len(),
            "Retrieved events from disk segments"
        );
        events.extend(segment_events);

        // Final cap only when no OFFSET: with OFFSET, let the handler sort and slice.
        if self.plan.offset().is_none() {
            if let Some(limit) = self.plan.limit() {
                if events.len() > limit {
                    events.truncate(limit);
                }
            }
        }

        info!(
            target: "sneldb::query_exec",
            total_matches = events.len(),
            "Query execution completed"
        );

        Ok(events)
    }
}
