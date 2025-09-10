use crate::engine::core::{
    Event, ExecutionStep, MemTable, MemTableQueryRunner, QueryPlan, SegmentQueryRunner,
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

        // Step 2: Disk segments
        let segment_events = SegmentQueryRunner::new(self.plan, self.steps.clone())
            .run()
            .await;
        debug!(
            target: "sneldb::query_exec",
            segment_hits = segment_events.len(),
            "Retrieved events from disk segments"
        );
        events.extend(segment_events);

        info!(
            target: "sneldb::query_exec",
            total_matches = events.len(),
            "Query execution completed"
        );

        Ok(events)
    }
}
