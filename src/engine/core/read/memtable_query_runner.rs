use crate::engine::core::read::flow::shard_pipeline::{ShardFlowHandle, build_memtable_flow};
use crate::engine::core::read::flow::{FlowContext, FlowOperatorError};
use crate::engine::core::{Event, EventSorter, MemTable, MemTableQuery, QueryContext, QueryPlan};
use std::sync::Arc;
use tracing::{debug, info};

pub struct MemTableQueryRunner<'a> {
    memtable: Option<&'a MemTable>,
    passive_memtables: &'a Vec<&'a Arc<tokio::sync::Mutex<MemTable>>>,
    plan: &'a QueryPlan,
    limit: Option<usize>,
}

impl<'a> MemTableQueryRunner<'a> {
    pub fn new(
        memtable: Option<&'a MemTable>,
        passive_memtables: &'a Vec<&'a Arc<tokio::sync::Mutex<MemTable>>>,
        plan: &'a QueryPlan,
    ) -> Self {
        Self {
            memtable,
            passive_memtables,
            plan,
            limit: None,
        }
    }

    pub async fn run(&self) -> Vec<Event> {
        info!(target: "sneldb::query_memtable", "Running memtable query runner");

        // Extract query context from command
        let ctx = QueryContext::from_command(&self.plan.command);

        // Determine evaluation limit (defer if ordering required)
        let eval_limit = self.determine_eval_limit(&ctx);

        // Query memtables
        let mut events = self.query_memtables(eval_limit).await;

        // Sort events if ORDER BY is present
        if let Some(sorter) = self.create_sorter(&ctx) {
            sorter.sort(&mut events);
        }

        info!(
            target: "sneldb::query_memtable",
            count = events.len(),
            "Total events found in memory"
        );

        events
    }

    pub async fn stream(
        &self,
        ctx: Arc<FlowContext>,
        limit_override: Option<usize>,
    ) -> Result<Option<ShardFlowHandle>, FlowOperatorError> {
        let plan_arc = Arc::new(self.plan.clone());
        let memtable_arc = self.memtable.map(|m| Arc::new(m.clone()));
        let passive_arcs: Vec<_> = self
            .passive_memtables
            .iter()
            .map(|arc| Arc::clone(arc))
            .collect();

        let handle = build_memtable_flow(
            plan_arc,
            memtable_arc,
            passive_arcs,
            Arc::clone(&ctx),
            limit_override,
        )
        .await?;
        Ok(Some(handle))
    }

    /// Queries all memtables (active and passive) with optional limit.
    async fn query_memtables(&self, limit: Option<usize>) -> Vec<Event> {
        let mut events = Vec::new();

        if let Some(m) = self.memtable {
            let count = m.len();
            debug!(
                target: "sneldb::query_memtable",
                count,
                "Querying active MemTable"
            );
            let found = MemTableQuery::new(m, self.plan).with_limit(limit).query();
            debug!(
                target: "sneldb::query_memtable",
                found = found.len(),
                "Found events in active MemTable"
            );
            events.extend(found);
        }

        for pm in self.passive_memtables.iter() {
            debug!(target: "sneldb::query_memtable", "Acquiring lock for passive MemTable");
            let guard = pm.lock().await;
            let count = guard.len();
            debug!(target: "sneldb::query_memtable", count, "Querying passive MemTable");

            // Calculate remaining limit if applicable
            let remaining_limit = limit.map(|lim| lim.saturating_sub(events.len()));

            let found = MemTableQuery::new(&*guard, self.plan)
                .with_limit(remaining_limit)
                .query();
            debug!(
                target: "sneldb::query_memtable",
                found = found.len(),
                "Found events in passive MemTable"
            );
            events.extend(found);

            // Early exit if limit satisfied
            if let Some(lim) = limit {
                if events.len() >= lim {
                    break;
                }
            }
        }

        if events.is_empty() {
            info!(
                target: "sneldb::query_memtable",
                "No events found in any MemTable, fallback to disk"
            );
        }

        events
    }

    /// Determines the evaluation limit based on context.
    ///
    /// If ORDER BY is present, returns None to allow all events to be collected
    /// for proper sorting at the handler level (k-way merge). Otherwise, returns
    /// the configured limit for efficient truncation.
    fn determine_eval_limit(&self, ctx: &QueryContext) -> Option<usize> {
        if ctx.should_defer_limit() {
            None // Return ALL events for k-way merge
        } else {
            self.limit // Truncate during query
        }
    }

    /// Creates an EventSorter if ORDER BY is present in context.
    fn create_sorter(&self, ctx: &QueryContext) -> Option<EventSorter> {
        ctx.order_by.as_ref().map(EventSorter::from_order_spec)
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }
}
