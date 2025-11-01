use std::sync::Arc;

use crate::engine::core::memory::passive_buffer_set::PassiveBufferSet;
use crate::engine::core::read::cache::query_caches::QueryCaches;
use crate::engine::core::read::flow::{BatchPool, FlowContext, FlowMetrics, FlowTelemetry};
use crate::engine::core::{MemTable, QueryPlan};
use crate::engine::errors::QueryExecutionError;

/// Shared context for orchestrating streaming scans. Encapsulates the query plan,
/// flow configuration, cached resources, and the passive memtable snapshot used
/// by the streaming operators.
pub struct StreamingContext {
    plan: Arc<QueryPlan>,
    flow_ctx: Arc<FlowContext>,
    caches: Arc<QueryCaches>,
    passive_snapshot: Vec<Arc<tokio::sync::Mutex<MemTable>>>,
    batch_size: usize,
    effective_limit: Option<usize>,
}

impl StreamingContext {
    pub async fn new(
        plan: Arc<QueryPlan>,
        passive_buffers: &Arc<PassiveBufferSet>,
        batch_size: usize,
    ) -> Result<Self, QueryExecutionError> {
        let pool = BatchPool::new(batch_size)
            .map_err(|err| QueryExecutionError::ExprEval(err.to_string()))?;
        let metrics = FlowMetrics::new();
        let flow_ctx = Arc::new(FlowContext::new(
            batch_size,
            pool,
            metrics,
            None::<&str>,
            FlowTelemetry::default(),
        ));

        let passive_snapshot = passive_buffers.non_empty().await;
        let caches = Arc::new(QueryCaches::new_abs(plan.segment_base_dir.clone()));
        let effective_limit = plan.limit().map(|limit| limit + plan.offset().unwrap_or(0));

        Ok(Self {
            plan,
            flow_ctx,
            caches,
            passive_snapshot,
            batch_size,
            effective_limit,
        })
    }

    pub fn plan(&self) -> &QueryPlan {
        self.plan.as_ref()
    }

    pub fn plan_arc(&self) -> Arc<QueryPlan> {
        Arc::clone(&self.plan)
    }

    pub fn flow_context(&self) -> Arc<FlowContext> {
        Arc::clone(&self.flow_ctx)
    }

    pub fn caches(&self) -> Arc<QueryCaches> {
        Arc::clone(&self.caches)
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn effective_limit(&self) -> Option<usize> {
        self.effective_limit
    }

    pub fn metrics(&self) -> Arc<FlowMetrics> {
        Arc::clone(self.flow_ctx.metrics())
    }

    pub fn passive_refs(&self) -> Vec<&Arc<tokio::sync::Mutex<MemTable>>> {
        self.passive_snapshot.iter().collect()
    }
}
