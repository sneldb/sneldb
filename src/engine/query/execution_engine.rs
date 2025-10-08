use crate::engine::core::memory::passive_buffer_set::PassiveBufferSet;
use crate::engine::core::read::execution_step::ExecutionStep;
use crate::engine::core::read::memtable_query::MemTableQuery;
use crate::engine::core::read::result::{
    AggregateResult, ColumnSpec, QueryResult, SelectionResult,
};
use crate::engine::core::read::segment_aggregate_runner::SegmentAggregateRunner;
use crate::engine::core::read::sink::{AggregateSink, ResultSink};
use crate::engine::core::{MemTable, QueryCaches, QueryExecution, QueryPlan};
use crate::engine::errors::QueryExecutionError;
use crate::engine::schema::registry::SchemaRegistry;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct ScanContext<'a> {
    pub registry: &'a Arc<tokio::sync::RwLock<SchemaRegistry>>,
    pub segment_dir_abs: PathBuf,
    pub segment_ids: &'a Arc<std::sync::RwLock<Vec<String>>>,
    pub memtable: &'a MemTable,
    pub passive_buffers: &'a Arc<PassiveBufferSet>,
    pub caches: QueryCaches,
}

impl<'a> ScanContext<'a> {
    pub fn new(
        registry: &'a Arc<tokio::sync::RwLock<SchemaRegistry>>,
        segment_dir_abs: PathBuf,
        segment_ids: &'a Arc<std::sync::RwLock<Vec<String>>>,
        memtable: &'a MemTable,
        passive_buffers: &'a Arc<PassiveBufferSet>,
    ) -> Self {
        let caches = QueryCaches::new_abs(segment_dir_abs.clone());
        Self {
            registry,
            segment_dir_abs,
            segment_ids,
            memtable,
            passive_buffers,
            caches,
        }
    }
}

pub struct ExecutionEngine;

impl ExecutionEngine {
    pub async fn execute<'a>(
        plan: &QueryPlan,
        ctx: &ScanContext<'a>,
    ) -> Result<QueryResult, QueryExecutionError> {
        if plan.aggregate_plan.is_some() {
            Self::run_aggregation(plan, ctx).await
        } else {
            Self::run_selection(plan, ctx).await
        }
    }

    async fn run_selection<'a>(
        plan: &QueryPlan,
        ctx: &ScanContext<'a>,
    ) -> Result<QueryResult, QueryExecutionError> {
        let passives = ctx.passive_buffers.non_empty().await;
        let passive_refs: Vec<&Arc<Mutex<MemTable>>> = passives.iter().collect();

        let mut execution = QueryExecution::new(plan)
            .with_memtable(ctx.memtable)
            .with_passive_memtables(passive_refs)
            .with_caches(&ctx.caches);

        let events = execution.run().await?;
        let rows: Vec<Vec<serde_json::Value>> = events
            .into_iter()
            .map(|e| {
                vec![
                    serde_json::json!(e.context_id),
                    serde_json::json!(e.event_type),
                    serde_json::json!(e.timestamp),
                    e.payload,
                ]
            })
            .collect();
        let cols = vec![
            ColumnSpec {
                name: "context_id".to_string(),
                logical_type: "String".to_string(),
            },
            ColumnSpec {
                name: "event_type".to_string(),
                logical_type: "String".to_string(),
            },
            ColumnSpec {
                name: "timestamp".to_string(),
                logical_type: "Timestamp".to_string(),
            },
            ColumnSpec {
                name: "payload".to_string(),
                logical_type: "Object".to_string(),
            },
        ];
        Ok(QueryResult::Selection(SelectionResult {
            columns: cols,
            rows,
        }))
    }

    async fn run_aggregation<'a>(
        plan: &QueryPlan,
        ctx: &ScanContext<'a>,
    ) -> Result<QueryResult, QueryExecutionError> {
        let mut sink = AggregateSink::from_query_plan(plan, plan.aggregate_plan.as_ref().unwrap());
        // Memtable path
        let mem_events = MemTableQuery::new(ctx.memtable, plan).query();
        for e in &mem_events {
            sink.on_event(e);
        }
        // Segment path
        SegmentAggregateRunner::new(
            plan,
            plan.filter_plans
                .iter()
                .map(|f| ExecutionStep::new(f.clone(), plan))
                .collect(),
        )
        .with_caches(Some(&ctx.caches))
        .run(&mut sink)
        .await;

        let partial = sink.into_partial();

        let result = AggregateResult {
            group_by: partial.group_by,
            time_bucket: partial.time_bucket,
            specs: partial.specs,
            groups: partial.groups,
        };
        Ok(QueryResult::Aggregation(result))
    }
}
