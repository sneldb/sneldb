use std::sync::Arc;

use crate::command::handlers::query_batch_stream::QueryBatchStream;
use crate::command::types::Command;
use crate::engine::core::read::result::QueryResult;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use tokio::sync::RwLock;

use super::context::QueryContext;
use super::dispatch::{
    BatchDispatch, BatchShardDispatcher, SequenceStreamingDispatcher, StreamingDispatch,
    StreamingShardDispatcher,
};
use super::merge::{BatchMerger, SequenceStreamMerger, StreamMergerKind};
use super::planner::{QueryPlanner, QueryPlannerBuilder};

pub struct QueryExecutionPipeline<'a> {
    ctx: QueryContext<'a>,
    planner: Box<dyn QueryPlanner>,
}

impl<'a> QueryExecutionPipeline<'a> {
    pub fn new(
        command: &'a Command,
        shard_manager: &'a ShardManager,
        registry: Arc<RwLock<SchemaRegistry>>,
    ) -> Self {
        let ctx = QueryContext::new(command, shard_manager, registry);
        let planner = QueryPlannerBuilder::new(command).build();
        Self { ctx, planner }
    }

    pub fn with_metadata(mut self, metadata: std::collections::HashMap<String, String>) -> Self {
        self.ctx = QueryContext {
            command: self.ctx.command,
            shard_manager: self.ctx.shard_manager,
            registry: self.ctx.registry,
            metadata,
        };
        self
    }

    pub fn streaming_supported(&self) -> bool {
        true
    }

    pub fn is_sequence_query(&self) -> bool {
        matches!(
            self.ctx.command,
            Command::Query {
                event_sequence: Some(_),
                link_field: Some(_),
                ..
            }
        )
    }

    pub async fn execute(&self) -> Result<QueryResult, String> {
        let plan = self.planner.build_plan(&self.ctx).await?;
        let dispatcher = BatchShardDispatcher::new();
        let shard_results = dispatcher.dispatch(&self.ctx, &plan).await?;
        let merger = BatchMerger::for_command(self.ctx.command);
        merger.merge(&self.ctx, shard_results)
    }

    pub async fn execute_streaming(&self) -> Result<Option<QueryBatchStream>, String> {
        // Check if this is a sequence query
        if self.is_sequence_query() {
            return self.execute_sequence_streaming().await;
        }

        if !self.streaming_supported() {
            return Ok(None);
        }

        let plan = self.planner.build_plan(&self.ctx).await?;
        let dispatcher = StreamingShardDispatcher::new();
        let handles = dispatcher.dispatch(&self.ctx, &plan).await?;
        let merger = StreamMergerKind::for_context(&self.ctx);
        let stream = merger.merge(&self.ctx, handles)?;
        Ok(Some(stream))
    }

    /// Executes sequence queries using the streaming infrastructure.
    async fn execute_sequence_streaming(&self) -> Result<Option<QueryBatchStream>, String> {
        let Command::Query {
            event_sequence: Some(event_sequence),
            link_field: Some(link_field),
            sequence_time_field,
            limit,
            ..
        } = &self.ctx.command
        else {
            return Err("Sequence query requires event_sequence and link_field".to_string());
        };

        let sequence_time_field = sequence_time_field
            .as_ref()
            .map(|s| s.clone())
            .unwrap_or_else(|| "timestamp".to_string());

        // Use sequence dispatcher to split into sub-queries
        let dispatcher = SequenceStreamingDispatcher::new();
        let sequence_handles = dispatcher.dispatch_grouped_internal(&self.ctx).await?;

        // Merge using sequence merger
        let merger = SequenceStreamMerger::new(
            event_sequence.clone(),
            link_field.clone(),
            sequence_time_field,
            limit.map(|l| l as usize),
        );

        let stream = merger
            .merge(&self.ctx, sequence_handles.handles_by_type)
            .await?;
        Ok(Some(stream))
    }
}
