use std::sync::Arc;

use crate::command::handlers::query_batch_stream::QueryBatchStream;
use crate::command::types::Command;
use crate::engine::core::read::result::QueryResult;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use tokio::sync::RwLock;

use super::context::QueryContext;
use super::dispatch::{
    BatchDispatch, BatchShardDispatcher, StreamingDispatch, StreamingShardDispatcher,
};
use super::merge::{BatchMerger, StreamMergerKind};
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

    pub fn streaming_supported(&self) -> bool {
        matches!(
            self.ctx.command,
            Command::Query {
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
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
}
