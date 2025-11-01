use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc::channel;
use tracing::info;

use crate::command::handlers::query::context::QueryContext;
use crate::command::handlers::query::dispatch::BatchDispatch;
use crate::command::handlers::query::planner::PlanOutcome;
use crate::command::handlers::shard_command_builder::ShardCommandBuilder;
use crate::engine::core::read::result::QueryResult;
use crate::engine::shard::message::ShardMessage;

pub struct BatchShardDispatcher;

impl BatchShardDispatcher {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl BatchDispatch for BatchShardDispatcher {
    async fn dispatch(
        &self,
        ctx: &QueryContext<'_>,
        plan: &PlanOutcome,
    ) -> Result<Vec<QueryResult>, String> {
        let (tx, mut rx) = channel(4096);
        let builder = ShardCommandBuilder::new(ctx.command, plan.picked_zones.as_ref());

        for shard in ctx.shard_manager.all_shards() {
            let tx_clone = tx.clone();
            info!(
                target: "sneldb::query_pipeline",
                shard_id = shard.id,
                "Dispatching buffered query to shard"
            );

            let command = builder.build_for_shard(shard.id);

            let _ = shard
                .tx
                .send(ShardMessage::Query(
                    command.into_owned(),
                    tx_clone,
                    Arc::clone(&ctx.registry),
                ))
                .await;
        }

        drop(tx);

        let mut results = Vec::new();
        while let Some(result) = rx.recv().await {
            results.push(result);
        }

        Ok(results)
    }
}
