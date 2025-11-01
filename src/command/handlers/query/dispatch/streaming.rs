use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::oneshot;
use tracing::info;

use crate::command::handlers::query::context::QueryContext;
use crate::command::handlers::query::dispatch::StreamingDispatch;
use crate::command::handlers::query::planner::PlanOutcome;
use crate::command::handlers::shard_command_builder::ShardCommandBuilder;
use crate::engine::core::read::flow::shard_pipeline::ShardFlowHandle;
use crate::engine::shard::message::ShardMessage;

/// Dispatches streaming query plans to every shard and collects the resulting
/// flow handles.
pub struct StreamingShardDispatcher;

impl StreamingShardDispatcher {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl StreamingDispatch for StreamingShardDispatcher {
    /// Sends a `QueryStream` message to each shard and awaits the resulting
    /// `ShardFlowHandle`, propagating shard-specific errors verbatim.
    async fn dispatch(
        &self,
        ctx: &QueryContext<'_>,
        plan: &PlanOutcome,
    ) -> Result<Vec<ShardFlowHandle>, String> {
        let builder = ShardCommandBuilder::new(ctx.command, plan.picked_zones.as_ref());
        let mut pending = Vec::new();

        for shard in ctx.shard_manager.all_shards() {
            let (response_tx, response_rx) = oneshot::channel();
            info!(
                target: "sneldb::query_pipeline",
                shard_id = shard.id,
                "Dispatching streaming query to shard"
            );

            let command = builder.build_for_shard(shard.id);

            shard
                .tx
                .send(ShardMessage::QueryStream {
                    command: command.into_owned(),
                    response: response_tx,
                    registry: Arc::clone(&ctx.registry),
                })
                .await
                .map_err(|error| {
                    format!(
                        "failed to send streaming command to shard {}: {}",
                        shard.id, error
                    )
                })?;

            pending.push((shard.id, response_rx));
        }

        let mut handles = Vec::new();
        for (shard_id, rx) in pending {
            match rx.await {
                Ok(Ok(handle)) => handles.push(handle),
                Ok(Err(err)) => {
                    return Err(format!("shard {} streaming error: {}", shard_id, err));
                }
                Err(_) => {
                    return Err(format!(
                        "shard {} dropped streaming response channel",
                        shard_id
                    ));
                }
            }
        }

        Ok(handles)
    }
}
