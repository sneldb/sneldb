use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::oneshot;
use tracing::info;

use crate::command::handlers::query::context::QueryContext;
use crate::command::handlers::query::dispatch::StreamingDispatch;
use crate::command::handlers::query::planner::{PlanOutcome, QueryPlannerBuilder};
use crate::command::handlers::shard_command_builder::ShardCommandBuilder;
use crate::command::types::{Command, EventSequence};
use crate::engine::core::read::flow::shard_pipeline::ShardFlowHandle;
use crate::engine::core::read::sequence::utils::transform_where_clause_for_event_type;
use crate::engine::shard::message::ShardMessage;

/// Groups handles by event type for sequence queries.
pub(crate) struct SequenceHandles {
    pub handles_by_type: HashMap<String, Vec<ShardFlowHandle>>,
    pub event_types: Vec<String>,
}

/// Dispatches sequence queries by splitting them into sub-queries per event type
/// and sending each through the existing streaming infrastructure.
pub struct SequenceStreamingDispatcher;

impl SequenceStreamingDispatcher {
    pub fn new() -> Self {
        Self
    }

    /// Extracts event types from the sequence.
    fn extract_event_types(sequence: &EventSequence) -> Vec<String> {
        let mut event_types = vec![sequence.head.event.clone()];
        for (_link, target) in &sequence.links {
            event_types.push(target.event.clone());
        }
        event_types
    }

    /// Creates a sub-query command for a specific event type.
    ///
    /// Transforms the WHERE clause to only include conditions relevant to this event type.
    fn create_sub_query(
        base_command: &Command,
        event_type: &str,
    ) -> Result<Command, String> {
        let Command::Query {
            context_id: _,
            since,
            time_field,
            where_clause,
            limit: _,
            offset: _,
            order_by,
            return_fields,
            ..
        } = base_command
        else {
            return Err("Base command must be a Query".to_string());
        };

        // Transform WHERE clause for this event type
        // This removes event-prefixed conditions that don't apply to this event type
        // and converts event-prefixed conditions to plain field names
        let transformed_where_clause = where_clause
            .as_ref()
            .and_then(|expr| transform_where_clause_for_event_type(expr, event_type));

        // Create a simple query for this event type without sequence info
        // NOTE: We don't pass LIMIT to sub-queries - they should return all matching events.
        // The LIMIT will be applied when matching sequences, not when collecting events.
        Ok(Command::Query {
            event_type: event_type.to_string(),
            context_id: None, // Clear context_id for sequence queries - they work across all contexts
            since: since.clone(),
            time_field: time_field.clone(),
            sequence_time_field: None,
            where_clause: transformed_where_clause,
            limit: None, // Don't limit sub-queries - we need all events to match sequences
            offset: None, // Don't apply offset to sub-queries
            order_by: order_by.clone(),
            picked_zones: None, // Will be set by planner
            return_fields: return_fields.clone(),
            link_field: None, // Not needed for sub-queries
            aggs: None,
            time_bucket: None,
            group_by: None,
            event_sequence: None, // Remove sequence info for sub-queries
        })
    }
}

#[async_trait]
impl StreamingDispatch for SequenceStreamingDispatcher {
    /// Splits sequence query into sub-queries per event type and dispatches each
    /// through the streaming infrastructure. Returns handles grouped by event type.
    async fn dispatch(
        &self,
        ctx: &QueryContext<'_>,
        _plan: &PlanOutcome,
    ) -> Result<Vec<ShardFlowHandle>, String> {
        // Extract sequence information
        let event_sequence = match &ctx.command {
            Command::Query {
                event_sequence: Some(seq),
                link_field: Some(_),
                ..
            } => seq,
            _ => return Err("Sequence dispatcher requires sequence query".to_string()),
        };

        let event_types = Self::extract_event_types(event_sequence);
        info!(
            target: "sneldb::sequence::streaming",
            event_types = ?event_types,
            "Splitting sequence query into sub-queries"
        );

        // Dispatch sub-queries for each event type
        let mut handles_by_type = HashMap::new();

        for event_type in &event_types {
            // Create sub-query command
            let sub_command = Self::create_sub_query(ctx.command, event_type)?;

            // Create new context for sub-query
            let sub_ctx = QueryContext::new(
                &sub_command,
                ctx.shard_manager,
                Arc::clone(&ctx.registry),
            );

            // Build plan for sub-query
            let planner = QueryPlannerBuilder::new(&sub_command).build();
            let sub_plan = planner.build_plan(&sub_ctx).await?;

            // Dispatch to all shards using standard streaming dispatcher logic
            let builder = ShardCommandBuilder::new(&sub_command, sub_plan.picked_zones.as_ref());
            let mut pending = Vec::new();

            for shard in ctx.shard_manager.all_shards() {
                let (response_tx, response_rx) = oneshot::channel();
                if tracing::enabled!(tracing::Level::INFO) {
                    info!(
                        target: "sneldb::sequence::streaming",
                        shard_id = shard.id,
                        event_type = %event_type,
                        "Dispatching sub-query to shard"
                    );
                }

                let command = builder.build_for_shard(shard.id);

                shard
                    .tx
                    .send(ShardMessage::QueryStream {
                        command: command.into_owned(),
                        metadata: if ctx.metadata.is_empty() {
                            None
                        } else {
                            Some(ctx.metadata.clone())
                        },
                        response: response_tx,
                        registry: Arc::clone(&ctx.registry),
                    })
                    .await
                    .map_err(|error| {
                        format!(
                            "failed to send sequence sub-query to shard {}: {}",
                            shard.id, error
                        )
                    })?;

                pending.push((shard.id, response_rx));
            }

            // Collect handles for this event type
            let mut type_handles = Vec::new();
            for (shard_id, rx) in pending {
                match rx.await {
                    Ok(Ok(handle)) => type_handles.push(handle),
                    Ok(Err(err)) => {
                        return Err(format!(
                            "shard {} streaming error for event type {}: {}",
                            shard_id, event_type, err
                        ));
                    }
                    Err(_) => {
                        return Err(format!(
                            "shard {} dropped streaming response channel for event type {}",
                            shard_id, event_type
                        ));
                    }
                }
            }
            handles_by_type.insert(event_type.clone(), type_handles);
        }

        // For sequence queries, we need grouped handles, so we return empty here
        // The actual dispatch happens via dispatch_grouped_internal
        Ok(Vec::new())
    }
}

impl SequenceStreamingDispatcher {
    /// Dispatches sequence queries and returns handles grouped by event type.
    /// This is a specialized method for sequence queries that preserves event type grouping.
    pub(crate) async fn dispatch_grouped_internal(
        &self,
        ctx: &QueryContext<'_>,
    ) -> Result<SequenceHandles, String> {
        // Extract sequence information
        let event_sequence = match &ctx.command {
            Command::Query {
                event_sequence: Some(seq),
                link_field: Some(_),
                ..
            } => seq,
            _ => return Err("Sequence dispatcher requires sequence query".to_string()),
        };

        let event_types = Self::extract_event_types(event_sequence);
        info!(
            target: "sneldb::sequence::streaming",
            event_types = ?event_types,
            "Splitting sequence query into sub-queries (grouped)"
        );

        // Dispatch sub-queries for each event type and group handles
        let mut handles_by_type = HashMap::new();

        for event_type in &event_types {
            // Create sub-query command
            let sub_command = Self::create_sub_query(ctx.command, event_type)?;

            // Create new context for sub-query
            let sub_ctx = QueryContext::new(
                &sub_command,
                ctx.shard_manager,
                Arc::clone(&ctx.registry),
            );

            // Build plan for sub-query
            let planner = QueryPlannerBuilder::new(&sub_command).build();
            let sub_plan = planner.build_plan(&sub_ctx).await?;

            // Dispatch to all shards
            let builder = ShardCommandBuilder::new(&sub_command, sub_plan.picked_zones.as_ref());
            let mut pending = Vec::new();

            for shard in ctx.shard_manager.all_shards() {
                let (response_tx, response_rx) = oneshot::channel();
                if tracing::enabled!(tracing::Level::INFO) {
                    info!(
                        target: "sneldb::sequence::streaming",
                        shard_id = shard.id,
                        event_type = %event_type,
                        "Dispatching sub-query to shard"
                    );
                }

                let command = builder.build_for_shard(shard.id);

                shard
                    .tx
                    .send(ShardMessage::QueryStream {
                        command: command.into_owned(),
                        metadata: if ctx.metadata.is_empty() {
                            None
                        } else {
                            Some(ctx.metadata.clone())
                        },
                        response: response_tx,
                        registry: Arc::clone(&ctx.registry),
                    })
                    .await
                    .map_err(|error| {
                        format!(
                            "failed to send sequence sub-query to shard {}: {}",
                            shard.id, error
                        )
                    })?;

                pending.push((shard.id, response_rx));
            }

            // Collect handles for this event type
            let mut type_handles = Vec::new();
            for (shard_id, rx) in pending {
                match rx.await {
                    Ok(Ok(handle)) => type_handles.push(handle),
                    Ok(Err(err)) => {
                        return Err(format!(
                            "shard {} streaming error for event type {}: {}",
                            shard_id, event_type, err
                        ));
                    }
                    Err(_) => {
                        return Err(format!(
                            "shard {} dropped streaming response channel for event type {}",
                            shard_id, event_type
                        ));
                    }
                }
            }
            handles_by_type.insert(event_type.clone(), type_handles);
        }

        Ok(SequenceHandles {
            handles_by_type,
            event_types,
        })
    }
}

