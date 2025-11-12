use std::sync::Arc;

use tokio::task::JoinHandle;
use tracing::{debug, error};

use crate::command::types::Command;
use crate::engine::core::Event;
use crate::engine::core::MemTable;
use crate::engine::core::QueryCaches;
use crate::engine::core::QueryPlan;
use crate::engine::core::read::execution_step::ExecutionStep;
use crate::engine::core::read::flow::operators::{
    AggregateOp, AggregateOpConfig, MemTableSource, MemTableSourceConfig, ProjectOp, Projection,
    SegmentSource, SegmentSourceConfig, aggregate_output_schema,
};
use crate::engine::core::read::flow::{
    BatchReceiver, BatchSchema, FlowChannel, FlowContext, FlowOperator, FlowOperatorError,
    FlowSource,
};
use crate::engine::core::read::segment_query_runner::SegmentQueryRunner;
use crate::engine::schema::registry::SchemaRegistry;

pub const DEFAULT_MEMTABLE_COLUMNS: &[&str] =
    &["context_id", "event_type", "timestamp", "event_id"];

/// Computes the output schema and projection indices for RETURN fields.
/// Returns (output_schema, projection_indices) where projection_indices maps
/// from input column positions to output column positions.
fn compute_return_projection(
    input_schema: &BatchSchema,
    return_fields: Option<&[String]>,
    registry: &SchemaRegistry,
    event_type: &str,
) -> Result<(Arc<BatchSchema>, Vec<usize>), FlowOperatorError> {
    let return_fields = match return_fields {
        None | Some([]) => {
            // No RETURN fields specified - identity projection
            let indices: Vec<usize> = (0..input_schema.column_count()).collect();
            return Ok((Arc::new(input_schema.clone()), indices));
        }
        Some(fields) => fields,
    };

    // Core fields that are always included
    let core_fields = vec![
        "context_id".to_string(),
        "event_type".to_string(),
        "timestamp".to_string(),
        "event_id".to_string(),
    ];

    // Get payload fields from schema
    let payload_fields: Vec<String> = registry
        .get(event_type)
        .map(|schema| {
            let mut fields: Vec<String> = schema.fields().cloned().collect();
            fields.sort();
            fields
        })
        .unwrap_or_default();

    let payload_set: std::collections::HashSet<String> = payload_fields.iter().cloned().collect();

    // Build output column list: core fields first, then RETURN payload fields
    let mut output_columns = Vec::new();
    let mut output_indices = Vec::new();

    // Add core fields (always in order: context_id, event_type, timestamp, event_id)
    for core_field in &core_fields {
        if let Some(idx) = input_schema
            .columns()
            .iter()
            .position(|c| c.name == *core_field)
        {
            output_columns.push(input_schema.columns()[idx].clone());
            output_indices.push(idx);
        }
    }

    // Add RETURN payload fields
    for return_field in return_fields {
        if payload_set.contains(return_field) {
            if let Some(idx) = input_schema
                .columns()
                .iter()
                .position(|c| c.name == *return_field)
            {
                // Avoid duplicates
                if !output_indices.contains(&idx) {
                    output_columns.push(input_schema.columns()[idx].clone());
                    output_indices.push(idx);
                }
            }
        }
    }

    let output_schema =
        Arc::new(BatchSchema::new(output_columns).map_err(|e| {
            FlowOperatorError::Batch(format!("failed to build output schema: {}", e))
        })?);

    Ok((output_schema, output_indices))
}

/// Handle returned by shard pipeline builders. Owns the downstream receiver,
/// resulting batch schema, and any background tasks driving the flow.
pub struct ShardFlowHandle {
    pub receiver: BatchReceiver,
    pub schema: Arc<BatchSchema>,
    tasks: Vec<JoinHandle<()>>,
}

impl ShardFlowHandle {
    pub fn new(
        receiver: BatchReceiver,
        schema: Arc<BatchSchema>,
        tasks: Vec<JoinHandle<()>>,
    ) -> Self {
        Self {
            receiver,
            schema,
            tasks,
        }
    }

    pub fn tasks(&self) -> &[JoinHandle<()>] {
        &self.tasks
    }

    pub fn into_parts(self) -> (BatchReceiver, Arc<BatchSchema>, Vec<JoinHandle<()>>) {
        (self.receiver, self.schema, self.tasks)
    }
}

/// Builds a flow that streams results from the active and passive memtables,
/// applying optional aggregates and projections inline.
pub async fn build_memtable_flow(
    plan: Arc<QueryPlan>,
    memtable: Option<Arc<MemTable>>,
    passive_memtables: Vec<Arc<tokio::sync::Mutex<MemTable>>>,
    ctx: Arc<FlowContext>,
    limit_override: Option<usize>,
) -> Result<ShardFlowHandle, FlowOperatorError> {
    let mandatory: Vec<String> = DEFAULT_MEMTABLE_COLUMNS
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    let columns = MemTableSource::compute_columns(plan.as_ref(), &mandatory).await?;
    let schema = Arc::new(
        BatchSchema::new(columns.clone()).map_err(|e| FlowOperatorError::Batch(e.to_string()))?,
    );

    let source_config = MemTableSourceConfig {
        plan: Arc::clone(&plan),
        memtable,
        passive_memtables,
        limit_override,
        mandatory_columns: mandatory.clone(),
    };

    let source = MemTableSource::new(source_config);
    let metrics = Arc::clone(ctx.metrics());
    let (source_tx, mut current_rx) = FlowChannel::bounded(ctx.batch_size(), Arc::clone(&metrics));
    let mut tasks: Vec<JoinHandle<()>> = Vec::new();

    let source_ctx = Arc::clone(&ctx);
    tasks.push(tokio::spawn(async move {
        if let Err(err) = source.run(source_tx, source_ctx).await {
            // ChannelClosed is expected when LIMIT is reached early - don't log as error
            match &err {
                FlowOperatorError::ChannelClosed => {
                    debug!(target: "sneldb::flow", "MemTable source stopped (channel closed, likely LIMIT reached)");
                }
                _ => {
                    error!(target: "sneldb::flow", error = %err, "MemTable source failed");
                }
            }
        }
    }));

    let mut final_schema = Arc::clone(&schema);

    if let Some(aggregate_plan) = plan.aggregate_plan.clone() {
        let aggregate_config = AggregateOpConfig {
            plan: Arc::clone(&plan),
            aggregate: aggregate_plan.clone(),
        };
        let aggregate = AggregateOp::new(aggregate_config);
        let (agg_tx, agg_rx) = FlowChannel::bounded(ctx.batch_size(), Arc::clone(&metrics));
        let agg_ctx = Arc::clone(&ctx);
        tasks.push(tokio::spawn(async move {
            if let Err(err) = aggregate.run(current_rx, agg_tx, agg_ctx).await {
                // ChannelClosed is expected when LIMIT is reached early - don't log as error
                match &err {
                    FlowOperatorError::ChannelClosed => {
                        debug!(target: "sneldb::flow", "Aggregate operator stopped (channel closed, likely LIMIT reached)");
                    }
                    _ => {
                        error!(target: "sneldb::flow", error = %err, "Aggregate operator failed");
                    }
                }
            }
        }));
        current_rx = agg_rx;
        final_schema = Arc::new(
            BatchSchema::new(aggregate_output_schema(&aggregate_plan))
                .map_err(|e| FlowOperatorError::Batch(e.to_string()))?,
        );
    }

    // Compute projection for RETURN fields if specified
    let projection = if let Command::Query {
        return_fields,
        event_type,
        ..
    } = &plan.command
    {
        let registry = plan.registry.read().await;
        match compute_return_projection(
            &final_schema,
            return_fields.as_deref(),
            &registry,
            event_type,
        ) {
            Ok((output_schema, indices)) => {
                final_schema = output_schema;
                Projection {
                    indices,
                    schema: Arc::clone(&final_schema),
                }
            }
            Err(e) => {
                return Err(e);
            }
        }
    } else {
        Projection {
            indices: (0..final_schema.column_count()).collect(),
            schema: Arc::clone(&final_schema),
        }
    };

    // Optimize: Skip ProjectOp if it's an identity projection (all columns in same order)
    // This avoids unnecessary cloning of all values
    if projection.is_identity() {
        // Identity projection - just pass through batches without cloning
        Ok(ShardFlowHandle::new(current_rx, final_schema, tasks))
    } else {
        let projector = ProjectOp::new(projection);
        let (proj_tx, proj_rx) = FlowChannel::bounded(ctx.batch_size(), Arc::clone(&metrics));
        let proj_ctx = Arc::clone(&ctx);
        tasks.push(tokio::spawn(async move {
            if let Err(err) = projector.run(current_rx, proj_tx, proj_ctx).await {
                // ChannelClosed is expected when LIMIT is reached early - don't log as error
                match &err {
                    FlowOperatorError::ChannelClosed => {
                        debug!(target: "sneldb::flow", "Projection operator stopped (channel closed, likely LIMIT reached)");
                    }
                    _ => {
                        error!(target: "sneldb::flow", error = %err, "Projection operator failed");
                    }
                }
            }
        }));

        Ok(ShardFlowHandle::new(proj_rx, final_schema, tasks))
    }
}

/// Hydrates a bounded set of events from disk segments and exposes them as a
/// streaming flow. Used when a shard has already enumerated specific events.
pub async fn build_segment_flow(
    plan: Arc<QueryPlan>,
    mut events: Vec<Event>,
    ctx: Arc<FlowContext>,
) -> Result<Option<ShardFlowHandle>, FlowOperatorError> {
    if events.is_empty() {
        return Ok(None);
    }

    if let Some(limit) = plan.limit() {
        if events.len() > limit {
            events.truncate(limit);
        }
    }

    let mandatory: Vec<String> = DEFAULT_MEMTABLE_COLUMNS
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    let columns = MemTableSource::compute_columns(plan.as_ref(), &mandatory).await?;
    let schema = Arc::new(
        BatchSchema::new(columns.clone()).map_err(|e| FlowOperatorError::Batch(e.to_string()))?,
    );

    let source_config = SegmentSourceConfig {
        events,
        schema: Arc::clone(&schema),
    };

    let source = SegmentSource::new(source_config);
    let metrics = Arc::clone(ctx.metrics());
    let (source_tx, current_rx) = FlowChannel::bounded(ctx.batch_size(), Arc::clone(&metrics));
    let mut tasks: Vec<JoinHandle<()>> = Vec::new();

    let source_ctx = Arc::clone(&ctx);
    tasks.push(tokio::spawn(async move {
        if let Err(err) = source.run(source_tx, source_ctx).await {
            // ChannelClosed is expected when LIMIT is reached early - don't log as error
            match &err {
                FlowOperatorError::ChannelClosed => {
                    debug!(target: "sneldb::flow", "Segment source stopped (channel closed, likely LIMIT reached)");
                }
                _ => {
                    error!(target: "sneldb::flow", error = %err, "Segment source failed");
                }
            }
        }
    }));

    let projection = Projection {
        indices: (0..schema.column_count()).collect(),
        schema: Arc::clone(&schema),
    };

    // Optimize: Skip ProjectOp if it's an identity projection (all columns in same order)
    if projection.is_identity() {
        // Identity projection - just pass through batches without cloning
        Ok(Some(ShardFlowHandle::new(current_rx, schema, tasks)))
    } else {
        let projector = ProjectOp::new(projection);
        let (proj_tx, proj_rx) = FlowChannel::bounded(ctx.batch_size(), Arc::clone(&metrics));
        let proj_ctx = Arc::clone(&ctx);
        tasks.push(tokio::spawn(async move {
            if let Err(err) = projector.run(current_rx, proj_tx, proj_ctx).await {
                // ChannelClosed is expected when LIMIT is reached early - don't log as error
                match &err {
                    FlowOperatorError::ChannelClosed => {
                        debug!(target: "sneldb::flow", "Segment projection stopped (channel closed, likely LIMIT reached)");
                    }
                    _ => {
                        error!(target: "sneldb::flow", error = %err, "Segment projection failed");
                    }
                }
            }
        }));

        Ok(Some(ShardFlowHandle::new(proj_rx, schema, tasks)))
    }
}

/// Streams matching rows from on-disk segments, executing filters and
/// projections as they are evaluated. Emits batches for later shard merging.
pub async fn build_segment_stream(
    plan: Arc<QueryPlan>,
    ctx: Arc<FlowContext>,
    caches: Arc<QueryCaches>,
    limit_override: Option<usize>,
) -> Result<Option<ShardFlowHandle>, FlowOperatorError> {
    if plan.limit() == Some(0) {
        return Ok(None);
    }

    let mandatory: Vec<String> = DEFAULT_MEMTABLE_COLUMNS
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    let columns = MemTableSource::compute_columns(plan.as_ref(), &mandatory).await?;
    if columns.is_empty() {
        return Ok(None);
    }

    let schema = Arc::new(
        BatchSchema::new(columns.clone()).map_err(|e| FlowOperatorError::Batch(e.to_string()))?,
    );

    let metrics = Arc::clone(ctx.metrics());
    let (source_tx, mut current_rx) = FlowChannel::bounded(ctx.batch_size(), Arc::clone(&metrics));
    let mut tasks: Vec<JoinHandle<()>> = Vec::new();

    let plan_for_task = Arc::clone(&plan);
    let schema_for_task = Arc::clone(&schema);
    let ctx_for_task = Arc::clone(&ctx);
    let caches_for_task = Arc::clone(&caches);
    tasks.push(tokio::spawn(async move {
        let steps: Vec<ExecutionStep<'_>> = plan_for_task
            .filter_groups
            .iter()
            .map(|filter| ExecutionStep::new(filter.clone(), plan_for_task.as_ref()))
            .collect();
        let runner = SegmentQueryRunner::new(plan_for_task.as_ref(), steps)
            .with_caches(Some(Arc::as_ref(&caches_for_task)))
            .with_limit(limit_override.or_else(|| plan_for_task.limit()));
        if let Err(err) = runner
            .stream_into(ctx_for_task, Arc::clone(&schema_for_task), source_tx)
            .await
        {
            // ChannelClosed is expected when LIMIT is reached early - don't log as error
            match &err {
                FlowOperatorError::ChannelClosed => {
                    debug!(target: "sneldb::flow", "Segment stream stopped (channel closed, likely LIMIT reached)");
                }
                _ => {
                    error!(target: "sneldb::flow", error = %err, "Segment stream failed");
                }
            }
        }
    }));

    let mut final_schema = Arc::clone(&schema);

    if let Some(aggregate_plan) = plan.aggregate_plan.clone() {
        let aggregate_config = AggregateOpConfig {
            plan: Arc::clone(&plan),
            aggregate: aggregate_plan.clone(),
        };
        let aggregate = AggregateOp::new(aggregate_config);
        let (agg_tx, agg_rx) = FlowChannel::bounded(ctx.batch_size(), Arc::clone(&metrics));
        let agg_ctx = Arc::clone(&ctx);
        tasks.push(tokio::spawn(async move {
            if let Err(err) = aggregate.run(current_rx, agg_tx, agg_ctx).await {
                // ChannelClosed is expected when LIMIT is reached early - don't log as error
                match &err {
                    FlowOperatorError::ChannelClosed => {
                        debug!(target: "sneldb::flow", "Aggregate operator stopped (channel closed, likely LIMIT reached)");
                    }
                    _ => {
                        error!(target: "sneldb::flow", error = %err, "Aggregate operator failed");
                    }
                }
            }
        }));
        current_rx = agg_rx;
        final_schema = Arc::new(
            BatchSchema::new(aggregate_output_schema(&aggregate_plan))
                .map_err(|e| FlowOperatorError::Batch(e.to_string()))?,
        );
    }

    // Compute projection for RETURN fields if specified
    let projection = if let Command::Query {
        return_fields,
        event_type,
        ..
    } = &plan.command
    {
        let registry = plan.registry.read().await;
        match compute_return_projection(
            &final_schema,
            return_fields.as_deref(),
            &registry,
            event_type,
        ) {
            Ok((output_schema, indices)) => {
                final_schema = output_schema;
                Projection {
                    indices,
                    schema: Arc::clone(&final_schema),
                }
            }
            Err(e) => {
                return Err(e);
            }
        }
    } else {
        Projection {
            indices: (0..final_schema.column_count()).collect(),
            schema: Arc::clone(&final_schema),
        }
    };

    // Optimize: Skip ProjectOp if it's an identity projection (all columns in same order)
    if projection.is_identity() {
        // Identity projection - just pass through batches without cloning
        Ok(Some(ShardFlowHandle::new(current_rx, final_schema, tasks)))
    } else {
        let projector = ProjectOp::new(projection);
        let (proj_tx, proj_rx) = FlowChannel::bounded(ctx.batch_size(), Arc::clone(&metrics));
        let proj_ctx = Arc::clone(&ctx);
        tasks.push(tokio::spawn(async move {
            if let Err(err) = projector.run(current_rx, proj_tx, proj_ctx).await {
                // ChannelClosed is expected when LIMIT is reached early - don't log as error
                match &err {
                    FlowOperatorError::ChannelClosed => {
                        debug!(target: "sneldb::flow", "Projection operator stopped (channel closed, likely LIMIT reached)");
                    }
                    _ => {
                        error!(target: "sneldb::flow", error = %err, "Projection operator failed");
                    }
                }
            }
        }));

        Ok(Some(ShardFlowHandle::new(proj_rx, final_schema, tasks)))
    }
}
