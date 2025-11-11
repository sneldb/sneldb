use crate::engine::core::read::flow::{BatchSchema, BatchSender, FlowContext, FlowOperatorError};
use crate::engine::core::{
    ConditionEvaluatorBuilder, Event, EventSorter, ExecutionStep, QueryCaches, QueryContext,
    QueryPlan, ZoneHydrator,
};
use crate::engine::types::ScalarValue;
use std::cmp::Ordering;
use std::sync::Arc;
use tracing::info;

pub struct SegmentQueryRunner<'a> {
    plan: &'a QueryPlan,
    steps: Vec<ExecutionStep<'a>>,
    caches: Option<&'a QueryCaches>,
    limit: Option<usize>,
}

impl<'a> SegmentQueryRunner<'a> {
    pub fn new(plan: &'a QueryPlan, steps: Vec<ExecutionStep<'a>>) -> Self {
        Self {
            plan,
            steps,
            caches: None,
            limit: None,
        }
    }

    pub async fn run(&self) -> Vec<Event> {
        info!(target: "sneldb::query::segment", "Running segment query runner");

        // Extract query context from command
        let ctx = QueryContext::from_command(&self.plan.command);

        // Hydrate zones with optional zone filtering
        let mut candidate_zones = self.hydrate_zones(&ctx).await;

        // Sort zones deterministically by (segment_id, zone_id) to ensure consistent processing order
        // This prevents flaky tests due to non-deterministic HashMap iteration order
        candidate_zones.sort_by(|a, b| {
            a.segment_id
                .cmp(&b.segment_id)
                .then_with(|| a.zone_id.cmp(&b.zone_id))
        });

        // Determine evaluation limit (defer if ordering required)
        let eval_limit = self.determine_eval_limit(&ctx);

        // Evaluate zones to get matching events
        let mut events = self.evaluate_zones(candidate_zones, eval_limit);

        // Sort events if ORDER BY is present
        if let Some(sorter) = self.create_sorter(&ctx) {
            sorter.sort(&mut events);
        }

        info!(
            target: "sneldb::query::segment",
            "Found {} matching events in disk segments",
            events.len()
        );
        events
    }

    /// Hydrates candidate zones, applying zone filtering if present in context.
    async fn hydrate_zones(&self, ctx: &QueryContext) -> Vec<crate::engine::core::CandidateZone> {
        ZoneHydrator::new(self.plan, self.steps.clone())
            .with_caches(self.caches)
            .with_allowed_zones(ctx.picked_zones.clone())
            .hydrate()
            .await
    }

    /// Determines the evaluation limit based on context.
    ///
    /// If ORDER BY is present, returns None to allow all events to be collected
    /// for proper sorting. Otherwise, returns the configured limit.
    fn determine_eval_limit(&self, ctx: &QueryContext) -> Option<usize> {
        if ctx.should_defer_limit() {
            None
        } else {
            self.limit
        }
    }

    /// Evaluates zones to produce matching events.
    fn evaluate_zones(
        &self,
        zones: Vec<crate::engine::core::CandidateZone>,
        limit: Option<usize>,
    ) -> Vec<Event> {
        let evaluator = ConditionEvaluatorBuilder::build_from_plan(self.plan);
        evaluator.evaluate_zones_with_limit(zones, limit)
    }

    /// Creates an EventSorter if ORDER BY is present in context.
    fn create_sorter(&self, ctx: &QueryContext) -> Option<EventSorter> {
        ctx.order_by.as_ref().map(EventSorter::from_order_spec)
    }

    pub fn with_caches(mut self, caches: Option<&'a QueryCaches>) -> Self {
        self.caches = caches;
        self
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    pub async fn stream_into(
        &self,
        flow_ctx: Arc<FlowContext>,
        schema: Arc<BatchSchema>,
        sender: BatchSender,
    ) -> Result<(), FlowOperatorError> {
        let query_ctx = QueryContext::from_command(&self.plan.command);
        let candidate_zones = self.hydrate_zones(&query_ctx).await;
        let eval_limit = self.determine_eval_limit(&query_ctx);
        let evaluator = ConditionEvaluatorBuilder::build_from_plan(self.plan);

        // For aggregate queries, ordering happens after aggregation in AggregateStreamMerger.
        // For non-aggregate queries, ordering can happen at the shard level.
        if let Some(order_spec) = self.plan.order_by_for_shard_level() {
            let order_index = schema
                .columns()
                .iter()
                .position(|col| col.name == order_spec.field)
                .ok_or_else(|| FlowOperatorError::Operator("order column missing".to_string()))?;
            let ascending = !order_spec.desc;

            let mut rows: Vec<Vec<ScalarValue>> = Vec::new();
            let mut emitted = 0usize;

            for zone in candidate_zones {
                let remaining_limit = eval_limit.map(|lim| lim.saturating_sub(emitted));
                if matches!(remaining_limit, Some(0)) {
                    break;
                }

                let events = evaluator.evaluate_zones_with_limit(vec![zone], remaining_limit);
                for event in events {
                    if let Some(limit) = eval_limit {
                        if emitted >= limit {
                            break;
                        }
                    }

                    let mut row = Vec::with_capacity(schema.column_count());
                    for column in schema.columns() {
                        row.push(
                            event
                                .get_field_scalar(&column.name)
                                .unwrap_or(ScalarValue::Null),
                        );
                    }
                    rows.push(row);
                    emitted += 1;
                }

                if let Some(limit) = eval_limit {
                    if emitted >= limit {
                        break;
                    }
                }
            }

            // Use sort_unstable_by for better performance - maintains relative order of equal elements
            rows.sort_unstable_by(|a, b| {
                let ord = compare_scalar_values(&a[order_index], &b[order_index]);
                if ascending { ord } else { ord.reverse() }
            });

            info!(
                target: "sneldb::segment_source",
                rows = rows.len(),
                limit = eval_limit,
                order_index = order_index,
                ascending = ascending,
                "Segment source collected ordered rows"
            );

            if let Some(limit) = eval_limit {
                if rows.len() > limit {
                    rows.truncate(limit);
                }
            }

            let mut builder = flow_ctx.pool().acquire(Arc::clone(&schema));
            for row in rows.iter() {
                builder
                    .push_row(row)
                    .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;

                if builder.is_full() {
                    let batch = builder
                        .finish()
                        .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;
                    sender
                        .send(Arc::new(batch))
                        .await
                        .map_err(|_| FlowOperatorError::ChannelClosed)?;
                    builder = flow_ctx.pool().acquire(Arc::clone(&schema));
                }
            }

            if builder.len() > 0 {
                let batch = builder
                    .finish()
                    .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;
                sender
                    .send(Arc::new(batch))
                    .await
                    .map_err(|_| FlowOperatorError::ChannelClosed)?;
            }

            Ok(())
        } else {
            let mut builder = flow_ctx.pool().acquire(Arc::clone(&schema));
            let mut row = Vec::with_capacity(schema.column_count());
            let mut emitted = 0usize;

            for zone in candidate_zones {
                let remaining_limit = eval_limit.map(|lim| lim.saturating_sub(emitted));
                if matches!(remaining_limit, Some(0)) {
                    break;
                }

                let events = evaluator.evaluate_zones_with_limit(vec![zone], remaining_limit);
                for event in events {
                    row.clear();
                    for column in schema.columns() {
                        row.push(
                            event
                                .get_field_scalar(&column.name)
                                .unwrap_or(ScalarValue::Null),
                        );
                    }

                    builder
                        .push_row(&row)
                        .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;

                    if builder.is_full() {
                        let batch = builder
                            .finish()
                            .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;
                        sender
                            .send(Arc::new(batch))
                            .await
                            .map_err(|_| FlowOperatorError::ChannelClosed)?;
                        builder = flow_ctx.pool().acquire(Arc::clone(&schema));
                    }

                    emitted += 1;
                    if let Some(limit) = eval_limit {
                        if emitted >= limit {
                            break;
                        }
                    }
                }

                if let Some(limit) = eval_limit {
                    if emitted >= limit {
                        break;
                    }
                }
            }

            if builder.len() > 0 {
                let batch = builder
                    .finish()
                    .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;
                sender
                    .send(Arc::new(batch))
                    .await
                    .map_err(|_| FlowOperatorError::ChannelClosed)?;
            }

            Ok(())
        }
    }
}

fn compare_scalar_values(a: &ScalarValue, b: &ScalarValue) -> Ordering {
    // Try u64 first (existing behavior)
    if let (Some(va), Some(vb)) = (a.as_u64(), b.as_u64()) {
        return va.cmp(&vb);
    }
    // Use the efficient direct comparison method
    a.compare(b)
}
