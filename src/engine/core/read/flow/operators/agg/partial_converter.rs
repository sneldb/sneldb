use crate::engine::core::read::aggregate::partial::{AggPartial, AggState, GroupKey as PartialKey};
use crate::engine::core::read::aggregate::plan::AggregateOpSpec;
use crate::engine::core::read::flow::{BatchSchema, FlowContext, FlowOperatorError};
use crate::engine::core::read::flow::BatchSender;
use crate::engine::types::ScalarValue;
use serde_json;
use std::sync::Arc;

/// Converts AggPartial to batches for streaming output
pub struct PartialConverter;

impl PartialConverter {
    /// Convert partial aggregation results to batches
    pub async fn to_batches(
        partial: AggPartial,
        schema: Arc<BatchSchema>,
        ctx: Arc<FlowContext>,
        output: BatchSender,
    ) -> Result<(), FlowOperatorError> {
        let mut builder = ctx.pool().acquire(Arc::clone(&schema));

        let time_bucket = partial.time_bucket;
        let group_by = partial.group_by;
        let specs = partial.specs;
        let groups = partial.groups;

        for (group_key, states) in groups {
            let row = Self::build_row(&group_key, &time_bucket, &group_by, &states, &specs)?;

            builder
                .push_row(&row)
                .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;

            if builder.is_full() {
                let batch = builder
                    .finish()
                    .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;
                output
                    .send(Arc::new(batch))
                    .await
                    .map_err(|_| FlowOperatorError::ChannelClosed)?;
                builder = ctx.pool().acquire(Arc::clone(&schema));
            }
        }

        if builder.len() > 0 {
            let batch = builder
                .finish()
                .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;
            output
                .send(Arc::new(batch))
                .await
                .map_err(|_| FlowOperatorError::ChannelClosed)?;
        }

        Ok(())
    }

    fn build_row(
        group_key: &PartialKey,
        time_bucket: &Option<crate::command::types::TimeGranularity>,
        group_by: &Option<Vec<String>>,
        states: &[AggState],
        specs: &[AggregateOpSpec],
    ) -> Result<Vec<ScalarValue>, FlowOperatorError> {
        let mut row = Vec::with_capacity(specs.len() + 2);

        if time_bucket.is_some() {
            row.push(ScalarValue::Int64(
                group_key.bucket.map(|b| b as i64).unwrap_or(0),
            ));
        }

        if group_by.is_some() {
            for val in &group_key.groups {
                row.push(ScalarValue::Utf8(val.clone()));
            }
        }

        for (spec, state) in specs.iter().zip(states.iter()) {
            match (spec, state) {
                (AggregateOpSpec::Avg { .. }, AggState::Avg { sum, count }) => {
                    // Avg outputs both sum and count
                    row.push(ScalarValue::Int64(*sum));
                    row.push(ScalarValue::Int64(*count));
                }
                _ => {
                    row.push(Self::state_to_scalar(spec, state)?);
                }
            }
        }

        Ok(row)
    }

    fn state_to_scalar(
        spec: &AggregateOpSpec,
        state: &AggState,
    ) -> Result<ScalarValue, FlowOperatorError> {
        match (spec, state) {
            (AggregateOpSpec::CountAll, AggState::CountAll { count })
            | (AggregateOpSpec::CountField { .. }, AggState::CountAll { count }) => {
                Ok(ScalarValue::Int64(*count))
            }
            (AggregateOpSpec::CountUnique { .. }, AggState::CountUnique { values }) => {
                let json_values: Vec<&String> = values.iter().collect();
                let json_str = serde_json::to_string(&json_values).map_err(|e| {
                    FlowOperatorError::Batch(format!(
                        "failed to serialize CountUnique values: {}",
                        e
                    ))
                })?;
                Ok(ScalarValue::Utf8(json_str))
            }
            (AggregateOpSpec::Total { .. }, AggState::Sum { sum }) => Ok(ScalarValue::Int64(*sum)),
            (AggregateOpSpec::Avg { .. }, AggState::Avg { .. }) => {
                // Avg is handled separately in build_row
                unreachable!("Avg should be handled in build_row")
            }
            (AggregateOpSpec::Min { .. }, AggState::Min { min_num, min_str }) => {
                if let Some(n) = min_num {
                    Ok(ScalarValue::Int64(*n))
                } else if let Some(s) = min_str {
                    Ok(ScalarValue::Utf8(s.clone()))
                } else {
                    Ok(ScalarValue::Utf8(String::new()))
                }
            }
            (AggregateOpSpec::Max { .. }, AggState::Max { max_num, max_str }) => {
                if let Some(n) = max_num {
                    Ok(ScalarValue::Int64(*n))
                } else if let Some(s) = max_str {
                    Ok(ScalarValue::Utf8(s.clone()))
                } else {
                    Ok(ScalarValue::Utf8(String::new()))
                }
            }
            _ => Err(FlowOperatorError::Batch(format!(
                "unsupported aggregate spec/state combination: {:?}/{:?}",
                spec, state
            ))),
        }
    }
}

