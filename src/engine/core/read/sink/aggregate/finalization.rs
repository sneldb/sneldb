use super::group_key::GroupKey;
use crate::command::types::TimeGranularity;
use crate::engine::core::read::aggregate::ops::{AggOutput, AggregatorImpl};
use crate::engine::core::read::aggregate::partial::{
    AggPartial, AggState, GroupKey as PartialKey, snapshot_aggregator,
};
use crate::engine::core::read::aggregate::plan::AggregateOpSpec;
use crate::engine::core::{Event, EventId, QueryPlan};
use crate::engine::types::ScalarValue;
use ahash::RandomState as AHashRandomState;
use std::collections::HashMap;
use std::sync::Arc;

/// Converts aggregated groups into Events
pub(crate) fn into_events(
    groups: HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState>,
    specs: Vec<AggregateOpSpec>,
    group_by: Option<Vec<String>>,
    plan: &QueryPlan,
) -> Vec<Event> {
    // If no grouping/bucketing, synthesize a single default key
    let groups = if groups.is_empty() {
        let mut m: HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
            HashMap::with_hasher(AHashRandomState::new());
        let key = GroupKey {
            prehash: 0,
            bucket: None,
            groups: Vec::new(),
            groups_str: None, // Lazy - will be computed when needed
        };
        let aggs = specs.iter().map(|s| AggregatorImpl::from_spec(s)).collect();
        m.insert(key, aggs);
        m
    } else {
        groups
    };

    let mut out = Vec::with_capacity(groups.len());
    for (mut gk, aggs) in groups.into_iter() {
        use std::collections::HashMap;
        let mut payload = HashMap::new();
        // Add key fields
        if let Some(b) = gk.bucket {
            payload.insert(Arc::from("bucket"), ScalarValue::Int64(b as i64));
        }
        if let Some(gb) = &group_by {
            let groups_str_vec = gk.groups_str();
            for (i, name) in gb.iter().enumerate() {
                if let Some(val) = groups_str_vec.get(i) {
                    payload.insert(Arc::from(name.as_str()), ScalarValue::Utf8(val.clone()));
                }
            }
        }

        for (spec, outv) in specs.iter().zip(aggs.iter().map(|a| a.finalize())) {
            let (key, val) = match (spec, outv) {
                (AggregateOpSpec::CountAll, AggOutput::Count(v)) => {
                    ("count".to_string(), ScalarValue::Int64(v as i64))
                }
                (AggregateOpSpec::CountField { field }, AggOutput::Count(v)) => {
                    (format!("count_{}", field), ScalarValue::Int64(v as i64))
                }
                (AggregateOpSpec::CountUnique { field }, AggOutput::CountUnique(v)) => (
                    format!("count_unique_{}", field),
                    ScalarValue::Int64(v as i64),
                ),
                (AggregateOpSpec::Total { field }, AggOutput::Sum(v)) => {
                    (format!("total_{}", field), ScalarValue::Int64(v))
                }
                (AggregateOpSpec::Avg { field }, AggOutput::Avg(v)) => {
                    (format!("avg_{}", field), ScalarValue::Float64(v))
                }
                (AggregateOpSpec::Min { field }, AggOutput::Min(v)) => {
                    (format!("min_{}", field), ScalarValue::Utf8(v))
                }
                (AggregateOpSpec::Max { field }, AggOutput::Max(v)) => {
                    (format!("max_{}", field), ScalarValue::Utf8(v))
                }
                (_, other) => (
                    "metric".to_string(),
                    match other {
                        AggOutput::Count(v) => ScalarValue::Int64(v as i64),
                        AggOutput::CountUnique(v) => ScalarValue::Int64(v as i64),
                        AggOutput::Sum(v) => ScalarValue::Int64(v),
                        AggOutput::Min(v) => ScalarValue::Utf8(v),
                        AggOutput::Max(v) => ScalarValue::Utf8(v),
                        AggOutput::Avg(v) => ScalarValue::Float64(v),
                    },
                ),
            };
            payload.insert(Arc::from(key), val);
        }

        let event = Event {
            event_type: plan.event_type().to_string(),
            context_id: plan.context_id().unwrap_or("").to_string(),
            timestamp: 0,
            id: EventId::default(),
            payload,
        };
        out.push(event);
    }
    out
}

/// Converts aggregated groups into partial aggregation state
pub(crate) fn into_partial(
    groups: HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState>,
    specs: Vec<AggregateOpSpec>,
    group_by: Option<Vec<String>>,
    time_bucket: Option<TimeGranularity>,
) -> AggPartial {
    let mut partial_groups: HashMap<PartialKey, Vec<AggState>> = HashMap::new();
    for (mut k, aggs) in groups.into_iter() {
        let groups_str_vec = k.groups_str().clone();
        let pk = PartialKey {
            bucket: k.bucket,
            groups: groups_str_vec,
        };
        let vec_states = aggs.into_iter().map(|a| snapshot_aggregator(&a)).collect();
        partial_groups.insert(pk, vec_states);
    }
    AggPartial {
        specs,
        group_by,
        time_bucket,
        groups: partial_groups,
    }
}
