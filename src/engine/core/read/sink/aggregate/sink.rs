use super::super::ResultSink;
use super::group_key::GroupKey;
use crate::command::types::TimeGranularity;
use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::read::aggregate::ops::AggregatorImpl;
use crate::engine::core::read::aggregate::partial::{
    AggPartial, AggState, GroupKey as PartialKey, snapshot_aggregator,
};
use crate::engine::core::read::aggregate::plan::{AggregateOpSpec, AggregatePlan};
use crate::engine::core::{Event, QueryPlan};
use ahash::RandomState as AHashRandomState;
use std::collections::HashMap;

pub struct AggregateSink {
    specs: Vec<AggregateOpSpec>,
    group_by: Option<Vec<String>>,
    time_bucket: Option<TimeGranularity>,
    // Selected time field for bucketing; defaults to core "timestamp"
    time_field: String,
    groups: std::collections::HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState>,
    // Optional cap on the number of distinct groups produced
    group_limit: Option<usize>,
}

impl AggregateSink {
    pub fn new(specs: Vec<AggregateOpSpec>) -> Self {
        Self {
            specs,
            group_by: None,
            time_bucket: None,
            time_field: "timestamp".to_string(),
            groups: std::collections::HashMap::with_hasher(AHashRandomState::new()),
            group_limit: None,
        }
    }

    pub fn from_plan(plan: &AggregatePlan) -> Self {
        Self {
            specs: plan.ops.clone(),
            group_by: plan.group_by.clone(),
            time_bucket: plan.time_bucket.clone(),
            time_field: "timestamp".to_string(),
            groups: std::collections::HashMap::with_hasher(AHashRandomState::new()),
            group_limit: None,
        }
    }

    /// Construct from full QueryPlan to honor selected time field for bucketing.
    pub fn from_query_plan(plan: &QueryPlan, agg: &AggregatePlan) -> Self {
        let time_field = match &plan.command {
            crate::command::types::Command::Query { time_field, .. } => time_field
                .clone()
                .unwrap_or_else(|| "timestamp".to_string()),
            _ => "timestamp".to_string(),
        };
        Self {
            specs: agg.ops.clone(),
            group_by: agg.group_by.clone(),
            time_bucket: agg.time_bucket.clone(),
            time_field,
            groups: std::collections::HashMap::with_hasher(AHashRandomState::new()),
            group_limit: None,
        }
    }

    /// Limit the number of distinct groups produced by this sink. If set, new groups
    /// beyond the limit will be ignored (existing groups continue to be updated).
    pub fn with_group_limit(mut self, limit: Option<usize>) -> Self {
        self.group_limit = limit;
        self
    }

    /// Finalizes into a single synthetic Event with metrics in payload
    pub fn into_events(self, plan: &QueryPlan) -> Vec<Event> {
        // If no grouping/bucketing, synthesize a single default key from map or empty
        let groups = if self.groups.is_empty() {
            let mut m: std::collections::HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
                std::collections::HashMap::with_hasher(AHashRandomState::new());
            let key = GroupKey {
                prehash: 0,
                bucket: None,
                groups: Vec::new(),
            };
            let aggs = self
                .specs
                .iter()
                .map(|s| AggregatorImpl::from_spec(s))
                .collect();
            m.insert(key, aggs);
            m
        } else {
            self.groups
        };

        let mut out = Vec::with_capacity(groups.len());
        for (gk, aggs) in groups.into_iter() {
            let mut payload = serde_json::Map::new();
            // Add key fields
            if let Some(b) = gk.bucket {
                payload.insert("bucket".to_string(), serde_json::json!(b));
            }
            if let Some(gb) = &self.group_by {
                for (i, name) in gb.iter().enumerate() {
                    if let Some(val) = gk.groups.get(i) {
                        payload.insert(name.clone(), serde_json::json!(val));
                    }
                }
            }

            for (spec, outv) in self.specs.iter().zip(aggs.iter().map(|a| a.finalize())) {
                let (key, val) = match (spec, outv) {
                    (
                        AggregateOpSpec::CountAll,
                        crate::engine::core::read::aggregate::ops::AggOutput::Count(v),
                    ) => ("count".to_string(), serde_json::json!(v)),
                    (
                        AggregateOpSpec::CountField { field },
                        crate::engine::core::read::aggregate::ops::AggOutput::Count(v),
                    ) => (format!("count_{}", field), serde_json::json!(v)),
                    (
                        AggregateOpSpec::CountUnique { field },
                        crate::engine::core::read::aggregate::ops::AggOutput::CountUnique(v),
                    ) => (format!("count_unique_{}", field), serde_json::json!(v)),
                    (
                        AggregateOpSpec::Total { field },
                        crate::engine::core::read::aggregate::ops::AggOutput::Sum(v),
                    ) => (format!("total_{}", field), serde_json::json!(v)),
                    (
                        AggregateOpSpec::Avg { field },
                        crate::engine::core::read::aggregate::ops::AggOutput::Avg(v),
                    ) => (format!("avg_{}", field), serde_json::json!(v)),
                    (
                        AggregateOpSpec::Min { field },
                        crate::engine::core::read::aggregate::ops::AggOutput::Min(v),
                    ) => (format!("min_{}", field), serde_json::json!(v)),
                    (
                        AggregateOpSpec::Max { field },
                        crate::engine::core::read::aggregate::ops::AggOutput::Max(v),
                    ) => (format!("max_{}", field), serde_json::json!(v)),
                    (_, other) => (
                        "metric".to_string(),
                        match other {
                            crate::engine::core::read::aggregate::ops::AggOutput::Count(v) => {
                                serde_json::json!(v)
                            }
                            crate::engine::core::read::aggregate::ops::AggOutput::CountUnique(
                                v,
                            ) => serde_json::json!(v),
                            crate::engine::core::read::aggregate::ops::AggOutput::Sum(v) => {
                                serde_json::json!(v)
                            }
                            crate::engine::core::read::aggregate::ops::AggOutput::Min(v) => {
                                serde_json::json!(v)
                            }
                            crate::engine::core::read::aggregate::ops::AggOutput::Max(v) => {
                                serde_json::json!(v)
                            }
                            crate::engine::core::read::aggregate::ops::AggOutput::Avg(v) => {
                                serde_json::json!(v)
                            }
                        },
                    ),
                };
                payload.insert(key, val);
            }

            let event = Event {
                event_type: plan.event_type().to_string(),
                context_id: plan.context_id().unwrap_or("").to_string(),
                timestamp: 0,
                payload: serde_json::Value::Object(payload),
            };
            out.push(event);
        }
        out
    }

    pub fn into_partial(self) -> AggPartial {
        // Convert internal GroupKey to partial GroupKey and aggregator states to AggState
        let mut groups: HashMap<PartialKey, Vec<AggState>> = HashMap::new();
        for (k, aggs) in self.groups.into_iter() {
            let pk = PartialKey {
                bucket: k.bucket,
                groups: k.groups,
            };
            let vec_states = aggs.into_iter().map(|a| snapshot_aggregator(&a)).collect();
            groups.insert(pk, vec_states);
        }
        AggPartial {
            specs: self.specs,
            group_by: self.group_by,
            time_bucket: self.time_bucket,
            groups,
        }
    }

    pub fn group_count_debug(&self) -> usize {
        self.groups.len()
    }
}

impl ResultSink for AggregateSink {
    fn on_row(&mut self, row_idx: usize, columns: &HashMap<String, ColumnValues>) {
        let key = GroupKey::from_row(
            self.time_bucket.as_ref(),
            self.group_by.as_deref(),
            &self.time_field,
            columns,
            row_idx,
        );

        // Enforce group limit: if key not present and limit reached, skip creating new group
        if !self.groups.contains_key(&key) {
            if let Some(max) = self.group_limit {
                if self.groups.len() >= max {
                    return;
                }
            }
        }

        let entry = self.groups.entry(key).or_insert_with(|| {
            self.specs
                .iter()
                .map(|s| AggregatorImpl::from_spec(s))
                .collect()
        });
        for agg in entry.iter_mut() {
            agg.update(row_idx, columns);
        }
    }

    fn on_event(&mut self, event: &Event) {
        let key = GroupKey::from_event(
            self.time_bucket.as_ref(),
            self.group_by.as_deref(),
            &self.time_field,
            event,
        );
        if !self.groups.contains_key(&key) {
            if let Some(max) = self.group_limit {
                if self.groups.len() >= max {
                    return;
                }
            }
        }
        let entry = self.groups.entry(key).or_insert_with(|| {
            self.specs
                .iter()
                .map(|s| AggregatorImpl::from_spec(s))
                .collect()
        });
        for agg in entry.iter_mut() {
            agg.update_from_event(event);
        }
    }
}
