use crate::command::types::TimeGranularity;
use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::event::event_builder::EventBuilder;
use crate::engine::core::read::aggregate::ops::{AggOutput, AggregatorImpl};
use crate::engine::core::read::aggregate::partial::{
    AggPartial, AggState, GroupKey as PartialKey, snapshot_aggregator,
};
use crate::engine::core::read::aggregate::plan::{AggregateOpSpec, AggregatePlan};
use crate::engine::core::{Event, QueryPlan};
use ahash::RandomState as AHashRandomState;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, Hasher};

/// A sink that consumes matching rows or events produced by the execution path
pub trait ResultSink {
    /// Called for each matching row in a zone (columnar path)
    fn on_row(&mut self, _row_idx: usize, _columns: &HashMap<String, ColumnValues>) {}
    /// Called for each matching event from memtable (row path)
    fn on_event(&mut self, _event: &Event) {}
}

/// Collects Events as the final result
pub struct EventSink {
    events: Vec<Event>,
}

impl EventSink {
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }
    pub fn into_events(self) -> Vec<Event> {
        self.events
    }
}

impl ResultSink for EventSink {
    fn on_row(&mut self, row_idx: usize, columns: &HashMap<String, ColumnValues>) {
        let mut builder = EventBuilder::new();
        // Emit all available fields from this zone row

        for (field, values) in columns {
            if tracing::enabled!(tracing::Level::DEBUG) {
                tracing::debug!(target: "event_emit", field = %field, row_idx, has_i64 = values.get_i64_at(row_idx).is_some(), has_u64 = values.get_u64_at(row_idx).is_some(), has_f64 = values.get_f64_at(row_idx).is_some(), has_bool = values.get_bool_at(row_idx).is_some(), has_str = values.get_str_at(row_idx).is_some(), "Emitting field for row");
            }
            if let Some(n) = values.get_i64_at(row_idx) {
                builder.add_field_i64(field, n);
                continue;
            }
            if let Some(n) = values.get_u64_at(row_idx) {
                builder.add_field_u64(field, n);
                continue;
            }
            if let Some(f) = values.get_f64_at(row_idx) {
                builder.add_field_f64(field, f);
                continue;
            }
            if let Some(b) = values.get_bool_at(row_idx) {
                builder.add_field_bool(field, b);
                continue;
            }
            match values.get_str_at(row_idx) {
                Some(val) => builder.add_field(field, val),
                None => builder.add_field_null(field),
            }
        }
        self.events.push(builder.build());
    }

    fn on_event(&mut self, event: &Event) {
        self.events.push(event.clone());
    }
}

/// Aggregation sink that maintains aggregator state and can output a synthetic event
#[derive(Clone, Debug, Eq)]
struct GroupKey {
    // Precomputed 64-bit hash to speed up HashMap lookups and reduce per-insert hashing cost
    prehash: u64,
    bucket: Option<u64>,
    groups: Vec<String>,
}

impl PartialEq for GroupKey {
    fn eq(&self, other: &Self) -> bool {
        // prehash is a cache; equality must be defined by the actual key fields
        self.bucket == other.bucket && self.groups == other.groups
    }
}

impl Hash for GroupKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use the precomputed hash to avoid re-hashing the full key
        self.prehash.hash(state);
    }
}

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
                    (AggregateOpSpec::CountAll, AggOutput::Count(v)) => {
                        ("count".to_string(), serde_json::json!(v))
                    }
                    (AggregateOpSpec::CountField { field }, AggOutput::Count(v)) => {
                        (format!("count_{}", field), serde_json::json!(v))
                    }
                    (AggregateOpSpec::CountUnique { field }, AggOutput::CountUnique(v)) => {
                        (format!("count_unique_{}", field), serde_json::json!(v))
                    }
                    (AggregateOpSpec::Total { field }, AggOutput::Sum(v)) => {
                        (format!("total_{}", field), serde_json::json!(v))
                    }
                    (AggregateOpSpec::Avg { field }, AggOutput::Avg(v)) => {
                        (format!("avg_{}", field), serde_json::json!(v))
                    }
                    (AggregateOpSpec::Min { field }, AggOutput::Min(v)) => {
                        (format!("min_{}", field), serde_json::json!(v))
                    }
                    (AggregateOpSpec::Max { field }, AggOutput::Max(v)) => {
                        (format!("max_{}", field), serde_json::json!(v))
                    }
                    (_, other) => (
                        "metric".to_string(),
                        match other {
                            AggOutput::Count(v) => serde_json::json!(v),
                            AggOutput::CountUnique(v) => serde_json::json!(v),
                            AggOutput::Sum(v) => serde_json::json!(v),
                            AggOutput::Min(v) => serde_json::json!(v),
                            AggOutput::Max(v) => serde_json::json!(v),
                            AggOutput::Avg(v) => serde_json::json!(v),
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
        let key = compute_group_key(
            self.time_bucket.as_ref(),
            self.group_by.as_ref(),
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
        let key = compute_group_key_from_event(
            self.time_bucket.as_ref(),
            self.group_by.as_ref(),
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

fn bucket_of(ts: u64, gran: &TimeGranularity) -> u64 {
    match gran {
        TimeGranularity::Hour => (ts / 3600) * 3600,
        TimeGranularity::Day => (ts / 86_400) * 86_400,
        TimeGranularity::Week => (ts / 604_800) * 604_800,
        TimeGranularity::Month => (ts / 2_592_000) * 2_592_000, // naive 30-day month bucket
    }
}

fn compute_group_key(
    bucket: Option<&TimeGranularity>,
    group_by: Option<&Vec<String>>,
    time_field: &str,
    columns: &HashMap<String, ColumnValues>,
    row_idx: usize,
) -> GroupKey {
    let mut bucket_val: Option<u64> = None;
    if let Some(gr) = bucket {
        if let Some(ts_col) = columns.get(time_field) {
            if let Some(ts) = ts_col.get_i64_at(row_idx) {
                bucket_val = Some(bucket_of(ts as u64, gr));
            }
        }
    }
    let mut groups: Vec<String> = if let Some(gb) = group_by {
        Vec::with_capacity(gb.len())
    } else {
        Vec::new()
    };
    if let Some(gb) = group_by {
        for name in gb.iter() {
            if let Some(col) = columns.get(name) {
                let val = col.get_str_at(row_idx).unwrap_or("").to_string();
                groups.push(val);
            } else {
                groups.push(String::new());
            }
        }
    }
    // Precompute a stable 64-bit hash for the group key
    let mut hasher = AHashRandomState::with_seeds(0, 0, 0, 0).build_hasher();
    bucket_val.hash(&mut hasher);
    for g in &groups {
        g.hash(&mut hasher);
    }
    let prehash = hasher.finish();
    GroupKey {
        prehash,
        bucket: bucket_val,
        groups,
    }
}

fn compute_group_key_from_event(
    bucket: Option<&TimeGranularity>,
    group_by: Option<&Vec<String>>,
    time_field: &str,
    event: &Event,
) -> GroupKey {
    let mut bucket_val: Option<u64> = None;
    if let Some(gr) = bucket {
        let ts = if time_field == "timestamp" {
            event.timestamp
        } else {
            event
                .payload
                .as_object()
                .and_then(|m| m.get(time_field))
                .and_then(|v| v.as_i64())
                .map(|v| v as u64)
                .unwrap_or(0)
        };
        bucket_val = Some(bucket_of(ts, gr));
    }
    let mut groups: Vec<String> = Vec::new();
    if let Some(gb) = group_by {
        for name in gb.iter() {
            groups.push(event.get_field_value(name));
        }
    }
    let mut hasher = AHashRandomState::with_seeds(0, 0, 0, 0).build_hasher();
    bucket_val.hash(&mut hasher);
    for g in &groups {
        g.hash(&mut hasher);
    }
    let prehash = hasher.finish();
    GroupKey {
        prehash,
        bucket: bucket_val,
        groups,
    }
}
