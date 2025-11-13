use super::super::ResultSink;
use super::columnar::ColumnarProcessor;
use super::finalization::{into_events, into_partial};
use super::group_key::GroupKey;
use super::group_key_cache::GroupKeyCache;
use super::schema::SchemaCache;
use crate::command::types::{Command, TimeGranularity};
use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::read::aggregate::ops::AggregatorImpl;
use crate::engine::core::read::aggregate::plan::{AggregateOpSpec, AggregatePlan};
use crate::engine::core::{Event, EventId, QueryPlan};
use ahash::RandomState as AHashRandomState;
use std::collections::HashMap;

pub struct AggregateSink {
    pub(crate) specs: Vec<AggregateOpSpec>,
    pub(crate) group_by: Option<Vec<String>>,
    pub(crate) time_bucket: Option<TimeGranularity>,
    pub(crate) time_field: String,
    groups: std::collections::HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState>,
    group_limit: Option<usize>,
    seen_event_ids: std::collections::HashSet<EventId, AHashRandomState>,
    group_key_cache: Option<GroupKeyCache>,
    schema_cache: SchemaCache,
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
            seen_event_ids: std::collections::HashSet::with_hasher(AHashRandomState::new()),
            group_key_cache: Some(GroupKeyCache::new(1000)),
            schema_cache: SchemaCache::new(),
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
            seen_event_ids: std::collections::HashSet::with_hasher(AHashRandomState::new()),
            group_key_cache: Some(GroupKeyCache::new(1000)),
            schema_cache: SchemaCache::new(),
        }
    }

    /// Construct from full QueryPlan to honor selected time field for bucketing.
    pub fn from_query_plan(plan: &QueryPlan, agg: &AggregatePlan) -> Self {
        let time_field = match &plan.command {
            Command::Query { time_field, .. } => time_field
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
            seen_event_ids: std::collections::HashSet::with_hasher(AHashRandomState::new()),
            group_key_cache: Some(GroupKeyCache::new(1000)),
            schema_cache: SchemaCache::new(),
        }
    }

    /// Initialize column indices from a batch schema to avoid HashMap lookups
    pub fn initialize_column_indices(&mut self, column_names: &[String]) {
        self.schema_cache.initialize_column_indices(column_names);
    }

    /// Limit the number of distinct groups produced by this sink
    pub fn with_group_limit(mut self, limit: Option<usize>) -> Self {
        self.group_limit = limit;
        self
    }

    fn record_event_id(&mut self, id: Option<EventId>) -> bool {
        if let Some(actual) = id {
            self.seen_event_ids.insert(actual)
        } else {
            true
        }
    }

    fn event_id_from_columns(
        columns: &HashMap<String, ColumnValues>,
        row_idx: usize,
    ) -> Option<EventId> {
        columns.get("event_id").and_then(|values| {
            values
                .get_u64_at(row_idx)
                .or_else(|| values.get_i64_at(row_idx).map(|v| v as u64))
                .or_else(|| {
                    values
                        .get_str_at(row_idx)
                        .and_then(|s| s.parse::<u64>().ok())
                })
                .map(EventId::from)
        })
    }

    /// Finalizes into Events with metrics in payload
    pub fn into_events(self, plan: &QueryPlan) -> Vec<Event> {
        into_events(self.groups, self.specs, self.group_by, plan)
    }

    pub fn into_partial(self) -> crate::engine::core::read::aggregate::partial::AggPartial {
        into_partial(self.groups, self.specs, self.group_by, self.time_bucket)
    }

    pub fn group_count_debug(&self) -> usize {
        self.groups.len()
    }

    fn has_grouping(&self) -> bool {
        self.group_by.is_some() || self.time_bucket.is_some()
    }

    fn compute_group_key(
        &mut self,
        columns: &HashMap<String, ColumnValues>,
        row_idx: usize,
    ) -> GroupKey {
        if let Some(cache) = &mut self.group_key_cache {
            let compute_key = || {
                GroupKey::from_row_with_indices(
                    self.time_bucket.as_ref(),
                    self.group_by.as_deref(),
                    &self.time_field,
                    columns,
                    self.schema_cache.column_indices(),
                    row_idx,
                )
            };
            let temp_key = compute_key();
            cache.get_or_insert(temp_key.prehash, || temp_key)
        } else {
            GroupKey::from_row_with_indices(
                self.time_bucket.as_ref(),
                self.group_by.as_deref(),
                &self.time_field,
                columns,
                self.schema_cache.column_indices(),
                row_idx,
            )
        }
    }

    fn ensure_group_entry(&mut self, key: GroupKey) -> Option<&mut Vec<AggregatorImpl>> {
        // Enforce group limit
        if !self.groups.contains_key(&key) {
            if let Some(max) = self.group_limit {
                if self.groups.len() >= max {
                    return None;
                }
            }
        }

        Some(self.groups.entry(key).or_insert_with(|| {
            self.specs
                .iter()
                .map(|s| AggregatorImpl::from_spec(s))
                .collect()
        }))
    }
}

impl ResultSink for AggregateSink {
    fn on_row(&mut self, row_idx: usize, columns: &HashMap<String, ColumnValues>) {
        if !self.record_event_id(Self::event_id_from_columns(columns, row_idx)) {
            return;
        }

        let key = self.compute_group_key(columns, row_idx);
        if let Some(entry) = self.ensure_group_entry(key) {
            for agg in entry.iter_mut() {
                agg.update(row_idx, columns);
            }
        }
    }

    fn on_event(&mut self, event: &Event) {
        if !self.record_event_id(Some(event.event_id())) {
            return;
        }
        let key = GroupKey::from_event(
            self.time_bucket.as_ref(),
            self.group_by.as_deref(),
            &self.time_field,
            event,
        );
        if let Some(entry) = self.ensure_group_entry(key) {
            for agg in entry.iter_mut() {
                agg.update_from_event(event);
            }
        }
    }
}

impl AggregateSink {
    /// Check if columnar processing can be used for this slice
    pub fn can_use_columnar_processing(
        &self,
        columns: &HashMap<String, ColumnValues>,
        _start: usize,
        _end: usize,
    ) -> bool {
        ColumnarProcessor::can_use_columnar_processing(&self.specs, columns)
    }

    /// Process a column slice using columnar processing when possible
    pub fn on_column_slice(
        &mut self,
        start: usize,
        end: usize,
        columns: &HashMap<String, ColumnValues>,
    ) {
        let can_use_columnar = ColumnarProcessor::can_use_columnar_processing(&self.specs, columns);

        if can_use_columnar {
            if self.has_grouping() {
                ColumnarProcessor::process_columnar_slice_with_grouping(
                    &mut self.groups,
                    &self.specs,
                    self.time_bucket.as_ref(),
                    self.group_by.as_deref(),
                    &self.time_field,
                    self.schema_cache.column_indices(),
                    start,
                    end,
                    columns,
                    self.group_limit,
                );
            } else {
                ColumnarProcessor::process_columnar_slice(
                    &mut self.groups,
                    &self.specs,
                    start,
                    end,
                    columns,
                );
            }
        } else {
            // Non-columnar path: process row-by-row
            for row_idx in start..end {
                self.on_row(row_idx, columns);
            }
        }
    }
}
