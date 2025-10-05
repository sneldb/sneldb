use std::collections::{HashMap, HashSet};

use crate::command::types::TimeGranularity;
use crate::engine::core::read::aggregate::partial::{AggState, GroupKey};
use crate::engine::core::read::aggregate::plan::AggregateOpSpec;
use crate::engine::core::read::result::AggregateResult;

pub struct AggregateResultFactory {
    group_by: Option<Vec<String>>,
    time_bucket: Option<TimeGranularity>,
    specs: Vec<AggregateOpSpec>,
    groups: HashMap<GroupKey, Vec<AggState>>,
}

impl AggregateResultFactory {
    pub fn new() -> Self {
        Self {
            group_by: None,
            time_bucket: None,
            specs: vec![AggregateOpSpec::CountAll],
            groups: HashMap::new(),
        }
    }

    pub fn with_group_by(mut self, fields: Vec<&str>) -> Self {
        self.group_by = Some(fields.into_iter().map(|s| s.to_string()).collect());
        self
    }

    pub fn with_time_bucket(mut self, bucket: TimeGranularity) -> Self {
        self.time_bucket = Some(bucket);
        self
    }

    pub fn with_specs(mut self, specs: Vec<AggregateOpSpec>) -> Self {
        self.specs = specs;
        self
    }

    pub fn add_group_key(
        mut self,
        bucket: Option<u64>,
        groups: &[&str],
        states: Vec<AggState>,
    ) -> Self {
        let key = GroupKey {
            bucket,
            groups: groups.iter().map(|s| s.to_string()).collect(),
        };
        self.groups.insert(key, states);
        self
    }

    pub fn add_count_unique_group(
        mut self,
        bucket: Option<u64>,
        group: &str,
        unique_values: &[&str],
    ) -> Self {
        let mut set: HashSet<String> = HashSet::new();
        for v in unique_values {
            set.insert((*v).to_string());
        }
        let key = GroupKey {
            bucket,
            groups: vec![group.to_string()],
        };
        self.groups
            .insert(key, vec![AggState::CountUnique { values: set }]);
        self
    }

    pub fn create(self) -> AggregateResult {
        AggregateResult {
            group_by: self.group_by,
            time_bucket: self.time_bucket,
            specs: self.specs,
            groups: self.groups,
        }
    }
}
