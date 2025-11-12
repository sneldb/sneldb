use std::collections::{HashMap, HashSet};

use crate::command::types::TimeGranularity;
use crate::engine::core::read::aggregate::ops::{AggOutput, AggregatorImpl};
use crate::engine::core::read::aggregate::plan::AggregateOpSpec;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct GroupKey {
    pub bucket: Option<u64>,
    pub groups: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AggState {
    CountAll {
        count: i64,
    },
    // Reuse CountAll state for CountField finalized value
    CountUnique {
        values: HashSet<String>,
    },
    Sum {
        sum: i64,
    },
    Avg {
        sum: i64,
        count: i64,
    },
    Min {
        min_num: Option<i64>,
        min_str: Option<String>,
    },
    Max {
        max_num: Option<i64>,
        max_str: Option<String>,
    },
}

impl AggState {
    pub fn merge(&mut self, other: &AggState) {
        match (self, other) {
            (AggState::CountAll { count: a }, AggState::CountAll { count: b }) => *a += *b,
            (AggState::CountUnique { values: a }, AggState::CountUnique { values: b }) => {
                for v in b {
                    a.insert(v.clone());
                }
            }
            (AggState::Sum { sum: a }, AggState::Sum { sum: b }) => *a += *b,
            (AggState::Avg { sum: a1, count: c1 }, AggState::Avg { sum: a2, count: c2 }) => {
                *a1 += *a2;
                *c1 += *c2;
            }
            (
                AggState::Min {
                    min_num: a,
                    min_str: sa,
                },
                AggState::Min {
                    min_num: b,
                    min_str: sb,
                },
            ) => {
                if let (Some(x), Some(y)) = (*a, *b) {
                    if y < x {
                        *a = Some(y);
                    }
                } else if a.is_none() {
                    *a = *b;
                }
                match (sa.as_ref(), sb.as_ref()) {
                    (Some(x), Some(y)) => {
                        if y < x {
                            *sa = Some(y.clone());
                        }
                    }
                    (None, Some(y)) => *sa = Some(y.clone()),
                    _ => {}
                }
            }
            (
                AggState::Max {
                    max_num: a,
                    max_str: sa,
                },
                AggState::Max {
                    max_num: b,
                    max_str: sb,
                },
            ) => {
                if let (Some(x), Some(y)) = (*a, *b) {
                    if y > x {
                        *a = Some(y);
                    }
                } else if a.is_none() {
                    *a = *b;
                }
                match (sa.as_ref(), sb.as_ref()) {
                    (Some(x), Some(y)) => {
                        if y > x {
                            *sa = Some(y.clone());
                        }
                    }
                    (None, Some(y)) => *sa = Some(y.clone()),
                    _ => {}
                }
            }
            _ => {}
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggPartial {
    pub specs: Vec<AggregateOpSpec>,
    pub group_by: Option<Vec<String>>,
    pub time_bucket: Option<TimeGranularity>,
    pub groups: HashMap<GroupKey, Vec<AggState>>, // states align with specs
}

impl AggPartial {
    pub fn merge(&mut self, other: &AggPartial) {
        for (k, v) in &other.groups {
            let entry = self.groups.entry(k.clone()).or_insert_with(|| v.clone());
            if entry.len() == v.len() {
                for (a, b) in entry.iter_mut().zip(v.iter()) {
                    a.merge(b);
                }
            }
        }
    }
}

pub fn snapshot_aggregator(agg: &AggregatorImpl) -> AggState {
    // Use finalize outputs where possible, but retain mergeability where needed
    match agg {
        AggregatorImpl::CountAll(a) => {
            // finalize gives Count(i64), but we can read via finalize
            if let AggOutput::Count(c) = a.finalize() {
                AggState::CountAll { count: c }
            } else {
                AggState::CountAll { count: 0 }
            }
        }
        AggregatorImpl::CountUnique(a) => AggState::CountUnique {
            values: a.values().clone(),
        },
        AggregatorImpl::CountField(a) => {
            // finalize gives Count(i64); represent as CountAll { count }
            if let AggOutput::Count(c) = a.finalize() {
                AggState::CountAll { count: c }
            } else {
                AggState::CountAll { count: 0 }
            }
        }
        AggregatorImpl::Sum(a) => {
            if let AggOutput::Sum(s) = a.finalize() {
                AggState::Sum { sum: s }
            } else {
                AggState::Sum { sum: 0 }
            }
        }
        AggregatorImpl::Min(a) => match a.finalize() {
            AggOutput::Min(s) => {
                if let Ok(n) = s.parse::<i64>() {
                    AggState::Min {
                        min_num: Some(n),
                        min_str: None,
                    }
                } else {
                    AggState::Min {
                        min_num: None,
                        min_str: Some(s),
                    }
                }
            }
            _ => AggState::Min {
                min_num: None,
                min_str: None,
            },
        },
        AggregatorImpl::Max(a) => match a.finalize() {
            AggOutput::Max(s) => {
                if let Ok(n) = s.parse::<i64>() {
                    AggState::Max {
                        max_num: Some(n),
                        max_str: None,
                    }
                } else {
                    AggState::Max {
                        max_num: None,
                        max_str: Some(s),
                    }
                }
            }
            _ => AggState::Max {
                max_num: None,
                max_str: None,
            },
        },
        AggregatorImpl::Avg(a) => {
            // Use sum_count() instead of finalize() to preserve mergeable state
            // This allows accurate merging of AVG aggregates across shards/segments
            let (sum, count) = a.sum_count();
            AggState::Avg { sum, count }
        }
    }
}
