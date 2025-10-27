use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, Hasher};

use ahash::RandomState as AHashRandomState;

use crate::command::types::TimeGranularity;
use crate::engine::core::Event;
use crate::engine::core::column::column_values::ColumnValues;

use super::time_bucketing::bucket_of;

#[derive(Clone, Debug, Eq)]
pub struct GroupKey {
    // Precomputed 64-bit hash to speed up HashMap lookups and reduce per-insert hashing cost
    pub(crate) prehash: u64,
    pub(crate) bucket: Option<u64>,
    pub(crate) groups: Vec<String>,
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

impl GroupKey {
    /// Construct a GroupKey from a columnar row
    pub fn from_row(
        bucket: Option<&TimeGranularity>,
        group_by: Option<&[String]>,
        time_field: &str,
        columns: &HashMap<String, ColumnValues>,
        row_idx: usize,
    ) -> Self {
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
        let prehash = Self::compute_prehash(bucket_val, &groups);
        Self {
            prehash,
            bucket: bucket_val,
            groups,
        }
    }

    /// Construct a GroupKey from a row-based Event
    pub fn from_event(
        bucket: Option<&TimeGranularity>,
        group_by: Option<&[String]>,
        time_field: &str,
        event: &Event,
    ) -> Self {
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
        let prehash = Self::compute_prehash(bucket_val, &groups);
        Self {
            prehash,
            bucket: bucket_val,
            groups,
        }
    }

    #[inline]
    fn compute_prehash(bucket_val: Option<u64>, groups: &[String]) -> u64 {
        let mut hasher = AHashRandomState::with_seeds(0, 0, 0, 0).build_hasher();
        bucket_val.hash(&mut hasher);
        for g in groups {
            g.hash(&mut hasher);
        }
        hasher.finish()
    }

    #[allow(dead_code)]
    #[inline]
    pub fn bucket(&self) -> Option<u64> {
        self.bucket
    }

    #[allow(dead_code)]
    #[inline]
    pub fn groups(&self) -> &Vec<String> {
        &self.groups
    }

    /// Consume and return parts for downstream conversion
    #[allow(dead_code)]
    pub fn into_parts(self) -> (Option<u64>, Vec<String>) {
        (self.bucket, self.groups)
    }
}

// Backward-compat helpers removed; use GroupKey::{from_row, from_event} or GroupKeyBuilder
