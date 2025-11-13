use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, Hasher};

use ahash::RandomState as AHashRandomState;

use crate::command::types::TimeGranularity;
use crate::engine::core::Event;
use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::types::ScalarValue;

use super::time_bucketing::bucket_of;

/// Group value that can hold either an integer or a string to avoid string allocations
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum GroupValue {
    Int(i64),
    Str(String),
}

impl GroupValue {
    /// Convert to string representation (for output)
    pub fn to_string(&self) -> String {
        match self {
            GroupValue::Int(i) => i.to_string(),
            GroupValue::Str(s) => s.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq)]
pub struct GroupKey {
    // Precomputed 64-bit hash to speed up HashMap lookups and reduce per-insert hashing cost
    pub(crate) prehash: u64,
    pub(crate) bucket: Option<u64>,
    // Use GroupValue to avoid string allocations for numeric values
    pub(crate) groups: Vec<GroupValue>,
    // String representation for finalization output (lazy - only computed when needed)
    pub(crate) groups_str: Option<Vec<String>>,
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
    /// Construct a GroupKey from a columnar row using column indices to avoid HashMap lookups
    pub fn from_row_with_indices(
        bucket: Option<&TimeGranularity>,
        group_by: Option<&[String]>,
        time_field: &str,
        columns: &HashMap<String, ColumnValues>,
        column_indices: Option<&HashMap<String, usize>>,
        row_idx: usize,
    ) -> Self {
        let mut bucket_val: Option<u64> = None;
        if let Some(gr) = bucket {
            let ts_col = columns.get(time_field);

            if let Some(ts_col) = ts_col {
                if let Some(ts) = ts_col.get_i64_at(row_idx) {
                    bucket_val = Some(bucket_of(ts as u64, gr));
                }
            }
        }

        let mut groups: Vec<GroupValue> = if let Some(gb) = group_by {
            Vec::with_capacity(gb.len())
        } else {
            Vec::new()
        };

        if let Some(gb) = group_by {
            for name in gb.iter() {
                let col = if let Some(indices) = column_indices {
                    indices.get(name).and_then(|_| columns.get(name))
                } else {
                    columns.get(name)
                };

                if let Some(col) = col {
                    // Use integer values when possible to avoid string allocations
                    let val = if let Some(u) = col.get_u64_at(row_idx) {
                        GroupValue::Int(u as i64)
                    } else if let Some(i) = col.get_i64_at(row_idx) {
                        GroupValue::Int(i)
                    } else if let Some(f) = col.get_f64_at(row_idx) {
                        // For floats, we still need strings for exact representation
                        GroupValue::Str(f.to_string())
                    } else if let Some(s) = col.get_str_at(row_idx) {
                        GroupValue::Str(s.to_string())
                    } else if let Some(b) = col.get_bool_at(row_idx) {
                        GroupValue::Str(b.to_string())
                    } else {
                        GroupValue::Str(String::new())
                    };
                    groups.push(val);
                } else {
                    groups.push(GroupValue::Str(String::new()));
                }
            }
        }

        let prehash = Self::compute_prehash(bucket_val, &groups);
        Self {
            prehash,
            bucket: bucket_val,
            groups,
            groups_str: None, // Lazy - only computed when needed for finalization
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
                match event.payload.get(time_field) {
                    Some(value) => value.as_u64().unwrap_or(0),
                    None => 0,
                }
            };
            bucket_val = Some(bucket_of(ts, gr));
        }
        let mut groups: Vec<GroupValue> = Vec::new();
        if let Some(gb) = group_by {
            for name in gb.iter() {
                // Use get_field_scalar to avoid string allocation when possible
                let val = match event.get_field_scalar(name) {
                    Some(ScalarValue::Utf8(s)) => GroupValue::Str(s.clone()),
                    Some(ScalarValue::Int64(i)) => GroupValue::Int(i),
                    Some(ScalarValue::Float64(f)) => GroupValue::Str(f.to_string()),
                    Some(ScalarValue::Boolean(b)) => GroupValue::Str(b.to_string()),
                    Some(ScalarValue::Timestamp(ts)) => GroupValue::Int(ts),
                    _ => GroupValue::Str(String::new()),
                };
                groups.push(val);
            }
        }
        let prehash = Self::compute_prehash(bucket_val, &groups);
        Self {
            prehash,
            bucket: bucket_val,
            groups,
            groups_str: None, // Lazy - only computed when needed for finalization
        }
    }

    #[inline]
    fn compute_prehash(bucket_val: Option<u64>, groups: &[GroupValue]) -> u64 {
        let mut hasher = AHashRandomState::with_seeds(0, 0, 0, 0).build_hasher();
        bucket_val.hash(&mut hasher);
        for g in groups {
            g.hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Compute prehash directly from column values without allocating GroupKey
    /// This is optimized for the columnar path where we only need the hash initially
    #[inline]
    pub(crate) fn compute_prehash_from_columns(
        time_bucket: Option<&TimeGranularity>,
        group_by: Option<&[String]>,
        time_field: &str,
        columns: &HashMap<String, ColumnValues>,
        column_indices: Option<&HashMap<String, usize>>,
        row_idx: usize,
    ) -> u64 {
        use super::time_bucketing::bucket_of;

        let mut hasher = AHashRandomState::with_seeds(0, 0, 0, 0).build_hasher();

        // Hash bucket value if time bucketing is enabled
        let bucket_val: Option<u64> = if let Some(gr) = time_bucket {
            if let Some(ts_col) = columns.get(time_field) {
                if let Some(ts) = ts_col.get_i64_at(row_idx) {
                    Some(bucket_of(ts as u64, gr))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        bucket_val.hash(&mut hasher);

        // Hash group values directly from columns without allocating GroupValue
        if let Some(gb) = group_by {
            for name in gb.iter() {
                let col = if let Some(indices) = column_indices {
                    indices.get(name).and_then(|_| columns.get(name))
                } else {
                    columns.get(name)
                };

                if let Some(col) = col {
                    // Hash directly from column values without string conversion
                    if let Some(u) = col.get_u64_at(row_idx) {
                        (GroupValue::Int(u as i64)).hash(&mut hasher);
                    } else if let Some(i) = col.get_i64_at(row_idx) {
                        (GroupValue::Int(i)).hash(&mut hasher);
                    } else if let Some(f) = col.get_f64_at(row_idx) {
                        // For floats, hash the bits directly to avoid string allocation
                        f.to_bits().hash(&mut hasher);
                    } else if let Some(s) = col.get_str_at(row_idx) {
                        // Hash string directly without cloning
                        s.hash(&mut hasher);
                    } else if let Some(b) = col.get_bool_at(row_idx) {
                        b.hash(&mut hasher);
                    } else {
                        // Hash empty value
                        Option::<i64>::None.hash(&mut hasher);
                    }
                } else {
                    // Hash empty value for missing column
                    Option::<i64>::None.hash(&mut hasher);
                }
            }
        }

        hasher.finish()
    }

    /// Get groups_str, computing it lazily if not already computed
    /// This avoids allocating strings during aggregation - they're only created when finalizing
    pub(crate) fn groups_str(&mut self) -> &Vec<String> {
        if self.groups_str.is_none() {
            self.groups_str = Some(self.groups.iter().map(|g| g.to_string()).collect());
        }
        self.groups_str.as_ref().unwrap()
    }
}
