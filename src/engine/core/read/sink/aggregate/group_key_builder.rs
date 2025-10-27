use std::collections::HashMap;

use crate::command::types::TimeGranularity;
use crate::engine::core::Event;
use crate::engine::core::column::column_values::ColumnValues;

use super::group_key::GroupKey;

#[allow(dead_code)]
pub struct GroupKeyBuilder {
    bucket: Option<TimeGranularity>,
    group_by: Option<Vec<String>>,
    time_field: String,
}

#[allow(dead_code)]
impl GroupKeyBuilder {
    pub fn new(
        bucket: Option<TimeGranularity>,
        group_by: Option<Vec<String>>,
        time_field: String,
    ) -> Self {
        Self {
            bucket,
            group_by,
            time_field,
        }
    }

    #[inline]
    pub fn build_from_row(
        &self,
        columns: &HashMap<String, ColumnValues>,
        row_idx: usize,
    ) -> GroupKey {
        GroupKey::from_row(
            self.bucket.as_ref(),
            self.group_by.as_deref(),
            &self.time_field,
            columns,
            row_idx,
        )
    }

    #[inline]
    pub fn build_from_event(&self, event: &Event) -> GroupKey {
        GroupKey::from_event(
            self.bucket.as_ref(),
            self.group_by.as_deref(),
            &self.time_field,
            event,
        )
    }
}
