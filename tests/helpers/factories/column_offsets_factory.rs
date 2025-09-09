use crate::engine::core::{ColumnKey, ColumnOffsets, ZoneId};

pub struct ColumnOffsetsFactory {
    inner: ColumnOffsets,
}

impl ColumnOffsetsFactory {
    pub fn new() -> Self {
        Self {
            inner: ColumnOffsets::new(),
        }
    }

    pub fn with_entry(
        mut self,
        uid: &str,
        field: &str,
        zone_id: ZoneId,
        offsets: Vec<u64>,
        values: Vec<&str>,
    ) -> Self {
        let key: ColumnKey = (uid.to_string(), field.to_string());

        for (offset, value) in offsets.into_iter().zip(values.into_iter()) {
            self.inner
                .insert_offset(key.clone(), zone_id, offset, value.to_string());
        }

        self
    }

    pub fn create(self) -> ColumnOffsets {
        self.inner
    }
}
