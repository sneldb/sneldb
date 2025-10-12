use std::collections::BTreeMap;

use crate::engine::core::{ColumnKey, WriteJob};

pub struct ColumnGroupBuilder {
    groups: BTreeMap<(ColumnKey, u32), (Vec<u8>, Vec<u32>, Vec<String>)>,
}

impl ColumnGroupBuilder {
    pub fn new() -> Self {
        Self {
            groups: BTreeMap::new(),
        }
    }

    pub fn add(&mut self, job: &WriteJob) {
        let entry = self
            .groups
            .entry((job.key.clone(), job.zone_id))
            .or_insert_with(|| (Vec::new(), Vec::new(), Vec::new()));
        let (buf, offs, values) = entry;
        offs.push(buf.len() as u32);
        let bytes = job.value.as_bytes();
        if bytes.len() <= u16::MAX as usize {
            buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(bytes);
            values.push(job.value.clone());
        }
    }

    pub fn finish(self) -> BTreeMap<(ColumnKey, u32), (Vec<u8>, Vec<u32>, Vec<String>)> {
        self.groups
    }
}
