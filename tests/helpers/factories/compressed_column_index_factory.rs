use std::collections::HashMap;

use crate::engine::core::column::compression::{CompressedColumnIndex, ZoneBlockEntry};

pub struct CompressedColumnIndexFactory {
    entries: HashMap<u32, ZoneBlockEntry>,
}

impl CompressedColumnIndexFactory {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn with_zone_entry(
        mut self,
        zone_id: u32,
        block_start: u64,
        comp_len: u32,
        uncomp_len: u32,
        num_rows: u32,
        in_block_offsets: Vec<u32>,
    ) -> Self {
        self.entries.insert(
            zone_id,
            ZoneBlockEntry {
                zone_id,
                block_start,
                comp_len,
                uncomp_len,
                num_rows,
            },
        );
        Self {
            entries: self.entries,
        }
    }

    pub fn create(self) -> CompressedColumnIndex {
        CompressedColumnIndex {
            entries: self.entries,
        }
    }
}
