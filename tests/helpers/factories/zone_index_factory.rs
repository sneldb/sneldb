use crate::engine::core::ZoneIndex;
use std::collections::BTreeMap;

pub struct ZoneIndexFactory {
    entries: BTreeMap<String, BTreeMap<String, Vec<u32>>>,
}

impl ZoneIndexFactory {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    /// Adds a mapping: event_type -> context_id -> zone_id
    pub fn with_entry(
        mut self,
        event_type: impl Into<String>,
        context_id: impl Into<String>,
        zone_id: u32,
    ) -> Self {
        let event_type = event_type.into();
        let context_id = context_id.into();

        self.entries
            .entry(event_type)
            .or_default()
            .entry(context_id)
            .or_default()
            .push(zone_id);

        self
    }

    pub fn create(self) -> ZoneIndex {
        ZoneIndex {
            index: self.entries,
        }
    }
}
