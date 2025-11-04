use crate::engine::materialize::MaterializationEntry;

pub struct ShowRefreshOutcome {
    entry: MaterializationEntry,
}

impl ShowRefreshOutcome {
    pub fn new(entry: MaterializationEntry) -> Self {
        Self { entry }
    }

    pub fn into_entry(self) -> MaterializationEntry {
        self.entry
    }
}
