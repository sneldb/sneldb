use crate::engine::core::{Event, MemTable};
use crate::engine::errors::StoreError;

/// Factory to build MemTable instances for testing
pub struct MemTableFactory {
    capacity: usize,
    events: Vec<Event>,
}

impl MemTableFactory {
    /// Create a new factory with default capacity 10
    pub fn new() -> Self {
        Self {
            capacity: 10,
            events: vec![],
        }
    }

    /// Set the capacity for the MemTable
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }

    /// Add a single event
    pub fn with_event(mut self, event: Event) -> Self {
        self.events.push(event);
        self
    }

    /// Add multiple events
    pub fn with_events(mut self, events: Vec<Event>) -> Self {
        self.events.extend(events);
        self
    }

    /// Finalize and create the MemTable
    pub fn create(self) -> Result<MemTable, StoreError> {
        let mut memtable = MemTable::new(self.capacity);
        for event in self.events {
            memtable.insert(event)?; // run validations
        }
        Ok(memtable)
    }
}
