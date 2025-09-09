use crate::engine::core::Event;
use crate::engine::errors::StoreError;
use std::collections::BTreeMap;

/// In-memory sorted buffer of events keyed by `context_id`.
/// Flushes to disk as a segment when full.
#[derive(Debug, Clone)]
pub struct MemTable {
    /// Sorted map of events by context_id (lexicographic order).
    pub events: BTreeMap<String, Vec<Event>>,

    /// Maximum number of total events allowed before flush is triggered.
    pub capacity: usize,

    /// Current count of stored events.
    pub count: usize,
}

impl MemTable {
    /// Create a new empty MemTable with a given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            events: BTreeMap::new(),
            capacity,
            count: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns true if MemTable is at or above flush threshold.
    pub fn is_full(&self) -> bool {
        self.count >= self.capacity
    }

    /// Returns the number of events currently stored.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Checks if the table is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Iterate over all events in `context_id` sorted order.
    pub fn iter(&self) -> impl Iterator<Item = &Event> {
        self.events.values().flat_map(|bucket| bucket.iter())
    }

    /// Moves all events out for flushing.
    pub fn take(self) -> BTreeMap<String, Vec<Event>> {
        self.events
    }

    pub fn flush(&mut self) {
        self.events.clear();
        self.count = 0;
    }

    /// Insert an event into the MemTable with validation.
    /// Returns an error if the event is invalid.
    pub fn insert(&mut self, event: Event) -> Result<(), StoreError> {
        // Validation
        if event.context_id.trim().is_empty() {
            return Err(StoreError::InvalidContextId);
        }

        if event.event_type.trim().is_empty() {
            return Err(StoreError::InvalidEventType);
        }

        // Insert into memtable (keeps sorted by context_id)
        self.insert_internal(event);
        Ok(())
    }

    /// Internal insertion logic
    pub(crate) fn insert_internal(&mut self, event: Event) {
        self.events
            .entry(event.context_id.clone())
            .or_default()
            .push(event);
        self.count += 1;
    }
}
