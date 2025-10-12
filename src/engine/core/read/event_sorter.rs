use crate::command::types::OrderSpec;
use crate::engine::core::{Event, FieldComparator};

/// Handles sorting of events based on a field and direction.
///
/// This struct encapsulates the sorting logic for events,
/// using the FieldComparator for the actual comparison.
#[derive(Debug, Clone)]
pub struct EventSorter {
    field: String,
    ascending: bool,
}

impl EventSorter {
    /// Creates a new EventSorter.
    pub fn new(field: String, ascending: bool) -> Self {
        Self { field, ascending }
    }

    /// Creates an EventSorter from an OrderSpec.
    pub fn from_order_spec(order_spec: &OrderSpec) -> Self {
        Self {
            field: order_spec.field.clone(),
            ascending: !order_spec.desc, // desc=true means ascending=false
        }
    }

    /// Returns the field this sorter operates on.
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Returns whether this sorter sorts in ascending order.
    pub fn is_ascending(&self) -> bool {
        self.ascending
    }

    /// Sorts a mutable slice of events in place.
    pub fn sort(&self, events: &mut [Event]) {
        FieldComparator::sort_by_field(events, &self.field, self.ascending);
    }

    /// Sorts a vector of events in place.
    pub fn sort_vec(&self, events: &mut Vec<Event>) {
        self.sort(events.as_mut_slice());
    }
}

impl std::fmt::Display for EventSorter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let direction = if self.ascending { "ASC" } else { "DESC" };
        write!(f, "EventSorter({} {})", self.field, direction)
    }
}
