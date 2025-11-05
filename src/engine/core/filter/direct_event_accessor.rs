use crate::engine::core::Event;

/// Direct accessor for evaluating conditions against a single event without materializing all fields.
/// This provides optimized field access that avoids creating a HashMap of all event fields,
/// significantly improving memtable query performance.
pub struct DirectEventAccessor<'a> {
    pub(crate) event: &'a Event,
}

impl<'a> DirectEventAccessor<'a> {
    pub fn new(event: &'a Event) -> Self {
        Self { event }
    }
}

impl<'a> DirectEventAccessor<'a> {
    /// Get a field value as a string, same as Event::get_field_value
    #[inline]
    pub fn get_field_value(&self, field: &str) -> String {
        self.event.get_field_value(field)
    }

    /// Get a field value as an i64, with fallback parsing for string numbers
    #[inline]
    pub fn get_field_as_i64(&self, field: &str) -> Option<i64> {
        match field {
            "timestamp" => Some(self.event.timestamp as i64),
            _ => self.event.payload.get(field).and_then(|v| {
                v.as_i64()
                    .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
            }),
        }
    }
}
