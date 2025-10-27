use std::collections::HashMap;

use crate::engine::core::Event;
use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::event::event_builder::EventBuilder;

use super::ResultSink;

/// Collects Events as the final result
pub struct EventSink {
    events: Vec<Event>,
}

impl EventSink {
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }
    pub fn into_events(self) -> Vec<Event> {
        self.events
    }
}

impl ResultSink for EventSink {
    fn on_row(&mut self, row_idx: usize, columns: &HashMap<String, ColumnValues>) {
        // Pre-allocate with known column count for better performance
        let mut builder = EventBuilder::with_capacity(columns.len());

        // Emit all available fields from this zone row
        // Use type information to skip unnecessary type checks
        for (field, values) in columns {
            // Fast path: use known column type if available
            use crate::engine::core::column::format::PhysicalType;
            match values.physical_type() {
                Some(PhysicalType::I64) => {
                    if let Some(n) = values.get_i64_at(row_idx) {
                        builder.add_field_i64(field, n);
                    } else {
                        builder.add_field_null(field);
                    }
                }
                Some(PhysicalType::U64) => {
                    if let Some(n) = values.get_u64_at(row_idx) {
                        builder.add_field_u64(field, n);
                    } else {
                        builder.add_field_null(field);
                    }
                }
                Some(PhysicalType::F64) => {
                    if let Some(f) = values.get_f64_at(row_idx) {
                        builder.add_field_f64(field, f);
                    } else {
                        builder.add_field_null(field);
                    }
                }
                Some(PhysicalType::Bool) => {
                    if let Some(b) = values.get_bool_at(row_idx) {
                        builder.add_field_bool(field, b);
                    } else {
                        builder.add_field_null(field);
                    }
                }
                Some(PhysicalType::I32Date) => {
                    // I32Date is stored as i64 internally
                    if let Some(n) = values.get_i64_at(row_idx) {
                        builder.add_field_i64(field, n);
                    } else {
                        builder.add_field_null(field);
                    }
                }
                Some(PhysicalType::VarBytes) | None => {
                    // VarBytes or untyped: check all types (legacy path)
                    if let Some(n) = values.get_i64_at(row_idx) {
                        builder.add_field_i64(field, n);
                    } else if let Some(n) = values.get_u64_at(row_idx) {
                        builder.add_field_u64(field, n);
                    } else if let Some(f) = values.get_f64_at(row_idx) {
                        builder.add_field_f64(field, f);
                    } else if let Some(b) = values.get_bool_at(row_idx) {
                        builder.add_field_bool(field, b);
                    } else {
                        match values.get_str_at(row_idx) {
                            Some(val) => builder.add_field(field, val),
                            None => builder.add_field_null(field),
                        }
                    }
                }
            }
        }
        self.events.push(builder.build());
    }

    fn on_event(&mut self, event: &Event) {
        self.events.push(event.clone());
    }
}
