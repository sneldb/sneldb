use crate::engine::core::CandidateZone;
use crate::engine::core::Event;
use crate::engine::core::event::event_builder::EventBuilder;
use crate::engine::core::event::event_id::EventId;
use crate::engine::core::filter::condition::{FieldAccessor, PreparedAccessor};
use crate::engine::core::read::sequence::group::RowIndex;
use crate::engine::core::read::sequence::matcher::MatchedSequenceIndices;
use crate::engine::types::ScalarValue;
use std::collections::HashMap;
use tracing::{debug, info, trace, warn};

/// Represents a fully materialized matched sequence.
///
/// This is the final output of sequence matching, containing actual Event objects.
#[derive(Debug, Clone)]
pub struct MatchedSequence {
    /// The link field value for this matched sequence
    pub link_value: ScalarValue,
    /// The events in the matched sequence
    pub events: Vec<Event>,
}

/// Materializes matched sequences from columnar data.
///
/// This class encapsulates the logic for converting row indices back into
/// materialized Event objects. It only materializes events that are part of
/// matched sequences, avoiding unnecessary work.
///
/// # Performance
///
/// - Only materializes matched sequences (not all events)
/// - Reuses EventBuilder for efficient construction
/// - Works directly with columnar data
pub struct SequenceMaterializer;

impl SequenceMaterializer {
    /// Creates a new `SequenceMaterializer`.
    pub fn new() -> Self {
        Self
    }

    /// Materializes matched sequences from row indices.
    ///
    /// This method converts `MatchedSequenceIndices` into `MatchedSequence` objects
    /// by building Event objects from the columnar data at the specified row indices.
    ///
    /// # Arguments
    ///
    /// * `matches` - Matched sequence indices to materialize
    /// * `zones_by_event_type` - Zones containing columnar data for each event type
    ///
    /// # Returns
    ///
    /// Vector of materialized matched sequences
    pub fn materialize_matches(
        &self,
        matches: Vec<MatchedSequenceIndices>,
        zones_by_event_type: &HashMap<String, Vec<CandidateZone>>,
    ) -> Vec<MatchedSequence> {
        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::sequence::materializer",
                match_count = matches.len(),
                "Starting materialization of matched sequences"
            );
        }

        let mut results = Vec::with_capacity(matches.len());
        let mut events_materialized = 0usize;
        let mut events_failed = 0usize;

        for (idx, match_indices) in matches.iter().enumerate() {
            if tracing::enabled!(tracing::Level::TRACE) {
                trace!(
                    target: "sneldb::sequence::materializer",
                    match_index = idx,
                    row_count = match_indices.matched_rows.len(),
                    "Materializing sequence"
                );
            }

            let events: Vec<Event> = match_indices
                .matched_rows
                .iter()
                .filter_map(|(event_type, row_index)| {
                    self.build_event_from_row_index(event_type, row_index, zones_by_event_type)
                })
                .collect();

            if !events.is_empty() {
                events_materialized += events.len();
                results.push(MatchedSequence {
                    link_value: match_indices.link_value.clone(),
                    events,
                });
            } else {
                events_failed += match_indices.matched_rows.len();
                if tracing::enabled!(tracing::Level::WARN) {
                    warn!(
                        target: "sneldb::sequence::materializer",
                        match_index = idx,
                        row_count = match_indices.matched_rows.len(),
                        "Failed to materialize sequence (all events failed)"
                    );
                }
            }
        }

        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::sequence::materializer",
                sequences_materialized = results.len(),
                events_materialized = events_materialized,
                events_failed = events_failed,
                "Materialized matched sequences"
            );
        }

        results
    }

    /// Builds a single Event from a row index in columnar data.
    ///
    /// This method extracts all field values from the columnar data at the specified
    /// row index and constructs an Event object. It follows the same pattern as
    /// `ConditionEvaluator::evaluate_zones_with_limit`.
    ///
    /// # Arguments
    ///
    /// * `event_type` - The type of event to build
    /// * `row_index` - The row index within the zone
    /// * `zones_by_event_type` - Zones containing columnar data
    ///
    /// # Returns
    ///
    /// The materialized Event, or None if the row index is invalid
    fn build_event_from_row_index(
        &self,
        event_type: &str,
        row_index: &RowIndex,
        zones_by_event_type: &HashMap<String, Vec<CandidateZone>>,
    ) -> Option<Event> {
        let zones = match zones_by_event_type.get(event_type) {
            Some(z) => z,
            None => {
                if tracing::enabled!(tracing::Level::WARN) {
                    warn!(
                        target: "sneldb::sequence::materializer",
                        event_type = %event_type,
                        "No zones found for event type"
                    );
                }
                return None;
            }
        };

        let zone = match zones.get(row_index.zone_idx) {
            Some(z) => z,
            None => {
                if tracing::enabled!(tracing::Level::WARN) {
                    warn!(
                        target: "sneldb::sequence::materializer",
                        event_type = %event_type,
                        zone_idx = row_index.zone_idx,
                        zone_count = zones.len(),
                        "Zone index out of bounds"
                    );
                }
                return None;
            }
        };

        let accessor = PreparedAccessor::new(&zone.values);
        let row_idx = row_index.row_idx;

        // Validate row index
        if row_idx >= accessor.event_count() {
            if tracing::enabled!(tracing::Level::WARN) {
                warn!(
                    target: "sneldb::sequence::materializer",
                    event_type = %event_type,
                    row_idx = row_idx,
                    event_count = accessor.event_count(),
                    "Row index out of bounds"
                );
            }
            return None;
        }

        let mut builder = EventBuilder::new();

        // Extract context_id
        if let Some(context_id) = accessor.get_str_at("context_id", row_idx) {
            builder.add_field("context_id", context_id);
        }

        // Extract timestamp
        if let Some(timestamp) = accessor.get_i64_at("timestamp", row_idx) {
            builder.add_field_i64("timestamp", timestamp);
        }

        // Extract event_type
        builder.add_field("event_type", event_type);

        // Extract all payload fields
        for (field, column_values) in &zone.values {
            // Skip system fields (already handled above)
            if matches!(field.as_str(), "context_id" | "timestamp" | "event_type") {
                continue;
            }

            // Extract value based on column type
            if let Some(i64_val) = column_values.get_i64_at(row_idx) {
                builder.add_field_i64(field, i64_val);
            } else if let Some(u64_val) = column_values.get_u64_at(row_idx) {
                builder.add_field_i64(field, u64_val as i64);
            } else if let Some(f64_val) = column_values.get_f64_at(row_idx) {
                builder.add_field_f64(field, f64_val);
            } else if let Some(bool_val) = column_values.get_bool_at(row_idx) {
                builder.add_field_bool(field, bool_val);
            } else if let Some(str_val) = column_values.get_str_at(row_idx) {
                builder.add_field(field, str_val);
            }
        }

        let mut event = builder.build();
        // Generate event ID (using a simple approach - in production this would use EventIdGenerator)
        event.set_event_id(EventId::from(0)); // Placeholder - should use proper ID generation

        Some(event)
    }
}

impl Default for SequenceMaterializer {
    fn default() -> Self {
        Self::new()
    }
}
