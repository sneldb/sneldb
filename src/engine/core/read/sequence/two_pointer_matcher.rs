use crate::command::types::{EventSequence, SequenceLink};
use crate::engine::core::CandidateZone;
use crate::engine::core::filter::condition::{FieldAccessor, PreparedAccessor};
use crate::engine::core::read::sequence::group::{GroupedRowIndices, RowIndex};
use crate::engine::core::read::sequence::matching_strategy::MatchingStrategy;
use crate::engine::core::read::sequence::matcher::MatchedSequenceIndices;
use crate::engine::core::read::sequence::where_evaluator::SequenceWhereEvaluator;
use std::collections::HashMap;
use tracing::{debug, trace};

/// Matches sequences using two-pointer technique (optimized for 2-event sequences).
///
/// This is the existing optimized algorithm extracted from SequenceMatcher.
/// It handles sequences with a single link (A FOLLOWED BY B or A PRECEDED BY B).
pub struct TwoPointerMatcher {
    sequence: EventSequence,
}

impl TwoPointerMatcher {
    pub fn new(sequence: EventSequence) -> Self {
        Self { sequence }
    }

    /// Matches A FOLLOWED BY B using two-pointer technique.
    ///
    /// This implements O(n+m) matching by using two pointers that advance
    /// through sorted row indices. When event B's timestamp is greater than or equal to
    /// event A's timestamp, we have a match. Events with the same timestamp are considered matches.
    fn match_followed_by(
        &self,
        group: &GroupedRowIndices,
        event_type_a: &str,
        event_type_b: &str,
        zones_by_event_type: &HashMap<String, Vec<CandidateZone>>,
        where_evaluator: Option<&SequenceWhereEvaluator>,
        time_field: &str,
    ) -> Vec<MatchedSequenceIndices> {
        let a_indices = group
            .rows_by_type
            .get(event_type_a)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let b_indices = group
            .rows_by_type
            .get(event_type_b)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        if a_indices.is_empty() || b_indices.is_empty() {
            return Vec::new();
        }

        let zones_a = zones_by_event_type.get(event_type_a).unwrap();
        let zones_b = zones_by_event_type.get(event_type_b).unwrap();

        let mut results = Vec::new();
        let mut a_ptr = 0;
        let mut b_ptr = 0;
        let mut comparisons = 0usize;
        let mut timestamp_passed = 0usize;
        let mut where_failed = 0usize;
        let mut where_passed = 0usize;

        // Log group statistics
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::sequence::two_pointer",
                event_type_a = %event_type_a,
                event_type_b = %event_type_b,
                a_count = a_indices.len(),
                b_count = b_indices.len(),
                link_value = ?group.link_value,
                "Starting two-pointer matching for FOLLOWED BY"
            );
        }

        // Two-pointer matching: advance both pointers through sorted indices
        while a_ptr < a_indices.len() && b_ptr < b_indices.len() {
            comparisons += 1;
            let row_a = &a_indices[a_ptr];
            let row_b = &b_indices[b_ptr];

            let ts_a = self.get_timestamp(zones_a, row_a, time_field);
            let ts_b = self.get_timestamp(zones_b, row_b, time_field);

            if tracing::enabled!(tracing::Level::DEBUG) && comparisons <= 20 {
                debug!(
                    target: "sneldb::sequence::two_pointer",
                    comparison_num = comparisons,
                    ts_a = ts_a,
                    ts_b = ts_b,
                    ts_b_ge_ts_a = ts_b >= ts_a,
                    link_value = ?group.link_value,
                    "Timestamp comparison"
                );
            }

            if ts_b >= ts_a {
                timestamp_passed += 1;
                // Match found: event B follows event A (or happens at the same time)
                // Apply WHERE clause filtering if present
                let passes_where = self.matches_where_clause(
                    event_type_a,
                    zones_a,
                    row_a,
                    event_type_b,
                    zones_b,
                    row_b,
                    where_evaluator,
                );

                if tracing::enabled!(tracing::Level::DEBUG) && timestamp_passed <= 20 {
                    debug!(
                        target: "sneldb::sequence::two_pointer",
                        ts_a = ts_a,
                        ts_b = ts_b,
                        passes_where = passes_where,
                        link_value = ?group.link_value,
                        "Potential match found, WHERE clause result"
                    );
                }

                if passes_where {
                    where_passed += 1;
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        debug!(
                            target: "sneldb::sequence::two_pointer",
                            event_type_a = %event_type_a,
                            event_type_b = %event_type_b,
                            ts_a = ts_a,
                            ts_b = ts_b,
                            link_value = ?group.link_value,
                            "Match found and passed WHERE clause"
                        );
                    }
                    results.push(MatchedSequenceIndices {
                        link_value: group.link_value.clone(),
                        matched_rows: vec![
                            (event_type_a.to_string(), row_a.clone()),
                            (event_type_b.to_string(), row_b.clone()),
                        ],
                    });
                } else {
                    where_failed += 1;
                    if tracing::enabled!(tracing::Level::DEBUG) && where_failed <= 20 {
                        debug!(
                            target: "sneldb::sequence::two_pointer",
                            event_type_a = %event_type_a,
                            event_type_b = %event_type_b,
                            ts_a = ts_a,
                            ts_b = ts_b,
                            link_value = ?group.link_value,
                            "Match filtered out by WHERE clause"
                        );
                    }
                }
                a_ptr += 1;
            } else {
                // Event B is not after event A, advance b_ptr
                b_ptr += 1;
            }
        }

        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::sequence::two_pointer",
                event_type_a = %event_type_a,
                event_type_b = %event_type_b,
                matches = results.len(),
                comparisons = comparisons,
                timestamp_passed = timestamp_passed,
                where_passed = where_passed,
                where_failed = where_failed,
                link_value = ?group.link_value,
                "Completed FOLLOWED BY matching"
            );
        }

        results
    }

    /// Matches A PRECEDED BY B using reverse two-pointer technique.
    ///
    /// This is similar to FOLLOWED BY but works backwards from the end of
    /// the sorted indices.
    fn match_preceded_by(
        &self,
        group: &GroupedRowIndices,
        event_type_a: &str,
        event_type_b: &str,
        zones_by_event_type: &HashMap<String, Vec<CandidateZone>>,
        where_evaluator: Option<&SequenceWhereEvaluator>,
        time_field: &str,
    ) -> Vec<MatchedSequenceIndices> {
        let a_indices = group
            .rows_by_type
            .get(event_type_a)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let b_indices = group
            .rows_by_type
            .get(event_type_b)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        if a_indices.is_empty() || b_indices.is_empty() {
            return Vec::new();
        }

        let zones_a = zones_by_event_type.get(event_type_a).unwrap();
        let zones_b = zones_by_event_type.get(event_type_b).unwrap();

        if tracing::enabled!(tracing::Level::TRACE) {
            trace!(
                target: "sneldb::sequence::two_pointer",
                event_type_a = %event_type_a,
                event_type_b = %event_type_b,
                a_count = a_indices.len(),
                b_count = b_indices.len(),
                "Starting reverse two-pointer matching for PRECEDED BY"
            );
        }

        let mut results = Vec::new();
        let mut a_ptr = 0;
        let mut b_ptr = 0;
        let mut comparisons = 0usize;

        // Two-pointer matching: for each event A, find the latest event B that precedes it
        // We iterate through A events and for each, find the most recent B that happened before it
        while a_ptr < a_indices.len() && b_ptr < b_indices.len() {
            comparisons += 1;
            let row_a = &a_indices[a_ptr];
            let row_b = &b_indices[b_ptr];

            let ts_a = self.get_timestamp(zones_a, row_a, time_field);
            let ts_b = self.get_timestamp(zones_b, row_b, time_field);

            if tracing::enabled!(tracing::Level::TRACE) {
                trace!(
                    target: "sneldb::sequence::two_pointer",
                    ts_a = ts_a,
                    ts_b = ts_b,
                    ts_b_lt_ts_a = ts_b < ts_a,
                    "Comparing timestamps for PRECEDED BY"
                );
            }

            if ts_b < ts_a {
                // Found a B that precedes A - advance b_ptr to find the latest one
                // Keep advancing b_ptr while B still precedes A (two-pointer optimization)
                let mut latest_b_ptr = b_ptr;
                while latest_b_ptr + 1 < b_indices.len() {
                    let next_b = &b_indices[latest_b_ptr + 1];
                    let ts_next_b = self.get_timestamp(zones_b, next_b, time_field);
                    if ts_next_b < ts_a {
                        latest_b_ptr += 1;
                    } else {
                        break;
                    }
                }

                // Use the latest B that precedes A
                let latest_row_b = &b_indices[latest_b_ptr];

                // Apply WHERE clause filtering if present
                let passes_where = self.matches_where_clause(
                    event_type_b,
                    zones_b,
                    latest_row_b,
                    event_type_a,
                    zones_a,
                    row_a,
                    where_evaluator,
                );

                if tracing::enabled!(tracing::Level::TRACE) {
                    trace!(
                        target: "sneldb::sequence::two_pointer",
                        ts_a = ts_a,
                        ts_b = self.get_timestamp(zones_b, latest_row_b, time_field),
                        passes_where = passes_where,
                        "Potential match found (PRECEDED BY)"
                    );
                }

                if passes_where {
                    results.push(MatchedSequenceIndices {
                        link_value: group.link_value.clone(),
                        matched_rows: vec![
                            (event_type_b.to_string(), latest_row_b.clone()),
                            (event_type_a.to_string(), row_a.clone()),
                        ],
                    });
                } else if tracing::enabled!(tracing::Level::TRACE) {
                    trace!(
                        target: "sneldb::sequence::two_pointer",
                        "Match filtered out by WHERE clause (PRECEDED BY)"
                    );
                }

                // Move to next A event
                a_ptr += 1;
                // Optimize: Since indices are sorted, keep b_ptr at latest_b_ptr for next iteration
                // This maintains the two-pointer invariant (b_ptr points to the last B that could match)
                b_ptr = latest_b_ptr;
            } else {
                // B is not before A (ts_b >= ts_a), advance b_ptr to find earlier B events
                // Since indices are sorted by timestamp, we need to advance b_ptr
                b_ptr += 1;
            }
        }

        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::sequence::two_pointer",
                event_type_a = %event_type_a,
                event_type_b = %event_type_b,
                matches = results.len(),
                comparisons = comparisons,
                "Completed PRECEDED BY matching"
            );
        }

        results
    }

    /// Checks if a matched sequence passes WHERE clause conditions.
    ///
    /// Evaluates WHERE clause conditions for both events in the sequence.
    /// Returns true if all conditions pass, false otherwise.
    fn matches_where_clause(
        &self,
        event_type_a: &str,
        zones_a: &[CandidateZone],
        row_a: &RowIndex,
        event_type_b: &str,
        zones_b: &[CandidateZone],
        row_b: &RowIndex,
        where_evaluator: Option<&SequenceWhereEvaluator>,
    ) -> bool {
        if let Some(evaluator) = where_evaluator {
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(
                    target: "sneldb::sequence::two_pointer",
                    event_type_a = %event_type_a,
                    event_type_b = %event_type_b,
                    "Evaluating WHERE clause for sequence match"
                );
            }

            // Check WHERE clause for event A
            let zone_a = &zones_a[row_a.zone_idx];
            let passes_a = evaluator.evaluate_row(event_type_a, zone_a, row_a);
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(
                    target: "sneldb::sequence::two_pointer",
                    event_type = %event_type_a,
                    row_idx = row_a.row_idx,
                    zone_idx = row_a.zone_idx,
                    passes = passes_a,
                    "WHERE clause evaluation for event A"
                );
            }
            if !passes_a {
                if tracing::enabled!(tracing::Level::DEBUG) {
                    debug!(
                        target: "sneldb::sequence::two_pointer",
                        event_type = %event_type_a,
                        row_idx = row_a.row_idx,
                        "WHERE clause failed for event A"
                    );
                }
                return false;
            }

            // Check WHERE clause for event B
            let zone_b = &zones_b[row_b.zone_idx];
            let passes_b = evaluator.evaluate_row(event_type_b, zone_b, row_b);
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(
                    target: "sneldb::sequence::two_pointer",
                    event_type = %event_type_b,
                    row_idx = row_b.row_idx,
                    zone_idx = row_b.zone_idx,
                    passes = passes_b,
                    "WHERE clause evaluation for event B"
                );
            }
            if !passes_b {
                if tracing::enabled!(tracing::Level::DEBUG) {
                    debug!(
                        target: "sneldb::sequence::two_pointer",
                        event_type = %event_type_b,
                        row_idx = row_b.row_idx,
                        "WHERE clause failed for event B"
                    );
                }
                return false;
            }

            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(
                    target: "sneldb::sequence::two_pointer",
                    "Both events passed WHERE clause"
                );
            }
        } else {
            if tracing::enabled!(tracing::Level::TRACE) {
                trace!(
                    target: "sneldb::sequence::two_pointer",
                    "No WHERE evaluator, match passes"
                );
            }
        }
        true
    }

    /// Gets the timestamp for a specific row index from columnar data.
    ///
    /// This helper method extracts timestamps without materializing events.
    /// Uses the configured time_field.
    fn get_timestamp(&self, zones: &[CandidateZone], row_index: &RowIndex, time_field: &str) -> u64 {
        if let Some(zone) = zones.get(row_index.zone_idx) {
            let accessor = PreparedAccessor::new(&zone.values);
            let result = accessor
                .get_i64_at(time_field, row_index.row_idx)
                .map(|ts| ts as u64)
                .unwrap_or_else(|| {
                    if tracing::enabled!(tracing::Level::WARN) {
                        tracing::warn!(
                            target: "sneldb::sequence::two_pointer",
                            time_field = %time_field,
                            zone_idx = row_index.zone_idx,
                            row_idx = row_index.row_idx,
                            available_fields = ?zone.values.keys().collect::<Vec<_>>(),
                            "Failed to extract timestamp field, using 0"
                        );
                    }
                    0
                });
            result
        } else {
            if tracing::enabled!(tracing::Level::WARN) {
                tracing::warn!(
                    target: "sneldb::sequence::two_pointer",
                    zone_idx = row_index.zone_idx,
                    zones_len = zones.len(),
                    "Zone index out of bounds, using timestamp 0"
                );
            }
            0
        }
    }
}

impl MatchingStrategy for TwoPointerMatcher {
    fn match_in_group(
        &self,
        group: &GroupedRowIndices,
        zones_by_event_type: &HashMap<String, Vec<CandidateZone>>,
        where_evaluator: Option<&SequenceWhereEvaluator>,
        time_field: &str,
    ) -> Vec<MatchedSequenceIndices> {
        // Should only be called for single-link sequences
        if self.sequence.links.len() != 1 {
            return Vec::new();
        }

        let (link_type, target) = &self.sequence.links[0];
        let event_type_a = &self.sequence.head.event;
        let event_type_b = &target.event;

        if tracing::enabled!(tracing::Level::TRACE) {
            trace!(
                target: "sneldb::sequence::two_pointer",
                link_type = ?link_type,
                event_type_a = %event_type_a,
                event_type_b = %event_type_b,
                "Matching sequence in group"
            );
        }

        match link_type {
            SequenceLink::FollowedBy => self.match_followed_by(
                group,
                event_type_a,
                event_type_b,
                zones_by_event_type,
                where_evaluator,
                time_field,
            ),
            SequenceLink::PrecededBy => self.match_preceded_by(
                group,
                event_type_a,
                event_type_b,
                zones_by_event_type,
                where_evaluator,
                time_field,
            ),
        }
    }

    fn validate_group(&self, group: &GroupedRowIndices) -> bool {
        // Check for both event types
        let event_type_a = &self.sequence.head.event;
        let event_type_b = if let Some((_, target)) = self.sequence.links.first() {
            &target.event
        } else {
            return false;
        };

        let a_indices = group.rows_by_type.get(event_type_a);
        let b_indices = group.rows_by_type.get(event_type_b);

        !(a_indices.is_none() || a_indices.unwrap().is_empty())
            && !(b_indices.is_none() || b_indices.unwrap().is_empty())
    }
}

