use crate::command::types::{EventSequence, SequenceLink};
use crate::engine::core::filter::condition::{FieldAccessor, PreparedAccessor};
use crate::engine::core::CandidateZone;
use crate::engine::core::read::sequence::group::{GroupedRowIndices, RowIndex};
use crate::engine::core::read::sequence::where_evaluator::SequenceWhereEvaluator;
use crate::engine::types::ScalarValue;
use std::collections::HashMap;
use tracing::{debug, info, trace, warn};

/// Represents a matched sequence with row indices (not materialized events).
///
/// This allows us to defer materialization until we know which sequences match.
#[derive(Debug, Clone)]
pub struct MatchedSequenceIndices {
    /// The link field value for this matched sequence
    pub link_value: ScalarValue,
    /// Row indices for events in the matched sequence
    /// Each tuple is (event_type, zone_idx, row_idx)
    pub matched_rows: Vec<(String, RowIndex)>,
}

/// Matches sequences using two-pointer technique on columnar data.
///
/// This class implements efficient sequence matching using a two-pointer algorithm
/// that operates on row indices rather than materialized events. It supports both
/// FOLLOWED BY and PRECEDED BY sequence links.
///
/// # Performance
///
/// - O(n+m) complexity using two-pointer technique (instead of O(n*m))
/// - Works with columnar data (no premature materialization)
/// - Supports limit short-circuiting
/// - Applies WHERE clause filtering during matching (before materialization)
pub struct SequenceMatcher {
    /// The sequence pattern to match
    sequence: EventSequence,
    /// Optional WHERE clause evaluator for filtering
    where_evaluator: Option<SequenceWhereEvaluator>,
    /// The field name to use for time-based comparisons (default: "timestamp")
    time_field: String,
}

impl SequenceMatcher {
    /// Creates a new `SequenceMatcher` for the specified sequence.
    ///
    /// # Arguments
    ///
    /// * `sequence` - The event sequence pattern to match
    /// * `time_field` - The field name to use for time-based comparisons (e.g., "created_at", defaults to "timestamp")
    pub fn new(sequence: EventSequence, time_field: String) -> Self {
        Self {
            sequence,
            where_evaluator: None,
            time_field,
        }
    }

    /// Sets the WHERE clause evaluator for filtering sequences.
    ///
    /// # Arguments
    ///
    /// * `evaluator` - The WHERE clause evaluator
    pub fn with_where_evaluator(mut self, evaluator: SequenceWhereEvaluator) -> Self {
        self.where_evaluator = Some(evaluator);
        self
    }

    /// Matches sequences in grouped row indices.
    ///
    /// This is the main entry point for sequence matching. It processes each group
    /// and finds matching sequences using the two-pointer technique.
    ///
    /// # Arguments
    ///
    /// * `groups` - Row indices grouped by link field value
    /// * `zones_by_event_type` - Zones containing columnar data for each event type
    /// * `limit` - Optional limit on number of matches to return
    ///
    /// # Returns
    ///
    /// Vector of matched sequence indices
    pub fn match_sequences(
        &self,
        groups: HashMap<String, GroupedRowIndices>,
        zones_by_event_type: &HashMap<String, Vec<CandidateZone>>,
        limit: Option<usize>,
    ) -> Vec<MatchedSequenceIndices> {
        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::sequence::matcher",
                group_count = groups.len(),
                limit = ?limit,
                has_where_evaluator = self.where_evaluator.is_some(),
                "Starting sequence matching"
            );
        }

        let mut all_matches = Vec::new();

        // Sort groups by earliest timestamp to ensure chronological processing
        // This ensures LIMIT returns the first N matches by time, not arbitrary groups
        // OPTIMIZATION: Pre-extract earliest timestamps to avoid repeated accessor creation during sorting
        let mut groups_with_timestamps: Vec<(u64, String, GroupedRowIndices)> = Vec::with_capacity(groups.len());
        for (link_key, group) in groups.into_iter() {
            // Get the earliest timestamp across all event types in this group
            // Since rows are already sorted by timestamp within each event type,
            // we just need to check the first row of each event type
            let mut earliest_ts = u64::MAX;
            for (event_type, row_indices) in &group.rows_by_type {
                if let Some(first_row) = row_indices.first() {
                    if let Some(zones) = zones_by_event_type.get(event_type) {
                        if let Some(zone) = zones.get(first_row.zone_idx) {
                            let accessor = PreparedAccessor::new(&zone.values);
                            if let Some(ts) = accessor.get_i64_at(&self.time_field, first_row.row_idx) {
                                earliest_ts = earliest_ts.min(ts as u64);
                            }
                        }
                    }
                }
            }
            groups_with_timestamps.push((earliest_ts, link_key, group));
        }

        // Sort by earliest timestamp
        groups_with_timestamps.sort_by_key(|(ts, _, _)| *ts);
        let sorted_groups: Vec<(String, GroupedRowIndices)> = groups_with_timestamps
            .into_iter()
            .map(|(_, key, group)| (key, group))
            .collect();

        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::sequence::matcher",
                sorted_group_count = sorted_groups.len(),
                "Sorted groups by earliest timestamp for chronological processing"
            );
        }

        let mut groups_processed = 0usize;
        let mut groups_with_matches = 0usize;
        let mut groups_empty_a = 0usize;
        let mut groups_empty_b = 0usize;

        // Get event types from sequence
        let event_type_a = &self.sequence.head.event;
        let event_type_b = if let Some((_, target)) = self.sequence.links.first() {
            &target.event
        } else {
            return all_matches;
        };

        for (_link_key, group) in sorted_groups {
            groups_processed += 1;

            // Check limit before processing group
            if let Some(lim) = limit {
                if all_matches.len() >= lim {
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        debug!(
                            target: "sneldb::sequence::matcher",
                            limit = lim,
                            matches_found = all_matches.len(),
                            groups_processed = groups_processed,
                            "Limit reached, stopping group processing"
                        );
                    }
                    break;
                }
            }

            // Check if group has events of both types
            let a_indices = group.rows_by_type.get(event_type_a);
            let b_indices = group.rows_by_type.get(event_type_b);

            if a_indices.is_none() || a_indices.unwrap().is_empty() {
                groups_empty_a += 1;
                if tracing::enabled!(tracing::Level::DEBUG) && groups_empty_a <= 10 {
                    debug!(
                        target: "sneldb::sequence::matcher",
                        group_index = groups_processed,
                        link_value = ?group.link_value,
                        event_type_a = %event_type_a,
                        "Group has no events of type A, skipping"
                    );
                }
                continue;
            }

            if b_indices.is_none() || b_indices.unwrap().is_empty() {
                groups_empty_b += 1;
                if tracing::enabled!(tracing::Level::DEBUG) && groups_empty_b <= 10 {
                    debug!(
                        target: "sneldb::sequence::matcher",
                        group_index = groups_processed,
                        link_value = ?group.link_value,
                        event_type_b = %event_type_b,
                        "Group has no events of type B, skipping"
                    );
                }
                continue;
            }

            if tracing::enabled!(tracing::Level::TRACE) {
                let total_rows: usize = group.rows_by_type.values().map(|v| v.len()).sum();
                trace!(
                    target: "sneldb::sequence::matcher",
                    group_index = groups_processed,
                    total_rows = total_rows,
                    event_types = ?group.rows_by_type.keys().collect::<Vec<_>>(),
                    "Processing group"
                );
            }

            let matches = self.match_in_group(&group, zones_by_event_type);

            if !matches.is_empty() {
                groups_with_matches += 1;
                if tracing::enabled!(tracing::Level::TRACE) {
                    trace!(
                        target: "sneldb::sequence::matcher",
                        matches_in_group = matches.len(),
                        "Found matches in group"
                    );
                }
            }

            all_matches.extend(matches);

            // Check limit after processing group
            if let Some(lim) = limit {
                if all_matches.len() >= lim {
                    all_matches.truncate(lim);
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        debug!(
                            target: "sneldb::sequence::matcher",
                            limit = lim,
                            groups_processed = groups_processed,
                            "Limit reached after group processing"
                        );
                    }
                    break;
                }
            }
        }

        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::sequence::matcher",
                matches_found = all_matches.len(),
                groups_processed = groups_processed,
                groups_with_matches = groups_with_matches,
                groups_empty_a = groups_empty_a,
                groups_empty_b = groups_empty_b,
                "Sequence matching completed"
            );
        }

        all_matches
    }

    /// Matches sequences within a single group.
    ///
    /// This method handles the sequence matching logic for a single link field value.
    /// It delegates to specific matchers based on the sequence link type.
    fn match_in_group(
        &self,
        group: &GroupedRowIndices,
        zones_by_event_type: &HashMap<String, Vec<CandidateZone>>,
    ) -> Vec<MatchedSequenceIndices> {
        // Handle single link (A FOLLOWED BY B or A PRECEDED BY B)
        if self.sequence.links.len() == 1 {
            let (link_type, target) = &self.sequence.links[0];
            let event_type_a = &self.sequence.head.event;
            let event_type_b = &target.event;

            if tracing::enabled!(tracing::Level::TRACE) {
                trace!(
                    target: "sneldb::sequence::matcher",
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
                ),
                SequenceLink::PrecededBy => self.match_preceded_by(
                    group,
                    event_type_a,
                    event_type_b,
                    zones_by_event_type,
                ),
            }
        } else {
            // Multiple links - for now, return empty (Phase 4 feature)
            if tracing::enabled!(tracing::Level::WARN) {
                warn!(
                    target: "sneldb::sequence::matcher",
                    link_count = self.sequence.links.len(),
                    "Multiple sequence links not yet supported"
                );
            }
            Vec::new()
        }
    }

    /// Matches A FOLLOWED BY B using two-pointer technique.
    ///
    /// This implements O(n+m) matching by using two pointers that advance
    /// through sorted row indices. When event B's timestamp is greater than
    /// event A's timestamp, we have a match.
    fn match_followed_by(
        &self,
        group: &GroupedRowIndices,
        event_type_a: &str,
        event_type_b: &str,
        zones_by_event_type: &HashMap<String, Vec<CandidateZone>>,
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
                target: "sneldb::sequence::matcher",
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

            let ts_a = self.get_timestamp(zones_a, row_a);
            let ts_b = self.get_timestamp(zones_b, row_b);

            if tracing::enabled!(tracing::Level::DEBUG) && comparisons <= 20 {
                debug!(
                    target: "sneldb::sequence::matcher",
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
                );

                if tracing::enabled!(tracing::Level::DEBUG) && timestamp_passed <= 20 {
                    debug!(
                        target: "sneldb::sequence::matcher",
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
                            target: "sneldb::sequence::matcher",
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
                            target: "sneldb::sequence::matcher",
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
                target: "sneldb::sequence::matcher",
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
                target: "sneldb::sequence::matcher",
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

            let ts_a = self.get_timestamp(zones_a, row_a);
            let ts_b = self.get_timestamp(zones_b, row_b);

            if tracing::enabled!(tracing::Level::TRACE) {
                trace!(
                    target: "sneldb::sequence::matcher",
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
                    let ts_next_b = self.get_timestamp(zones_b, next_b);
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
                );

                if tracing::enabled!(tracing::Level::TRACE) {
                    trace!(
                        target: "sneldb::sequence::matcher",
                        ts_a = ts_a,
                        ts_b = self.get_timestamp(zones_b, latest_row_b),
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
                        target: "sneldb::sequence::matcher",
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
                target: "sneldb::sequence::matcher",
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
    ) -> bool {
        if let Some(ref evaluator) = self.where_evaluator {
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(
                    target: "sneldb::sequence::matcher",
                    event_type_a = %event_type_a,
                    event_type_b = %event_type_b,
                    "Evaluating WHERE clause for sequence match"
                );
            }

            // Check WHERE clause for event A
            let zone_a = &zones_a[row_a.zone_idx];
            let passes_a = evaluator.evaluate_row(event_type_a, zone_a, row_a);
            if tracing::enabled!(tracing::Level::DEBUG) {
                // Extract id field value for debugging
                let accessor_a = PreparedAccessor::new(&zone_a.values);
                let id_value_a = accessor_a.get_i64_at("id", row_a.row_idx);
                debug!(
                    target: "sneldb::sequence::matcher",
                    event_type = %event_type_a,
                    row_idx = row_a.row_idx,
                    zone_idx = row_a.zone_idx,
                    passes = passes_a,
                    id_value = ?id_value_a,
                    available_fields = ?zone_a.values.keys().collect::<Vec<_>>(),
                    "WHERE clause evaluation for event A"
                );
            }
            if !passes_a {
                if tracing::enabled!(tracing::Level::DEBUG) {
                    debug!(
                        target: "sneldb::sequence::matcher",
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
                // Extract id field value for debugging
                let accessor_b = PreparedAccessor::new(&zone_b.values);
                let id_value_b = accessor_b.get_i64_at("id", row_b.row_idx);
                debug!(
                    target: "sneldb::sequence::matcher",
                    event_type = %event_type_b,
                    row_idx = row_b.row_idx,
                    zone_idx = row_b.zone_idx,
                    passes = passes_b,
                    id_value = ?id_value_b,
                    available_fields = ?zone_b.values.keys().collect::<Vec<_>>(),
                    "WHERE clause evaluation for event B"
                );
            }
            if !passes_b {
                if tracing::enabled!(tracing::Level::DEBUG) {
                    debug!(
                        target: "sneldb::sequence::matcher",
                        event_type = %event_type_b,
                        row_idx = row_b.row_idx,
                        "WHERE clause failed for event B"
                    );
                }
                return false;
            }

            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(
                    target: "sneldb::sequence::matcher",
                    "Both events passed WHERE clause"
                );
            }
        } else {
            if tracing::enabled!(tracing::Level::TRACE) {
                trace!(
                    target: "sneldb::sequence::matcher",
                    "No WHERE evaluator, match passes"
                );
            }
        }
        true
    }

    /// Gets the timestamp for a specific row index from columnar data.
    ///
    /// This helper method extracts timestamps without materializing events.
    /// Uses the configured time_field (default: "timestamp").
    fn get_timestamp(&self, zones: &[CandidateZone], row_index: &RowIndex) -> u64 {
        if let Some(zone) = zones.get(row_index.zone_idx) {
            let accessor = PreparedAccessor::new(&zone.values);
            let result = accessor
                .get_i64_at(&self.time_field, row_index.row_idx)
                .map(|ts| ts as u64)
                .unwrap_or_else(|| {
                    if tracing::enabled!(tracing::Level::WARN) {
                        warn!(
                            target: "sneldb::sequence::matcher",
                            time_field = %self.time_field,
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
                warn!(
                    target: "sneldb::sequence::matcher",
                    zone_idx = row_index.zone_idx,
                    zones_len = zones.len(),
                    "Zone index out of bounds, using timestamp 0"
                );
            }
            0
        }
    }
}

