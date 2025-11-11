use crate::command::types::EventSequence;
use crate::engine::core::CandidateZone;
use crate::engine::core::filter::condition::{FieldAccessor, PreparedAccessor};
use crate::engine::core::read::sequence::group::{GroupedRowIndices, RowIndex};
use crate::engine::core::read::sequence::matching_strategy::MatchingStrategy;
use crate::engine::core::read::sequence::matcher_factory::MatcherFactory;
use crate::engine::core::read::sequence::where_evaluator::SequenceWhereEvaluator;
use crate::engine::types::ScalarValue;
use std::collections::HashMap;
use tracing::{debug, info, trace};

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

/// Matches sequences using strategy pattern for different sequence complexities.
///
/// This class delegates to appropriate matching strategies:
/// - TwoPointerMatcher: Optimized for 2-event sequences (single link)
/// - MultiLinkMatcher: Handles N-event sequences (multiple links)
///
/// # Performance
///
/// - O(n+m) complexity for 2-event sequences using two-pointer technique
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
    /// Matching strategy (delegates to appropriate matcher)
    strategy: Box<dyn MatchingStrategy>,
}

impl SequenceMatcher {
    /// Creates a new `SequenceMatcher` for the specified sequence.
    ///
    /// # Arguments
    ///
    /// * `sequence` - The event sequence pattern to match
    /// * `time_field` - The field name to use for time-based comparisons (e.g., "created_at", defaults to "timestamp")
    pub fn new(sequence: EventSequence, time_field: String) -> Self {
        let strategy = MatcherFactory::create_strategy(sequence.clone());
        Self {
            sequence,
            where_evaluator: None,
            time_field,
            strategy,
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
        let mut groups_with_timestamps: Vec<(u64, String, GroupedRowIndices)> =
            Vec::with_capacity(groups.len());
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
                            if let Some(ts) =
                                accessor.get_i64_at(&self.time_field, first_row.row_idx)
                            {
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
        let mut groups_skipped = 0usize;

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

            // Validate group has all required event types (delegates to strategy)
            if !self.strategy.validate_group(&group) {
                groups_skipped += 1;
                if tracing::enabled!(tracing::Level::DEBUG) && groups_skipped <= 10 {
                    debug!(
                        target: "sneldb::sequence::matcher",
                        group_index = groups_processed,
                        link_value = ?group.link_value,
                        "Group missing required event types, skipping"
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

            let matches = self
                .strategy
                .match_in_group(&group, zones_by_event_type, self.where_evaluator.as_ref(), &self.time_field);

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
                groups_skipped = groups_skipped,
                "Sequence matching completed"
            );
        }

        all_matches
    }
}
