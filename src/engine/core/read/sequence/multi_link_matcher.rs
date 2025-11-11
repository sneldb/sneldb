use crate::command::types::{EventSequence, SequenceLink};
use crate::engine::core::CandidateZone;
use crate::engine::core::filter::condition::{FieldAccessor, PreparedAccessor};
use crate::engine::core::read::sequence::group::{GroupedRowIndices, RowIndex};
use crate::engine::core::read::sequence::matching_strategy::MatchingStrategy;
use crate::engine::core::read::sequence::matcher::MatchedSequenceIndices;
use crate::engine::core::read::sequence::where_evaluator::SequenceWhereEvaluator;
use std::collections::HashMap;
use tracing::{debug, trace};

/// Matches sequences with multiple links using recursive algorithm.
///
/// Handles sequences like: A FOLLOWED BY B FOLLOWED BY C LINKED BY field
pub struct MultiLinkMatcher {
    sequence: EventSequence,
    event_types: Vec<String>, // Cached for performance
}

impl MultiLinkMatcher {
    pub fn new(sequence: EventSequence) -> Self {
        let event_types = Self::extract_event_types(&sequence);
        Self {
            sequence,
            event_types,
        }
    }

    /// Extracts all event types from sequence.
    fn extract_event_types(sequence: &EventSequence) -> Vec<String> {
        let mut types = vec![sequence.head.event.clone()];
        for (_, target) in &sequence.links {
            types.push(target.event.clone());
        }
        types
    }

    /// Recursive matching algorithm for multi-link sequences.
    ///
    /// Matches sequences by recursively matching each link in order.
    /// For each matched prefix, finds matching events for the next link.
    fn match_recursive(
        &self,
        group: &GroupedRowIndices,
        zones_by_event_type: &HashMap<String, Vec<CandidateZone>>,
        where_evaluator: Option<&SequenceWhereEvaluator>,
        time_field: &str,
        current_matches: Vec<(String, RowIndex)>,
        link_idx: usize,
    ) -> Vec<MatchedSequenceIndices> {
        // Base case: all links matched, return complete sequence
        if link_idx >= self.sequence.links.len() {
            if self.matches_where_clause_multi(
                &current_matches,
                zones_by_event_type,
                where_evaluator,
            ) {
                return vec![MatchedSequenceIndices {
                    link_value: group.link_value.clone(),
                    matched_rows: current_matches,
                }];
            }
            return Vec::new();
        }

        let (link_type, target) = &self.sequence.links[link_idx];

        // Determine current and next event types
        let (current_event_type, current_row_opt): (&str, Option<&RowIndex>) = if current_matches.is_empty() {
            // First link: start with head event
            let head_event_type = &self.sequence.head.event;
            let head_indices = group
                .rows_by_type
                .get(head_event_type)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            (head_event_type, head_indices.first())
        } else {
            // Subsequent links: use last matched event
            let last_match = &current_matches[current_matches.len() - 1];
            (last_match.0.as_str(), Some(&last_match.1))
        };

        let next_event_type = &target.event;

        let next_indices = group
            .rows_by_type
            .get(next_event_type)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        if next_indices.is_empty() {
            return Vec::new();
        }

        let zones_current = zones_by_event_type.get(current_event_type).unwrap();
        let zones_next = zones_by_event_type.get(next_event_type).unwrap();

        let mut results = Vec::new();

        if current_matches.is_empty() {
            // First link: iterate through all head events
            let head_indices = group
                .rows_by_type
                .get(current_event_type)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);

            for current_row in head_indices {
                let ts_current = self.get_timestamp(zones_current, current_row, time_field);
                let matching_next = self.find_matching_events(
                    next_indices,
                    zones_next,
                    ts_current,
                    link_type,
                    time_field,
                );

                for next_row in matching_next {
                    let new_matches = vec![
                        (current_event_type.to_string(), current_row.clone()),
                        (next_event_type.clone(), next_row),
                    ];
                    let recursive_results = self.match_recursive(
                        group,
                        zones_by_event_type,
                        where_evaluator,
                        time_field,
                        new_matches,
                        link_idx + 1,
                    );
                    results.extend(recursive_results);
                }
            }
        } else {
            // Subsequent links: use the last matched event as current
            let last_row = current_row_opt.expect("current_row_opt should be Some when current_matches is not empty");
            let ts_current = self.get_timestamp(zones_current, last_row, time_field);

            let matching_next = self.find_matching_events(
                next_indices,
                zones_next,
                ts_current,
                link_type,
                time_field,
            );

            for next_row in matching_next {
                let mut new_matches = current_matches.clone();
                new_matches.push((next_event_type.clone(), next_row));
                let recursive_results = self.match_recursive(
                    group,
                    zones_by_event_type,
                    where_evaluator,
                    time_field,
                    new_matches,
                    link_idx + 1,
                );
                results.extend(recursive_results);
            }
        }

        results
    }

    /// Finds events matching the temporal relationship for a link.
    ///
    /// Returns indices of events that satisfy the temporal condition.
    /// Optimized to use early termination since indices are sorted by timestamp.
    fn find_matching_events(
        &self,
        indices: &[RowIndex],
        zones: &[CandidateZone],
        reference_ts: u64,
        link_type: &SequenceLink,
        time_field: &str,
    ) -> Vec<RowIndex> {
        let mut matches = Vec::new();

        match link_type {
            SequenceLink::FollowedBy => {
                // Find events where ts >= reference_ts
                // Since indices are sorted, we can use binary search for efficiency
                // but for simplicity, we'll iterate and break early
                for row_idx in indices {
                    let ts = self.get_timestamp(zones, row_idx, time_field);
                    if ts >= reference_ts {
                        matches.push(row_idx.clone());
                    }
                    // Note: We don't break here because we want all matching events
                    // for recursive matching (each can lead to different sequences)
                }
            }
            SequenceLink::PrecededBy => {
                // Find events where ts < reference_ts
                // Since indices are sorted, find the latest matching event
                let mut best_match: Option<usize> = None;
                for (idx, row_idx) in indices.iter().enumerate() {
                    let ts = self.get_timestamp(zones, row_idx, time_field);
                    if ts < reference_ts {
                        best_match = Some(idx);
                    } else {
                        break; // Sorted, so no more matches
                    }
                }
                if let Some(idx) = best_match {
                    matches.push(indices[idx].clone());
                }
            }
        }

        matches
    }

    /// Evaluates WHERE clause for multiple events.
    ///
    /// Returns true if all events pass their WHERE clause conditions.
    fn matches_where_clause_multi(
        &self,
        matched_rows: &[(String, RowIndex)],
        zones_by_event_type: &HashMap<String, Vec<CandidateZone>>,
        where_evaluator: Option<&SequenceWhereEvaluator>,
    ) -> bool {
        if let Some(evaluator) = where_evaluator {
            for (event_type, row_index) in matched_rows {
                let zones = match zones_by_event_type.get(event_type) {
                    Some(z) => z,
                    None => return false,
                };
                let zone = match zones.get(row_index.zone_idx) {
                    Some(z) => z,
                    None => return false,
                };
                if !evaluator.evaluate_row(event_type, zone, row_index) {
                    if tracing::enabled!(tracing::Level::TRACE) {
                        trace!(
                            target: "sneldb::sequence::multi_link",
                            event_type = %event_type,
                            row_idx = row_index.row_idx,
                            "WHERE clause failed for event"
                        );
                    }
                    return false;
                }
            }
        }
        true
    }

    /// Gets the timestamp for a specific row index from columnar data.
    fn get_timestamp(
        &self,
        zones: &[CandidateZone],
        row_index: &RowIndex,
        time_field: &str,
    ) -> u64 {
        if let Some(zone) = zones.get(row_index.zone_idx) {
            let accessor = PreparedAccessor::new(&zone.values);
            accessor
                .get_i64_at(time_field, row_index.row_idx)
                .map(|ts| ts as u64)
                .unwrap_or(0)
        } else {
            0
        }
    }
}

impl MatchingStrategy for MultiLinkMatcher {
    fn match_in_group(
        &self,
        group: &GroupedRowIndices,
        zones_by_event_type: &HashMap<String, Vec<CandidateZone>>,
        where_evaluator: Option<&SequenceWhereEvaluator>,
        time_field: &str,
    ) -> Vec<MatchedSequenceIndices> {
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::sequence::multi_link",
                link_count = self.sequence.links.len(),
                event_types = ?self.event_types,
                "Starting multi-link matching"
            );
        }

        self.match_recursive(
            group,
            zones_by_event_type,
            where_evaluator,
            time_field,
            Vec::new(),
            0,
        )
    }

    fn validate_group(&self, group: &GroupedRowIndices) -> bool {
        // Check all event types are present
        for event_type in &self.event_types {
            if group
                .rows_by_type
                .get(event_type)
                .map(|v| v.is_empty())
                .unwrap_or(true)
            {
                return false;
            }
        }
        true
    }
}

