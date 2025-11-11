use crate::engine::core::CandidateZone;
use crate::engine::core::read::sequence::group::GroupedRowIndices;
use crate::engine::core::read::sequence::matcher::MatchedSequenceIndices;
use crate::engine::core::read::sequence::where_evaluator::SequenceWhereEvaluator;
use std::collections::HashMap;

/// Trait for sequence matching strategies.
///
/// Different strategies handle different sequence complexities:
/// - TwoPointerMatcher: Optimized for 2-event sequences (single link)
/// - MultiLinkMatcher: Handles N-event sequences (multiple links)
pub trait MatchingStrategy: Send + Sync {
    /// Matches sequences within a single group.
    ///
    /// # Arguments
    /// * `group` - Row indices grouped by link field value
    /// * `zones_by_event_type` - Zones containing columnar data for each event type
    /// * `where_evaluator` - Optional WHERE clause evaluator
    /// * `time_field` - Field name for time-based comparisons
    ///
    /// # Returns
    /// Vector of matched sequence indices
    fn match_in_group(
        &self,
        group: &GroupedRowIndices,
        zones_by_event_type: &HashMap<String, Vec<CandidateZone>>,
        where_evaluator: Option<&SequenceWhereEvaluator>,
        time_field: &str,
    ) -> Vec<MatchedSequenceIndices>;

    /// Validates that a group has all required event types.
    ///
    /// Returns true if group can potentially produce matches, false otherwise.
    fn validate_group(&self, group: &GroupedRowIndices) -> bool;
}

