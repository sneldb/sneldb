use crate::engine::core::{CandidateZone, SegmentZoneId};
use std::collections::HashSet;
use tracing::debug;

/// A filter for candidate zones based on an allowed set of segment/zone combinations.
///
/// This struct encapsulates the logic for filtering candidate zones to only include
/// those that are in the allowed set. It handles normalization of segment IDs
/// (with or without "segment-" prefix) to ensure consistent matching.
pub struct ZoneFilter {
    allowed: HashSet<SegmentZoneId>,
}

impl ZoneFilter {
    /// Creates a new ZoneFilter with the given set of allowed zone identifiers.
    pub fn new(allowed: HashSet<SegmentZoneId>) -> Self {
        Self { allowed }
    }

    /// Creates a ZoneFilter from a set of (segment_id, zone_id) tuples.
    pub fn from_tuples<I>(tuples: I) -> Self
    where
        I: IntoIterator<Item = (String, u32)>,
    {
        let allowed = tuples.into_iter().map(SegmentZoneId::from).collect();
        Self::new(allowed)
    }

    /// Returns the number of allowed zones in this filter.
    pub fn allowed_count(&self) -> usize {
        self.allowed.len()
    }

    /// Checks if a specific zone is allowed by this filter.
    pub fn allows(&self, zone: &CandidateZone) -> bool {
        let id = SegmentZoneId::from_zone(zone);
        self.allows_id(&id)
    }

    /// Checks if a specific SegmentZoneId is allowed by this filter.
    ///
    /// This performs normalized comparison, so segment IDs with or without
    /// the "segment-" prefix will match correctly.
    pub fn allows_id(&self, id: &SegmentZoneId) -> bool {
        // Build normalized lookup set - this could be cached but for now
        // we build it fresh each time to keep the API simple
        let normalized_allowed: HashSet<(&str, u32)> =
            self.allowed.iter().map(|id| id.normalized()).collect();

        normalized_allowed.contains(&id.normalized())
    }

    /// Applies the filter to a mutable vector of candidate zones.
    ///
    /// This method removes any zones that are not in the allowed set,
    /// modifying the vector in place. It logs before and after counts
    /// for debugging purposes.
    pub fn apply(&self, candidate_zones: &mut Vec<CandidateZone>) {
        debug!(
            target: "sneldb::query",
            "Applying zone filter with {} allowed entries",
            self.allowed.len()
        );

        // Build normalized lookup set once to avoid repeated allocations
        let normalized_allowed: HashSet<(&str, u32)> =
            self.allowed.iter().map(|id| id.normalized()).collect();

        let before_count = candidate_zones.len();

        candidate_zones.retain(|z| {
            let id = SegmentZoneId::from_zone(z);
            normalized_allowed.contains(&id.normalized())
        });

        let after_count = candidate_zones.len();
        debug!(
            target: "sneldb::query",
            before = before_count,
            after = after_count,
            filtered = before_count - after_count,
            "Zone filter applied"
        );
    }

    /// Returns true if the filter allows all zones (no restrictions).
    pub fn is_empty(&self) -> bool {
        self.allowed.is_empty()
    }

    /// Returns a reference to the underlying allowed set.
    pub fn allowed_zones(&self) -> &HashSet<SegmentZoneId> {
        &self.allowed
    }
}

impl std::fmt::Debug for ZoneFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZoneFilter")
            .field("allowed_count", &self.allowed.len())
            .finish()
    }
}
