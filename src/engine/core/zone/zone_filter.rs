use crate::engine::core::{CandidateZone, SegmentZoneId};
use std::collections::HashSet;
use tracing::debug;

/// A filter for candidate zones based on an allowed set of segment/zone combinations.
///
/// This struct encapsulates the logic for filtering candidate zones to only include
/// those that are in the allowed set. Segment IDs are numeric-only in the
/// current scheme, so no normalization is required.
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
    pub fn allows_id(&self, id: &SegmentZoneId) -> bool {
        self.allowed.contains(id)
    }

    /// Applies the filter to a mutable vector of candidate zones.
    ///
    /// This method removes any zones that are not in the allowed set,
    /// modifying the vector in place. It logs before and after counts
    /// for debugging purposes.
    pub fn apply(&self, candidate_zones: &mut Vec<CandidateZone>) {
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::query",
                "Applying zone filter with {} allowed entries",
                self.allowed.len()
            );
        }

        let before_count = candidate_zones.len();

        candidate_zones.retain(|z| {
            let id = SegmentZoneId::from_zone(z);
            self.allowed.contains(&id)
        });

        let after_count = candidate_zones.len();
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::query",
                before = before_count,
                after = after_count,
                filtered = before_count - after_count,
                "Zone filter applied"
            );
        }
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
