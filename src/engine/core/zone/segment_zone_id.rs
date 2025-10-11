use crate::engine::core::CandidateZone;

/// A type-safe identifier for a zone within a segment.
///
/// This type encapsulates the combination of segment_id and zone_id,
/// providing normalization logic for segment IDs that may or may not
/// have the "segment-" prefix.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SegmentZoneId {
    segment_id: String,
    zone_id: u32,
}

impl SegmentZoneId {
    /// Creates a new SegmentZoneId.
    pub fn new(segment_id: String, zone_id: u32) -> Self {
        Self {
            segment_id,
            zone_id,
        }
    }

    /// Returns the segment ID as stored (may include "segment-" prefix).
    pub fn segment_id(&self) -> &str {
        &self.segment_id
    }

    /// Returns the zone ID.
    pub fn zone_id(&self) -> u32 {
        self.zone_id
    }

    /// Returns a normalized tuple of (segment_id, zone_id).
    ///
    /// The segment ID is normalized by stripping the "segment-" prefix if present.
    /// This is useful for comparisons when segment IDs may or may not have the prefix.
    pub fn normalized(&self) -> (&str, u32) {
        let normalized_id = self
            .segment_id
            .strip_prefix("segment-")
            .unwrap_or(&self.segment_id);
        (normalized_id, self.zone_id)
    }

    /// Creates a SegmentZoneId from a CandidateZone.
    pub fn from_zone(zone: &CandidateZone) -> Self {
        Self::new(zone.segment_id.clone(), zone.zone_id)
    }

    /// Creates a SegmentZoneId from a tuple of (segment_id, zone_id).
    pub fn from_tuple(tuple: (String, u32)) -> Self {
        Self::new(tuple.0, tuple.1)
    }
}

impl From<(String, u32)> for SegmentZoneId {
    fn from(tuple: (String, u32)) -> Self {
        Self::new(tuple.0, tuple.1)
    }
}

impl From<(&str, u32)> for SegmentZoneId {
    fn from(tuple: (&str, u32)) -> Self {
        Self::new(tuple.0.to_string(), tuple.1)
    }
}
