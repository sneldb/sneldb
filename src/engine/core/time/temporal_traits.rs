use crate::command::types::CompareOp;
use roaring::RoaringBitmap;

/// Generic per-field index capability over an ordered value domain.
pub trait FieldIndex<Value: Ord + Copy> {
    /// Return candidate zone ids that may satisfy a single-value comparison.
    fn zones_intersecting(&self, op: CompareOp, v: Value) -> RoaringBitmap;

    /// Return candidate zone ids that may intersect the given value range [min, max].
    fn zones_intersecting_range(&self, min: Value, max: Value) -> RoaringBitmap;
}

/// Per-zone range-checking interface used to refine calendar candidates.
pub trait ZoneRangeIndex<Value: Ord + Copy> {
    /// Quick check whether any value in the zone may satisfy comparison with v.
    fn may_match(&self, op: CompareOp, v: Value) -> bool;

    /// Quick check whether any value in the zone may intersect [min, max].
    fn may_match_range(&self, min: Value, max: Value) -> bool;
}
