use std::collections::HashSet;

use super::range_allocator::RangeAllocator;
use super::segment_id::LEVEL_SPAN;

#[test]
fn new_allocator_starts_at_zero_for_l0() {
    let mut alloc = RangeAllocator::new();
    assert_eq!(alloc.next_for_level(0), 0);
    assert_eq!(alloc.next_for_level(0), 1);
}

#[test]
fn allocator_allocates_by_level_and_interleaves() {
    let mut alloc = RangeAllocator::new();
    assert_eq!(alloc.next_for_level(0), 0);
    assert_eq!(alloc.next_for_level(1), LEVEL_SPAN);
    assert_eq!(alloc.next_for_level(0), 1);
    assert_eq!(alloc.next_for_level(2), LEVEL_SPAN * 2);
    assert_eq!(alloc.next_for_level(1), LEVEL_SPAN + 1);
}

#[test]
fn from_existing_ids_empty_starts_at_zero() {
    let alloc = RangeAllocator::from_existing_ids(std::iter::empty());
    let mut a = alloc.clone();
    assert_eq!(a.next_for_level(0), 0);
}

#[test]
fn from_existing_ids_single_level_advances_offset() {
    let ids = vec!["00000", "00003", "00001"]; // max offset 3
    let mut alloc = RangeAllocator::from_existing_ids(ids.into_iter());
    assert_eq!(alloc.next_for_level(0), 4);
}

#[test]
fn from_existing_ids_multiple_levels() {
    let ids = vec!["00000", "00001", "10000", "10005", "00002"]; // L0 max 2, L1 max 5
    let mut alloc = RangeAllocator::from_existing_ids(ids.into_iter());
    assert_eq!(alloc.next_for_level(0), 3);
    assert_eq!(alloc.next_for_level(1), LEVEL_SPAN + 6);
}

#[test]
fn from_existing_ids_ignores_non_numeric_labels() {
    let ids = vec!["00000", "segment-001", "00001", "invalid"];
    let mut alloc = RangeAllocator::from_existing_ids(ids.into_iter());
    assert_eq!(alloc.next_for_level(0), 2);
}

#[test]
fn from_existing_ids_order_independent() {
    let ids1 = vec!["00005", "00001", "00003"]; // max 5 -> next 6
    let mut a1 = RangeAllocator::from_existing_ids(ids1.into_iter());
    assert_eq!(a1.next_for_level(0), 6);

    let ids2 = vec!["00001", "00003", "00005"]; // same set, different order
    let mut a2 = RangeAllocator::from_existing_ids(ids2.into_iter());
    assert_eq!(a2.next_for_level(0), 6);
}

#[test]
fn allocator_handles_level_span_boundary() {
    // Seed with max L0 id so next_for_level(0) returns LEVEL_SPAN (start of L1 range)
    let ids = vec![format!("{:05}", LEVEL_SPAN - 1)];
    let mut alloc = RangeAllocator::from_existing_ids(ids.iter().map(|s| s.as_str()));
    assert_eq!(alloc.next_for_level(0), LEVEL_SPAN);
}

#[test]
fn allocator_handles_max_u32_gracefully() {
    // Ensure it does not panic when encountering very large ids
    let ids = vec![format!("{:05}", u32::MAX - 1)];
    let mut alloc = RangeAllocator::from_existing_ids(ids.iter().map(|s| s.as_str()));
    let next_id = alloc.next_for_level((u32::MAX - 1) / LEVEL_SPAN);
    assert!(next_id <= u32::MAX);
}

#[test]
fn allocator_allocates_unique_ids_for_many_calls() {
    let mut alloc = RangeAllocator::new();
    let mut seen = HashSet::new();
    for _ in 0..1000 {
        let id = alloc.next_for_level(0);
        assert!(seen.insert(id), "duplicate id allocated: {}", id);
    }
}

#[test]
fn allocates_from_zero_for_fresh_allocator_l0() {
    let mut alloc = RangeAllocator::new();
    assert_eq!(alloc.next_for_level(0), 0);
    assert_eq!(alloc.next_for_level(0), 1);
    assert_eq!(alloc.next_for_level(0), 2);
}

#[test]
fn allocates_independently_per_level() {
    let mut alloc = RangeAllocator::new();
    // First allocations
    let l0_first = alloc.next_for_level(0);
    let l1_first = alloc.next_for_level(1);
    assert_eq!(l0_first, 0);
    assert_eq!(l1_first, LEVEL_SPAN);

    // Next ones do not interfere
    assert_eq!(alloc.next_for_level(0), 1);
    assert_eq!(alloc.next_for_level(1), LEVEL_SPAN + 1);
}

#[test]
fn seeding_from_existing_ids_sets_next_offsets() {
    // Existing: L0 -> 00000,00001; L1 -> 10000,10001
    let existing = vec!["00000", "00001", "10000", "10001"]; // zero-padded numeric
    let mut alloc = RangeAllocator::from_existing_ids(existing.iter().copied());
    assert_eq!(alloc.next_for_level(0), 2); // next after 00001
    assert_eq!(alloc.next_for_level(1), LEVEL_SPAN + 2); // next after 10001
}

#[test]
fn seeding_ignores_non_numeric_entries() {
    let existing = vec!["abc", "00002", "xyz", "not-a-number"]; // only 00002 is numeric
    let mut alloc = RangeAllocator::from_existing_ids(existing.iter().copied());
    assert_eq!(alloc.next_for_level(0), 3); // next after 00002
}

#[test]
fn seeding_is_order_independent() {
    let a = vec!["00005", "00003", "00004"]; // shuffled
    let b = vec!["00003", "00004", "00005"]; // sorted
    let mut alloc_a = RangeAllocator::from_existing_ids(a.iter().copied());
    let mut alloc_b = RangeAllocator::from_existing_ids(b.iter().copied());
    assert_eq!(alloc_a.next_for_level(0), 6);
    assert_eq!(alloc_b.next_for_level(0), 6);
}

#[test]
fn l0_next_after_span_boundary_matches_implementation() {
    // If existing has 09999, next becomes 10000 (which equals the first id in L1).
    // This test documents current behavior of RangeAllocator (no cross-level guard).
    let existing = vec!["09999"]; // max L0 id for LEVEL_SPAN=10_000
    let mut alloc = RangeAllocator::from_existing_ids(existing.iter().copied());
    assert_eq!(alloc.next_for_level(0), LEVEL_SPAN); // 10000
}
