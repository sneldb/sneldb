use crate::engine::core::SegmentIndex;
use crate::engine::core::compaction::policy::{CompactionPolicy, KWayCountPolicy};
use crate::engine::core::segment::segment_id::SegmentId;
use crate::engine::core::segment::segment_index::SegmentEntry;

fn create_index(entries: Vec<SegmentEntry>) -> SegmentIndex {
    SegmentIndex::from_entries(std::path::PathBuf::from("/tmp/unused"), entries)
}

#[test]
fn plans_chunks_of_k_per_uid_with_leftovers() {
    // Build a synthetic SegmentIndex with L0 segments for two UIDs
    let mut index = create_index(Vec::new());

    // 17 segments for uidA at L0, and 3 for uidB at L0
    for id in 0..17u32 {
        index.insert_entry(SegmentEntry {
            id,
            uids: vec!["uidA".to_string()],
        });
    }
    for id in 100..103u32 {
        // still L0 (< 10_000)
        index.insert_entry(SegmentEntry {
            id,
            uids: vec!["uidB".to_string()],
        });
    }

    // Existing L1 id will be allocated by allocator; we just assert counts
    let policy = KWayCountPolicy::new(8);
    let plans = policy.plan(&index);

    // uidA: floor(17/8) = 2 plans; uidB: floor(3/8) = 0 plans (3 < 8, and 3 < 7 threshold)
    let count_uid_a = plans.iter().filter(|p| p.uid == "uidA").count();
    let count_uid_b = plans.iter().filter(|p| p.uid == "uidB").count();
    assert_eq!(count_uid_a, 2);
    assert_eq!(count_uid_b, 0);

    // Each plan must have exactly k inputs and target level_to == 1
    for p in plans {
        assert_eq!(p.input_segment_labels.len(), 8);
        assert_eq!(p.level_from, 0);
        assert_eq!(p.level_to, 1);
        assert!(SegmentId::from(p.output_segment_id).level() == 1);
    }
}

#[test]
fn forces_compaction_when_leftovers_reach_threshold() {
    // Test forced compaction: with k=3, threshold=(k*2)/3=2, so 2 segments should be force-compacted
    let mut index = create_index(Vec::new());

    // Add 2 segments for uidA (threshold = 2, so 2 >= 2, should force compact)
    for id in 0..2u32 {
        index.insert_entry(SegmentEntry {
            id,
            uids: vec!["uidA".to_string()],
        });
    }

    let policy = KWayCountPolicy::new(3);
    let plans = policy.plan(&index);

    // Should force compact the 2 leftover segments (2 >= threshold=2)
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].uid, "uidA");
    assert_eq!(plans[0].input_segment_labels.len(), 2);
    assert_eq!(plans[0].level_from, 0);
    assert_eq!(plans[0].level_to, 1);
}

#[test]
fn waits_when_leftovers_below_threshold() {
    // Test that segments below threshold are not compacted
    // With k=3, threshold = (k*2)/3 = 2, so 1 segment should wait
    let mut index = create_index(Vec::new());

    // Add 1 segment for uidA (k=3, threshold=2, so 1 < 2, should wait)
    index.insert_entry(SegmentEntry {
        id: 0,
        uids: vec!["uidA".to_string()],
    });

    let policy = KWayCountPolicy::new(3);
    let plans = policy.plan(&index);

    // Should not compact (too few segments: 1 < threshold=2)
    assert_eq!(plans.len(), 0);
}

#[test]
fn handles_exactly_k_segments() {
    // Test normal compaction with exactly k segments
    let mut index = create_index(Vec::new());

    // Add exactly k=3 segments
    for id in 0..3u32 {
        index.insert_entry(SegmentEntry {
            id,
            uids: vec!["uidA".to_string()],
        });
    }

    let policy = KWayCountPolicy::new(3);
    let plans = policy.plan(&index);

    // Should create exactly 1 plan with k segments
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].input_segment_labels.len(), 3);
    assert_eq!(plans[0].level_from, 0);
    assert_eq!(plans[0].level_to, 1);
}

#[test]
fn handles_multi_level_compaction() {
    // Test compaction across multiple levels: L0->L1 and L1->L2
    let mut index = create_index(Vec::new());

    // Add L0 segments (0-2)
    for id in 0..3u32 {
        index.insert_entry(SegmentEntry {
            id,
            uids: vec!["uidA".to_string()],
        });
    }

    // Add L1 segments (10000-10002)
    for id in 10000..10003u32 {
        index.insert_entry(SegmentEntry {
            id,
            uids: vec!["uidA".to_string()],
        });
    }

    let policy = KWayCountPolicy::new(3);
    let plans = policy.plan(&index);

    // Should create plans for both L0->L1 and L1->L2
    assert_eq!(plans.len(), 2);

    let l0_plans: Vec<_> = plans.iter().filter(|p| p.level_from == 0).collect();
    let l1_plans: Vec<_> = plans.iter().filter(|p| p.level_from == 1).collect();

    assert_eq!(l0_plans.len(), 1);
    assert_eq!(l1_plans.len(), 1);

    assert_eq!(l0_plans[0].level_to, 1);
    assert_eq!(l1_plans[0].level_to, 2);
    assert!(SegmentId::from(l0_plans[0].output_segment_id).level() == 1);
    assert!(SegmentId::from(l1_plans[0].output_segment_id).level() == 2);
}

#[test]
fn handles_segments_with_multiple_uids() {
    // Test segments that contain multiple UIDs
    let mut index = create_index(Vec::new());

    // Add segments with multiple UIDs
    for id in 0..3u32 {
        index.insert_entry(SegmentEntry {
            id,
            uids: vec!["uidA".to_string(), "uidB".to_string()],
        });
    }

    let policy = KWayCountPolicy::new(3);
    let plans = policy.plan(&index);

    // Should create plans for both UIDs (they share the same segments)
    let uid_a_plans: Vec<_> = plans.iter().filter(|p| p.uid == "uidA").collect();
    let uid_b_plans: Vec<_> = plans.iter().filter(|p| p.uid == "uidB").collect();

    assert_eq!(uid_a_plans.len(), 1);
    assert_eq!(uid_b_plans.len(), 1);

    // Both should reference the same input segments
    assert_eq!(
        uid_a_plans[0].input_segment_labels,
        uid_b_plans[0].input_segment_labels
    );
}

#[test]
fn handles_mixed_leftovers_and_full_batches() {
    // Test mix of full batches and leftovers
    let mut index = create_index(Vec::new());

    // Add 8 segments (2 full batches of 3 + 2 leftovers)
    for id in 0..8u32 {
        index.insert_entry(SegmentEntry {
            id,
            uids: vec!["uidA".to_string()],
        });
    }

    let policy = KWayCountPolicy::new(3);
    let plans = policy.plan(&index);

    // Should create 2 full batches of 3
    // The 2 leftovers are NOT force-compacted after chunking (less aggressive)
    // They will accumulate and be compacted in a future cycle
    assert_eq!(plans.len(), 2);

    let full_batches: Vec<_> = plans
        .iter()
        .filter(|p| p.input_segment_labels.len() == 3)
        .collect();
    let leftover_batch: Vec<_> = plans
        .iter()
        .filter(|p| p.input_segment_labels.len() == 2)
        .collect();

    assert_eq!(full_batches.len(), 2);
    assert_eq!(leftover_batch.len(), 0); // No force-compaction after chunking
}

#[test]
fn handles_empty_index() {
    // Test empty index
    let index = create_index(Vec::new());

    let policy = KWayCountPolicy::new(3);
    let plans = policy.plan(&index);

    assert_eq!(plans.len(), 0);
}

#[test]
fn handles_single_segment_below_threshold() {
    // Test single segment when threshold > 1
    let mut index = create_index(Vec::new());

    index.insert_entry(SegmentEntry {
        id: 0,
        uids: vec!["uidA".to_string()],
    });

    // With k=3, threshold = 2, so 1 segment should wait
    let policy = KWayCountPolicy::new(3);
    let plans = policy.plan(&index);

    // Should not compact (1 < threshold=2)
    assert_eq!(plans.len(), 0);
}

#[test]
fn handles_k_equals_one() {
    // Edge case: k=1 (min_leftover_threshold should be 1)
    let mut index = create_index(Vec::new());

    index.insert_entry(SegmentEntry {
        id: 0,
        uids: vec!["uidA".to_string()],
    });

    let policy = KWayCountPolicy::new(1);
    let plans = policy.plan(&index);

    // With k=1, should compact immediately
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].input_segment_labels.len(), 1);
}

#[test]
fn handles_large_number_of_segments() {
    // Test with many segments to ensure proper chunking
    let mut index = create_index(Vec::new());

    // Add 25 segments (8 full batches of 3 + 1 leftover)
    for id in 0..25u32 {
        index.insert_entry(SegmentEntry {
            id,
            uids: vec!["uidA".to_string()],
        });
    }

    let policy = KWayCountPolicy::new(3);
    let plans = policy.plan(&index);

    // Should create 8 full batches + 0 forced leftover batches (1 leftover < k-1=2, so wait)
    let full_batches: Vec<_> = plans
        .iter()
        .filter(|p| p.input_segment_labels.len() == 3)
        .collect();
    let leftover_batch: Vec<_> = plans
        .iter()
        .filter(|p| p.input_segment_labels.len() == 1)
        .collect();

    assert_eq!(full_batches.len(), 8);
    assert_eq!(leftover_batch.len(), 0);
    assert_eq!(plans.len(), 8);
}

#[test]
fn handles_multiple_uids_with_different_counts() {
    // Test multiple UIDs with different segment counts
    let mut index = create_index(Vec::new());

    // uidA: 5 segments (1 full batch + 2 leftovers)
    // The 2 leftovers are NOT force-compacted after chunking - they accumulate
    for id in 0..5u32 {
        index.insert_entry(SegmentEntry {
            id,
            uids: vec!["uidA".to_string()],
        });
    }

    // uidB: 2 segments (threshold = 2, so 2 >= 2, should force compact BEFORE chunking)
    for id in 10..12u32 {
        index.insert_entry(SegmentEntry {
            id,
            uids: vec!["uidB".to_string()],
        });
    }

    // uidC: 1 segment (below threshold=2, should wait)
    index.insert_entry(SegmentEntry {
        id: 20,
        uids: vec!["uidC".to_string()],
    });

    let policy = KWayCountPolicy::new(3);
    let plans = policy.plan(&index);

    // uidA: 1 full batch (3) = 1 plan (2 leftovers accumulate, not force-compacted)
    // uidB: 1 forced batch (2) = 1 plan (accumulated leftovers >= threshold)
    // uidC: 0 plans (below threshold)
    assert_eq!(plans.len(), 2);

    let uid_a_plans: Vec<_> = plans.iter().filter(|p| p.uid == "uidA").collect();
    let uid_b_plans: Vec<_> = plans.iter().filter(|p| p.uid == "uidB").collect();
    let uid_c_plans: Vec<_> = plans.iter().filter(|p| p.uid == "uidC").collect();

    assert_eq!(uid_a_plans.len(), 1);
    assert_eq!(uid_b_plans.len(), 1);
    assert_eq!(uid_c_plans.len(), 0);

    // Verify uidA has one batch of 3 (leftovers accumulate, not force-compacted)
    assert_eq!(uid_a_plans[0].input_segment_labels.len(), 3);

    // Verify uidB has one batch of 2 (accumulated leftovers force-compacted)
    assert_eq!(uid_b_plans[0].input_segment_labels.len(), 2);
}

#[test]
fn handles_progressive_compaction_through_levels() {
    // Test that leftovers at one level get compacted when they accumulate
    let mut index = create_index(Vec::new());

    // Start with L0: 2 segments (threshold = 2, so 2 >= 2, should force compact to L1)
    for id in 0..2u32 {
        index.insert_entry(SegmentEntry {
            id,
            uids: vec!["uidA".to_string()],
        });
    }

    let policy = KWayCountPolicy::new(3);
    let plans = policy.plan(&index);

    // Should force compact L0->L1
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].level_from, 0);
    assert_eq!(plans[0].level_to, 1);

    // Simulate the compaction: remove L0 segments, add L1 segment
    let mut index2 = create_index(Vec::new());
    index2.insert_entry(SegmentEntry {
        id: 10000, // L1 segment
        uids: vec!["uidA".to_string()],
    });

    // Add 1 more L0 segment (now 1 L0 + 1 L1)
    index2.insert_entry(SegmentEntry {
        id: 0,
        uids: vec!["uidA".to_string()],
    });

    let plans2 = policy.plan(&index2);

    // L0: 1 segment (below threshold=2, wait)
    // L1: 1 segment (below threshold=2, wait)
    assert_eq!(plans2.len(), 0);

    // Add 1 more L1 segment (now 2 L1 segments, threshold=2, should force compact)
    index2.insert_entry(SegmentEntry {
        id: 10001,
        uids: vec!["uidA".to_string()],
    });

    let plans3 = policy.plan(&index2);

    // Should force compact L1->L2
    let l1_plans: Vec<_> = plans3.iter().filter(|p| p.level_from == 1).collect();
    assert_eq!(l1_plans.len(), 1);
    assert_eq!(l1_plans[0].level_to, 2);
    assert_eq!(l1_plans[0].input_segment_labels.len(), 2);
}

#[test]
fn handles_segments_with_overlapping_uids() {
    // Test segments where UIDs overlap but not all segments have all UIDs
    let mut index = create_index(Vec::new());

    // Segment 0: uidA only
    index.insert_entry(SegmentEntry {
        id: 0,
        uids: vec!["uidA".to_string()],
    });

    // Segments 1-2: both uidA and uidB
    for id in 1..3u32 {
        index.insert_entry(SegmentEntry {
            id,
            uids: vec!["uidA".to_string(), "uidB".to_string()],
        });
    }

    let policy = KWayCountPolicy::new(3);
    let plans = policy.plan(&index);

    // uidA: 3 segments (full batch)
    // uidB: 2 segments (k-1, should force compact)
    let uid_a_plans: Vec<_> = plans.iter().filter(|p| p.uid == "uidA").collect();
    let uid_b_plans: Vec<_> = plans.iter().filter(|p| p.uid == "uidB").collect();

    assert_eq!(uid_a_plans.len(), 1);
    assert_eq!(uid_b_plans.len(), 1);

    assert_eq!(uid_a_plans[0].input_segment_labels.len(), 3);
    assert_eq!(uid_b_plans[0].input_segment_labels.len(), 2);
}
