use crate::engine::core::SegmentIndex;
use crate::engine::core::compaction::policy::{CompactionPolicy, KWayCountPolicy};
use crate::engine::core::segment::segment_id::SegmentId;

#[test]
fn plans_chunks_of_k_per_uid_with_leftovers() {
    // Build a synthetic SegmentIndex with L0 segments for two UIDs
    let mut index = SegmentIndex::from_entries(std::path::PathBuf::from("/tmp/unused"), Vec::new());

    // 17 segments for uidA at L0, and 3 for uidB at L0
    for id in 0..17u32 {
        index.insert_entry(crate::engine::core::segment::segment_index::SegmentEntry {
            id,
            uids: vec!["uidA".to_string()],
        });
    }
    for id in 100..103u32 {
        // still L0 (< 10_000)
        index.insert_entry(crate::engine::core::segment::segment_index::SegmentEntry {
            id,
            uids: vec!["uidB".to_string()],
        });
    }

    // Existing L1 id will be allocated by allocator; we just assert counts
    let policy = KWayCountPolicy { k: 8 };
    let plans = policy.plan(&index);

    // uidA: floor(17/8) = 2 plans; uidB: floor(3/8) = 0 plans
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
