use crate::engine::core::compaction::policy::{CompactionPolicy, KWayCountPolicy};
use crate::engine::core::{SegmentEntry, SegmentIndex};
use tempfile::tempdir;

#[tokio::test]
async fn test_segment_index_e2e() {
    // Setup
    let temp = tempdir().unwrap();
    let shard_dir = temp.path().to_path_buf();
    let uid = "test_event_type".to_string();
    let label = "00001".to_string();
    let counter = 1;
    let level = 0;

    // Create dummy SegmentEntry
    let entry = SegmentEntry {
        id: counter,
        uids: vec![uid.clone()],
    };

    // Load or create SegmentIndex
    let mut index = SegmentIndex::load(&shard_dir).await.unwrap();
    assert!(index.entries.is_empty());

    // Append and verify
    index.append(entry.clone()).await.unwrap();
    assert_eq!(index.entries.len(), 1);
    assert_eq!(format!("{:05}", index.entries[0].id), label);

    // Reload and verify persistence
    let reloaded = SegmentIndex::load(&shard_dir).await.unwrap();
    assert_eq!(reloaded.entries.len(), 1);
    assert_eq!(reloaded.entries[0].uids, vec![uid.clone()]);

    // Check list_for_uid
    let listed = reloaded.list_for_uid(&uid);
    assert_eq!(listed.len(), 1);
    assert_eq!(format!("{:05}", listed[0].id), label);

    // Check all_labels
    assert_eq!(reloaded.all_labels(), vec![label.clone()]);

    // Check current_level
    assert_eq!(reloaded.current_level(), level);

    for i in 2..=8 {
        let seg = SegmentEntry {
            id: i,
            uids: vec![uid.clone()],
        };
        index.append(seg).await.unwrap();
    }

    // Policy-based planning: with compaction_k=2 in test config and 8 L0 entries,
    // expect 4 merge plans for the same uid
    let policy = KWayCountPolicy { k: 2 };
    let plans = policy.plan(&index);
    assert_eq!(plans.len(), 4);
}
