use crate::engine::core::{SegmentEntry, SegmentIndex};
use tempfile::tempdir;

#[tokio::test]
async fn test_segment_index_e2e() {
    // Setup
    let temp = tempdir().unwrap();
    let shard_dir = temp.path().to_path_buf();
    let uid = "test_event_type".to_string();
    let label = "segment-L0-00001".to_string();
    let counter = 1;
    let level = 0;

    // Create dummy SegmentEntry
    let entry = SegmentEntry {
        level,
        label: label.clone(),
        counter,
        uids: vec![uid.clone()],
    };

    // Load or create SegmentIndex
    let mut index = SegmentIndex::load(&shard_dir).await.unwrap();
    assert!(index.entries.is_empty());

    // Append and verify
    index.append(entry.clone()).await.unwrap();
    assert_eq!(index.entries.len(), 1);
    assert_eq!(index.entries[0].label, label);

    // Reload and verify persistence
    let reloaded = SegmentIndex::load(&shard_dir).await.unwrap();
    assert_eq!(reloaded.entries.len(), 1);
    assert_eq!(reloaded.entries[0].uids, vec![uid.clone()]);

    // Check list_for_uid
    let listed = reloaded.list_for_uid(&uid);
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].label, label);

    // Check all_labels
    assert_eq!(reloaded.all_labels(), vec![label.clone()]);

    // Check current_level
    assert_eq!(reloaded.current_level(), level);

    for i in 2..=8 {
        let seg = SegmentEntry {
            level,
            label: format!("segment-L0-0000{}", i),
            counter: i,
            uids: vec![uid.clone()],
        };
        index.append(seg).await.unwrap();
    }

    // Check compaction trigger
    assert!(index.needs_compaction());
}
